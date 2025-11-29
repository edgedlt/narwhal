package narwhal

import (
	"fmt"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"
)

// sortCertificates sorts certificates by (round, author) for deterministic ordering.
func sortCertificates[H Hash](certs []*Certificate[H]) {
	slices.SortFunc(certs, func(a, b *Certificate[H]) int {
		if a.Header.Round != b.Header.Round {
			if a.Header.Round < b.Header.Round {
				return -1
			}
			return 1
		}
		if a.Header.Author < b.Header.Author {
			return -1
		}
		if a.Header.Author > b.Header.Author {
			return 1
		}
		return 0
	})
}

// Vertex represents a certified DAG vertex with its data.
type Vertex[H Hash, T Transaction[H]] struct {
	Certificate *Certificate[H]
	Batches     []*Batch[H, T]
}

// EquivocationEvidence contains proof of a validator creating conflicting certificates.
type EquivocationEvidence[H Hash] struct {
	Author       uint16
	Round        uint64
	Certificate1 *Certificate[H]
	Certificate2 *Certificate[H]
}

// PendingCertificate represents a certificate waiting for missing parents.
type PendingCertificate[H Hash, T Transaction[H]] struct {
	Certificate    *Certificate[H]
	Batches        []*Batch[H, T]
	MissingParents []H
}

// DAG manages the directed acyclic graph of certified vertices.
type DAG[H Hash, T Transaction[H]] struct {
	mu sync.RWMutex

	// Round -> Author -> Certificate
	vertices map[uint64]map[uint16]*Certificate[H]

	// Certificate digest (as string) -> Vertex
	digestToVertex map[string]*Vertex[H, T]

	// Uncommitted certificates (not yet ordered by consensus)
	uncommitted map[string]*Certificate[H]

	// Pending certificates waiting for parents (digest -> pending)
	pending map[string]*PendingCertificate[H, T]

	// Current round (advances when 2f+1 certs exist for a round)
	currentRound uint64

	// GC watermark - rounds below this have been garbage collected
	gcRound uint64

	validators ValidatorSet
	hooks      *Hooks[H, T]
	logger     *zap.Logger

	// Optimized crypto verification (optional)
	cryptoProvider CryptoProvider
	sigCache       *SignatureCache

	// LRU cache for hot vertex lookups (optional)
	// When enabled, frequently accessed vertices are cached for faster retrieval.
	vertexCache *VertexCache[H, T]

	// Callbacks (legacy - prefer using Hooks)
	onEquivocation func(evidence *EquivocationEvidence[H])
}

// NewDAG creates a new DAG instance.
func NewDAG[H Hash, T Transaction[H]](
	validators ValidatorSet,
	logger *zap.Logger,
) *DAG[H, T] {
	return NewDAGWithHooks[H, T](validators, nil, logger)
}

// NewDAGWithHooks creates a new DAG instance with observability hooks.
func NewDAGWithHooks[H Hash, T Transaction[H]](
	validators ValidatorSet,
	hooks *Hooks[H, T],
	logger *zap.Logger,
) *DAG[H, T] {
	return NewDAGWithCrypto[H, T](validators, hooks, nil, nil, logger)
}

// NewDAGWithCrypto creates a new DAG instance with optimized crypto verification.
// If cryptoProvider is non-nil, certificate validation uses batch/parallel verification.
// If sigCache is non-nil, verified signatures are cached to avoid re-verification.
func NewDAGWithCrypto[H Hash, T Transaction[H]](
	validators ValidatorSet,
	hooks *Hooks[H, T],
	cryptoProvider CryptoProvider,
	sigCache *SignatureCache,
	logger *zap.Logger,
) *DAG[H, T] {
	return NewDAGWithCache[H, T](validators, hooks, cryptoProvider, sigCache, nil, logger)
}

// DAGCacheConfig configures the optional LRU cache for DAG vertex lookups.
type DAGCacheConfig struct {
	// Enabled controls whether vertex caching is enabled.
	// When enabled, frequently accessed vertices are cached for faster retrieval.
	Enabled bool

	// Capacity is the maximum number of vertices to cache.
	// Default: 10000 if Enabled is true but Capacity is 0.
	Capacity int
}

// DefaultDAGCacheConfig returns the default cache configuration (disabled).
func DefaultDAGCacheConfig() DAGCacheConfig {
	return DAGCacheConfig{Enabled: false, Capacity: 10000}
}

// NewDAGWithCache creates a new DAG instance with all optional features.
// This is the most flexible constructor, allowing configuration of:
//   - cryptoProvider: optimized signature verification
//   - sigCache: signature verification caching
//   - cacheConfig: LRU cache for vertex lookups
func NewDAGWithCache[H Hash, T Transaction[H]](
	validators ValidatorSet,
	hooks *Hooks[H, T],
	cryptoProvider CryptoProvider,
	sigCache *SignatureCache,
	cacheConfig *DAGCacheConfig,
	logger *zap.Logger,
) *DAG[H, T] {
	if logger == nil {
		logger = zap.NewNop()
	}

	dag := &DAG[H, T]{
		vertices:       make(map[uint64]map[uint16]*Certificate[H]),
		digestToVertex: make(map[string]*Vertex[H, T]),
		uncommitted:    make(map[string]*Certificate[H]),
		pending:        make(map[string]*PendingCertificate[H, T]),
		validators:     validators,
		hooks:          hooks,
		cryptoProvider: cryptoProvider,
		sigCache:       sigCache,
		logger:         logger,
	}

	// Initialize vertex cache if enabled
	if cacheConfig != nil && cacheConfig.Enabled {
		capacity := cacheConfig.Capacity
		if capacity <= 0 {
			capacity = 10000 // default
		}
		dag.vertexCache = NewVertexCache[H, T](capacity)
		logger.Debug("DAG vertex cache enabled", zap.Int("capacity", capacity))
	}

	return dag
}

// OnEquivocation sets a callback that is invoked when equivocation is detected.
func (d *DAG[H, T]) OnEquivocation(callback func(evidence *EquivocationEvidence[H])) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onEquivocation = callback
}

// ValidateCertificate validates a certificate using the DAG's crypto provider.
// If no crypto provider is configured, falls back to standard sequential verification.
func (d *DAG[H, T]) ValidateCertificate(cert *Certificate[H]) error {
	if d.cryptoProvider != nil {
		return cert.ValidateWithCrypto(d.validators, d.cryptoProvider, d.sigCache)
	}
	return cert.Validate(d.validators)
}

// InsertValidatedCertificate validates and inserts a certificate into the DAG.
// This is a convenience method that combines validation and insertion.
// Returns an error if validation fails or if the certificate is an equivocation.
func (d *DAG[H, T]) InsertValidatedCertificate(cert *Certificate[H], batches []*Batch[H, T]) error {
	// Validate first (outside lock for parallelism)
	if err := d.ValidateCertificate(cert); err != nil {
		return fmt.Errorf("certificate validation failed: %w", err)
	}

	// Insert (holds lock)
	return d.InsertCertificate(cert, batches)
}

// InsertCertificate adds a certified vertex to the DAG.
// Returns an error if the certificate is invalid or an equivocation is detected.
// If parents are missing, the certificate is queued as pending.
func (d *DAG[H, T]) InsertCertificate(cert *Certificate[H], batches []*Batch[H, T]) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.insertCertificateLocked(cert, batches)
}

func (d *DAG[H, T]) insertCertificateLocked(cert *Certificate[H], batches []*Batch[H, T]) error {
	digest := cert.Header.Digest.String()
	round := cert.Header.Round
	author := cert.Header.Author

	// Check for duplicate
	if _, exists := d.digestToVertex[digest]; exists {
		return nil // Already have it
	}

	// Also check if already pending
	if _, exists := d.pending[digest]; exists {
		return nil // Already pending
	}

	// Check for equivocation (same author, same round, different digest)
	if d.vertices[round] != nil {
		if existing := d.vertices[round][author]; existing != nil {
			if !existing.Header.Digest.Equals(cert.Header.Digest) {
				evidence := &EquivocationEvidence[H]{
					Author:       author,
					Round:        round,
					Certificate1: existing,
					Certificate2: cert,
				}
				d.logger.Warn("equivocation detected",
					zap.Uint16("author", author),
					zap.Uint64("round", round),
					zap.String("cert1", existing.Header.Digest.String()),
					zap.String("cert2", cert.Header.Digest.String()))

				// Invoke callback if set (outside lock to avoid deadlock)
				if d.onEquivocation != nil {
					go d.onEquivocation(evidence)
				}
				// Invoke hook
				if d.hooks != nil && d.hooks.OnEquivocationDetected != nil {
					d.hooks.OnEquivocationDetected(EquivocationDetectedEvent[H]{
						Author:       author,
						Round:        round,
						FirstDigest:  existing.Header.Digest,
						SecondDigest: cert.Header.Digest,
						DetectedAt:   time.Now(),
					})
				}
				return fmt.Errorf("equivocation detected: author %d round %d", author, round)
			}
		}
	}

	// Verify parents exist (except for genesis round 0)
	var missingParents []H
	if round > 0 {
		for _, parentDigest := range cert.Header.Parents {
			if _, exists := d.digestToVertex[parentDigest.String()]; !exists {
				missingParents = append(missingParents, parentDigest)
			}
		}
	}

	// If parents are missing, queue as pending
	if len(missingParents) > 0 {
		d.pending[digest] = &PendingCertificate[H, T]{
			Certificate:    cert,
			Batches:        batches,
			MissingParents: missingParents,
		}
		d.logger.Debug("certificate pending for missing parents",
			zap.Uint64("round", round),
			zap.Uint16("author", author),
			zap.Int("missing_count", len(missingParents)))

		// Invoke hook
		if d.hooks != nil && d.hooks.OnCertificatePending != nil {
			d.hooks.OnCertificatePending(CertificatePendingEvent[H]{
				Certificate:    cert,
				MissingParents: missingParents,
				QueuedAt:       time.Now(),
			})
		}
		return nil
	}

	// Insert into vertices map
	if d.vertices[round] == nil {
		d.vertices[round] = make(map[uint16]*Certificate[H])
	}
	d.vertices[round][author] = cert

	// Create vertex
	vertex := &Vertex[H, T]{
		Certificate: cert,
		Batches:     batches,
	}

	// Insert into digest lookup
	d.digestToVertex[digest] = vertex

	// Insert into cache if enabled
	if d.vertexCache != nil {
		d.vertexCache.Put(vertex)
	}

	// Track as uncommitted
	d.uncommitted[digest] = cert

	// Get count in round before potentially advancing
	totalInRound := len(d.vertices[round])

	// Invoke hook
	if d.hooks != nil && d.hooks.OnVertexInserted != nil {
		d.hooks.OnVertexInserted(VertexInsertedEvent[H]{
			Certificate:  cert,
			Round:        round,
			Author:       author,
			ParentCount:  len(cert.Header.Parents),
			TotalInRound: totalInRound,
			InsertedAt:   time.Now(),
		})
	}

	// Check if we should advance the round
	d.maybeAdvanceRound()

	d.logger.Debug("inserted certificate",
		zap.Uint64("round", round),
		zap.Uint16("author", author),
		zap.String("digest", digest))

	// Check if this unblocks any pending certificates
	d.processPendingLocked()

	return nil
}

// processPendingLocked checks if any pending certificates can now be inserted.
func (d *DAG[H, T]) processPendingLocked() {
	// Keep processing until no more progress
	for {
		progress := false
		for digest, pending := range d.pending {
			// Check if all parents are now available
			allAvailable := true
			for _, parent := range pending.MissingParents {
				if _, exists := d.digestToVertex[parent.String()]; !exists {
					allAvailable = false
					break
				}
			}

			if allAvailable {
				delete(d.pending, digest)
				// Insert the certificate (recursive)
				if err := d.insertCertificateLocked(pending.Certificate, pending.Batches); err != nil {
					d.logger.Warn("failed to insert previously pending certificate",
						zap.String("digest", digest),
						zap.Error(err))
				}
				progress = true
				break // Restart the loop since map was modified
			}
		}
		if !progress {
			break
		}
	}
}

// maybeAdvanceRound advances to the next round if 2f+1 certs exist for current round.
func (d *DAG[H, T]) maybeAdvanceRound() {
	quorum := 2*d.validators.F() + 1
	for {
		if d.vertices[d.currentRound] == nil {
			return
		}
		certsInRound := len(d.vertices[d.currentRound])
		if certsInRound >= quorum {
			oldRound := d.currentRound
			d.currentRound++
			d.logger.Info("advanced to round", zap.Uint64("round", d.currentRound))

			// Invoke hook
			if d.hooks != nil && d.hooks.OnRoundAdvanced != nil {
				d.hooks.OnRoundAdvanced(RoundAdvancedEvent{
					OldRound:            oldRound,
					NewRound:            d.currentRound,
					CertificatesInRound: certsInRound,
					AdvancedAt:          time.Now(),
				})
			}
		} else {
			return
		}
	}
}

// GetUncommitted returns certificates not yet ordered by consensus.
// Results are sorted by (round, author) for deterministic ordering.
func (d *DAG[H, T]) GetUncommitted() []*Certificate[H] {
	d.mu.RLock()
	defer d.mu.RUnlock()

	certs := make([]*Certificate[H], 0, len(d.uncommitted))
	for _, cert := range d.uncommitted {
		certs = append(certs, cert)
	}

	// Sort by (round, author) for deterministic ordering
	sortCertificates(certs)
	return certs
}

// MarkCommitted marks certificates as committed by consensus.
// This allows garbage collection of the underlying data.
func (d *DAG[H, T]) MarkCommitted(certs []*Certificate[H]) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, cert := range certs {
		delete(d.uncommitted, cert.Header.Digest.String())
	}
}

// GetTransactions extracts all transactions from the given certificates.
// Transactions are deduplicated by hash.
func (d *DAG[H, T]) GetTransactions(certs []*Certificate[H]) ([]T, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var txs []T
	seen := make(map[string]bool)

	for _, cert := range certs {
		vertex, exists := d.digestToVertex[cert.Header.Digest.String()]
		if !exists {
			return nil, fmt.Errorf("missing vertex: %s", cert.Header.Digest.String())
		}

		for _, batch := range vertex.Batches {
			for _, tx := range batch.Transactions {
				hashStr := tx.Hash().String()
				if !seen[hashStr] {
					seen[hashStr] = true
					txs = append(txs, tx)
				}
			}
		}
	}

	return txs, nil
}

// IsCertified checks if a certificate with the given digest exists in the DAG.
// If caching is enabled, this checks the cache first for faster lookups.
func (d *DAG[H, T]) IsCertified(digest H) bool {
	// Try cache first (outside main lock for better concurrency)
	if d.vertexCache != nil {
		if d.vertexCache.Contains(digest) {
			return true
		}
	}

	d.mu.RLock()
	exists := d.digestToVertex[digest.String()] != nil
	d.mu.RUnlock()
	return exists
}

// CurrentRound returns the current DAG round.
func (d *DAG[H, T]) CurrentRound() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.currentRound
}

// GetParents returns certificate digests from the previous round to use as parents.
// Returns nil for round 0.
// Results are sorted by validator index for deterministic ordering.
func (d *DAG[H, T]) GetParents() []H {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.currentRound == 0 {
		return nil
	}

	parentRound := d.currentRound - 1
	certs := d.vertices[parentRound]
	if certs == nil {
		return nil
	}

	// Sort by validator index for deterministic ordering
	parents := make([]H, 0, len(certs))
	for i := 0; i < d.validators.Count(); i++ {
		if cert, exists := certs[uint16(i)]; exists {
			parents = append(parents, cert.Header.Digest)
		}
	}
	return parents
}

// GetCertificate retrieves a certificate by its digest.
// If caching is enabled, this checks the cache first for faster lookups.
func (d *DAG[H, T]) GetCertificate(digest H) *Certificate[H] {
	// Try cache first (outside main lock for better concurrency)
	if d.vertexCache != nil {
		if vertex, ok := d.vertexCache.Get(digest); ok {
			return vertex.Certificate
		}
	}

	d.mu.RLock()
	vertex := d.digestToVertex[digest.String()]
	d.mu.RUnlock()

	if vertex == nil {
		return nil
	}

	// Populate cache on miss if enabled
	if d.vertexCache != nil {
		d.vertexCache.Put(vertex)
	}

	return vertex.Certificate
}

// GetVertex retrieves a vertex (certificate + batches) by its digest.
// If caching is enabled, this checks the cache first for faster lookups.
func (d *DAG[H, T]) GetVertex(digest H) *Vertex[H, T] {
	// Try cache first (outside main lock for better concurrency)
	if d.vertexCache != nil {
		if vertex, ok := d.vertexCache.Get(digest); ok {
			return vertex
		}
	}

	d.mu.RLock()
	vertex := d.digestToVertex[digest.String()]
	d.mu.RUnlock()

	// Populate cache on miss if enabled
	if vertex != nil && d.vertexCache != nil {
		d.vertexCache.Put(vertex)
	}

	return vertex
}

// GetCertificatesForRound returns all certificates for a given round.
// Results are sorted by validator index for deterministic ordering.
func (d *DAG[H, T]) GetCertificatesForRound(round uint64) []*Certificate[H] {
	d.mu.RLock()
	defer d.mu.RUnlock()

	roundCerts := d.vertices[round]
	if roundCerts == nil {
		return nil
	}

	// Sort by validator index for deterministic ordering
	certs := make([]*Certificate[H], 0, len(roundCerts))
	for i := uint16(0); i < uint16(d.validators.Count()); i++ {
		if cert, exists := roundCerts[i]; exists {
			certs = append(certs, cert)
		}
	}
	return certs
}

// CertificateCountForRound returns the number of certificates for a given round.
func (d *DAG[H, T]) CertificateCountForRound(round uint64) int {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.vertices[round] == nil {
		return 0
	}
	return len(d.vertices[round])
}

// GarbageCollect removes all data for rounds strictly less than the given round.
func (d *DAG[H, T]) GarbageCollect(beforeRound uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for round := d.gcRound; round < beforeRound; round++ {
		roundCerts := d.vertices[round]
		if roundCerts != nil {
			for _, cert := range roundCerts {
				digestStr := cert.Header.Digest.String()
				delete(d.digestToVertex, digestStr)
				delete(d.uncommitted, digestStr)
				// Remove from cache if enabled
				if d.vertexCache != nil {
					d.vertexCache.Remove(cert.Header.Digest)
				}
			}
			delete(d.vertices, round)
		}
	}
	d.gcRound = beforeRound

	d.logger.Info("garbage collected",
		zap.Uint64("before_round", beforeRound),
		zap.Uint64("gc_round", d.gcRound))
}

// DAGStats contains DAG statistics for monitoring.
type DAGStats struct {
	CurrentRound     uint64
	GCRound          uint64
	TotalVertices    int
	UncommittedCount int
	PendingCount     int
	RoundCounts      map[uint64]int
	// CacheStats contains LRU cache statistics (nil if caching disabled)
	CacheStats *LRUCacheStats
}

// Stats returns current DAG statistics.
func (d *DAG[H, T]) Stats() DAGStats {
	d.mu.RLock()
	defer d.mu.RUnlock()

	roundCounts := make(map[uint64]int)
	for round, certs := range d.vertices {
		roundCounts[round] = len(certs)
	}

	stats := DAGStats{
		CurrentRound:     d.currentRound,
		GCRound:          d.gcRound,
		TotalVertices:    len(d.digestToVertex),
		UncommittedCount: len(d.uncommitted),
		PendingCount:     len(d.pending),
		RoundCounts:      roundCounts,
	}

	// Include cache stats if caching is enabled
	if d.vertexCache != nil {
		cacheStats := d.vertexCache.Stats()
		stats.CacheStats = &cacheStats
	}

	return stats
}

// GetPendingCertificates returns all certificates waiting for missing parents.
func (d *DAG[H, T]) GetPendingCertificates() []*PendingCertificate[H, T] {
	d.mu.RLock()
	defer d.mu.RUnlock()

	result := make([]*PendingCertificate[H, T], 0, len(d.pending))
	for _, p := range d.pending {
		result = append(result, p)
	}
	return result
}

// GetMissingParents returns all unique parent digests that are missing from the DAG.
func (d *DAG[H, T]) GetMissingParents() []H {
	d.mu.RLock()
	defer d.mu.RUnlock()

	seen := make(map[string]bool)
	var missing []H

	for _, p := range d.pending {
		for _, parent := range p.MissingParents {
			parentStr := parent.String()
			if !seen[parentStr] {
				seen[parentStr] = true
				if _, exists := d.digestToVertex[parentStr]; !exists {
					missing = append(missing, parent)
				}
			}
		}
	}
	return missing
}
