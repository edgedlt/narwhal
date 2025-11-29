// Package narwhal provides optimized cryptographic verification for both
// Ed25519 and BLS signature schemes.

package narwhal

import (
	"sync"
)

// SignatureScheme identifies the signature algorithm in use.
type SignatureScheme uint8

const (
	// SignatureSchemeEd25519 uses Ed25519 signatures (individual verification).
	SignatureSchemeEd25519 SignatureScheme = iota

	// SignatureSchemeBLS uses BLS signatures (supports aggregation and batch verification).
	SignatureSchemeBLS
)

// BatchVerifier provides optimized signature verification.
// For BLS, it can verify multiple signatures in a single operation.
// For Ed25519, it parallelizes verification across goroutines.
//
// Usage:
//
//	verifier := NewBatchVerifier(validators, SignatureSchemeBLS)
//	for i, sig := range signatures {
//	    verifier.Add(message, sig, validatorIndex)
//	}
//	if err := verifier.Verify(); err != nil {
//	    // verification failed
//	}
type BatchVerifier interface {
	// Add queues a signature for batch verification.
	// validatorIndex is used to look up the public key.
	Add(message []byte, signature []byte, validatorIndex uint16)

	// Verify performs batch verification of all queued signatures.
	// Returns nil if all signatures are valid, or an error describing the failure.
	Verify() error

	// Reset clears all queued signatures for reuse.
	Reset()
}

// BLSBatchVerifier provides batch verification capability for BLS signatures.
// This interface should be implemented by the integrating application using
// their preferred BLS library (e.g., blst, bls12-381).
type BLSBatchVerifier interface {
	// AddSignature adds a signature to the batch.
	// pubKey is the raw public key bytes.
	// message is the data that was signed.
	// signature is the BLS signature bytes.
	AddSignature(pubKey []byte, message []byte, signature []byte)

	// VerifyBatch verifies all signatures added to the batch.
	// Returns true if all signatures are valid.
	VerifyBatch() bool

	// Reset clears the batch for reuse.
	Reset()
}

// BLSAggregator provides signature aggregation for BLS.
// Aggregated signatures reduce certificate size and verification time.
type BLSAggregator interface {
	// Aggregate combines multiple BLS signatures into one.
	// Returns the aggregated signature bytes.
	Aggregate(signatures [][]byte) ([]byte, error)

	// VerifyAggregate verifies an aggregated signature against multiple public keys.
	// pubKeys are the raw public key bytes of all signers.
	// message is the data that was signed (must be same for all).
	// aggSig is the aggregated signature.
	VerifyAggregate(pubKeys [][]byte, message []byte, aggSig []byte) bool
}

// CryptoProvider abstracts the cryptographic operations for Narwhal.
// Implementations should provide optimized verification for their signature scheme.
type CryptoProvider interface {
	// Scheme returns the signature scheme this provider implements.
	Scheme() SignatureScheme

	// NewBatchVerifier creates a new batch verifier.
	// The verifier uses the provided validator set for public key lookups.
	NewBatchVerifier(validators ValidatorSet) BatchVerifier

	// SupportsAggregation returns true if this scheme supports signature aggregation.
	// BLS supports aggregation, Ed25519 does not.
	SupportsAggregation() bool

	// Aggregator returns the BLS aggregator if aggregation is supported.
	// Returns nil for Ed25519.
	Aggregator() BLSAggregator
}

// ed25519BatchVerifier implements BatchVerifier for Ed25519 using parallel verification.
type ed25519BatchVerifier struct {
	validators ValidatorSet
	items      []verifyItem
	workers    int
}

type verifyItem struct {
	message        []byte
	signature      []byte
	validatorIndex uint16
}

// NewEd25519BatchVerifier creates a batch verifier for Ed25519.
// It parallelizes verification across multiple goroutines.
func NewEd25519BatchVerifier(validators ValidatorSet, workers int) BatchVerifier {
	if workers <= 0 {
		workers = 4 // default parallelism
	}
	return &ed25519BatchVerifier{
		validators: validators,
		items:      make([]verifyItem, 0, 16),
		workers:    workers,
	}
}

func (v *ed25519BatchVerifier) Add(message []byte, signature []byte, validatorIndex uint16) {
	v.items = append(v.items, verifyItem{
		message:        message,
		signature:      signature,
		validatorIndex: validatorIndex,
	})
}

func (v *ed25519BatchVerifier) Verify() error {
	if len(v.items) == 0 {
		return nil
	}

	// For small batches, verify sequentially
	if len(v.items) <= 2 {
		for _, item := range v.items {
			pubKey, err := v.validators.GetByIndex(item.validatorIndex)
			if err != nil {
				return err
			}
			if !pubKey.Verify(item.message, item.signature) {
				return &SignatureError{ValidatorIndex: item.validatorIndex}
			}
		}
		return nil
	}

	// For larger batches, verify in parallel
	type result struct {
		index uint16
		err   error
	}

	results := make(chan result, len(v.items))
	var wg sync.WaitGroup

	// Create work channel
	work := make(chan verifyItem, len(v.items))
	for _, item := range v.items {
		work <- item
	}
	close(work)

	// Start workers
	numWorkers := v.workers
	if numWorkers > len(v.items) {
		numWorkers = len(v.items)
	}

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range work {
				pubKey, err := v.validators.GetByIndex(item.validatorIndex)
				if err != nil {
					results <- result{index: item.validatorIndex, err: err}
					continue
				}
				if !pubKey.Verify(item.message, item.signature) {
					results <- result{index: item.validatorIndex, err: &SignatureError{ValidatorIndex: item.validatorIndex}}
				}
			}
		}()
	}

	// Wait for all workers and close results
	go func() {
		wg.Wait()
		close(results)
	}()

	// Check for any errors
	for res := range results {
		if res.err != nil {
			return res.err
		}
	}

	return nil
}

func (v *ed25519BatchVerifier) Reset() {
	v.items = v.items[:0]
}

// SignatureError indicates a signature verification failure.
type SignatureError struct {
	ValidatorIndex uint16
}

func (e *SignatureError) Error() string {
	return "invalid signature from validator"
}

// blsBatchVerifier implements BatchVerifier for BLS using the provided BLSBatchVerifier.
type blsBatchVerifier struct {
	validators ValidatorSet
	batcher    BLSBatchVerifier
	items      []verifyItem
}

// NewBLSBatchVerifier creates a batch verifier for BLS signatures.
// The batcher parameter should be provided by the integrating application.
func NewBLSBatchVerifier(validators ValidatorSet, batcher BLSBatchVerifier) BatchVerifier {
	return &blsBatchVerifier{
		validators: validators,
		batcher:    batcher,
		items:      make([]verifyItem, 0, 16),
	}
}

func (v *blsBatchVerifier) Add(message []byte, signature []byte, validatorIndex uint16) {
	v.items = append(v.items, verifyItem{
		message:        message,
		signature:      signature,
		validatorIndex: validatorIndex,
	})
}

func (v *blsBatchVerifier) Verify() error {
	if len(v.items) == 0 {
		return nil
	}

	// Add all signatures to the BLS batch verifier
	for _, item := range v.items {
		pubKey, err := v.validators.GetByIndex(item.validatorIndex)
		if err != nil {
			return err
		}
		v.batcher.AddSignature(pubKey.Bytes(), item.message, item.signature)
	}

	// Perform batch verification
	if !v.batcher.VerifyBatch() {
		// Batch failed - fall back to individual verification to find the bad signature
		v.batcher.Reset()
		for _, item := range v.items {
			pubKey, _ := v.validators.GetByIndex(item.validatorIndex)
			if !pubKey.Verify(item.message, item.signature) {
				return &SignatureError{ValidatorIndex: item.validatorIndex}
			}
		}
		// Shouldn't reach here, but return generic error
		return &SignatureError{ValidatorIndex: 0}
	}

	return nil
}

func (v *blsBatchVerifier) Reset() {
	v.items = v.items[:0]
	v.batcher.Reset()
}

// Ed25519CryptoProvider implements CryptoProvider for Ed25519.
type Ed25519CryptoProvider struct {
	workers int
}

// NewEd25519CryptoProvider creates a crypto provider for Ed25519.
func NewEd25519CryptoProvider(workers int) *Ed25519CryptoProvider {
	if workers <= 0 {
		workers = 4
	}
	return &Ed25519CryptoProvider{workers: workers}
}

func (p *Ed25519CryptoProvider) Scheme() SignatureScheme {
	return SignatureSchemeEd25519
}

func (p *Ed25519CryptoProvider) NewBatchVerifier(validators ValidatorSet) BatchVerifier {
	return NewEd25519BatchVerifier(validators, p.workers)
}

func (p *Ed25519CryptoProvider) SupportsAggregation() bool {
	return false
}

func (p *Ed25519CryptoProvider) Aggregator() BLSAggregator {
	return nil
}

// BLSCryptoProvider implements CryptoProvider for BLS.
type BLSCryptoProvider struct {
	batcherFactory func() BLSBatchVerifier
	aggregator     BLSAggregator
}

// NewBLSCryptoProvider creates a crypto provider for BLS.
// batcherFactory creates new BLSBatchVerifier instances.
// aggregator provides signature aggregation (optional, can be nil).
func NewBLSCryptoProvider(batcherFactory func() BLSBatchVerifier, aggregator BLSAggregator) *BLSCryptoProvider {
	return &BLSCryptoProvider{
		batcherFactory: batcherFactory,
		aggregator:     aggregator,
	}
}

func (p *BLSCryptoProvider) Scheme() SignatureScheme {
	return SignatureSchemeBLS
}

func (p *BLSCryptoProvider) NewBatchVerifier(validators ValidatorSet) BatchVerifier {
	return NewBLSBatchVerifier(validators, p.batcherFactory())
}

func (p *BLSCryptoProvider) SupportsAggregation() bool {
	return p.aggregator != nil
}

func (p *BLSCryptoProvider) Aggregator() BLSAggregator {
	return p.aggregator
}

// VerifyCertificateSignatures verifies all signatures in a certificate.
// Uses the provided CryptoProvider for optimized verification.
func VerifyCertificateSignatures[H Hash](
	cert *Certificate[H],
	validators ValidatorSet,
	crypto CryptoProvider,
) error {
	verifier := crypto.NewBatchVerifier(validators)
	defer verifier.Reset()

	message := cert.Header.Digest.Bytes()
	sigIdx := 0

	for i := 0; i < validators.Count(); i++ {
		if cert.SignerBitmap&(1<<uint(i)) != 0 {
			if sigIdx >= len(cert.Signatures) {
				return &SignatureError{ValidatorIndex: uint16(i)}
			}
			verifier.Add(message, cert.Signatures[sigIdx], uint16(i))
			sigIdx++
		}
	}

	return verifier.Verify()
}

// SignatureCache caches verified signatures to avoid re-verification.
// Thread-safe for concurrent access.
type SignatureCache struct {
	mu      sync.RWMutex
	cache   map[string]bool // digest+validatorIndex -> verified
	maxSize int
}

// NewSignatureCache creates a new signature cache.
func NewSignatureCache(maxSize int) *SignatureCache {
	if maxSize <= 0 {
		maxSize = 10000
	}
	return &SignatureCache{
		cache:   make(map[string]bool, maxSize),
		maxSize: maxSize,
	}
}

// cacheKey creates a cache key from digest and validator index.
func cacheKey(digest []byte, validatorIndex uint16) string {
	// Simple key: first 16 bytes of digest + validator index
	key := make([]byte, 18)
	copy(key, digest[:min(16, len(digest))])
	key[16] = byte(validatorIndex >> 8)
	key[17] = byte(validatorIndex)
	return string(key)
}

// IsVerified checks if a signature has been verified.
func (c *SignatureCache) IsVerified(digest []byte, validatorIndex uint16) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cache[cacheKey(digest, validatorIndex)]
}

// MarkVerified marks a signature as verified.
func (c *SignatureCache) MarkVerified(digest []byte, validatorIndex uint16) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Simple eviction: clear half when full
	if len(c.cache) >= c.maxSize {
		count := 0
		for k := range c.cache {
			delete(c.cache, k)
			count++
			if count >= c.maxSize/2 {
				break
			}
		}
	}

	c.cache[cacheKey(digest, validatorIndex)] = true
}

// Clear removes all cached entries.
func (c *SignatureCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache = make(map[string]bool, c.maxSize)
}

// Size returns the number of cached entries.
func (c *SignatureCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache)
}

// CachedBatchVerifier wraps a BatchVerifier with signature caching.
type CachedBatchVerifier struct {
	inner      BatchVerifier
	cache      *SignatureCache
	validators ValidatorSet
	items      []cachedVerifyItem
}

type cachedVerifyItem struct {
	message        []byte
	signature      []byte
	validatorIndex uint16
	cached         bool
}

// NewCachedBatchVerifier wraps a batch verifier with caching.
func NewCachedBatchVerifier(inner BatchVerifier, cache *SignatureCache, validators ValidatorSet) *CachedBatchVerifier {
	return &CachedBatchVerifier{
		inner:      inner,
		cache:      cache,
		validators: validators,
		items:      make([]cachedVerifyItem, 0, 16),
	}
}

func (v *CachedBatchVerifier) Add(message []byte, signature []byte, validatorIndex uint16) {
	cached := v.cache.IsVerified(message, validatorIndex)
	v.items = append(v.items, cachedVerifyItem{
		message:        message,
		signature:      signature,
		validatorIndex: validatorIndex,
		cached:         cached,
	})

	// Only add uncached items to inner verifier
	if !cached {
		v.inner.Add(message, signature, validatorIndex)
	}
}

func (v *CachedBatchVerifier) Verify() error {
	// Verify uncached items
	if err := v.inner.Verify(); err != nil {
		return err
	}

	// Cache newly verified signatures
	for _, item := range v.items {
		if !item.cached {
			v.cache.MarkVerified(item.message, item.validatorIndex)
		}
	}

	return nil
}

func (v *CachedBatchVerifier) Reset() {
	v.items = v.items[:0]
	v.inner.Reset()
}
