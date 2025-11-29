package narwhal

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Narwhal implements the Narwhal DAG-based mempool protocol.
type Narwhal[H Hash, T Transaction[H]] struct {
	cfg            *Config[H, T]
	dag            *DAG[H, T]
	workers        []*Worker[H, T]
	primary        *Primary[H, T]
	headerWaiter   *HeaderWaiter[H, T]
	requestTracker *RequestTracker[H]
	validator      *Validator[H, T]
	hashFunc       func([]byte) H
	hooks          *Hooks[H, T]

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	logger *zap.Logger
}

// New creates a new Narwhal instance.
// The hashFunc parameter is required to compute digests for batches, headers, etc.
func New[H Hash, T Transaction[H]](cfg *Config[H, T], hashFunc func([]byte) H) (*Narwhal[H, T], error) {
	if hashFunc == nil {
		return nil, fmt.Errorf("hashFunc is required")
	}

	ctx, cancel := context.WithCancel(context.Background())

	n := &Narwhal[H, T]{
		cfg:      cfg,
		hashFunc: hashFunc,
		hooks:    cfg.Hooks,
		ctx:      ctx,
		cancel:   cancel,
		logger:   cfg.Logger,
	}

	// Initialize RequestTracker for cancelling stale network operations
	n.requestTracker = NewRequestTracker[H](RequestTrackerConfig{
		MaxPendingPerRound:  1000,
		StaleRoundThreshold: 2,
		CancelOnAdvance:     true,
	}, cfg.Logger)

	// Wrap hooks to include RequestTracker round-advance notification
	wrappedHooks := cfg.Hooks
	if wrappedHooks == nil {
		wrappedHooks = &Hooks[H, T]{}
	} else {
		wrappedHooks = wrappedHooks.Clone()
	}
	originalOnRoundAdvanced := wrappedHooks.OnRoundAdvanced
	wrappedHooks.OnRoundAdvanced = func(e RoundAdvancedEvent) {
		// Notify RequestTracker to cancel stale requests
		n.requestTracker.OnRoundAdvance(e.NewRound)
		// Call original hook if set
		if originalOnRoundAdvanced != nil {
			originalOnRoundAdvanced(e)
		}
	}

	// Initialize DAG with wrapped hooks, crypto provider, and optional cache
	n.dag = NewDAGWithCache[H, T](cfg.Validators, wrappedHooks, cfg.CryptoProvider, cfg.SignatureCache, &cfg.DAGCache, cfg.Logger)

	// Initialize Validator for input validation
	n.validator = NewValidator[H, T](cfg.Validation, cfg.Validators, hashFunc)

	// Initialize workers
	n.workers = make([]*Worker[H, T], cfg.WorkerCount)
	for i := 0; i < cfg.WorkerCount; i++ {
		n.workers[i] = NewWorker(WorkerConfig[H, T]{
			ID:           uint16(i),
			ValidatorID:  cfg.MyIndex,
			BatchSize:    cfg.BatchSize,
			BatchTimeout: cfg.BatchTimeout,
			HashFunc:     hashFunc,
			OnBatch:      n.onWorkerBatch,
			Hooks:        cfg.Hooks,
			Logger:       cfg.Logger.With(zap.Int("worker", i)),
			MaxPending:   cfg.MaxPendingTransactions,
			DropOnFull:   cfg.DropOnFull,
		})
	}

	// Initialize primary
	maxHeaderDelay := cfg.MaxHeaderDelay
	if maxHeaderDelay == 0 {
		maxHeaderDelay = cfg.HeaderTimeout
	}
	n.primary = NewPrimary(PrimaryConfig[H, T]{
		ValidatorID:       cfg.MyIndex,
		DAG:               n.dag,
		Validators:        cfg.Validators,
		Signer:            cfg.Signer,
		Network:           cfg.Network,
		Storage:           cfg.Storage,
		HeaderTimeout:     cfg.HeaderTimeout,
		HashFunc:          hashFunc,
		Hooks:             cfg.Hooks,
		Logger:            cfg.Logger,
		MaxHeaderBatches:  cfg.MaxHeaderBatches,
		MaxHeaderDelay:    maxHeaderDelay,
		MaxPendingBatches: cfg.MaxPendingBatches,
		DropOnFull:        cfg.DropOnFull,
		CryptoProvider:    cfg.CryptoProvider,
		SignatureCache:    cfg.SignatureCache,
	})

	// Initialize HeaderWaiter for queuing headers with missing parents
	n.headerWaiter = NewHeaderWaiter[H, T](
		HeaderWaiterConfig{
			MaxPendingHeaders: cfg.MaxPendingHeaders,
			RetryInterval:     cfg.HeaderWaiterRetryInterval,
			MaxRetries:        cfg.HeaderWaiterMaxRetries,
			MaxAge:            cfg.HeaderWaiterMaxAge,
			FetchParents:      cfg.FetchMissingParents,
		},
		func(header *Header[H], from uint16) error {
			return n.primary.OnHeaderReceived(header, from)
		},
		func(digest H) bool {
			return n.dag.IsCertified(digest)
		},
		cfg.Hooks,
		cfg.Logger,
	)

	// Set up proactive parent fetching if enabled
	if cfg.FetchMissingParents {
		n.headerWaiter.SetFetchParentFunc(func(digest H, from uint16) error {
			// Track this certificate fetch for cancellation on round advance
			// Use round 0 for parent fetches since we don't know the exact round
			trackedCtx, complete := n.requestTracker.Track(n.ctx, 0, RequestTypeCertificate, digest)
			defer complete()

			cert, err := cfg.Network.FetchCertificate(from, digest)
			if err != nil {
				return err
			}
			// Check if cancelled
			if trackedCtx.Err() != nil {
				return trackedCtx.Err()
			}
			// Validate the fetched certificate using DAG's crypto provider
			if err := n.dag.ValidateCertificate(cert); err != nil {
				return err
			}
			// Fetch batches for this certificate (use tracked context)
			batches, err := n.fetchBatchesForHeaderWithContext(trackedCtx, cert.Header)
			if err != nil {
				return err
			}
			// Store and insert into DAG
			if err := cfg.Storage.PutCertificate(cert); err != nil {
				n.logger.Warn("failed to store fetched certificate", zap.Error(err))
			}
			return n.dag.InsertCertificate(cert, batches)
		})
	}

	return n, nil
}

// Start starts the Narwhal protocol.
func (n *Narwhal[H, T]) Start() error {
	n.logger.Info("starting Narwhal",
		zap.Uint16("validator", n.cfg.MyIndex),
		zap.Int("workers", len(n.workers)))

	// Restore DAG state from storage (re-sync on startup)
	if err := n.restoreFromStorage(); err != nil {
		n.logger.Warn("failed to restore from storage, starting fresh",
			zap.Error(err))
	}

	// Start workers
	for _, w := range n.workers {
		n.wg.Add(1)
		go func(wrk *Worker[H, T]) {
			defer n.wg.Done()
			wrk.Run(n.ctx)
		}(w)
	}

	// Start primary
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.primary.Run(n.ctx)
	}()

	// Start header waiter
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.headerWaiter.Run(n.ctx)
	}()

	// Start message handler
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.messageLoop()
	}()

	return nil
}

// restoreFromStorage restores DAG state from persistent storage on startup.
// This allows the node to resume from where it left off after a restart.
func (n *Narwhal[H, T]) restoreFromStorage() error {
	// Get the highest round from storage
	highestRound, err := n.cfg.Storage.GetHighestRound()
	if err != nil {
		return fmt.Errorf("failed to get highest round: %w", err)
	}

	if highestRound == 0 {
		n.logger.Debug("no stored rounds found, starting fresh")
		return nil
	}

	n.logger.Info("restoring DAG from storage",
		zap.Uint64("highest_round", highestRound))

	// Calculate the range of rounds to restore
	// We keep GCDepth rounds of history, so restore from (highestRound - GCDepth) to highestRound
	var startRound uint64
	if highestRound > n.cfg.GCDepth {
		startRound = highestRound - n.cfg.GCDepth
	}

	// Get all certificates in the range
	certs, err := n.cfg.Storage.GetCertificatesInRange(startRound, highestRound)
	if err != nil {
		return fmt.Errorf("failed to get certificates: %w", err)
	}

	if len(certs) == 0 {
		n.logger.Debug("no certificates found in range",
			zap.Uint64("start_round", startRound),
			zap.Uint64("end_round", highestRound))
		return nil
	}

	n.logger.Info("restoring certificates",
		zap.Int("count", len(certs)),
		zap.Uint64("start_round", startRound),
		zap.Uint64("end_round", highestRound))

	// Insert certificates into DAG (they're already sorted by round)
	var restored, failed int
	for _, cert := range certs {
		// Fetch batches for this certificate
		batches, err := n.fetchBatchesFromStorage(cert.Header)
		if err != nil {
			n.logger.Warn("failed to fetch batches for restored certificate",
				zap.String("digest", cert.Header.Digest.String()),
				zap.Error(err))
			failed++
			continue
		}

		// Insert into DAG (skip validation since these are from our own storage)
		if err := n.dag.InsertCertificate(cert, batches); err != nil {
			// May fail if already inserted or parents missing - that's OK
			n.logger.Debug("failed to insert restored certificate",
				zap.String("digest", cert.Header.Digest.String()),
				zap.Error(err))
			failed++
			continue
		}
		restored++
	}

	n.logger.Info("DAG restoration complete",
		zap.Int("restored", restored),
		zap.Int("failed", failed),
		zap.Uint64("current_round", n.dag.CurrentRound()))

	return nil
}

// fetchBatchesFromStorage fetches batches referenced by a header from storage.
func (n *Narwhal[H, T]) fetchBatchesFromStorage(header *Header[H]) ([]*Batch[H, T], error) {
	batches := make([]*Batch[H, T], 0, len(header.BatchRefs))
	for _, ref := range header.BatchRefs {
		batch, err := n.cfg.Storage.GetBatch(ref)
		if err != nil {
			// Batch might be missing if it was garbage collected
			continue
		}
		batches = append(batches, batch)
	}
	return batches, nil
}

// Stop stops the Narwhal protocol gracefully.
func (n *Narwhal[H, T]) Stop() {
	n.logger.Info("stopping Narwhal")
	n.cancel()
	n.wg.Wait()
	n.logger.Info("Narwhal stopped")
}

// AddTransaction adds a transaction to be disseminated.
// Transactions are routed to workers based on their hash for load balancing.
func (n *Narwhal[H, T]) AddTransaction(tx T) {
	// Deterministic routing for load balancing and deduplication
	hashBytes := tx.Hash().Bytes()
	workerIdx := int(hashBytes[0]) % len(n.workers)
	n.workers[workerIdx].AddTransaction(tx)
}

// DAG returns the underlying DAG for direct access.
func (n *Narwhal[H, T]) GetDAG() *DAG[H, T] {
	return n.dag
}

// GetCertifiedVertices returns certificates ready for consensus ordering.
// These are certificates that have not yet been included in a committed block.
func (n *Narwhal[H, T]) GetCertifiedVertices() []*Certificate[H] {
	return n.dag.GetUncommitted()
}

// MarkCommitted marks certificates as committed by consensus.
// This allows garbage collection of the underlying data.
func (n *Narwhal[H, T]) MarkCommitted(certs []*Certificate[H]) {
	n.dag.MarkCommitted(certs)
}

// GetTransactions extracts all transactions from the given certificates.
// Used by execution layer to get ordered transactions from consensus block.
// Transactions are deduplicated by hash.
func (n *Narwhal[H, T]) GetTransactions(certs []*Certificate[H]) ([]T, error) {
	return n.dag.GetTransactions(certs)
}

// CurrentRound returns the current DAG round.
func (n *Narwhal[H, T]) CurrentRound() uint64 {
	return n.dag.CurrentRound()
}

// RequestTrackerStats returns statistics about pending network operations.
func (n *Narwhal[H, T]) RequestTrackerStats() RequestTrackerStats {
	return n.requestTracker.Stats()
}

// onWorkerBatch is called when a worker creates a batch.
func (n *Narwhal[H, T]) onWorkerBatch(batch *Batch[H, T]) {
	// Store batch
	if err := n.cfg.Storage.PutBatch(batch); err != nil {
		n.logger.Error("failed to store batch", zap.Error(err))
	}

	// Broadcast to other validators
	n.cfg.Network.BroadcastBatch(batch)

	// Notify primary
	n.primary.OnBatch(batch)
}

// messageLoop processes incoming network messages.
func (n *Narwhal[H, T]) messageLoop() {
	for {
		select {
		case <-n.ctx.Done():
			return
		case msg := <-n.cfg.Network.Receive():
			n.handleMessage(msg)
		}
	}
}

func (n *Narwhal[H, T]) handleMessage(msg Message[H, T]) {
	switch msg.Type() {
	case MessageBatch:
		if batchMsg, ok := msg.(BatchMessageAccessor[H, T]); ok {
			batch := batchMsg.Batch()
			// Validate batch before processing
			if err := n.validator.ValidateBatch(batch, false); err != nil {
				n.logger.Warn("invalid batch received",
					zap.Uint16("from", msg.Sender()),
					zap.Error(err))
				return
			}
			n.onBatchReceived(batch, msg.Sender())
		}

	case MessageHeader:
		if headerMsg, ok := msg.(HeaderMessageAccessor[H]); ok {
			header := headerMsg.Header()
			// Validate header before processing (don't verify signature here, done in vote handler)
			currentRound := n.dag.CurrentRound()
			if err := n.validator.ValidateHeader(header, currentRound); err != nil {
				n.logger.Warn("invalid header received",
					zap.Uint16("from", msg.Sender()),
					zap.Error(err))
				return
			}
			// Invoke hook
			if n.hooks != nil && n.hooks.OnHeaderReceived != nil {
				n.hooks.OnHeaderReceived(HeaderReceivedEvent[H]{
					Header:     header,
					From:       msg.Sender(),
					ReceivedAt: time.Now(),
				})
			}
			n.handleHeaderReceived(header, msg.Sender())
		}

	case MessageVote:
		if voteMsg, ok := msg.(VoteMessageAccessor[H]); ok {
			vote := voteMsg.Vote()
			// Validate vote before processing (verify signature in primary)
			if err := n.validator.ValidateVote(vote, false); err != nil {
				n.logger.Warn("invalid vote received",
					zap.Uint16("from", msg.Sender()),
					zap.Error(err))
				return
			}
			if err := n.primary.OnVoteReceived(vote); err != nil {
				n.logger.Warn("failed to process vote",
					zap.Uint16("from", msg.Sender()),
					zap.Error(err))
			}
		}

	case MessageCertificate:
		if certMsg, ok := msg.(CertificateMessageAccessor[H]); ok {
			cert := certMsg.Certificate()
			// Validate certificate before processing (signature verification is done in onCertificateReceived)
			currentRound := n.dag.CurrentRound()
			if err := n.validator.ValidateCertificate(cert, currentRound, false); err != nil {
				n.logger.Warn("invalid certificate received",
					zap.Uint16("from", msg.Sender()),
					zap.Error(err))
				return
			}
			// Invoke hook
			if n.hooks != nil && n.hooks.OnCertificateReceived != nil {
				n.hooks.OnCertificateReceived(CertificateReceivedEvent[H]{
					Certificate: cert,
					From:        msg.Sender(),
					ReceivedAt:  time.Now(),
				})
			}
			n.onCertificateReceived(cert)
		}

	case MessageBatchRequest:
		reqMsg := msg.(*BatchRequestMessage[H, T])
		n.handleBatchRequest(reqMsg.Digest, msg.Sender())

	case MessageCertificateRequest:
		reqMsg := msg.(*CertificateRequestMessage[H, T])
		n.handleCertificateRequest(reqMsg.Digest, msg.Sender())
	}
}

// handleHeaderReceived processes an incoming header, queueing it if parents are missing.
func (n *Narwhal[H, T]) handleHeaderReceived(header *Header[H], from uint16) {
	// Check for missing parent certificates
	var missingParents []H
	for _, parent := range header.Parents {
		if !n.dag.IsCertified(parent) {
			missingParents = append(missingParents, parent)
		}
	}

	if len(missingParents) > 0 {
		// Queue for later processing
		n.headerWaiter.Add(header, from, missingParents)
		n.logger.Debug("queued header with missing parents",
			zap.String("header", header.Digest.String()),
			zap.Int("missing", len(missingParents)))
		return
	}

	// All parents available, process immediately
	if err := n.primary.OnHeaderReceived(header, from); err != nil {
		n.logger.Warn("failed to process header",
			zap.Uint16("from", from),
			zap.Error(err))
	}
}

func (n *Narwhal[H, T]) onBatchReceived(batch *Batch[H, T], from uint16) {
	// Note: Batch has already been validated by n.validator.ValidateBatch in handleMessage
	// Store batch
	if err := n.cfg.Storage.PutBatch(batch); err != nil {
		n.logger.Warn("failed to store received batch", zap.Error(err))
	}
}

func (n *Narwhal[H, T]) onCertificateReceived(cert *Certificate[H]) {
	// Validate certificate using DAG's crypto provider (if configured)
	if err := n.dag.ValidateCertificate(cert); err != nil {
		n.logger.Warn("invalid certificate received", zap.Error(err))
		return
	}

	// Fetch missing batches
	batches, err := n.fetchBatchesForHeader(cert.Header)
	if err != nil {
		n.logger.Warn("failed to fetch batches for certificate",
			zap.String("digest", cert.Header.Digest.String()),
			zap.Error(err))
		return
	}

	// Store certificate
	if err := n.cfg.Storage.PutCertificate(cert); err != nil {
		n.logger.Warn("failed to store certificate", zap.Error(err))
	}

	// Insert into DAG (already validated, no need to validate again)
	if err := n.dag.InsertCertificate(cert, batches); err != nil {
		n.logger.Warn("failed to insert certificate into DAG",
			zap.String("digest", cert.Header.Digest.String()),
			zap.Error(err))
		return
	}

	// Notify HeaderWaiter that this certificate is now available
	// (may unblock pending headers waiting for this parent)
	n.headerWaiter.OnParentAvailable(cert.Header.Digest)
}

func (n *Narwhal[H, T]) fetchBatchesForHeader(header *Header[H]) ([]*Batch[H, T], error) {
	return n.fetchBatchesForHeaderWithContext(n.ctx, header)
}

func (n *Narwhal[H, T]) fetchBatchesForHeaderWithContext(ctx context.Context, header *Header[H]) ([]*Batch[H, T], error) {
	batches := make([]*Batch[H, T], 0, len(header.BatchRefs))

	for _, ref := range header.BatchRefs {
		// Check context before each fetch
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Check local storage first
		batch, err := n.cfg.Storage.GetBatch(ref)
		if err == nil {
			batches = append(batches, batch)
			continue
		}

		// Track this fetch request for cancellation on round advance
		trackedCtx, complete := n.requestTracker.Track(ctx, header.Round, RequestTypeBatch, ref)
		defer complete()

		// Fetch from the header author
		batch, err = n.cfg.Network.FetchBatch(header.Author, ref)
		complete() // Mark complete early to avoid defer stacking

		// Check if cancelled
		if trackedCtx.Err() != nil {
			return nil, trackedCtx.Err()
		}
		if err != nil {
			return nil, fmt.Errorf("failed to fetch batch %s: %w", ref.String(), err)
		}

		// Verify and store
		if err := batch.Verify(n.hashFunc); err != nil {
			return nil, fmt.Errorf("fetched batch %s is invalid: %w", ref.String(), err)
		}
		if err := n.cfg.Storage.PutBatch(batch); err != nil {
			n.logger.Warn("failed to store fetched batch", zap.Error(err))
		}

		batches = append(batches, batch)
	}

	return batches, nil
}

func (n *Narwhal[H, T]) handleBatchRequest(digest H, from uint16) {
	batch, err := n.cfg.Storage.GetBatch(digest)
	if err != nil {
		return // Don't have it
	}
	n.cfg.Network.SendBatch(from, batch)
}

func (n *Narwhal[H, T]) handleCertificateRequest(digest H, from uint16) {
	cert, err := n.cfg.Storage.GetCertificate(digest)
	if err != nil {
		return // Don't have it
	}
	n.cfg.Network.SendCertificate(from, cert)
}

// Worker receives transactions and creates batches.
type Worker[H Hash, T Transaction[H]] struct {
	mu sync.Mutex

	cfg     WorkerConfig[H, T]
	pending []T
	round   uint64
	hooks   *Hooks[H, T]

	// Bounded transaction queue (if MaxPending > 0)
	txChan     chan T
	maxPending int
	dropOnFull bool

	// Stats for monitoring
	droppedTx uint64

	logger *zap.Logger
}

// WorkerConfig configures a worker.
type WorkerConfig[H Hash, T Transaction[H]] struct {
	ID           uint16
	ValidatorID  uint16
	BatchSize    int
	BatchTimeout time.Duration
	HashFunc     func([]byte) H
	OnBatch      func(*Batch[H, T])
	Hooks        *Hooks[H, T]
	Logger       *zap.Logger

	// Backpressure settings
	MaxPending int  // Max pending transactions (0 = unbounded)
	DropOnFull bool // If true, drop when full; if false, block
}

// NewWorker creates a new worker.
func NewWorker[H Hash, T Transaction[H]](cfg WorkerConfig[H, T]) *Worker[H, T] {
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	w := &Worker[H, T]{
		cfg:        cfg,
		pending:    make([]T, 0, cfg.BatchSize),
		hooks:      cfg.Hooks,
		maxPending: cfg.MaxPending,
		dropOnFull: cfg.DropOnFull,
		logger:     logger.With(zap.Uint16("worker_id", cfg.ID)),
	}

	// Create bounded channel if max pending is set
	if cfg.MaxPending > 0 {
		w.txChan = make(chan T, cfg.MaxPending)
	}

	return w
}

// AddTransaction adds a transaction to the pending batch.
// If MaxPending is configured and the queue is full:
//   - If DropOnFull is true, the transaction is dropped silently
//   - If DropOnFull is false, this call blocks until space is available
//
// Returns true if the transaction was accepted, false if dropped.
func (w *Worker[H, T]) AddTransaction(tx T) bool {
	// Invoke hook (before potential drop)
	if w.hooks != nil && w.hooks.OnTransactionReceived != nil {
		w.hooks.OnTransactionReceived(TransactionReceivedEvent[H]{
			TxHash:     tx.Hash(),
			WorkerID:   w.cfg.ID,
			ReceivedAt: time.Now(),
		})
	}

	// If using bounded channel, send to channel instead of direct append
	if w.txChan != nil {
		if w.dropOnFull {
			// Non-blocking send
			select {
			case w.txChan <- tx:
				return true
			default:
				// Queue full, drop transaction
				w.mu.Lock()
				w.droppedTx++
				w.mu.Unlock()
				w.logger.Debug("transaction dropped, queue full",
					zap.String("tx_hash", tx.Hash().String()))
				return false
			}
		} else {
			// Blocking send
			w.txChan <- tx
			return true
		}
	}

	// Unbounded mode - direct append
	w.mu.Lock()
	defer w.mu.Unlock()

	w.pending = append(w.pending, tx)

	if len(w.pending) >= w.cfg.BatchSize {
		w.createBatch()
	}
	return true
}

// Run starts the worker's batch creation loop.
func (w *Worker[H, T]) Run(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.BatchTimeout)
	defer ticker.Stop()

	// If using bounded channel, drain transactions from it
	if w.txChan != nil {
		for {
			select {
			case <-ctx.Done():
				w.drainAndFlush()
				return
			case tx := <-w.txChan:
				w.mu.Lock()
				w.pending = append(w.pending, tx)
				if len(w.pending) >= w.cfg.BatchSize {
					w.createBatch()
				}
				w.mu.Unlock()
			case <-ticker.C:
				w.mu.Lock()
				if len(w.pending) > 0 {
					w.createBatch()
				}
				w.mu.Unlock()
			}
		}
	}

	// Unbounded mode - just use ticker
	for {
		select {
		case <-ctx.Done():
			w.mu.Lock()
			if len(w.pending) > 0 {
				w.createBatch()
			}
			w.mu.Unlock()
			return
		case <-ticker.C:
			w.mu.Lock()
			if len(w.pending) > 0 {
				w.createBatch()
			}
			w.mu.Unlock()
		}
	}
}

// drainAndFlush drains remaining transactions from the channel and creates final batch.
func (w *Worker[H, T]) drainAndFlush() {
	// Drain remaining transactions from channel
	for {
		select {
		case tx := <-w.txChan:
			w.mu.Lock()
			w.pending = append(w.pending, tx)
			w.mu.Unlock()
		default:
			// Channel empty
			w.mu.Lock()
			if len(w.pending) > 0 {
				w.createBatch()
			}
			w.mu.Unlock()
			return
		}
	}
}

func (w *Worker[H, T]) createBatch() {
	if len(w.pending) == 0 {
		return
	}

	now := time.Now()
	batch := &Batch[H, T]{
		WorkerID:     w.cfg.ID,
		ValidatorID:  w.cfg.ValidatorID,
		Round:        w.round,
		Transactions: w.pending,
	}
	batch.ComputeDigest(w.cfg.HashFunc)

	w.logger.Debug("created batch",
		zap.Uint64("round", batch.Round),
		zap.Int("tx_count", len(batch.Transactions)),
		zap.String("digest", batch.Digest.String()))

	// Invoke hook
	if w.hooks != nil && w.hooks.OnBatchCreated != nil {
		sizeBytes := 0
		for _, tx := range batch.Transactions {
			sizeBytes += len(tx.Bytes())
		}
		w.hooks.OnBatchCreated(BatchCreatedEvent[H, T]{
			Batch:            batch,
			WorkerID:         w.cfg.ID,
			TransactionCount: len(batch.Transactions),
			SizeBytes:        sizeBytes,
			CreatedAt:        now,
		})
	}

	if w.cfg.OnBatch != nil {
		w.cfg.OnBatch(batch)
	}

	w.pending = make([]T, 0, w.cfg.BatchSize)
}

// SetRound updates the worker's current round.
func (w *Worker[H, T]) SetRound(round uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.round = round
}

// WorkerStats contains worker statistics for monitoring.
type WorkerStats struct {
	PendingCount int
	QueuedCount  int // Transactions in channel (if bounded)
	DroppedCount uint64
	MaxPending   int
	IsBounded    bool
}

// Stats returns current worker statistics.
func (w *Worker[H, T]) Stats() WorkerStats {
	w.mu.Lock()
	defer w.mu.Unlock()

	stats := WorkerStats{
		PendingCount: len(w.pending),
		DroppedCount: w.droppedTx,
		MaxPending:   w.maxPending,
		IsBounded:    w.txChan != nil,
	}

	if w.txChan != nil {
		stats.QueuedCount = len(w.txChan)
	}

	return stats
}

// Primary creates headers and collects votes to form certificates.
type Primary[H Hash, T Transaction[H]] struct {
	mu sync.Mutex

	cfg PrimaryConfig[H, T]

	pendingBatches   []H
	pendingBatchData []*Batch[H, T]
	currentHeader    *Header[H]
	votes            map[uint16][]byte

	// Header timeout tracking
	headerCreatedAt   time.Time
	headerRetries     int
	lastBatchReceived time.Time // Track when we last received a batch

	// Bounded batch channel (if MaxPending > 0)
	batchChan  chan *Batch[H, T]
	maxPending int
	dropOnFull bool

	// Stats for monitoring
	droppedBatches uint64

	// Optimized crypto verification
	cryptoProvider CryptoProvider
	sigCache       *SignatureCache

	// Vote idempotence tracking (prevents double-voting on equivocating headers)
	voteTracker *VoteTracker[H]

	hooks  *Hooks[H, T]
	logger *zap.Logger
}

// PrimaryConfig configures a primary.
type PrimaryConfig[H Hash, T Transaction[H]] struct {
	ValidatorID   uint16
	DAG           *DAG[H, T]
	Validators    ValidatorSet
	Signer        Signer
	Network       Network[H, T]
	Storage       Storage[H, T]
	HeaderTimeout time.Duration
	HashFunc      func([]byte) H
	Hooks         *Hooks[H, T]
	Logger        *zap.Logger

	// Retry configuration
	MaxHeaderRetries    int           // Max retries before abandoning a header (default: 3)
	HeaderRetryInterval time.Duration // Time before retrying header broadcast (default: HeaderTimeout)

	// Header creation thresholds (aligned with reference implementation)
	MaxHeaderBatches int           // Max batch refs per header (default: 100)
	MaxHeaderDelay   time.Duration // Max delay before creating header (default: HeaderTimeout)

	// Backpressure settings
	MaxPendingBatches int  // Max pending batches (0 = unbounded)
	DropOnFull        bool // If true, drop when full; if false, block

	// Optimized crypto verification (optional)
	CryptoProvider CryptoProvider
	SignatureCache *SignatureCache
}

// NewPrimary creates a new primary.
func NewPrimary[H Hash, T Transaction[H]](cfg PrimaryConfig[H, T]) *Primary[H, T] {
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	// Set defaults for retry config
	if cfg.MaxHeaderRetries == 0 {
		cfg.MaxHeaderRetries = 3
	}
	if cfg.HeaderRetryInterval == 0 {
		cfg.HeaderRetryInterval = cfg.HeaderTimeout
	}

	// Set defaults for header creation thresholds
	if cfg.MaxHeaderBatches == 0 {
		cfg.MaxHeaderBatches = 100
	}
	if cfg.MaxHeaderDelay == 0 {
		cfg.MaxHeaderDelay = cfg.HeaderTimeout
	}

	p := &Primary[H, T]{
		cfg:            cfg,
		pendingBatches: make([]H, 0),
		votes:          make(map[uint16][]byte),
		maxPending:     cfg.MaxPendingBatches,
		dropOnFull:     cfg.DropOnFull,
		cryptoProvider: cfg.CryptoProvider,
		sigCache:       cfg.SignatureCache,
		voteTracker:    NewVoteTracker[H](logger),
		hooks:          cfg.Hooks,
		logger:         logger.With(zap.Uint16("validator_id", cfg.ValidatorID)),
	}

	// Create bounded channel if max pending is set
	if cfg.MaxPendingBatches > 0 {
		p.batchChan = make(chan *Batch[H, T], cfg.MaxPendingBatches)
	}

	return p
}

// Run starts the primary's header creation loop.
func (p *Primary[H, T]) Run(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.HeaderTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.tick()
		case batch := <-p.batchChanOrNil():
			if batch != nil {
				p.processBatch(batch)
			}
		}
	}
}

// batchChanOrNil returns the batch channel if bounded mode is enabled, nil otherwise.
func (p *Primary[H, T]) batchChanOrNil() <-chan *Batch[H, T] {
	return p.batchChan
}

// processBatch handles a batch received from the channel.
func (p *Primary[H, T]) processBatch(batch *Batch[H, T]) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.pendingBatches = append(p.pendingBatches, batch.Digest)
	p.pendingBatchData = append(p.pendingBatchData, batch)
	p.lastBatchReceived = time.Now()

	// Check if we should create a header immediately (batch threshold reached)
	if len(p.pendingBatches) >= p.cfg.MaxHeaderBatches && p.currentHeader == nil {
		p.tryCreateHeaderLocked()
	}
}

// tick is called periodically to manage header creation and timeouts.
func (p *Primary[H, T]) tick() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// If we have a pending header, check for timeout
	if p.currentHeader != nil {
		elapsed := time.Since(p.headerCreatedAt)
		if elapsed >= p.cfg.HeaderRetryInterval {
			if p.headerRetries < p.cfg.MaxHeaderRetries {
				// Retry: re-broadcast the header
				p.headerRetries++
				p.logger.Info("retrying header broadcast",
					zap.Uint64("round", p.currentHeader.Round),
					zap.Int("attempt", p.headerRetries+1),
					zap.Int("votes", len(p.votes)))

				// Invoke timeout hook (will retry)
				if p.hooks != nil && p.hooks.OnHeaderTimeout != nil {
					p.hooks.OnHeaderTimeout(HeaderTimeoutEvent[H]{
						HeaderDigest:  p.currentHeader.Digest,
						Round:         p.currentHeader.Round,
						RetryCount:    p.headerRetries,
						WillRetry:     true,
						VotesReceived: len(p.votes),
						QuorumNeeded:  2*p.cfg.Validators.F() + 1,
						TimeoutAt:     time.Now(),
					})
				}

				p.cfg.Network.BroadcastHeader(p.currentHeader)
				p.headerCreatedAt = time.Now()
			} else {
				// Invoke timeout hook (abandoning)
				if p.hooks != nil && p.hooks.OnHeaderTimeout != nil {
					p.hooks.OnHeaderTimeout(HeaderTimeoutEvent[H]{
						HeaderDigest:  p.currentHeader.Digest,
						Round:         p.currentHeader.Round,
						RetryCount:    p.headerRetries,
						WillRetry:     false,
						VotesReceived: len(p.votes),
						QuorumNeeded:  2*p.cfg.Validators.F() + 1,
						TimeoutAt:     time.Now(),
					})
				}

				// Max retries exceeded - abandon this header
				p.logger.Warn("abandoning header after max retries",
					zap.Uint64("round", p.currentHeader.Round),
					zap.Int("votes", len(p.votes)))
				p.abandonCurrentHeader()
			}
		}
		return
	}

	// Check if MaxHeaderDelay has elapsed since last batch was received
	if len(p.pendingBatches) > 0 && !p.lastBatchReceived.IsZero() {
		if time.Since(p.lastBatchReceived) >= p.cfg.MaxHeaderDelay {
			p.tryCreateHeaderLocked()
			return
		}
	}

	// Try to create a new header (standard timeout path)
	p.tryCreateHeaderLocked()
}

// abandonCurrentHeader clears the current header without forming a certificate.
func (p *Primary[H, T]) abandonCurrentHeader() {
	p.currentHeader = nil
	p.votes = make(map[uint16][]byte)
	p.headerRetries = 0
	// Note: we keep pendingBatches so they can be included in the next header
}

// OnBatch is called when a local worker creates a batch.
// If MaxPendingBatches is configured and the queue is full:
//   - If DropOnFull is true, the batch is dropped silently
//   - If DropOnFull is false, this call blocks until space is available
//
// Returns true if the batch was accepted, false if dropped.
func (p *Primary[H, T]) OnBatch(batch *Batch[H, T]) bool {
	// If using bounded channel, send to channel
	if p.batchChan != nil {
		if p.dropOnFull {
			// Non-blocking send
			select {
			case p.batchChan <- batch:
				return true
			default:
				// Queue full, drop batch
				p.mu.Lock()
				p.droppedBatches++
				p.mu.Unlock()
				p.logger.Debug("batch dropped, queue full",
					zap.String("batch_digest", batch.Digest.String()))
				return false
			}
		} else {
			// Blocking send
			p.batchChan <- batch
			return true
		}
	}

	// Unbounded mode - direct processing
	p.mu.Lock()
	defer p.mu.Unlock()

	p.pendingBatches = append(p.pendingBatches, batch.Digest)
	p.pendingBatchData = append(p.pendingBatchData, batch)
	p.lastBatchReceived = time.Now()

	// Check if we should create a header immediately (batch threshold reached)
	if len(p.pendingBatches) >= p.cfg.MaxHeaderBatches && p.currentHeader == nil {
		p.tryCreateHeaderLocked()
	}

	return true
}
func (p *Primary[H, T]) tryCreateHeaderLocked() {
	if p.currentHeader != nil || len(p.pendingBatches) == 0 {
		return
	}

	currentRound := p.cfg.DAG.CurrentRound()
	parents := p.cfg.DAG.GetParents()

	if currentRound > 0 && len(parents) < 2*p.cfg.Validators.F()+1 {
		return
	}

	header := &Header[H]{
		Author:    p.cfg.ValidatorID,
		Round:     currentRound,
		BatchRefs: p.pendingBatches,
		Parents:   parents,
		Timestamp: uint64(time.Now().UnixMilli()),
	}
	header.ComputeDigest(p.cfg.HashFunc)

	p.currentHeader = header
	p.votes = make(map[uint16][]byte)
	p.headerCreatedAt = time.Now()
	p.headerRetries = 0

	sig, err := p.cfg.Signer.Sign(header.Digest.Bytes())
	if err != nil {
		p.logger.Error("failed to sign header", zap.Error(err))
		p.currentHeader = nil
		return
	}
	p.votes[p.cfg.ValidatorID] = sig

	p.logger.Info("created header",
		zap.Uint64("round", header.Round),
		zap.Int("batch_refs", len(header.BatchRefs)))

	// Invoke hook
	if p.hooks != nil && p.hooks.OnHeaderCreated != nil {
		p.hooks.OnHeaderCreated(HeaderCreatedEvent[H]{
			Header:     header,
			BatchCount: len(header.BatchRefs),
			CreatedAt:  p.headerCreatedAt,
		})
	}

	_ = p.cfg.Storage.PutHeader(header) // Best-effort storage
	p.cfg.Network.BroadcastHeader(header)
	p.checkQuorumLocked()
}

// OnHeaderReceived handles a header from another primary.
// Before voting, validates:
// 1. Header digest is correct
// 2. Author is a valid validator
// 3. Round is not too far ahead
// 4. All parent certificates exist in the DAG
// 5. All batch digests are available (in local storage or fetched from author)
// 6. We haven't already voted for a different header from this author at this round
func (p *Primary[H, T]) OnHeaderReceived(header *Header[H], from uint16) error {
	if !header.VerifyDigest(p.cfg.HashFunc) {
		return fmt.Errorf("header digest mismatch")
	}

	if !p.cfg.Validators.Contains(header.Author) {
		return fmt.Errorf("invalid author: %d", header.Author)
	}

	currentRound := p.cfg.DAG.CurrentRound()
	if header.Round > currentRound+1 {
		return fmt.Errorf("header round %d too far ahead", header.Round)
	}

	// Check vote idempotence before doing expensive validation work
	// This prevents double-voting on equivocating headers (same author, same round, different digest)
	decision, existingDigest := p.voteTracker.ShouldVote(header.Author, header.Round, header.Epoch, header.Digest)
	switch decision {
	case VoteDecisionSkipOldRound:
		// We've already voted for a higher round from this author
		p.logger.Debug("skipping vote for old round",
			zap.Uint16("author", header.Author),
			zap.Uint64("round", header.Round))
		return nil
	case VoteDecisionSkipOldEpoch:
		return fmt.Errorf("header from old epoch %d", header.Epoch)
	case VoteDecisionSkipDuplicate:
		// Already voted for this exact header, silently skip
		return nil
	case VoteDecisionSkipEquivocation:
		// Equivocation detected! Author sent different headers at same round
		p.logger.Warn("equivocation detected - skipping vote",
			zap.Uint16("author", header.Author),
			zap.Uint64("round", header.Round),
			zap.String("existing_digest", (*existingDigest).String()),
			zap.String("new_digest", header.Digest.String()))
		return fmt.Errorf("equivocation detected: author %d at round %d", header.Author, header.Round)
	case VoteDecisionAllow:
		// Proceed with validation
	}

	if header.Round > 0 {
		for _, parent := range header.Parents {
			if !p.cfg.DAG.IsCertified(parent) {
				return fmt.Errorf("missing parent: %s", parent.String())
			}
		}
	}

	// Validate batch availability before voting (security: ensures we can reconstruct)
	for _, batchDigest := range header.BatchRefs {
		// Check if batch is in local storage
		_, err := p.cfg.Storage.GetBatch(batchDigest)
		if err != nil {
			// Try to fetch from header author
			batch, fetchErr := p.cfg.Network.FetchBatch(header.Author, batchDigest)
			if fetchErr != nil {
				return fmt.Errorf("batch %s not available: %w", batchDigest.String(), fetchErr)
			}
			// Verify fetched batch digest matches
			batch.ComputeDigest(p.cfg.HashFunc)
			if !batch.Digest.Equals(batchDigest) {
				return fmt.Errorf("fetched batch digest mismatch: got %s, want %s",
					batch.Digest.String(), batchDigest.String())
			}
			// Store the fetched batch
			if storeErr := p.cfg.Storage.PutBatch(batch); storeErr != nil {
				p.logger.Warn("failed to store fetched batch", zap.Error(storeErr))
			}
		}
	}

	sig, err := p.cfg.Signer.Sign(header.Digest.Bytes())
	if err != nil {
		return fmt.Errorf("failed to sign: %w", err)
	}

	vote := &HeaderVote[H]{
		HeaderDigest:   header.Digest,
		ValidatorIndex: p.cfg.ValidatorID,
		Signature:      sig,
	}

	// Record the vote BEFORE sending (to prevent race with incoming duplicates)
	p.voteTracker.RecordVote(header.Author, header.Round, header.Epoch, header.Digest)

	// Invoke hook for vote sent
	if p.hooks != nil && p.hooks.OnVoteSent != nil {
		p.hooks.OnVoteSent(VoteSentEvent[H]{
			HeaderDigest: header.Digest,
			HeaderAuthor: header.Author,
			SentAt:       time.Now(),
		})
	}

	p.cfg.Network.SendVote(header.Author, vote)
	return nil
}

// OnVoteReceived handles a vote for our header.
func (p *Primary[H, T]) OnVoteReceived(vote *HeaderVote[H]) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.currentHeader == nil || !vote.HeaderDigest.Equals(p.currentHeader.Digest) {
		return nil
	}

	if !p.cfg.Validators.Contains(vote.ValidatorIndex) {
		return fmt.Errorf("invalid voter: %d", vote.ValidatorIndex)
	}

	// Check signature cache first (if available)
	message := vote.HeaderDigest.Bytes()
	if p.sigCache != nil && p.sigCache.IsVerified(message, vote.ValidatorIndex) {
		// Already verified this signature
	} else {
		// Verify signature
		pubKey, err := p.cfg.Validators.GetByIndex(vote.ValidatorIndex)
		if err != nil {
			return err
		}
		if !pubKey.Verify(message, vote.Signature) {
			return fmt.Errorf("invalid signature")
		}
		// Cache the verified signature
		if p.sigCache != nil {
			p.sigCache.MarkVerified(message, vote.ValidatorIndex)
		}
	}

	if _, exists := p.votes[vote.ValidatorIndex]; exists {
		return nil
	}
	p.votes[vote.ValidatorIndex] = vote.Signature

	// Invoke hook
	if p.hooks != nil && p.hooks.OnVoteReceived != nil {
		p.hooks.OnVoteReceived(VoteReceivedEvent[H]{
			HeaderDigest:   vote.HeaderDigest,
			Voter:          vote.ValidatorIndex,
			TotalVotes:     len(p.votes),
			QuorumRequired: 2*p.cfg.Validators.F() + 1,
			ReceivedAt:     time.Now(),
		})
	}

	p.checkQuorumLocked()
	return nil
}
func (p *Primary[H, T]) checkQuorumLocked() {
	if p.currentHeader == nil {
		return
	}

	quorum := 2*p.cfg.Validators.F() + 1
	if len(p.votes) < quorum {
		return
	}

	cert := NewCertificate(p.currentHeader, p.votes)
	certFormedAt := time.Now()

	p.logger.Info("formed certificate",
		zap.Uint64("round", cert.Header.Round),
		zap.Int("signers", cert.SignerCount()))

	// Invoke hook
	if p.hooks != nil && p.hooks.OnCertificateFormed != nil {
		p.hooks.OnCertificateFormed(CertificateFormedEvent[H]{
			Certificate: cert,
			SignerCount: cert.SignerCount(),
			Latency:     certFormedAt.Sub(p.headerCreatedAt),
			FormedAt:    certFormedAt,
		})
	}

	_ = p.cfg.Storage.PutCertificate(cert) // Best-effort storage
	p.cfg.Network.BroadcastCertificate(cert)
	_ = p.cfg.DAG.InsertCertificate(cert, p.pendingBatchData) // Already validated

	p.currentHeader = nil
	p.pendingBatches = make([]H, 0)
	p.pendingBatchData = nil
	p.votes = make(map[uint16][]byte)
	p.headerRetries = 0
	p.lastBatchReceived = time.Time{} // Reset
}

// PrimaryStats contains primary statistics for monitoring.
type PrimaryStats struct {
	PendingBatches int    // Number of batch refs waiting for header creation
	QueuedBatches  int    // Batches in channel (if bounded)
	DroppedBatches uint64 // Batches dropped due to full queue
	MaxPending     int    // Maximum pending batches allowed
	IsBounded      bool   // Whether bounded mode is enabled
	HasPendingHdr  bool   // Whether there's a header waiting for votes
	CurrentVotes   int    // Votes received for current header
	HeaderRetries  int    // Retry count for current header
}

// Stats returns current primary statistics.
func (p *Primary[H, T]) Stats() PrimaryStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	stats := PrimaryStats{
		PendingBatches: len(p.pendingBatches),
		DroppedBatches: p.droppedBatches,
		MaxPending:     p.maxPending,
		IsBounded:      p.batchChan != nil,
		HasPendingHdr:  p.currentHeader != nil,
		CurrentVotes:   len(p.votes),
		HeaderRetries:  p.headerRetries,
	}

	if p.batchChan != nil {
		stats.QueuedBatches = len(p.batchChan)
	}

	return stats
}
