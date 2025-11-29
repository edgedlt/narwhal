package narwhal

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Proposer creates headers from batches. Standalone component for advanced use cases
// where header creation needs to be separated from vote collection.
type Proposer[H Hash, T Transaction[H]] struct {
	mu sync.Mutex

	cfg ProposerConfig[H, T]

	// Pending batches waiting to be included in a header
	pendingBatches   []H
	pendingBatchData []*Batch[H, T]

	// Timing
	lastBatchReceived time.Time
	lastHeaderCreated time.Time

	// Bounded batch channel (if MaxPending > 0)
	batchChan  chan *Batch[H, T]
	maxPending int
	dropOnFull bool

	// Stats
	droppedBatches  uint64
	headersProposed uint64
	batchesIncluded uint64

	// Output channel for proposed headers
	headerOut chan *ProposedHeader[H, T]

	hooks  *Hooks[H, T]
	logger *zap.Logger
}

// ProposedHeader represents a header proposed by the Proposer.
type ProposedHeader[H Hash, T Transaction[H]] struct {
	Header    *Header[H]
	Batches   []*Batch[H, T]
	CreatedAt time.Time
}

// ProposerConfig configures the Proposer.
type ProposerConfig[H Hash, T Transaction[H]] struct {
	ValidatorID uint16
	DAG         *DAG[H, T]
	Validators  ValidatorSet
	HashFunc    func([]byte) H
	Hooks       *Hooks[H, T]
	Logger      *zap.Logger

	// Header creation thresholds
	MaxHeaderBatches int           // Max batch refs per header before proposing
	MaxHeaderDelay   time.Duration // Max delay before proposing regardless of batch count
	MinHeaderBatches int           // Min batches before proposing (default: 1)

	// Backpressure
	MaxPendingBatches int  // Max pending batches (0 = unbounded)
	DropOnFull        bool // If true, drop when full; if false, block

	// Output buffer size
	HeaderOutputBuffer int // Size of header output channel (default: 10)
}

// DefaultProposerConfig returns sensible defaults.
func DefaultProposerConfig[H Hash, T Transaction[H]]() ProposerConfig[H, T] {
	return ProposerConfig[H, T]{
		MaxHeaderBatches:   100,
		MaxHeaderDelay:     500 * time.Millisecond,
		MinHeaderBatches:   1,
		MaxPendingBatches:  1000,
		DropOnFull:         false,
		HeaderOutputBuffer: 10,
	}
}

// NewProposer creates a new Proposer.
func NewProposer[H Hash, T Transaction[H]](cfg ProposerConfig[H, T]) *Proposer[H, T] {
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	// Set defaults
	if cfg.MaxHeaderBatches == 0 {
		cfg.MaxHeaderBatches = 100
	}
	if cfg.MaxHeaderDelay == 0 {
		cfg.MaxHeaderDelay = 500 * time.Millisecond
	}
	if cfg.MinHeaderBatches == 0 {
		cfg.MinHeaderBatches = 1
	}
	if cfg.HeaderOutputBuffer == 0 {
		cfg.HeaderOutputBuffer = 10
	}

	p := &Proposer[H, T]{
		cfg:            cfg,
		pendingBatches: make([]H, 0, cfg.MaxHeaderBatches),
		maxPending:     cfg.MaxPendingBatches,
		dropOnFull:     cfg.DropOnFull,
		headerOut:      make(chan *ProposedHeader[H, T], cfg.HeaderOutputBuffer),
		hooks:          cfg.Hooks,
		logger:         logger.With(zap.String("component", "proposer")),
	}

	if cfg.MaxPendingBatches > 0 {
		p.batchChan = make(chan *Batch[H, T], cfg.MaxPendingBatches)
	}

	return p
}

// Run starts the proposer's main loop.
func (p *Proposer[H, T]) Run(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.MaxHeaderDelay / 2) // Check more frequently than max delay
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			p.checkAndPropose()

		case batch := <-p.batchChanOrNil():
			if batch != nil {
				p.processBatch(batch)
			}
		}
	}
}

// batchChanOrNil returns the batch channel if bounded mode is enabled.
func (p *Proposer[H, T]) batchChanOrNil() <-chan *Batch[H, T] {
	return p.batchChan
}

// AddBatch adds a batch to be included in a future header.
// Returns true if accepted, false if dropped due to full queue.
func (p *Proposer[H, T]) AddBatch(batch *Batch[H, T]) bool {
	if p.batchChan != nil {
		if p.dropOnFull {
			select {
			case p.batchChan <- batch:
				return true
			default:
				p.mu.Lock()
				p.droppedBatches++
				p.mu.Unlock()
				p.logger.Debug("batch dropped, queue full",
					zap.String("digest", batch.Digest.String()))
				return false
			}
		} else {
			p.batchChan <- batch
			return true
		}
	}

	// Unbounded mode
	p.processBatch(batch)
	return true
}

// processBatch handles a received batch.
func (p *Proposer[H, T]) processBatch(batch *Batch[H, T]) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.pendingBatches = append(p.pendingBatches, batch.Digest)
	p.pendingBatchData = append(p.pendingBatchData, batch)
	p.lastBatchReceived = time.Now()

	// Check if we should propose immediately (batch threshold)
	if len(p.pendingBatches) >= p.cfg.MaxHeaderBatches {
		p.tryProposeLocked()
	}
}

// checkAndPropose checks if we should propose based on time threshold.
func (p *Proposer[H, T]) checkAndPropose() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.pendingBatches) < p.cfg.MinHeaderBatches {
		return
	}

	// Check time threshold
	if !p.lastBatchReceived.IsZero() {
		if time.Since(p.lastBatchReceived) >= p.cfg.MaxHeaderDelay {
			p.tryProposeLocked()
		}
	}
}

// tryProposeLocked attempts to create and propose a header.
func (p *Proposer[H, T]) tryProposeLocked() {
	if len(p.pendingBatches) < p.cfg.MinHeaderBatches {
		return
	}

	currentRound := p.cfg.DAG.CurrentRound()
	parents := p.cfg.DAG.GetParents()

	// Need 2f+1 parents for rounds > 0
	if currentRound > 0 && len(parents) < 2*p.cfg.Validators.F()+1 {
		p.logger.Debug("not enough parents for header",
			zap.Uint64("round", currentRound),
			zap.Int("parents", len(parents)),
			zap.Int("needed", 2*p.cfg.Validators.F()+1))
		return
	}

	// Create header
	header := &Header[H]{
		Author:    p.cfg.ValidatorID,
		Round:     currentRound,
		BatchRefs: p.pendingBatches,
		Parents:   parents,
		Timestamp: uint64(time.Now().UnixMilli()),
	}
	header.ComputeDigest(p.cfg.HashFunc)

	proposed := &ProposedHeader[H, T]{
		Header:    header,
		Batches:   p.pendingBatchData,
		CreatedAt: time.Now(),
	}

	// Send to output channel (non-blocking with drop if full)
	select {
	case p.headerOut <- proposed:
		p.headersProposed++
		p.batchesIncluded += uint64(len(p.pendingBatches))

		p.logger.Info("proposed header",
			zap.Uint64("round", header.Round),
			zap.Int("batches", len(header.BatchRefs)),
			zap.Int("parents", len(header.Parents)))

		// Invoke hook
		if p.hooks != nil && p.hooks.OnHeaderCreated != nil {
			p.hooks.OnHeaderCreated(HeaderCreatedEvent[H]{
				Header:     header,
				BatchCount: len(header.BatchRefs),
				CreatedAt:  proposed.CreatedAt,
			})
		}

		// Clear pending batches
		p.pendingBatches = make([]H, 0, p.cfg.MaxHeaderBatches)
		p.pendingBatchData = nil
		p.lastHeaderCreated = time.Now()

	default:
		p.logger.Warn("header output channel full, dropping proposal")
	}
}

// Headers returns the channel for receiving proposed headers.
func (p *Proposer[H, T]) Headers() <-chan *ProposedHeader[H, T] {
	return p.headerOut
}

// ProposerStats contains statistics for monitoring.
type ProposerStats struct {
	PendingBatches  int
	QueuedBatches   int
	DroppedBatches  uint64
	HeadersProposed uint64
	BatchesIncluded uint64
	MaxPending      int
	IsBounded       bool
}

// Stats returns current statistics.
func (p *Proposer[H, T]) Stats() ProposerStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	stats := ProposerStats{
		PendingBatches:  len(p.pendingBatches),
		DroppedBatches:  p.droppedBatches,
		HeadersProposed: p.headersProposed,
		BatchesIncluded: p.batchesIncluded,
		MaxPending:      p.maxPending,
		IsBounded:       p.batchChan != nil,
	}

	if p.batchChan != nil {
		stats.QueuedBatches = len(p.batchChan)
	}

	return stats
}

// PendingCount returns the number of pending batches.
func (p *Proposer[H, T]) PendingCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pendingBatches)
}

// ForcePropose forces creation of a header if there are any pending batches.
// Used for testing and flushing on shutdown.
func (p *Proposer[H, T]) ForcePropose() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.pendingBatches) == 0 {
		return false
	}

	// Temporarily set min to 0
	origMin := p.cfg.MinHeaderBatches
	p.cfg.MinHeaderBatches = 0
	p.tryProposeLocked()
	p.cfg.MinHeaderBatches = origMin

	return true
}
