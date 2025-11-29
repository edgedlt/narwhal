package narwhal

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

// PendingHeader represents a header waiting for missing dependencies.
type PendingHeader[H Hash] struct {
	// Header is the header waiting to be processed.
	Header *Header[H]

	// From is the validator that sent this header.
	From uint16

	// MissingParents are parent certificate digests we don't have.
	MissingParents []H

	// ReceivedAt is when this header was first received.
	ReceivedAt time.Time

	// RetryCount tracks how many times we've tried to process this header.
	RetryCount int
}

// HeaderWaiterConfig configures the HeaderWaiter.
type HeaderWaiterConfig struct {
	// MaxPendingHeaders is the maximum number of headers to queue.
	// Headers beyond this limit are dropped (oldest first).
	// Default: 1000
	MaxPendingHeaders int

	// RetryInterval is how often to retry processing pending headers.
	// Default: 1s
	RetryInterval time.Duration

	// MaxRetries is the maximum number of retry attempts before dropping a header.
	// Default: 10
	MaxRetries int

	// MaxAge is the maximum time a header can wait before being dropped.
	// Default: 30s
	MaxAge time.Duration

	// FetchParents enables proactive fetching of missing parent certificates.
	// Default: false
	FetchParents bool
}

// DefaultHeaderWaiterConfig returns sensible defaults.
func DefaultHeaderWaiterConfig() HeaderWaiterConfig {
	return HeaderWaiterConfig{
		MaxPendingHeaders: 1000,
		RetryInterval:     time.Second,
		MaxRetries:        10,
		MaxAge:            30 * time.Second,
	}
}

// HeaderWaiter queues headers that can't be processed immediately due to
// missing parent certificates or batches. It periodically retries processing
// them as dependencies become available.
type HeaderWaiter[H Hash, T Transaction[H]] struct {
	mu sync.Mutex

	cfg HeaderWaiterConfig

	// pending maps header digest to pending header info
	pending map[string]*PendingHeader[H]

	// byMissingParent maps parent digest to headers waiting for that parent
	byMissingParent map[string][]*PendingHeader[H]

	// processFunc is called when a header is ready to be processed
	processFunc func(header *Header[H], from uint16) error

	// checkParentFunc checks if a parent certificate exists
	checkParentFunc func(digest H) bool

	// fetchParentFunc fetches a missing parent certificate (optional)
	fetchParentFunc func(digest H, from uint16) error

	// hooks for observability
	hooks *Hooks[H, T]

	logger *zap.Logger

	// Stats
	totalReceived  uint64
	totalProcessed uint64
	totalDropped   uint64
	totalExpired   uint64
	totalFetched   uint64
}

// NewHeaderWaiter creates a new HeaderWaiter.
func NewHeaderWaiter[H Hash, T Transaction[H]](
	cfg HeaderWaiterConfig,
	processFunc func(header *Header[H], from uint16) error,
	checkParentFunc func(digest H) bool,
	hooks *Hooks[H, T],
	logger *zap.Logger,
) *HeaderWaiter[H, T] {
	if logger == nil {
		logger = zap.NewNop()
	}

	return &HeaderWaiter[H, T]{
		cfg:             cfg,
		pending:         make(map[string]*PendingHeader[H]),
		byMissingParent: make(map[string][]*PendingHeader[H]),
		processFunc:     processFunc,
		checkParentFunc: checkParentFunc,
		hooks:           hooks,
		logger:          logger.With(zap.String("component", "header_waiter")),
	}
}

// SetFetchParentFunc sets the function to fetch missing parent certificates.
// When set and FetchParents is enabled, the HeaderWaiter will proactively
// fetch missing parents from the peer that sent the header.
func (hw *HeaderWaiter[H, T]) SetFetchParentFunc(fn func(digest H, from uint16) error) {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	hw.fetchParentFunc = fn
}

// Run starts the periodic retry loop.
func (hw *HeaderWaiter[H, T]) Run(ctx context.Context) {
	ticker := time.NewTicker(hw.cfg.RetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			hw.retryPending()
		}
	}
}

// Add queues a header with missing parents for later processing.
// Returns true if the header was queued, false if it was dropped (queue full or duplicate).
// If FetchParents is enabled and fetchParentFunc is set, missing parents will be
// proactively fetched from the sender.
func (hw *HeaderWaiter[H, T]) Add(header *Header[H], from uint16, missingParents []H) bool {
	hw.mu.Lock()
	defer hw.mu.Unlock()

	hw.totalReceived++

	digestStr := header.Digest.String()

	// Check if already pending
	if _, exists := hw.pending[digestStr]; exists {
		return false // Duplicate
	}

	// Check capacity
	if len(hw.pending) >= hw.cfg.MaxPendingHeaders {
		// Drop oldest header
		hw.dropOldestLocked()
		hw.totalDropped++
	}

	pending := &PendingHeader[H]{
		Header:         header,
		From:           from,
		MissingParents: missingParents,
		ReceivedAt:     time.Now(),
		RetryCount:     0,
	}

	hw.pending[digestStr] = pending

	// Index by missing parents for fast lookup when parents arrive
	for _, parent := range missingParents {
		parentStr := parent.String()
		hw.byMissingParent[parentStr] = append(hw.byMissingParent[parentStr], pending)
	}

	// Proactively fetch missing parents if enabled
	if hw.cfg.FetchParents && hw.fetchParentFunc != nil {
		go hw.fetchMissingParents(missingParents, from)
	}

	hw.logger.Debug("queued header with missing parents",
		zap.String("header", digestStr),
		zap.Int("missing_parents", len(missingParents)),
		zap.Int("pending_count", len(hw.pending)))

	return true
}

// OnParentAvailable should be called when a new certificate is inserted into the DAG.
// It triggers retry of any headers that were waiting for this parent.
func (hw *HeaderWaiter[H, T]) OnParentAvailable(parentDigest H) {
	hw.mu.Lock()
	defer hw.mu.Unlock()

	parentStr := parentDigest.String()
	waitingHeaders := hw.byMissingParent[parentStr]
	if len(waitingHeaders) == 0 {
		return
	}

	// Remove from index
	delete(hw.byMissingParent, parentStr)

	// Update missing parents for each waiting header
	for _, pending := range waitingHeaders {
		// Remove this parent from the missing list
		currentLen := len(pending.MissingParents)
		if currentLen == 0 {
			// Already empty, try to process
			hw.tryProcessLocked(pending)
			continue
		}
		newCap := currentLen - 1
		if newCap < 0 {
			newCap = 0
		}
		newMissing := make([]H, 0, newCap)
		for _, p := range pending.MissingParents {
			if !p.Equals(parentDigest) {
				newMissing = append(newMissing, p)
			}
		}
		pending.MissingParents = newMissing

		// If no more missing parents, try to process
		if len(pending.MissingParents) == 0 {
			hw.tryProcessLocked(pending)
		}
	}
}

// retryPending attempts to process all pending headers.
func (hw *HeaderWaiter[H, T]) retryPending() {
	hw.mu.Lock()
	defer hw.mu.Unlock()

	now := time.Now()
	toRemove := make([]string, 0)

	for digestStr, pending := range hw.pending {
		// Check if expired
		if now.Sub(pending.ReceivedAt) > hw.cfg.MaxAge {
			toRemove = append(toRemove, digestStr)
			hw.totalExpired++
			hw.logger.Debug("header expired",
				zap.String("header", digestStr),
				zap.Duration("age", now.Sub(pending.ReceivedAt)))
			continue
		}

		// Check if max retries exceeded
		if pending.RetryCount >= hw.cfg.MaxRetries {
			toRemove = append(toRemove, digestStr)
			hw.totalDropped++
			hw.logger.Debug("header dropped after max retries",
				zap.String("header", digestStr),
				zap.Int("retries", pending.RetryCount))
			continue
		}

		// Check if parents are now available
		stillMissing := make([]H, 0)
		for _, parent := range pending.MissingParents {
			if !hw.checkParentFunc(parent) {
				stillMissing = append(stillMissing, parent)
			}
		}
		pending.MissingParents = stillMissing

		if len(stillMissing) == 0 {
			hw.tryProcessLocked(pending)
			if _, exists := hw.pending[digestStr]; !exists {
				// Successfully processed
				continue
			}
		}

		pending.RetryCount++
	}

	// Remove expired/dropped headers
	for _, digestStr := range toRemove {
		hw.removePendingLocked(digestStr)
	}
}

// tryProcessLocked attempts to process a pending header.
func (hw *HeaderWaiter[H, T]) tryProcessLocked(pending *PendingHeader[H]) {
	digestStr := pending.Header.Digest.String()

	// Check if still pending (may have been processed by another path)
	if _, exists := hw.pending[digestStr]; !exists {
		return
	}

	// Try to process
	err := hw.processFunc(pending.Header, pending.From)
	if err != nil {
		hw.logger.Debug("failed to process pending header",
			zap.String("header", digestStr),
			zap.Error(err))
		return
	}

	// Success - remove from pending
	hw.removePendingLocked(digestStr)
	hw.totalProcessed++

	hw.logger.Debug("processed pending header",
		zap.String("header", digestStr),
		zap.Int("retry_count", pending.RetryCount),
		zap.Duration("wait_time", time.Since(pending.ReceivedAt)))
}

// removePendingLocked removes a header from all indexes.
func (hw *HeaderWaiter[H, T]) removePendingLocked(digestStr string) {
	pending, exists := hw.pending[digestStr]
	if !exists {
		return
	}

	// Remove from parent index
	for _, parent := range pending.MissingParents {
		parentStr := parent.String()
		headers := hw.byMissingParent[parentStr]
		newHeaders := make([]*PendingHeader[H], 0, len(headers)-1)
		for _, h := range headers {
			if h.Header.Digest.String() != digestStr {
				newHeaders = append(newHeaders, h)
			}
		}
		if len(newHeaders) == 0 {
			delete(hw.byMissingParent, parentStr)
		} else {
			hw.byMissingParent[parentStr] = newHeaders
		}
	}

	delete(hw.pending, digestStr)
}

// dropOldestLocked removes the oldest pending header.
func (hw *HeaderWaiter[H, T]) dropOldestLocked() {
	var oldest *PendingHeader[H]
	var oldestKey string

	for key, pending := range hw.pending {
		if oldest == nil || pending.ReceivedAt.Before(oldest.ReceivedAt) {
			oldest = pending
			oldestKey = key
		}
	}

	if oldest != nil {
		hw.removePendingLocked(oldestKey)
		hw.logger.Debug("dropped oldest header due to capacity",
			zap.String("header", oldestKey))
	}
}

// fetchMissingParents proactively fetches missing parent certificates.
// This runs in a separate goroutine to avoid blocking header queuing.
func (hw *HeaderWaiter[H, T]) fetchMissingParents(parents []H, from uint16) {
	for _, parent := range parents {
		// Check if still missing (may have arrived while we were fetching others)
		if hw.checkParentFunc(parent) {
			continue
		}

		// Try to fetch
		err := hw.fetchParentFunc(parent, from)
		if err != nil {
			hw.logger.Debug("failed to fetch missing parent",
				zap.String("parent", parent.String()),
				zap.Uint16("from", from),
				zap.Error(err))
			continue
		}

		hw.mu.Lock()
		hw.totalFetched++
		hw.mu.Unlock()

		hw.logger.Debug("fetched missing parent",
			zap.String("parent", parent.String()),
			zap.Uint16("from", from))

		// Notify that this parent is now available
		hw.OnParentAvailable(parent)
	}
}

// HeaderWaiterStats contains statistics for monitoring.
type HeaderWaiterStats struct {
	PendingCount   int
	TotalReceived  uint64
	TotalProcessed uint64
	TotalDropped   uint64
	TotalExpired   uint64
	TotalFetched   uint64
}

// Stats returns current statistics.
func (hw *HeaderWaiter[H, T]) Stats() HeaderWaiterStats {
	hw.mu.Lock()
	defer hw.mu.Unlock()

	return HeaderWaiterStats{
		PendingCount:   len(hw.pending),
		TotalReceived:  hw.totalReceived,
		TotalProcessed: hw.totalProcessed,
		TotalDropped:   hw.totalDropped,
		TotalExpired:   hw.totalExpired,
		TotalFetched:   hw.totalFetched,
	}
}

// PendingCount returns the number of headers currently waiting.
func (hw *HeaderWaiter[H, T]) PendingCount() int {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	return len(hw.pending)
}
