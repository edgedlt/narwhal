package narwhal

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// RequestTracker tracks pending network operations and provides cancel-on-round-advance.
// When the round advances, stale requests from previous rounds can be cancelled.
type RequestTracker[H Hash] struct {
	mu sync.Mutex

	// Current round - requests for rounds older than this can be cancelled
	currentRound uint64

	// Pending requests indexed by round
	// Each request has a cancel function and metadata
	pendingByRound map[uint64][]*trackedRequest[H]

	// All pending requests indexed by ID for fast lookup
	pendingByID map[uint64]*trackedRequest[H]

	// ID counter for unique request IDs
	nextID atomic.Uint64

	// Config
	cfg RequestTrackerConfig

	logger *zap.Logger

	// Stats
	totalStarted   uint64
	totalCompleted uint64
	totalCancelled uint64
}

// trackedRequest represents a pending network request.
type trackedRequest[H Hash] struct {
	ID        uint64
	Round     uint64
	Type      RequestType
	Digest    H
	StartedAt time.Time
	Cancel    context.CancelFunc
}

// RequestType identifies the type of network request.
type RequestType uint8

const (
	RequestTypeBatch RequestType = iota
	RequestTypeCertificate
	RequestTypeHeader
)

func (t RequestType) String() string {
	switch t {
	case RequestTypeBatch:
		return "batch"
	case RequestTypeCertificate:
		return "certificate"
	case RequestTypeHeader:
		return "header"
	default:
		return "unknown"
	}
}

// RequestTrackerConfig configures the RequestTracker.
type RequestTrackerConfig struct {
	// MaxPendingPerRound limits requests per round to prevent memory bloat.
	// Default: 1000
	MaxPendingPerRound int

	// StaleRoundThreshold is how many rounds behind a request must be to be considered stale.
	// Requests for rounds < (currentRound - StaleRoundThreshold) are cancelled.
	// Default: 2
	StaleRoundThreshold uint64

	// CancelOnAdvance enables automatic cancellation when round advances.
	// Default: true
	CancelOnAdvance bool
}

// DefaultRequestTrackerConfig returns sensible defaults.
func DefaultRequestTrackerConfig() RequestTrackerConfig {
	return RequestTrackerConfig{
		MaxPendingPerRound:  1000,
		StaleRoundThreshold: 2,
		CancelOnAdvance:     true,
	}
}

// NewRequestTracker creates a new RequestTracker.
func NewRequestTracker[H Hash](cfg RequestTrackerConfig, logger *zap.Logger) *RequestTracker[H] {
	if logger == nil {
		logger = zap.NewNop()
	}

	return &RequestTracker[H]{
		pendingByRound: make(map[uint64][]*trackedRequest[H]),
		pendingByID:    make(map[uint64]*trackedRequest[H]),
		cfg:            cfg,
		logger:         logger.With(zap.String("component", "request_tracker")),
	}
}

// Track registers a new request for tracking.
// Returns a context that will be cancelled if the request becomes stale,
// and a completion function that must be called when the request finishes.
func (rt *RequestTracker[H]) Track(
	parentCtx context.Context,
	round uint64,
	reqType RequestType,
	digest H,
) (context.Context, func()) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	// Create cancellable context
	ctx, cancel := context.WithCancel(parentCtx)

	id := rt.nextID.Add(1)
	req := &trackedRequest[H]{
		ID:        id,
		Round:     round,
		Type:      reqType,
		Digest:    digest,
		StartedAt: time.Now(),
		Cancel:    cancel,
	}

	// Check capacity for this round
	if len(rt.pendingByRound[round]) >= rt.cfg.MaxPendingPerRound {
		// Cancel oldest request for this round
		if len(rt.pendingByRound[round]) > 0 {
			oldest := rt.pendingByRound[round][0]
			oldest.Cancel()
			delete(rt.pendingByID, oldest.ID)
			rt.pendingByRound[round] = rt.pendingByRound[round][1:]
			rt.totalCancelled++
		}
	}

	rt.pendingByRound[round] = append(rt.pendingByRound[round], req)
	rt.pendingByID[id] = req
	rt.totalStarted++

	rt.logger.Debug("tracking request",
		zap.Uint64("id", id),
		zap.Uint64("round", round),
		zap.String("type", reqType.String()),
		zap.String("digest", digest.String()))

	// Return completion function
	complete := func() {
		rt.complete(id)
	}

	return ctx, complete
}

// complete marks a request as finished.
func (rt *RequestTracker[H]) complete(id uint64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	req, exists := rt.pendingByID[id]
	if !exists {
		return
	}

	delete(rt.pendingByID, id)

	// Remove from round index
	roundReqs := rt.pendingByRound[req.Round]
	for i, r := range roundReqs {
		if r.ID == id {
			rt.pendingByRound[req.Round] = append(roundReqs[:i], roundReqs[i+1:]...)
			break
		}
	}

	// Clean up empty round entries
	if len(rt.pendingByRound[req.Round]) == 0 {
		delete(rt.pendingByRound, req.Round)
	}

	rt.totalCompleted++

	rt.logger.Debug("request completed",
		zap.Uint64("id", id),
		zap.Duration("duration", time.Since(req.StartedAt)))
}

// OnRoundAdvance should be called when the DAG round advances.
// It cancels any requests for rounds that are now considered stale.
func (rt *RequestTracker[H]) OnRoundAdvance(newRound uint64) {
	if !rt.cfg.CancelOnAdvance {
		return
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()

	rt.currentRound = newRound

	// Calculate the stale threshold
	var staleThreshold uint64
	if newRound > rt.cfg.StaleRoundThreshold {
		staleThreshold = newRound - rt.cfg.StaleRoundThreshold
	}

	// Cancel and remove stale requests
	roundsToRemove := make([]uint64, 0)
	for round, reqs := range rt.pendingByRound {
		if round < staleThreshold {
			for _, req := range reqs {
				req.Cancel()
				delete(rt.pendingByID, req.ID)
				rt.totalCancelled++

				rt.logger.Debug("cancelled stale request",
					zap.Uint64("id", req.ID),
					zap.Uint64("request_round", round),
					zap.Uint64("current_round", newRound))
			}
			roundsToRemove = append(roundsToRemove, round)
		}
	}

	for _, round := range roundsToRemove {
		delete(rt.pendingByRound, round)
	}
}

// CancelAll cancels all pending requests.
func (rt *RequestTracker[H]) CancelAll() {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	for _, req := range rt.pendingByID {
		req.Cancel()
		rt.totalCancelled++
	}

	rt.pendingByRound = make(map[uint64][]*trackedRequest[H])
	rt.pendingByID = make(map[uint64]*trackedRequest[H])
}

// CancelForRound cancels all pending requests for a specific round.
func (rt *RequestTracker[H]) CancelForRound(round uint64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	reqs := rt.pendingByRound[round]
	for _, req := range reqs {
		req.Cancel()
		delete(rt.pendingByID, req.ID)
		rt.totalCancelled++
	}
	delete(rt.pendingByRound, round)
}

// RequestTrackerStats contains statistics for monitoring.
type RequestTrackerStats struct {
	CurrentRound   uint64
	PendingCount   int
	TotalStarted   uint64
	TotalCompleted uint64
	TotalCancelled uint64
}

// Stats returns current statistics.
func (rt *RequestTracker[H]) Stats() RequestTrackerStats {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	return RequestTrackerStats{
		CurrentRound:   rt.currentRound,
		PendingCount:   len(rt.pendingByID),
		TotalStarted:   rt.totalStarted,
		TotalCompleted: rt.totalCompleted,
		TotalCancelled: rt.totalCancelled,
	}
}

// PendingCount returns the number of pending requests.
func (rt *RequestTracker[H]) PendingCount() int {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return len(rt.pendingByID)
}

// PendingForRound returns the number of pending requests for a specific round.
func (rt *RequestTracker[H]) PendingForRound(round uint64) int {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return len(rt.pendingByRound[round])
}
