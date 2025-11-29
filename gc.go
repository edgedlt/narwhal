package narwhal

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

// GCConfig configures the GarbageCollector.
type GCConfig struct {
	// Interval is how often GC runs (in rounds, not time).
	// GC is triggered when currentRound - lastGCRound >= Interval.
	Interval uint64

	// RetainRounds is the number of rounds to keep after the committed round.
	// Older data is eligible for garbage collection.
	RetainRounds uint64

	// CheckInterval is how often to check if GC should run (in time).
	CheckInterval time.Duration
}

// DefaultGCConfig returns sensible defaults for the GarbageCollector.
func DefaultGCConfig() GCConfig {
	return GCConfig{
		Interval:      50,
		RetainRounds:  100,
		CheckInterval: 10 * time.Second,
	}
}

// GarbageCollector periodically cleans up old DAG data. Standalone component
// for automatic round-based cleanup independent of the main Narwhal struct.
type GarbageCollector[H Hash, T Transaction[H]] struct {
	mu sync.Mutex

	cfg     GCConfig
	dag     *DAG[H, T]
	storage Storage[H, T]

	// Tracking
	lastGCRound    uint64
	committedRound uint64

	// Callbacks (legacy - prefer using Hooks)
	onGC func(beforeRound uint64)

	hooks  *Hooks[H, T]
	logger *zap.Logger
}

// NewGarbageCollector creates a new GarbageCollector.
func NewGarbageCollector[H Hash, T Transaction[H]](
	cfg GCConfig,
	dag *DAG[H, T],
	storage Storage[H, T],
	logger *zap.Logger,
) *GarbageCollector[H, T] {
	return NewGarbageCollectorWithHooks(cfg, dag, storage, nil, logger)
}

// NewGarbageCollectorWithHooks creates a new GarbageCollector with observability hooks.
func NewGarbageCollectorWithHooks[H Hash, T Transaction[H]](
	cfg GCConfig,
	dag *DAG[H, T],
	storage Storage[H, T],
	hooks *Hooks[H, T],
	logger *zap.Logger,
) *GarbageCollector[H, T] {
	if logger == nil {
		logger = zap.NewNop()
	}

	return &GarbageCollector[H, T]{
		cfg:     cfg,
		dag:     dag,
		storage: storage,
		hooks:   hooks,
		logger:  logger,
	}
}

// Run starts the garbage collection loop.
// It runs until the context is cancelled.
func (gc *GarbageCollector[H, T]) Run(ctx context.Context) {
	ticker := time.NewTicker(gc.cfg.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			gc.maybeCollect()
		}
	}
}

// SetCommittedRound updates the committed round.
// GC will only clean up data older than (committedRound - retainRounds).
func (gc *GarbageCollector[H, T]) SetCommittedRound(round uint64) {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	if round > gc.committedRound {
		gc.committedRound = round
	}
}

// OnGC sets a callback that is invoked when GC runs.
func (gc *GarbageCollector[H, T]) OnGC(callback func(beforeRound uint64)) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.onGC = callback
}

// ForceGC forces an immediate garbage collection cycle.
func (gc *GarbageCollector[H, T]) ForceGC() {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.collect()
}

func (gc *GarbageCollector[H, T]) maybeCollect() {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	currentRound := gc.dag.CurrentRound()

	// Check if enough rounds have passed since last GC
	if currentRound < gc.lastGCRound+gc.cfg.Interval {
		return
	}

	gc.collect()
}

func (gc *GarbageCollector[H, T]) collect() {
	// Calculate the round before which to GC
	// We want to keep at least RetainRounds rounds of data after committed
	var gcBeforeRound uint64
	if gc.committedRound > gc.cfg.RetainRounds {
		gcBeforeRound = gc.committedRound - gc.cfg.RetainRounds
	}

	// Don't GC if nothing to clean
	if gcBeforeRound <= gc.lastGCRound {
		return
	}

	gc.logger.Info("running garbage collection",
		zap.Uint64("before_round", gcBeforeRound),
		zap.Uint64("committed_round", gc.committedRound),
		zap.Uint64("last_gc_round", gc.lastGCRound))

	// GC the DAG
	gc.dag.GarbageCollect(gcBeforeRound)

	// GC the storage
	if err := gc.storage.DeleteBeforeRound(gcBeforeRound); err != nil {
		gc.logger.Warn("storage GC failed", zap.Error(err))
	}

	gc.lastGCRound = gcBeforeRound

	// Invoke callback (legacy)
	if gc.onGC != nil {
		gc.onGC(gcBeforeRound)
	}

	// Invoke hook
	if gc.hooks != nil && gc.hooks.OnGarbageCollected != nil {
		gc.hooks.OnGarbageCollected(GarbageCollectedEvent{
			BeforeRound:    gcBeforeRound,
			CommittedRound: gc.committedRound,
			CollectedAt:    time.Now(),
		})
	}

	gc.logger.Info("garbage collection complete",
		zap.Uint64("gc_round", gcBeforeRound))
}

// Stats returns current GC statistics.
func (gc *GarbageCollector[H, T]) Stats() GCStats {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	return GCStats{
		LastGCRound:    gc.lastGCRound,
		CommittedRound: gc.committedRound,
	}
}

// GCStats contains garbage collection statistics.
type GCStats struct {
	LastGCRound    uint64
	CommittedRound uint64
}
