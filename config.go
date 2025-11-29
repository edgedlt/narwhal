package narwhal

import (
	"fmt"
	"time"

	"go.uber.org/zap"
)

// Config holds the configuration for a Narwhal instance.
// Use NewConfig with functional options to create a properly configured instance.
//
// The type parameters specify the hash and transaction types used.
type Config[H Hash, T Transaction[H]] struct {
	// Identity

	// MyIndex is this validator's index in the validator set.
	MyIndex uint16

	// Validators is the set of all validators.
	// Required.
	Validators ValidatorSet

	// Signer provides cryptographic signing capability.
	// Required.
	Signer Signer

	// Storage provides persistent storage.
	// Required.
	Storage Storage[H, T]

	// Network provides message delivery.
	// Required.
	Network Network[H, T]

	// Timer provides timeout management.
	// Required.
	Timer Timer

	// Logger for structured logging.
	// Defaults to a no-op logger if not provided.
	Logger *zap.Logger

	// Worker configuration

	// WorkerCount is the number of workers for parallel batch creation.
	// Default: 4
	WorkerCount int

	// BatchSize is the maximum number of transactions per batch.
	// When this threshold is reached, a batch is created immediately.
	// Default: 500
	BatchSize int

	// BatchTimeout is the maximum time to wait before creating a batch.
	// Even if BatchSize is not reached, a batch is created after this duration.
	// Default: 100ms
	BatchTimeout time.Duration

	// Primary configuration

	// HeaderTimeout is the interval between header creation attempts.
	// Default: 500ms
	HeaderTimeout time.Duration

	// DAG configuration

	// GCInterval is the number of rounds between garbage collection runs.
	// Older rounds are cleaned up to free memory and storage.
	// Default: 50
	GCInterval uint64

	// MaxRoundsGap is the maximum number of rounds ahead we accept headers for.
	// Headers for rounds beyond currentRound + MaxRoundsGap are rejected.
	// Default: 10
	MaxRoundsGap uint64

	// Backpressure configuration

	// MaxPendingTransactions is the maximum number of transactions queued per worker.
	// When this limit is reached, AddTransaction will block or drop based on DropOnFull.
	// Default: 10000
	MaxPendingTransactions int

	// MaxPendingBatches is the maximum number of batches queued in the primary.
	// Default: 1000
	MaxPendingBatches int

	// DropOnFull determines behavior when queues are full.
	// If true, new items are dropped. If false, AddTransaction blocks.
	// Default: false (block)
	DropOnFull bool

	// Header creation configuration (aligned with reference implementation)

	// MaxHeaderBatches is the maximum number of batch refs to include in a header.
	// Headers are created when this limit is reached, even if MaxHeaderDelay hasn't elapsed.
	// Default: 100
	MaxHeaderBatches int

	// MaxHeaderDelay is the maximum time to wait before creating a header.
	// Even if MaxHeaderBatches is not reached, a header is created after this duration.
	// Default: HeaderTimeout (uses HeaderTimeout value if not set)
	MaxHeaderDelay time.Duration

	// Synchronization configuration (aligned with reference implementation)

	// SyncRetryDelay is the delay before retrying sync requests for missing data.
	// Default: 10s
	SyncRetryDelay time.Duration

	// SyncRetryNodes is the number of random nodes to contact when syncing.
	// Default: 3
	SyncRetryNodes int

	// GCDepth is the number of rounds to retain after the committed round.
	// This is separate from GCInterval which controls how often GC runs.
	// Default: 100
	GCDepth uint64

	// Hooks provides callbacks for observability events.
	// All hooks are optional - nil hooks are ignored.
	// See the Hooks struct for available callbacks.
	Hooks *Hooks[H, T]

	// HeaderWaiter configuration

	// MaxPendingHeaders is the maximum number of headers with missing parents to queue.
	// Default: 1000
	MaxPendingHeaders int

	// HeaderWaiterRetryInterval is how often to retry processing pending headers.
	// Default: 1s
	HeaderWaiterRetryInterval time.Duration

	// HeaderWaiterMaxRetries is the maximum retries before dropping a pending header.
	// Default: 10
	HeaderWaiterMaxRetries int

	// HeaderWaiterMaxAge is the maximum time a header can wait before being dropped.
	// Default: 30s
	HeaderWaiterMaxAge time.Duration

	// FetchMissingParents enables proactive fetching of missing parent certificates
	// when headers are queued. This can reduce latency by fetching parents in parallel.
	// Default: true
	FetchMissingParents bool

	// CryptoProvider provides optimized cryptographic operations.
	// When set, certificate validation uses batch/parallel signature verification.
	// For Ed25519, this parallelizes verification across goroutines.
	// For BLS, this enables true batch verification and signature aggregation.
	// Default: nil (uses standard sequential verification)
	CryptoProvider CryptoProvider

	// SignatureCache caches verified signatures to avoid re-verification.
	// Only used when CryptoProvider is set.
	// Default: nil (no caching)
	SignatureCache *SignatureCache

	// Validation is the configuration for input validation.
	// Controls limits for DoS prevention (max batch refs, max transactions, etc.)
	// Default: DefaultValidationConfig()
	Validation ValidationConfig

	// RecommendedMessageQueueSize is the advisory buffer size for Network receive channels.
	// Default: 10000
	RecommendedMessageQueueSize int

	// DAGCache configures the optional LRU cache for DAG vertex lookups.
	// When enabled, frequently accessed vertices are cached for faster retrieval.
	// This improves performance for workloads with repeated vertex lookups.
	// Default: DAGCacheConfig{Enabled: true, Capacity: 10000}
	DAGCache DAGCacheConfig

	// NetworkModel specifies the network timing assumptions.
	// Asynchronous mode advances rounds as soon as 2f+1 parents are available.
	// Partially synchronous mode waits for leader blocks before advancing.
	// Default: NetworkModelAsynchronous
	NetworkModel NetworkModel

	// LeaderSchedule determines which validator is leader for each round.
	// Only used when NetworkModel is NetworkModelPartiallySynchronous.
	// Default: nil (round-robin schedule created from validator count)
	LeaderSchedule LeaderSchedule

	// Epoch is the initial epoch for the validator set.
	// Used for epoch-aware reconfiguration and vote tracking.
	// Default: 0
	Epoch uint64
}

// ConfigOption is a functional option for configuring Narwhal.
// Options are applied in order, so later options override earlier ones.
type ConfigOption[H Hash, T Transaction[H]] func(*Config[H, T]) error

// NewConfig creates a new Config with the given options.
// Required options: WithValidators, WithSigner, WithStorage, WithNetwork, WithTimer.
//
// Returns an error if any option fails or if required options are missing.
func NewConfig[H Hash, T Transaction[H]](opts ...ConfigOption[H, T]) (*Config[H, T], error) {
	cfg := &Config[H, T]{
		Logger:                      zap.NewNop(),
		WorkerCount:                 4,
		BatchSize:                   500,
		BatchTimeout:                100 * time.Millisecond,
		HeaderTimeout:               500 * time.Millisecond,
		GCInterval:                  50,
		MaxRoundsGap:                10,
		MaxPendingTransactions:      10000,
		MaxPendingBatches:           1000,
		DropOnFull:                  false,
		MaxHeaderBatches:            100,
		MaxHeaderDelay:              0, // Will use HeaderTimeout if 0
		SyncRetryDelay:              10 * time.Second,
		SyncRetryNodes:              3,
		GCDepth:                     100,
		MaxPendingHeaders:           1000,
		HeaderWaiterRetryInterval:   time.Second,
		HeaderWaiterMaxRetries:      10,
		HeaderWaiterMaxAge:          30 * time.Second,
		FetchMissingParents:         true,
		Validation:                  DefaultValidationConfig(),
		RecommendedMessageQueueSize: 10000,
		DAGCache:                    DefaultDAGCacheConfig(),
	}

	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return nil, err
		}
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return cfg, nil
}

// validate checks that all required fields are set and values are valid.
func (c *Config[H, T]) validate() error {
	if c.Validators == nil {
		return fmt.Errorf("validators is required")
	}
	if c.Signer == nil {
		return fmt.Errorf("signer is required")
	}
	if c.Storage == nil {
		return fmt.Errorf("storage is required")
	}
	if c.Network == nil {
		return fmt.Errorf("network is required")
	}
	if c.Timer == nil {
		return fmt.Errorf("timer is required")
	}
	if c.Logger == nil {
		return fmt.Errorf("logger is required")
	}

	// Validate MyIndex is within validator set
	if !c.Validators.Contains(c.MyIndex) {
		return fmt.Errorf("myIndex %d is not in validator set (count: %d)", c.MyIndex, c.Validators.Count())
	}

	// Validate numeric ranges
	if c.WorkerCount < 1 {
		return fmt.Errorf("worker count must be at least 1, got %d", c.WorkerCount)
	}
	if c.BatchSize < 1 {
		return fmt.Errorf("batch size must be at least 1, got %d", c.BatchSize)
	}
	if c.BatchTimeout <= 0 {
		return fmt.Errorf("batch timeout must be positive, got %v", c.BatchTimeout)
	}
	if c.HeaderTimeout <= 0 {
		return fmt.Errorf("header timeout must be positive, got %v", c.HeaderTimeout)
	}
	if c.GCInterval == 0 {
		return fmt.Errorf("GC interval must be positive")
	}
	if c.MaxRoundsGap == 0 {
		return fmt.Errorf("max rounds gap must be positive")
	}

	return nil
}

// ConfigWarning represents a warning about potentially suboptimal configuration.
type ConfigWarning struct {
	// Field is the name of the config field that triggered the warning.
	Field string
	// Message describes the potential issue.
	Message string
	// Suggestion provides a recommended action or value.
	Suggestion string
}

// String returns a human-readable warning message.
func (w ConfigWarning) String() string {
	return fmt.Sprintf("%s: %s (suggestion: %s)", w.Field, w.Message, w.Suggestion)
}

// Warnings returns warnings for suboptimal configuration choices.
func (c *Config[H, T]) Warnings() []ConfigWarning {
	var warnings []ConfigWarning

	// Check validator count vs quorum
	n := c.Validators.Count()
	f := c.Validators.F()
	quorum := 2*f + 1

	if n < 4 {
		warnings = append(warnings, ConfigWarning{
			Field:      "Validators",
			Message:    fmt.Sprintf("only %d validators cannot tolerate any failures (need n >= 4 for f >= 1)", n),
			Suggestion: "use at least 4 validators for fault tolerance",
		})
	}

	// Check if quorum is achievable
	if n < quorum {
		warnings = append(warnings, ConfigWarning{
			Field:      "Validators",
			Message:    fmt.Sprintf("validator count %d is less than quorum %d", n, quorum),
			Suggestion: "this should never happen; check ValidatorSet.F() implementation",
		})
	}

	// Batch size warnings
	if c.BatchSize < 10 {
		warnings = append(warnings, ConfigWarning{
			Field:      "BatchSize",
			Message:    fmt.Sprintf("batch size %d is very small, increasing header overhead", c.BatchSize),
			Suggestion: "use BatchSize >= 100 for production workloads",
		})
	}
	if c.BatchSize > 10000 {
		warnings = append(warnings, ConfigWarning{
			Field:      "BatchSize",
			Message:    fmt.Sprintf("batch size %d is very large, may cause latency spikes", c.BatchSize),
			Suggestion: "use BatchSize <= 5000 for consistent latency",
		})
	}

	// Batch timeout warnings
	if c.BatchTimeout < 10*time.Millisecond {
		warnings = append(warnings, ConfigWarning{
			Field:      "BatchTimeout",
			Message:    fmt.Sprintf("batch timeout %v is very short, may create many small batches", c.BatchTimeout),
			Suggestion: "use BatchTimeout >= 50ms to reduce overhead",
		})
	}
	if c.BatchTimeout > 5*time.Second {
		warnings = append(warnings, ConfigWarning{
			Field:      "BatchTimeout",
			Message:    fmt.Sprintf("batch timeout %v is very long, may cause high latency", c.BatchTimeout),
			Suggestion: "use BatchTimeout <= 1s for responsive transaction processing",
		})
	}

	// Header timeout warnings
	if c.HeaderTimeout < 50*time.Millisecond {
		warnings = append(warnings, ConfigWarning{
			Field:      "HeaderTimeout",
			Message:    fmt.Sprintf("header timeout %v is very short, may not allow vote collection", c.HeaderTimeout),
			Suggestion: "use HeaderTimeout >= 100ms to allow quorum formation",
		})
	}
	if c.HeaderTimeout > 10*time.Second {
		warnings = append(warnings, ConfigWarning{
			Field:      "HeaderTimeout",
			Message:    fmt.Sprintf("header timeout %v is very long, may cause slow round progression", c.HeaderTimeout),
			Suggestion: "use HeaderTimeout <= 2s for reasonable throughput",
		})
	}

	// Header timeout vs batch timeout relationship
	if c.BatchTimeout > c.HeaderTimeout {
		warnings = append(warnings, ConfigWarning{
			Field:      "BatchTimeout/HeaderTimeout",
			Message:    fmt.Sprintf("batch timeout (%v) > header timeout (%v), batches may not be included in headers", c.BatchTimeout, c.HeaderTimeout),
			Suggestion: "set BatchTimeout < HeaderTimeout for timely batch inclusion",
		})
	}

	// GC configuration
	if c.GCDepth < 10 {
		warnings = append(warnings, ConfigWarning{
			Field:      "GCDepth",
			Message:    fmt.Sprintf("GC depth %d is very small, may delete data needed for sync", c.GCDepth),
			Suggestion: "use GCDepth >= 20 to allow time for lagging nodes to catch up",
		})
	}
	if c.GCDepth > 1000 {
		warnings = append(warnings, ConfigWarning{
			Field:      "GCDepth",
			Message:    fmt.Sprintf("GC depth %d retains significant data, increasing memory/storage usage", c.GCDepth),
			Suggestion: "use GCDepth <= 100 unless consensus requires deep history",
		})
	}
	if c.GCInterval > c.GCDepth {
		warnings = append(warnings, ConfigWarning{
			Field:      "GCInterval/GCDepth",
			Message:    fmt.Sprintf("GC runs every %d rounds but retains only %d rounds; GC may not free memory", c.GCInterval, c.GCDepth),
			Suggestion: "set GCInterval <= GCDepth for effective cleanup",
		})
	}

	// Backpressure configuration
	if c.MaxPendingTransactions < 100 {
		warnings = append(warnings, ConfigWarning{
			Field:      "MaxPendingTransactions",
			Message:    fmt.Sprintf("max pending transactions %d is very small, may cause frequent blocking/drops", c.MaxPendingTransactions),
			Suggestion: "use MaxPendingTransactions >= 1000 for smoother operation",
		})
	}
	if c.MaxPendingBatches < 10 {
		warnings = append(warnings, ConfigWarning{
			Field:      "MaxPendingBatches",
			Message:    fmt.Sprintf("max pending batches %d is very small, may cause worker stalls", c.MaxPendingBatches),
			Suggestion: "use MaxPendingBatches >= 100 for consistent throughput",
		})
	}

	// SyncRetryNodes validation
	if c.SyncRetryNodes > n-1 {
		warnings = append(warnings, ConfigWarning{
			Field:      "SyncRetryNodes",
			Message:    fmt.Sprintf("SyncRetryNodes %d exceeds available peers (%d)", c.SyncRetryNodes, n-1),
			Suggestion: fmt.Sprintf("set SyncRetryNodes <= %d", n-1),
		})
	}

	// Crypto provider recommendations for large validator sets
	if n > 20 && c.CryptoProvider == nil {
		warnings = append(warnings, ConfigWarning{
			Field:      "CryptoProvider",
			Message:    fmt.Sprintf("no crypto provider with %d validators; signature verification may be slow", n),
			Suggestion: "use WithCryptoProvider for parallel/batch signature verification",
		})
	}

	// Signature cache recommendation when crypto provider is set
	if c.CryptoProvider != nil && c.SignatureCache == nil {
		warnings = append(warnings, ConfigWarning{
			Field:      "SignatureCache",
			Message:    "CryptoProvider set but no SignatureCache; duplicate verifications not cached",
			Suggestion: "use WithSignatureCache for better performance",
		})
	}

	// MaxRoundsGap vs network latency
	if c.MaxRoundsGap < 3 {
		warnings = append(warnings, ConfigWarning{
			Field:      "MaxRoundsGap",
			Message:    fmt.Sprintf("max rounds gap %d is very small, may reject valid headers during network delays", c.MaxRoundsGap),
			Suggestion: "use MaxRoundsGap >= 5 to tolerate network jitter",
		})
	}

	// HeaderWaiter configuration
	if c.HeaderWaiterMaxAge < c.SyncRetryDelay {
		warnings = append(warnings, ConfigWarning{
			Field:      "HeaderWaiterMaxAge",
			Message:    fmt.Sprintf("header max age (%v) < sync retry delay (%v); headers may be dropped before sync completes", c.HeaderWaiterMaxAge, c.SyncRetryDelay),
			Suggestion: "set HeaderWaiterMaxAge >= SyncRetryDelay * 2",
		})
	}

	// Worker count vs CPU
	if c.WorkerCount > 16 {
		warnings = append(warnings, ConfigWarning{
			Field:      "WorkerCount",
			Message:    fmt.Sprintf("worker count %d is very high; diminishing returns beyond CPU core count", c.WorkerCount),
			Suggestion: "set WorkerCount to approximate CPU core count",
		})
	}

	return warnings
}

// LogWarnings logs all configuration warnings.
func (c *Config[H, T]) LogWarnings() {
	for _, w := range c.Warnings() {
		c.Logger.Warn("suboptimal configuration",
			zap.String("field", w.Field),
			zap.String("message", w.Message),
			zap.String("suggestion", w.Suggestion),
		)
	}
}

// WithMyIndex sets this validator's index in the validator set.
func WithMyIndex[H Hash, T Transaction[H]](index uint16) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		c.MyIndex = index
		return nil
	}
}

// WithValidators sets the validator set.
// This is a required option.
func WithValidators[H Hash, T Transaction[H]](validators ValidatorSet) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if validators == nil {
			return fmt.Errorf("validators cannot be nil")
		}
		c.Validators = validators
		return nil
	}
}

// WithSigner sets the cryptographic signer.
// This is a required option.
func WithSigner[H Hash, T Transaction[H]](signer Signer) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if signer == nil {
			return fmt.Errorf("signer cannot be nil")
		}
		c.Signer = signer
		return nil
	}
}

// WithStorage sets the persistent storage backend.
// This is a required option.
func WithStorage[H Hash, T Transaction[H]](storage Storage[H, T]) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if storage == nil {
			return fmt.Errorf("storage cannot be nil")
		}
		c.Storage = storage
		return nil
	}
}

// WithNetwork sets the network layer for message delivery.
// This is a required option.
func WithNetwork[H Hash, T Transaction[H]](network Network[H, T]) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if network == nil {
			return fmt.Errorf("network cannot be nil")
		}
		c.Network = network
		return nil
	}
}

// WithTimer sets the timer for timeout management.
// This is a required option.
func WithTimer[H Hash, T Transaction[H]](timer Timer) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if timer == nil {
			return fmt.Errorf("timer cannot be nil")
		}
		c.Timer = timer
		return nil
	}
}

// WithLogger sets the structured logger.
// If not provided, a no-op logger is used.
func WithLogger[H Hash, T Transaction[H]](logger *zap.Logger) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if logger == nil {
			return fmt.Errorf("logger cannot be nil")
		}
		c.Logger = logger
		return nil
	}
}

// WithWorkerCount sets the number of workers for parallel batch creation.
// More workers can increase throughput but use more resources.
// Default: 4
func WithWorkerCount[H Hash, T Transaction[H]](count int) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if count < 1 {
			return fmt.Errorf("worker count must be at least 1, got %d", count)
		}
		c.WorkerCount = count
		return nil
	}
}

// WithBatchSize sets the maximum number of transactions per batch.
// Larger batches improve throughput but increase latency.
// Default: 500
func WithBatchSize[H Hash, T Transaction[H]](size int) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if size < 1 {
			return fmt.Errorf("batch size must be at least 1, got %d", size)
		}
		c.BatchSize = size
		return nil
	}
}

// WithBatchTimeout sets the maximum time to wait before creating a batch.
// Shorter timeouts reduce latency but may create smaller batches.
// Default: 100ms
func WithBatchTimeout[H Hash, T Transaction[H]](timeout time.Duration) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if timeout <= 0 {
			return fmt.Errorf("batch timeout must be positive, got %v", timeout)
		}
		c.BatchTimeout = timeout
		return nil
	}
}

// WithHeaderTimeout sets the interval between header creation attempts.
// Shorter intervals can improve throughput under high load.
// Default: 500ms
func WithHeaderTimeout[H Hash, T Transaction[H]](timeout time.Duration) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if timeout <= 0 {
			return fmt.Errorf("header timeout must be positive, got %v", timeout)
		}
		c.HeaderTimeout = timeout
		return nil
	}
}

// WithGCInterval sets the number of rounds between garbage collection runs.
// Lower values free memory sooner but may impact performance.
// Default: 50
func WithGCInterval[H Hash, T Transaction[H]](interval uint64) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if interval == 0 {
			return fmt.Errorf("GC interval must be positive")
		}
		c.GCInterval = interval
		return nil
	}
}

// WithMaxRoundsGap sets the maximum rounds ahead we accept headers for.
// Headers for rounds beyond currentRound + MaxRoundsGap are rejected.
// Default: 10
func WithMaxRoundsGap[H Hash, T Transaction[H]](gap uint64) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if gap == 0 {
			return fmt.Errorf("max rounds gap must be positive")
		}
		c.MaxRoundsGap = gap
		return nil
	}
}

// WithHooks sets the observability hooks.
// Hooks provide callbacks for metrics, logging, and monitoring integration.
// All hooks are optional - nil hooks are ignored.
func WithHooks[H Hash, T Transaction[H]](hooks *Hooks[H, T]) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		c.Hooks = hooks
		return nil
	}
}

// WithMaxPendingTransactions sets the maximum transactions queued per worker.
// When this limit is reached, behavior depends on DropOnFull setting.
// Default: 10000
func WithMaxPendingTransactions[H Hash, T Transaction[H]](max int) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if max < 1 {
			return fmt.Errorf("max pending transactions must be at least 1, got %d", max)
		}
		c.MaxPendingTransactions = max
		return nil
	}
}

// WithMaxPendingBatches sets the maximum batches queued in the primary.
// Default: 1000
func WithMaxPendingBatches[H Hash, T Transaction[H]](max int) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if max < 1 {
			return fmt.Errorf("max pending batches must be at least 1, got %d", max)
		}
		c.MaxPendingBatches = max
		return nil
	}
}

// WithDropOnFull sets the behavior when queues are full.
// If true, new items are dropped silently. If false, operations block.
// Default: false (block)
func WithDropOnFull[H Hash, T Transaction[H]](drop bool) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		c.DropOnFull = drop
		return nil
	}
}

// WithSyncRetryDelay sets the delay before retrying sync requests.
// Default: 10s
func WithSyncRetryDelay[H Hash, T Transaction[H]](delay time.Duration) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if delay <= 0 {
			return fmt.Errorf("sync retry delay must be positive, got %v", delay)
		}
		c.SyncRetryDelay = delay
		return nil
	}
}

// WithSyncRetryNodes sets the number of nodes to contact when syncing.
// Default: 3
func WithSyncRetryNodes[H Hash, T Transaction[H]](nodes int) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if nodes < 1 {
			return fmt.Errorf("sync retry nodes must be at least 1, got %d", nodes)
		}
		c.SyncRetryNodes = nodes
		return nil
	}
}

// WithGCDepth sets the number of rounds to retain after committed round.
// Default: 100
func WithGCDepth[H Hash, T Transaction[H]](depth uint64) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if depth == 0 {
			return fmt.Errorf("GC depth must be positive")
		}
		c.GCDepth = depth
		return nil
	}
}

// WithMaxHeaderBatches sets the maximum batch refs to include in a header.
// Headers are created when this limit is reached.
// Default: 100
func WithMaxHeaderBatches[H Hash, T Transaction[H]](max int) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if max < 1 {
			return fmt.Errorf("max header batches must be at least 1, got %d", max)
		}
		c.MaxHeaderBatches = max
		return nil
	}
}

// WithMaxHeaderDelay sets the maximum time before creating a header.
// Headers are created after this duration even if MaxHeaderBatches isn't reached.
// Default: HeaderTimeout value
func WithMaxHeaderDelay[H Hash, T Transaction[H]](delay time.Duration) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if delay <= 0 {
			return fmt.Errorf("max header delay must be positive, got %v", delay)
		}
		c.MaxHeaderDelay = delay
		return nil
	}
}

// WithMaxPendingHeaders sets the maximum headers with missing parents to queue.
// Default: 1000
func WithMaxPendingHeaders[H Hash, T Transaction[H]](max int) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if max < 1 {
			return fmt.Errorf("max pending headers must be at least 1, got %d", max)
		}
		c.MaxPendingHeaders = max
		return nil
	}
}

// WithHeaderWaiterRetryInterval sets how often to retry pending headers.
// Default: 1s
func WithHeaderWaiterRetryInterval[H Hash, T Transaction[H]](interval time.Duration) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if interval <= 0 {
			return fmt.Errorf("header waiter retry interval must be positive, got %v", interval)
		}
		c.HeaderWaiterRetryInterval = interval
		return nil
	}
}

// WithHeaderWaiterMaxRetries sets the max retries before dropping a pending header.
// Default: 10
func WithHeaderWaiterMaxRetries[H Hash, T Transaction[H]](max int) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if max < 1 {
			return fmt.Errorf("header waiter max retries must be at least 1, got %d", max)
		}
		c.HeaderWaiterMaxRetries = max
		return nil
	}
}

// WithHeaderWaiterMaxAge sets the maximum age before dropping a pending header.
// Default: 30s
func WithHeaderWaiterMaxAge[H Hash, T Transaction[H]](age time.Duration) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if age <= 0 {
			return fmt.Errorf("header waiter max age must be positive, got %v", age)
		}
		c.HeaderWaiterMaxAge = age
		return nil
	}
}

// WithCryptoProvider sets the crypto provider for optimized signature verification.
// When set, certificate validation uses batch/parallel verification.
// Default: nil (uses standard sequential verification)
func WithCryptoProvider[H Hash, T Transaction[H]](crypto CryptoProvider) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		c.CryptoProvider = crypto
		return nil
	}
}

// WithSignatureCache sets the signature cache for avoiding re-verification.
// Only effective when CryptoProvider is also set.
// Default: nil (no caching)
func WithSignatureCache[H Hash, T Transaction[H]](cache *SignatureCache) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		c.SignatureCache = cache
		return nil
	}
}

// WithValidation sets the validation configuration.
// Controls limits for DoS prevention (max batch refs, max transactions, etc.)
// Default: DefaultValidationConfig()
func WithValidation[H Hash, T Transaction[H]](cfg ValidationConfig) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		c.Validation = cfg
		return nil
	}
}

// WithRecommendedMessageQueueSize sets the advisory buffer size for Network receive channels.
func WithRecommendedMessageQueueSize[H Hash, T Transaction[H]](size int) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if size < 1 {
			return fmt.Errorf("recommended message queue size must be at least 1, got %d", size)
		}
		c.RecommendedMessageQueueSize = size
		return nil
	}
}

// WithDAGCache configures the LRU cache for DAG vertex lookups.
// When enabled, frequently accessed vertices are cached for faster retrieval.
// Default: DAGCacheConfig{Enabled: true, Capacity: 10000}
func WithDAGCache[H Hash, T Transaction[H]](cfg DAGCacheConfig) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if cfg.Enabled && cfg.Capacity < 1 {
			cfg.Capacity = 10000 // Use default capacity
		}
		c.DAGCache = cfg
		return nil
	}
}

// WithDAGCacheCapacity enables DAG caching with the specified capacity.
// This is a convenience method equivalent to WithDAGCache(DAGCacheConfig{Enabled: true, Capacity: capacity}).
func WithDAGCacheCapacity[H Hash, T Transaction[H]](capacity int) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		if capacity < 1 {
			return fmt.Errorf("DAG cache capacity must be at least 1, got %d", capacity)
		}
		c.DAGCache = DAGCacheConfig{Enabled: true, Capacity: capacity}
		return nil
	}
}

// WithDAGCacheDisabled disables DAG vertex caching.
// Use this if memory is constrained or if your access pattern doesn't benefit from caching.
func WithDAGCacheDisabled[H Hash, T Transaction[H]]() ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		c.DAGCache = DAGCacheConfig{Enabled: false}
		return nil
	}
}

// WithNetworkModel sets the network timing assumptions.
// Use NetworkModelAsynchronous (default) for optimal throughput.
// Use NetworkModelPartiallySynchronous for better commit latency with leader awareness.
func WithNetworkModel[H Hash, T Transaction[H]](model NetworkModel) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		c.NetworkModel = model
		return nil
	}
}

// WithLeaderSchedule sets the leader schedule for partially synchronous mode.
// If not set and NetworkModel is PartialSync, a round-robin schedule is created.
func WithLeaderSchedule[H Hash, T Transaction[H]](schedule LeaderSchedule) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		c.LeaderSchedule = schedule
		return nil
	}
}

// WithEpoch sets the initial epoch for the validator set.
// The epoch is used for reconfiguration and vote tracking.
// Default: 0
func WithEpoch[H Hash, T Transaction[H]](epoch uint64) ConfigOption[H, T] {
	return func(c *Config[H, T]) error {
		c.Epoch = epoch
		return nil
	}
}

// Preset configurations for common use cases

// DefaultConfig returns a configuration suitable for most use cases.
// Balances throughput and latency for typical blockchain workloads.
//
// Settings:
//   - WorkerCount: 4
//   - BatchSize: 500
//   - BatchTimeout: 100ms
//   - HeaderTimeout: 500ms
//   - GCInterval: 100
//   - MaxRoundsGap: 10
func DefaultConfig[H Hash, T Transaction[H]]() Config[H, T] {
	return Config[H, T]{
		Logger:        zap.NewNop(),
		WorkerCount:   4,
		BatchSize:     500,
		BatchTimeout:  100 * time.Millisecond,
		HeaderTimeout: 500 * time.Millisecond,
		GCInterval:    100,
		MaxRoundsGap:  10,
	}
}

// HighThroughputConfig returns a configuration optimized for maximum throughput.
// Uses more workers and larger batches, suitable for high-volume systems.
//
// Settings:
//   - WorkerCount: 8
//   - BatchSize: 1000
//   - BatchTimeout: 50ms
//   - HeaderTimeout: 200ms
//   - GCInterval: 50
//   - MaxRoundsGap: 5
func HighThroughputConfig[H Hash, T Transaction[H]]() Config[H, T] {
	return Config[H, T]{
		Logger:        zap.NewNop(),
		WorkerCount:   8,
		BatchSize:     1000,
		BatchTimeout:  50 * time.Millisecond,
		HeaderTimeout: 200 * time.Millisecond,
		GCInterval:    50,
		MaxRoundsGap:  5,
	}
}

// LowLatencyConfig returns a configuration optimized for low latency.
// Uses smaller batches and shorter timeouts, suitable for interactive applications.
//
// Settings:
//   - WorkerCount: 2
//   - BatchSize: 100
//   - BatchTimeout: 20ms
//   - HeaderTimeout: 100ms
//   - GCInterval: 100
//   - MaxRoundsGap: 10
func LowLatencyConfig[H Hash, T Transaction[H]]() Config[H, T] {
	return Config[H, T]{
		Logger:        zap.NewNop(),
		WorkerCount:   2,
		BatchSize:     100,
		BatchTimeout:  20 * time.Millisecond,
		HeaderTimeout: 100 * time.Millisecond,
		GCInterval:    100,
		MaxRoundsGap:  10,
	}
}

// DemoConfig returns a configuration suitable for demonstrations and testing.
// Uses visible timing to make the protocol easier to observe.
//
// Settings:
//   - WorkerCount: 2
//   - BatchSize: 10
//   - BatchTimeout: 1s
//   - HeaderTimeout: 2s
//   - GCInterval: 50
//   - MaxRoundsGap: 10
func DemoConfig[H Hash, T Transaction[H]]() Config[H, T] {
	return Config[H, T]{
		Logger:        zap.NewNop(),
		WorkerCount:   2,
		BatchSize:     10,
		BatchTimeout:  1 * time.Second,
		HeaderTimeout: 2 * time.Second,
		GCInterval:    50,
		MaxRoundsGap:  10,
	}
}
