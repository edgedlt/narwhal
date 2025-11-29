package narwhal

import (
	"context"
	"runtime/debug"

	"go.uber.org/zap"
)

// PanicHandler is called when a panic is recovered.
// It receives the panic value and stack trace.
type PanicHandler func(panicVal interface{}, stack []byte)

// RecoveryConfig configures panic recovery behavior.
type RecoveryConfig struct {
	// Handler is called when a panic is recovered.
	// If nil, panics are logged and the goroutine terminates cleanly.
	Handler PanicHandler

	// Logger for recording recovered panics.
	Logger *zap.Logger

	// Rethrow causes the panic to be re-raised after handling.
	// Use this if you want panics to propagate after logging.
	Rethrow bool
}

// GoWithRecovery starts a goroutine with panic recovery.
func GoWithRecovery(cfg RecoveryConfig, fn func()) {
	go func() {
		defer RecoverPanic(cfg)
		fn()
	}()
}

// GoWithRecoveryCtx starts a goroutine with panic recovery and context.
// The function receives the context and can check for cancellation.
func GoWithRecoveryCtx(ctx context.Context, cfg RecoveryConfig, fn func(context.Context)) {
	go func() {
		defer RecoverPanic(cfg)
		fn(ctx)
	}()
}

// RecoverPanic recovers from panics and handles them according to config.
// Use as: defer RecoverPanic(cfg)
func RecoverPanic(cfg RecoveryConfig) {
	if r := recover(); r != nil {
		stack := debug.Stack()

		if cfg.Logger != nil {
			cfg.Logger.Error("recovered panic",
				zap.Any("panic", r),
				zap.ByteString("stack", stack))
		}

		if cfg.Handler != nil {
			cfg.Handler(r, stack)
		}

		if cfg.Rethrow {
			panic(r)
		}
	}
}

// SafeGo starts a goroutine with panic recovery using a simple logger.
// This is a convenience wrapper for common cases.
func SafeGo(logger *zap.Logger, fn func()) {
	GoWithRecovery(RecoveryConfig{Logger: logger}, fn)
}

// SafeGoCtx starts a goroutine with panic recovery using a simple logger and context.
func SafeGoCtx(ctx context.Context, logger *zap.Logger, fn func(context.Context)) {
	GoWithRecoveryCtx(ctx, RecoveryConfig{Logger: logger}, fn)
}

// NewRecoveryMiddleware wraps hooks with panic recovery.
func NewRecoveryMiddleware[H Hash, T Transaction[H]](hooks *Hooks[H, T], logger *zap.Logger) *Hooks[H, T] {
	if hooks == nil {
		return nil
	}

	wrapped := hooks.Clone()

	// Wrap each hook with recovery
	if wrapped.OnBatchCreated != nil {
		original := wrapped.OnBatchCreated
		wrapped.OnBatchCreated = func(e BatchCreatedEvent[H, T]) {
			defer RecoverPanic(RecoveryConfig{Logger: logger})
			original(e)
		}
	}

	if wrapped.OnTransactionReceived != nil {
		original := wrapped.OnTransactionReceived
		wrapped.OnTransactionReceived = func(e TransactionReceivedEvent[H]) {
			defer RecoverPanic(RecoveryConfig{Logger: logger})
			original(e)
		}
	}

	if wrapped.OnHeaderCreated != nil {
		original := wrapped.OnHeaderCreated
		wrapped.OnHeaderCreated = func(e HeaderCreatedEvent[H]) {
			defer RecoverPanic(RecoveryConfig{Logger: logger})
			original(e)
		}
	}

	if wrapped.OnHeaderReceived != nil {
		original := wrapped.OnHeaderReceived
		wrapped.OnHeaderReceived = func(e HeaderReceivedEvent[H]) {
			defer RecoverPanic(RecoveryConfig{Logger: logger})
			original(e)
		}
	}

	if wrapped.OnVoteReceived != nil {
		original := wrapped.OnVoteReceived
		wrapped.OnVoteReceived = func(e VoteReceivedEvent[H]) {
			defer RecoverPanic(RecoveryConfig{Logger: logger})
			original(e)
		}
	}

	if wrapped.OnVoteSent != nil {
		original := wrapped.OnVoteSent
		wrapped.OnVoteSent = func(e VoteSentEvent[H]) {
			defer RecoverPanic(RecoveryConfig{Logger: logger})
			original(e)
		}
	}

	if wrapped.OnCertificateFormed != nil {
		original := wrapped.OnCertificateFormed
		wrapped.OnCertificateFormed = func(e CertificateFormedEvent[H]) {
			defer RecoverPanic(RecoveryConfig{Logger: logger})
			original(e)
		}
	}

	if wrapped.OnCertificateReceived != nil {
		original := wrapped.OnCertificateReceived
		wrapped.OnCertificateReceived = func(e CertificateReceivedEvent[H]) {
			defer RecoverPanic(RecoveryConfig{Logger: logger})
			original(e)
		}
	}

	if wrapped.OnHeaderTimeout != nil {
		original := wrapped.OnHeaderTimeout
		wrapped.OnHeaderTimeout = func(e HeaderTimeoutEvent[H]) {
			defer RecoverPanic(RecoveryConfig{Logger: logger})
			original(e)
		}
	}

	if wrapped.OnVertexInserted != nil {
		original := wrapped.OnVertexInserted
		wrapped.OnVertexInserted = func(e VertexInsertedEvent[H]) {
			defer RecoverPanic(RecoveryConfig{Logger: logger})
			original(e)
		}
	}

	if wrapped.OnRoundAdvanced != nil {
		original := wrapped.OnRoundAdvanced
		wrapped.OnRoundAdvanced = func(e RoundAdvancedEvent) {
			defer RecoverPanic(RecoveryConfig{Logger: logger})
			original(e)
		}
	}

	if wrapped.OnEquivocationDetected != nil {
		original := wrapped.OnEquivocationDetected
		wrapped.OnEquivocationDetected = func(e EquivocationDetectedEvent[H]) {
			defer RecoverPanic(RecoveryConfig{Logger: logger})
			original(e)
		}
	}

	if wrapped.OnCertificatePending != nil {
		original := wrapped.OnCertificatePending
		wrapped.OnCertificatePending = func(e CertificatePendingEvent[H]) {
			defer RecoverPanic(RecoveryConfig{Logger: logger})
			original(e)
		}
	}

	if wrapped.OnFetchStarted != nil {
		original := wrapped.OnFetchStarted
		wrapped.OnFetchStarted = func(e FetchStartedEvent[H]) {
			defer RecoverPanic(RecoveryConfig{Logger: logger})
			original(e)
		}
	}

	if wrapped.OnFetchCompleted != nil {
		original := wrapped.OnFetchCompleted
		wrapped.OnFetchCompleted = func(e FetchCompletedEvent[H]) {
			defer RecoverPanic(RecoveryConfig{Logger: logger})
			original(e)
		}
	}

	if wrapped.OnGarbageCollected != nil {
		original := wrapped.OnGarbageCollected
		wrapped.OnGarbageCollected = func(e GarbageCollectedEvent) {
			defer RecoverPanic(RecoveryConfig{Logger: logger})
			original(e)
		}
	}

	return wrapped
}
