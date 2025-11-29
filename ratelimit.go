package narwhal

import (
	"sync"
	"sync/atomic"
	"time"
)

// RateLimiter provides a token bucket rate limiter. Thread-safe.
type RateLimiter struct {
	// Tokens are refilled at this rate (tokens per second)
	rate float64

	// Maximum tokens that can accumulate
	burst int64

	// Current tokens available
	tokens float64

	// Last time tokens were updated
	lastUpdate time.Time

	mu sync.Mutex

	// Stats
	allowed  atomic.Uint64
	rejected atomic.Uint64
}

// NewRateLimiter creates a new rate limiter.
// - rate: tokens per second to add
// - burst: maximum tokens that can accumulate
func NewRateLimiter(rate float64, burst int) *RateLimiter {
	return &RateLimiter{
		rate:       rate,
		burst:      int64(burst),
		tokens:     float64(burst), // Start full
		lastUpdate: time.Now(),
	}
}

// Allow checks if one token is available and consumes it if so.
// Returns true if allowed, false if rate limited.
func (r *RateLimiter) Allow() bool {
	return r.AllowN(1)
}

// AllowN checks if n tokens are available and consumes them if so.
// Returns true if allowed, false if rate limited.
func (r *RateLimiter) AllowN(n int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(r.lastUpdate).Seconds()
	r.lastUpdate = now

	// Add tokens based on elapsed time
	r.tokens += elapsed * r.rate
	if r.tokens > float64(r.burst) {
		r.tokens = float64(r.burst)
	}

	// Check if we have enough tokens
	if r.tokens >= float64(n) {
		r.tokens -= float64(n)
		r.allowed.Add(1)
		return true
	}

	r.rejected.Add(1)
	return false
}

// Reserve reserves n tokens, returning the time to wait before they're available.
// If wait is 0, the tokens were immediately available.
// If wait is negative, the request can never be satisfied (n > burst).
func (r *RateLimiter) Reserve(n int) time.Duration {
	if n > int(r.burst) {
		return -1 // Can never satisfy
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(r.lastUpdate).Seconds()
	r.lastUpdate = now

	// Add tokens
	r.tokens += elapsed * r.rate
	if r.tokens > float64(r.burst) {
		r.tokens = float64(r.burst)
	}

	// If we have enough, consume and return 0
	if r.tokens >= float64(n) {
		r.tokens -= float64(n)
		return 0
	}

	// Calculate wait time
	needed := float64(n) - r.tokens
	wait := time.Duration(needed / r.rate * float64(time.Second))
	r.tokens = 0 // Reserve all current tokens

	return wait
}

// Stats returns rate limiter statistics.
type RateLimiterStats struct {
	Allowed  uint64
	Rejected uint64
	Rate     float64
	Burst    int64
}

// Stats returns current statistics.
func (r *RateLimiter) Stats() RateLimiterStats {
	return RateLimiterStats{
		Allowed:  r.allowed.Load(),
		Rejected: r.rejected.Load(),
		Rate:     r.rate,
		Burst:    r.burst,
	}
}

// PerPeerRateLimiter maintains separate rate limits for each peer.
type PerPeerRateLimiter struct {
	mu       sync.RWMutex
	limiters map[uint16]*RateLimiter
	rate     float64
	burst    int
}

// NewPerPeerRateLimiter creates a rate limiter that tracks per-peer limits.
func NewPerPeerRateLimiter(rate float64, burst int) *PerPeerRateLimiter {
	return &PerPeerRateLimiter{
		limiters: make(map[uint16]*RateLimiter),
		rate:     rate,
		burst:    burst,
	}
}

// Allow checks if the peer is within rate limits.
func (p *PerPeerRateLimiter) Allow(peer uint16) bool {
	return p.AllowN(peer, 1)
}

// AllowN checks if n tokens are available for the peer.
func (p *PerPeerRateLimiter) AllowN(peer uint16, n int) bool {
	limiter := p.getLimiter(peer)
	return limiter.AllowN(n)
}

func (p *PerPeerRateLimiter) getLimiter(peer uint16) *RateLimiter {
	p.mu.RLock()
	limiter, exists := p.limiters[peer]
	p.mu.RUnlock()

	if exists {
		return limiter
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring write lock
	if limiter, exists = p.limiters[peer]; exists {
		return limiter
	}

	limiter = NewRateLimiter(p.rate, p.burst)
	p.limiters[peer] = limiter
	return limiter
}

// Reset removes all peer state.
func (p *PerPeerRateLimiter) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.limiters = make(map[uint16]*RateLimiter)
}

// CircuitBreaker implements the circuit breaker pattern for network operations.
// It prevents repeated failures from overwhelming the system.
type CircuitBreaker struct {
	// Configuration
	failureThreshold int           // Failures before opening
	successThreshold int           // Successes to close from half-open
	timeout          time.Duration // Time before half-open from open

	// State
	state         circuitState
	failures      int
	successes     int
	lastFailure   time.Time
	lastStateTime time.Time

	mu sync.Mutex

	// Stats
	totalFailures  atomic.Uint64
	totalSuccesses atomic.Uint64
	totalRejected  atomic.Uint64
}

type circuitState int

const (
	circuitClosed circuitState = iota
	circuitOpen
	circuitHalfOpen
)

// CircuitBreakerConfig configures the circuit breaker.
type CircuitBreakerConfig struct {
	// FailureThreshold is the number of failures before opening the circuit.
	// Default: 5
	FailureThreshold int

	// SuccessThreshold is the number of successes needed to close from half-open.
	// Default: 2
	SuccessThreshold int

	// Timeout is how long to wait before trying again (moving to half-open).
	// Default: 30s
	Timeout time.Duration
}

// DefaultCircuitBreakerConfig returns sensible defaults.
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		Timeout:          30 * time.Second,
	}
}

// NewCircuitBreaker creates a new circuit breaker.
func NewCircuitBreaker(cfg CircuitBreakerConfig) *CircuitBreaker {
	if cfg.FailureThreshold == 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.SuccessThreshold == 0 {
		cfg.SuccessThreshold = 2
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}

	return &CircuitBreaker{
		failureThreshold: cfg.FailureThreshold,
		successThreshold: cfg.SuccessThreshold,
		timeout:          cfg.Timeout,
		state:            circuitClosed,
		lastStateTime:    time.Now(),
	}
}

// Allow checks if a request should be allowed.
// Returns true if allowed, false if the circuit is open.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case circuitClosed:
		return true

	case circuitOpen:
		// Check if timeout has elapsed
		if time.Since(cb.lastStateTime) >= cb.timeout {
			cb.state = circuitHalfOpen
			cb.successes = 0
			cb.lastStateTime = time.Now()
			return true // Allow one request to test
		}
		cb.totalRejected.Add(1)
		return false

	case circuitHalfOpen:
		return true // Allow requests in half-open to test the service

	default:
		return true
	}
}

// RecordSuccess records a successful operation.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.totalSuccesses.Add(1)

	switch cb.state {
	case circuitHalfOpen:
		cb.successes++
		if cb.successes >= cb.successThreshold {
			cb.state = circuitClosed
			cb.failures = 0
			cb.lastStateTime = time.Now()
		}
	case circuitClosed:
		// Reset failure count on success
		cb.failures = 0
	}
}

// RecordFailure records a failed operation.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.totalFailures.Add(1)
	cb.lastFailure = time.Now()

	switch cb.state {
	case circuitClosed:
		cb.failures++
		if cb.failures >= cb.failureThreshold {
			cb.state = circuitOpen
			cb.lastStateTime = time.Now()
		}
	case circuitHalfOpen:
		// Any failure in half-open opens the circuit
		cb.state = circuitOpen
		cb.lastStateTime = time.Now()
	}
}

// State returns the current circuit state as a string.
func (cb *CircuitBreaker) State() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case circuitClosed:
		return "closed"
	case circuitOpen:
		return "open"
	case circuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// CircuitBreakerStats contains circuit breaker statistics.
type CircuitBreakerStats struct {
	State          string
	Failures       int
	Successes      int
	TotalFailures  uint64
	TotalSuccesses uint64
	TotalRejected  uint64
	LastFailure    time.Time
}

// Stats returns current statistics.
func (cb *CircuitBreaker) Stats() CircuitBreakerStats {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// Inline the state string conversion to avoid deadlock
	var stateStr string
	switch cb.state {
	case circuitClosed:
		stateStr = "closed"
	case circuitOpen:
		stateStr = "open"
	case circuitHalfOpen:
		stateStr = "half-open"
	default:
		stateStr = "unknown"
	}

	return CircuitBreakerStats{
		State:          stateStr,
		Failures:       cb.failures,
		Successes:      cb.successes,
		TotalFailures:  cb.totalFailures.Load(),
		TotalSuccesses: cb.totalSuccesses.Load(),
		TotalRejected:  cb.totalRejected.Load(),
		LastFailure:    cb.lastFailure,
	}
}

// Reset resets the circuit breaker to closed state.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.state = circuitClosed
	cb.failures = 0
	cb.successes = 0
	cb.lastStateTime = time.Now()
}
