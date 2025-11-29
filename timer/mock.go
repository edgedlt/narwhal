package timer

import (
	"sync"
)

// MockTimer is a timer for testing that can be triggered manually.
type MockTimer struct {
	mu      sync.Mutex
	c       chan struct{}
	started bool
	stopped bool
}

// NewMockTimer creates a new MockTimer.
func NewMockTimer() *MockTimer {
	return &MockTimer{
		c: make(chan struct{}, 1),
	}
}

// Start starts the mock timer.
func (t *MockTimer) Start() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.started = true
	t.stopped = false
}

// Stop stops the mock timer.
func (t *MockTimer) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stopped = true
}

// Reset resets the mock timer.
func (t *MockTimer) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.started = true
	t.stopped = false
}

// C returns the timer channel.
func (t *MockTimer) C() <-chan struct{} {
	return t.c
}

// Fire triggers the timer, simulating a timeout.
// This is the test helper method.
func (t *MockTimer) Fire() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.started && !t.stopped {
		select {
		case t.c <- struct{}{}:
		default:
		}
	}
}

// IsStarted returns true if the timer has been started.
func (t *MockTimer) IsStarted() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.started
}

// IsStopped returns true if the timer has been stopped.
func (t *MockTimer) IsStopped() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stopped
}
