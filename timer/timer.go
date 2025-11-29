// Package timer provides timer implementations for Narwhal.
package timer

import (
	"sync"
	"time"
)

// Timer provides timeout management for batch and header creation.
type Timer interface {
	// Start starts the timer to fire after the configured duration.
	Start()

	// Stop stops the timer, preventing it from firing.
	Stop()

	// Reset resets the timer to fire after the configured duration.
	// If the timer was stopped, this starts it again.
	Reset()

	// C returns the channel that receives when the timer fires.
	C() <-chan struct{}
}

// RealTimer implements Timer using actual time.
type RealTimer struct {
	mu       sync.Mutex
	duration time.Duration
	timer    *time.Timer
	c        chan struct{}
	stopped  bool
}

// NewRealTimer creates a new RealTimer with the given duration.
func NewRealTimer(duration time.Duration) *RealTimer {
	return &RealTimer{
		duration: duration,
		c:        make(chan struct{}, 1),
	}
}

// Start starts the timer.
func (t *RealTimer) Start() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.timer != nil {
		t.timer.Stop()
	}

	t.stopped = false
	t.timer = time.AfterFunc(t.duration, func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		if !t.stopped {
			select {
			case t.c <- struct{}{}:
			default:
			}
		}
	})
}

// Stop stops the timer.
func (t *RealTimer) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.stopped = true
	if t.timer != nil {
		t.timer.Stop()
	}
}

// Reset resets the timer.
func (t *RealTimer) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.stopped = false
	if t.timer != nil {
		t.timer.Stop()
	}

	t.timer = time.AfterFunc(t.duration, func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		if !t.stopped {
			select {
			case t.c <- struct{}{}:
			default:
			}
		}
	})
}

// C returns the timer channel.
func (t *RealTimer) C() <-chan struct{} {
	return t.c
}

// Duration returns the timer's configured duration.
func (t *RealTimer) Duration() time.Duration {
	return t.duration
}
