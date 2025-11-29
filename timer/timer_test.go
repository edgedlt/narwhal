package timer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealTimer_FiresAfterDuration(t *testing.T) {
	timer := NewRealTimer(50 * time.Millisecond)
	timer.Start()

	select {
	case <-timer.C():
		// Success
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timer did not fire")
	}
}

func TestRealTimer_Stop(t *testing.T) {
	timer := NewRealTimer(50 * time.Millisecond)
	timer.Start()
	timer.Stop()

	select {
	case <-timer.C():
		t.Fatal("timer should not fire after stop")
	case <-time.After(100 * time.Millisecond):
		// Success - timer was stopped
	}
}

func TestRealTimer_Reset(t *testing.T) {
	timer := NewRealTimer(50 * time.Millisecond)
	timer.Start()

	// Wait a bit, then reset
	time.Sleep(30 * time.Millisecond)
	timer.Reset()

	// Original timer should have been at ~30ms, after reset it should be back to 0
	// So we wait 70ms (more than 50ms from reset, less than 80ms total from original start)
	start := time.Now()
	select {
	case <-timer.C():
		elapsed := time.Since(start)
		// Should fire around 50ms after reset
		assert.True(t, elapsed >= 40*time.Millisecond, "fired too early")
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timer did not fire after reset")
	}
}

func TestRealTimer_Duration(t *testing.T) {
	d := 123 * time.Millisecond
	timer := NewRealTimer(d)
	assert.Equal(t, d, timer.Duration())
}

func TestMockTimer_Fire(t *testing.T) {
	timer := NewMockTimer()
	timer.Start()

	// Channel should be empty initially
	select {
	case <-timer.C():
		t.Fatal("timer should not fire before Fire() called")
	default:
	}

	// Fire the timer
	timer.Fire()

	// Now channel should have a value
	select {
	case <-timer.C():
		// Success
	default:
		t.Fatal("timer should fire after Fire() called")
	}
}

func TestMockTimer_StopPreventsfire(t *testing.T) {
	timer := NewMockTimer()
	timer.Start()
	timer.Stop()
	timer.Fire()

	select {
	case <-timer.C():
		t.Fatal("stopped timer should not fire")
	default:
		// Success
	}
}

func TestMockTimer_ResetAllowsFire(t *testing.T) {
	timer := NewMockTimer()
	timer.Start()
	timer.Stop()
	timer.Reset()
	timer.Fire()

	select {
	case <-timer.C():
		// Success
	default:
		t.Fatal("reset timer should fire")
	}
}

func TestMockTimer_IsStarted(t *testing.T) {
	timer := NewMockTimer()
	require.False(t, timer.IsStarted())

	timer.Start()
	require.True(t, timer.IsStarted())
}

func TestMockTimer_IsStopped(t *testing.T) {
	timer := NewMockTimer()
	require.False(t, timer.IsStopped())

	timer.Start()
	require.False(t, timer.IsStopped())

	timer.Stop()
	require.True(t, timer.IsStopped())
}
