package narwhal_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/edgedlt/narwhal"
	"github.com/edgedlt/narwhal/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestRequestTracker_BasicTracking(t *testing.T) {
	cfg := narwhal.DefaultRequestTrackerConfig()
	tracker := narwhal.NewRequestTracker[testutil.TestHash](cfg, zap.NewNop())

	digest := testutil.ComputeHash([]byte("test"))

	// Track a request
	ctx, complete := tracker.Track(context.Background(), 5, narwhal.RequestTypeBatch, digest)
	require.NotNil(t, ctx)
	require.NotNil(t, complete)

	// Should be pending
	assert.Equal(t, 1, tracker.PendingCount())
	assert.Equal(t, 1, tracker.PendingForRound(5))

	// Complete the request
	complete()

	// Should no longer be pending
	assert.Equal(t, 0, tracker.PendingCount())
	assert.Equal(t, 0, tracker.PendingForRound(5))

	// Check stats
	stats := tracker.Stats()
	assert.Equal(t, uint64(1), stats.TotalStarted)
	assert.Equal(t, uint64(1), stats.TotalCompleted)
	assert.Equal(t, uint64(0), stats.TotalCancelled)
}

func TestRequestTracker_CancelOnRoundAdvance(t *testing.T) {
	cfg := narwhal.RequestTrackerConfig{
		MaxPendingPerRound:  100,
		StaleRoundThreshold: 2,
		CancelOnAdvance:     true,
	}
	tracker := narwhal.NewRequestTracker[testutil.TestHash](cfg, zap.NewNop())

	digest1 := testutil.ComputeHash([]byte("test1"))
	digest2 := testutil.ComputeHash([]byte("test2"))
	digest3 := testutil.ComputeHash([]byte("test3"))

	// Track requests for different rounds
	ctx1, _ := tracker.Track(context.Background(), 1, narwhal.RequestTypeBatch, digest1)
	ctx2, _ := tracker.Track(context.Background(), 3, narwhal.RequestTypeBatch, digest2)
	ctx3, _ := tracker.Track(context.Background(), 5, narwhal.RequestTypeBatch, digest3)

	assert.Equal(t, 3, tracker.PendingCount())

	// Advance round to 5 (stale threshold is 2, so rounds < 3 should be cancelled)
	tracker.OnRoundAdvance(5)

	// Wait for cancellation to propagate
	time.Sleep(10 * time.Millisecond)

	// Round 1 should be cancelled (5 - 2 = 3, so < 3)
	select {
	case <-ctx1.Done():
		// Expected - cancelled
	default:
		t.Error("expected ctx1 to be cancelled")
	}

	// Round 3 should NOT be cancelled (= 3)
	select {
	case <-ctx2.Done():
		t.Error("expected ctx2 to NOT be cancelled")
	default:
		// Expected - still valid
	}

	// Round 5 should NOT be cancelled (> 3)
	select {
	case <-ctx3.Done():
		t.Error("expected ctx3 to NOT be cancelled")
	default:
		// Expected - still valid
	}

	// Check pending count (rounds 3 and 5 should still be pending)
	assert.Equal(t, 2, tracker.PendingCount())

	stats := tracker.Stats()
	assert.Equal(t, uint64(1), stats.TotalCancelled)
}

func TestRequestTracker_CancelAll(t *testing.T) {
	cfg := narwhal.DefaultRequestTrackerConfig()
	tracker := narwhal.NewRequestTracker[testutil.TestHash](cfg, zap.NewNop())

	// Track multiple requests
	var contexts []context.Context
	for i := 0; i < 5; i++ {
		digest := testutil.ComputeHash([]byte{byte(i)})
		ctx, _ := tracker.Track(context.Background(), uint64(i), narwhal.RequestTypeCertificate, digest)
		contexts = append(contexts, ctx)
	}

	assert.Equal(t, 5, tracker.PendingCount())

	// Cancel all
	tracker.CancelAll()

	// All should be cancelled
	for i, ctx := range contexts {
		select {
		case <-ctx.Done():
			// Expected
		default:
			t.Errorf("expected context %d to be cancelled", i)
		}
	}

	assert.Equal(t, 0, tracker.PendingCount())

	stats := tracker.Stats()
	assert.Equal(t, uint64(5), stats.TotalCancelled)
}

func TestRequestTracker_CancelForRound(t *testing.T) {
	cfg := narwhal.DefaultRequestTrackerConfig()
	tracker := narwhal.NewRequestTracker[testutil.TestHash](cfg, zap.NewNop())

	digest1 := testutil.ComputeHash([]byte("round5-1"))
	digest2 := testutil.ComputeHash([]byte("round5-2"))
	digest3 := testutil.ComputeHash([]byte("round7"))

	// Track requests
	ctx1, _ := tracker.Track(context.Background(), 5, narwhal.RequestTypeBatch, digest1)
	ctx2, _ := tracker.Track(context.Background(), 5, narwhal.RequestTypeBatch, digest2)
	ctx3, _ := tracker.Track(context.Background(), 7, narwhal.RequestTypeBatch, digest3)

	assert.Equal(t, 3, tracker.PendingCount())
	assert.Equal(t, 2, tracker.PendingForRound(5))

	// Cancel only round 5
	tracker.CancelForRound(5)

	// Round 5 requests should be cancelled
	select {
	case <-ctx1.Done():
		// Expected
	default:
		t.Error("expected ctx1 to be cancelled")
	}

	select {
	case <-ctx2.Done():
		// Expected
	default:
		t.Error("expected ctx2 to be cancelled")
	}

	// Round 7 should still be valid
	select {
	case <-ctx3.Done():
		t.Error("expected ctx3 to NOT be cancelled")
	default:
		// Expected
	}

	assert.Equal(t, 1, tracker.PendingCount())
	assert.Equal(t, 0, tracker.PendingForRound(5))
	assert.Equal(t, 1, tracker.PendingForRound(7))
}

func TestRequestTracker_MaxPendingPerRound(t *testing.T) {
	cfg := narwhal.RequestTrackerConfig{
		MaxPendingPerRound:  3,
		StaleRoundThreshold: 10,
		CancelOnAdvance:     true,
	}
	tracker := narwhal.NewRequestTracker[testutil.TestHash](cfg, zap.NewNop())

	var contexts []context.Context

	// Track 5 requests for the same round (max is 3)
	for i := 0; i < 5; i++ {
		digest := testutil.ComputeHash([]byte{byte(i)})
		ctx, _ := tracker.Track(context.Background(), 5, narwhal.RequestTypeBatch, digest)
		contexts = append(contexts, ctx)
	}

	// Should only have 3 pending (2 oldest were cancelled)
	assert.Equal(t, 3, tracker.PendingCount())

	// First 2 should be cancelled
	for i := 0; i < 2; i++ {
		select {
		case <-contexts[i].Done():
			// Expected - cancelled due to capacity
		default:
			t.Errorf("expected context %d to be cancelled", i)
		}
	}

	// Last 3 should still be valid
	for i := 2; i < 5; i++ {
		select {
		case <-contexts[i].Done():
			t.Errorf("expected context %d to NOT be cancelled", i)
		default:
			// Expected
		}
	}

	stats := tracker.Stats()
	assert.Equal(t, uint64(5), stats.TotalStarted)
	assert.Equal(t, uint64(2), stats.TotalCancelled)
}

func TestRequestTracker_ContextCancellation(t *testing.T) {
	cfg := narwhal.DefaultRequestTrackerConfig()
	tracker := narwhal.NewRequestTracker[testutil.TestHash](cfg, zap.NewNop())

	// Create a cancellable parent context
	parentCtx, parentCancel := context.WithCancel(context.Background())

	digest := testutil.ComputeHash([]byte("test"))
	ctx, complete := tracker.Track(parentCtx, 5, narwhal.RequestTypeBatch, digest)
	defer complete()

	// Context should be valid
	select {
	case <-ctx.Done():
		t.Error("context should not be done yet")
	default:
		// Expected
	}

	// Cancel parent
	parentCancel()

	// Child context should also be cancelled
	select {
	case <-ctx.Done():
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Error("expected context to be cancelled when parent is cancelled")
	}
}

func TestRequestTracker_ConcurrentAccess(t *testing.T) {
	cfg := narwhal.DefaultRequestTrackerConfig()
	tracker := narwhal.NewRequestTracker[testutil.TestHash](cfg, zap.NewNop())

	const numGoroutines = 10
	const numRequestsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			defer wg.Done()

			for i := 0; i < numRequestsPerGoroutine; i++ {
				digest := testutil.ComputeHash([]byte{byte(id), byte(i)})
				_, complete := tracker.Track(context.Background(), uint64(i%10), narwhal.RequestTypeBatch, digest)

				// Randomly complete some
				if i%3 == 0 {
					complete()
				}
			}
		}(g)
	}

	wg.Wait()

	stats := tracker.Stats()
	assert.Equal(t, uint64(numGoroutines*numRequestsPerGoroutine), stats.TotalStarted)
	// Some should be completed, some still pending
	assert.Greater(t, stats.TotalCompleted, uint64(0))
}

func TestRequestTracker_DisabledCancelOnAdvance(t *testing.T) {
	cfg := narwhal.RequestTrackerConfig{
		MaxPendingPerRound:  100,
		StaleRoundThreshold: 2,
		CancelOnAdvance:     false, // Disabled
	}
	tracker := narwhal.NewRequestTracker[testutil.TestHash](cfg, zap.NewNop())

	digest := testutil.ComputeHash([]byte("test"))
	ctx, _ := tracker.Track(context.Background(), 1, narwhal.RequestTypeBatch, digest)

	// Advance round significantly
	tracker.OnRoundAdvance(100)

	// Should NOT be cancelled because CancelOnAdvance is disabled
	select {
	case <-ctx.Done():
		t.Error("context should NOT be cancelled when CancelOnAdvance is disabled")
	default:
		// Expected
	}

	assert.Equal(t, 1, tracker.PendingCount())
}
