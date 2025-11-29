package narwhal_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edgedlt/narwhal"
	"github.com/edgedlt/narwhal/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestHeaderWaiter_BasicFlow(t *testing.T) {
	// Track available parents
	var availableParentsMu sync.RWMutex
	availableParents := make(map[string]bool)

	// Track processed headers
	var processedCount atomic.Int32
	processedHeaders := make(chan *narwhal.Header[testutil.TestHash], 10)

	processFunc := func(header *narwhal.Header[testutil.TestHash], from uint16) error {
		processedCount.Add(1)
		processedHeaders <- header
		return nil
	}

	checkParentFunc := func(digest testutil.TestHash) bool {
		availableParentsMu.RLock()
		defer availableParentsMu.RUnlock()
		return availableParents[digest.String()]
	}

	cfg := narwhal.HeaderWaiterConfig{
		MaxPendingHeaders: 100,
		RetryInterval:     50 * time.Millisecond,
		MaxRetries:        5,
		MaxAge:            5 * time.Second,
	}

	waiter := narwhal.NewHeaderWaiter[testutil.TestHash, *testutil.TestTransaction](
		cfg,
		processFunc,
		checkParentFunc,
		nil,
		zap.NewNop(),
	)

	// Start the waiter
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go waiter.Run(ctx)

	// Create a header with missing parents
	parent1 := testutil.ComputeHash([]byte("parent1"))
	parent2 := testutil.ComputeHash([]byte("parent2"))

	header := &narwhal.Header[testutil.TestHash]{
		Author:    0,
		Round:     1,
		Parents:   []testutil.TestHash{parent1, parent2},
		Timestamp: uint64(time.Now().UnixMilli()),
	}
	header.ComputeDigest(testutil.ComputeHash)

	// Add header with missing parents
	added := waiter.Add(header, 1, []testutil.TestHash{parent1, parent2})
	require.True(t, added, "should accept header")

	// Verify it's pending
	assert.Equal(t, 1, waiter.PendingCount())

	// Make one parent available
	availableParentsMu.Lock()
	availableParents[parent1.String()] = true
	availableParentsMu.Unlock()
	waiter.OnParentAvailable(parent1)

	// Still pending (one parent still missing)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, waiter.PendingCount())

	// Make second parent available
	availableParentsMu.Lock()
	availableParents[parent2.String()] = true
	availableParentsMu.Unlock()
	waiter.OnParentAvailable(parent2)

	// Should be processed now
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 0, waiter.PendingCount())
	assert.Equal(t, int32(1), processedCount.Load())

	// Verify correct header was processed
	select {
	case processed := <-processedHeaders:
		assert.True(t, processed.Digest.Equals(header.Digest))
	default:
		t.Fatal("expected header to be processed")
	}

	// Check stats
	stats := waiter.Stats()
	assert.Equal(t, uint64(1), stats.TotalReceived)
	assert.Equal(t, uint64(1), stats.TotalProcessed)
	assert.Equal(t, uint64(0), stats.TotalDropped)
}

func TestHeaderWaiter_RetryProcessing(t *testing.T) {
	var availableParentsMu sync.RWMutex
	availableParents := make(map[string]bool)
	var processedCount atomic.Int32

	processFunc := func(header *narwhal.Header[testutil.TestHash], from uint16) error {
		processedCount.Add(1)
		return nil
	}

	checkParentFunc := func(digest testutil.TestHash) bool {
		availableParentsMu.RLock()
		defer availableParentsMu.RUnlock()
		return availableParents[digest.String()]
	}

	cfg := narwhal.HeaderWaiterConfig{
		MaxPendingHeaders: 100,
		RetryInterval:     50 * time.Millisecond,
		MaxRetries:        10,
		MaxAge:            5 * time.Second,
	}

	waiter := narwhal.NewHeaderWaiter[testutil.TestHash, *testutil.TestTransaction](
		cfg,
		processFunc,
		checkParentFunc,
		nil,
		zap.NewNop(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go waiter.Run(ctx)

	// Create header with missing parent
	parent := testutil.ComputeHash([]byte("missing-parent"))
	header := &narwhal.Header[testutil.TestHash]{
		Author:    0,
		Round:     1,
		Parents:   []testutil.TestHash{parent},
		Timestamp: uint64(time.Now().UnixMilli()),
	}
	header.ComputeDigest(testutil.ComputeHash)

	waiter.Add(header, 1, []testutil.TestHash{parent})
	assert.Equal(t, 1, waiter.PendingCount())

	// Wait for a few retries
	time.Sleep(150 * time.Millisecond)

	// Still pending
	assert.Equal(t, 1, waiter.PendingCount())
	assert.Equal(t, int32(0), processedCount.Load())

	// Make parent available via checkParentFunc (will be picked up on next retry)
	availableParentsMu.Lock()
	availableParents[parent.String()] = true
	availableParentsMu.Unlock()

	// Wait for retry to process
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, 0, waiter.PendingCount())
	assert.Equal(t, int32(1), processedCount.Load())
}

func TestHeaderWaiter_MaxRetries(t *testing.T) {
	processFunc := func(header *narwhal.Header[testutil.TestHash], from uint16) error {
		return nil
	}

	// Never available
	checkParentFunc := func(digest testutil.TestHash) bool {
		return false
	}

	cfg := narwhal.HeaderWaiterConfig{
		MaxPendingHeaders: 100,
		RetryInterval:     20 * time.Millisecond,
		MaxRetries:        3,
		MaxAge:            5 * time.Second,
	}

	waiter := narwhal.NewHeaderWaiter[testutil.TestHash, *testutil.TestTransaction](
		cfg,
		processFunc,
		checkParentFunc,
		nil,
		zap.NewNop(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go waiter.Run(ctx)

	parent := testutil.ComputeHash([]byte("never-available"))
	header := &narwhal.Header[testutil.TestHash]{
		Author:    0,
		Round:     1,
		Parents:   []testutil.TestHash{parent},
		Timestamp: uint64(time.Now().UnixMilli()),
	}
	header.ComputeDigest(testutil.ComputeHash)

	waiter.Add(header, 1, []testutil.TestHash{parent})

	// Wait for max retries to be exceeded
	time.Sleep(150 * time.Millisecond)

	// Should be dropped
	assert.Equal(t, 0, waiter.PendingCount())

	stats := waiter.Stats()
	assert.Equal(t, uint64(1), stats.TotalReceived)
	assert.Equal(t, uint64(0), stats.TotalProcessed)
	assert.Equal(t, uint64(1), stats.TotalDropped)
}

func TestHeaderWaiter_MaxAge(t *testing.T) {
	processFunc := func(header *narwhal.Header[testutil.TestHash], from uint16) error {
		return nil
	}

	checkParentFunc := func(digest testutil.TestHash) bool {
		return false
	}

	cfg := narwhal.HeaderWaiterConfig{
		MaxPendingHeaders: 100,
		RetryInterval:     20 * time.Millisecond,
		MaxRetries:        100, // High so we hit MaxAge first
		MaxAge:            50 * time.Millisecond,
	}

	waiter := narwhal.NewHeaderWaiter[testutil.TestHash, *testutil.TestTransaction](
		cfg,
		processFunc,
		checkParentFunc,
		nil,
		zap.NewNop(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go waiter.Run(ctx)

	parent := testutil.ComputeHash([]byte("never-available"))
	header := &narwhal.Header[testutil.TestHash]{
		Author:    0,
		Round:     1,
		Parents:   []testutil.TestHash{parent},
		Timestamp: uint64(time.Now().UnixMilli()),
	}
	header.ComputeDigest(testutil.ComputeHash)

	waiter.Add(header, 1, []testutil.TestHash{parent})

	// Wait for MaxAge to be exceeded
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, 0, waiter.PendingCount())

	stats := waiter.Stats()
	assert.Equal(t, uint64(1), stats.TotalExpired)
}

func TestHeaderWaiter_Capacity(t *testing.T) {
	processFunc := func(header *narwhal.Header[testutil.TestHash], from uint16) error {
		return nil
	}

	checkParentFunc := func(digest testutil.TestHash) bool {
		return false
	}

	cfg := narwhal.HeaderWaiterConfig{
		MaxPendingHeaders: 3,
		RetryInterval:     1 * time.Second, // Long so we don't auto-process
		MaxRetries:        100,
		MaxAge:            10 * time.Second,
	}

	waiter := narwhal.NewHeaderWaiter[testutil.TestHash, *testutil.TestTransaction](
		cfg,
		processFunc,
		checkParentFunc,
		nil,
		zap.NewNop(),
	)

	// Add 5 headers (capacity is 3)
	for i := 0; i < 5; i++ {
		parent := testutil.ComputeHash([]byte("parent"))
		header := &narwhal.Header[testutil.TestHash]{
			Author:    uint16(i),
			Round:     1,
			Parents:   []testutil.TestHash{parent},
			Timestamp: uint64(time.Now().UnixMilli()) + uint64(i),
		}
		header.ComputeDigest(testutil.ComputeHash)

		// Small delay between adds so timestamps are different
		time.Sleep(1 * time.Millisecond)

		waiter.Add(header, uint16(i), []testutil.TestHash{parent})
	}

	// Should have dropped 2 (oldest ones)
	assert.Equal(t, 3, waiter.PendingCount())

	stats := waiter.Stats()
	assert.Equal(t, uint64(5), stats.TotalReceived)
	assert.Equal(t, uint64(2), stats.TotalDropped)
}

func TestHeaderWaiter_DuplicateHeader(t *testing.T) {
	processFunc := func(header *narwhal.Header[testutil.TestHash], from uint16) error {
		return nil
	}

	checkParentFunc := func(digest testutil.TestHash) bool {
		return false
	}

	cfg := narwhal.DefaultHeaderWaiterConfig()
	waiter := narwhal.NewHeaderWaiter[testutil.TestHash, *testutil.TestTransaction](
		cfg,
		processFunc,
		checkParentFunc,
		nil,
		zap.NewNop(),
	)

	parent := testutil.ComputeHash([]byte("parent"))
	header := &narwhal.Header[testutil.TestHash]{
		Author:    0,
		Round:     1,
		Parents:   []testutil.TestHash{parent},
		Timestamp: uint64(time.Now().UnixMilli()),
	}
	header.ComputeDigest(testutil.ComputeHash)

	// Add same header twice
	added1 := waiter.Add(header, 1, []testutil.TestHash{parent})
	added2 := waiter.Add(header, 1, []testutil.TestHash{parent})

	assert.True(t, added1)
	assert.False(t, added2) // Duplicate rejected

	assert.Equal(t, 1, waiter.PendingCount())
}
