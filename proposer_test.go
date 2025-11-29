package narwhal_test

import (
	"context"
	"testing"
	"time"

	"github.com/edgedlt/narwhal"
	"github.com/edgedlt/narwhal/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestProposer_BasicFlow(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	dag := narwhal.NewDAG[testutil.TestHash, *testutil.TestTransaction](validators, zap.NewNop())

	cfg := narwhal.ProposerConfig[testutil.TestHash, *testutil.TestTransaction]{
		ValidatorID:      0,
		DAG:              dag,
		Validators:       validators,
		HashFunc:         testutil.ComputeHash,
		Logger:           zap.NewNop(),
		MaxHeaderBatches: 5,
		MaxHeaderDelay:   100 * time.Millisecond,
		MinHeaderBatches: 1,
	}

	proposer := narwhal.NewProposer(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go proposer.Run(ctx)

	// Add batches
	for i := 0; i < 5; i++ {
		tx := testutil.NewTestTransaction([]byte{byte(i)})
		batch := &narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]{
			WorkerID:     0,
			ValidatorID:  0,
			Round:        0,
			Transactions: []*testutil.TestTransaction{tx},
		}
		batch.ComputeDigest(testutil.ComputeHash)
		proposer.AddBatch(batch)
	}

	// Should receive a proposed header (hit batch threshold)
	select {
	case proposed := <-proposer.Headers():
		require.NotNil(t, proposed)
		assert.Equal(t, 5, len(proposed.Header.BatchRefs))
		assert.Equal(t, uint64(0), proposed.Header.Round)
		assert.Equal(t, uint16(0), proposed.Header.Author)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected header to be proposed")
	}

	// Stats should reflect the proposal
	stats := proposer.Stats()
	assert.Equal(t, uint64(1), stats.HeadersProposed)
	assert.Equal(t, uint64(5), stats.BatchesIncluded)
}

func TestProposer_TimeThreshold(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	dag := narwhal.NewDAG[testutil.TestHash, *testutil.TestTransaction](validators, zap.NewNop())

	cfg := narwhal.ProposerConfig[testutil.TestHash, *testutil.TestTransaction]{
		ValidatorID:      0,
		DAG:              dag,
		Validators:       validators,
		HashFunc:         testutil.ComputeHash,
		Logger:           zap.NewNop(),
		MaxHeaderBatches: 100, // High threshold
		MaxHeaderDelay:   50 * time.Millisecond,
		MinHeaderBatches: 1,
	}

	proposer := narwhal.NewProposer(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go proposer.Run(ctx)

	// Add just one batch
	tx := testutil.NewTestTransaction([]byte("test"))
	batch := &narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]{
		WorkerID:     0,
		ValidatorID:  0,
		Round:        0,
		Transactions: []*testutil.TestTransaction{tx},
	}
	batch.ComputeDigest(testutil.ComputeHash)
	proposer.AddBatch(batch)

	// Should receive a proposed header after time threshold
	select {
	case proposed := <-proposer.Headers():
		require.NotNil(t, proposed)
		assert.Equal(t, 1, len(proposed.Header.BatchRefs))
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected header to be proposed after time threshold")
	}
}

func TestProposer_MinBatches(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	dag := narwhal.NewDAG[testutil.TestHash, *testutil.TestTransaction](validators, zap.NewNop())

	cfg := narwhal.ProposerConfig[testutil.TestHash, *testutil.TestTransaction]{
		ValidatorID:      0,
		DAG:              dag,
		Validators:       validators,
		HashFunc:         testutil.ComputeHash,
		Logger:           zap.NewNop(),
		MaxHeaderBatches: 100,
		MaxHeaderDelay:   50 * time.Millisecond,
		MinHeaderBatches: 3, // Require at least 3 batches
	}

	proposer := narwhal.NewProposer(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go proposer.Run(ctx)

	// Add only 2 batches (less than minimum)
	for i := 0; i < 2; i++ {
		tx := testutil.NewTestTransaction([]byte{byte(i)})
		batch := &narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]{
			WorkerID:     0,
			ValidatorID:  0,
			Round:        0,
			Transactions: []*testutil.TestTransaction{tx},
		}
		batch.ComputeDigest(testutil.ComputeHash)
		proposer.AddBatch(batch)
	}

	// Should NOT receive a header (below minimum)
	select {
	case <-proposer.Headers():
		t.Fatal("should not propose with fewer than MinHeaderBatches")
	case <-time.After(150 * time.Millisecond):
		// Expected - no proposal
	}

	// Add one more batch
	tx := testutil.NewTestTransaction([]byte{2})
	batch := &narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]{
		WorkerID:     0,
		ValidatorID:  0,
		Round:        0,
		Transactions: []*testutil.TestTransaction{tx},
	}
	batch.ComputeDigest(testutil.ComputeHash)
	proposer.AddBatch(batch)

	// Now should receive a header
	select {
	case proposed := <-proposer.Headers():
		require.NotNil(t, proposed)
		assert.Equal(t, 3, len(proposed.Header.BatchRefs))
	case <-time.After(150 * time.Millisecond):
		t.Fatal("expected header after meeting MinHeaderBatches")
	}
}

func TestProposer_ForcePropose(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	dag := narwhal.NewDAG[testutil.TestHash, *testutil.TestTransaction](validators, zap.NewNop())

	cfg := narwhal.ProposerConfig[testutil.TestHash, *testutil.TestTransaction]{
		ValidatorID:      0,
		DAG:              dag,
		Validators:       validators,
		HashFunc:         testutil.ComputeHash,
		Logger:           zap.NewNop(),
		MaxHeaderBatches: 100,
		MaxHeaderDelay:   10 * time.Second, // Very long delay
		MinHeaderBatches: 10,               // High minimum
	}

	proposer := narwhal.NewProposer(cfg)

	// Add just one batch
	tx := testutil.NewTestTransaction([]byte("test"))
	batch := &narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]{
		WorkerID:     0,
		ValidatorID:  0,
		Round:        0,
		Transactions: []*testutil.TestTransaction{tx},
	}
	batch.ComputeDigest(testutil.ComputeHash)
	proposer.AddBatch(batch)

	assert.Equal(t, 1, proposer.PendingCount())

	// Force propose
	ok := proposer.ForcePropose()
	assert.True(t, ok)

	// Should receive a header
	select {
	case proposed := <-proposer.Headers():
		require.NotNil(t, proposed)
		assert.Equal(t, 1, len(proposed.Header.BatchRefs))
	default:
		t.Fatal("expected header from force propose")
	}

	assert.Equal(t, 0, proposer.PendingCount())
}

func TestProposer_BoundedQueue(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	dag := narwhal.NewDAG[testutil.TestHash, *testutil.TestTransaction](validators, zap.NewNop())

	cfg := narwhal.ProposerConfig[testutil.TestHash, *testutil.TestTransaction]{
		ValidatorID:       0,
		DAG:               dag,
		Validators:        validators,
		HashFunc:          testutil.ComputeHash,
		Logger:            zap.NewNop(),
		MaxHeaderBatches:  1000, // High threshold so we don't auto-propose
		MaxHeaderDelay:    10 * time.Second,
		MinHeaderBatches:  1,
		MaxPendingBatches: 5,
		DropOnFull:        true,
	}

	proposer := narwhal.NewProposer(cfg)

	// Don't start the proposer so batches stay queued

	// Add 10 batches (only 5 should fit)
	for i := 0; i < 10; i++ {
		tx := testutil.NewTestTransaction([]byte{byte(i)})
		batch := &narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]{
			WorkerID:     0,
			ValidatorID:  0,
			Round:        0,
			Transactions: []*testutil.TestTransaction{tx},
		}
		batch.ComputeDigest(testutil.ComputeHash)
		proposer.AddBatch(batch)
	}

	stats := proposer.Stats()
	assert.Equal(t, 5, stats.QueuedBatches)
	assert.Equal(t, uint64(5), stats.DroppedBatches)
	assert.True(t, stats.IsBounded)
}

func TestProposer_NeedsParents(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	dag := narwhal.NewDAG[testutil.TestHash, *testutil.TestTransaction](validators, zap.NewNop())

	// Insert round 0 certificates to advance to round 1
	for author := uint16(0); author < 4; author++ {
		header := &narwhal.Header[testutil.TestHash]{
			Author:    author,
			Round:     0,
			Timestamp: uint64(time.Now().UnixMilli()) + uint64(author),
		}
		header.ComputeDigest(testutil.ComputeHash)

		votes := make(map[uint16][]byte)
		for j := uint16(0); j < 3; j++ {
			sig, _ := validators.GetSigner(j).Sign(header.Digest.Bytes())
			votes[j] = sig
		}
		cert := narwhal.NewCertificate(header, votes)
		_ = dag.InsertCertificate(cert, nil)
	}

	// DAG should now be at round 1
	assert.Equal(t, uint64(1), dag.CurrentRound())

	cfg := narwhal.ProposerConfig[testutil.TestHash, *testutil.TestTransaction]{
		ValidatorID:      0,
		DAG:              dag,
		Validators:       validators,
		HashFunc:         testutil.ComputeHash,
		Logger:           zap.NewNop(),
		MaxHeaderBatches: 1,
		MaxHeaderDelay:   50 * time.Millisecond,
		MinHeaderBatches: 1,
	}

	proposer := narwhal.NewProposer(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go proposer.Run(ctx)

	// Add a batch
	tx := testutil.NewTestTransaction([]byte("test"))
	batch := &narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]{
		WorkerID:     0,
		ValidatorID:  0,
		Round:        1,
		Transactions: []*testutil.TestTransaction{tx},
	}
	batch.ComputeDigest(testutil.ComputeHash)
	proposer.AddBatch(batch)

	// Should receive a header for round 1 with parents
	select {
	case proposed := <-proposer.Headers():
		require.NotNil(t, proposed)
		assert.Equal(t, uint64(1), proposed.Header.Round)
		assert.GreaterOrEqual(t, len(proposed.Header.Parents), 3) // 2f+1 = 3
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected header to be proposed")
	}
}
