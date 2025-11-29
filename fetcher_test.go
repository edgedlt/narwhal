package narwhal_test

import (
	"context"
	"fmt"
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

// mockFetchNetwork is a test network that allows controlling fetch behavior.
type mockFetchNetwork struct {
	testutil.TestNetwork
	fetchBatchFunc func(from uint16, digest testutil.TestHash) (*narwhal.Batch[testutil.TestHash, *testutil.TestTransaction], error)
	fetchCertFunc  func(from uint16, digest testutil.TestHash) (*narwhal.Certificate[testutil.TestHash], error)
	fetchCount     atomic.Int32
}

func newMockFetchNetwork(index uint16) *mockFetchNetwork {
	return &mockFetchNetwork{
		TestNetwork: *testutil.NewTestNetwork(index),
	}
}

func (n *mockFetchNetwork) FetchBatch(from uint16, digest testutil.TestHash) (*narwhal.Batch[testutil.TestHash, *testutil.TestTransaction], error) {
	n.fetchCount.Add(1)
	if n.fetchBatchFunc != nil {
		return n.fetchBatchFunc(from, digest)
	}
	return nil, fmt.Errorf("batch not found")
}

func (n *mockFetchNetwork) FetchCertificate(from uint16, digest testutil.TestHash) (*narwhal.Certificate[testutil.TestHash], error) {
	n.fetchCount.Add(1)
	if n.fetchCertFunc != nil {
		return n.fetchCertFunc(from, digest)
	}
	return nil, fmt.Errorf("certificate not found")
}

func (n *mockFetchNetwork) FetchCertificatesInRange(from uint16, startRound, endRound uint64) ([]*narwhal.Certificate[testutil.TestHash], error) {
	return nil, fmt.Errorf("not implemented")
}

func TestFetcher_FetchBatch_LocalStorage(t *testing.T) {
	storage := testutil.NewTestStorage()
	network := newMockFetchNetwork(0)
	validators := testutil.NewTestValidatorSet(4)

	// Create and store a batch
	tx := testutil.NewTestTransaction([]byte("test"))
	batch := &narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]{
		WorkerID:     0,
		ValidatorID:  0,
		Round:        0,
		Transactions: []*testutil.TestTransaction{tx},
	}
	batch.ComputeDigest(testutil.ComputeHash)
	require.NoError(t, storage.PutBatch(batch))

	fetcher := narwhal.NewFetcher(
		narwhal.DefaultFetcherConfig(),
		network,
		storage,
		validators,
		testutil.ComputeHash,
		zap.NewNop(),
	)

	// Fetch should return from local storage without network call
	ctx := context.Background()
	result, err := fetcher.FetchBatch(ctx, batch.Digest, 1)
	require.NoError(t, err)
	assert.True(t, result.Digest.Equals(batch.Digest))
	assert.Equal(t, int32(0), network.fetchCount.Load(), "should not call network")
}

func TestFetcher_FetchBatch_FromNetwork(t *testing.T) {
	storage := testutil.NewTestStorage()
	network := newMockFetchNetwork(0)
	validators := testutil.NewTestValidatorSet(4)

	// Create a batch that will be returned by the network
	tx := testutil.NewTestTransaction([]byte("test"))
	batch := &narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]{
		WorkerID:     0,
		ValidatorID:  1,
		Round:        0,
		Transactions: []*testutil.TestTransaction{tx},
	}
	batch.ComputeDigest(testutil.ComputeHash)

	network.fetchBatchFunc = func(from uint16, digest testutil.TestHash) (*narwhal.Batch[testutil.TestHash, *testutil.TestTransaction], error) {
		if digest.Equals(batch.Digest) {
			return batch, nil
		}
		return nil, fmt.Errorf("not found")
	}

	fetcher := narwhal.NewFetcher(
		narwhal.DefaultFetcherConfig(),
		network,
		storage,
		validators,
		testutil.ComputeHash,
		zap.NewNop(),
	)

	ctx := context.Background()
	result, err := fetcher.FetchBatch(ctx, batch.Digest, 1)
	require.NoError(t, err)
	assert.True(t, result.Digest.Equals(batch.Digest))
	assert.GreaterOrEqual(t, network.fetchCount.Load(), int32(1), "should call network")

	// Batch should now be in storage
	stored, err := storage.GetBatch(batch.Digest)
	require.NoError(t, err)
	assert.True(t, stored.Digest.Equals(batch.Digest))
}

func TestFetcher_FetchBatch_Retry(t *testing.T) {
	storage := testutil.NewTestStorage()
	network := newMockFetchNetwork(0)
	validators := testutil.NewTestValidatorSet(4)

	tx := testutil.NewTestTransaction([]byte("test"))
	batch := &narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]{
		WorkerID:     0,
		ValidatorID:  1,
		Round:        0,
		Transactions: []*testutil.TestTransaction{tx},
	}
	batch.ComputeDigest(testutil.ComputeHash)

	// Fail first 2 attempts, succeed on 3rd
	var attempts atomic.Int32
	network.fetchBatchFunc = func(from uint16, digest testutil.TestHash) (*narwhal.Batch[testutil.TestHash, *testutil.TestTransaction], error) {
		count := attempts.Add(1)
		if count < 3 {
			return nil, fmt.Errorf("temporary error")
		}
		return batch, nil
	}

	cfg := narwhal.DefaultFetcherConfig()
	cfg.RetryDelay = 10 * time.Millisecond // Fast retries for test

	fetcher := narwhal.NewFetcher(
		cfg,
		network,
		storage,
		validators,
		testutil.ComputeHash,
		zap.NewNop(),
	)

	ctx := context.Background()
	result, err := fetcher.FetchBatch(ctx, batch.Digest, 1)
	require.NoError(t, err)
	assert.True(t, result.Digest.Equals(batch.Digest))
	assert.GreaterOrEqual(t, attempts.Load(), int32(3), "should have retried")
}

func TestFetcher_FetchBatch_Timeout(t *testing.T) {
	storage := testutil.NewTestStorage()
	network := newMockFetchNetwork(0)
	validators := testutil.NewTestValidatorSet(4)

	// Always fail
	network.fetchBatchFunc = func(from uint16, digest testutil.TestHash) (*narwhal.Batch[testutil.TestHash, *testutil.TestTransaction], error) {
		return nil, fmt.Errorf("network error")
	}

	cfg := narwhal.DefaultFetcherConfig()
	cfg.MaxRetries = 1
	cfg.RetryDelay = 10 * time.Millisecond

	fetcher := narwhal.NewFetcher(
		cfg,
		network,
		storage,
		validators,
		testutil.ComputeHash,
		zap.NewNop(),
	)

	ctx := context.Background()
	digest := testutil.ComputeHash([]byte("nonexistent"))
	_, err := fetcher.FetchBatch(ctx, digest, 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to fetch batch")
}

func TestFetcher_FetchBatch_ContextCancellation(t *testing.T) {
	storage := testutil.NewTestStorage()
	network := newMockFetchNetwork(0)
	validators := testutil.NewTestValidatorSet(4)

	// Slow fetch
	network.fetchBatchFunc = func(from uint16, digest testutil.TestHash) (*narwhal.Batch[testutil.TestHash, *testutil.TestTransaction], error) {
		time.Sleep(1 * time.Second)
		return nil, fmt.Errorf("should not reach here")
	}

	fetcher := narwhal.NewFetcher(
		narwhal.DefaultFetcherConfig(),
		network,
		storage,
		validators,
		testutil.ComputeHash,
		zap.NewNop(),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	digest := testutil.ComputeHash([]byte("test"))
	_, err := fetcher.FetchBatch(ctx, digest, 1)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestFetcher_FetchBatch_Deduplication(t *testing.T) {
	storage := testutil.NewTestStorage()
	network := newMockFetchNetwork(0)
	validators := testutil.NewTestValidatorSet(4)

	tx := testutil.NewTestTransaction([]byte("test"))
	batch := &narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]{
		WorkerID:     0,
		ValidatorID:  1,
		Round:        0,
		Transactions: []*testutil.TestTransaction{tx},
	}
	batch.ComputeDigest(testutil.ComputeHash)

	// Slow fetch to allow concurrent requests
	var fetchCalls atomic.Int32
	network.fetchBatchFunc = func(from uint16, digest testutil.TestHash) (*narwhal.Batch[testutil.TestHash, *testutil.TestTransaction], error) {
		fetchCalls.Add(1)
		time.Sleep(100 * time.Millisecond)
		return batch, nil
	}

	fetcher := narwhal.NewFetcher(
		narwhal.DefaultFetcherConfig(),
		network,
		storage,
		validators,
		testutil.ComputeHash,
		zap.NewNop(),
	)

	// Launch multiple concurrent fetches for the same digest
	const numConcurrent = 5
	var wg sync.WaitGroup
	results := make(chan error, numConcurrent)

	for i := 0; i < numConcurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			_, err := fetcher.FetchBatch(ctx, batch.Digest, 1)
			results <- err
		}()
	}

	wg.Wait()
	close(results)

	// All should succeed
	for err := range results {
		require.NoError(t, err)
	}

	// But only one actual network fetch should have occurred
	assert.Equal(t, int32(1), fetchCalls.Load(), "should deduplicate concurrent fetches")
}

func TestFetcher_FetchBatchesParallel(t *testing.T) {
	storage := testutil.NewTestStorage()
	network := newMockFetchNetwork(0)
	validators := testutil.NewTestValidatorSet(4)

	// Create multiple batches
	batches := make([]*narwhal.Batch[testutil.TestHash, *testutil.TestTransaction], 3)
	digests := make([]testutil.TestHash, 3)
	for i := 0; i < 3; i++ {
		tx := testutil.NewTestTransaction([]byte(fmt.Sprintf("tx%d", i)))
		batches[i] = &narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]{
			WorkerID:     uint16(i),
			ValidatorID:  1,
			Round:        0,
			Transactions: []*testutil.TestTransaction{tx},
		}
		batches[i].ComputeDigest(testutil.ComputeHash)
		digests[i] = batches[i].Digest
	}

	network.fetchBatchFunc = func(from uint16, digest testutil.TestHash) (*narwhal.Batch[testutil.TestHash, *testutil.TestTransaction], error) {
		for _, b := range batches {
			if digest.Equals(b.Digest) {
				return b, nil
			}
		}
		return nil, fmt.Errorf("not found")
	}

	fetcher := narwhal.NewFetcher(
		narwhal.DefaultFetcherConfig(),
		network,
		storage,
		validators,
		testutil.ComputeHash,
		zap.NewNop(),
	)

	ctx := context.Background()
	results, err := fetcher.FetchBatchesParallel(ctx, digests, 1)
	require.NoError(t, err)
	require.Len(t, results, 3)

	for i, result := range results {
		assert.True(t, result.Digest.Equals(digests[i]))
	}
}

func TestFetcher_FetchCertificate(t *testing.T) {
	storage := testutil.NewTestStorage()
	network := newMockFetchNetwork(0)
	validators := testutil.NewTestValidatorSet(4)

	// Create a valid certificate
	header := &narwhal.Header[testutil.TestHash]{
		Author:    1,
		Round:     0,
		Timestamp: uint64(time.Now().UnixMilli()),
	}
	header.ComputeDigest(testutil.ComputeHash)

	votes := make(map[uint16][]byte)
	for i := uint16(0); i < 3; i++ {
		signer := validators.GetSigner(i)
		sig, _ := signer.Sign(header.Digest.Bytes())
		votes[i] = sig
	}
	cert := narwhal.NewCertificate(header, votes)

	network.fetchCertFunc = func(from uint16, digest testutil.TestHash) (*narwhal.Certificate[testutil.TestHash], error) {
		if digest.Equals(header.Digest) {
			return cert, nil
		}
		return nil, fmt.Errorf("not found")
	}

	fetcher := narwhal.NewFetcher(
		narwhal.DefaultFetcherConfig(),
		network,
		storage,
		validators,
		testutil.ComputeHash,
		zap.NewNop(),
	)

	ctx := context.Background()
	result, err := fetcher.FetchCertificate(ctx, header.Digest, 1)
	require.NoError(t, err)
	assert.True(t, result.Header.Digest.Equals(header.Digest))

	// Certificate should now be in storage
	stored, err := storage.GetCertificate(header.Digest)
	require.NoError(t, err)
	assert.True(t, stored.Header.Digest.Equals(header.Digest))
}

func TestFetcher_RejectsInvalidBatch(t *testing.T) {
	storage := testutil.NewTestStorage()
	network := newMockFetchNetwork(0)
	validators := testutil.NewTestValidatorSet(4)

	// Create a batch with wrong digest
	tx := testutil.NewTestTransaction([]byte("test"))
	batch := &narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]{
		WorkerID:     0,
		ValidatorID:  1,
		Round:        0,
		Transactions: []*testutil.TestTransaction{tx},
	}
	batch.ComputeDigest(testutil.ComputeHash)
	correctDigest := batch.Digest

	// Corrupt the digest
	batch.Digest = testutil.ComputeHash([]byte("wrong"))

	network.fetchBatchFunc = func(from uint16, digest testutil.TestHash) (*narwhal.Batch[testutil.TestHash, *testutil.TestTransaction], error) {
		return batch, nil
	}

	cfg := narwhal.DefaultFetcherConfig()
	cfg.MaxRetries = 0 // No retries for faster test

	fetcher := narwhal.NewFetcher(
		cfg,
		network,
		storage,
		validators,
		testutil.ComputeHash,
		zap.NewNop(),
	)

	ctx := context.Background()
	_, err := fetcher.FetchBatch(ctx, correctDigest, 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to fetch batch")
}

func TestFetcher_PendingFetchCount(t *testing.T) {
	storage := testutil.NewTestStorage()
	network := newMockFetchNetwork(0)
	validators := testutil.NewTestValidatorSet(4)

	// Slow fetch to test pending count
	network.fetchBatchFunc = func(from uint16, digest testutil.TestHash) (*narwhal.Batch[testutil.TestHash, *testutil.TestTransaction], error) {
		time.Sleep(200 * time.Millisecond)
		return nil, fmt.Errorf("timeout")
	}

	fetcher := narwhal.NewFetcher(
		narwhal.DefaultFetcherConfig(),
		network,
		storage,
		validators,
		testutil.ComputeHash,
		zap.NewNop(),
	)

	// Initially no pending
	assert.Equal(t, 0, fetcher.PendingFetchCount())

	// Start a fetch
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		_, _ = fetcher.FetchBatch(ctx, testutil.ComputeHash([]byte("test")), 1)
		close(done)
	}()

	// Wait a bit for fetch to start
	time.Sleep(50 * time.Millisecond)
	assert.GreaterOrEqual(t, fetcher.PendingFetchCount(), 0)

	<-done
}
