package narwhal_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edgedlt/narwhal"
	"github.com/edgedlt/narwhal/internal/testutil"
	"github.com/edgedlt/narwhal/timer"
	"go.uber.org/zap"
)

// BenchmarkDAG_InsertCertificate benchmarks certificate insertion into the DAG.
// Uses a fixed number of rounds to avoid unbounded round advancement.
func BenchmarkDAG_InsertCertificate(b *testing.B) {
	validators := testutil.NewTestValidatorSet(4)

	// Pre-create certificates for a fixed number of rounds
	const maxRounds = 100
	certs := make([]*narwhal.Certificate[testutil.TestHash], maxRounds*4)
	for round := uint64(0); round < maxRounds; round++ {
		for author := uint16(0); author < 4; author++ {
			idx := int(round)*4 + int(author)
			header := &narwhal.Header[testutil.TestHash]{
				Author:    author,
				Round:     round,
				Timestamp: uint64(time.Now().UnixNano()) + uint64(idx),
			}
			header.ComputeDigest(testutil.ComputeHash)

			votes := make(map[uint16][]byte)
			for j := uint16(0); j < 3; j++ {
				sig, _ := validators.GetSigner(j).Sign(header.Digest.Bytes())
				votes[j] = sig
			}
			certs[idx] = narwhal.NewCertificate(header, votes)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create a fresh DAG for each iteration to avoid duplicates
		dag := narwhal.NewDAG[testutil.TestHash, *testutil.TestTransaction](validators, zap.NewNop())

		// Insert certificates sequentially (valid order)
		for _, cert := range certs {
			_ = dag.InsertCertificate(cert, nil)
		}
	}
}

// BenchmarkDAG_InsertSingleCertificate benchmarks inserting a single certificate.
// Uses a pool of pre-created certificates to avoid expensive setup scaling with b.N.
func BenchmarkDAG_InsertSingleCertificate(b *testing.B) {
	validators := testutil.NewTestValidatorSet(4)

	// Pre-create a fixed pool of certificates (use multiple rounds to avoid equivocation)
	const poolSize = 400 // 100 rounds * 4 validators
	certs := make([]*narwhal.Certificate[testutil.TestHash], poolSize)
	for round := uint64(0); round < 100; round++ {
		for author := uint16(0); author < 4; author++ {
			idx := int(round)*4 + int(author)
			header := &narwhal.Header[testutil.TestHash]{
				Author:    author,
				Round:     round,
				Timestamp: uint64(time.Now().UnixNano()) + uint64(idx)*1000,
			}
			header.ComputeDigest(testutil.ComputeHash)

			votes := make(map[uint16][]byte)
			for j := uint16(0); j < 3; j++ {
				sig, _ := validators.GetSigner(j).Sign(header.Digest.Bytes())
				votes[j] = sig
			}
			certs[idx] = narwhal.NewCertificate(header, votes)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create fresh DAG for each iteration to avoid duplicates
		dag := narwhal.NewDAG[testutil.TestHash, *testutil.TestTransaction](validators, zap.NewNop())
		// Insert one certificate
		_ = dag.InsertCertificate(certs[i%poolSize], nil)
	}
}

// BenchmarkWorker_AddTransaction benchmarks adding transactions to a worker.
func BenchmarkWorker_AddTransaction(b *testing.B) {
	var batchCount atomic.Int64

	worker := narwhal.NewWorker(narwhal.WorkerConfig[testutil.TestHash, *testutil.TestTransaction]{
		ID:           0,
		ValidatorID:  0,
		BatchSize:    100,
		BatchTimeout: time.Hour, // Don't trigger timeout
		HashFunc:     testutil.ComputeHash,
		OnBatch: func(batch *narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]) {
			batchCount.Add(1)
		},
		Logger: zap.NewNop(),
	})

	// Pre-create transactions
	txs := make([]*testutil.TestTransaction, b.N)
	for i := 0; i < b.N; i++ {
		txs[i] = testutil.NewTestTransaction([]byte(fmt.Sprintf("tx-%d", i)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		worker.AddTransaction(txs[i])
	}
	b.StopTimer()

	b.ReportMetric(float64(batchCount.Load()), "batches")
}

// BenchmarkWorker_AddTransaction_Bounded benchmarks bounded worker queue.
func BenchmarkWorker_AddTransaction_Bounded(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var batchCount atomic.Int64

	worker := narwhal.NewWorker(narwhal.WorkerConfig[testutil.TestHash, *testutil.TestTransaction]{
		ID:           0,
		ValidatorID:  0,
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		HashFunc:     testutil.ComputeHash,
		OnBatch: func(batch *narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]) {
			batchCount.Add(1)
		},
		Logger:     zap.NewNop(),
		MaxPending: 1000,
		DropOnFull: true,
	})

	// Start worker in background
	go worker.Run(ctx)

	// Pre-create transactions
	txs := make([]*testutil.TestTransaction, b.N)
	for i := 0; i < b.N; i++ {
		txs[i] = testutil.NewTestTransaction([]byte(fmt.Sprintf("tx-%d", i)))
	}

	b.ResetTimer()
	accepted := 0
	for i := 0; i < b.N; i++ {
		if worker.AddTransaction(txs[i]) {
			accepted++
		}
	}
	b.StopTimer()

	// Wait for batches to drain
	time.Sleep(50 * time.Millisecond)

	b.ReportMetric(float64(accepted), "accepted")
	b.ReportMetric(float64(batchCount.Load()), "batches")
}

// BenchmarkBatch_ComputeDigest benchmarks batch digest computation.
func BenchmarkBatch_ComputeDigest(b *testing.B) {
	// Create a batch with 100 transactions
	txs := make([]*testutil.TestTransaction, 100)
	for i := 0; i < 100; i++ {
		txs[i] = testutil.NewTestTransaction([]byte(fmt.Sprintf("tx-%d", i)))
	}

	batch := &narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]{
		WorkerID:     0,
		ValidatorID:  0,
		Round:        0,
		Transactions: txs,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch.ComputeDigest(testutil.ComputeHash)
	}
}

// BenchmarkHeader_ComputeDigest benchmarks header digest computation.
func BenchmarkHeader_ComputeDigest(b *testing.B) {
	// Create a header with parents
	parents := make([]testutil.TestHash, 4)
	for i := 0; i < 4; i++ {
		parents[i] = testutil.ComputeHash([]byte(fmt.Sprintf("parent-%d", i)))
	}

	batchRefs := make([]testutil.TestHash, 4)
	for i := 0; i < 4; i++ {
		batchRefs[i] = testutil.ComputeHash([]byte(fmt.Sprintf("batch-%d", i)))
	}

	header := &narwhal.Header[testutil.TestHash]{
		Author:    0,
		Round:     10,
		Epoch:     1,
		BatchRefs: batchRefs,
		Parents:   parents,
		Timestamp: uint64(time.Now().UnixMilli()),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		header.ComputeDigest(testutil.ComputeHash)
	}
}

// BenchmarkCertificate_Validate benchmarks certificate validation.
func BenchmarkCertificate_Validate(b *testing.B) {
	validators := testutil.NewTestValidatorSet(4)

	header := &narwhal.Header[testutil.TestHash]{
		Author:    0,
		Round:     0,
		Timestamp: uint64(time.Now().UnixMilli()),
	}
	header.ComputeDigest(testutil.ComputeHash)

	votes := make(map[uint16][]byte)
	for i := uint16(0); i < 3; i++ {
		sig, _ := validators.GetSigner(i).Sign(header.Digest.Bytes())
		votes[i] = sig
	}
	cert := narwhal.NewCertificate(header, votes)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cert.Validate(validators)
	}
}

// BenchmarkDAG_GetUncommitted benchmarks retrieving uncommitted certificates.
func BenchmarkDAG_GetUncommitted(b *testing.B) {
	validators := testutil.NewTestValidatorSet(4)
	dag := narwhal.NewDAG[testutil.TestHash, *testutil.TestTransaction](validators, zap.NewNop())

	// Insert certificates for 25 rounds (100 total certs)
	for round := uint64(0); round < 25; round++ {
		for author := uint16(0); author < 4; author++ {
			header := &narwhal.Header[testutil.TestHash]{
				Author:    author,
				Round:     round,
				Timestamp: uint64(time.Now().UnixNano()) + uint64(round)*10 + uint64(author),
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
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dag.GetUncommitted()
	}
}

// BenchmarkDAG_IsCertified benchmarks checking if a certificate exists.
func BenchmarkDAG_IsCertified(b *testing.B) {
	validators := testutil.NewTestValidatorSet(4)
	dag := narwhal.NewDAG[testutil.TestHash, *testutil.TestTransaction](validators, zap.NewNop())

	// Insert certificates and collect digests
	digests := make([]testutil.TestHash, 100)
	idx := 0
	for round := uint64(0); round < 25; round++ {
		for author := uint16(0); author < 4; author++ {
			header := &narwhal.Header[testutil.TestHash]{
				Author:    author,
				Round:     round,
				Timestamp: uint64(time.Now().UnixNano()) + uint64(round)*10 + uint64(author),
			}
			header.ComputeDigest(testutil.ComputeHash)
			digests[idx] = header.Digest
			idx++

			votes := make(map[uint16][]byte)
			for j := uint16(0); j < 3; j++ {
				sig, _ := validators.GetSigner(j).Sign(header.Digest.Bytes())
				votes[j] = sig
			}
			cert := narwhal.NewCertificate(header, votes)
			_ = dag.InsertCertificate(cert, nil)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dag.IsCertified(digests[i%100])
	}
}

// ============================================================================
// DAG Cache Benchmarks
// ============================================================================

// BenchmarkDAG_GetVertex_Cached benchmarks vertex retrieval with cache enabled.
func BenchmarkDAG_GetVertex_Cached(b *testing.B) {
	validators := testutil.NewTestValidatorSet(4)
	cacheConfig := &narwhal.DAGCacheConfig{Enabled: true, Capacity: 10000}
	dag := narwhal.NewDAGWithCache[testutil.TestHash, *testutil.TestTransaction](
		validators, nil, nil, nil, cacheConfig, zap.NewNop())

	// Insert certificates and collect digests
	digests := insertTestCertificates(b, dag, validators, 25, 4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dag.GetVertex(digests[i%len(digests)])
	}
}

// BenchmarkDAG_GetVertex_Uncached benchmarks vertex retrieval without cache.
func BenchmarkDAG_GetVertex_Uncached(b *testing.B) {
	validators := testutil.NewTestValidatorSet(4)
	cacheConfig := &narwhal.DAGCacheConfig{Enabled: false}
	dag := narwhal.NewDAGWithCache[testutil.TestHash, *testutil.TestTransaction](
		validators, nil, nil, nil, cacheConfig, zap.NewNop())

	// Insert certificates and collect digests
	digests := insertTestCertificates(b, dag, validators, 25, 4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dag.GetVertex(digests[i%len(digests)])
	}
}

// BenchmarkDAG_GetCertificate_Cached benchmarks certificate retrieval with cache enabled.
func BenchmarkDAG_GetCertificate_Cached(b *testing.B) {
	validators := testutil.NewTestValidatorSet(4)
	cacheConfig := &narwhal.DAGCacheConfig{Enabled: true, Capacity: 10000}
	dag := narwhal.NewDAGWithCache[testutil.TestHash, *testutil.TestTransaction](
		validators, nil, nil, nil, cacheConfig, zap.NewNop())

	digests := insertTestCertificates(b, dag, validators, 25, 4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dag.GetCertificate(digests[i%len(digests)])
	}
}

// BenchmarkDAG_GetCertificate_Uncached benchmarks certificate retrieval without cache.
func BenchmarkDAG_GetCertificate_Uncached(b *testing.B) {
	validators := testutil.NewTestValidatorSet(4)
	cacheConfig := &narwhal.DAGCacheConfig{Enabled: false}
	dag := narwhal.NewDAGWithCache[testutil.TestHash, *testutil.TestTransaction](
		validators, nil, nil, nil, cacheConfig, zap.NewNop())

	digests := insertTestCertificates(b, dag, validators, 25, 4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dag.GetCertificate(digests[i%len(digests)])
	}
}

// BenchmarkDAG_IsCertified_Cached benchmarks IsCertified check with cache enabled.
func BenchmarkDAG_IsCertified_Cached(b *testing.B) {
	validators := testutil.NewTestValidatorSet(4)
	cacheConfig := &narwhal.DAGCacheConfig{Enabled: true, Capacity: 10000}
	dag := narwhal.NewDAGWithCache[testutil.TestHash, *testutil.TestTransaction](
		validators, nil, nil, nil, cacheConfig, zap.NewNop())

	digests := insertTestCertificates(b, dag, validators, 25, 4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dag.IsCertified(digests[i%len(digests)])
	}
}

// BenchmarkDAG_IsCertified_Uncached benchmarks IsCertified check without cache.
func BenchmarkDAG_IsCertified_Uncached(b *testing.B) {
	validators := testutil.NewTestValidatorSet(4)
	cacheConfig := &narwhal.DAGCacheConfig{Enabled: false}
	dag := narwhal.NewDAGWithCache[testutil.TestHash, *testutil.TestTransaction](
		validators, nil, nil, nil, cacheConfig, zap.NewNop())

	digests := insertTestCertificates(b, dag, validators, 25, 4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dag.IsCertified(digests[i%len(digests)])
	}
}

// BenchmarkDAG_CacheHitRate benchmarks cache hit rate under repeated access.
func BenchmarkDAG_CacheHitRate(b *testing.B) {
	validators := testutil.NewTestValidatorSet(4)
	cacheConfig := &narwhal.DAGCacheConfig{Enabled: true, Capacity: 100} // Small cache
	dag := narwhal.NewDAGWithCache[testutil.TestHash, *testutil.TestTransaction](
		validators, nil, nil, nil, cacheConfig, zap.NewNop())

	// Insert 200 certificates (2x cache capacity)
	digests := insertTestCertificates(b, dag, validators, 50, 4)

	// Access pattern: 80% hot set (first 20 digests), 20% cold set
	hotSetSize := 20
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if i%5 == 0 {
			// 20% cold access
			dag.GetVertex(digests[(hotSetSize+i)%len(digests)])
		} else {
			// 80% hot access
			dag.GetVertex(digests[i%hotSetSize])
		}
	}

	b.StopTimer()
	stats := dag.Stats()
	if stats.CacheStats != nil {
		b.ReportMetric(stats.CacheStats.HitRate*100, "hit%")
	}
}

// BenchmarkDAG_ConcurrentAccess_Cached benchmarks concurrent vertex access with cache.
func BenchmarkDAG_ConcurrentAccess_Cached(b *testing.B) {
	validators := testutil.NewTestValidatorSet(4)
	cacheConfig := &narwhal.DAGCacheConfig{Enabled: true, Capacity: 10000}
	dag := narwhal.NewDAGWithCache[testutil.TestHash, *testutil.TestTransaction](
		validators, nil, nil, nil, cacheConfig, zap.NewNop())

	digests := insertTestCertificates(b, dag, validators, 25, 4)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			dag.GetVertex(digests[i%len(digests)])
			i++
		}
	})
}

// BenchmarkDAG_ConcurrentAccess_Uncached benchmarks concurrent vertex access without cache.
func BenchmarkDAG_ConcurrentAccess_Uncached(b *testing.B) {
	validators := testutil.NewTestValidatorSet(4)
	cacheConfig := &narwhal.DAGCacheConfig{Enabled: false}
	dag := narwhal.NewDAGWithCache[testutil.TestHash, *testutil.TestTransaction](
		validators, nil, nil, nil, cacheConfig, zap.NewNop())

	digests := insertTestCertificates(b, dag, validators, 25, 4)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			dag.GetVertex(digests[i%len(digests)])
			i++
		}
	})
}

// insertTestCertificates is a helper that inserts test certificates and returns their digests.
// Uses pre-computed signatures for speed in benchmarks.
func insertTestCertificates(b testing.TB, dag *narwhal.DAG[testutil.TestHash, *testutil.TestTransaction],
	validators *testutil.TestValidatorSet, numRounds int, numAuthors int) []testutil.TestHash {

	digests := make([]testutil.TestHash, 0, numRounds*numAuthors)

	// Pre-create a signature once for reuse (benchmarks don't need valid signatures)
	dummySig := make([]byte, 64)
	for i := range dummySig {
		dummySig[i] = byte(i)
	}

	for round := uint64(0); round < uint64(numRounds); round++ {
		for author := uint16(0); author < uint16(numAuthors); author++ {
			header := &narwhal.Header[testutil.TestHash]{
				Author:    author,
				Round:     round,
				Timestamp: uint64(round)*10 + uint64(author),
			}
			header.ComputeDigest(testutil.ComputeHash)
			digests = append(digests, header.Digest)

			// Use dummy signatures for speed
			votes := make(map[uint16][]byte)
			for j := uint16(0); j < 3; j++ {
				votes[j] = dummySig
			}
			cert := narwhal.NewCertificate(header, votes)
			_ = dag.InsertCertificate(cert, nil)
		}
	}
	return digests
}

// BenchmarkFetcher_FetchBatch_LocalStorage benchmarks batch fetching from local storage.
func BenchmarkFetcher_FetchBatch_LocalStorage(b *testing.B) {
	validators := testutil.NewTestValidatorSet(4)
	storage := testutil.NewTestStorage()
	network := testutil.NewTestNetwork(0)

	// Pre-store batches
	batches := make([]*narwhal.Batch[testutil.TestHash, *testutil.TestTransaction], 100)
	for i := 0; i < 100; i++ {
		tx := testutil.NewTestTransaction([]byte(fmt.Sprintf("tx-%d", i)))
		batch := &narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]{
			WorkerID:     0,
			ValidatorID:  0,
			Round:        0,
			Transactions: []*testutil.TestTransaction{tx},
		}
		batch.ComputeDigest(testutil.ComputeHash)
		batches[i] = batch
		_ = storage.PutBatch(batch)
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = fetcher.FetchBatch(ctx, batches[i%100].Digest, 0)
	}
}

// BenchmarkSignatureVerification benchmarks Ed25519 signature verification.
func BenchmarkSignatureVerification(b *testing.B) {
	validators := testutil.NewTestValidatorSet(4)
	signer := validators.GetSigner(0)
	pubKey, _ := validators.GetByIndex(0)

	message := []byte("test message for signature verification benchmark")
	signature, _ := signer.Sign(message)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pubKey.Verify(message, signature)
	}
}

// BenchmarkSignatureSigning benchmarks Ed25519 signature creation.
func BenchmarkSignatureSigning(b *testing.B) {
	validators := testutil.NewTestValidatorSet(4)
	signer := validators.GetSigner(0)

	message := []byte("test message for signature signing benchmark")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = signer.Sign(message)
	}
}

// BenchmarkHash_Computation benchmarks SHA256 hash computation.
func BenchmarkHash_Computation(b *testing.B) {
	data := make([]byte, 1024) // 1KB of data
	for i := range data {
		data[i] = byte(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testutil.ComputeHash(data)
	}
}

// BenchmarkNarwhal_TransactionSubmission measures transaction submission throughput.
// This is a short benchmark that doesn't wait for full consensus.
func BenchmarkNarwhal_TransactionSubmission(b *testing.B) {
	// Note: b.N is controlled by the benchmark framework, don't assign to it
	const numNodes = 4
	validators := testutil.NewTestValidatorSet(numNodes)

	// Create networks
	networks := make([]*testutil.TestNetwork, numNodes)
	for i := 0; i < numNodes; i++ {
		networks[i] = testutil.NewTestNetwork(uint16(i))
	}
	for i := 0; i < numNodes; i++ {
		for j := i + 1; j < numNodes; j++ {
			networks[i].Connect(networks[j])
		}
	}

	// Create nodes with bounded queues (drop on full for benchmark)
	nodes := make([]*narwhal.Narwhal[testutil.TestHash, *testutil.TestTransaction], numNodes)
	for i := 0; i < numNodes; i++ {
		storage := testutil.NewTestStorage()
		cfg, _ := narwhal.NewConfig(
			narwhal.WithMyIndex[testutil.TestHash, *testutil.TestTransaction](uint16(i)),
			narwhal.WithValidators[testutil.TestHash, *testutil.TestTransaction](validators),
			narwhal.WithSigner[testutil.TestHash, *testutil.TestTransaction](validators.GetSigner(uint16(i))),
			narwhal.WithStorage[testutil.TestHash, *testutil.TestTransaction](storage),
			narwhal.WithNetwork[testutil.TestHash, *testutil.TestTransaction](networks[i]),
			narwhal.WithTimer[testutil.TestHash, *testutil.TestTransaction](timer.NewRealTimer(time.Millisecond)),
			narwhal.WithBatchSize[testutil.TestHash, *testutil.TestTransaction](100),
			narwhal.WithBatchTimeout[testutil.TestHash, *testutil.TestTransaction](10*time.Millisecond),
			narwhal.WithHeaderTimeout[testutil.TestHash, *testutil.TestTransaction](50*time.Millisecond),
			narwhal.WithLogger[testutil.TestHash, *testutil.TestTransaction](zap.NewNop()),
			narwhal.WithMaxPendingTransactions[testutil.TestHash, *testutil.TestTransaction](10000),
			narwhal.WithDropOnFull[testutil.TestHash, *testutil.TestTransaction](true),
		)

		node, _ := narwhal.New(cfg, testutil.ComputeHash)
		nodes[i] = node
		_ = node.Start()
	}

	// Pre-create transactions
	txs := make([]*testutil.TestTransaction, b.N)
	for i := 0; i < b.N; i++ {
		txs[i] = testutil.NewTestTransaction([]byte(fmt.Sprintf("bench-tx-%d", i)))
	}

	b.ResetTimer()

	// Submit transactions round-robin
	for i := 0; i < b.N; i++ {
		nodes[i%numNodes].AddTransaction(txs[i])
	}

	b.StopTimer()

	// Cleanup - stop all nodes first, then close networks
	for _, node := range nodes {
		node.Stop()
	}
	// Small delay to let any in-flight operations complete
	time.Sleep(10 * time.Millisecond)
	for _, network := range networks {
		network.Close()
	}
}

// BenchmarkNarwhal_ConcurrentSubmission benchmarks concurrent transaction submission.
func BenchmarkNarwhal_ConcurrentSubmission(b *testing.B) {
	const numNodes = 4

	validators := testutil.NewTestValidatorSet(numNodes)

	networks := make([]*testutil.TestNetwork, numNodes)
	for i := 0; i < numNodes; i++ {
		networks[i] = testutil.NewTestNetwork(uint16(i))
	}
	for i := 0; i < numNodes; i++ {
		for j := i + 1; j < numNodes; j++ {
			networks[i].Connect(networks[j])
		}
	}

	nodes := make([]*narwhal.Narwhal[testutil.TestHash, *testutil.TestTransaction], numNodes)
	for i := 0; i < numNodes; i++ {
		storage := testutil.NewTestStorage()
		cfg, _ := narwhal.NewConfig(
			narwhal.WithMyIndex[testutil.TestHash, *testutil.TestTransaction](uint16(i)),
			narwhal.WithValidators[testutil.TestHash, *testutil.TestTransaction](validators),
			narwhal.WithSigner[testutil.TestHash, *testutil.TestTransaction](validators.GetSigner(uint16(i))),
			narwhal.WithStorage[testutil.TestHash, *testutil.TestTransaction](storage),
			narwhal.WithNetwork[testutil.TestHash, *testutil.TestTransaction](networks[i]),
			narwhal.WithTimer[testutil.TestHash, *testutil.TestTransaction](timer.NewRealTimer(time.Millisecond)),
			narwhal.WithBatchSize[testutil.TestHash, *testutil.TestTransaction](100),
			narwhal.WithBatchTimeout[testutil.TestHash, *testutil.TestTransaction](10*time.Millisecond),
			narwhal.WithHeaderTimeout[testutil.TestHash, *testutil.TestTransaction](50*time.Millisecond),
			narwhal.WithLogger[testutil.TestHash, *testutil.TestTransaction](zap.NewNop()),
			narwhal.WithMaxPendingTransactions[testutil.TestHash, *testutil.TestTransaction](10000),
			narwhal.WithDropOnFull[testutil.TestHash, *testutil.TestTransaction](true),
		)

		node, _ := narwhal.New(cfg, testutil.ComputeHash)
		nodes[i] = node
		_ = node.Start()
	}

	defer func() {
		// Stop all nodes first, then close networks
		for _, node := range nodes {
			node.Stop()
		}
		time.Sleep(10 * time.Millisecond)
		for _, network := range networks {
			network.Close()
		}
	}()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			tx := testutil.NewTestTransaction([]byte(fmt.Sprintf("concurrent-tx-%d", i)))
			nodes[i%numNodes].AddTransaction(tx)
			i++
		}
	})
}

// BenchmarkStorage_PutBatch benchmarks batch storage.
func BenchmarkStorage_PutBatch(b *testing.B) {
	storage := testutil.NewTestStorage()

	// Pre-create batches
	batches := make([]*narwhal.Batch[testutil.TestHash, *testutil.TestTransaction], b.N)
	for i := 0; i < b.N; i++ {
		tx := testutil.NewTestTransaction([]byte(fmt.Sprintf("tx-%d", i)))
		batch := &narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]{
			WorkerID:     0,
			ValidatorID:  0,
			Round:        uint64(i),
			Transactions: []*testutil.TestTransaction{tx},
		}
		batch.ComputeDigest(testutil.ComputeHash)
		batches[i] = batch
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = storage.PutBatch(batches[i])
	}
}

// BenchmarkStorage_GetBatch benchmarks batch retrieval.
func BenchmarkStorage_GetBatch(b *testing.B) {
	storage := testutil.NewTestStorage()

	// Pre-store batches
	batches := make([]*narwhal.Batch[testutil.TestHash, *testutil.TestTransaction], 100)
	for i := 0; i < 100; i++ {
		tx := testutil.NewTestTransaction([]byte(fmt.Sprintf("tx-%d", i)))
		batch := &narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]{
			WorkerID:     0,
			ValidatorID:  0,
			Round:        uint64(i),
			Transactions: []*testutil.TestTransaction{tx},
		}
		batch.ComputeDigest(testutil.ComputeHash)
		batches[i] = batch
		_ = storage.PutBatch(batch)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = storage.GetBatch(batches[i%100].Digest)
	}
}
