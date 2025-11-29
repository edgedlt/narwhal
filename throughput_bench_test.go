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
	"github.com/edgedlt/narwhal/timer"
	"go.uber.org/zap"
)

// ThroughputBenchConfig configures a throughput benchmark run.
type ThroughputBenchConfig struct {
	NumNodes      int
	Duration      time.Duration
	TxBatchSize   int           // Number of txs to submit per batch
	TxSize        int           // Size of each transaction in bytes
	BatchSize     int           // Batch size config for workers
	BatchTimeout  time.Duration // Batch timeout for workers
	HeaderTimeout time.Duration // Header timeout for primaries
}

// DefaultThroughputBenchConfig returns config similar to Facebook reference.
func DefaultThroughputBenchConfig() ThroughputBenchConfig {
	return ThroughputBenchConfig{
		NumNodes:      4,
		Duration:      5 * time.Second,
		TxBatchSize:   10,
		TxSize:        512, // 512 bytes like Facebook reference
		BatchSize:     500, // ~500KB batches like reference (500 * 1000B)
		BatchTimeout:  100 * time.Millisecond,
		HeaderTimeout: 100 * time.Millisecond,
	}
}

// ThroughputBenchResult contains metrics from a throughput benchmark run.
type ThroughputBenchResult struct {
	Config ThroughputBenchConfig

	Duration           time.Duration
	TransactionsSubmit int64
	CertificatesFormed int64
	RoundsAdvanced     uint64
	StartRound         uint64
	EndRound           uint64
	BytesSubmitted     int64

	// Derived metrics
	SubmitTPS      float64 // Transactions submitted per second
	ConsensusTPS   float64 // Transactions certified per second (estimated from certs)
	CertsPerSecond float64
	AvgRoundTime   time.Duration
	ThroughputBPS  float64 // Bytes per second
}

func (r *ThroughputBenchResult) String() string {
	return fmt.Sprintf(`
Throughput Benchmark Results
============================
Config:
  Nodes:           %d
  TX Size:         %d bytes
  Batch Size:      %d txs
  Batch Timeout:   %v
  Header Timeout:  %v

Duration:               %v
Transactions Submitted: %d
Bytes Submitted:        %d (%.2f MB)
Certificates Formed:    %d
Rounds Advanced:        %d (from %d to %d)

Performance Metrics:
  Submit TPS:        %.2f tx/s
  Consensus TPS:     %.2f tx/s (estimated)
  Throughput:        %.2f MB/s
  Certs/Second:      %.2f
  Avg Round Time:    %v

Comparison to Facebook Reference (4 nodes, 512B tx):
  Reference TPS:     ~46,000 tx/s
  Reference Latency: ~464 ms
`, r.Config.NumNodes, r.Config.TxSize, r.Config.BatchSize,
		r.Config.BatchTimeout, r.Config.HeaderTimeout,
		r.Duration, r.TransactionsSubmit, r.BytesSubmitted,
		float64(r.BytesSubmitted)/(1024*1024),
		r.CertificatesFormed, r.RoundsAdvanced,
		r.StartRound, r.EndRound, r.SubmitTPS, r.ConsensusTPS,
		r.ThroughputBPS/(1024*1024), r.CertsPerSecond, r.AvgRoundTime)
}

// runThroughputBench runs a real throughput benchmark for a specified duration.
func runThroughputBench(t *testing.T, numNodes int, duration time.Duration, txBatchSize int) *ThroughputBenchResult {
	cfg := DefaultThroughputBenchConfig()
	cfg.NumNodes = numNodes
	cfg.Duration = duration
	cfg.TxBatchSize = txBatchSize
	return runThroughputBenchWithConfig(t, cfg)
}

// runThroughputBenchWithConfig runs a benchmark with full config control.
func runThroughputBenchWithConfig(t *testing.T, cfg ThroughputBenchConfig) *ThroughputBenchResult {
	t.Helper()

	validators := testutil.NewTestValidatorSet(cfg.NumNodes)

	// Create networks
	networks := make([]*testutil.TestNetwork, cfg.NumNodes)
	for i := 0; i < cfg.NumNodes; i++ {
		networks[i] = testutil.NewTestNetwork(uint16(i))
	}
	for i := 0; i < cfg.NumNodes; i++ {
		for j := i + 1; j < cfg.NumNodes; j++ {
			networks[i].Connect(networks[j])
		}
	}

	// Track metrics using hooks
	var certsFormed atomic.Int64
	var roundsAdvanced atomic.Int64
	var latestRound atomic.Uint64

	hooks := &narwhal.Hooks[testutil.TestHash, *testutil.TestTransaction]{
		OnCertificateFormed: func(e narwhal.CertificateFormedEvent[testutil.TestHash]) {
			certsFormed.Add(1)
		},
		OnRoundAdvanced: func(e narwhal.RoundAdvancedEvent) {
			roundsAdvanced.Add(1)
			latestRound.Store(e.NewRound)
		},
	}

	// Create nodes
	nodes := make([]*narwhal.Narwhal[testutil.TestHash, *testutil.TestTransaction], cfg.NumNodes)
	for i := 0; i < cfg.NumNodes; i++ {
		storage := testutil.NewTestStorage()
		nodeCfg, err := narwhal.NewConfig(
			narwhal.WithMyIndex[testutil.TestHash, *testutil.TestTransaction](uint16(i)),
			narwhal.WithValidators[testutil.TestHash, *testutil.TestTransaction](validators),
			narwhal.WithSigner[testutil.TestHash, *testutil.TestTransaction](validators.GetSigner(uint16(i))),
			narwhal.WithStorage[testutil.TestHash, *testutil.TestTransaction](storage),
			narwhal.WithNetwork[testutil.TestHash, *testutil.TestTransaction](networks[i]),
			narwhal.WithTimer[testutil.TestHash, *testutil.TestTransaction](timer.NewRealTimer(time.Millisecond)),
			narwhal.WithBatchSize[testutil.TestHash, *testutil.TestTransaction](cfg.BatchSize),
			narwhal.WithBatchTimeout[testutil.TestHash, *testutil.TestTransaction](cfg.BatchTimeout),
			narwhal.WithHeaderTimeout[testutil.TestHash, *testutil.TestTransaction](cfg.HeaderTimeout),
			narwhal.WithLogger[testutil.TestHash, *testutil.TestTransaction](zap.NewNop()),
			narwhal.WithMaxPendingTransactions[testutil.TestHash, *testutil.TestTransaction](50000),
			narwhal.WithDropOnFull[testutil.TestHash, *testutil.TestTransaction](true),
			narwhal.WithHooks[testutil.TestHash, *testutil.TestTransaction](hooks),
		)
		if err != nil {
			t.Fatalf("failed to create config: %v", err)
		}

		node, err := narwhal.New(nodeCfg, testutil.ComputeHash)
		if err != nil {
			t.Fatalf("failed to create node: %v", err)
		}
		nodes[i] = node
	}

	// Start all nodes
	for _, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf("failed to start node: %v", err)
		}
	}

	// Record start state
	startRound := nodes[0].CurrentRound()
	startTime := time.Now()

	// Submit transactions continuously
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer cancel()

	var txSubmitted atomic.Int64
	var bytesSubmitted atomic.Int64
	var wg sync.WaitGroup

	// Use multiple goroutines to submit transactions
	numSubmitters := 4
	for s := 0; s < numSubmitters; s++ {
		wg.Add(1)
		go func(submitterID int) {
			defer wg.Done()
			txID := 0
			for {
				select {
				case <-ctx.Done():
					return
				default:
					// Submit a batch of transactions with realistic sizes
					for j := 0; j < cfg.TxBatchSize; j++ {
						tx := testutil.NewTestTransactionSized(
							[]byte(fmt.Sprintf("bench-%d-%d", submitterID, txID)),
							cfg.TxSize,
						)
						nodes[txID%cfg.NumNodes].AddTransaction(tx)
						txID++
					}
					txSubmitted.Add(int64(cfg.TxBatchSize))
					bytesSubmitted.Add(int64(cfg.TxBatchSize * cfg.TxSize))
				}
			}
		}(s)
	}

	// Wait for duration
	<-ctx.Done()
	wg.Wait()

	// Allow time for in-flight operations to complete
	time.Sleep(200 * time.Millisecond)

	// Record end state
	endTime := time.Now()
	endRound := latestRound.Load()
	actualDuration := endTime.Sub(startTime)

	// Cleanup
	for _, node := range nodes {
		node.Stop()
	}
	time.Sleep(10 * time.Millisecond)
	for _, network := range networks {
		network.Close()
	}

	// Calculate metrics
	submitted := txSubmitted.Load()
	bytes := bytesSubmitted.Load()
	certs := certsFormed.Load()
	rounds := roundsAdvanced.Load()
	roundsDiff := endRound - startRound

	result := &ThroughputBenchResult{
		Config:             cfg,
		Duration:           actualDuration,
		TransactionsSubmit: submitted,
		BytesSubmitted:     bytes,
		CertificatesFormed: certs,
		RoundsAdvanced:     uint64(rounds),
		StartRound:         startRound,
		EndRound:           endRound,
		SubmitTPS:          float64(submitted) / actualDuration.Seconds(),
		CertsPerSecond:     float64(certs) / actualDuration.Seconds(),
		ThroughputBPS:      float64(bytes) / actualDuration.Seconds(),
	}

	// Estimate consensus TPS based on batch size config
	txPerCert := float64(cfg.BatchSize) // Use configured batch size
	result.ConsensusTPS = result.CertsPerSecond * txPerCert

	if roundsDiff > 0 {
		result.AvgRoundTime = time.Duration(float64(actualDuration) / float64(roundsDiff))
	}

	return result
}

// TestThroughputBench_Short runs a short (1 second) throughput benchmark.
func TestThroughputBench_Short(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput benchmark in short mode")
	}

	result := runThroughputBench(t, 4, 1*time.Second, 10)
	t.Log(result.String())

	// Sanity checks
	if result.TransactionsSubmit == 0 {
		t.Error("expected some transactions to be submitted")
	}
	if result.CertificatesFormed == 0 {
		t.Error("expected some certificates to be formed")
	}
}

// TestThroughputBench_Medium runs a medium (5 second) throughput benchmark.
func TestThroughputBench_Medium(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput benchmark in short mode")
	}

	result := runThroughputBench(t, 4, 5*time.Second, 50)
	t.Log(result.String())

	// Performance expectations for 4 nodes
	if result.SubmitTPS < 1000 {
		t.Logf("Warning: submit TPS (%.2f) is lower than expected", result.SubmitTPS)
	}
	if result.CertsPerSecond < 5 {
		t.Logf("Warning: certificates per second (%.2f) is lower than expected", result.CertsPerSecond)
	}
}

// TestThroughputBench_ReferenceConfig runs a benchmark matching Facebook reference config.
// Reference config: 4 nodes, 512B transactions, 500KB batches, 100ms timeouts
func TestThroughputBench_ReferenceConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput benchmark in short mode")
	}

	cfg := DefaultThroughputBenchConfig()
	cfg.Duration = 10 * time.Second // Longer duration for more stable results

	result := runThroughputBenchWithConfig(t, cfg)
	t.Log(result.String())

	// Compare to reference numbers
	t.Logf("\n=== Comparison to Facebook Reference ===")
	t.Logf("Consensus TPS: %.2f (reference: ~46,000)", result.ConsensusTPS)
	t.Logf("Throughput: %.2f MB/s (reference: ~23 MB/s)", result.ThroughputBPS/(1024*1024))
	t.Logf("Certs/Second: %.2f", result.CertsPerSecond)

	// Note: Our local test with in-memory network can't match real network performance
	// The reference numbers are from a real distributed deployment with actual latency
}

// TestThroughputBench_ScaleNodes tests throughput scaling with different node counts.
func TestThroughputBench_ScaleNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput benchmark in short mode")
	}

	nodeCounts := []int{4, 7, 10}
	results := make(map[int]*ThroughputBenchResult)

	for _, n := range nodeCounts {
		t.Run(fmt.Sprintf("%d_nodes", n), func(t *testing.T) {
			result := runThroughputBench(t, n, 2*time.Second, 20)
			results[n] = result
			t.Log(result.String())
		})
	}

	// Print comparison
	t.Log("\n=== Node Scaling Comparison ===")
	for _, n := range nodeCounts {
		r := results[n]
		t.Logf("%2d nodes: %.2f submit TPS, %.2f certs/s, %v avg round", n, r.SubmitTPS, r.CertsPerSecond, r.AvgRoundTime)
	}
}

// TestLatencyBench measures header-to-certificate latency.
func TestLatencyBench(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping latency benchmark in short mode")
	}

	numNodes := 4
	duration := 3 * time.Second

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

	// Track latencies
	var mu sync.Mutex
	latencies := make([]time.Duration, 0, 1000)

	hooks := &narwhal.Hooks[testutil.TestHash, *testutil.TestTransaction]{
		OnCertificateFormed: func(e narwhal.CertificateFormedEvent[testutil.TestHash]) {
			mu.Lock()
			latencies = append(latencies, e.Latency)
			mu.Unlock()
		},
	}

	// Create nodes
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
			narwhal.WithBatchSize[testutil.TestHash, *testutil.TestTransaction](50),
			narwhal.WithBatchTimeout[testutil.TestHash, *testutil.TestTransaction](10*time.Millisecond),
			narwhal.WithHeaderTimeout[testutil.TestHash, *testutil.TestTransaction](50*time.Millisecond),
			narwhal.WithLogger[testutil.TestHash, *testutil.TestTransaction](zap.NewNop()),
			narwhal.WithHooks[testutil.TestHash, *testutil.TestTransaction](hooks),
		)
		node, _ := narwhal.New(cfg, testutil.ComputeHash)
		nodes[i] = node
		_ = node.Start()
	}

	// Submit transactions
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	go func() {
		txID := 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
				tx := testutil.NewTestTransaction([]byte(fmt.Sprintf("lat-%d", txID)))
				nodes[txID%numNodes].AddTransaction(tx)
				txID++
				time.Sleep(100 * time.Microsecond) // Slow down to reduce noise
			}
		}
	}()

	<-ctx.Done()
	time.Sleep(200 * time.Millisecond)

	// Cleanup
	for _, node := range nodes {
		node.Stop()
	}
	for _, network := range networks {
		network.Close()
	}

	// Analyze latencies
	mu.Lock()
	defer mu.Unlock()

	if len(latencies) == 0 {
		t.Fatal("no latency samples collected")
	}

	// Calculate statistics
	var sum time.Duration
	min := latencies[0]
	max := latencies[0]
	for _, l := range latencies {
		sum += l
		if l < min {
			min = l
		}
		if l > max {
			max = l
		}
	}
	avg := time.Duration(int64(sum) / int64(len(latencies)))

	// Find p50, p95, p99
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	// Simple bubble sort for small datasets
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j] < sorted[i] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}
	p50 := sorted[len(sorted)*50/100]
	p95 := sorted[len(sorted)*95/100]
	p99 := sorted[len(sorted)*99/100]

	t.Logf(`
Latency Benchmark Results
=========================
Samples:     %d
Min:         %v
Max:         %v
Average:     %v
P50:         %v
P95:         %v
P99:         %v
`, len(latencies), min, max, avg, p50, p95, p99)
}

// TestEndToEndLatency measures end-to-end latency from tx submission to certificate.
// This is the key metric for comparing to the Facebook reference (~557ms).
func TestEndToEndLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping latency benchmark in short mode")
	}

	numNodes := 4
	duration := 5 * time.Second

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

	// Track transaction submission times
	var txMu sync.Mutex
	pendingTxs := make(map[string]time.Time) // hash -> submit time
	e2eLatencies := make([]time.Duration, 0, 10000)

	hooks := &narwhal.Hooks[testutil.TestHash, *testutil.TestTransaction]{
		OnBatchCreated: func(e narwhal.BatchCreatedEvent[testutil.TestHash, *testutil.TestTransaction]) {
			// When a batch is created, we can calculate tx-to-batch latency
			batchTime := time.Now()
			txMu.Lock()
			for _, tx := range e.Batch.Transactions {
				hashStr := tx.Hash().String()
				if submitTime, ok := pendingTxs[hashStr]; ok {
					latency := batchTime.Sub(submitTime)
					e2eLatencies = append(e2eLatencies, latency)
					delete(pendingTxs, hashStr)
				}
			}
			txMu.Unlock()
		},
	}

	// Create nodes
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
			narwhal.WithBatchTimeout[testutil.TestHash, *testutil.TestTransaction](100*time.Millisecond),
			narwhal.WithHeaderTimeout[testutil.TestHash, *testutil.TestTransaction](100*time.Millisecond),
			narwhal.WithLogger[testutil.TestHash, *testutil.TestTransaction](zap.NewNop()),
			narwhal.WithHooks[testutil.TestHash, *testutil.TestTransaction](hooks),
		)
		node, _ := narwhal.New(cfg, testutil.ComputeHash)
		nodes[i] = node
		_ = node.Start()
	}

	// Submit transactions with timing
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	go func() {
		txID := 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
				tx := testutil.NewTestTransactionSized(
					[]byte(fmt.Sprintf("e2e-%d", txID)),
					512,
				)
				// Track submission time
				txMu.Lock()
				pendingTxs[tx.Hash().String()] = time.Now()
				txMu.Unlock()

				nodes[txID%numNodes].AddTransaction(tx)
				txID++

				// Submit at a controlled rate to avoid overwhelming
				time.Sleep(50 * time.Microsecond)
			}
		}
	}()

	<-ctx.Done()
	time.Sleep(500 * time.Millisecond) // Wait for final batches

	// Cleanup
	for _, node := range nodes {
		node.Stop()
	}
	for _, network := range networks {
		network.Close()
	}

	// Analyze latencies (tx submission to batch creation)
	txMu.Lock()
	defer txMu.Unlock()

	if len(e2eLatencies) == 0 {
		t.Log("No latency samples collected - this measures tx-to-batch latency")
		t.Log("For full e2e (tx-to-certificate), add batch timeout + header timeout")
		return
	}

	// Calculate statistics
	var sum time.Duration
	min := e2eLatencies[0]
	max := e2eLatencies[0]
	for _, l := range e2eLatencies {
		sum += l
		if l < min {
			min = l
		}
		if l > max {
			max = l
		}
	}
	avg := time.Duration(int64(sum) / int64(len(e2eLatencies)))

	// Sort for percentiles
	sorted := make([]time.Duration, len(e2eLatencies))
	copy(sorted, e2eLatencies)
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j] < sorted[i] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}
	p50 := sorted[len(sorted)*50/100]
	p95 := sorted[len(sorted)*95/100]
	p99 := sorted[len(sorted)*99/100]

	// Add estimated e2e latency (tx -> batch -> header -> certificate)
	// Batch timeout (100ms) + Header timeout (100ms) + vote collection (~50ms)
	estimatedE2E := avg + 250*time.Millisecond

	t.Logf(`
End-to-End Latency Results (TX submission to Batch)
====================================================
Samples:     %d
Pending TXs: %d (not yet batched)

TX-to-Batch Latency:
  Min:     %v
  Max:     %v
  Average: %v
  P50:     %v
  P95:     %v
  P99:     %v

Estimated Full E2E (TX -> Certificate):
  Average: ~%v (batch latency + batch_timeout + header_timeout + voting)

Facebook Reference:
  E2E Latency: ~557ms
`, len(e2eLatencies), len(pendingTxs), min, max, avg, p50, p95, p99, estimatedE2E)
}

// BenchmarkThroughput_Sustained runs a sustained throughput benchmark using testing.B.
// This allows using `go test -bench` for quick performance checks.
func BenchmarkThroughput_Sustained(b *testing.B) {
	numNodes := 4
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

	var certsFormed atomic.Int64
	hooks := &narwhal.Hooks[testutil.TestHash, *testutil.TestTransaction]{
		OnCertificateFormed: func(e narwhal.CertificateFormedEvent[testutil.TestHash]) {
			certsFormed.Add(1)
		},
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
			narwhal.WithMaxPendingTransactions[testutil.TestHash, *testutil.TestTransaction](50000),
			narwhal.WithDropOnFull[testutil.TestHash, *testutil.TestTransaction](true),
			narwhal.WithHooks[testutil.TestHash, *testutil.TestTransaction](hooks),
		)
		node, _ := narwhal.New(cfg, testutil.ComputeHash)
		nodes[i] = node
		_ = node.Start()
	}

	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
		time.Sleep(10 * time.Millisecond)
		for _, network := range networks {
			network.Close()
		}
	}()

	// Pre-create transactions
	txs := make([]*testutil.TestTransaction, b.N)
	for i := 0; i < b.N; i++ {
		txs[i] = testutil.NewTestTransaction([]byte(fmt.Sprintf("sustained-%d", i)))
	}

	b.ResetTimer()
	start := time.Now()

	for i := 0; i < b.N; i++ {
		nodes[i%numNodes].AddTransaction(txs[i])
	}

	b.StopTimer()
	elapsed := time.Since(start)

	// Wait for certificates to form
	time.Sleep(200 * time.Millisecond)

	// Report metrics
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "tx/s")
	b.ReportMetric(float64(certsFormed.Load()), "certs")
	b.ReportMetric(float64(certsFormed.Load())/elapsed.Seconds(), "certs/s")
}
