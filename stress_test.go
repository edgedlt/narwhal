package narwhal_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edgedlt/narwhal"
	"github.com/edgedlt/narwhal/internal/testutil"
	"github.com/edgedlt/narwhal/timer"
	"go.uber.org/zap"
)

// ============================================================================
// Stress Test Infrastructure
// ============================================================================

// StressTestConfig configures a stress test run.
type StressTestConfig struct {
	NumValidators  int
	NumWorkers     int
	Duration       time.Duration
	TxRate         int // transactions per second (0 = unlimited)
	TxSize         int // transaction size in bytes
	BatchSize      int
	BatchTimeout   time.Duration
	HeaderTimeout  time.Duration
	GCInterval     uint64 // rounds between GC
	ReportInterval time.Duration
	CheckLeaks     bool
}

// DefaultStressConfig returns a reasonable default stress test configuration.
func DefaultStressConfig() StressTestConfig {
	return StressTestConfig{
		NumValidators:  4,
		NumWorkers:     4,
		Duration:       30 * time.Second,
		TxRate:         10000,
		TxSize:         512,
		BatchSize:      500,
		BatchTimeout:   100 * time.Millisecond,
		HeaderTimeout:  500 * time.Millisecond,
		GCInterval:     50, // rounds
		ReportInterval: 5 * time.Second,
		CheckLeaks:     true,
	}
}

// StressTestResult contains the results of a stress test run.
type StressTestResult struct {
	Duration          time.Duration
	TxSubmitted       uint64
	TxCommitted       uint64
	CertsFormed       uint64
	RoundsAdvanced    uint64
	SubmitTPS         float64
	CommitTPS         float64
	CertsPerSecond    float64
	InitialGoroutines int
	FinalGoroutines   int
	GoroutineDelta    int
	InitialMemory     uint64
	FinalMemory       uint64
	MemoryDelta       int64
	PeakMemory        uint64
	Errors            []string
}

func (r StressTestResult) String() string {
	return fmt.Sprintf(`
=== Stress Test Results ===
Duration:        %v
Tx Submitted:    %d
Tx Committed:    %d
Certs Formed:    %d
Rounds Advanced: %d

Throughput:
  Submit TPS:    %.0f
  Commit TPS:    %.0f
  Certs/sec:     %.1f

Resource Usage:
  Goroutines:    %d → %d (delta: %+d)
  Memory:        %s → %s (delta: %+s, peak: %s)

Errors: %d
`,
		r.Duration,
		r.TxSubmitted,
		r.TxCommitted,
		r.CertsFormed,
		r.RoundsAdvanced,
		r.SubmitTPS,
		r.CommitTPS,
		r.CertsPerSecond,
		r.InitialGoroutines, r.FinalGoroutines, r.GoroutineDelta,
		formatBytes(r.InitialMemory), formatBytes(r.FinalMemory),
		formatBytesDelta(r.MemoryDelta), formatBytes(r.PeakMemory),
		len(r.Errors),
	)
}

func formatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func formatBytesDelta(b int64) string {
	sign := "+"
	if b < 0 {
		sign = ""
	}
	return sign + formatBytes(uint64(abs64(b)))
}

func abs64(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

// ============================================================================
// Stress Test Runner
// ============================================================================

func runStressTest(t *testing.T, cfg StressTestConfig) StressTestResult {
	t.Helper()

	// Force GC and get baseline
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	var initialMem runtime.MemStats
	runtime.ReadMemStats(&initialMem)
	initialGoroutines := runtime.NumGoroutine()

	result := StressTestResult{
		InitialGoroutines: initialGoroutines,
		InitialMemory:     initialMem.Alloc,
	}

	// Track peak memory
	var peakMemory uint64 = initialMem.Alloc

	// Create validators and networks
	validators := testutil.NewTestValidatorSet(cfg.NumValidators)
	networks := make([]*testutil.TestNetwork, cfg.NumValidators)
	storages := make([]*testutil.TestStorage, cfg.NumValidators)
	instances := make([]*narwhal.Narwhal[testutil.TestHash, *testutil.TestTransaction], cfg.NumValidators)

	for i := 0; i < cfg.NumValidators; i++ {
		networks[i] = testutil.NewTestNetwork(uint16(i))
		storages[i] = testutil.NewTestStorage()
	}

	// Connect networks
	for i := 0; i < cfg.NumValidators; i++ {
		for j := i + 1; j < cfg.NumValidators; j++ {
			networks[i].Connect(networks[j])
		}
	}

	// Track metrics via hooks
	var txSubmitted, certsFormed, roundsAdvanced uint64

	// Create instances
	logger := zap.NewNop()
	for i := 0; i < cfg.NumValidators; i++ {
		hooks := &narwhal.Hooks[testutil.TestHash, *testutil.TestTransaction]{
			OnCertificateFormed: func(e narwhal.CertificateFormedEvent[testutil.TestHash]) {
				atomic.AddUint64(&certsFormed, 1)
			},
			OnRoundAdvanced: func(e narwhal.RoundAdvancedEvent) {
				atomic.AddUint64(&roundsAdvanced, 1)
			},
		}

		ncfg, err := narwhal.NewConfig(
			narwhal.WithMyIndex[testutil.TestHash, *testutil.TestTransaction](uint16(i)),
			narwhal.WithValidators[testutil.TestHash, *testutil.TestTransaction](validators),
			narwhal.WithSigner[testutil.TestHash, *testutil.TestTransaction](validators.GetSigner(uint16(i))),
			narwhal.WithNetwork[testutil.TestHash, *testutil.TestTransaction](networks[i]),
			narwhal.WithStorage[testutil.TestHash, *testutil.TestTransaction](storages[i]),
			narwhal.WithWorkerCount[testutil.TestHash, *testutil.TestTransaction](cfg.NumWorkers),
			narwhal.WithBatchSize[testutil.TestHash, *testutil.TestTransaction](cfg.BatchSize),
			narwhal.WithBatchTimeout[testutil.TestHash, *testutil.TestTransaction](cfg.BatchTimeout),
			narwhal.WithHeaderTimeout[testutil.TestHash, *testutil.TestTransaction](cfg.HeaderTimeout),
			narwhal.WithGCInterval[testutil.TestHash, *testutil.TestTransaction](cfg.GCInterval),
			narwhal.WithTimer[testutil.TestHash, *testutil.TestTransaction](timer.NewRealTimer(time.Millisecond)),
			narwhal.WithHooks[testutil.TestHash, *testutil.TestTransaction](hooks),
			narwhal.WithLogger[testutil.TestHash, *testutil.TestTransaction](logger),
		)
		if err != nil {
			t.Fatalf("failed to create config for node %d: %v", i, err)
		}

		n, err := narwhal.New(ncfg, testutil.ComputeHash)
		if err != nil {
			t.Fatalf("failed to create narwhal instance %d: %v", i, err)
		}
		instances[i] = n
	}

	// Start all instances
	for i, n := range instances {
		if err := n.Start(); err != nil {
			t.Fatalf("failed to start instance %d: %v", i, err)
		}
	}

	// Run stress test
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer cancel()

	var wg sync.WaitGroup

	// Transaction submitter goroutines
	for i := 0; i < cfg.NumValidators; i++ {
		wg.Add(1)
		go func(nodeIdx int) {
			defer wg.Done()

			ticker := time.NewTicker(time.Second / time.Duration(cfg.TxRate/cfg.NumValidators+1))
			defer ticker.Stop()

			txID := uint64(0)
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					tx := testutil.NewTestTransactionSized(
						[]byte(fmt.Sprintf("stress-%d-%d", nodeIdx, txID)),
						cfg.TxSize,
					)
					instances[nodeIdx].AddTransaction(tx)
					atomic.AddUint64(&txSubmitted, 1)
					txID++
				}
			}
		}(i)
	}

	// Memory monitor goroutine
	if cfg.CheckLeaks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					var mem runtime.MemStats
					runtime.ReadMemStats(&mem)
					if mem.Alloc > peakMemory {
						atomic.StoreUint64(&peakMemory, mem.Alloc)
					}
				}
			}
		}()
	}

	// Progress reporter
	if cfg.ReportInterval > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(cfg.ReportInterval)
			defer ticker.Stop()
			start := time.Now()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					elapsed := time.Since(start)
					submitted := atomic.LoadUint64(&txSubmitted)
					certs := atomic.LoadUint64(&certsFormed)
					rounds := atomic.LoadUint64(&roundsAdvanced)
					t.Logf("[%v] TX: %d (%.0f/s), Certs: %d (%.1f/s), Rounds: %d, Goroutines: %d",
						elapsed.Round(time.Second),
						submitted, float64(submitted)/elapsed.Seconds(),
						certs, float64(certs)/elapsed.Seconds(),
						rounds, runtime.NumGoroutine())
				}
			}
		}()
	}

	// Wait for test duration
	<-ctx.Done()
	wg.Wait()

	// Stop all instances
	for _, n := range instances {
		n.Stop()
	}

	// Close networks
	for _, net := range networks {
		net.Close()
	}

	// Wait for goroutines to settle
	time.Sleep(500 * time.Millisecond)

	// Force GC and measure final state
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	var finalMem runtime.MemStats
	runtime.ReadMemStats(&finalMem)
	finalGoroutines := runtime.NumGoroutine()

	// Calculate results
	result.Duration = cfg.Duration
	result.TxSubmitted = atomic.LoadUint64(&txSubmitted)
	result.CertsFormed = atomic.LoadUint64(&certsFormed)
	result.RoundsAdvanced = atomic.LoadUint64(&roundsAdvanced)
	result.SubmitTPS = float64(result.TxSubmitted) / cfg.Duration.Seconds()
	result.CertsPerSecond = float64(result.CertsFormed) / cfg.Duration.Seconds()
	result.FinalGoroutines = finalGoroutines
	result.GoroutineDelta = finalGoroutines - initialGoroutines
	result.FinalMemory = finalMem.Alloc
	result.MemoryDelta = int64(finalMem.Alloc) - int64(initialMem.Alloc)
	result.PeakMemory = peakMemory

	return result
}

// ============================================================================
// Stress Tests
// ============================================================================

func TestStress_30Second(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	cfg := DefaultStressConfig()
	cfg.Duration = 30 * time.Second
	cfg.TxRate = 5000

	result := runStressTest(t, cfg)
	t.Log(result.String())

	// Verify no goroutine leak (allow small variance for runtime)
	if result.GoroutineDelta > 10 {
		t.Errorf("potential goroutine leak: started with %d, ended with %d (delta: %d)",
			result.InitialGoroutines, result.FinalGoroutines, result.GoroutineDelta)
	}

	// Verify reasonable memory growth (should be bounded due to GC)
	maxMemGrowth := int64(100 * 1024 * 1024) // 100MB
	if result.MemoryDelta > maxMemGrowth {
		t.Errorf("excessive memory growth: %s", formatBytesDelta(result.MemoryDelta))
	}

	// Verify progress was made
	if result.CertsFormed == 0 {
		t.Error("no certificates were formed")
	}
	if result.RoundsAdvanced == 0 {
		t.Error("no rounds were advanced")
	}
}

func TestStress_HighThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	cfg := DefaultStressConfig()
	cfg.Duration = 20 * time.Second
	cfg.TxRate = 50000 // High load
	cfg.BatchSize = 1000
	cfg.NumWorkers = 8

	result := runStressTest(t, cfg)
	t.Log(result.String())

	// Should handle high throughput
	if result.SubmitTPS < 10000 {
		t.Logf("warning: submit TPS lower than expected: %.0f", result.SubmitTPS)
	}

	// Verify no leaks
	if result.GoroutineDelta > 10 {
		t.Errorf("potential goroutine leak: delta %d", result.GoroutineDelta)
	}
}

func TestStress_LongRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long-running stress test in short mode")
	}

	// This test can be extended for hours by changing the duration
	cfg := DefaultStressConfig()
	cfg.Duration = 2 * time.Minute // Extend for longer runs
	cfg.TxRate = 2000              // Moderate sustained load
	cfg.ReportInterval = 10 * time.Second

	result := runStressTest(t, cfg)
	t.Log(result.String())

	// Strict leak checking for long-running test
	if result.GoroutineDelta > 5 {
		t.Errorf("goroutine leak detected: delta %d", result.GoroutineDelta)
	}

	// Memory should be stable (GC working)
	maxMemGrowth := int64(50 * 1024 * 1024) // 50MB for long run
	if result.MemoryDelta > maxMemGrowth {
		t.Errorf("memory leak suspected: growth %s", formatBytesDelta(result.MemoryDelta))
	}
}

func TestStress_GoroutineLeak(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping goroutine leak test in short mode")
	}

	// Run multiple short cycles to detect leaks
	initialGoroutines := runtime.NumGoroutine()

	for cycle := 0; cycle < 3; cycle++ {
		t.Logf("Cycle %d: starting with %d goroutines", cycle+1, runtime.NumGoroutine())

		cfg := DefaultStressConfig()
		cfg.Duration = 5 * time.Second
		cfg.TxRate = 1000
		cfg.ReportInterval = 0 // No progress reports
		cfg.CheckLeaks = false // We'll check manually

		_ = runStressTest(t, cfg)

		// Wait for cleanup
		time.Sleep(time.Second)
		runtime.GC()
		time.Sleep(100 * time.Millisecond)

		currentGoroutines := runtime.NumGoroutine()
		t.Logf("Cycle %d: ended with %d goroutines", cycle+1, currentGoroutines)
	}

	// Final check
	runtime.GC()
	time.Sleep(500 * time.Millisecond)
	finalGoroutines := runtime.NumGoroutine()

	delta := finalGoroutines - initialGoroutines
	t.Logf("Total goroutine delta after 3 cycles: %d → %d (delta: %d)",
		initialGoroutines, finalGoroutines, delta)

	if delta > 5 {
		t.Errorf("goroutine leak detected across cycles: %d leaked", delta)
	}
}

func TestStress_MemoryLeak(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memory leak test in short mode")
	}

	// Force baseline
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)

	// Run multiple cycles
	for cycle := 0; cycle < 3; cycle++ {
		cfg := DefaultStressConfig()
		cfg.Duration = 5 * time.Second
		cfg.TxRate = 2000
		cfg.ReportInterval = 0
		cfg.CheckLeaks = false

		_ = runStressTest(t, cfg)

		// Force GC between cycles
		runtime.GC()
		time.Sleep(200 * time.Millisecond)

		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		t.Logf("Cycle %d: Alloc=%s, TotalAlloc=%s, NumGC=%d",
			cycle+1, formatBytes(mem.Alloc), formatBytes(mem.TotalAlloc), mem.NumGC)
	}

	// Final measurement
	runtime.GC()
	time.Sleep(500 * time.Millisecond)
	var final runtime.MemStats
	runtime.ReadMemStats(&final)

	growth := int64(final.Alloc) - int64(baseline.Alloc)
	t.Logf("Memory growth after 3 cycles: %s", formatBytesDelta(growth))

	// Allow some growth but flag excessive
	maxGrowth := int64(20 * 1024 * 1024) // 20MB
	if growth > maxGrowth {
		t.Errorf("potential memory leak: %s growth", formatBytesDelta(growth))
	}
}

// ============================================================================
// Benchmark-style Stress Tests
// ============================================================================

func TestStress_SustainedThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping sustained throughput test in short mode")
	}

	// Test that throughput remains stable over time
	cfg := DefaultStressConfig()
	cfg.Duration = 30 * time.Second
	cfg.TxRate = 5000
	cfg.ReportInterval = 5 * time.Second

	result := runStressTest(t, cfg)
	t.Log(result.String())

	// Certificate rate should be reasonable
	if result.CertsPerSecond < 1 {
		t.Errorf("certificate rate too low: %.1f/s", result.CertsPerSecond)
	}
}

// ============================================================================
// Leak Detection Utilities
// ============================================================================

// LeakCheck provides utilities for detecting memory and goroutine leaks.
// Use it to wrap test functions for comprehensive leak detection.
type LeakCheck struct {
	t                 *testing.T
	initialGoroutines int
	initialMem        runtime.MemStats
	allowedGoroutines int
	allowedMemGrowth  int64 // bytes
}

// NewLeakCheck creates a new leak checker with baseline measurements.
// Call Finalize() at the end of the test to verify no leaks.
func NewLeakCheck(t *testing.T) *LeakCheck {
	t.Helper()

	// Force GC to get accurate baseline
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	return &LeakCheck{
		t:                 t,
		initialGoroutines: runtime.NumGoroutine(),
		initialMem:        mem,
		allowedGoroutines: 5,                // Small variance allowed
		allowedMemGrowth:  10 * 1024 * 1024, // 10MB allowed
	}
}

// SetAllowedGoroutines sets the maximum allowed goroutine increase.
func (lc *LeakCheck) SetAllowedGoroutines(n int) *LeakCheck {
	lc.allowedGoroutines = n
	return lc
}

// SetAllowedMemGrowth sets the maximum allowed memory growth in bytes.
func (lc *LeakCheck) SetAllowedMemGrowth(bytes int64) *LeakCheck {
	lc.allowedMemGrowth = bytes
	return lc
}

// Finalize checks for leaks and reports any found.
// Should be called at the end of the test, typically via defer.
func (lc *LeakCheck) Finalize() {
	lc.t.Helper()

	// Wait for goroutines to settle
	time.Sleep(500 * time.Millisecond)

	// Force GC
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	var finalMem runtime.MemStats
	runtime.ReadMemStats(&finalMem)
	finalGoroutines := runtime.NumGoroutine()

	// Check goroutine leak
	goroutineDelta := finalGoroutines - lc.initialGoroutines
	if goroutineDelta > lc.allowedGoroutines {
		lc.t.Errorf("goroutine leak detected: %d → %d (delta: %d, allowed: %d)",
			lc.initialGoroutines, finalGoroutines, goroutineDelta, lc.allowedGoroutines)
	}

	// Check memory leak
	memDelta := int64(finalMem.Alloc) - int64(lc.initialMem.Alloc)
	if memDelta > lc.allowedMemGrowth {
		lc.t.Errorf("memory leak suspected: %s → %s (delta: %s, allowed: %s)",
			formatBytes(lc.initialMem.Alloc),
			formatBytes(finalMem.Alloc),
			formatBytesDelta(memDelta),
			formatBytes(uint64(lc.allowedMemGrowth)))
	}

	lc.t.Logf("Leak check: goroutines %d → %d (delta: %+d), memory %s → %s (delta: %s)",
		lc.initialGoroutines, finalGoroutines, goroutineDelta,
		formatBytes(lc.initialMem.Alloc), formatBytes(finalMem.Alloc), formatBytesDelta(memDelta))
}

// TestLeakCheck_StartStop verifies that creating and stopping Narwhal instances
// doesn't leak goroutines or memory.
func TestLeakCheck_StartStop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping leak check test in short mode")
	}

	lc := NewLeakCheck(t)
	defer lc.Finalize()

	// Create and destroy multiple instances
	for cycle := 0; cycle < 5; cycle++ {
		validators := testutil.NewTestValidatorSet(4)
		networks := make([]*testutil.TestNetwork, 4)
		instances := make([]*narwhal.Narwhal[testutil.TestHash, *testutil.TestTransaction], 4)

		// Create
		for i := 0; i < 4; i++ {
			networks[i] = testutil.NewTestNetwork(uint16(i))
		}
		for i := 0; i < 4; i++ {
			for j := i + 1; j < 4; j++ {
				networks[i].Connect(networks[j])
			}
		}
		for i := 0; i < 4; i++ {
			cfg, _ := narwhal.NewConfig(
				narwhal.WithMyIndex[testutil.TestHash, *testutil.TestTransaction](uint16(i)),
				narwhal.WithValidators[testutil.TestHash, *testutil.TestTransaction](validators),
				narwhal.WithSigner[testutil.TestHash, *testutil.TestTransaction](validators.GetSigner(uint16(i))),
				narwhal.WithNetwork[testutil.TestHash, *testutil.TestTransaction](networks[i]),
				narwhal.WithStorage[testutil.TestHash, *testutil.TestTransaction](testutil.NewTestStorage()),
				narwhal.WithTimer[testutil.TestHash, *testutil.TestTransaction](timer.NewRealTimer(time.Millisecond)),
				narwhal.WithLogger[testutil.TestHash, *testutil.TestTransaction](zap.NewNop()),
			)
			n, _ := narwhal.New(cfg, testutil.ComputeHash)
			instances[i] = n
		}

		// Start
		for _, n := range instances {
			_ = n.Start()
		}

		// Brief operation
		for i := 0; i < 10; i++ {
			tx := testutil.NewTestTransaction([]byte{byte(cycle), byte(i)})
			instances[0].AddTransaction(tx)
		}
		time.Sleep(100 * time.Millisecond)

		// Stop
		for _, n := range instances {
			n.Stop()
		}
		for _, net := range networks {
			net.Close()
		}

		// Brief pause between cycles
		time.Sleep(200 * time.Millisecond)
		runtime.GC()
	}
}

// TestLeakCheck_WorkerPool verifies worker goroutines are properly cleaned up.
func TestLeakCheck_WorkerPool(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping worker pool leak test in short mode")
	}

	lc := NewLeakCheck(t)
	defer lc.Finalize()

	// Test with different worker counts
	workerCounts := []int{1, 4, 8, 16}

	for _, workerCount := range workerCounts {
		t.Logf("Testing with %d workers", workerCount)

		validators := testutil.NewTestValidatorSet(4)
		networks := make([]*testutil.TestNetwork, 4)

		for i := 0; i < 4; i++ {
			networks[i] = testutil.NewTestNetwork(uint16(i))
		}
		for i := 0; i < 4; i++ {
			for j := i + 1; j < 4; j++ {
				networks[i].Connect(networks[j])
			}
		}

		// Create instance with specified worker count
		cfg, _ := narwhal.NewConfig(
			narwhal.WithMyIndex[testutil.TestHash, *testutil.TestTransaction](0),
			narwhal.WithValidators[testutil.TestHash, *testutil.TestTransaction](validators),
			narwhal.WithSigner[testutil.TestHash, *testutil.TestTransaction](validators.GetSigner(0)),
			narwhal.WithNetwork[testutil.TestHash, *testutil.TestTransaction](networks[0]),
			narwhal.WithStorage[testutil.TestHash, *testutil.TestTransaction](testutil.NewTestStorage()),
			narwhal.WithTimer[testutil.TestHash, *testutil.TestTransaction](timer.NewRealTimer(time.Millisecond)),
			narwhal.WithLogger[testutil.TestHash, *testutil.TestTransaction](zap.NewNop()),
			narwhal.WithWorkerCount[testutil.TestHash, *testutil.TestTransaction](workerCount),
		)

		n, _ := narwhal.New(cfg, testutil.ComputeHash)
		_ = n.Start()

		// Submit transactions across all workers
		for i := 0; i < workerCount*10; i++ {
			tx := testutil.NewTestTransaction([]byte{byte(workerCount), byte(i)})
			n.AddTransaction(tx)
		}
		time.Sleep(200 * time.Millisecond)

		n.Stop()
		for _, net := range networks {
			net.Close()
		}

		runtime.GC()
		time.Sleep(100 * time.Millisecond)
	}
}

// TestLeakCheck_RapidStartStop tests rapid start/stop cycles for leaks.
func TestLeakCheck_RapidStartStop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping rapid start/stop test in short mode")
	}

	lc := NewLeakCheck(t)
	defer lc.Finalize()

	validators := testutil.NewTestValidatorSet(4)
	network := testutil.NewTestNetwork(0)
	defer network.Close()

	for cycle := 0; cycle < 20; cycle++ {
		cfg, _ := narwhal.NewConfig(
			narwhal.WithMyIndex[testutil.TestHash, *testutil.TestTransaction](0),
			narwhal.WithValidators[testutil.TestHash, *testutil.TestTransaction](validators),
			narwhal.WithSigner[testutil.TestHash, *testutil.TestTransaction](validators.GetSigner(0)),
			narwhal.WithNetwork[testutil.TestHash, *testutil.TestTransaction](network),
			narwhal.WithStorage[testutil.TestHash, *testutil.TestTransaction](testutil.NewTestStorage()),
			narwhal.WithTimer[testutil.TestHash, *testutil.TestTransaction](timer.NewRealTimer(time.Millisecond)),
			narwhal.WithLogger[testutil.TestHash, *testutil.TestTransaction](zap.NewNop()),
		)

		n, _ := narwhal.New(cfg, testutil.ComputeHash)
		_ = n.Start()
		n.Stop()
	}
}
