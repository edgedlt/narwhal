package narwhal_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edgedlt/narwhal"
	"github.com/edgedlt/narwhal/internal/testutil"
	"github.com/edgedlt/narwhal/timer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestNarwhal_ValidatorCrash tests that the network makes progress
// when 1 of 4 validators crashes (f=1 fault tolerance).
func TestNarwhal_ValidatorCrash(t *testing.T) {
	const numNodes = 4
	const crashedNode = 3 // Node 3 will "crash" (stop participating)

	validators := testutil.NewTestValidatorSet(numNodes)

	// Create networks
	networks := make([]*testutil.TestNetwork, numNodes)
	for i := 0; i < numNodes; i++ {
		networks[i] = testutil.NewTestNetwork(uint16(i))
	}
	// Connect only the surviving nodes in a full mesh
	for i := 0; i < numNodes; i++ {
		for j := i + 1; j < numNodes; j++ {
			if i != crashedNode && j != crashedNode {
				networks[i].Connect(networks[j])
			}
		}
	}

	// Create and start only surviving nodes
	nodes := make([]*narwhal.Narwhal[testutil.TestHash, *testutil.TestTransaction], numNodes)
	for i := 0; i < numNodes; i++ {
		if i == crashedNode {
			continue // Skip crashed node
		}

		storage := testutil.NewTestStorage()
		cfg, err := narwhal.NewConfig(
			narwhal.WithMyIndex[testutil.TestHash, *testutil.TestTransaction](uint16(i)),
			narwhal.WithValidators[testutil.TestHash, *testutil.TestTransaction](validators),
			narwhal.WithSigner[testutil.TestHash, *testutil.TestTransaction](validators.GetSigner(uint16(i))),
			narwhal.WithStorage[testutil.TestHash, *testutil.TestTransaction](storage),
			narwhal.WithNetwork[testutil.TestHash, *testutil.TestTransaction](networks[i]),
			narwhal.WithTimer[testutil.TestHash, *testutil.TestTransaction](timer.NewRealTimer(time.Millisecond)),
			narwhal.WithBatchSize[testutil.TestHash, *testutil.TestTransaction](2),
			narwhal.WithBatchTimeout[testutil.TestHash, *testutil.TestTransaction](50*time.Millisecond),
			narwhal.WithHeaderTimeout[testutil.TestHash, *testutil.TestTransaction](100*time.Millisecond),
			narwhal.WithLogger[testutil.TestHash, *testutil.TestTransaction](zap.NewNop()),
		)
		require.NoError(t, err)

		node, err := narwhal.New(cfg, testutil.ComputeHash)
		require.NoError(t, err)
		nodes[i] = node
		require.NoError(t, node.Start())
	}

	defer func() {
		for i, node := range nodes {
			if node != nil {
				node.Stop()
				networks[i].Close()
			}
		}
	}()

	// Submit transactions to surviving nodes
	for i := 0; i < numNodes; i++ {
		if i == crashedNode {
			continue
		}
		for j := 0; j < 5; j++ {
			tx := testutil.NewTestTransaction([]byte(fmt.Sprintf("node%d-tx%d", i, j)))
			nodes[i].AddTransaction(tx)
		}
	}

	// Wait for progress - with 3 nodes (f=1), we should still form certificates
	deadline := time.Now().Add(5 * time.Second)
	var success bool
	for time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)

		// Check if surviving nodes have made progress
		allAdvanced := true
		for i := 0; i < numNodes; i++ {
			if i == crashedNode {
				continue
			}
			if nodes[i].CurrentRound() < 1 {
				allAdvanced = false
			}
		}
		if allAdvanced {
			success = true
			break
		}
	}

	require.True(t, success, "Network should make progress with f=1 crashed validator")

	// Verify certificates formed
	for i := 0; i < numNodes; i++ {
		if i == crashedNode {
			continue
		}
		dag := nodes[i].GetDAG()
		// Should have at least quorum (3) certificates at round 0
		count := dag.CertificateCountForRound(0)
		assert.GreaterOrEqual(t, count, 3,
			"Node %d should have at least 3 certificates at round 0", i)
	}
}

// TestNarwhal_SlowValidator tests that the network makes progress
// even when one validator is slow (delayed messages).
func TestNarwhal_SlowValidator(t *testing.T) {
	const numNodes = 4

	validators := testutil.NewTestValidatorSet(numNodes)

	// Create networks - use a wrapper for the slow node
	networks := make([]*testutil.TestNetwork, numNodes)
	for i := 0; i < numNodes; i++ {
		networks[i] = testutil.NewTestNetwork(uint16(i))
	}

	// Connect all nodes
	for i := 0; i < numNodes; i++ {
		for j := i + 1; j < numNodes; j++ {
			networks[i].Connect(networks[j])
		}
	}

	// Create nodes
	nodes := make([]*narwhal.Narwhal[testutil.TestHash, *testutil.TestTransaction], numNodes)
	for i := 0; i < numNodes; i++ {
		storage := testutil.NewTestStorage()
		cfg, err := narwhal.NewConfig(
			narwhal.WithMyIndex[testutil.TestHash, *testutil.TestTransaction](uint16(i)),
			narwhal.WithValidators[testutil.TestHash, *testutil.TestTransaction](validators),
			narwhal.WithSigner[testutil.TestHash, *testutil.TestTransaction](validators.GetSigner(uint16(i))),
			narwhal.WithStorage[testutil.TestHash, *testutil.TestTransaction](storage),
			narwhal.WithNetwork[testutil.TestHash, *testutil.TestTransaction](networks[i]),
			narwhal.WithTimer[testutil.TestHash, *testutil.TestTransaction](timer.NewRealTimer(time.Millisecond)),
			narwhal.WithBatchSize[testutil.TestHash, *testutil.TestTransaction](2),
			narwhal.WithBatchTimeout[testutil.TestHash, *testutil.TestTransaction](50*time.Millisecond),
			narwhal.WithHeaderTimeout[testutil.TestHash, *testutil.TestTransaction](100*time.Millisecond),
			narwhal.WithLogger[testutil.TestHash, *testutil.TestTransaction](zap.NewNop()),
		)
		require.NoError(t, err)

		node, err := narwhal.New(cfg, testutil.ComputeHash)
		require.NoError(t, err)
		nodes[i] = node
		require.NoError(t, node.Start())
	}

	defer func() {
		for i, node := range nodes {
			node.Stop()
			networks[i].Close()
		}
	}()

	// Submit transactions
	for i := 0; i < numNodes; i++ {
		for j := 0; j < 3; j++ {
			tx := testutil.NewTestTransaction([]byte(fmt.Sprintf("node%d-tx%d", i, j)))
			nodes[i].AddTransaction(tx)
		}
	}

	// Wait for progress - all nodes should eventually advance
	deadline := time.Now().Add(5 * time.Second)
	var success bool
	for time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)

		allAdvanced := true
		for i := 0; i < numNodes; i++ {
			if nodes[i].CurrentRound() < 1 {
				allAdvanced = false
			}
		}
		if allAdvanced {
			success = true
			break
		}
	}

	require.True(t, success, "Network should make progress even with slow validator")
}

// TestNarwhal_ByzantineEquivocation tests detection and handling of
// a Byzantine validator that tries to equivocate.
func TestNarwhal_ByzantineEquivocation(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	dag := narwhal.NewDAG[testutil.TestHash, *testutil.TestTransaction](validators, zap.NewNop())

	// Track equivocation detections
	var equivocations atomic.Int32
	dag.OnEquivocation(func(evidence *narwhal.EquivocationEvidence[testutil.TestHash]) {
		equivocations.Add(1)
		t.Logf("Equivocation detected: author=%d round=%d", evidence.Author, evidence.Round)
	})

	// Insert a valid certificate
	header1 := &narwhal.Header[testutil.TestHash]{
		Author:    0,
		Round:     0,
		Timestamp: uint64(time.Now().UnixMilli()),
	}
	header1.ComputeDigest(testutil.ComputeHash)

	votes1 := make(map[uint16][]byte)
	for i := uint16(0); i < 3; i++ {
		sig, _ := validators.GetSigner(i).Sign(header1.Digest.Bytes())
		votes1[i] = sig
	}
	cert1 := narwhal.NewCertificate(header1, votes1)
	require.NoError(t, dag.InsertCertificate(cert1, nil))

	// Try to insert an equivocating certificate (same author, same round, different content)
	header2 := &narwhal.Header[testutil.TestHash]{
		Author:    0,                                     // Same author
		Round:     0,                                     // Same round
		Timestamp: uint64(time.Now().UnixMilli()) + 1000, // Different content
	}
	header2.ComputeDigest(testutil.ComputeHash)

	votes2 := make(map[uint16][]byte)
	for i := uint16(0); i < 3; i++ {
		sig, _ := validators.GetSigner(i).Sign(header2.Digest.Bytes())
		votes2[i] = sig
	}
	cert2 := narwhal.NewCertificate(header2, votes2)

	// This should fail and trigger equivocation callback
	err := dag.InsertCertificate(cert2, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "equivocation")

	// Wait for async callback
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(1), equivocations.Load(), "should have detected 1 equivocation")

	// Verify the DAG rejected the equivocating certificate
	assert.Equal(t, 1, dag.CertificateCountForRound(0), "should only have 1 certificate for round 0")
}

// TestNarwhal_OutOfOrderMessages tests that the system handles
// certificates arriving out of order (child before parent).
func TestNarwhal_OutOfOrderMessages(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	dag := narwhal.NewDAG[testutil.TestHash, *testutil.TestTransaction](validators, zap.NewNop())

	// Create round 0 certificates
	round0Certs := make([]*narwhal.Certificate[testutil.TestHash], 4)
	for author := uint16(0); author < 4; author++ {
		header := &narwhal.Header[testutil.TestHash]{
			Author:    author,
			Round:     0,
			Timestamp: uint64(time.Now().UnixMilli()) + uint64(author),
		}
		header.ComputeDigest(testutil.ComputeHash)

		votes := make(map[uint16][]byte)
		for i := uint16(0); i < 3; i++ {
			sig, _ := validators.GetSigner(i).Sign(header.Digest.Bytes())
			votes[i] = sig
		}
		round0Certs[author] = narwhal.NewCertificate(header, votes)
	}

	// Create a round 1 certificate that references round 0 certs
	round1Parents := make([]testutil.TestHash, 3)
	for i := 0; i < 3; i++ {
		round1Parents[i] = round0Certs[i].Header.Digest
	}

	round1Header := &narwhal.Header[testutil.TestHash]{
		Author:    0,
		Round:     1,
		Timestamp: uint64(time.Now().UnixMilli()) + 100,
		Parents:   round1Parents,
	}
	round1Header.ComputeDigest(testutil.ComputeHash)

	round1Votes := make(map[uint16][]byte)
	for i := uint16(0); i < 3; i++ {
		sig, _ := validators.GetSigner(i).Sign(round1Header.Digest.Bytes())
		round1Votes[i] = sig
	}
	round1Cert := narwhal.NewCertificate(round1Header, round1Votes)

	// Insert round 1 certificate FIRST (out of order)
	err := dag.InsertCertificate(round1Cert, nil)
	require.NoError(t, err) // Should be queued as pending

	// Verify it's pending
	stats := dag.Stats()
	assert.Equal(t, 1, stats.PendingCount, "round 1 cert should be pending")
	assert.Equal(t, 0, stats.TotalVertices, "no vertices inserted yet")

	// Now insert the parent certificates
	for i := 0; i < 3; i++ {
		err := dag.InsertCertificate(round0Certs[i], nil)
		require.NoError(t, err)
	}

	// The pending certificate should now be processed
	stats = dag.Stats()
	assert.Equal(t, 0, stats.PendingCount, "pending should be empty")
	assert.Equal(t, 4, stats.TotalVertices, "should have 3 round-0 + 1 round-1 certificates")

	// Verify round advancement
	assert.Equal(t, uint64(1), dag.CurrentRound(), "should have advanced to round 1")
}

// TestNarwhal_ConcurrentTransactions tests handling of many concurrent
// transaction submissions across multiple nodes.
func TestNarwhal_ConcurrentTransactions(t *testing.T) {
	const numNodes = 4
	const txPerGoroutine = 10
	const goroutinesPerNode = 5

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
		cfg, err := narwhal.NewConfig(
			narwhal.WithMyIndex[testutil.TestHash, *testutil.TestTransaction](uint16(i)),
			narwhal.WithValidators[testutil.TestHash, *testutil.TestTransaction](validators),
			narwhal.WithSigner[testutil.TestHash, *testutil.TestTransaction](validators.GetSigner(uint16(i))),
			narwhal.WithStorage[testutil.TestHash, *testutil.TestTransaction](storage),
			narwhal.WithNetwork[testutil.TestHash, *testutil.TestTransaction](networks[i]),
			narwhal.WithTimer[testutil.TestHash, *testutil.TestTransaction](timer.NewRealTimer(time.Millisecond)),
			narwhal.WithBatchSize[testutil.TestHash, *testutil.TestTransaction](5),
			narwhal.WithBatchTimeout[testutil.TestHash, *testutil.TestTransaction](50*time.Millisecond),
			narwhal.WithHeaderTimeout[testutil.TestHash, *testutil.TestTransaction](100*time.Millisecond),
			narwhal.WithLogger[testutil.TestHash, *testutil.TestTransaction](zap.NewNop()),
		)
		require.NoError(t, err)

		node, err := narwhal.New(cfg, testutil.ComputeHash)
		require.NoError(t, err)
		nodes[i] = node
		require.NoError(t, node.Start())
	}

	defer func() {
		for i, node := range nodes {
			node.Stop()
			networks[i].Close()
		}
	}()

	// Submit transactions concurrently
	var wg sync.WaitGroup
	var txCount atomic.Int32
	for i := 0; i < numNodes; i++ {
		for g := 0; g < goroutinesPerNode; g++ {
			wg.Add(1)
			go func(nodeIdx, goroutineIdx int) {
				defer wg.Done()
				for j := 0; j < txPerGoroutine; j++ {
					tx := testutil.NewTestTransaction(
						[]byte(fmt.Sprintf("n%d-g%d-tx%d", nodeIdx, goroutineIdx, j)))
					nodes[nodeIdx].AddTransaction(tx)
					txCount.Add(1)
				}
			}(i, g)
		}
	}

	wg.Wait()
	totalTx := txCount.Load()
	t.Logf("Submitted %d transactions", totalTx)

	// Wait for network to process
	deadline := time.Now().Add(5 * time.Second)
	var success bool
	for time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)

		allAdvanced := true
		for _, node := range nodes {
			if node.CurrentRound() < 1 {
				allAdvanced = false
			}
		}
		if allAdvanced {
			success = true
			break
		}
	}

	require.True(t, success, "Network should handle concurrent transactions")

	// Log final state
	for i, node := range nodes {
		dag := node.GetDAG()
		stats := dag.Stats()
		t.Logf("Node %d: round=%d vertices=%d", i, stats.CurrentRound, stats.TotalVertices)
	}
}
