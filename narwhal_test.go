package narwhal_test

import (
	"fmt"
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

func TestDAG_InsertCertificate(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	dag := narwhal.NewDAG[testutil.TestHash, *testutil.TestTransaction](validators, zap.NewNop())

	// Create a certificate for round 0
	header := &narwhal.Header[testutil.TestHash]{
		Author:    0,
		Round:     0,
		BatchRefs: nil,
		Parents:   nil,
		Timestamp: uint64(time.Now().UnixMilli()),
	}
	header.ComputeDigest(testutil.ComputeHash)

	// Collect signatures
	votes := make(map[uint16][]byte)
	for i := uint16(0); i < 3; i++ { // 2f+1 = 3 for n=4
		signer := validators.GetSigner(i)
		sig, err := signer.Sign(header.Digest.Bytes())
		require.NoError(t, err)
		votes[i] = sig
	}

	cert := narwhal.NewCertificate(header, votes)

	// Insert into DAG
	err := dag.InsertCertificate(cert, nil)
	require.NoError(t, err)

	// Verify it was inserted
	assert.True(t, dag.IsCertified(header.Digest))
	assert.Equal(t, uint64(0), dag.CurrentRound())
}

func TestDAG_RoundAdvancement(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	dag := narwhal.NewDAG[testutil.TestHash, *testutil.TestTransaction](validators, zap.NewNop())

	// Insert 3 certificates for round 0 (quorum = 2f+1 = 3)
	for author := uint16(0); author < 3; author++ {
		header := &narwhal.Header[testutil.TestHash]{
			Author:    author,
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
		err := dag.InsertCertificate(cert, nil)
		require.NoError(t, err)
	}

	// Round should advance to 1 after quorum reached
	assert.Equal(t, uint64(1), dag.CurrentRound())
}

func TestDAG_EquivocationDetection(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	dag := narwhal.NewDAG[testutil.TestHash, *testutil.TestTransaction](validators, zap.NewNop())

	// Track equivocation callback (use atomic pointer for thread safety)
	var equivocationEvidence atomic.Pointer[narwhal.EquivocationEvidence[testutil.TestHash]]
	dag.OnEquivocation(func(evidence *narwhal.EquivocationEvidence[testutil.TestHash]) {
		equivocationEvidence.Store(evidence)
	})

	// Create first certificate for author 0, round 0
	header1 := &narwhal.Header[testutil.TestHash]{
		Author:    0,
		Round:     0,
		Timestamp: uint64(time.Now().UnixMilli()),
	}
	header1.ComputeDigest(testutil.ComputeHash)

	votes1 := make(map[uint16][]byte)
	for i := uint16(0); i < 3; i++ {
		signer := validators.GetSigner(i)
		sig, _ := signer.Sign(header1.Digest.Bytes())
		votes1[i] = sig
	}
	cert1 := narwhal.NewCertificate(header1, votes1)

	// Insert first certificate
	err := dag.InsertCertificate(cert1, nil)
	require.NoError(t, err)

	// Create second (conflicting) certificate for same author and round
	header2 := &narwhal.Header[testutil.TestHash]{
		Author:    0,
		Round:     0,
		Timestamp: uint64(time.Now().UnixMilli()) + 1, // Different timestamp = different hash
	}
	header2.ComputeDigest(testutil.ComputeHash)

	votes2 := make(map[uint16][]byte)
	for i := uint16(0); i < 3; i++ {
		signer := validators.GetSigner(i)
		sig, _ := signer.Sign(header2.Digest.Bytes())
		votes2[i] = sig
	}
	cert2 := narwhal.NewCertificate(header2, votes2)

	// Should fail with equivocation error
	err = dag.InsertCertificate(cert2, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "equivocation")

	// Wait for callback (it's async)
	time.Sleep(50 * time.Millisecond)
	evidence := equivocationEvidence.Load()
	require.NotNil(t, evidence, "equivocation callback should have been called")
	assert.Equal(t, uint16(0), evidence.Author)
	assert.Equal(t, uint64(0), evidence.Round)
}

func TestDAG_PendingCertificates(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	dag := narwhal.NewDAG[testutil.TestHash, *testutil.TestTransaction](validators, zap.NewNop())

	// Create round 0 certificates (only 2, not quorum yet)
	round0Certs := make([]*narwhal.Certificate[testutil.TestHash], 2)
	for author := uint16(0); author < 2; author++ {
		header := &narwhal.Header[testutil.TestHash]{
			Author:    author,
			Round:     0,
			Timestamp: uint64(time.Now().UnixMilli()) + uint64(author),
		}
		header.ComputeDigest(testutil.ComputeHash)

		votes := make(map[uint16][]byte)
		for i := uint16(0); i < 3; i++ {
			signer := validators.GetSigner(i)
			sig, _ := signer.Sign(header.Digest.Bytes())
			votes[i] = sig
		}
		round0Certs[author] = narwhal.NewCertificate(header, votes)
		err := dag.InsertCertificate(round0Certs[author], nil)
		require.NoError(t, err)
	}

	// Create a round 1 certificate that references round 0 certs (including a missing one)
	round1Header := &narwhal.Header[testutil.TestHash]{
		Author:    0,
		Round:     1,
		Timestamp: uint64(time.Now().UnixMilli()) + 100,
		Parents: []testutil.TestHash{
			round0Certs[0].Header.Digest,
			round0Certs[1].Header.Digest,
			testutil.ComputeHash([]byte("missing_parent")), // This parent doesn't exist yet
		},
	}
	round1Header.ComputeDigest(testutil.ComputeHash)

	votes := make(map[uint16][]byte)
	for i := uint16(0); i < 3; i++ {
		signer := validators.GetSigner(i)
		sig, _ := signer.Sign(round1Header.Digest.Bytes())
		votes[i] = sig
	}
	round1Cert := narwhal.NewCertificate(round1Header, votes)

	// Insert round 1 cert - it should be queued as pending
	err := dag.InsertCertificate(round1Cert, nil)
	require.NoError(t, err) // No error, just pending

	// Verify it's pending
	stats := dag.Stats()
	assert.Equal(t, 1, stats.PendingCount, "should have 1 pending certificate")
	assert.Equal(t, 2, stats.TotalVertices, "should only have 2 inserted vertices")

	// Verify we can get missing parents
	missing := dag.GetMissingParents()
	assert.Len(t, missing, 1, "should have 1 missing parent")
}

func TestCertificate_Validate(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)

	header := &narwhal.Header[testutil.TestHash]{
		Author:    0,
		Round:     0,
		Timestamp: uint64(time.Now().UnixMilli()),
	}
	header.ComputeDigest(testutil.ComputeHash)

	// Create certificate with quorum signatures
	votes := make(map[uint16][]byte)
	for i := uint16(0); i < 3; i++ {
		signer := validators.GetSigner(i)
		sig, _ := signer.Sign(header.Digest.Bytes())
		votes[i] = sig
	}
	cert := narwhal.NewCertificate(header, votes)

	// Should validate successfully
	err := cert.Validate(validators)
	require.NoError(t, err)

	// Certificate with insufficient signatures should fail
	insufficientVotes := make(map[uint16][]byte)
	for i := uint16(0); i < 2; i++ { // Only 2 signatures, need 3
		signer := validators.GetSigner(i)
		sig, _ := signer.Sign(header.Digest.Bytes())
		insufficientVotes[i] = sig
	}
	insufficientCert := narwhal.NewCertificate(header, insufficientVotes)

	err = insufficientCert.Validate(validators)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient signatures")
}

func TestWorker_BatchCreation(t *testing.T) {
	batches := make([]*narwhal.Batch[testutil.TestHash, *testutil.TestTransaction], 0)
	onBatch := func(b *narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]) {
		batches = append(batches, b)
	}

	worker := narwhal.NewWorker(narwhal.WorkerConfig[testutil.TestHash, *testutil.TestTransaction]{
		ID:           0,
		ValidatorID:  0,
		BatchSize:    3,         // Small batch size for testing
		BatchTimeout: time.Hour, // Long timeout so we only trigger on size
		HashFunc:     testutil.ComputeHash,
		OnBatch:      onBatch,
		Logger:       zap.NewNop(),
	})

	// Add 3 transactions (should trigger batch creation)
	for i := 0; i < 3; i++ {
		tx := testutil.NewTestTransaction([]byte{byte(i)})
		worker.AddTransaction(tx)
	}

	require.Len(t, batches, 1)
	assert.Equal(t, 3, batches[0].TransactionCount())
}

func TestBatch_Serialization(t *testing.T) {
	tx1 := testutil.NewTestTransaction([]byte("tx1"))
	tx2 := testutil.NewTestTransaction([]byte("tx2"))

	batch := &narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]{
		WorkerID:     1,
		ValidatorID:  2,
		Round:        5,
		Transactions: []*testutil.TestTransaction{tx1, tx2},
	}
	batch.ComputeDigest(testutil.ComputeHash)

	// Serialize
	data := batch.Bytes()
	require.NotEmpty(t, data)

	// Deserialize
	restored, err := narwhal.BatchFromBytes(data, testutil.HashFromBytes, testutil.TransactionFromBytes)
	require.NoError(t, err)

	assert.Equal(t, batch.WorkerID, restored.WorkerID)
	assert.Equal(t, batch.ValidatorID, restored.ValidatorID)
	assert.Equal(t, batch.Round, restored.Round)
	assert.True(t, batch.Digest.Equals(restored.Digest))
	assert.Len(t, restored.Transactions, 2)
}

func TestHeader_Serialization(t *testing.T) {
	batchRef1 := testutil.ComputeHash([]byte("batch1"))
	batchRef2 := testutil.ComputeHash([]byte("batch2"))
	parent1 := testutil.ComputeHash([]byte("parent1"))

	header := &narwhal.Header[testutil.TestHash]{
		Author:    3,
		Round:     10,
		Epoch:     1,
		Timestamp: 123456789,
		BatchRefs: []testutil.TestHash{batchRef1, batchRef2},
		Parents:   []testutil.TestHash{parent1},
	}
	header.ComputeDigest(testutil.ComputeHash)

	// Serialize
	data := header.Bytes()
	require.NotEmpty(t, data)

	// Deserialize
	restored, err := narwhal.HeaderFromBytes(data, testutil.HashFromBytes)
	require.NoError(t, err)

	assert.Equal(t, header.Author, restored.Author)
	assert.Equal(t, header.Round, restored.Round)
	assert.Equal(t, header.Epoch, restored.Epoch)
	assert.Equal(t, header.Timestamp, restored.Timestamp)
	assert.True(t, header.Digest.Equals(restored.Digest))
	assert.Len(t, restored.BatchRefs, 2)
	assert.Len(t, restored.Parents, 1)
}

func TestVote_Serialization(t *testing.T) {
	headerDigest := testutil.ComputeHash([]byte("header"))
	signer := testutil.NewTestSigner()

	vote, err := narwhal.NewVote(headerDigest, 5, signer)
	require.NoError(t, err)

	// Serialize
	data := vote.Bytes()
	require.NotEmpty(t, data)

	// Deserialize
	restored, err := narwhal.VoteFromBytes(data, testutil.HashFromBytes)
	require.NoError(t, err)

	assert.True(t, vote.HeaderDigest.Equals(restored.HeaderDigest))
	assert.Equal(t, vote.ValidatorIndex, restored.ValidatorIndex)
	assert.Equal(t, vote.Signature, restored.Signature)
}

func TestCertificate_Serialization(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)

	header := &narwhal.Header[testutil.TestHash]{
		Author:    1,
		Round:     5,
		Epoch:     0,
		Timestamp: 123456789,
		BatchRefs: []testutil.TestHash{testutil.ComputeHash([]byte("batch1"))},
		Parents:   []testutil.TestHash{testutil.ComputeHash([]byte("parent1"))},
	}
	header.ComputeDigest(testutil.ComputeHash)

	// Create certificate with 3 signatures (2f+1 for f=1)
	votes := make(map[uint16][]byte)
	for i := uint16(0); i < 3; i++ {
		signer := validators.GetSigner(i)
		sig, err := signer.Sign(header.Digest.Bytes())
		require.NoError(t, err)
		votes[i] = sig
	}
	cert := narwhal.NewCertificate(header, votes)

	// Serialize
	data := cert.Bytes()
	require.NotEmpty(t, data)

	// Deserialize
	restored, err := narwhal.CertificateFromBytes(data, testutil.HashFromBytes)
	require.NoError(t, err)

	assert.True(t, cert.Header.Digest.Equals(restored.Header.Digest))
	assert.Equal(t, cert.Header.Author, restored.Header.Author)
	assert.Equal(t, cert.Header.Round, restored.Header.Round)
	assert.Equal(t, cert.SignerBitmap, restored.SignerBitmap)
	assert.Equal(t, len(cert.Signatures), len(restored.Signatures))
}

func TestNarwhal_New(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	signer := validators.GetSigner(0)
	storage := testutil.NewTestStorage()
	network := testutil.NewTestNetwork(0)

	cfg, err := narwhal.NewConfig(
		narwhal.WithMyIndex[testutil.TestHash, *testutil.TestTransaction](0),
		narwhal.WithValidators[testutil.TestHash, *testutil.TestTransaction](validators),
		narwhal.WithSigner[testutil.TestHash, *testutil.TestTransaction](signer),
		narwhal.WithStorage[testutil.TestHash, *testutil.TestTransaction](storage),
		narwhal.WithNetwork[testutil.TestHash, *testutil.TestTransaction](network),
		narwhal.WithTimer[testutil.TestHash, *testutil.TestTransaction](timer.NewMockTimer()),
		narwhal.WithLogger[testutil.TestHash, *testutil.TestTransaction](zap.NewNop()),
	)
	require.NoError(t, err)

	nw, err := narwhal.New(cfg, testutil.ComputeHash)
	require.NoError(t, err)
	require.NotNil(t, nw)

	// Start and stop should work
	err = nw.Start()
	require.NoError(t, err)

	// Give it a moment to start
	time.Sleep(10 * time.Millisecond)

	nw.Stop()
}

// TestNarwhal_FourNodeIntegration tests a 4-node Narwhal network:
// - Spins up 4 nodes with connected test networks
// - Submits transactions to each node
// - Verifies certificates form across all nodes
// - Confirms DAG round advancement
// - Extracts transactions from certificates
func TestNarwhal_FourNodeIntegration(t *testing.T) {
	const numNodes = 4
	const txPerNode = 5

	// Create shared validator set
	validators := testutil.NewTestValidatorSet(numNodes)

	// Create networks and connect them in a full mesh
	networks := make([]*testutil.TestNetwork, numNodes)
	for i := 0; i < numNodes; i++ {
		networks[i] = testutil.NewTestNetwork(uint16(i))
	}
	for i := 0; i < numNodes; i++ {
		for j := i + 1; j < numNodes; j++ {
			networks[i].Connect(networks[j])
		}
	}

	// Create storage for each node
	storages := make([]*testutil.TestStorage, numNodes)
	for i := 0; i < numNodes; i++ {
		storages[i] = testutil.NewTestStorage()
	}

	// Create and start all nodes
	nodes := make([]*narwhal.Narwhal[testutil.TestHash, *testutil.TestTransaction], numNodes)
	for i := 0; i < numNodes; i++ {
		cfg, err := narwhal.NewConfig(
			narwhal.WithMyIndex[testutil.TestHash, *testutil.TestTransaction](uint16(i)),
			narwhal.WithValidators[testutil.TestHash, *testutil.TestTransaction](validators),
			narwhal.WithSigner[testutil.TestHash, *testutil.TestTransaction](validators.GetSigner(uint16(i))),
			narwhal.WithStorage[testutil.TestHash, *testutil.TestTransaction](storages[i]),
			narwhal.WithNetwork[testutil.TestHash, *testutil.TestTransaction](networks[i]),
			narwhal.WithTimer[testutil.TestHash, *testutil.TestTransaction](timer.NewRealTimer(time.Millisecond)),
			narwhal.WithBatchSize[testutil.TestHash, *testutil.TestTransaction](2), // Small batches for testing
			narwhal.WithBatchTimeout[testutil.TestHash, *testutil.TestTransaction](50*time.Millisecond),
			narwhal.WithHeaderTimeout[testutil.TestHash, *testutil.TestTransaction](100*time.Millisecond),
			narwhal.WithLogger[testutil.TestHash, *testutil.TestTransaction](zap.NewNop()),
		)
		require.NoError(t, err, "failed to create config for node %d", i)

		node, err := narwhal.New(cfg, testutil.ComputeHash)
		require.NoError(t, err, "failed to create node %d", i)
		nodes[i] = node

		err = node.Start()
		require.NoError(t, err, "failed to start node %d", i)
	}

	// Cleanup on exit
	defer func() {
		for i, node := range nodes {
			node.Stop()
			networks[i].Close()
		}
	}()

	// Submit transactions to each node
	submittedTxs := make(map[string]bool)
	for i := 0; i < numNodes; i++ {
		for j := 0; j < txPerNode; j++ {
			txData := []byte(fmt.Sprintf("node%d-tx%d", i, j))
			tx := testutil.NewTestTransaction(txData)
			nodes[i].AddTransaction(tx)
			submittedTxs[tx.Hash().String()] = true
		}
	}
	t.Logf("Submitted %d transactions across %d nodes", len(submittedTxs), numNodes)

	// Wait for certificates to form and DAG to advance
	// We expect at least round 1 to be reached (requires 3 certs at round 0)
	deadline := time.Now().Add(5 * time.Second)
	var success bool
	for time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)

		// Check if all nodes have advanced to at least round 1
		allAdvanced := true
		for i, node := range nodes {
			round := node.CurrentRound()
			if round < 1 {
				allAdvanced = false
				t.Logf("Node %d at round %d", i, round)
			}
		}
		if allAdvanced {
			success = true
			break
		}
	}

	require.True(t, success, "Not all nodes advanced to round 1")

	// Verify certificates exist on each node's DAG
	for i, node := range nodes {
		dag := node.GetDAG()
		stats := dag.Stats()
		t.Logf("Node %d: round=%d, vertices=%d, uncommitted=%d",
			i, stats.CurrentRound, stats.TotalVertices, stats.UncommittedCount)

		// Each node should have at least quorum (3) certificates for round 0
		round0Count := dag.CertificateCountForRound(0)
		assert.GreaterOrEqual(t, round0Count, 3,
			"Node %d should have at least 3 round-0 certificates, got %d", i, round0Count)
	}

	// Extract uncommitted certificates from node 0 and verify transactions
	uncommitted := nodes[0].GetCertifiedVertices()
	require.NotEmpty(t, uncommitted, "Should have uncommitted certificates")
	t.Logf("Found %d uncommitted certificates", len(uncommitted))

	// Get transactions from certificates
	txs, err := nodes[0].GetTransactions(uncommitted)
	require.NoError(t, err, "Failed to get transactions from certificates")
	t.Logf("Extracted %d transactions from certificates", len(txs))

	// Verify at least some transactions were captured
	// Note: Not all transactions may be included yet depending on timing
	assert.NotEmpty(t, txs, "Should have extracted some transactions")

	// Mark certificates as committed
	nodes[0].MarkCommitted(uncommitted)

	// Verify uncommitted is now empty
	remaining := nodes[0].GetCertifiedVertices()
	assert.Empty(t, remaining, "All certificates should be marked as committed")
}

// TestNarwhal_CertificateFormation tests that certificates form correctly
// when votes are collected from a quorum of validators.
func TestNarwhal_CertificateFormation(t *testing.T) {
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
			narwhal.WithBatchSize[testutil.TestHash, *testutil.TestTransaction](1), // Single tx per batch
			narwhal.WithBatchTimeout[testutil.TestHash, *testutil.TestTransaction](20*time.Millisecond),
			narwhal.WithHeaderTimeout[testutil.TestHash, *testutil.TestTransaction](50*time.Millisecond),
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

	// Submit one transaction to trigger certificate formation
	tx := testutil.NewTestTransaction([]byte("test-tx"))
	nodes[0].AddTransaction(tx)

	// Wait for certificate to form
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		certs := nodes[0].GetCertifiedVertices()
		if len(certs) > 0 {
			// Verify the certificate has quorum signatures
			cert := certs[0]
			signerCount := cert.SignerCount()
			require.GreaterOrEqual(t, signerCount, 3, "Certificate should have at least 2f+1 signers")
			t.Logf("Certificate formed with %d signers", signerCount)
			return
		}
	}

	t.Fatal("No certificate formed within timeout")
}
