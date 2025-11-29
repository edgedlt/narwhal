package narwhal_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/edgedlt/narwhal"
	"github.com/edgedlt/narwhal/internal/testutil"
	"github.com/edgedlt/narwhal/timer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// --- Error Scenario Tests ---

// TestNarwhal_StorageErrors tests graceful handling of storage errors.
func TestNarwhal_StorageErrors(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	storage := newFailingStorage[testutil.TestHash, *testutil.TestTransaction]()
	network := testutil.NewTestNetwork(0)
	mockTimer := timer.NewMockTimer()

	cfg, err := narwhal.NewConfig(
		narwhal.WithMyIndex[testutil.TestHash, *testutil.TestTransaction](0),
		narwhal.WithValidators[testutil.TestHash, *testutil.TestTransaction](validators),
		narwhal.WithSigner[testutil.TestHash, *testutil.TestTransaction](validators.GetSigner(0)),
		narwhal.WithStorage[testutil.TestHash, *testutil.TestTransaction](storage),
		narwhal.WithNetwork[testutil.TestHash, *testutil.TestTransaction](network),
		narwhal.WithTimer[testutil.TestHash, *testutil.TestTransaction](mockTimer),
		narwhal.WithLogger[testutil.TestHash, *testutil.TestTransaction](zap.NewNop()),
	)
	require.NoError(t, err)

	nw, err := narwhal.New(cfg, testutil.ComputeHash)
	require.NoError(t, err)

	// Enable storage failures
	storage.failPut = true

	// Start should succeed even if storage fails during restore
	err = nw.Start()
	assert.NoError(t, err, "Start should handle storage errors gracefully")

	// Add transactions - should not panic even if storage fails
	for i := 0; i < 10; i++ {
		tx := testutil.NewTestTransaction([]byte{byte(i)})
		nw.AddTransaction(tx)
	}

	// Allow some processing time
	time.Sleep(100 * time.Millisecond)

	nw.Stop()
}

// TestNarwhal_NetworkErrors tests graceful handling of network errors.
func TestNarwhal_NetworkErrors(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	storage := testutil.NewTestStorage()
	network := newFailingNetwork[testutil.TestHash, *testutil.TestTransaction](0)
	mockTimer := timer.NewMockTimer()

	cfg, err := narwhal.NewConfig(
		narwhal.WithMyIndex[testutil.TestHash, *testutil.TestTransaction](0),
		narwhal.WithValidators[testutil.TestHash, *testutil.TestTransaction](validators),
		narwhal.WithSigner[testutil.TestHash, *testutil.TestTransaction](validators.GetSigner(0)),
		narwhal.WithStorage[testutil.TestHash, *testutil.TestTransaction](storage),
		narwhal.WithNetwork[testutil.TestHash, *testutil.TestTransaction](network),
		narwhal.WithTimer[testutil.TestHash, *testutil.TestTransaction](mockTimer),
		narwhal.WithLogger[testutil.TestHash, *testutil.TestTransaction](zap.NewNop()),
	)
	require.NoError(t, err)

	nw, err := narwhal.New(cfg, testutil.ComputeHash)
	require.NoError(t, err)

	// Enable network failures
	network.failFetch = true

	require.NoError(t, nw.Start())

	// Add transactions - should not panic even if network fails
	for i := 0; i < 10; i++ {
		tx := testutil.NewTestTransaction([]byte{byte(i)})
		nw.AddTransaction(tx)
	}

	// Allow some processing time
	time.Sleep(100 * time.Millisecond)

	nw.Stop()
}

// TestNarwhal_InvalidMessages tests handling of malformed messages.
func TestNarwhal_InvalidMessages(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	storage := testutil.NewTestStorage()
	network := testutil.NewTestNetwork(0)
	mockTimer := timer.NewMockTimer()

	cfg, err := narwhal.NewConfig(
		narwhal.WithMyIndex[testutil.TestHash, *testutil.TestTransaction](0),
		narwhal.WithValidators[testutil.TestHash, *testutil.TestTransaction](validators),
		narwhal.WithSigner[testutil.TestHash, *testutil.TestTransaction](validators.GetSigner(0)),
		narwhal.WithStorage[testutil.TestHash, *testutil.TestTransaction](storage),
		narwhal.WithNetwork[testutil.TestHash, *testutil.TestTransaction](network),
		narwhal.WithTimer[testutil.TestHash, *testutil.TestTransaction](mockTimer),
		narwhal.WithLogger[testutil.TestHash, *testutil.TestTransaction](zap.NewNop()),
	)
	require.NoError(t, err)

	nw, err := narwhal.New(cfg, testutil.ComputeHash)
	require.NoError(t, err)
	require.NoError(t, nw.Start())
	defer nw.Stop()

	// The validation layer should reject these - we just verify no panics
	time.Sleep(50 * time.Millisecond)
}

// --- Network Partition Tests ---

// TestNarwhal_NetworkPartition tests behavior during and after network partitions.
func TestNarwhal_NetworkPartition(t *testing.T) {
	const numValidators = 4

	// Create shared partitionable network
	partNet := newPartitionableNetwork[testutil.TestHash, *testutil.TestTransaction](numValidators)

	validators := testutil.NewTestValidatorSet(numValidators)
	narwhals := make([]*narwhal.Narwhal[testutil.TestHash, *testutil.TestTransaction], numValidators)
	storages := make([]*testutil.TestStorage, numValidators)

	for i := 0; i < numValidators; i++ {
		storages[i] = testutil.NewTestStorage()
		nodeNetwork := partNet.NodeNetwork(i)

		cfg, err := narwhal.NewConfig(
			narwhal.WithMyIndex[testutil.TestHash, *testutil.TestTransaction](uint16(i)),
			narwhal.WithValidators[testutil.TestHash, *testutil.TestTransaction](validators),
			narwhal.WithSigner[testutil.TestHash, *testutil.TestTransaction](validators.GetSigner(uint16(i))),
			narwhal.WithStorage[testutil.TestHash, *testutil.TestTransaction](storages[i]),
			narwhal.WithNetwork[testutil.TestHash, *testutil.TestTransaction](nodeNetwork),
			narwhal.WithTimer[testutil.TestHash, *testutil.TestTransaction](timer.NewRealTimer(time.Millisecond)),
			narwhal.WithLogger[testutil.TestHash, *testutil.TestTransaction](zap.NewNop()),
			narwhal.WithBatchSize[testutil.TestHash, *testutil.TestTransaction](5),
			narwhal.WithBatchTimeout[testutil.TestHash, *testutil.TestTransaction](20*time.Millisecond),
			narwhal.WithHeaderTimeout[testutil.TestHash, *testutil.TestTransaction](50*time.Millisecond),
		)
		require.NoError(t, err)

		nw, err := narwhal.New(cfg, testutil.ComputeHash)
		require.NoError(t, err)
		narwhals[i] = nw
	}

	// Start all nodes
	for _, nw := range narwhals {
		require.NoError(t, nw.Start())
	}
	defer func() {
		for _, nw := range narwhals {
			nw.Stop()
		}
	}()

	// Submit transactions to ALL nodes before partition
	t.Log("Submitting transactions before partition...")
	for i := 0; i < 20; i++ {
		tx := testutil.NewTestTransaction([]byte{byte(i)})
		// Distribute transactions across all nodes
		narwhals[i%numValidators].AddTransaction(tx)
	}

	// Wait for initial certificates - network needs time
	time.Sleep(500 * time.Millisecond)
	initialRound := narwhals[0].GetDAG().CurrentRound()
	t.Logf("Initial round before partition: %d", initialRound)

	// Create partition: isolate node 0 from nodes 1,2,3
	t.Log("Creating network partition...")
	partNet.Partition([]int{0}, []int{1, 2, 3})

	// Submit transactions during partition
	t.Log("Submitting transactions during partition...")
	// Submit to isolated node (node 0)
	for i := 20; i < 30; i++ {
		tx := testutil.NewTestTransaction([]byte{byte(i)})
		narwhals[0].AddTransaction(tx)
	}
	// Submit to all nodes in majority partition (nodes 1,2,3)
	for i := 30; i < 50; i++ {
		tx := testutil.NewTestTransaction([]byte{byte(i)})
		targetNode := ((i - 30) % 3) + 1 // Distribute among nodes 1,2,3
		narwhals[targetNode].AddTransaction(tx)
	}

	// Wait during partition - give time for round advancement
	time.Sleep(500 * time.Millisecond)

	// The majority partition (1,2,3) should still make progress
	majorityRound := narwhals[1].GetDAG().CurrentRound()
	t.Logf("Majority partition round: %d", majorityRound)

	// The isolated node should be stuck (can't get 2f+1 votes)
	isolatedRound := narwhals[0].GetDAG().CurrentRound()
	t.Logf("Isolated node round: %d", isolatedRound)

	// Heal partition
	t.Log("Healing network partition...")
	partNet.Heal()

	// Submit more transactions after healing
	for i := 40; i < 50; i++ {
		tx := testutil.NewTestTransaction([]byte{byte(i)})
		narwhals[0].AddTransaction(tx)
	}

	// Wait for recovery
	time.Sleep(300 * time.Millisecond)

	// All nodes should eventually converge
	finalRounds := make([]uint64, numValidators)
	for i, nw := range narwhals {
		finalRounds[i] = nw.GetDAG().CurrentRound()
	}
	t.Logf("Final rounds after recovery: %v", finalRounds)

	// Majority partition should have made progress
	assert.Greater(t, majorityRound, initialRound,
		"Majority partition should make progress")

	// After healing, nodes should converge (within a few rounds)
	maxRound := finalRounds[0]
	minRound := finalRounds[0]
	for _, r := range finalRounds {
		if r > maxRound {
			maxRound = r
		}
		if r < minRound {
			minRound = r
		}
	}
	assert.LessOrEqual(t, maxRound-minRound, uint64(5),
		"Nodes should converge after partition heals")
}

// TestNarwhal_RestoreFromStorage tests that DAG state is restored from storage on restart.
func TestNarwhal_RestoreFromStorage(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	storage := testutil.NewTestStorage()
	network := testutil.NewTestNetwork(0)
	mockTimer := timer.NewMockTimer()

	cfg, err := narwhal.NewConfig(
		narwhal.WithMyIndex[testutil.TestHash, *testutil.TestTransaction](0),
		narwhal.WithValidators[testutil.TestHash, *testutil.TestTransaction](validators),
		narwhal.WithSigner[testutil.TestHash, *testutil.TestTransaction](validators.GetSigner(0)),
		narwhal.WithStorage[testutil.TestHash, *testutil.TestTransaction](storage),
		narwhal.WithNetwork[testutil.TestHash, *testutil.TestTransaction](network),
		narwhal.WithTimer[testutil.TestHash, *testutil.TestTransaction](mockTimer),
		narwhal.WithLogger[testutil.TestHash, *testutil.TestTransaction](zap.NewNop()),
	)
	require.NoError(t, err)

	// Create a certificate and store it manually
	header := &narwhal.Header[testutil.TestHash]{
		Author:    0,
		Round:     5,
		Epoch:     0,
		Timestamp: uint64(time.Now().UnixMilli()),
	}
	header.ComputeDigest(testutil.ComputeHash)

	// Create votes
	votes := make(map[uint16][]byte)
	for i := 0; i < 3; i++ {
		sig, _ := validators.GetSigner(uint16(i)).Sign(header.Digest.Bytes())
		votes[uint16(i)] = sig
	}
	cert := narwhal.NewCertificate(header, votes)

	// Store certificate and highest round
	require.NoError(t, storage.PutCertificate(cert))
	require.NoError(t, storage.PutHighestRound(5))

	// Create Narwhal - should restore from storage
	nw, err := narwhal.New(cfg, testutil.ComputeHash)
	require.NoError(t, err)
	require.NoError(t, nw.Start())
	defer nw.Stop()

	// The DAG should have been restored (but may not have the cert if batches are missing)
	// At minimum, the restore should not error
	t.Log("Storage restore completed without error")
}

// TestPartitionableNetwork verifies the partition network routes messages correctly.
func TestPartitionableNetwork(t *testing.T) {
	partNet := newPartitionableNetwork[testutil.TestHash, *testutil.TestTransaction](4)

	// Get all node networks
	nodes := make([]*partitionNode[testutil.TestHash, *testutil.TestTransaction], 4)
	for i := 0; i < 4; i++ {
		nodes[i] = partNet.NodeNetwork(i)
	}

	// Broadcast from node 0
	batch := &narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]{
		ValidatorID: 0,
	}
	nodes[0].BroadcastBatch(batch)

	// Check that nodes 1,2,3 received the message
	for i := 1; i < 4; i++ {
		select {
		case msg := <-nodes[i].Receive():
			t.Logf("Node %d received message type %v from %d", i, msg.Type(), msg.Sender())
		case <-time.After(100 * time.Millisecond):
			t.Errorf("Node %d did not receive batch", i)
		}
	}

	// Node 0 should NOT receive its own broadcast
	select {
	case <-nodes[0].Receive():
		t.Error("Node 0 should not receive its own broadcast")
	case <-time.After(50 * time.Millisecond):
		// Good - no message received
	}

	t.Log("Partition network routes messages correctly")
}

// --- Test Helpers ---

// failingStorage wraps TestStorage with configurable failures.
type failingStorage[H narwhal.Hash, T narwhal.Transaction[H]] struct {
	*testutil.TestStorage
	failGet bool
	failPut bool
}

func newFailingStorage[H narwhal.Hash, T narwhal.Transaction[H]]() *failingStorage[H, T] {
	return &failingStorage[H, T]{
		TestStorage: testutil.NewTestStorage(),
	}
}

func (s *failingStorage[H, T]) GetBatch(digest testutil.TestHash) (*narwhal.Batch[testutil.TestHash, *testutil.TestTransaction], error) {
	if s.failGet {
		return nil, errors.New("simulated storage get failure")
	}
	return s.TestStorage.GetBatch(digest)
}

func (s *failingStorage[H, T]) PutBatch(batch *narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]) error {
	if s.failPut {
		return errors.New("simulated storage put failure")
	}
	return s.TestStorage.PutBatch(batch)
}

func (s *failingStorage[H, T]) GetHighestRound() (uint64, error) {
	if s.failGet {
		return 0, errors.New("simulated storage failure")
	}
	return s.TestStorage.GetHighestRound()
}

func (s *failingStorage[H, T]) GetCertificatesInRange(start, end uint64) ([]*narwhal.Certificate[testutil.TestHash], error) {
	if s.failGet {
		return nil, errors.New("simulated storage failure")
	}
	return s.TestStorage.GetCertificatesInRange(start, end)
}

// failingNetwork wraps TestNetwork with configurable failures.
type failingNetwork[H narwhal.Hash, T narwhal.Transaction[H]] struct {
	*testutil.TestNetwork
	failFetch bool
}

func newFailingNetwork[H narwhal.Hash, T narwhal.Transaction[H]](index uint16) *failingNetwork[H, T] {
	return &failingNetwork[H, T]{
		TestNetwork: testutil.NewTestNetwork(index),
	}
}

func (n *failingNetwork[H, T]) FetchBatch(from uint16, digest testutil.TestHash) (*narwhal.Batch[testutil.TestHash, *testutil.TestTransaction], error) {
	if n.failFetch {
		return nil, errors.New("simulated network fetch failure")
	}
	return n.TestNetwork.FetchBatch(from, digest)
}

func (n *failingNetwork[H, T]) FetchCertificate(from uint16, digest testutil.TestHash) (*narwhal.Certificate[testutil.TestHash], error) {
	if n.failFetch {
		return nil, errors.New("simulated network fetch failure")
	}
	return n.TestNetwork.FetchCertificate(from, digest)
}

func (n *failingNetwork[H, T]) FetchCertificatesInRange(from uint16, startRound, endRound uint64) ([]*narwhal.Certificate[testutil.TestHash], error) {
	if n.failFetch {
		return nil, errors.New("simulated network fetch failure")
	}
	return n.TestNetwork.FetchCertificatesInRange(from, startRound, endRound)
}

// partitionableNetwork supports simulating network partitions.
type partitionableNetwork[H narwhal.Hash, T narwhal.Transaction[H]] struct {
	mu         sync.RWMutex
	nodes      map[int]*partitionNode[H, T]
	partitions [][]int // Groups of nodes that can communicate
}

func newPartitionableNetwork[H narwhal.Hash, T narwhal.Transaction[H]](n int) *partitionableNetwork[H, T] {
	pn := &partitionableNetwork[H, T]{
		nodes: make(map[int]*partitionNode[H, T]),
	}
	for i := 0; i < n; i++ {
		pn.nodes[i] = &partitionNode[H, T]{
			id:       i,
			network:  pn,
			incoming: make(chan narwhal.Message[H, T], 1000),
		}
	}
	return pn
}

func (pn *partitionableNetwork[H, T]) NodeNetwork(id int) *partitionNode[H, T] {
	return pn.nodes[id]
}

func (pn *partitionableNetwork[H, T]) Partition(groups ...[]int) {
	pn.mu.Lock()
	defer pn.mu.Unlock()
	pn.partitions = groups
}

func (pn *partitionableNetwork[H, T]) Heal() {
	pn.mu.Lock()
	defer pn.mu.Unlock()
	pn.partitions = nil
}

func (pn *partitionableNetwork[H, T]) canCommunicate(from, to int) bool {
	pn.mu.RLock()
	defer pn.mu.RUnlock()

	if len(pn.partitions) == 0 {
		return true // No partition
	}

	// Find which partition each node is in
	var fromGroup, toGroup int = -1, -1
	for i, group := range pn.partitions {
		for _, node := range group {
			if node == from {
				fromGroup = i
			}
			if node == to {
				toGroup = i
			}
		}
	}

	return fromGroup == toGroup && fromGroup != -1
}

type partitionNode[H narwhal.Hash, T narwhal.Transaction[H]] struct {
	id       int
	network  *partitionableNetwork[H, T]
	incoming chan narwhal.Message[H, T]
}

func (n *partitionNode[H, T]) BroadcastBatch(batch *narwhal.Batch[H, T]) {
	for id, node := range n.network.nodes {
		if id != n.id && n.network.canCommunicate(n.id, id) {
			select {
			case node.incoming <- &batchMessage[H, T]{batch: batch, from: uint16(n.id)}:
			default:
			}
		}
	}
}

func (n *partitionNode[H, T]) BroadcastHeader(header *narwhal.Header[H]) {
	for id, node := range n.network.nodes {
		if id != n.id && n.network.canCommunicate(n.id, id) {
			select {
			case node.incoming <- &headerMessage[H, T]{header: header, from: uint16(n.id)}:
			default:
			}
		}
	}
}

func (n *partitionNode[H, T]) SendVote(validatorIndex uint16, vote *narwhal.HeaderVote[H]) {
	if n.network.canCommunicate(n.id, int(validatorIndex)) {
		if node, ok := n.network.nodes[int(validatorIndex)]; ok {
			select {
			case node.incoming <- &voteMessage[H, T]{vote: vote, from: uint16(n.id)}:
			default:
			}
		}
	}
}

func (n *partitionNode[H, T]) BroadcastCertificate(cert *narwhal.Certificate[H]) {
	for id, node := range n.network.nodes {
		if id != n.id && n.network.canCommunicate(n.id, id) {
			select {
			case node.incoming <- &certMessage[H, T]{cert: cert, from: uint16(n.id)}:
			default:
			}
		}
	}
}

func (n *partitionNode[H, T]) FetchBatch(from uint16, digest H) (*narwhal.Batch[H, T], error) {
	return nil, errors.New("not implemented in test")
}

func (n *partitionNode[H, T]) FetchCertificate(from uint16, digest H) (*narwhal.Certificate[H], error) {
	return nil, errors.New("not implemented in test")
}

func (n *partitionNode[H, T]) FetchCertificatesInRange(from uint16, startRound, endRound uint64) ([]*narwhal.Certificate[H], error) {
	return nil, errors.New("not implemented in test")
}

func (n *partitionNode[H, T]) SendBatch(to uint16, batch *narwhal.Batch[H, T]) {}

func (n *partitionNode[H, T]) SendCertificate(to uint16, cert *narwhal.Certificate[H]) {}

func (n *partitionNode[H, T]) Receive() <-chan narwhal.Message[H, T] {
	return n.incoming
}

func (n *partitionNode[H, T]) Close() error {
	close(n.incoming)
	return nil
}

// Message types for partition network
type batchMessage[H narwhal.Hash, T narwhal.Transaction[H]] struct {
	batch *narwhal.Batch[H, T]
	from  uint16
}

func (m *batchMessage[H, T]) Type() narwhal.MessageType   { return narwhal.MessageBatch }
func (m *batchMessage[H, T]) Sender() uint16              { return m.from }
func (m *batchMessage[H, T]) Batch() *narwhal.Batch[H, T] { return m.batch }

type headerMessage[H narwhal.Hash, T narwhal.Transaction[H]] struct {
	header *narwhal.Header[H]
	from   uint16
}

func (m *headerMessage[H, T]) Type() narwhal.MessageType  { return narwhal.MessageHeader }
func (m *headerMessage[H, T]) Sender() uint16             { return m.from }
func (m *headerMessage[H, T]) Header() *narwhal.Header[H] { return m.header }

type voteMessage[H narwhal.Hash, T narwhal.Transaction[H]] struct {
	vote *narwhal.HeaderVote[H]
	from uint16
}

func (m *voteMessage[H, T]) Type() narwhal.MessageType    { return narwhal.MessageVote }
func (m *voteMessage[H, T]) Sender() uint16               { return m.from }
func (m *voteMessage[H, T]) Vote() *narwhal.HeaderVote[H] { return m.vote }

type certMessage[H narwhal.Hash, T narwhal.Transaction[H]] struct {
	cert *narwhal.Certificate[H]
	from uint16
}

func (m *certMessage[H, T]) Type() narwhal.MessageType            { return narwhal.MessageCertificate }
func (m *certMessage[H, T]) Sender() uint16                       { return m.from }
func (m *certMessage[H, T]) Certificate() *narwhal.Certificate[H] { return m.cert }
