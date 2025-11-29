// Package testutil provides test utilities for the Narwhal library.
package testutil

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"

	"github.com/edgedlt/narwhal"
)

// TestHash is a 32-byte hash type for testing.
type TestHash [32]byte

// Bytes returns the hash bytes.
func (h TestHash) Bytes() []byte { return h[:] }

// Equals returns true if the hashes are equal.
func (h TestHash) Equals(other narwhal.Hash) bool {
	if o, ok := other.(TestHash); ok {
		return h == o
	}
	return false
}

// String returns the hex-encoded hash.
func (h TestHash) String() string { return hex.EncodeToString(h[:8]) + "..." }

// HashFromBytes creates a TestHash from bytes.
func HashFromBytes(data []byte) (TestHash, error) {
	if len(data) != 32 {
		return TestHash{}, fmt.Errorf("expected 32 bytes, got %d", len(data))
	}
	var h TestHash
	copy(h[:], data)
	return h, nil
}

// ComputeHash computes a SHA256 hash.
func ComputeHash(data []byte) TestHash {
	return sha256.Sum256(data)
}

// TestTransaction is a simple transaction for testing.
type TestTransaction struct {
	hash TestHash
	data []byte
}

// NewTestTransaction creates a new test transaction.
func NewTestTransaction(data []byte) *TestTransaction {
	tx := &TestTransaction{data: data}
	tx.hash = ComputeHash(data)
	return tx
}

// NewTestTransactionSized creates a test transaction of a specific size.
// The data is padded with zeros to reach the target size.
func NewTestTransactionSized(id []byte, size int) *TestTransaction {
	data := make([]byte, size)
	copy(data, id)
	tx := &TestTransaction{data: data}
	tx.hash = ComputeHash(data)
	return tx
}

// Hash returns the transaction hash.
func (t *TestTransaction) Hash() TestHash { return t.hash }

// Bytes returns the transaction data.
func (t *TestTransaction) Bytes() []byte { return t.data }

// TransactionFromBytes deserializes a transaction.
func TransactionFromBytes(data []byte) (*TestTransaction, error) {
	return NewTestTransaction(data), nil
}

// TestPublicKey wraps an Ed25519 public key.
type TestPublicKey struct {
	key ed25519.PublicKey
}

// Bytes returns the public key bytes.
func (k TestPublicKey) Bytes() []byte { return k.key }

// Verify verifies a signature.
func (k TestPublicKey) Verify(message, signature []byte) bool {
	return ed25519.Verify(k.key, message, signature)
}

// Equals returns true if the keys are equal.
func (k TestPublicKey) Equals(other interface{ Bytes() []byte }) bool {
	if other == nil {
		return false
	}
	if o, ok := other.(TestPublicKey); ok {
		return string(k.key) == string(o.key)
	}
	// Fall back to byte comparison
	otherBytes := other.Bytes()
	return string(k.key) == string(otherBytes)
}

// TestSigner wraps an Ed25519 private key.
type TestSigner struct {
	privateKey ed25519.PrivateKey
	publicKey  TestPublicKey
}

// NewTestSigner creates a new test signer with a random key.
func NewTestSigner() *TestSigner {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	return &TestSigner{
		privateKey: priv,
		publicKey:  TestPublicKey{key: pub},
	}
}

// Sign signs a message.
func (s *TestSigner) Sign(message []byte) ([]byte, error) {
	return ed25519.Sign(s.privateKey, message), nil
}

// PublicKey returns the public key.
func (s *TestSigner) PublicKey() narwhal.PublicKey {
	return s.publicKey
}

// TestValidatorSet is a validator set for testing.
type TestValidatorSet struct {
	signers []*TestSigner
}

// NewTestValidatorSet creates a new validator set with n validators.
func NewTestValidatorSet(n int) *TestValidatorSet {
	vs := &TestValidatorSet{signers: make([]*TestSigner, n)}
	for i := 0; i < n; i++ {
		vs.signers[i] = NewTestSigner()
	}
	return vs
}

// Count returns the number of validators.
func (v *TestValidatorSet) Count() int { return len(v.signers) }

// GetByIndex returns the public key for a validator.
func (v *TestValidatorSet) GetByIndex(index uint16) (narwhal.PublicKey, error) {
	if int(index) >= len(v.signers) {
		return nil, fmt.Errorf("index %d out of range", index)
	}
	return v.signers[index].PublicKey(), nil
}

// Contains returns true if the index is valid.
func (v *TestValidatorSet) Contains(index uint16) bool {
	return int(index) < len(v.signers)
}

// F returns the max Byzantine faults.
func (v *TestValidatorSet) F() int { return (len(v.signers) - 1) / 3 }

// GetSigner returns the signer for a validator index.
func (v *TestValidatorSet) GetSigner(index uint16) *TestSigner {
	if int(index) >= len(v.signers) {
		return nil
	}
	return v.signers[index]
}

// TestStorage is an in-memory storage for testing.
type TestStorage struct {
	mu           sync.RWMutex
	batches      map[string]*narwhal.Batch[TestHash, *TestTransaction]
	headers      map[string]*narwhal.Header[TestHash]
	certificates map[string]*narwhal.Certificate[TestHash]
	highestRound uint64
}

// NewTestStorage creates a new test storage.
func NewTestStorage() *TestStorage {
	return &TestStorage{
		batches:      make(map[string]*narwhal.Batch[TestHash, *TestTransaction]),
		headers:      make(map[string]*narwhal.Header[TestHash]),
		certificates: make(map[string]*narwhal.Certificate[TestHash]),
	}
}

func (s *TestStorage) GetBatch(digest TestHash) (*narwhal.Batch[TestHash, *TestTransaction], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.batches[digest.String()]
	if !ok {
		return nil, fmt.Errorf("batch not found")
	}
	return b, nil
}

func (s *TestStorage) PutBatch(batch *narwhal.Batch[TestHash, *TestTransaction]) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches[batch.Digest.String()] = batch
	return nil
}

func (s *TestStorage) HasBatch(digest TestHash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.batches[digest.String()]
	return ok
}

func (s *TestStorage) GetHeader(digest TestHash) (*narwhal.Header[TestHash], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h, ok := s.headers[digest.String()]
	if !ok {
		return nil, fmt.Errorf("header not found")
	}
	return h, nil
}

func (s *TestStorage) PutHeader(header *narwhal.Header[TestHash]) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.headers[header.Digest.String()] = header
	return nil
}

func (s *TestStorage) GetCertificate(digest TestHash) (*narwhal.Certificate[TestHash], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.certificates[digest.String()]
	if !ok {
		return nil, fmt.Errorf("certificate not found")
	}
	return c, nil
}

func (s *TestStorage) PutCertificate(cert *narwhal.Certificate[TestHash]) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.certificates[cert.Header.Digest.String()] = cert
	return nil
}

func (s *TestStorage) GetHighestRound() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.highestRound, nil
}

func (s *TestStorage) PutHighestRound(round uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.highestRound = round
	return nil
}

func (s *TestStorage) DeleteBeforeRound(_ uint64) error {
	return nil // No-op for tests
}

func (s *TestStorage) GetCertificatesByRound(round uint64) ([]*narwhal.Certificate[TestHash], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var certs []*narwhal.Certificate[TestHash]
	for _, cert := range s.certificates {
		if cert.Header.Round == round {
			certs = append(certs, cert)
		}
	}
	return certs, nil
}

func (s *TestStorage) GetCertificatesInRange(startRound, endRound uint64) ([]*narwhal.Certificate[TestHash], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var certs []*narwhal.Certificate[TestHash]
	for _, cert := range s.certificates {
		if cert.Header.Round >= startRound && cert.Header.Round <= endRound {
			certs = append(certs, cert)
		}
	}
	// Sort by round
	sort.Slice(certs, func(i, j int) bool {
		return certs[i].Header.Round < certs[j].Header.Round
	})
	return certs, nil
}

func (s *TestStorage) Close() error { return nil }

// TestNetwork is a channel-based network for testing.
type TestNetwork struct {
	mu       sync.RWMutex
	nodes    map[uint16]*TestNetwork
	myIndex  uint16
	incoming chan narwhal.Message[TestHash, *TestTransaction]
	closed   bool
}

// testBatchMessage wraps a batch message with sender info.
type testBatchMessage struct {
	batch  *narwhal.Batch[TestHash, *TestTransaction]
	sender uint16
}

func (m *testBatchMessage) Type() narwhal.MessageType { return narwhal.MessageBatch }
func (m *testBatchMessage) Sender() uint16            { return m.sender }
func (m *testBatchMessage) Batch() *narwhal.Batch[TestHash, *TestTransaction] {
	return m.batch
}

// testHeaderMessage wraps a header message with sender info.
type testHeaderMessage struct {
	header *narwhal.Header[TestHash]
	sender uint16
}

func (m *testHeaderMessage) Type() narwhal.MessageType { return narwhal.MessageHeader }
func (m *testHeaderMessage) Sender() uint16            { return m.sender }
func (m *testHeaderMessage) Header() *narwhal.Header[TestHash] {
	return m.header
}

// testVoteMessage wraps a vote message with sender info.
type testVoteMessage struct {
	vote   *narwhal.HeaderVote[TestHash]
	sender uint16
}

func (m *testVoteMessage) Type() narwhal.MessageType { return narwhal.MessageVote }
func (m *testVoteMessage) Sender() uint16            { return m.sender }
func (m *testVoteMessage) Vote() *narwhal.HeaderVote[TestHash] {
	return m.vote
}

// testCertificateMessage wraps a certificate message with sender info.
type testCertificateMessage struct {
	cert   *narwhal.Certificate[TestHash]
	sender uint16
}

func (m *testCertificateMessage) Type() narwhal.MessageType { return narwhal.MessageCertificate }
func (m *testCertificateMessage) Sender() uint16            { return m.sender }
func (m *testCertificateMessage) Certificate() *narwhal.Certificate[TestHash] {
	return m.cert
}

// NewTestNetwork creates a new test network node.
func NewTestNetwork(myIndex uint16) *TestNetwork {
	return &TestNetwork{
		myIndex:  myIndex,
		nodes:    make(map[uint16]*TestNetwork),
		incoming: make(chan narwhal.Message[TestHash, *TestTransaction], 1000),
	}
}

// Connect connects two network nodes.
func (n *TestNetwork) Connect(other *TestNetwork) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[other.myIndex] = other
	other.mu.Lock()
	defer other.mu.Unlock()
	other.nodes[n.myIndex] = n
}

func (n *TestNetwork) BroadcastBatch(batch *narwhal.Batch[TestHash, *TestTransaction]) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, node := range n.nodes {
		msg := &testBatchMessage{batch: batch, sender: n.myIndex}
		select {
		case node.incoming <- msg:
		default:
		}
	}
}

func (n *TestNetwork) BroadcastHeader(header *narwhal.Header[TestHash]) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, node := range n.nodes {
		msg := &testHeaderMessage{header: header, sender: n.myIndex}
		select {
		case node.incoming <- msg:
		default:
		}
	}
}

func (n *TestNetwork) SendVote(validatorIndex uint16, vote *narwhal.HeaderVote[TestHash]) {
	n.mu.RLock()
	node := n.nodes[validatorIndex]
	n.mu.RUnlock()
	if node != nil {
		msg := &testVoteMessage{vote: vote, sender: n.myIndex}
		select {
		case node.incoming <- msg:
		default:
		}
	}
}

func (n *TestNetwork) BroadcastCertificate(cert *narwhal.Certificate[TestHash]) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, node := range n.nodes {
		msg := &testCertificateMessage{cert: cert, sender: n.myIndex}
		select {
		case node.incoming <- msg:
		default:
		}
	}
}

func (n *TestNetwork) FetchBatch(_ uint16, _ TestHash) (*narwhal.Batch[TestHash, *TestTransaction], error) {
	return nil, fmt.Errorf("not implemented in test")
}

func (n *TestNetwork) FetchCertificate(_ uint16, _ TestHash) (*narwhal.Certificate[TestHash], error) {
	return nil, fmt.Errorf("not implemented in test")
}

func (n *TestNetwork) FetchCertificatesInRange(_ uint16, _, _ uint64) ([]*narwhal.Certificate[TestHash], error) {
	return nil, fmt.Errorf("not implemented in test")
}

func (n *TestNetwork) SendBatch(to uint16, batch *narwhal.Batch[TestHash, *TestTransaction]) {}

func (n *TestNetwork) SendCertificate(to uint16, cert *narwhal.Certificate[TestHash]) {}

func (n *TestNetwork) Receive() <-chan narwhal.Message[TestHash, *TestTransaction] {
	return n.incoming
}

func (n *TestNetwork) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.closed {
		n.closed = true
		close(n.incoming)
	}
	return nil
}
