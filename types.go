// Package narwhal implements a DAG-based mempool for BFT consensus.
// It separates data availability from ordering, enabling parallel transaction
// broadcast across validators without leader bottlenecks.
package narwhal

// Hash represents a cryptographic hash for content addressing.
type Hash interface {
	Bytes() []byte
	Equals(other Hash) bool
	String() string
}

// Transaction represents a unit of data to be disseminated.
type Transaction[H Hash] interface {
	Hash() H
	Bytes() []byte
}

// ValidatorSet represents the set of validators. Quorum requires 2f+1 validators.
type ValidatorSet interface {
	Count() int
	GetByIndex(index uint16) (PublicKey, error)
	Contains(index uint16) bool
	F() int // max Byzantine faults: (n-1)/3
}

// PublicKey provides signature verification.
type PublicKey interface {
	Bytes() []byte
	Verify(message []byte, signature []byte) bool
	Equals(other interface{ Bytes() []byte }) bool
}

// Signer provides cryptographic signing.
type Signer interface {
	Sign(message []byte) ([]byte, error)
	PublicKey() PublicKey
}

// Storage provides persistent storage. Put operations must be durable before returning.
type Storage[H Hash, T Transaction[H]] interface {
	GetBatch(digest H) (*Batch[H, T], error)
	PutBatch(batch *Batch[H, T]) error
	HasBatch(digest H) bool

	GetHeader(digest H) (*Header[H], error)
	PutHeader(header *Header[H]) error

	GetCertificate(digest H) (*Certificate[H], error)
	PutCertificate(cert *Certificate[H]) error

	GetHighestRound() (uint64, error)
	PutHighestRound(round uint64) error
	DeleteBeforeRound(round uint64) error

	GetCertificatesByRound(round uint64) ([]*Certificate[H], error)
	GetCertificatesInRange(startRound, endRound uint64) ([]*Certificate[H], error)

	Close() error
}

// Network provides message delivery between validators.
type Network[H Hash, T Transaction[H]] interface {
	BroadcastBatch(batch *Batch[H, T])
	BroadcastHeader(header *Header[H])
	SendVote(validatorIndex uint16, vote *HeaderVote[H])
	BroadcastCertificate(cert *Certificate[H])

	FetchBatch(from uint16, digest H) (*Batch[H, T], error)
	FetchCertificate(from uint16, digest H) (*Certificate[H], error)
	FetchCertificatesInRange(from uint16, startRound, endRound uint64) ([]*Certificate[H], error)

	SendBatch(to uint16, batch *Batch[H, T])
	SendCertificate(to uint16, cert *Certificate[H])

	Receive() <-chan Message[H, T]
	Close() error
}

type NetworkStatus uint8

const (
	NetworkStatusUnknown NetworkStatus = iota
	NetworkStatusConnected
	NetworkStatusDisconnected
	NetworkStatusConnecting
)

// ReconnectableNetwork extends Network with reconnection and health checking.
type ReconnectableNetwork[H Hash, T Transaction[H]] interface {
	Network[H, T]
	Connect(validatorIndex uint16) error
	Disconnect(validatorIndex uint16) error
	Reconnect(validatorIndex uint16) error
	Status(validatorIndex uint16) NetworkStatus
	IsConnected(validatorIndex uint16) bool
	ConnectedPeers() []uint16
	OnDisconnect(callback func(validatorIndex uint16))
	OnReconnect(callback func(validatorIndex uint16))
}

// Timer provides timeout management.
type Timer interface {
	Start()
	Stop()
	Reset()
	C() <-chan struct{}
}

// Message represents a protocol message.
type Message[H Hash, T Transaction[H]] interface {
	Type() MessageType
	Sender() uint16
}

type BatchMessageAccessor[H Hash, T Transaction[H]] interface{ Batch() *Batch[H, T] }
type HeaderMessageAccessor[H Hash] interface{ Header() *Header[H] }
type VoteMessageAccessor[H Hash] interface{ Vote() *HeaderVote[H] }
type CertificateMessageAccessor[H Hash] interface{ Certificate() *Certificate[H] }

type MessageType uint8

const (
	MessageBatch MessageType = iota
	MessageHeader
	MessageVote
	MessageCertificate
	MessageBatchRequest
	MessageCertificateRequest
	MessageCertificateRangeRequest
	MessageCertificateRangeResponse
)

// String returns a human-readable name for the message type.
func (t MessageType) String() string {
	switch t {
	case MessageBatch:
		return "BATCH"
	case MessageHeader:
		return "HEADER"
	case MessageVote:
		return "VOTE"
	case MessageCertificate:
		return "CERTIFICATE"
	case MessageBatchRequest:
		return "BATCH_REQUEST"
	case MessageCertificateRequest:
		return "CERTIFICATE_REQUEST"
	case MessageCertificateRangeRequest:
		return "CERTIFICATE_RANGE_REQUEST"
	case MessageCertificateRangeResponse:
		return "CERTIFICATE_RANGE_RESPONSE"
	default:
		return "UNKNOWN"
	}
}

// Batch is a collection of transactions created by a worker.
type Batch[H Hash, T Transaction[H]] struct {
	WorkerID     uint16
	ValidatorID  uint16
	Round        uint64
	Transactions []T
	Digest       H
}

// Header is a proposal from a primary to form a DAG vertex.
type Header[H Hash] struct {
	Author    uint16
	Round     uint64
	Epoch     uint64
	BatchRefs []H
	Parents   []H // digests of parent certificates from round-1
	Timestamp uint64
	Digest    H
}

// Certificate is a header with 2f+1 validator signatures.
type Certificate[H Hash] struct {
	Header       *Header[H]
	Signatures   [][]byte
	SignerBitmap uint64 // bit i set if validator i signed
}

// HeaderVote is a vote from a validator for a header.
type HeaderVote[H Hash] struct {
	HeaderDigest   H
	ValidatorIndex uint16
	Signature      []byte
}

type BatchMessage[H Hash, T Transaction[H]] struct {
	BatchData *Batch[H, T]
	sender    uint16
}

func (m *BatchMessage[H, T]) Type() MessageType   { return MessageBatch }
func (m *BatchMessage[H, T]) Sender() uint16      { return m.sender }
func (m *BatchMessage[H, T]) Batch() *Batch[H, T] { return m.BatchData }

// HeaderMessage wraps a header for network transmission.
type HeaderMessage[H Hash, T Transaction[H]] struct {
	HeaderData *Header[H]
	sender     uint16
}

func (m *HeaderMessage[H, T]) Type() MessageType  { return MessageHeader }
func (m *HeaderMessage[H, T]) Sender() uint16     { return m.sender }
func (m *HeaderMessage[H, T]) Header() *Header[H] { return m.HeaderData }

// VoteMessage wraps a vote for network transmission.
type VoteMessage[H Hash, T Transaction[H]] struct {
	VoteData *HeaderVote[H]
	sender   uint16
}

func (m *VoteMessage[H, T]) Type() MessageType    { return MessageVote }
func (m *VoteMessage[H, T]) Sender() uint16       { return m.sender }
func (m *VoteMessage[H, T]) Vote() *HeaderVote[H] { return m.VoteData }

// CertificateMessage wraps a certificate for network transmission.
type CertificateMessage[H Hash, T Transaction[H]] struct {
	CertificateData *Certificate[H]
	sender          uint16
}

func (m *CertificateMessage[H, T]) Type() MessageType            { return MessageCertificate }
func (m *CertificateMessage[H, T]) Sender() uint16               { return m.sender }
func (m *CertificateMessage[H, T]) Certificate() *Certificate[H] { return m.CertificateData }

// BatchRequestMessage requests a batch by digest.
type BatchRequestMessage[H Hash, T Transaction[H]] struct {
	Digest H
	sender uint16
}

func (m *BatchRequestMessage[H, T]) Type() MessageType { return MessageBatchRequest }
func (m *BatchRequestMessage[H, T]) Sender() uint16    { return m.sender }

// CertificateRequestMessage requests a certificate by digest.
type CertificateRequestMessage[H Hash, T Transaction[H]] struct {
	Digest H
	sender uint16
}

func (m *CertificateRequestMessage[H, T]) Type() MessageType { return MessageCertificateRequest }
func (m *CertificateRequestMessage[H, T]) Sender() uint16    { return m.sender }

// CertificateRangeRequestMessage requests certificates for a range of rounds.
type CertificateRangeRequestMessage[H Hash, T Transaction[H]] struct {
	StartRound uint64
	EndRound   uint64
	sender     uint16
}

func (m *CertificateRangeRequestMessage[H, T]) Type() MessageType {
	return MessageCertificateRangeRequest
}
func (m *CertificateRangeRequestMessage[H, T]) Sender() uint16 { return m.sender }

// CertificateRangeResponseMessage contains certificates for a range of rounds.
type CertificateRangeResponseMessage[H Hash, T Transaction[H]] struct {
	Certificates []*Certificate[H]
	StartRound   uint64
	EndRound     uint64
	sender       uint16
}

func (m *CertificateRangeResponseMessage[H, T]) Type() MessageType {
	return MessageCertificateRangeResponse
}
func (m *CertificateRangeResponseMessage[H, T]) Sender() uint16 { return m.sender }
