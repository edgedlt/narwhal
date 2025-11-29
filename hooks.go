package narwhal

import "time"

// Hooks provides optional callbacks for observability events.
// All hooks are invoked synchronously - keep implementations fast.
type Hooks[H Hash, T Transaction[H]] struct {
	// Worker events

	// OnBatchCreated is called when a worker creates a new batch.
	OnBatchCreated func(BatchCreatedEvent[H, T])

	// OnTransactionReceived is called when a transaction is added to a worker.
	OnTransactionReceived func(TransactionReceivedEvent[H])

	// Primary events

	// OnHeaderCreated is called when a primary creates a new header.
	OnHeaderCreated func(HeaderCreatedEvent[H])

	// OnHeaderReceived is called when a primary receives a header from another validator.
	OnHeaderReceived func(HeaderReceivedEvent[H])

	// OnVoteReceived is called when a primary receives a vote for its header.
	OnVoteReceived func(VoteReceivedEvent[H])

	// OnVoteSent is called when a primary sends a vote for another validator's header.
	OnVoteSent func(VoteSentEvent[H])

	// OnCertificateFormed is called when a primary forms a certificate (collected quorum).
	OnCertificateFormed func(CertificateFormedEvent[H])

	// OnCertificateReceived is called when a certificate is received from another validator.
	OnCertificateReceived func(CertificateReceivedEvent[H])

	// OnHeaderTimeout is called when a header times out waiting for votes.
	OnHeaderTimeout func(HeaderTimeoutEvent[H])

	// DAG events

	// OnVertexInserted is called when a certificate is inserted into the DAG.
	OnVertexInserted func(VertexInsertedEvent[H])

	// OnRoundAdvanced is called when the DAG advances to a new round.
	OnRoundAdvanced func(RoundAdvancedEvent)

	// OnEquivocationDetected is called when equivocation is detected.
	// Note: This is in addition to the DAG's OnEquivocation callback,
	// which provides the full evidence for slashing.
	OnEquivocationDetected func(EquivocationDetectedEvent[H])

	// OnCertificatePending is called when a certificate is queued as pending
	// due to missing parents.
	OnCertificatePending func(CertificatePendingEvent[H])

	// Fetcher events

	// OnFetchStarted is called when a fetch operation begins.
	OnFetchStarted func(FetchStartedEvent[H])

	// OnFetchCompleted is called when a fetch operation completes (success or failure).
	OnFetchCompleted func(FetchCompletedEvent[H])

	// GC events

	// OnGarbageCollected is called when garbage collection runs.
	OnGarbageCollected func(GarbageCollectedEvent)
}

// Clone returns a shallow copy of the hooks.
func (h *Hooks[H, T]) Clone() *Hooks[H, T] {
	if h == nil {
		return nil
	}
	clone := *h
	return &clone
}

// BatchCreatedEvent contains information about a newly created batch.
type BatchCreatedEvent[H Hash, T Transaction[H]] struct {
	Batch            *Batch[H, T]
	WorkerID         uint16
	TransactionCount int
	SizeBytes        int
	CreatedAt        time.Time
}

// TransactionReceivedEvent contains information about a received transaction.
type TransactionReceivedEvent[H Hash] struct {
	TxHash     H
	WorkerID   uint16
	ReceivedAt time.Time
}

// HeaderCreatedEvent contains information about a newly created header.
type HeaderCreatedEvent[H Hash] struct {
	Header     *Header[H]
	BatchCount int
	CreatedAt  time.Time
}

// HeaderReceivedEvent contains information about a received header.
type HeaderReceivedEvent[H Hash] struct {
	Header     *Header[H]
	From       uint16
	ReceivedAt time.Time
}

// VoteReceivedEvent contains information about a received vote.
type VoteReceivedEvent[H Hash] struct {
	HeaderDigest   H
	Voter          uint16
	TotalVotes     int // Current vote count for this header
	QuorumRequired int
	ReceivedAt     time.Time
}

// VoteSentEvent contains information about a vote we sent.
type VoteSentEvent[H Hash] struct {
	HeaderDigest H
	HeaderAuthor uint16
	SentAt       time.Time
}

// CertificateFormedEvent contains information about a newly formed certificate.
type CertificateFormedEvent[H Hash] struct {
	Certificate *Certificate[H]
	SignerCount int
	Latency     time.Duration // Time from header creation to certificate formation
	FormedAt    time.Time
}

// CertificateReceivedEvent contains information about a received certificate.
type CertificateReceivedEvent[H Hash] struct {
	Certificate *Certificate[H]
	From        uint16
	ReceivedAt  time.Time
}

// HeaderTimeoutEvent contains information about a header that timed out.
type HeaderTimeoutEvent[H Hash] struct {
	HeaderDigest  H
	Round         uint64
	RetryCount    int
	WillRetry     bool
	VotesReceived int
	QuorumNeeded  int
	TimeoutAt     time.Time
}

// VertexInsertedEvent contains information about a vertex inserted into the DAG.
type VertexInsertedEvent[H Hash] struct {
	Certificate  *Certificate[H]
	Round        uint64
	Author       uint16
	ParentCount  int
	TotalInRound int // Total certificates in this round after insertion
	InsertedAt   time.Time
}

// RoundAdvancedEvent contains information about a round advancement.
type RoundAdvancedEvent struct {
	OldRound            uint64
	NewRound            uint64
	CertificatesInRound int // Number of certificates that triggered advancement
	AdvancedAt          time.Time
}

// EquivocationDetectedEvent contains information about detected equivocation.
type EquivocationDetectedEvent[H Hash] struct {
	Author       uint16
	Round        uint64
	FirstDigest  H
	SecondDigest H
	DetectedAt   time.Time
}

// CertificatePendingEvent contains information about a certificate queued as pending.
type CertificatePendingEvent[H Hash] struct {
	Certificate    *Certificate[H]
	MissingParents []H
	QueuedAt       time.Time
}

// FetchType indicates what is being fetched.
type FetchType uint8

const (
	FetchTypeBatch FetchType = iota
	FetchTypeCertificate
)

func (t FetchType) String() string {
	switch t {
	case FetchTypeBatch:
		return "batch"
	case FetchTypeCertificate:
		return "certificate"
	default:
		return "unknown"
	}
}

// FetchStartedEvent contains information about a fetch operation starting.
type FetchStartedEvent[H Hash] struct {
	Type          FetchType
	Digest        H
	PreferredPeer uint16
	StartedAt     time.Time
}

// FetchCompletedEvent contains information about a completed fetch operation.
type FetchCompletedEvent[H Hash] struct {
	Type        FetchType
	Digest      H
	Success     bool
	Error       error
	Attempts    int
	FromPeer    uint16 // Peer that provided the data (if successful)
	Latency     time.Duration
	CompletedAt time.Time
}

// GarbageCollectedEvent contains information about a GC cycle.
type GarbageCollectedEvent struct {
	BeforeRound    uint64
	CommittedRound uint64
	CollectedAt    time.Time
}
