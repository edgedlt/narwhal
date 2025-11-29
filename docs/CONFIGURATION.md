# Narwhal Integration Guide

This guide shows how to integrate the Narwhal DAG-based mempool with your blockchain.

## Overview

Narwhal separates data availability from ordering:
- **Workers** batch transactions and broadcast to all validators
- **Primary** creates headers referencing batches
- **Validators** vote on headers after verifying data availability
- **Certificates** form with 2f+1 votes, proving data availability
- **Consensus** orders certificates; Narwhal handles dissemination

## Quick Start

```go
package main

import (
    "github.com/edgedlt/narwhal"
    "github.com/edgedlt/narwhal/timer"
    "go.uber.org/zap"
)

func main() {
    // 1. Create implementations for your chain
    validators := NewMyValidatorSet(...)
    storage := NewMyStorage(...)
    network := NewMyNetwork(...)
    signer := NewMySigner(privateKey)

    // 2. Configure Narwhal
    cfg, err := narwhal.NewConfig[MyHash, *MyTransaction](
        narwhal.WithMyIndex[MyHash, *MyTransaction](0),
        narwhal.WithValidators[MyHash, *MyTransaction](validators),
        narwhal.WithSigner[MyHash, *MyTransaction](signer),
        narwhal.WithStorage[MyHash, *MyTransaction](storage),
        narwhal.WithNetwork[MyHash, *MyTransaction](network),
        narwhal.WithTimer[MyHash, *MyTransaction](timer.NewRealTimer()),
        narwhal.WithLogger[MyHash, *MyTransaction](zap.Must(zap.NewProduction())),
    )
    if err != nil {
        panic(err)
    }

    // 3. Create and start Narwhal
    nw, err := narwhal.New(cfg, computeHash)
    if err != nil {
        panic(err)
    }

    if err := nw.Start(); err != nil {
        panic(err)
    }
    defer nw.Stop()

    // 4. Submit transactions
    nw.AddTransaction(tx)

    // 5. Integration with consensus (e.g., HotStuff-2)
    certs := nw.GetCertifiedVertices()  // Include in block proposal
    nw.MarkCommitted(certs)             // After block commits
    txs, _ := nw.GetTransactions(certs) // For execution
}
```

## Interfaces to Implement

### 1. Hash

Your hash type for content addressing.

```go
type Hash interface {
    Bytes() []byte
    Equals(other Hash) bool
    String() string
}

// Example implementation
type MyHash [32]byte

func (h MyHash) Bytes() []byte { return h[:] }

func (h MyHash) Equals(other narwhal.Hash) bool {
    if o, ok := other.(MyHash); ok {
        return h == o
    }
    return false
}

func (h MyHash) String() string {
    return hex.EncodeToString(h[:])
}
```

### 2. Transaction

Your transaction type to be disseminated.

```go
type Transaction[H Hash] interface {
    Hash() H
    Bytes() []byte
}

// Example implementation
type MyTransaction struct {
    hash    MyHash
    payload []byte
}

func (tx *MyTransaction) Hash() MyHash   { return tx.hash }
func (tx *MyTransaction) Bytes() []byte  { return tx.payload }
```

### 3. ValidatorSet

Manages the set of validators.

```go
type ValidatorSet interface {
    Count() int
    GetByIndex(index uint16) (PublicKey, error)
    Contains(index uint16) bool
    F() int  // max Byzantine faults: (n-1)/3
}

// Example implementation
type MyValidatorSet struct {
    validators []PublicKey
}

func (vs *MyValidatorSet) Count() int {
    return len(vs.validators)
}

func (vs *MyValidatorSet) GetByIndex(index uint16) (narwhal.PublicKey, error) {
    if int(index) >= len(vs.validators) {
        return nil, fmt.Errorf("invalid index: %d", index)
    }
    return vs.validators[index], nil
}

func (vs *MyValidatorSet) Contains(index uint16) bool {
    return int(index) < len(vs.validators)
}

func (vs *MyValidatorSet) F() int {
    return (len(vs.validators) - 1) / 3
}
```

### 4. Storage

Persistent storage for batches, headers, and certificates.

**CRITICAL**: All `Put` operations must be durable before returning. Use sync writes.

```go
type Storage[H Hash, T Transaction[H]] interface {
    // Batch operations
    GetBatch(digest H) (*Batch[H, T], error)
    PutBatch(batch *Batch[H, T]) error
    HasBatch(digest H) bool

    // Header operations
    GetHeader(digest H) (*Header[H], error)
    PutHeader(header *Header[H]) error

    // Certificate operations
    GetCertificate(digest H) (*Certificate[H], error)
    PutCertificate(cert *Certificate[H]) error

    // Round tracking
    GetHighestRound() (uint64, error)
    PutHighestRound(round uint64) error

    // Garbage collection
    DeleteBeforeRound(round uint64) error

    // Bulk retrieval
    GetCertificatesByRound(round uint64) ([]*Certificate[H], error)
    GetCertificatesInRange(startRound, endRound uint64) ([]*Certificate[H], error)

    Close() error
}
```

#### Example: BadgerDB Storage

```go
import "github.com/dgraph-io/badger/v4"

type BadgerStorage struct {
    db *badger.DB
}

func NewBadgerStorage(path string) (*BadgerStorage, error) {
    opts := badger.DefaultOptions(path)
    opts.SyncWrites = true  // CRITICAL: Ensure durability
    
    db, err := badger.Open(opts)
    if err != nil {
        return nil, err
    }
    return &BadgerStorage{db: db}, nil
}

func (s *BadgerStorage) PutBatch(batch *Batch[MyHash, *MyTransaction]) error {
    data := batch.Bytes()
    key := append([]byte("batch:"), batch.Digest.Bytes()...)
    return s.db.Update(func(txn *badger.Txn) error {
        return txn.Set(key, data)
    })
}

func (s *BadgerStorage) GetBatch(digest MyHash) (*Batch[MyHash, *MyTransaction], error) {
    var data []byte
    err := s.db.View(func(txn *badger.Txn) error {
        key := append([]byte("batch:"), digest.Bytes()...)
        item, err := txn.Get(key)
        if err != nil {
            return err
        }
        data, err = item.ValueCopy(nil)
        return err
    })
    if err != nil {
        return nil, err
    }
    return BatchFromBytes(data)
}
```

**Storage requirements**:
- Thread-safe (concurrent access from workers, primary, GC)
- Durable writes (fsync before return)
- Efficient range deletion for GC

**Recommended backends**: BadgerDB, BoltDB, LevelDB, RocksDB

### 5. Network

Message broadcasting and point-to-point delivery.

```go
type Network[H Hash, T Transaction[H]] interface {
    // Broadcasting
    BroadcastBatch(batch *Batch[H, T])
    BroadcastHeader(header *Header[H])
    BroadcastCertificate(cert *Certificate[H])

    // Point-to-point
    SendVote(validatorIndex uint16, vote *HeaderVote[H])
    SendBatch(to uint16, batch *Batch[H, T])
    SendCertificate(to uint16, cert *Certificate[H])

    // Fetching (blocking with timeout)
    FetchBatch(from uint16, digest H) (*Batch[H, T], error)
    FetchCertificate(from uint16, digest H) (*Certificate[H], error)
    FetchCertificatesInRange(from uint16, startRound, endRound uint64) ([]*Certificate[H], error)

    // Message receiving
    Receive() <-chan Message[H, T]

    Close() error
}
```

#### Example: gRPC Network

```go
type GRPCNetwork struct {
    myIndex  uint16
    clients  map[uint16]pb.NarwhalClient
    incoming chan Message[MyHash, *MyTransaction]
}

func NewGRPCNetwork(myIndex uint16, peers map[uint16]string) (*GRPCNetwork, error) {
    n := &GRPCNetwork{
        myIndex:  myIndex,
        clients:  make(map[uint16]pb.NarwhalClient),
        incoming: make(chan Message[MyHash, *MyTransaction], 10000),
    }
    
    for idx, addr := range peers {
        conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
        if err != nil {
            return nil, err
        }
        n.clients[idx] = pb.NewNarwhalClient(conn)
    }
    
    return n, nil
}

func (n *GRPCNetwork) BroadcastBatch(batch *Batch[MyHash, *MyTransaction]) {
    proto := batchToProto(batch)
    for _, client := range n.clients {
        go client.SendBatch(context.Background(), proto)
    }
}

func (n *GRPCNetwork) Receive() <-chan Message[MyHash, *MyTransaction] {
    return n.incoming
}
```

**Network requirements**:
- Buffered receive channel (10000+ recommended)
- Timeouts on fetch operations (5-30 seconds)
- Consider message prioritization: votes > certificates > headers > batches

### 6. Signer

Cryptographic signing for votes.

```go
type Signer interface {
    Sign(message []byte) ([]byte, error)
    PublicKey() PublicKey
}

type PublicKey interface {
    Bytes() []byte
    Verify(message []byte, signature []byte) bool
    Equals(other interface{ Bytes() []byte }) bool
}

// Example with Ed25519
type Ed25519Signer struct {
    privateKey ed25519.PrivateKey
}

func (s *Ed25519Signer) Sign(message []byte) ([]byte, error) {
    return ed25519.Sign(s.privateKey, message), nil
}

func (s *Ed25519Signer) PublicKey() narwhal.PublicKey {
    return &Ed25519PublicKey{s.privateKey.Public().(ed25519.PublicKey)}
}
```

### 7. Timer

Use the provided implementations:

```go
import "github.com/edgedlt/narwhal/timer"

// Production
t := timer.NewRealTimer()

// Testing (manually controlled)
t := timer.NewMockTimer()
t.Fire() // Trigger timeout
```

## Configuration Options

### Required Options

| Option | Description |
|--------|-------------|
| `WithMyIndex` | Validator index in the set |
| `WithValidators` | ValidatorSet implementation |
| `WithSigner` | Signer implementation |
| `WithStorage` | Storage implementation |
| `WithNetwork` | Network implementation |
| `WithTimer` | Timer implementation |

### Worker Settings

| Option | Default | Description |
|--------|---------|-------------|
| `WithWorkerCount` | 4 | Parallel workers for batching |
| `WithBatchSize` | 500 | Max transactions per batch |
| `WithBatchTimeout` | 100ms | Max wait before creating batch |

### Primary Settings

| Option | Default | Description |
|--------|---------|-------------|
| `WithHeaderTimeout` | 500ms | Interval between header proposals |
| `WithMaxHeaderBatches` | 100 | Max batch refs per header |

### DAG Settings

| Option | Default | Description |
|--------|---------|-------------|
| `WithGCInterval` | 50 | Rounds between garbage collection |
| `WithGCDepth` | 100 | Rounds to retain before GC |
| `WithMaxRoundsGap` | 10 | Max rounds ahead to accept |

### Backpressure

| Option | Default | Description |
|--------|---------|-------------|
| `WithMaxPendingTransactions` | 10000 | Max queued transactions per worker |
| `WithMaxPendingBatches` | 1000 | Max queued batches |
| `WithDropOnFull` | false | Drop vs block when full |

### Observability

| Option | Default | Description |
|--------|---------|-------------|
| `WithLogger` | no-op | Zap logger |
| `WithHooks` | nil | Event callbacks |

## Consensus Integration

### With HotStuff-2

Narwhal integrates naturally with HotStuff-2 through the Executor interface:

```go
type NarwhalExecutor struct {
    narwhal *narwhal.Narwhal[MyHash, *MyTransaction]
    state   *StateDB
}

func (e *NarwhalExecutor) CreateBlock(height uint32, prevHash MyHash, proposer uint16) (Block, error) {
    // Get uncommitted certificates from Narwhal DAG
    certs := e.narwhal.GetCertifiedVertices()
    
    // Serialize certificate digests as block payload
    payload := serializeCertRefs(certs)
    
    return NewBlock(height, prevHash, payload, proposer), nil
}

func (e *NarwhalExecutor) Execute(block Block) (MyHash, error) {
    // Deserialize certificate references
    certRefs := deserializeCertRefs(block.Payload())
    
    // Mark certificates as committed
    e.narwhal.MarkCommitted(certRefs)
    
    // Extract and execute transactions
    txs, _ := e.narwhal.GetTransactions(certRefs)
    for _, tx := range txs {
        e.state.Apply(tx)
    }
    
    return e.state.Hash(), nil
}

func (e *NarwhalExecutor) Verify(block Block) error {
    certRefs := deserializeCertRefs(block.Payload())
    
    // Verify all certificates exist in DAG
    for _, ref := range certRefs {
        if _, err := e.narwhal.GetCertificate(ref); err != nil {
            return fmt.Errorf("missing certificate: %s", ref)
        }
    }
    return nil
}
```

## Metrics and Monitoring

### Prometheus Metrics

Set up Prometheus metrics using event hooks:

```go
import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
)

var (
    // Counters
    batchesCreated = promauto.NewCounter(prometheus.CounterOpts{
        Name: "narwhal_batches_created_total",
        Help: "Total batches created",
    })
    certificatesFormed = promauto.NewCounter(prometheus.CounterOpts{
        Name: "narwhal_certificates_formed_total",
        Help: "Total certificates formed",
    })
    transactionsReceived = promauto.NewCounter(prometheus.CounterOpts{
        Name: "narwhal_transactions_received_total",
        Help: "Total transactions received",
    })
    votesSent = promauto.NewCounter(prometheus.CounterOpts{
        Name: "narwhal_votes_sent_total",
        Help: "Total votes sent",
    })
    votesReceived = promauto.NewCounter(prometheus.CounterOpts{
        Name: "narwhal_votes_received_total",
        Help: "Total votes received",
    })
    headerTimeouts = promauto.NewCounter(prometheus.CounterOpts{
        Name: "narwhal_header_timeouts_total",
        Help: "Total header timeouts",
    })
    fetchesCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "narwhal_fetches_completed_total",
        Help: "Total fetch operations completed",
    }, []string{"type", "success"})
    equivocationsDetected = promauto.NewCounter(prometheus.CounterOpts{
        Name: "narwhal_equivocations_detected_total",
        Help: "Total equivocations detected",
    })

    // Gauges
    currentRound = promauto.NewGauge(prometheus.GaugeOpts{
        Name: "narwhal_current_round",
        Help: "Current DAG round",
    })
    pendingCertificates = promauto.NewGauge(prometheus.GaugeOpts{
        Name: "narwhal_pending_certificates",
        Help: "Certificates waiting for parents",
    })

    // Histograms
    certificateLatency = promauto.NewHistogram(prometheus.HistogramOpts{
        Name:    "narwhal_certificate_latency_seconds",
        Help:    "Time from header creation to certificate formation",
        Buckets: prometheus.ExponentialBuckets(0.01, 2, 10), // 10ms to 10s
    })
    batchSize = promauto.NewHistogram(prometheus.HistogramOpts{
        Name:    "narwhal_batch_size_transactions",
        Help:    "Number of transactions per batch",
        Buckets: prometheus.ExponentialBuckets(1, 2, 12), // 1 to 4096
    })
    fetchLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
        Name:    "narwhal_fetch_latency_seconds",
        Help:    "Fetch operation latency",
        Buckets: prometheus.ExponentialBuckets(0.001, 2, 12), // 1ms to 4s
    }, []string{"type"})
)

func NewMetricsHooks[H narwhal.Hash, T narwhal.Transaction[H]]() *narwhal.Hooks[H, T] {
    return &narwhal.Hooks[H, T]{
        OnBatchCreated: func(e narwhal.BatchCreatedEvent[H, T]) {
            batchesCreated.Inc()
            batchSize.Observe(float64(e.TransactionCount))
        },
        OnTransactionReceived: func(e narwhal.TransactionReceivedEvent[H]) {
            transactionsReceived.Inc()
        },
        OnCertificateFormed: func(e narwhal.CertificateFormedEvent[H]) {
            certificatesFormed.Inc()
            certificateLatency.Observe(e.Latency.Seconds())
        },
        OnRoundAdvanced: func(e narwhal.RoundAdvancedEvent) {
            currentRound.Set(float64(e.NewRound))
        },
        OnVoteSent: func(e narwhal.VoteSentEvent[H]) {
            votesSent.Inc()
        },
        OnVoteReceived: func(e narwhal.VoteReceivedEvent[H]) {
            votesReceived.Inc()
        },
        OnHeaderTimeout: func(e narwhal.HeaderTimeoutEvent[H]) {
            headerTimeouts.Inc()
        },
        OnFetchCompleted: func(e narwhal.FetchCompletedEvent[H]) {
            fetchesCompleted.WithLabelValues(e.Type.String(), fmt.Sprint(e.Success)).Inc()
            if e.Success {
                fetchLatency.WithLabelValues(e.Type.String()).Observe(e.Latency.Seconds())
            }
        },
        OnEquivocationDetected: func(e narwhal.EquivocationDetectedEvent[H]) {
            equivocationsDetected.Inc()
        },
        OnCertificatePending: func(e narwhal.CertificatePendingEvent[H]) {
            pendingCertificates.Inc()
        },
        OnVertexInserted: func(e narwhal.VertexInsertedEvent[H]) {
            pendingCertificates.Dec() // Assuming it was pending
        },
    }
}
```

### Using Metrics Hooks

```go
cfg, _ := narwhal.NewConfig[MyHash, *MyTransaction](
    // ... required options ...
    narwhal.WithHooks[MyHash, *MyTransaction](NewMetricsHooks[MyHash, *MyTransaction]()),
)

// Expose metrics endpoint
http.Handle("/metrics", promhttp.Handler())
go http.ListenAndServe(":9090", nil)
```

### Key Metrics to Monitor

| Metric | Type | Description |
|--------|------|-------------|
| `narwhal_current_round` | Gauge | Current DAG round (should increase steadily) |
| `narwhal_certificates_formed_total` | Counter | Certificate formation rate |
| `narwhal_certificate_latency_seconds` | Histogram | Time to form certificates |
| `narwhal_header_timeouts_total` | Counter | Timeout rate (high = network issues) |
| `narwhal_fetches_completed_total` | Counter | Fetch success/failure rate |
| `narwhal_equivocations_detected_total` | Counter | Byzantine behavior (should be 0) |

### Available Events

| Event | Description |
|-------|-------------|
| `OnBatchCreated` | Worker created a batch (includes size, tx count) |
| `OnTransactionReceived` | Transaction added to worker |
| `OnHeaderCreated` | Primary created a header |
| `OnHeaderReceived` | Received header from peer |
| `OnVoteSent` | Sent vote for a header |
| `OnVoteReceived` | Received vote for our header (includes vote count) |
| `OnCertificateFormed` | Formed certificate (includes latency) |
| `OnCertificateReceived` | Received certificate from peer |
| `OnHeaderTimeout` | Header timed out (includes retry info) |
| `OnVertexInserted` | Certificate inserted into DAG |
| `OnRoundAdvanced` | DAG advanced to new round |
| `OnEquivocationDetected` | Detected conflicting headers |
| `OnCertificatePending` | Certificate queued for missing parents |
| `OnFetchStarted` | Started fetching missing data |
| `OnFetchCompleted` | Fetch completed (includes success, latency) |
| `OnGarbageCollected` | GC cycle completed |

## Performance Tuning

### High Throughput

```go
cfg, _ := narwhal.NewConfig[MyHash, *MyTransaction](
    // ... required options ...
    narwhal.WithWorkerCount[MyHash, *MyTransaction](8),
    narwhal.WithBatchSize[MyHash, *MyTransaction](1000),
    narwhal.WithMaxHeaderBatches[MyHash, *MyTransaction](200),
    narwhal.WithDropOnFull[MyHash, *MyTransaction](true),
)
```

### Low Latency

```go
cfg, _ := narwhal.NewConfig[MyHash, *MyTransaction](
    // ... required options ...
    narwhal.WithBatchSize[MyHash, *MyTransaction](100),
    narwhal.WithBatchTimeout[MyHash, *MyTransaction](50*time.Millisecond),
    narwhal.WithHeaderTimeout[MyHash, *MyTransaction](200*time.Millisecond),
)
```

### Memory Constrained

```go
cfg, _ := narwhal.NewConfig[MyHash, *MyTransaction](
    // ... required options ...
    narwhal.WithGCDepth[MyHash, *MyTransaction](20),
    narwhal.WithGCInterval[MyHash, *MyTransaction](50),
    narwhal.WithMaxPendingTransactions[MyHash, *MyTransaction](5000),
    narwhal.WithDropOnFull[MyHash, *MyTransaction](true),
)
```

## Safety Guarantees

Narwhal provides:

1. **Data Availability**: A certificate proves 2f+1 validators have the referenced batches
2. **No Equivocation**: Validators detect and reject conflicting headers from the same round
3. **Garbage Collection Safety**: Only committed data is garbage collected

## References

- [Narwhal and Tusk Paper](https://arxiv.org/abs/2105.11827)
- [HotStuff-2 Library](https://github.com/edgedlt/hotstuff2)
