# Narwhal

A production-ready Go implementation of the [Narwhal](https://arxiv.org/abs/2105.11827) DAG-based mempool protocol.

[![Go Version](https://img.shields.io/badge/Go-1.23+-blue)](https://golang.org/dl/)
[![Tests](https://github.com/edgedlt/narwhal/actions/workflows/test.yml/badge.svg)](https://github.com/edgedlt/narwhal/actions)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

## Features

- **DAG-based mempool** - Parallel transaction dissemination across all validators
- **No leader bottleneck** - All validators broadcast simultaneously
- **Reliable availability** - Certificates guarantee data availability (2f+1 acknowledgments)
- **Consensus-agnostic** - Integrates with any BFT consensus (HotStuff-2, Bullshark, etc.)
- **Generic design** - Supports custom hash and transaction types
- **Reconfiguration** - Epoch-based validator set changes

## Installation

```bash
go get github.com/edgedlt/narwhal
```

**Requirements**: Go 1.23+

## Quick Start

```go
package main

import (
    "github.com/edgedlt/narwhal"
    "github.com/edgedlt/narwhal/timer"
    "go.uber.org/zap"
)

func main() {
    // Configure Narwhal
    cfg, _ := narwhal.NewConfig[MyHash, *MyTransaction](
        narwhal.WithMyIndex[MyHash, *MyTransaction](0),
        narwhal.WithValidators[MyHash, *MyTransaction](validators),
        narwhal.WithSigner[MyHash, *MyTransaction](signer),
        narwhal.WithStorage[MyHash, *MyTransaction](storage),
        narwhal.WithNetwork[MyHash, *MyTransaction](network),
        narwhal.WithTimer[MyHash, *MyTransaction](timer.NewRealTimer()),
        narwhal.WithLogger[MyHash, *MyTransaction](zap.NewProduction()),
    )

    // Create and start
    nw, _ := narwhal.New(cfg, myHashFunc)
    nw.Start()
    defer nw.Stop()

    // Submit transactions
    nw.AddTransaction(tx)

    // Integration with consensus
    certs := nw.GetCertifiedVertices()  // For block proposals
    nw.MarkCommitted(certs)             // After commit
    txs, _ := nw.GetTransactions(certs) // For execution
}
```

## Architecture

```
narwhal/
├── narwhal.go           # Core mempool coordinator
├── config.go            # Configuration with functional options
├── types.go             # Core interfaces (Hash, Transaction, Storage, Network)
├── dag.go               # DAG structure and certificate management
├── proposer.go          # Header proposal logic
├── fetcher.go           # Missing data fetching
├── gc.go                # Garbage collection
├── validation.go        # Header and certificate validation
├── reconfiguration.go   # Epoch transitions
├── hooks.go             # Observability callbacks
└── timer/               # Timer implementations (Real, Mock)
```

## Interfaces

Implement these interfaces to integrate with your blockchain:

```go
// Your hash type
type Hash interface {
    Bytes() []byte
    Equals(other Hash) bool
    String() string
}

// Your transaction type
type Transaction[H Hash] interface {
    Hash() H
    Bytes() []byte
}

// Validator set
type ValidatorSet interface {
    Count() int
    GetByIndex(index uint16) (PublicKey, error)
    Contains(index uint16) bool
    F() int  // max Byzantine faults: (n-1)/3
}

// Persistent storage
type Storage[H Hash, T Transaction[H]] interface {
    GetBatch(digest H) (*Batch[H, T], error)
    PutBatch(batch *Batch[H, T]) error
    GetHeader(digest H) (*Header[H], error)
    PutHeader(header *Header[H]) error
    GetCertificate(digest H) (*Certificate[H], error)
    PutCertificate(cert *Certificate[H]) error
    GetHighestRound() (uint64, error)
    DeleteBeforeRound(round uint64) error
    // ... see types.go for full interface
}

// Network transport
type Network[H Hash, T Transaction[H]] interface {
    BroadcastBatch(batch *Batch[H, T])
    BroadcastHeader(header *Header[H])
    SendVote(validatorIndex uint16, vote *HeaderVote[H])
    BroadcastCertificate(cert *Certificate[H])
    FetchBatch(from uint16, digest H) (*Batch[H, T], error)
    Receive() <-chan Message[H, T]
    // ... see types.go for full interface
}
```

## Protocol Overview

Narwhal separates data availability from ordering:

1. **Workers** batch transactions and broadcast to all validators
2. **Primary** creates headers referencing batches and parent certificates
3. **Validators** vote on headers after verifying data availability
4. **Certificates** form when 2f+1 votes are collected
5. **DAG** stores certified vertices for consensus to order

**Data Availability**: A certificate proves 2f+1 validators have the referenced data.

**Consensus Integration**: Your consensus protocol orders certificates; Narwhal handles dissemination.

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `WithWorkerCount` | 4 | Parallel workers for batching |
| `WithBatchSize` | 500 | Max transactions per batch |
| `WithBatchTimeout` | 100ms | Max wait before creating batch |
| `WithHeaderTimeout` | 500ms | Interval between header proposals |
| `WithGCInterval` | 50 | Rounds between garbage collection |
| `WithMaxRoundsGap` | 10 | Max rounds ahead to accept |

See [Configuration Guide](docs/CONFIGURATION.md) for tuning and implementation guides.

## Testing

```bash
# Run all tests
go test ./...

# Run with race detection
go test -race ./...

# Run benchmarks
go test -bench=. -benchmem
```

## References

- [Narwhal and Tusk Paper](https://arxiv.org/abs/2105.11827) - Protocol specification
- [HotStuff-2 Library](https://github.com/edgedlt/hotstuff2) - Compatible consensus implementation

## License

Apache 2.0 - see [LICENSE](LICENSE) for details.
