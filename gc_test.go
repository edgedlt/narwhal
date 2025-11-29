package narwhal_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edgedlt/narwhal"
	"github.com/edgedlt/narwhal/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestGarbageCollector_ForceGC(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	dag := narwhal.NewDAG[testutil.TestHash, *testutil.TestTransaction](validators, zap.NewNop())
	storage := testutil.NewTestStorage()

	cfg := narwhal.DefaultGCConfig()
	cfg.RetainRounds = 10

	gc := narwhal.NewGarbageCollector(cfg, dag, storage, zap.NewNop())

	// Insert certificates for rounds 0-50
	for round := uint64(0); round <= 50; round++ {
		for author := uint16(0); author < 3; author++ {
			header := &narwhal.Header[testutil.TestHash]{
				Author:    author,
				Round:     round,
				Timestamp: uint64(time.Now().UnixMilli()) + round*1000 + uint64(author),
			}
			if round > 0 {
				header.Parents = dag.GetParents()
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
	}

	// Verify DAG has all rounds
	assert.Equal(t, uint64(51), dag.CurrentRound())
	assert.Equal(t, 3, dag.CertificateCountForRound(0))
	assert.Equal(t, 3, dag.CertificateCountForRound(30))

	// Set committed round and force GC
	gc.SetCommittedRound(40)
	gc.ForceGC()

	// After GC, rounds 0-29 should be cleaned (40 - 10 retainRounds = 30)
	// Note: GC cleans rounds < gcBeforeRound
	stats := gc.Stats()
	assert.Equal(t, uint64(30), stats.LastGCRound)

	// Check DAG state after GC
	dagStats := dag.Stats()
	t.Logf("DAG after GC: current=%d, gc=%d, vertices=%d",
		dagStats.CurrentRound, dagStats.GCRound, dagStats.TotalVertices)

	// Rounds before GC should be cleaned
	assert.Equal(t, 0, dag.CertificateCountForRound(0))
	assert.Equal(t, 0, dag.CertificateCountForRound(29))

	// Rounds after GC should remain
	assert.Equal(t, 3, dag.CertificateCountForRound(30))
	assert.Equal(t, 3, dag.CertificateCountForRound(50))
}

func TestGarbageCollector_PeriodicGC(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	dag := narwhal.NewDAG[testutil.TestHash, *testutil.TestTransaction](validators, zap.NewNop())
	storage := testutil.NewTestStorage()

	cfg := narwhal.GCConfig{
		Interval:      10,
		RetainRounds:  5,
		CheckInterval: 50 * time.Millisecond,
	}

	gc := narwhal.NewGarbageCollector(cfg, dag, storage, zap.NewNop())

	// Track GC calls (use atomic for thread safety)
	var gcCalls atomic.Int64
	gc.OnGC(func(beforeRound uint64) {
		gcCalls.Add(1)
	})

	// Insert initial certificates
	for round := uint64(0); round <= 20; round++ {
		for author := uint16(0); author < 3; author++ {
			header := &narwhal.Header[testutil.TestHash]{
				Author:    author,
				Round:     round,
				Timestamp: uint64(time.Now().UnixMilli()) + round*1000 + uint64(author),
			}
			if round > 0 {
				header.Parents = dag.GetParents()
			}
			header.ComputeDigest(testutil.ComputeHash)

			votes := make(map[uint16][]byte)
			for i := uint16(0); i < 3; i++ {
				signer := validators.GetSigner(i)
				sig, _ := signer.Sign(header.Digest.Bytes())
				votes[i] = sig
			}
			cert := narwhal.NewCertificate(header, votes)
			_ = dag.InsertCertificate(cert, nil)
		}
	}

	// Set committed round high enough to trigger GC
	gc.SetCommittedRound(20)

	// Run GC in background
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go gc.Run(ctx)

	// Wait for periodic GC to trigger
	time.Sleep(200 * time.Millisecond)

	// GC should have run at least once
	assert.GreaterOrEqual(t, gcCalls.Load(), int64(1), "GC should have been triggered")
}

func TestGarbageCollector_NoGCWhenNotEnoughRounds(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	dag := narwhal.NewDAG[testutil.TestHash, *testutil.TestTransaction](validators, zap.NewNop())
	storage := testutil.NewTestStorage()

	cfg := narwhal.DefaultGCConfig()
	cfg.RetainRounds = 100 // High retain rounds

	gc := narwhal.NewGarbageCollector(cfg, dag, storage, zap.NewNop())

	// Track GC calls
	var gcCalls int
	gc.OnGC(func(beforeRound uint64) {
		gcCalls++
	})

	// Insert only a few certificates
	for round := uint64(0); round <= 10; round++ {
		for author := uint16(0); author < 3; author++ {
			header := &narwhal.Header[testutil.TestHash]{
				Author:    author,
				Round:     round,
				Timestamp: uint64(time.Now().UnixMilli()) + round*1000 + uint64(author),
			}
			if round > 0 {
				header.Parents = dag.GetParents()
			}
			header.ComputeDigest(testutil.ComputeHash)

			votes := make(map[uint16][]byte)
			for i := uint16(0); i < 3; i++ {
				signer := validators.GetSigner(i)
				sig, _ := signer.Sign(header.Digest.Bytes())
				votes[i] = sig
			}
			cert := narwhal.NewCertificate(header, votes)
			_ = dag.InsertCertificate(cert, nil)
		}
	}

	// Committed round is low, so GC shouldn't clean anything
	gc.SetCommittedRound(10)
	gc.ForceGC()

	assert.Equal(t, 0, gcCalls, "GC should not run when nothing to clean")

	// All rounds should still exist
	assert.Equal(t, 3, dag.CertificateCountForRound(0))
	assert.Equal(t, 3, dag.CertificateCountForRound(10))
}

func TestGarbageCollector_Stats(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	dag := narwhal.NewDAG[testutil.TestHash, *testutil.TestTransaction](validators, zap.NewNop())
	storage := testutil.NewTestStorage()

	gc := narwhal.NewGarbageCollector(narwhal.DefaultGCConfig(), dag, storage, zap.NewNop())

	// Initial stats
	stats := gc.Stats()
	assert.Equal(t, uint64(0), stats.LastGCRound)
	assert.Equal(t, uint64(0), stats.CommittedRound)

	// Update committed round
	gc.SetCommittedRound(100)
	stats = gc.Stats()
	assert.Equal(t, uint64(100), stats.CommittedRound)
}
