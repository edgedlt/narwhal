package narwhal_test

import (
	"testing"
	"time"

	"github.com/edgedlt/narwhal"
	"github.com/edgedlt/narwhal/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidator_ValidateHeader(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	cfg := narwhal.DefaultValidationConfig()
	v := narwhal.NewValidator[testutil.TestHash, *testutil.TestTransaction](cfg, validators, testutil.ComputeHash)

	t.Run("valid header", func(t *testing.T) {
		header := &narwhal.Header[testutil.TestHash]{
			Author:    0,
			Round:     1,
			Epoch:     0,
			Timestamp: uint64(time.Now().UnixMilli()),
			BatchRefs: []testutil.TestHash{testutil.ComputeHash([]byte("batch1"))},
			Parents:   []testutil.TestHash{testutil.ComputeHash([]byte("parent1")), testutil.ComputeHash([]byte("parent2")), testutil.ComputeHash([]byte("parent3"))},
		}
		header.ComputeDigest(testutil.ComputeHash)

		err := v.ValidateHeader(header, 0)
		assert.NoError(t, err)
	})

	t.Run("nil header", func(t *testing.T) {
		err := v.ValidateHeader(nil, 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "nil")
	})

	t.Run("invalid author", func(t *testing.T) {
		header := &narwhal.Header[testutil.TestHash]{
			Author:    99, // Invalid
			Round:     1,
			Timestamp: uint64(time.Now().UnixMilli()),
		}
		header.ComputeDigest(testutil.ComputeHash)

		err := v.ValidateHeader(header, 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid author")
	})

	t.Run("digest mismatch", func(t *testing.T) {
		header := &narwhal.Header[testutil.TestHash]{
			Author:    0,
			Round:     1,
			Timestamp: uint64(time.Now().UnixMilli()),
			Digest:    testutil.ComputeHash([]byte("wrong")), // Wrong digest
		}

		err := v.ValidateHeader(header, 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "digest mismatch")
	})

	t.Run("round too far ahead", func(t *testing.T) {
		header := &narwhal.Header[testutil.TestHash]{
			Author:    0,
			Round:     200, // Way ahead
			Timestamp: uint64(time.Now().UnixMilli()),
		}
		header.ComputeDigest(testutil.ComputeHash)

		err := v.ValidateHeader(header, 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "too far ahead")
	})

	t.Run("timestamp in future", func(t *testing.T) {
		header := &narwhal.Header[testutil.TestHash]{
			Author:    0,
			Round:     1,
			Timestamp: uint64(time.Now().Add(5 * time.Minute).UnixMilli()), // 5 min in future
		}
		header.ComputeDigest(testutil.ComputeHash)

		err := v.ValidateHeader(header, 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "future")
	})

	t.Run("too many batch refs", func(t *testing.T) {
		batchRefs := make([]testutil.TestHash, narwhal.DefaultMaxBatchRefs+1)
		for i := range batchRefs {
			batchRefs[i] = testutil.ComputeHash([]byte{byte(i)})
		}

		header := &narwhal.Header[testutil.TestHash]{
			Author:    0,
			Round:     0, // Round 0 doesn't require parents
			Timestamp: uint64(time.Now().UnixMilli()),
			BatchRefs: batchRefs,
		}
		header.ComputeDigest(testutil.ComputeHash)

		err := v.ValidateHeader(header, 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "too many batch refs")
	})

	t.Run("insufficient parents for non-genesis", func(t *testing.T) {
		header := &narwhal.Header[testutil.TestHash]{
			Author:    0,
			Round:     1,
			Timestamp: uint64(time.Now().UnixMilli()),
			Parents:   []testutil.TestHash{testutil.ComputeHash([]byte("parent1"))}, // Only 1 parent
		}
		header.ComputeDigest(testutil.ComputeHash)

		err := v.ValidateHeader(header, 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "insufficient parents")
	})

	t.Run("duplicate batch refs", func(t *testing.T) {
		dup := testutil.ComputeHash([]byte("batch1"))
		header := &narwhal.Header[testutil.TestHash]{
			Author:    0,
			Round:     0,
			Timestamp: uint64(time.Now().UnixMilli()),
			BatchRefs: []testutil.TestHash{dup, dup}, // Duplicate
		}
		header.ComputeDigest(testutil.ComputeHash)

		err := v.ValidateHeader(header, 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate batch ref")
	})

	t.Run("duplicate parents", func(t *testing.T) {
		dup := testutil.ComputeHash([]byte("parent1"))
		header := &narwhal.Header[testutil.TestHash]{
			Author:    0,
			Round:     1,
			Timestamp: uint64(time.Now().UnixMilli()),
			Parents:   []testutil.TestHash{dup, dup, dup}, // Duplicates
		}
		header.ComputeDigest(testutil.ComputeHash)

		err := v.ValidateHeader(header, 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate parent")
	})
}

func TestValidator_ValidateBatch(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	cfg := narwhal.DefaultValidationConfig()
	v := narwhal.NewValidator[testutil.TestHash, *testutil.TestTransaction](cfg, validators, testutil.ComputeHash)

	t.Run("valid batch", func(t *testing.T) {
		batch := &narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]{
			WorkerID:    0,
			ValidatorID: 0,
			Round:       0,
			Transactions: []*testutil.TestTransaction{
				testutil.NewTestTransaction([]byte("tx1")),
			},
		}
		batch.ComputeDigest(testutil.ComputeHash)

		err := v.ValidateBatch(batch, false)
		assert.NoError(t, err)
	})

	t.Run("nil batch", func(t *testing.T) {
		err := v.ValidateBatch(nil, false)
		assert.Error(t, err)
	})

	t.Run("invalid validator", func(t *testing.T) {
		batch := &narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]{
			WorkerID:    0,
			ValidatorID: 99, // Invalid
			Round:       0,
		}
		batch.ComputeDigest(testutil.ComputeHash)

		err := v.ValidateBatch(batch, false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid validator")
	})

	t.Run("digest mismatch", func(t *testing.T) {
		batch := &narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]{
			WorkerID:    0,
			ValidatorID: 0,
			Round:       0,
			Digest:      testutil.ComputeHash([]byte("wrong")),
		}

		err := v.ValidateBatch(batch, false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "verification failed")
	})

	t.Run("too many transactions", func(t *testing.T) {
		// Use a smaller limit for this test
		smallCfg := cfg
		smallCfg.MaxTransactionsPerBatch = 5
		smallV := narwhal.NewValidator[testutil.TestHash, *testutil.TestTransaction](smallCfg, validators, testutil.ComputeHash)

		txs := make([]*testutil.TestTransaction, 10)
		for i := range txs {
			txs[i] = testutil.NewTestTransaction([]byte{byte(i)})
		}

		batch := &narwhal.Batch[testutil.TestHash, *testutil.TestTransaction]{
			WorkerID:     0,
			ValidatorID:  0,
			Round:        0,
			Transactions: txs,
		}
		batch.ComputeDigest(testutil.ComputeHash)

		err := smallV.ValidateBatch(batch, false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "too many transactions")
	})
}

func TestValidator_ValidateVote(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	cfg := narwhal.DefaultValidationConfig()
	v := narwhal.NewValidator[testutil.TestHash, *testutil.TestTransaction](cfg, validators, testutil.ComputeHash)

	headerDigest := testutil.ComputeHash([]byte("header"))

	t.Run("valid vote", func(t *testing.T) {
		sig, err := validators.GetSigner(0).Sign(headerDigest.Bytes())
		require.NoError(t, err)

		vote := &narwhal.HeaderVote[testutil.TestHash]{
			HeaderDigest:   headerDigest,
			ValidatorIndex: 0,
			Signature:      sig,
		}

		err = v.ValidateVote(vote, true)
		assert.NoError(t, err)
	})

	t.Run("nil vote", func(t *testing.T) {
		err := v.ValidateVote(nil, false)
		assert.Error(t, err)
	})

	t.Run("invalid voter", func(t *testing.T) {
		vote := &narwhal.HeaderVote[testutil.TestHash]{
			HeaderDigest:   headerDigest,
			ValidatorIndex: 99, // Invalid
			Signature:      []byte("sig"),
		}

		err := v.ValidateVote(vote, false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid voter")
	})

	t.Run("invalid signature", func(t *testing.T) {
		vote := &narwhal.HeaderVote[testutil.TestHash]{
			HeaderDigest:   headerDigest,
			ValidatorIndex: 0,
			Signature:      []byte("wrong signature"),
		}

		err := v.ValidateVote(vote, true)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid vote signature")
	})
}

func TestValidator_ValidateCertificate(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	cfg := narwhal.DefaultValidationConfig()
	v := narwhal.NewValidator[testutil.TestHash, *testutil.TestTransaction](cfg, validators, testutil.ComputeHash)

	t.Run("valid certificate", func(t *testing.T) {
		header := &narwhal.Header[testutil.TestHash]{
			Author:    0,
			Round:     0,
			Timestamp: uint64(time.Now().UnixMilli()),
		}
		header.ComputeDigest(testutil.ComputeHash)

		// Collect 3 votes (2f+1 = 3 for n=4)
		votes := make(map[uint16][]byte)
		for i := 0; i < 3; i++ {
			sig, err := validators.GetSigner(uint16(i)).Sign(header.Digest.Bytes())
			require.NoError(t, err)
			votes[uint16(i)] = sig
		}

		cert := narwhal.NewCertificate(header, votes)
		err := v.ValidateCertificate(cert, 0, true)
		assert.NoError(t, err)
	})

	t.Run("nil certificate", func(t *testing.T) {
		err := v.ValidateCertificate(nil, 0, false)
		assert.Error(t, err)
	})

	t.Run("nil header", func(t *testing.T) {
		cert := &narwhal.Certificate[testutil.TestHash]{Header: nil}
		err := v.ValidateCertificate(cert, 0, false)
		assert.Error(t, err)
	})

	t.Run("insufficient signatures", func(t *testing.T) {
		header := &narwhal.Header[testutil.TestHash]{
			Author:    0,
			Round:     0,
			Timestamp: uint64(time.Now().UnixMilli()),
		}
		header.ComputeDigest(testutil.ComputeHash)

		// Only 2 votes (need 3)
		votes := make(map[uint16][]byte)
		for i := 0; i < 2; i++ {
			sig, _ := validators.GetSigner(uint16(i)).Sign(header.Digest.Bytes())
			votes[uint16(i)] = sig
		}

		cert := narwhal.NewCertificate(header, votes)
		err := v.ValidateCertificate(cert, 0, false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "insufficient signatures")
	})
}

func TestValidator_ValidateMessageSize(t *testing.T) {
	validators := testutil.NewTestValidatorSet(4)
	cfg := narwhal.DefaultValidationConfig()
	cfg.MaxBatchSize = 100 // Small limit for testing
	v := narwhal.NewValidator[testutil.TestHash, *testutil.TestTransaction](cfg, validators, testutil.ComputeHash)

	t.Run("within limit", func(t *testing.T) {
		err := v.ValidateMessageSize(narwhal.MessageBatch, make([]byte, 50))
		assert.NoError(t, err)
	})

	t.Run("exceeds limit", func(t *testing.T) {
		err := v.ValidateMessageSize(narwhal.MessageBatch, make([]byte, 200))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "too large")
	})
}

func TestDefaultValidationConfig(t *testing.T) {
	cfg := narwhal.DefaultValidationConfig()

	assert.Equal(t, narwhal.DefaultMaxBatchRefs, cfg.MaxBatchRefs)
	assert.Equal(t, narwhal.DefaultMaxParents, cfg.MaxParents)
	assert.Equal(t, narwhal.DefaultMaxSignatures, cfg.MaxSignatures)
	assert.Equal(t, narwhal.DefaultMaxTransactionsPerBatch, cfg.MaxTransactionsPerBatch)
	assert.Equal(t, narwhal.DefaultMaxTransactionSize, cfg.MaxTransactionSize)
	assert.Equal(t, narwhal.DefaultMaxBatchSize, cfg.MaxBatchSize)
	assert.Equal(t, narwhal.DefaultMaxHeaderSize, cfg.MaxHeaderSize)
	assert.Equal(t, narwhal.DefaultMaxCertificateSize, cfg.MaxCertificateSize)
	assert.Equal(t, uint64(narwhal.DefaultMaxRoundSkip), cfg.MaxRoundSkip)
	assert.Equal(t, narwhal.DefaultMaxTimestampDrift, cfg.MaxTimestampDrift)
}

func TestValidationError(t *testing.T) {
	err := &narwhal.ValidationError{
		Type:    "header",
		Field:   "round",
		Message: "too far ahead",
	}

	assert.Contains(t, err.Error(), "header")
	assert.Contains(t, err.Error(), "round")
	assert.Contains(t, err.Error(), "too far ahead")
	assert.True(t, narwhal.IsValidationError(err))
	assert.False(t, narwhal.IsValidationError(nil))
}
