package narwhal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// mockHash implements Hash for testing
type mockHash [32]byte

func (h mockHash) Bytes() []byte { return h[:] }
func (h mockHash) Equals(other Hash) bool {
	if o, ok := other.(mockHash); ok {
		return h == o
	}
	return false
}
func (h mockHash) String() string { return "mock" }

// mockTransaction implements Transaction for testing
type mockTransaction struct {
	hash mockHash
	data []byte
}

func (t *mockTransaction) Hash() mockHash { return t.hash }
func (t *mockTransaction) Bytes() []byte  { return t.data }

// mockPublicKey implements PublicKey for testing
type mockPublicKey struct{}

func (k mockPublicKey) Bytes() []byte                                 { return []byte("pub") }
func (k mockPublicKey) Verify(msg, sig []byte) bool                   { return true }
func (k mockPublicKey) Equals(other interface{ Bytes() []byte }) bool { return true }

// mockSigner implements Signer for testing
type mockSigner struct{}

func (s mockSigner) Sign(msg []byte) ([]byte, error) { return []byte("sig"), nil }
func (s mockSigner) PublicKey() PublicKey            { return mockPublicKey{} }

// mockValidatorSet implements ValidatorSet for testing
type mockValidatorSet struct {
	count int
}

func (v mockValidatorSet) Count() int                               { return v.count }
func (v mockValidatorSet) GetByIndex(idx uint16) (PublicKey, error) { return mockPublicKey{}, nil }
func (v mockValidatorSet) Contains(idx uint16) bool                 { return int(idx) < v.count }
func (v mockValidatorSet) F() int                                   { return (v.count - 1) / 3 }

// mockStorage implements Storage for testing
type mockStorage struct{}

func (s mockStorage) GetBatch(digest mockHash) (*Batch[mockHash, *mockTransaction], error) {
	return nil, nil
}
func (s mockStorage) PutBatch(batch *Batch[mockHash, *mockTransaction]) error        { return nil }
func (s mockStorage) HasBatch(digest mockHash) bool                                  { return false }
func (s mockStorage) GetHeader(digest mockHash) (*Header[mockHash], error)           { return nil, nil }
func (s mockStorage) PutHeader(header *Header[mockHash]) error                       { return nil }
func (s mockStorage) GetCertificate(digest mockHash) (*Certificate[mockHash], error) { return nil, nil }
func (s mockStorage) PutCertificate(cert *Certificate[mockHash]) error               { return nil }
func (s mockStorage) GetHighestRound() (uint64, error)                               { return 0, nil }
func (s mockStorage) PutHighestRound(round uint64) error                             { return nil }
func (s mockStorage) DeleteBeforeRound(round uint64) error                           { return nil }
func (s mockStorage) GetCertificatesByRound(round uint64) ([]*Certificate[mockHash], error) {
	return nil, nil
}
func (s mockStorage) GetCertificatesInRange(start, end uint64) ([]*Certificate[mockHash], error) {
	return nil, nil
}
func (s mockStorage) Close() error { return nil }

// mockNetwork implements Network for testing
type mockNetwork struct{}

func (n mockNetwork) BroadcastBatch(batch *Batch[mockHash, *mockTransaction])    {}
func (n mockNetwork) BroadcastHeader(header *Header[mockHash])                   {}
func (n mockNetwork) SendVote(validatorIndex uint16, vote *HeaderVote[mockHash]) {}
func (n mockNetwork) BroadcastCertificate(cert *Certificate[mockHash])           {}
func (n mockNetwork) FetchBatch(from uint16, digest mockHash) (*Batch[mockHash, *mockTransaction], error) {
	return nil, nil
}
func (n mockNetwork) FetchCertificate(from uint16, digest mockHash) (*Certificate[mockHash], error) {
	return nil, nil
}
func (n mockNetwork) FetchCertificatesInRange(from uint16, startRound, endRound uint64) ([]*Certificate[mockHash], error) {
	return nil, nil
}
func (n mockNetwork) SendBatch(to uint16, batch *Batch[mockHash, *mockTransaction]) {}
func (n mockNetwork) SendCertificate(to uint16, cert *Certificate[mockHash])        {}
func (n mockNetwork) Receive() <-chan Message[mockHash, *mockTransaction]           { return nil }
func (n mockNetwork) Close() error                                                  { return nil }

// mockTimer implements Timer for testing
type mockTimer struct{}

func (t mockTimer) Start()             {}
func (t mockTimer) Stop()              {}
func (t mockTimer) Reset()             {}
func (t mockTimer) C() <-chan struct{} { return nil }

func TestNewConfig_WithAllOptions(t *testing.T) {
	cfg, err := NewConfig[mockHash, *mockTransaction](
		WithMyIndex[mockHash, *mockTransaction](0),
		WithValidators[mockHash, *mockTransaction](mockValidatorSet{count: 4}),
		WithSigner[mockHash, *mockTransaction](mockSigner{}),
		WithStorage[mockHash, *mockTransaction](mockStorage{}),
		WithNetwork[mockHash, *mockTransaction](mockNetwork{}),
		WithTimer[mockHash, *mockTransaction](mockTimer{}),
		WithLogger[mockHash, *mockTransaction](zap.NewNop()),
		WithWorkerCount[mockHash, *mockTransaction](8),
		WithBatchSize[mockHash, *mockTransaction](1000),
		WithBatchTimeout[mockHash, *mockTransaction](50*time.Millisecond),
		WithHeaderTimeout[mockHash, *mockTransaction](200*time.Millisecond),
		WithGCInterval[mockHash, *mockTransaction](50),
		WithMaxRoundsGap[mockHash, *mockTransaction](5),
	)

	require.NoError(t, err)
	assert.Equal(t, uint16(0), cfg.MyIndex)
	assert.Equal(t, 8, cfg.WorkerCount)
	assert.Equal(t, 1000, cfg.BatchSize)
	assert.Equal(t, 50*time.Millisecond, cfg.BatchTimeout)
	assert.Equal(t, 200*time.Millisecond, cfg.HeaderTimeout)
	assert.Equal(t, uint64(50), cfg.GCInterval)
	assert.Equal(t, uint64(5), cfg.MaxRoundsGap)
}

func TestNewConfig_Defaults(t *testing.T) {
	cfg, err := NewConfig[mockHash, *mockTransaction](
		WithMyIndex[mockHash, *mockTransaction](0),
		WithValidators[mockHash, *mockTransaction](mockValidatorSet{count: 4}),
		WithSigner[mockHash, *mockTransaction](mockSigner{}),
		WithStorage[mockHash, *mockTransaction](mockStorage{}),
		WithNetwork[mockHash, *mockTransaction](mockNetwork{}),
		WithTimer[mockHash, *mockTransaction](mockTimer{}),
	)

	require.NoError(t, err)
	assert.Equal(t, 4, cfg.WorkerCount)
	assert.Equal(t, 500, cfg.BatchSize)
	assert.Equal(t, 100*time.Millisecond, cfg.BatchTimeout)
	assert.Equal(t, 500*time.Millisecond, cfg.HeaderTimeout)
	assert.Equal(t, uint64(50), cfg.GCInterval)
	assert.Equal(t, uint64(100), cfg.GCDepth)
	assert.Equal(t, uint64(10), cfg.MaxRoundsGap)
}

func TestNewConfig_MissingValidators(t *testing.T) {
	_, err := NewConfig[mockHash, *mockTransaction](
		WithMyIndex[mockHash, *mockTransaction](0),
		WithSigner[mockHash, *mockTransaction](mockSigner{}),
		WithStorage[mockHash, *mockTransaction](mockStorage{}),
		WithNetwork[mockHash, *mockTransaction](mockNetwork{}),
		WithTimer[mockHash, *mockTransaction](mockTimer{}),
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "validators is required")
}

func TestNewConfig_MissingSigner(t *testing.T) {
	_, err := NewConfig[mockHash, *mockTransaction](
		WithMyIndex[mockHash, *mockTransaction](0),
		WithValidators[mockHash, *mockTransaction](mockValidatorSet{count: 4}),
		WithStorage[mockHash, *mockTransaction](mockStorage{}),
		WithNetwork[mockHash, *mockTransaction](mockNetwork{}),
		WithTimer[mockHash, *mockTransaction](mockTimer{}),
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "signer is required")
}

func TestNewConfig_MissingStorage(t *testing.T) {
	_, err := NewConfig[mockHash, *mockTransaction](
		WithMyIndex[mockHash, *mockTransaction](0),
		WithValidators[mockHash, *mockTransaction](mockValidatorSet{count: 4}),
		WithSigner[mockHash, *mockTransaction](mockSigner{}),
		WithNetwork[mockHash, *mockTransaction](mockNetwork{}),
		WithTimer[mockHash, *mockTransaction](mockTimer{}),
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "storage is required")
}

func TestNewConfig_MissingNetwork(t *testing.T) {
	_, err := NewConfig[mockHash, *mockTransaction](
		WithMyIndex[mockHash, *mockTransaction](0),
		WithValidators[mockHash, *mockTransaction](mockValidatorSet{count: 4}),
		WithSigner[mockHash, *mockTransaction](mockSigner{}),
		WithStorage[mockHash, *mockTransaction](mockStorage{}),
		WithTimer[mockHash, *mockTransaction](mockTimer{}),
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "network is required")
}

func TestNewConfig_MissingTimer(t *testing.T) {
	_, err := NewConfig[mockHash, *mockTransaction](
		WithMyIndex[mockHash, *mockTransaction](0),
		WithValidators[mockHash, *mockTransaction](mockValidatorSet{count: 4}),
		WithSigner[mockHash, *mockTransaction](mockSigner{}),
		WithStorage[mockHash, *mockTransaction](mockStorage{}),
		WithNetwork[mockHash, *mockTransaction](mockNetwork{}),
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "timer is required")
}

func TestNewConfig_InvalidMyIndex(t *testing.T) {
	_, err := NewConfig[mockHash, *mockTransaction](
		WithMyIndex[mockHash, *mockTransaction](10), // Out of range
		WithValidators[mockHash, *mockTransaction](mockValidatorSet{count: 4}),
		WithSigner[mockHash, *mockTransaction](mockSigner{}),
		WithStorage[mockHash, *mockTransaction](mockStorage{}),
		WithNetwork[mockHash, *mockTransaction](mockNetwork{}),
		WithTimer[mockHash, *mockTransaction](mockTimer{}),
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "myIndex")
}

func TestNewConfig_InvalidWorkerCount(t *testing.T) {
	_, err := NewConfig[mockHash, *mockTransaction](
		WithMyIndex[mockHash, *mockTransaction](0),
		WithValidators[mockHash, *mockTransaction](mockValidatorSet{count: 4}),
		WithSigner[mockHash, *mockTransaction](mockSigner{}),
		WithStorage[mockHash, *mockTransaction](mockStorage{}),
		WithNetwork[mockHash, *mockTransaction](mockNetwork{}),
		WithTimer[mockHash, *mockTransaction](mockTimer{}),
		WithWorkerCount[mockHash, *mockTransaction](0),
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "worker count")
}

func TestNewConfig_InvalidBatchSize(t *testing.T) {
	_, err := NewConfig[mockHash, *mockTransaction](
		WithMyIndex[mockHash, *mockTransaction](0),
		WithValidators[mockHash, *mockTransaction](mockValidatorSet{count: 4}),
		WithSigner[mockHash, *mockTransaction](mockSigner{}),
		WithStorage[mockHash, *mockTransaction](mockStorage{}),
		WithNetwork[mockHash, *mockTransaction](mockNetwork{}),
		WithTimer[mockHash, *mockTransaction](mockTimer{}),
		WithBatchSize[mockHash, *mockTransaction](0),
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "batch size")
}

func TestNewConfig_InvalidBatchTimeout(t *testing.T) {
	_, err := NewConfig[mockHash, *mockTransaction](
		WithMyIndex[mockHash, *mockTransaction](0),
		WithValidators[mockHash, *mockTransaction](mockValidatorSet{count: 4}),
		WithSigner[mockHash, *mockTransaction](mockSigner{}),
		WithStorage[mockHash, *mockTransaction](mockStorage{}),
		WithNetwork[mockHash, *mockTransaction](mockNetwork{}),
		WithTimer[mockHash, *mockTransaction](mockTimer{}),
		WithBatchTimeout[mockHash, *mockTransaction](0),
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "batch timeout")
}

func TestNewConfig_NilValidators(t *testing.T) {
	_, err := NewConfig[mockHash, *mockTransaction](
		WithValidators[mockHash, *mockTransaction](nil),
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "validators cannot be nil")
}

func TestNewConfig_NilSigner(t *testing.T) {
	_, err := NewConfig[mockHash, *mockTransaction](
		WithSigner[mockHash, *mockTransaction](nil),
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "signer cannot be nil")
}

func TestNewConfig_NilStorage(t *testing.T) {
	_, err := NewConfig[mockHash, *mockTransaction](
		WithStorage[mockHash, *mockTransaction](nil),
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "storage cannot be nil")
}

func TestNewConfig_NilNetwork(t *testing.T) {
	_, err := NewConfig[mockHash, *mockTransaction](
		WithNetwork[mockHash, *mockTransaction](nil),
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "network cannot be nil")
}

func TestNewConfig_NilTimer(t *testing.T) {
	_, err := NewConfig[mockHash, *mockTransaction](
		WithTimer[mockHash, *mockTransaction](nil),
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "timer cannot be nil")
}

func TestNewConfig_NilLogger(t *testing.T) {
	_, err := NewConfig[mockHash, *mockTransaction](
		WithLogger[mockHash, *mockTransaction](nil),
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "logger cannot be nil")
}

func TestPresetConfigs(t *testing.T) {
	t.Run("DefaultConfig", func(t *testing.T) {
		cfg := DefaultConfig[mockHash, *mockTransaction]()
		assert.Equal(t, 4, cfg.WorkerCount)
		assert.Equal(t, 500, cfg.BatchSize)
		assert.Equal(t, 100*time.Millisecond, cfg.BatchTimeout)
		assert.Equal(t, 500*time.Millisecond, cfg.HeaderTimeout)
	})

	t.Run("HighThroughputConfig", func(t *testing.T) {
		cfg := HighThroughputConfig[mockHash, *mockTransaction]()
		assert.Equal(t, 8, cfg.WorkerCount)
		assert.Equal(t, 1000, cfg.BatchSize)
		assert.Equal(t, 50*time.Millisecond, cfg.BatchTimeout)
		assert.Equal(t, 200*time.Millisecond, cfg.HeaderTimeout)
	})

	t.Run("LowLatencyConfig", func(t *testing.T) {
		cfg := LowLatencyConfig[mockHash, *mockTransaction]()
		assert.Equal(t, 2, cfg.WorkerCount)
		assert.Equal(t, 100, cfg.BatchSize)
		assert.Equal(t, 20*time.Millisecond, cfg.BatchTimeout)
		assert.Equal(t, 100*time.Millisecond, cfg.HeaderTimeout)
	})

	t.Run("DemoConfig", func(t *testing.T) {
		cfg := DemoConfig[mockHash, *mockTransaction]()
		assert.Equal(t, 2, cfg.WorkerCount)
		assert.Equal(t, 10, cfg.BatchSize)
		assert.Equal(t, 1*time.Second, cfg.BatchTimeout)
		assert.Equal(t, 2*time.Second, cfg.HeaderTimeout)
	})
}

func TestMessageType_String(t *testing.T) {
	tests := []struct {
		mt       MessageType
		expected string
	}{
		{MessageBatch, "BATCH"},
		{MessageHeader, "HEADER"},
		{MessageVote, "VOTE"},
		{MessageCertificate, "CERTIFICATE"},
		{MessageBatchRequest, "BATCH_REQUEST"},
		{MessageCertificateRequest, "CERTIFICATE_REQUEST"},
		{MessageType(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, tt.mt.String())
	}
}

func TestConfig_Warnings(t *testing.T) {
	baseOpts := func() []ConfigOption[mockHash, *mockTransaction] {
		return []ConfigOption[mockHash, *mockTransaction]{
			WithMyIndex[mockHash, *mockTransaction](0),
			WithValidators[mockHash, *mockTransaction](mockValidatorSet{count: 4}),
			WithSigner[mockHash, *mockTransaction](mockSigner{}),
			WithStorage[mockHash, *mockTransaction](mockStorage{}),
			WithNetwork[mockHash, *mockTransaction](mockNetwork{}),
			WithTimer[mockHash, *mockTransaction](mockTimer{}),
		}
	}

	t.Run("no warnings for good config", func(t *testing.T) {
		cfg, err := NewConfig(baseOpts()...)
		require.NoError(t, err)
		warnings := cfg.Warnings()
		assert.Empty(t, warnings)
	})

	t.Run("small validator set warning", func(t *testing.T) {
		cfg, err := NewConfig(
			WithMyIndex[mockHash, *mockTransaction](0),
			WithValidators[mockHash, *mockTransaction](mockValidatorSet{count: 3}),
			WithSigner[mockHash, *mockTransaction](mockSigner{}),
			WithStorage[mockHash, *mockTransaction](mockStorage{}),
			WithNetwork[mockHash, *mockTransaction](mockNetwork{}),
			WithTimer[mockHash, *mockTransaction](mockTimer{}),
		)
		require.NoError(t, err)
		warnings := cfg.Warnings()
		require.NotEmpty(t, warnings)
		hasValidatorWarning := false
		for _, w := range warnings {
			if w.Field == "Validators" {
				hasValidatorWarning = true
				break
			}
		}
		assert.True(t, hasValidatorWarning, "expected validator count warning")
	})

	t.Run("small batch size warning", func(t *testing.T) {
		opts := append(baseOpts(), WithBatchSize[mockHash, *mockTransaction](5))
		cfg, err := NewConfig(opts...)
		require.NoError(t, err)
		warnings := cfg.Warnings()
		hasBatchWarning := false
		for _, w := range warnings {
			if w.Field == "BatchSize" {
				hasBatchWarning = true
				break
			}
		}
		assert.True(t, hasBatchWarning, "expected batch size warning")
	})

	t.Run("large batch size warning", func(t *testing.T) {
		opts := append(baseOpts(), WithBatchSize[mockHash, *mockTransaction](20000))
		cfg, err := NewConfig(opts...)
		require.NoError(t, err)
		warnings := cfg.Warnings()
		hasBatchWarning := false
		for _, w := range warnings {
			if w.Field == "BatchSize" {
				hasBatchWarning = true
				break
			}
		}
		assert.True(t, hasBatchWarning, "expected batch size warning")
	})

	t.Run("batch timeout > header timeout warning", func(t *testing.T) {
		opts := append(baseOpts(),
			WithBatchTimeout[mockHash, *mockTransaction](1*time.Second),
			WithHeaderTimeout[mockHash, *mockTransaction](100*time.Millisecond),
		)
		cfg, err := NewConfig(opts...)
		require.NoError(t, err)
		warnings := cfg.Warnings()
		hasTimeoutWarning := false
		for _, w := range warnings {
			if w.Field == "BatchTimeout/HeaderTimeout" {
				hasTimeoutWarning = true
				break
			}
		}
		assert.True(t, hasTimeoutWarning, "expected batch/header timeout warning")
	})

	t.Run("small GC depth warning", func(t *testing.T) {
		opts := append(baseOpts(), WithGCDepth[mockHash, *mockTransaction](5))
		cfg, err := NewConfig(opts...)
		require.NoError(t, err)
		warnings := cfg.Warnings()
		hasGCWarning := false
		for _, w := range warnings {
			if w.Field == "GCDepth" {
				hasGCWarning = true
				break
			}
		}
		assert.True(t, hasGCWarning, "expected GC depth warning")
	})

	t.Run("GC interval > GC depth warning", func(t *testing.T) {
		opts := append(baseOpts(),
			WithGCInterval[mockHash, *mockTransaction](200),
			WithGCDepth[mockHash, *mockTransaction](50),
		)
		cfg, err := NewConfig(opts...)
		require.NoError(t, err)
		warnings := cfg.Warnings()
		hasGCWarning := false
		for _, w := range warnings {
			if w.Field == "GCInterval/GCDepth" {
				hasGCWarning = true
				break
			}
		}
		assert.True(t, hasGCWarning, "expected GC interval/depth warning")
	})

	t.Run("small max rounds gap warning", func(t *testing.T) {
		opts := append(baseOpts(), WithMaxRoundsGap[mockHash, *mockTransaction](2))
		cfg, err := NewConfig(opts...)
		require.NoError(t, err)
		warnings := cfg.Warnings()
		hasGapWarning := false
		for _, w := range warnings {
			if w.Field == "MaxRoundsGap" {
				hasGapWarning = true
				break
			}
		}
		assert.True(t, hasGapWarning, "expected max rounds gap warning")
	})

	t.Run("high worker count warning", func(t *testing.T) {
		opts := append(baseOpts(), WithWorkerCount[mockHash, *mockTransaction](32))
		cfg, err := NewConfig(opts...)
		require.NoError(t, err)
		warnings := cfg.Warnings()
		hasWorkerWarning := false
		for _, w := range warnings {
			if w.Field == "WorkerCount" {
				hasWorkerWarning = true
				break
			}
		}
		assert.True(t, hasWorkerWarning, "expected worker count warning")
	})

	t.Run("SyncRetryNodes > peers warning", func(t *testing.T) {
		opts := append(baseOpts(), WithSyncRetryNodes[mockHash, *mockTransaction](10))
		cfg, err := NewConfig(opts...)
		require.NoError(t, err)
		warnings := cfg.Warnings()
		hasSyncWarning := false
		for _, w := range warnings {
			if w.Field == "SyncRetryNodes" {
				hasSyncWarning = true
				break
			}
		}
		assert.True(t, hasSyncWarning, "expected SyncRetryNodes warning")
	})
}

func TestConfigWarning_String(t *testing.T) {
	w := ConfigWarning{
		Field:      "TestField",
		Message:    "test message",
		Suggestion: "test suggestion",
	}
	s := w.String()
	assert.Contains(t, s, "TestField")
	assert.Contains(t, s, "test message")
	assert.Contains(t, s, "test suggestion")
}

func TestConfig_LogWarnings(t *testing.T) {
	// Just ensure it doesn't panic with a real logger
	cfg, err := NewConfig(
		WithMyIndex[mockHash, *mockTransaction](0),
		WithValidators[mockHash, *mockTransaction](mockValidatorSet{count: 3}), // Will generate warning
		WithSigner[mockHash, *mockTransaction](mockSigner{}),
		WithStorage[mockHash, *mockTransaction](mockStorage{}),
		WithNetwork[mockHash, *mockTransaction](mockNetwork{}),
		WithTimer[mockHash, *mockTransaction](mockTimer{}),
		WithLogger[mockHash, *mockTransaction](zap.NewNop()),
	)
	require.NoError(t, err)

	// Should not panic
	cfg.LogWarnings()
}
