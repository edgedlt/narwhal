package narwhal

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// mockValidatorSetForReconfig implements ValidatorSet for testing
type mockValidatorSetForReconfig struct {
	count int
}

func (v mockValidatorSetForReconfig) Count() int                               { return v.count }
func (v mockValidatorSetForReconfig) GetByIndex(idx uint16) (PublicKey, error) { return nil, nil }
func (v mockValidatorSetForReconfig) Contains(idx uint16) bool                 { return int(idx) < v.count }
func (v mockValidatorSetForReconfig) F() int                                   { return (v.count - 1) / 3 }

func TestReconfigurationState_String(t *testing.T) {
	tests := []struct {
		state    ReconfigurationState
		expected string
	}{
		{ReconfigurationStateIdle, "IDLE"},
		{ReconfigurationStatePending, "PENDING"},
		{ReconfigurationStateCommitting, "COMMITTING"},
		{ReconfigurationStateComplete, "COMPLETE"},
		{ReconfigurationState(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.state.String())
		})
	}
}

func TestReconfigurer_NewReconfigurer(t *testing.T) {
	validators := mockValidatorSetForReconfig{count: 4}

	t.Run("with logger", func(t *testing.T) {
		r := NewReconfigurer(0, validators, zap.NewNop())
		require.NotNil(t, r)
		assert.Equal(t, uint64(0), r.CurrentEpoch())
		assert.Equal(t, ReconfigurationStateIdle, r.State())
	})

	t.Run("with nil logger", func(t *testing.T) {
		r := NewReconfigurer(5, validators, nil)
		require.NotNil(t, r)
		assert.Equal(t, uint64(5), r.CurrentEpoch())
	})
}

func TestReconfigurer_CurrentEpoch(t *testing.T) {
	r := NewReconfigurer(10, mockValidatorSetForReconfig{count: 4}, nil)
	assert.Equal(t, uint64(10), r.CurrentEpoch())
}

func TestReconfigurer_CurrentValidators(t *testing.T) {
	validators := mockValidatorSetForReconfig{count: 7}
	r := NewReconfigurer(0, validators, nil)
	assert.Equal(t, 7, r.CurrentValidators().Count())
}

func TestReconfigurer_ProposeEpochChange(t *testing.T) {
	validators := mockValidatorSetForReconfig{count: 4}
	newValidators := mockValidatorSetForReconfig{count: 5}

	t.Run("successful proposal", func(t *testing.T) {
		r := NewReconfigurer(0, validators, nil)

		err := r.ProposeEpochChange(1, 100, newValidators)
		require.NoError(t, err)

		assert.Equal(t, ReconfigurationStatePending, r.State())

		change := r.PendingChange()
		require.NotNil(t, change)
		assert.Equal(t, uint64(0), change.FromEpoch)
		assert.Equal(t, uint64(1), change.ToEpoch)
		assert.Equal(t, uint64(100), change.EffectiveRound)
	})

	t.Run("error when reconfiguration in progress", func(t *testing.T) {
		r := NewReconfigurer(0, validators, nil)
		require.NoError(t, r.ProposeEpochChange(1, 100, nil))

		err := r.ProposeEpochChange(2, 200, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already in progress")
	})

	t.Run("error for invalid epoch transition", func(t *testing.T) {
		r := NewReconfigurer(0, validators, nil)

		err := r.ProposeEpochChange(5, 100, nil) // Should be 1
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid epoch transition")
	})
}

func TestReconfigurer_BeginCommit(t *testing.T) {
	validators := mockValidatorSetForReconfig{count: 4}

	t.Run("successful begin commit", func(t *testing.T) {
		r := NewReconfigurer(0, validators, nil)
		require.NoError(t, r.ProposeEpochChange(1, 100, nil))

		err := r.BeginCommit()
		require.NoError(t, err)

		assert.Equal(t, ReconfigurationStateCommitting, r.State())
	})

	t.Run("error when not pending", func(t *testing.T) {
		r := NewReconfigurer(0, validators, nil)

		err := r.BeginCommit()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not in pending state")
	})
}

func TestReconfigurer_CompleteCommit(t *testing.T) {
	validators := mockValidatorSetForReconfig{count: 4}
	newValidators := mockValidatorSetForReconfig{count: 5}

	t.Run("successful complete commit", func(t *testing.T) {
		r := NewReconfigurer(0, validators, nil)
		require.NoError(t, r.ProposeEpochChange(1, 100, newValidators))
		require.NoError(t, r.BeginCommit())

		err := r.CompleteCommit()
		require.NoError(t, err)

		assert.Equal(t, ReconfigurationStateIdle, r.State())
		assert.Equal(t, uint64(1), r.CurrentEpoch())
		assert.Equal(t, 5, r.CurrentValidators().Count())
		assert.Nil(t, r.PendingChange())
	})

	t.Run("complete without new validators", func(t *testing.T) {
		r := NewReconfigurer(0, validators, nil)
		require.NoError(t, r.ProposeEpochChange(1, 100, nil))
		require.NoError(t, r.BeginCommit())

		err := r.CompleteCommit()
		require.NoError(t, err)

		// Validators unchanged
		assert.Equal(t, 4, r.CurrentValidators().Count())
	})

	t.Run("error when not committing", func(t *testing.T) {
		r := NewReconfigurer(0, validators, nil)

		err := r.CompleteCommit()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not in committing state")
	})
}

func TestReconfigurer_CancelPendingChange(t *testing.T) {
	validators := mockValidatorSetForReconfig{count: 4}

	t.Run("successful cancel", func(t *testing.T) {
		r := NewReconfigurer(0, validators, nil)
		require.NoError(t, r.ProposeEpochChange(1, 100, nil))

		err := r.CancelPendingChange()
		require.NoError(t, err)

		assert.Equal(t, ReconfigurationStateIdle, r.State())
		assert.Nil(t, r.PendingChange())
	})

	t.Run("error when not pending", func(t *testing.T) {
		r := NewReconfigurer(0, validators, nil)

		err := r.CancelPendingChange()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not in pending state")
	})
}

func TestReconfigurer_OnEpochChange(t *testing.T) {
	validators := mockValidatorSetForReconfig{count: 4}

	var callbackCalled atomic.Int32
	var receivedChange EpochChange

	r := NewReconfigurer(0, validators, nil)
	r.OnEpochChange(func(change EpochChange) {
		callbackCalled.Add(1)
		receivedChange = change
	})

	require.NoError(t, r.ProposeEpochChange(1, 100, nil))
	require.NoError(t, r.BeginCommit())
	require.NoError(t, r.CompleteCommit())

	assert.Equal(t, int32(1), callbackCalled.Load())
	assert.Equal(t, uint64(0), receivedChange.FromEpoch)
	assert.Equal(t, uint64(1), receivedChange.ToEpoch)
	assert.False(t, receivedChange.CommittedAt.IsZero())
}

func TestReconfigurer_MultipleCallbacks(t *testing.T) {
	validators := mockValidatorSetForReconfig{count: 4}

	var callback1Called, callback2Called atomic.Int32

	r := NewReconfigurer(0, validators, nil)
	r.OnEpochChange(func(change EpochChange) {
		callback1Called.Add(1)
	})
	r.OnEpochChange(func(change EpochChange) {
		callback2Called.Add(1)
	})

	require.NoError(t, r.ProposeEpochChange(1, 100, nil))
	require.NoError(t, r.BeginCommit())
	require.NoError(t, r.CompleteCommit())

	assert.Equal(t, int32(1), callback1Called.Load())
	assert.Equal(t, int32(1), callback2Called.Load())
}

func TestReconfigurer_ShouldAcceptForEpoch(t *testing.T) {
	validators := mockValidatorSetForReconfig{count: 4}
	r := NewReconfigurer(5, validators, nil)

	// Current epoch should be accepted
	assert.True(t, r.ShouldAcceptForEpoch(5))

	// Old epoch should not be accepted
	assert.False(t, r.ShouldAcceptForEpoch(4))

	// Future epoch should not be accepted
	assert.False(t, r.ShouldAcceptForEpoch(6))

	// During transition, new epoch should be accepted when committing
	require.NoError(t, r.ProposeEpochChange(6, 100, nil))
	assert.False(t, r.ShouldAcceptForEpoch(6)) // Not yet committing

	require.NoError(t, r.BeginCommit())
	assert.True(t, r.ShouldAcceptForEpoch(6)) // Now committing
}

func TestReconfigurer_IsEpochBoundaryRound(t *testing.T) {
	validators := mockValidatorSetForReconfig{count: 4}
	r := NewReconfigurer(0, validators, nil)

	// No pending change
	assert.False(t, r.IsEpochBoundaryRound(100))

	// With pending change
	require.NoError(t, r.ProposeEpochChange(1, 100, nil))
	assert.True(t, r.IsEpochBoundaryRound(100))
	assert.False(t, r.IsEpochBoundaryRound(99))
	assert.False(t, r.IsEpochBoundaryRound(101))
}

func TestReconfigurer_History(t *testing.T) {
	validators := mockValidatorSetForReconfig{count: 4}
	r := NewReconfigurer(0, validators, nil)

	// Initially empty
	assert.Len(t, r.History(), 0)

	// Complete a few reconfigurations
	for i := uint64(1); i <= 3; i++ {
		require.NoError(t, r.ProposeEpochChange(i, i*100, nil))
		require.NoError(t, r.BeginCommit())
		require.NoError(t, r.CompleteCommit())
	}

	history := r.History()
	assert.Len(t, history, 3)
	assert.Equal(t, uint64(0), history[0].FromEpoch)
	assert.Equal(t, uint64(1), history[0].ToEpoch)
	assert.Equal(t, uint64(2), history[1].ToEpoch)
	assert.Equal(t, uint64(3), history[2].ToEpoch)
}

func TestReconfigurer_HistoryLimit(t *testing.T) {
	validators := mockValidatorSetForReconfig{count: 4}
	r := NewReconfigurer(0, validators, nil)
	r.maxHistory = 5

	// Complete more reconfigurations than the limit
	for i := uint64(1); i <= 10; i++ {
		require.NoError(t, r.ProposeEpochChange(i, i*100, nil))
		require.NoError(t, r.BeginCommit())
		require.NoError(t, r.CompleteCommit())
	}

	history := r.History()
	assert.Len(t, history, 5)
	// Should have the most recent 5
	assert.Equal(t, uint64(6), history[0].ToEpoch)
	assert.Equal(t, uint64(10), history[4].ToEpoch)
}

func TestReconfigurer_Stats(t *testing.T) {
	validators := mockValidatorSetForReconfig{count: 7}
	r := NewReconfigurer(5, validators, nil)
	r.OnEpochChange(func(change EpochChange) {})

	stats := r.Stats()
	assert.Equal(t, uint64(5), stats.CurrentEpoch)
	assert.Equal(t, ReconfigurationStateIdle, stats.State)
	assert.False(t, stats.HasPendingChange)
	assert.Equal(t, 7, stats.ValidatorCount)
	assert.Equal(t, 1, stats.CallbackCount)

	// With pending change
	require.NoError(t, r.ProposeEpochChange(6, 100, nil))
	stats = r.Stats()
	assert.True(t, stats.HasPendingChange)
	assert.Equal(t, uint64(6), stats.PendingToEpoch)
}

func TestValidatorSetChangeType_String(t *testing.T) {
	tests := []struct {
		changeType ValidatorSetChangeType
		expected   string
	}{
		{ValidatorSetChangeAdd, "ADD"},
		{ValidatorSetChangeRemove, "REMOVE"},
		{ValidatorSetChangeUpdate, "UPDATE"},
		{ValidatorSetChangeType(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.changeType.String())
		})
	}
}

func TestEpochAwareValidatorSet(t *testing.T) {
	validators := mockValidatorSetForReconfig{count: 4}
	epochAware := NewEpochAwareValidatorSet(validators, 10)

	assert.Equal(t, uint64(10), epochAware.Epoch())
	assert.Equal(t, 4, epochAware.Count())
	assert.Equal(t, 1, epochAware.F())
}

// testReconfigHash is a simple hash type for coordinator testing
type testReconfigHash [32]byte

func (h testReconfigHash) Bytes() []byte { return h[:] }
func (h testReconfigHash) Equals(other Hash) bool {
	if o, ok := other.(testReconfigHash); ok {
		return h == o
	}
	return false
}
func (h testReconfigHash) String() string { return string(h[:8]) }

func TestReconfigurationCoordinator_NewReconfigurationCoordinator(t *testing.T) {
	validators := mockValidatorSetForReconfig{count: 4}
	r := NewReconfigurer(0, validators, nil)
	hashFunc := func(b []byte) testReconfigHash { var h testReconfigHash; copy(h[:], b); return h }

	coord := NewReconfigurationCoordinator[testReconfigHash](r, hashFunc, nil)
	require.NotNil(t, coord)
}

func TestReconfigurationCoordinator_ProposeReconfiguration(t *testing.T) {
	validators := mockValidatorSetForReconfig{count: 4}
	r := NewReconfigurer(0, validators, nil)
	hashFunc := func(b []byte) testReconfigHash { var h testReconfigHash; copy(h[:], b); return h }
	coord := NewReconfigurationCoordinator[testReconfigHash](r, hashFunc, nil)

	changes := []ValidatorSetChange{
		{Type: ValidatorSetChangeAdd, ValidatorIndex: 4},
	}

	err := coord.ProposeReconfiguration(100, changes, 0, []byte("sig"))
	require.NoError(t, err)

	proposal := coord.CurrentProposal()
	require.NotNil(t, proposal)
	assert.Equal(t, uint64(1), proposal.Epoch)
	assert.Equal(t, uint64(100), proposal.EffectiveRound)
	assert.Len(t, proposal.Changes, 1)
}

func TestReconfigurationCoordinator_ProposeReconfiguration_AlreadyInProgress(t *testing.T) {
	validators := mockValidatorSetForReconfig{count: 4}
	r := NewReconfigurer(0, validators, nil)
	hashFunc := func(b []byte) testReconfigHash { var h testReconfigHash; copy(h[:], b); return h }
	coord := NewReconfigurationCoordinator[testReconfigHash](r, hashFunc, nil)

	require.NoError(t, coord.ProposeReconfiguration(100, nil, 0, nil))

	err := coord.ProposeReconfiguration(200, nil, 1, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already in progress")
}

func TestReconfigurationCoordinator_AddVote(t *testing.T) {
	validators := mockValidatorSetForReconfig{count: 4}
	r := NewReconfigurer(0, validators, nil)
	hashFunc := func(b []byte) testReconfigHash { var h testReconfigHash; copy(h[:], b); return h }
	coord := NewReconfigurationCoordinator[testReconfigHash](r, hashFunc, nil)

	require.NoError(t, coord.ProposeReconfiguration(100, nil, 0, nil))

	vote := &ReconfigurationVote{
		ProposalHash: []byte("hash"),
		Voter:        1,
		Approve:      true,
		Signature:    []byte("sig"),
	}

	err := coord.AddVote(vote)
	require.NoError(t, err)

	approvals, rejections, total := coord.VoteStats()
	assert.Equal(t, 1, approvals)
	assert.Equal(t, 0, rejections)
	assert.Equal(t, 1, total)
}

func TestReconfigurationCoordinator_AddVote_NoProposal(t *testing.T) {
	validators := mockValidatorSetForReconfig{count: 4}
	r := NewReconfigurer(0, validators, nil)
	hashFunc := func(b []byte) testReconfigHash { var h testReconfigHash; copy(h[:], b); return h }
	coord := NewReconfigurationCoordinator[testReconfigHash](r, hashFunc, nil)

	vote := &ReconfigurationVote{Voter: 1, Approve: true}
	err := coord.AddVote(vote)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no proposal in progress")
}

func TestReconfigurationCoordinator_AddVote_Duplicate(t *testing.T) {
	validators := mockValidatorSetForReconfig{count: 4}
	r := NewReconfigurer(0, validators, nil)
	hashFunc := func(b []byte) testReconfigHash { var h testReconfigHash; copy(h[:], b); return h }
	coord := NewReconfigurationCoordinator[testReconfigHash](r, hashFunc, nil)

	require.NoError(t, coord.ProposeReconfiguration(100, nil, 0, nil))

	vote := &ReconfigurationVote{Voter: 1, Approve: true}
	require.NoError(t, coord.AddVote(vote))

	err := coord.AddVote(vote)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate vote")
}

func TestReconfigurationCoordinator_CheckQuorum(t *testing.T) {
	validators := mockValidatorSetForReconfig{count: 4} // f=1, quorum=3
	r := NewReconfigurer(0, validators, nil)
	hashFunc := func(b []byte) testReconfigHash { var h testReconfigHash; copy(h[:], b); return h }
	coord := NewReconfigurationCoordinator[testReconfigHash](r, hashFunc, nil)

	// No proposal
	assert.False(t, coord.CheckQuorum())

	require.NoError(t, coord.ProposeReconfiguration(100, nil, 0, nil))

	// Not enough votes
	_ = coord.AddVote(&ReconfigurationVote{Voter: 0, Approve: true})
	_ = coord.AddVote(&ReconfigurationVote{Voter: 1, Approve: true})
	assert.False(t, coord.CheckQuorum())

	// Quorum reached
	_ = coord.AddVote(&ReconfigurationVote{Voter: 2, Approve: true})
	assert.True(t, coord.CheckQuorum())
}

func TestReconfigurationCoordinator_IsRejected(t *testing.T) {
	validators := mockValidatorSetForReconfig{count: 4} // f=1
	r := NewReconfigurer(0, validators, nil)
	hashFunc := func(b []byte) testReconfigHash { var h testReconfigHash; copy(h[:], b); return h }
	coord := NewReconfigurationCoordinator[testReconfigHash](r, hashFunc, nil)

	// No proposal
	assert.False(t, coord.IsRejected())

	require.NoError(t, coord.ProposeReconfiguration(100, nil, 0, nil))

	// Not enough rejections
	_ = coord.AddVote(&ReconfigurationVote{Voter: 0, Approve: false})
	assert.False(t, coord.IsRejected())

	// f+1 = 2 rejections makes quorum impossible
	_ = coord.AddVote(&ReconfigurationVote{Voter: 1, Approve: false})
	assert.True(t, coord.IsRejected())
}

func TestReconfigurationCoordinator_Clear(t *testing.T) {
	validators := mockValidatorSetForReconfig{count: 4}
	r := NewReconfigurer(0, validators, nil)
	hashFunc := func(b []byte) testReconfigHash { var h testReconfigHash; copy(h[:], b); return h }
	coord := NewReconfigurationCoordinator[testReconfigHash](r, hashFunc, nil)

	require.NoError(t, coord.ProposeReconfiguration(100, nil, 0, nil))
	_ = coord.AddVote(&ReconfigurationVote{Voter: 0, Approve: true})

	coord.Clear()

	assert.Nil(t, coord.CurrentProposal())
	approvals, rejections, total := coord.VoteStats()
	assert.Equal(t, 0, approvals)
	assert.Equal(t, 0, rejections)
	assert.Equal(t, 0, total)
}

func TestEpochChange_Fields(t *testing.T) {
	now := time.Now()
	change := EpochChange{
		FromEpoch:      1,
		ToEpoch:        2,
		EffectiveRound: 100,
		NewValidators:  mockValidatorSetForReconfig{count: 5},
		ProposedAt:     now,
		CommittedAt:    now.Add(time.Second),
	}

	assert.Equal(t, uint64(1), change.FromEpoch)
	assert.Equal(t, uint64(2), change.ToEpoch)
	assert.Equal(t, uint64(100), change.EffectiveRound)
	assert.Equal(t, 5, change.NewValidators.Count())
}

func TestReconfigurationProposal_Fields(t *testing.T) {
	now := time.Now()
	proposal := ReconfigurationProposal{
		Epoch:          5,
		EffectiveRound: 500,
		Changes: []ValidatorSetChange{
			{Type: ValidatorSetChangeAdd, ValidatorIndex: 10},
		},
		Proposer:   0,
		Signature:  []byte("sig"),
		ProposedAt: now,
	}

	assert.Equal(t, uint64(5), proposal.Epoch)
	assert.Equal(t, uint64(500), proposal.EffectiveRound)
	assert.Len(t, proposal.Changes, 1)
	assert.Equal(t, uint16(0), proposal.Proposer)
}

func TestValidatorSetChange_Fields(t *testing.T) {
	change := ValidatorSetChange{
		Type:           ValidatorSetChangeUpdate,
		ValidatorIndex: 3,
		PublicKey:      nil,
	}

	assert.Equal(t, ValidatorSetChangeUpdate, change.Type)
	assert.Equal(t, uint16(3), change.ValidatorIndex)
}
