package narwhal

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// testHash is a simple hash type for testing
type testVoteHash [32]byte

func (h testVoteHash) Bytes() []byte { return h[:] }
func (h testVoteHash) Equals(other Hash) bool {
	if o, ok := other.(testVoteHash); ok {
		return h == o
	}
	return false
}
func (h testVoteHash) String() string { return string(h[:8]) }

func makeTestVoteHash(b byte) testVoteHash {
	var h testVoteHash
	h[0] = b
	return h
}

func TestVoteTracker_NewVoteTracker(t *testing.T) {
	t.Run("with logger", func(t *testing.T) {
		logger := zap.NewNop()
		vt := NewVoteTracker[testVoteHash](logger)
		require.NotNil(t, vt)
		assert.NotNil(t, vt.votes)
		assert.Equal(t, uint64(0), vt.currentEpoch)
	})

	t.Run("with nil logger", func(t *testing.T) {
		vt := NewVoteTracker[testVoteHash](nil)
		require.NotNil(t, vt)
		assert.NotNil(t, vt.logger) // Should get nop logger
	})
}

func TestVoteTracker_ShouldVote_Allow(t *testing.T) {
	vt := NewVoteTracker[testVoteHash](nil)

	// First vote for an author should be allowed
	decision, existing := vt.ShouldVote(0, 1, 0, makeTestVoteHash(1))
	assert.Equal(t, VoteDecisionAllow, decision)
	assert.Nil(t, existing)
}

func TestVoteTracker_ShouldVote_AllowNewRound(t *testing.T) {
	vt := NewVoteTracker[testVoteHash](nil)

	// Record a vote for round 1
	vt.RecordVote(0, 1, 0, makeTestVoteHash(1))

	// Vote for round 2 should be allowed
	decision, existing := vt.ShouldVote(0, 2, 0, makeTestVoteHash(2))
	assert.Equal(t, VoteDecisionAllow, decision)
	assert.Nil(t, existing)
}

func TestVoteTracker_ShouldVote_SkipDuplicate(t *testing.T) {
	vt := NewVoteTracker[testVoteHash](nil)
	digest := makeTestVoteHash(1)

	// Record a vote
	vt.RecordVote(0, 1, 0, digest)

	// Same vote should be skipped as duplicate
	decision, existing := vt.ShouldVote(0, 1, 0, digest)
	assert.Equal(t, VoteDecisionSkipDuplicate, decision)
	assert.Nil(t, existing)
}

func TestVoteTracker_ShouldVote_SkipOldRound(t *testing.T) {
	vt := NewVoteTracker[testVoteHash](nil)

	// Record a vote for round 5
	vt.RecordVote(0, 5, 0, makeTestVoteHash(5))

	// Vote for round 3 should be skipped (old round)
	decision, existing := vt.ShouldVote(0, 3, 0, makeTestVoteHash(3))
	assert.Equal(t, VoteDecisionSkipOldRound, decision)
	assert.Nil(t, existing)
}

func TestVoteTracker_ShouldVote_SkipEquivocation(t *testing.T) {
	vt := NewVoteTracker[testVoteHash](nil)
	existingDigest := makeTestVoteHash(1)

	// Record a vote for round 1 with digest 1
	vt.RecordVote(0, 1, 0, existingDigest)

	// Different digest at same round = equivocation
	newDigest := makeTestVoteHash(2)
	decision, existing := vt.ShouldVote(0, 1, 0, newDigest)
	assert.Equal(t, VoteDecisionSkipEquivocation, decision)
	require.NotNil(t, existing)
	assert.True(t, existing.Equals(existingDigest))
}

func TestVoteTracker_ShouldVote_SkipOldEpoch(t *testing.T) {
	vt := NewVoteTracker[testVoteHash](nil)
	vt.SetEpoch(5)

	// Vote for old epoch should be skipped
	decision, existing := vt.ShouldVote(0, 1, 3, makeTestVoteHash(1))
	assert.Equal(t, VoteDecisionSkipOldEpoch, decision)
	assert.Nil(t, existing)
}

func TestVoteTracker_RecordVote(t *testing.T) {
	vt := NewVoteTracker[testVoteHash](nil)

	// Record a vote
	vt.RecordVote(0, 1, 0, makeTestVoteHash(1))

	// Verify it was recorded
	stats := vt.Stats()
	assert.Equal(t, 1, stats.TrackedAuthors)
}

func TestVoteTracker_RecordVote_SkipsOldRound(t *testing.T) {
	vt := NewVoteTracker[testVoteHash](nil)

	// Record a vote for round 5
	vt.RecordVote(0, 5, 0, makeTestVoteHash(5))

	// Try to record for old round - should be ignored
	vt.RecordVote(0, 3, 0, makeTestVoteHash(3))

	// Should still show round 5 as highest
	decision, _ := vt.ShouldVote(0, 4, 0, makeTestVoteHash(4))
	assert.Equal(t, VoteDecisionSkipOldRound, decision)
}

func TestVoteTracker_RecordVote_SkipsOldEpoch(t *testing.T) {
	vt := NewVoteTracker[testVoteHash](nil)
	vt.SetEpoch(5)

	// Try to record for old epoch - should be ignored
	vt.RecordVote(0, 1, 3, makeTestVoteHash(1))

	stats := vt.Stats()
	assert.Equal(t, 0, stats.TrackedAuthors)
}

func TestVoteTracker_SetEpoch(t *testing.T) {
	vt := NewVoteTracker[testVoteHash](nil)

	// Record votes in epoch 0
	vt.RecordVote(0, 1, 0, makeTestVoteHash(1))
	vt.RecordVote(1, 2, 0, makeTestVoteHash(2))

	assert.Equal(t, 2, vt.Stats().TrackedAuthors)

	// Advance epoch - old records should be cleared
	vt.SetEpoch(1)

	assert.Equal(t, uint64(1), vt.Stats().CurrentEpoch)
	assert.Equal(t, 0, vt.Stats().TrackedAuthors)
}

func TestVoteTracker_SetEpoch_IgnoresOldEpoch(t *testing.T) {
	vt := NewVoteTracker[testVoteHash](nil)
	vt.SetEpoch(5)

	// Try to set old epoch - should be ignored
	vt.SetEpoch(3)

	assert.Equal(t, uint64(5), vt.Stats().CurrentEpoch)
}

func TestVoteTracker_SetEpoch_AllowsRecordsFromNewEpoch(t *testing.T) {
	vt := NewVoteTracker[testVoteHash](nil)

	// Record in epoch 1
	vt.RecordVote(0, 1, 1, makeTestVoteHash(1))

	// Advance to epoch 1
	vt.SetEpoch(1)

	// Record from epoch 1 should still exist
	assert.Equal(t, 1, vt.Stats().TrackedAuthors)
}

func TestVoteTracker_GarbageCollect(t *testing.T) {
	vt := NewVoteTracker[testVoteHash](nil)

	// Record votes at various rounds
	vt.RecordVote(0, 5, 0, makeTestVoteHash(5))
	vt.RecordVote(1, 10, 0, makeTestVoteHash(10))
	vt.RecordVote(2, 15, 0, makeTestVoteHash(15))

	assert.Equal(t, 3, vt.Stats().TrackedAuthors)

	// GC rounds below 12
	vt.GarbageCollect(12)

	assert.Equal(t, uint64(12), vt.Stats().GCRound)
	assert.Equal(t, 1, vt.Stats().TrackedAuthors) // Only round 15 remains
}

func TestVoteTracker_GarbageCollect_IgnoresOldGCRound(t *testing.T) {
	vt := NewVoteTracker[testVoteHash](nil)

	vt.GarbageCollect(10)
	vt.GarbageCollect(5) // Should be ignored

	assert.Equal(t, uint64(10), vt.Stats().GCRound)
}

func TestVoteTracker_Clear(t *testing.T) {
	vt := NewVoteTracker[testVoteHash](nil)

	// Record votes
	vt.RecordVote(0, 1, 0, makeTestVoteHash(1))
	vt.RecordVote(1, 2, 0, makeTestVoteHash(2))
	vt.GarbageCollect(5)

	// Clear
	vt.Clear()

	stats := vt.Stats()
	assert.Equal(t, 0, stats.TrackedAuthors)
	assert.Equal(t, uint64(0), stats.GCRound)
}

func TestVoteTracker_Stats(t *testing.T) {
	vt := NewVoteTracker[testVoteHash](nil)

	vt.RecordVote(0, 1, 0, makeTestVoteHash(1))
	vt.RecordVote(1, 2, 0, makeTestVoteHash(2))
	vt.SetEpoch(5)
	vt.GarbageCollect(10)

	// Record in new epoch
	vt.RecordVote(2, 15, 5, makeTestVoteHash(15))

	stats := vt.Stats()
	assert.Equal(t, 1, stats.TrackedAuthors) // Only epoch 5 record
	assert.Equal(t, uint64(5), stats.CurrentEpoch)
	assert.Equal(t, uint64(10), stats.GCRound)
}

func TestVoteDecision_String(t *testing.T) {
	tests := []struct {
		decision VoteDecision
		expected string
	}{
		{VoteDecisionAllow, "ALLOW"},
		{VoteDecisionSkipOldRound, "SKIP_OLD_ROUND"},
		{VoteDecisionSkipEquivocation, "SKIP_EQUIVOCATION"},
		{VoteDecisionSkipOldEpoch, "SKIP_OLD_EPOCH"},
		{VoteDecisionSkipDuplicate, "SKIP_DUPLICATE"},
		{VoteDecision(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.decision.String())
		})
	}
}

func TestVoteTracker_MultipleAuthors(t *testing.T) {
	vt := NewVoteTracker[testVoteHash](nil)

	// Record votes from multiple authors
	for i := uint16(0); i < 10; i++ {
		vt.RecordVote(i, uint64(i+1), 0, makeTestVoteHash(byte(i)))
	}

	assert.Equal(t, 10, vt.Stats().TrackedAuthors)

	// Each author's vote should be tracked independently
	for i := uint16(0); i < 10; i++ {
		// Voting for same round with same digest = duplicate
		decision, _ := vt.ShouldVote(i, uint64(i+1), 0, makeTestVoteHash(byte(i)))
		assert.Equal(t, VoteDecisionSkipDuplicate, decision)

		// Voting for higher round = allowed
		decision, _ = vt.ShouldVote(i, uint64(i+100), 0, makeTestVoteHash(byte(i+100)))
		assert.Equal(t, VoteDecisionAllow, decision)
	}
}

func TestVoteTracker_OldEpochRecord(t *testing.T) {
	vt := NewVoteTracker[testVoteHash](nil)

	// Record in epoch 0
	vt.RecordVote(0, 1, 0, makeTestVoteHash(1))

	// Advance to epoch 1
	vt.SetEpoch(1)

	// New vote in epoch 1 should be allowed (old record is from old epoch)
	decision, _ := vt.ShouldVote(0, 1, 1, makeTestVoteHash(2))
	assert.Equal(t, VoteDecisionAllow, decision)
}
