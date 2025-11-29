package narwhal

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNetworkModel_String(t *testing.T) {
	tests := []struct {
		model    NetworkModel
		expected string
	}{
		{NetworkModelAsynchronous, "asynchronous"},
		{NetworkModelPartiallySynchronous, "partially_synchronous"},
		{NetworkModel(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.model.String())
		})
	}
}

func TestRoundRobinLeaderSchedule(t *testing.T) {
	t.Run("basic leader rotation", func(t *testing.T) {
		schedule := NewRoundRobinLeaderSchedule(4)

		assert.Equal(t, uint16(0), schedule.Leader(0))
		assert.Equal(t, uint16(1), schedule.Leader(1))
		assert.Equal(t, uint16(2), schedule.Leader(2))
		assert.Equal(t, uint16(3), schedule.Leader(3))
		assert.Equal(t, uint16(0), schedule.Leader(4)) // Wraps around
		assert.Equal(t, uint16(1), schedule.Leader(5))
	})

	t.Run("IsLeader", func(t *testing.T) {
		schedule := NewRoundRobinLeaderSchedule(4)

		assert.True(t, schedule.IsLeader(0, 0))
		assert.False(t, schedule.IsLeader(0, 1))
		assert.False(t, schedule.IsLeader(0, 2))
		assert.False(t, schedule.IsLeader(0, 3))

		assert.False(t, schedule.IsLeader(1, 0))
		assert.True(t, schedule.IsLeader(1, 1))
		assert.False(t, schedule.IsLeader(1, 2))
		assert.False(t, schedule.IsLeader(1, 3))
	})

	t.Run("large rounds", func(t *testing.T) {
		schedule := NewRoundRobinLeaderSchedule(7)

		assert.Equal(t, uint16(0), schedule.Leader(1000*7))
		assert.Equal(t, uint16(1), schedule.Leader(1000*7+1))
		assert.Equal(t, uint16(6), schedule.Leader(1000*7+6))
	})
}

// testLeaderHash is a simple hash type for LeaderTracker testing
type testLeaderHash [32]byte

func (h testLeaderHash) Bytes() []byte { return h[:] }
func (h testLeaderHash) Equals(other Hash) bool {
	if o, ok := other.(testLeaderHash); ok {
		return h == o
	}
	return false
}
func (h testLeaderHash) String() string { return string(h[:8]) }

func makeTestLeaderHash(b byte) testLeaderHash {
	var h testLeaderHash
	h[0] = b
	return h
}

func makeTestHeader(author uint16, round uint64) *Header[testLeaderHash] {
	h := &Header[testLeaderHash]{
		Author: author,
		Round:  round,
		Digest: makeTestLeaderHash(byte(round)),
	}
	return h
}

func makeTestCert(author uint16, round uint64) *Certificate[testLeaderHash] {
	return &Certificate[testLeaderHash]{
		Header: makeTestHeader(author, round),
	}
}

func TestLeaderTracker_NewLeaderTracker(t *testing.T) {
	schedule := NewRoundRobinLeaderSchedule(4)
	tracker := NewLeaderTracker[testLeaderHash](schedule)

	require.NotNil(t, tracker)
	assert.NotNil(t, tracker.lastLeader)
	assert.Equal(t, schedule, tracker.schedule)
}

func TestLeaderTracker_SetRound(t *testing.T) {
	schedule := NewRoundRobinLeaderSchedule(4)
	tracker := NewLeaderTracker[testLeaderHash](schedule)

	tracker.SetRound(10)
	assert.Equal(t, uint64(10), tracker.currentRound)
}

func TestLeaderTracker_RecordCertificate(t *testing.T) {
	schedule := NewRoundRobinLeaderSchedule(4)
	tracker := NewLeaderTracker[testLeaderHash](schedule)

	t.Run("leader certificate is recorded", func(t *testing.T) {
		// Round 0 leader is validator 0
		cert := makeTestCert(0, 0)
		isLeader := tracker.RecordCertificate(cert)

		assert.True(t, isLeader)
		assert.Equal(t, cert, tracker.GetLeaderForRound(0))
	})

	t.Run("non-leader certificate is not recorded", func(t *testing.T) {
		// Round 0 leader is validator 0, not 1
		cert := makeTestCert(1, 0)
		isLeader := tracker.RecordCertificate(cert)

		assert.False(t, isLeader)
		// Leader cert from previous test should still be there
		assert.NotNil(t, tracker.GetLeaderForRound(0))
	})

	t.Run("leader at different rounds", func(t *testing.T) {
		// Round 1 leader is validator 1
		cert1 := makeTestCert(1, 1)
		assert.True(t, tracker.RecordCertificate(cert1))

		// Round 2 leader is validator 2
		cert2 := makeTestCert(2, 2)
		assert.True(t, tracker.RecordCertificate(cert2))

		assert.Equal(t, cert1, tracker.GetLeaderForRound(1))
		assert.Equal(t, cert2, tracker.GetLeaderForRound(2))
	})
}

func TestLeaderTracker_HasLeaderForRound(t *testing.T) {
	schedule := NewRoundRobinLeaderSchedule(4)
	tracker := NewLeaderTracker[testLeaderHash](schedule)

	assert.False(t, tracker.HasLeaderForRound(0))

	cert := makeTestCert(0, 0)
	tracker.RecordCertificate(cert)

	assert.True(t, tracker.HasLeaderForRound(0))
	assert.False(t, tracker.HasLeaderForRound(1))
}

func TestLeaderTracker_ShouldWaitForLeader(t *testing.T) {
	schedule := NewRoundRobinLeaderSchedule(4)
	tracker := NewLeaderTracker[testLeaderHash](schedule)

	// Even rounds should wait for leader
	assert.True(t, tracker.ShouldWaitForLeader(0))
	assert.True(t, tracker.ShouldWaitForLeader(2))
	assert.True(t, tracker.ShouldWaitForLeader(4))
	assert.True(t, tracker.ShouldWaitForLeader(100))

	// Odd rounds should not wait
	assert.False(t, tracker.ShouldWaitForLeader(1))
	assert.False(t, tracker.ShouldWaitForLeader(3))
	assert.False(t, tracker.ShouldWaitForLeader(5))
	assert.False(t, tracker.ShouldWaitForLeader(101))
}

func TestLeaderTracker_CheckLeaderVotes(t *testing.T) {
	schedule := NewRoundRobinLeaderSchedule(4)
	tracker := NewLeaderTracker[testLeaderHash](schedule)

	leaderDigest := makeTestLeaderHash(1)
	otherDigest := makeTestLeaderHash(2)

	t.Run("no leader digest known", func(t *testing.T) {
		parents := []*Certificate[testLeaderHash]{
			makeTestCert(0, 0),
			makeTestCert(1, 0),
			makeTestCert(2, 0),
		}

		status := tracker.CheckLeaderVotes(parents, nil, 1) // f=1

		assert.Equal(t, 0, status.VotesForLeader)
		assert.Equal(t, 3, status.VotesNotForLeader)
		assert.False(t, status.HasEnoughForLeaderVotes)   // Need f+1 = 2
		assert.True(t, status.HasEnoughNotForLeaderVotes) // Have 2f+1 = 3
	})

	t.Run("some parents have leader", func(t *testing.T) {
		// Create parents where some reference the leader
		parent1 := &Certificate[testLeaderHash]{
			Header: &Header[testLeaderHash]{
				Author:  0,
				Round:   1,
				Parents: []testLeaderHash{leaderDigest}, // Has leader
			},
		}
		parent2 := &Certificate[testLeaderHash]{
			Header: &Header[testLeaderHash]{
				Author:  1,
				Round:   1,
				Parents: []testLeaderHash{leaderDigest}, // Has leader
			},
		}
		parent3 := &Certificate[testLeaderHash]{
			Header: &Header[testLeaderHash]{
				Author:  2,
				Round:   1,
				Parents: []testLeaderHash{otherDigest}, // No leader
			},
		}

		parents := []*Certificate[testLeaderHash]{parent1, parent2, parent3}
		status := tracker.CheckLeaderVotes(parents, &leaderDigest, 1)

		assert.Equal(t, 2, status.VotesForLeader)
		assert.Equal(t, 1, status.VotesNotForLeader)
		assert.True(t, status.HasEnoughForLeaderVotes)     // f+1 = 2
		assert.False(t, status.HasEnoughNotForLeaderVotes) // Need 2f+1 = 3
	})

	t.Run("all parents have leader", func(t *testing.T) {
		parents := make([]*Certificate[testLeaderHash], 4)
		for i := 0; i < 4; i++ {
			parents[i] = &Certificate[testLeaderHash]{
				Header: &Header[testLeaderHash]{
					Author:  uint16(i),
					Round:   1,
					Parents: []testLeaderHash{leaderDigest},
				},
			}
		}

		status := tracker.CheckLeaderVotes(parents, &leaderDigest, 1)

		assert.Equal(t, 4, status.VotesForLeader)
		assert.Equal(t, 0, status.VotesNotForLeader)
		assert.True(t, status.HasEnoughForLeaderVotes)
		assert.False(t, status.HasEnoughNotForLeaderVotes)
	})
}

func TestLeaderTracker_GarbageCollect(t *testing.T) {
	schedule := NewRoundRobinLeaderSchedule(4)
	tracker := NewLeaderTracker[testLeaderHash](schedule)

	// Record leaders at various rounds
	for round := uint64(0); round < 10; round++ {
		leader := schedule.Leader(round)
		cert := makeTestCert(leader, round)
		tracker.RecordCertificate(cert)
	}

	assert.True(t, tracker.HasLeaderForRound(0))
	assert.True(t, tracker.HasLeaderForRound(5))
	assert.True(t, tracker.HasLeaderForRound(9))

	// GC rounds before 5
	tracker.GarbageCollect(5)

	assert.False(t, tracker.HasLeaderForRound(0))
	assert.False(t, tracker.HasLeaderForRound(4))
	assert.True(t, tracker.HasLeaderForRound(5))
	assert.True(t, tracker.HasLeaderForRound(9))
}

func TestLeaderTracker_Clear(t *testing.T) {
	schedule := NewRoundRobinLeaderSchedule(4)
	tracker := NewLeaderTracker[testLeaderHash](schedule)

	// Record some leaders
	tracker.RecordCertificate(makeTestCert(0, 0))
	tracker.RecordCertificate(makeTestCert(1, 1))
	tracker.SetRound(5)

	// Clear
	tracker.Clear()

	assert.False(t, tracker.HasLeaderForRound(0))
	assert.False(t, tracker.HasLeaderForRound(1))
	assert.Equal(t, uint64(0), tracker.currentRound)
}

func TestLeaderVoteStatus(t *testing.T) {
	status := LeaderVoteStatus{
		VotesForLeader:             3,
		VotesNotForLeader:          1,
		HasEnoughForLeaderVotes:    true,
		HasEnoughNotForLeaderVotes: false,
	}

	assert.Equal(t, 3, status.VotesForLeader)
	assert.Equal(t, 1, status.VotesNotForLeader)
	assert.True(t, status.HasEnoughForLeaderVotes)
	assert.False(t, status.HasEnoughNotForLeaderVotes)
}
