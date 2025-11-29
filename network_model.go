package narwhal

import (
	"sync"
)

// NetworkModel specifies the network timing assumptions for the consensus protocol.
// This affects how the proposer decides when to advance rounds.
type NetworkModel uint8

const (
	// NetworkModelAsynchronous assumes no timing bounds on message delivery.
	// The proposer advances to the next round as soon as it has 2f+1 parent certificates.
	// This provides optimal throughput but may have higher latency in practice.
	NetworkModelAsynchronous NetworkModel = iota

	// NetworkModelPartiallySynchronous assumes eventual synchrony.
	// The proposer may wait for leader blocks and leader votes before advancing.
	// This can provide better commit latency by ensuring leaders are included.
	NetworkModelPartiallySynchronous
)

// String returns a human-readable name for the network model.
func (m NetworkModel) String() string {
	switch m {
	case NetworkModelAsynchronous:
		return "asynchronous"
	case NetworkModelPartiallySynchronous:
		return "partially_synchronous"
	default:
		return "unknown"
	}
}

// LeaderSchedule determines which validator is leader for a given round.
type LeaderSchedule interface {
	// Leader returns the validator index of the leader for the given round.
	// This must be deterministic - all validators must agree on the leader.
	Leader(round uint64) uint16

	// IsLeader returns true if the given validator is the leader for the round.
	IsLeader(round uint64, validatorIndex uint16) bool
}

// RoundRobinLeaderSchedule implements a simple round-robin leader election.
// Each validator takes turns being the leader.
type RoundRobinLeaderSchedule struct {
	validatorCount int
}

// NewRoundRobinLeaderSchedule creates a new round-robin leader schedule.
func NewRoundRobinLeaderSchedule(validatorCount int) *RoundRobinLeaderSchedule {
	return &RoundRobinLeaderSchedule{
		validatorCount: validatorCount,
	}
}

// Leader returns the leader for the given round.
func (s *RoundRobinLeaderSchedule) Leader(round uint64) uint16 {
	return uint16(round % uint64(s.validatorCount))
}

// IsLeader returns true if the validator is the leader for the round.
func (s *RoundRobinLeaderSchedule) IsLeader(round uint64, validatorIndex uint16) bool {
	return s.Leader(round) == validatorIndex
}

// LeaderTracker tracks leader blocks and votes for partially synchronous mode.
// In this mode, we want to:
// 1. Wait for the leader's block before advancing to odd rounds
// 2. Wait for f+1 votes for the leader or 2f+1 non-votes before advancing to even rounds
//
// This follows the Sui Narwhal proposer logic for partially synchronous networks.
type LeaderTracker[H Hash] struct {
	mu sync.RWMutex

	schedule LeaderSchedule

	// lastLeader tracks the leader certificate we saw for each round
	lastLeader map[uint64]*Certificate[H]

	// currentRound is our current round
	currentRound uint64
}

// NewLeaderTracker creates a new LeaderTracker.
func NewLeaderTracker[H Hash](schedule LeaderSchedule) *LeaderTracker[H] {
	return &LeaderTracker[H]{
		schedule:   schedule,
		lastLeader: make(map[uint64]*Certificate[H]),
	}
}

// SetRound updates the current round.
func (lt *LeaderTracker[H]) SetRound(round uint64) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.currentRound = round
}

// RecordCertificate records a certificate and returns true if it's from the leader.
func (lt *LeaderTracker[H]) RecordCertificate(cert *Certificate[H]) bool {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	round := cert.Header.Round
	isLeader := lt.schedule.IsLeader(round, cert.Header.Author)

	if isLeader {
		lt.lastLeader[round] = cert
	}

	return isLeader
}

// GetLeaderForRound returns the leader certificate for a round, if we have it.
func (lt *LeaderTracker[H]) GetLeaderForRound(round uint64) *Certificate[H] {
	lt.mu.RLock()
	defer lt.mu.RUnlock()
	return lt.lastLeader[round]
}

// HasLeaderForRound returns true if we have the leader's certificate for the round.
func (lt *LeaderTracker[H]) HasLeaderForRound(round uint64) bool {
	lt.mu.RLock()
	defer lt.mu.RUnlock()
	return lt.lastLeader[round] != nil
}

// ShouldWaitForLeader returns true if we should wait for the leader block.
// This is used in partially synchronous mode on even rounds (round % 2 == 0).
func (lt *LeaderTracker[H]) ShouldWaitForLeader(round uint64) bool {
	// In partially synchronous mode, we wait for the leader on even rounds
	// (round 0, 2, 4, ...) before creating our block for round+1
	return round%2 == 0
}

// LeaderVoteStatus tracks votes for/against the leader block.
type LeaderVoteStatus struct {
	// VotesForLeader is the stake voting for the leader block
	VotesForLeader int

	// VotesNotForLeader is the stake not voting for the leader block
	// (voting for a different parent set that doesn't include the leader)
	VotesNotForLeader int

	// HasEnoughForLeaderVotes returns true if we have f+1 votes for the leader
	HasEnoughForLeaderVotes bool

	// HasEnoughNotForLeaderVotes returns true if we have 2f+1 votes not for leader
	HasEnoughNotForLeaderVotes bool
}

// CheckLeaderVotes analyzes parent certificates to determine leader vote status.
// This is used on odd rounds to decide if we have enough support to advance.
//
// Parameters:
// - parents: certificates from the previous round that we're using as parents
// - leaderDigest: the digest of the leader's certificate from the previous round (if known)
// - f: the maximum Byzantine faults tolerated
//
// Returns the vote status analysis.
func (lt *LeaderTracker[H]) CheckLeaderVotes(parents []*Certificate[H], leaderDigest *H, f int) LeaderVoteStatus {
	var status LeaderVoteStatus

	if leaderDigest == nil {
		// No leader block known, all votes are "not for leader"
		status.VotesNotForLeader = len(parents)
		status.HasEnoughNotForLeaderVotes = status.VotesNotForLeader >= 2*f+1
		return status
	}

	for _, parent := range parents {
		hasLeaderAsParent := false
		for _, parentRef := range parent.Header.Parents {
			if parentRef.Equals(*leaderDigest) {
				hasLeaderAsParent = true
				break
			}
		}

		if hasLeaderAsParent {
			status.VotesForLeader++
		} else {
			status.VotesNotForLeader++
		}
	}

	status.HasEnoughForLeaderVotes = status.VotesForLeader >= f+1
	status.HasEnoughNotForLeaderVotes = status.VotesNotForLeader >= 2*f+1

	return status
}

// GarbageCollect removes leader records for old rounds.
func (lt *LeaderTracker[H]) GarbageCollect(beforeRound uint64) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	for round := range lt.lastLeader {
		if round < beforeRound {
			delete(lt.lastLeader, round)
		}
	}
}

// Clear removes all leader records.
func (lt *LeaderTracker[H]) Clear() {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.lastLeader = make(map[uint64]*Certificate[H])
	lt.currentRound = 0
}
