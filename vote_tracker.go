package narwhal

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

// VoteRecord tracks a vote we sent for a specific (author, round) pair.
// Used to prevent double-voting on equivocating headers from the same author.
type VoteRecord[H Hash] struct {
	// Round is the round we voted for.
	Round uint64

	// Epoch is the epoch we voted for (for cross-epoch safety).
	Epoch uint64

	// HeaderDigest is the digest of the header we voted for.
	HeaderDigest H

	// VotedAt is when we cast this vote.
	VotedAt time.Time
}

// VoteTracker prevents double-voting on equivocating headers. Thread-safe.
type VoteTracker[H Hash] struct {
	mu sync.RWMutex

	// votes maps author -> VoteRecord for their highest voted round
	votes map[uint16]*VoteRecord[H]

	// gcRound is the minimum round we track (older records are garbage collected)
	gcRound uint64

	// currentEpoch is the current epoch (records from older epochs are invalid)
	currentEpoch uint64

	logger *zap.Logger
}

// NewVoteTracker creates a new VoteTracker.
func NewVoteTracker[H Hash](logger *zap.Logger) *VoteTracker[H] {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &VoteTracker[H]{
		votes:  make(map[uint16]*VoteRecord[H]),
		logger: logger,
	}
}

// VoteDecision represents the decision about whether to vote for a header.
type VoteDecision uint8

const (
	// VoteDecisionAllow means we should vote for this header.
	VoteDecisionAllow VoteDecision = iota

	// VoteDecisionSkipOldRound means we've already voted for a higher round from this author.
	VoteDecisionSkipOldRound

	// VoteDecisionSkipEquivocation means we've already voted for a different header
	// from this author at the same round (equivocation detected).
	VoteDecisionSkipEquivocation

	// VoteDecisionSkipOldEpoch means the header is from an old epoch.
	VoteDecisionSkipOldEpoch

	// VoteDecisionSkipDuplicate means we've already voted for this exact header.
	VoteDecisionSkipDuplicate
)

// String returns a human-readable description of the vote decision.
func (d VoteDecision) String() string {
	switch d {
	case VoteDecisionAllow:
		return "ALLOW"
	case VoteDecisionSkipOldRound:
		return "SKIP_OLD_ROUND"
	case VoteDecisionSkipEquivocation:
		return "SKIP_EQUIVOCATION"
	case VoteDecisionSkipOldEpoch:
		return "SKIP_OLD_EPOCH"
	case VoteDecisionSkipDuplicate:
		return "SKIP_DUPLICATE"
	default:
		return "UNKNOWN"
	}
}

// ShouldVote determines whether we should vote for a header.
// Returns the decision and, if equivocation is detected, the digest of the header we already voted for.
//
// The logic follows Sui's Narwhal implementation:
// 1. If header.Round < lastVotedRound for this author: skip (old round)
// 2. If header.Round == lastVotedRound and header.Digest == lastVotedDigest: skip (duplicate)
// 3. If header.Round == lastVotedRound and header.Digest != lastVotedDigest: skip (equivocation)
// 4. If header.Round > lastVotedRound: allow (new round)
// 5. If no previous vote for this author: allow
func (vt *VoteTracker[H]) ShouldVote(author uint16, round, epoch uint64, headerDigest H) (VoteDecision, *H) {
	vt.mu.RLock()
	defer vt.mu.RUnlock()

	// Check epoch
	if epoch < vt.currentEpoch {
		return VoteDecisionSkipOldEpoch, nil
	}

	// Check if we have a previous vote for this author
	record, exists := vt.votes[author]
	if !exists {
		return VoteDecisionAllow, nil
	}

	// Check if the record is from the current epoch
	if record.Epoch < vt.currentEpoch {
		// Old epoch record, treat as if no vote exists
		return VoteDecisionAllow, nil
	}

	// Compare rounds
	if round < record.Round {
		// We've already voted for a higher round
		return VoteDecisionSkipOldRound, nil
	}

	if round == record.Round {
		// Same round - check if same header or equivocation
		if record.HeaderDigest.Equals(headerDigest) {
			// Same header, we've already voted for it
			return VoteDecisionSkipDuplicate, nil
		}
		// Different header at same round = equivocation
		existingDigest := record.HeaderDigest
		return VoteDecisionSkipEquivocation, &existingDigest
	}

	// round > record.Round - new round, allow vote
	return VoteDecisionAllow, nil
}

// RecordVote records that we voted for a header.
// This should be called AFTER successfully sending the vote.
func (vt *VoteTracker[H]) RecordVote(author uint16, round, epoch uint64, headerDigest H) {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	// Only record if this is for current or future epoch
	if epoch < vt.currentEpoch {
		return
	}

	// Only update if this is a newer round
	existing, exists := vt.votes[author]
	if exists && existing.Epoch == epoch && existing.Round >= round {
		return
	}

	vt.votes[author] = &VoteRecord[H]{
		Round:        round,
		Epoch:        epoch,
		HeaderDigest: headerDigest,
		VotedAt:      time.Now(),
	}

	vt.logger.Debug("recorded vote",
		zap.Uint16("author", author),
		zap.Uint64("round", round),
		zap.Uint64("epoch", epoch),
		zap.String("digest", headerDigest.String()))
}

// SetEpoch updates the current epoch and clears records from old epochs.
func (vt *VoteTracker[H]) SetEpoch(epoch uint64) {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	if epoch <= vt.currentEpoch {
		return
	}

	// Clear all vote records from old epochs
	for author, record := range vt.votes {
		if record.Epoch < epoch {
			delete(vt.votes, author)
		}
	}

	vt.currentEpoch = epoch
	vt.logger.Info("vote tracker epoch updated",
		zap.Uint64("epoch", epoch),
		zap.Int("remaining_records", len(vt.votes)))
}

// GarbageCollect removes vote records for rounds below gcRound.
// Called periodically to prevent unbounded memory growth.
func (vt *VoteTracker[H]) GarbageCollect(gcRound uint64) {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	if gcRound <= vt.gcRound {
		return
	}

	removed := 0
	for author, record := range vt.votes {
		if record.Round < gcRound {
			delete(vt.votes, author)
			removed++
		}
	}

	vt.gcRound = gcRound
	if removed > 0 {
		vt.logger.Debug("vote tracker garbage collected",
			zap.Uint64("gc_round", gcRound),
			zap.Int("removed", removed))
	}
}

// Clear removes all vote records. Used during reconfiguration.
func (vt *VoteTracker[H]) Clear() {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	vt.votes = make(map[uint16]*VoteRecord[H])
	vt.gcRound = 0
}

// VoteTrackerStats contains statistics for monitoring.
type VoteTrackerStats struct {
	TrackedAuthors int
	CurrentEpoch   uint64
	GCRound        uint64
}

// Stats returns current statistics.
func (vt *VoteTracker[H]) Stats() VoteTrackerStats {
	vt.mu.RLock()
	defer vt.mu.RUnlock()

	return VoteTrackerStats{
		TrackedAuthors: len(vt.votes),
		CurrentEpoch:   vt.currentEpoch,
		GCRound:        vt.gcRound,
	}
}
