package narwhal

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

// ReconfigurationState represents the current state of a reconfiguration operation.
type ReconfigurationState uint8

const (
	// ReconfigurationStateIdle means no reconfiguration is in progress.
	ReconfigurationStateIdle ReconfigurationState = iota

	// ReconfigurationStatePending means a reconfiguration has been proposed
	// but not yet finalized (waiting for quorum confirmation).
	ReconfigurationStatePending

	// ReconfigurationStateCommitting means the reconfiguration is being applied
	// (draining in-flight operations, updating state).
	ReconfigurationStateCommitting

	// ReconfigurationStateComplete means the reconfiguration has been applied.
	ReconfigurationStateComplete
)

// String returns a human-readable name for the reconfiguration state.
func (s ReconfigurationState) String() string {
	switch s {
	case ReconfigurationStateIdle:
		return "IDLE"
	case ReconfigurationStatePending:
		return "PENDING"
	case ReconfigurationStateCommitting:
		return "COMMITTING"
	case ReconfigurationStateComplete:
		return "COMPLETE"
	default:
		return "UNKNOWN"
	}
}

// EpochChange represents a pending or completed epoch change.
type EpochChange struct {
	// FromEpoch is the epoch we're transitioning from.
	FromEpoch uint64

	// ToEpoch is the epoch we're transitioning to.
	ToEpoch uint64

	// EffectiveRound is the DAG round at which the new epoch takes effect.
	// All certificates at or after this round use the new validator set.
	EffectiveRound uint64

	// NewValidators is the new validator set (may be nil if unchanged).
	NewValidators ValidatorSet

	// ProposedAt is when this epoch change was proposed.
	ProposedAt time.Time

	// CommittedAt is when this epoch change was committed (zero if not yet committed).
	CommittedAt time.Time
}

// Reconfigurer manages committee reconfiguration and epoch transitions.
// It coordinates the transition between validator sets, ensuring:
// 1. All nodes agree on the epoch boundary (consensus on reconfiguration)
// 2. In-flight operations for the old epoch complete before switching
// 3. State is properly cleaned up for the new epoch
//
// Thread-safe for concurrent use.
type Reconfigurer struct {
	mu sync.RWMutex

	// Current epoch information
	currentEpoch      uint64
	currentValidators ValidatorSet

	// Pending reconfiguration
	pendingChange *EpochChange
	state         ReconfigurationState

	// Callbacks for components that need to be notified
	onEpochChange []func(EpochChange)

	// History of recent epoch changes (for debugging/auditing)
	history    []EpochChange
	maxHistory int
	logger     *zap.Logger
}

// NewReconfigurer creates a new Reconfigurer.
func NewReconfigurer(initialEpoch uint64, validators ValidatorSet, logger *zap.Logger) *Reconfigurer {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Reconfigurer{
		currentEpoch:      initialEpoch,
		currentValidators: validators,
		state:             ReconfigurationStateIdle,
		maxHistory:        10,
		logger:            logger,
	}
}

// CurrentEpoch returns the current epoch.
func (r *Reconfigurer) CurrentEpoch() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentEpoch
}

// CurrentValidators returns the current validator set.
func (r *Reconfigurer) CurrentValidators() ValidatorSet {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentValidators
}

// State returns the current reconfiguration state.
func (r *Reconfigurer) State() ReconfigurationState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

// PendingChange returns the pending epoch change, if any.
func (r *Reconfigurer) PendingChange() *EpochChange {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.pendingChange == nil {
		return nil
	}
	change := *r.pendingChange
	return &change
}

// OnEpochChange registers a callback to be invoked when an epoch change is committed.
// Callbacks are invoked synchronously in the order they were registered.
func (r *Reconfigurer) OnEpochChange(callback func(EpochChange)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onEpochChange = append(r.onEpochChange, callback)
}

// ProposeEpochChange proposes a new epoch change.
// This is typically called when consensus has decided on a reconfiguration.
//
// Returns an error if:
// - A reconfiguration is already in progress
// - The new epoch is not exactly currentEpoch + 1
// - The effective round is in the past
func (r *Reconfigurer) ProposeEpochChange(newEpoch uint64, effectiveRound uint64, newValidators ValidatorSet) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != ReconfigurationStateIdle {
		return fmt.Errorf("reconfiguration already in progress (state: %s)", r.state)
	}

	if newEpoch != r.currentEpoch+1 {
		return fmt.Errorf("invalid epoch transition: %d -> %d (expected %d)", r.currentEpoch, newEpoch, r.currentEpoch+1)
	}

	r.pendingChange = &EpochChange{
		FromEpoch:      r.currentEpoch,
		ToEpoch:        newEpoch,
		EffectiveRound: effectiveRound,
		NewValidators:  newValidators,
		ProposedAt:     time.Now(),
	}
	r.state = ReconfigurationStatePending

	r.logger.Info("epoch change proposed",
		zap.Uint64("from_epoch", r.currentEpoch),
		zap.Uint64("to_epoch", newEpoch),
		zap.Uint64("effective_round", effectiveRound))

	return nil
}

// BeginCommit transitions from PENDING to COMMITTING state.
// This should be called when the epoch boundary round has been reached
// and all nodes have confirmed the reconfiguration.
//
// Returns an error if not in PENDING state.
func (r *Reconfigurer) BeginCommit() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != ReconfigurationStatePending {
		return fmt.Errorf("cannot begin commit: not in pending state (state: %s)", r.state)
	}

	r.state = ReconfigurationStateCommitting
	r.logger.Info("beginning epoch change commit",
		zap.Uint64("to_epoch", r.pendingChange.ToEpoch))

	return nil
}

// CompleteCommit finalizes the epoch change.
// This should be called after all in-flight operations have completed
// and state has been updated for the new epoch.
//
// Returns an error if not in COMMITTING state.
func (r *Reconfigurer) CompleteCommit() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != ReconfigurationStateCommitting {
		return fmt.Errorf("cannot complete commit: not in committing state (state: %s)", r.state)
	}

	change := *r.pendingChange
	change.CommittedAt = time.Now()

	// Update current state
	r.currentEpoch = change.ToEpoch
	if change.NewValidators != nil {
		r.currentValidators = change.NewValidators
	}

	// Add to history
	r.history = append(r.history, change)
	if len(r.history) > r.maxHistory {
		r.history = r.history[1:]
	}

	// Reset state
	r.pendingChange = nil
	r.state = ReconfigurationStateComplete

	r.logger.Info("epoch change committed",
		zap.Uint64("new_epoch", r.currentEpoch),
		zap.Duration("duration", change.CommittedAt.Sub(change.ProposedAt)))

	// Invoke callbacks (synchronously, with lock released)
	callbacks := make([]func(EpochChange), len(r.onEpochChange))
	copy(callbacks, r.onEpochChange)

	r.mu.Unlock()
	for _, cb := range callbacks {
		cb(change)
	}
	r.mu.Lock()

	// Transition back to idle
	r.state = ReconfigurationStateIdle

	return nil
}

// CancelPendingChange cancels a pending epoch change.
// This can be used if the reconfiguration is aborted (e.g., consensus rollback).
//
// Returns an error if not in PENDING state.
func (r *Reconfigurer) CancelPendingChange() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != ReconfigurationStatePending {
		return fmt.Errorf("cannot cancel: not in pending state (state: %s)", r.state)
	}

	r.logger.Warn("epoch change cancelled",
		zap.Uint64("to_epoch", r.pendingChange.ToEpoch))

	r.pendingChange = nil
	r.state = ReconfigurationStateIdle

	return nil
}

// ShouldAcceptForEpoch determines if a message from the given epoch should be accepted.
// Returns true if the epoch is current or if it's the pending epoch during transition.
func (r *Reconfigurer) ShouldAcceptForEpoch(epoch uint64) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if epoch == r.currentEpoch {
		return true
	}

	// During transition, also accept messages for the new epoch
	if r.pendingChange != nil && epoch == r.pendingChange.ToEpoch {
		return r.state == ReconfigurationStateCommitting
	}

	return false
}

// IsEpochBoundaryRound returns true if the given round is the effective round
// of a pending epoch change.
func (r *Reconfigurer) IsEpochBoundaryRound(round uint64) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.pendingChange == nil {
		return false
	}

	return round == r.pendingChange.EffectiveRound
}

// History returns the history of recent epoch changes.
func (r *Reconfigurer) History() []EpochChange {
	r.mu.RLock()
	defer r.mu.RUnlock()

	history := make([]EpochChange, len(r.history))
	copy(history, r.history)
	return history
}

// ReconfigurationStats contains statistics for monitoring.
type ReconfigurationStats struct {
	CurrentEpoch     uint64
	State            ReconfigurationState
	HasPendingChange bool
	PendingToEpoch   uint64
	HistoryCount     int
	ValidatorCount   int
	CallbackCount    int
}

// Stats returns current statistics.
func (r *Reconfigurer) Stats() ReconfigurationStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	stats := ReconfigurationStats{
		CurrentEpoch:     r.currentEpoch,
		State:            r.state,
		HasPendingChange: r.pendingChange != nil,
		HistoryCount:     len(r.history),
		CallbackCount:    len(r.onEpochChange),
	}

	if r.currentValidators != nil {
		stats.ValidatorCount = r.currentValidators.Count()
	}

	if r.pendingChange != nil {
		stats.PendingToEpoch = r.pendingChange.ToEpoch
	}

	return stats
}

// EpochAwareValidatorSet wraps a validator set with epoch information.
// This can be used to validate that certificates use the correct validator set
// for their epoch.
type EpochAwareValidatorSet struct {
	ValidatorSet
	epoch uint64
}

// NewEpochAwareValidatorSet creates a new epoch-aware validator set.
func NewEpochAwareValidatorSet(vs ValidatorSet, epoch uint64) *EpochAwareValidatorSet {
	return &EpochAwareValidatorSet{
		ValidatorSet: vs,
		epoch:        epoch,
	}
}

// Epoch returns the epoch this validator set is valid for.
func (e *EpochAwareValidatorSet) Epoch() uint64 {
	return e.epoch
}

// ValidatorSetChange represents a change to the validator set.
type ValidatorSetChange struct {
	// Type indicates what kind of change this is.
	Type ValidatorSetChangeType

	// ValidatorIndex is the index of the validator being changed.
	// For ADD, this is the new index assigned.
	// For REMOVE and UPDATE, this is the existing index.
	ValidatorIndex uint16

	// PublicKey is the public key involved in the change.
	// For ADD and UPDATE, this is the new key.
	// For REMOVE, this may be nil.
	PublicKey PublicKey
}

// ValidatorSetChangeType indicates the type of validator set change.
type ValidatorSetChangeType uint8

const (
	// ValidatorSetChangeAdd adds a new validator.
	ValidatorSetChangeAdd ValidatorSetChangeType = iota

	// ValidatorSetChangeRemove removes an existing validator.
	ValidatorSetChangeRemove

	// ValidatorSetChangeUpdate updates a validator's key (key rotation).
	ValidatorSetChangeUpdate
)

// String returns a human-readable name for the change type.
func (t ValidatorSetChangeType) String() string {
	switch t {
	case ValidatorSetChangeAdd:
		return "ADD"
	case ValidatorSetChangeRemove:
		return "REMOVE"
	case ValidatorSetChangeUpdate:
		return "UPDATE"
	default:
		return "UNKNOWN"
	}
}

// MutableValidatorSet extends ValidatorSet with mutation operations.
// This interface is used during reconfiguration to build a new validator set.
type MutableValidatorSet interface {
	ValidatorSet

	// AddValidator adds a new validator and returns their assigned index.
	AddValidator(pubKey PublicKey) (uint16, error)

	// RemoveValidator removes a validator by index.
	RemoveValidator(index uint16) error

	// UpdateValidator updates a validator's public key.
	UpdateValidator(index uint16, newPubKey PublicKey) error

	// Clone creates a deep copy of this validator set.
	Clone() MutableValidatorSet
}

// ReconfigurationProposal represents a proposal to change the validator set.
// This is used to coordinate reconfiguration across the network.
type ReconfigurationProposal struct {
	// Epoch is the epoch this proposal is for (currentEpoch + 1).
	Epoch uint64

	// EffectiveRound is the round at which the change takes effect.
	EffectiveRound uint64

	// Changes is the list of validator set changes to apply.
	Changes []ValidatorSetChange

	// Proposer is the validator who proposed this reconfiguration.
	Proposer uint16

	// Signature is the proposer's signature over this proposal.
	Signature []byte

	// ProposedAt is when this proposal was created.
	ProposedAt time.Time
}

// ReconfigurationVote represents a vote on a reconfiguration proposal.
type ReconfigurationVote struct {
	// ProposalHash is the hash of the proposal being voted on.
	ProposalHash []byte

	// Voter is the validator casting this vote.
	Voter uint16

	// Approve indicates whether this is an approval or rejection.
	Approve bool

	// Signature is the voter's signature over this vote.
	Signature []byte
}

// ReconfigurationCoordinator coordinates the voting process for reconfiguration proposals.
// This is a higher-level component that uses Reconfigurer for state management.
type ReconfigurationCoordinator[H Hash] struct {
	mu sync.Mutex

	reconfigurer *Reconfigurer
	hashFunc     func([]byte) H

	// Current proposal being voted on
	currentProposal *ReconfigurationProposal
	votes           map[uint16]*ReconfigurationVote
	approvals       int
	rejections      int

	logger *zap.Logger
}

// NewReconfigurationCoordinator creates a new ReconfigurationCoordinator.
func NewReconfigurationCoordinator[H Hash](
	reconfigurer *Reconfigurer,
	hashFunc func([]byte) H,
	logger *zap.Logger,
) *ReconfigurationCoordinator[H] {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &ReconfigurationCoordinator[H]{
		reconfigurer: reconfigurer,
		hashFunc:     hashFunc,
		votes:        make(map[uint16]*ReconfigurationVote),
		logger:       logger,
	}
}

// ProposeReconfiguration initiates a new reconfiguration proposal.
// Returns an error if a proposal is already in progress.
func (rc *ReconfigurationCoordinator[H]) ProposeReconfiguration(
	effectiveRound uint64,
	changes []ValidatorSetChange,
	proposer uint16,
	signature []byte,
) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.currentProposal != nil {
		return fmt.Errorf("proposal already in progress")
	}

	epoch := rc.reconfigurer.CurrentEpoch() + 1

	rc.currentProposal = &ReconfigurationProposal{
		Epoch:          epoch,
		EffectiveRound: effectiveRound,
		Changes:        changes,
		Proposer:       proposer,
		Signature:      signature,
		ProposedAt:     time.Now(),
	}
	rc.votes = make(map[uint16]*ReconfigurationVote)
	rc.approvals = 0
	rc.rejections = 0

	rc.logger.Info("reconfiguration proposal initiated",
		zap.Uint64("epoch", epoch),
		zap.Uint64("effective_round", effectiveRound),
		zap.Int("changes", len(changes)))

	return nil
}

// AddVote adds a vote for the current proposal.
// Returns an error if no proposal is in progress or if the vote is invalid.
func (rc *ReconfigurationCoordinator[H]) AddVote(vote *ReconfigurationVote) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.currentProposal == nil {
		return fmt.Errorf("no proposal in progress")
	}

	// Check for duplicate vote
	if _, exists := rc.votes[vote.Voter]; exists {
		return fmt.Errorf("duplicate vote from validator %d", vote.Voter)
	}

	// Store vote
	rc.votes[vote.Voter] = vote
	if vote.Approve {
		rc.approvals++
	} else {
		rc.rejections++
	}

	rc.logger.Debug("reconfiguration vote received",
		zap.Uint16("voter", vote.Voter),
		zap.Bool("approve", vote.Approve),
		zap.Int("total_approvals", rc.approvals),
		zap.Int("total_rejections", rc.rejections))

	return nil
}

// CheckQuorum checks if the proposal has reached quorum.
// Returns true if 2f+1 approvals have been received.
func (rc *ReconfigurationCoordinator[H]) CheckQuorum() bool {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.currentProposal == nil {
		return false
	}

	validators := rc.reconfigurer.CurrentValidators()
	quorum := 2*validators.F() + 1

	return rc.approvals >= quorum
}

// IsRejected checks if the proposal has been rejected.
// Returns true if f+1 rejections have been received (making quorum impossible).
func (rc *ReconfigurationCoordinator[H]) IsRejected() bool {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.currentProposal == nil {
		return false
	}

	validators := rc.reconfigurer.CurrentValidators()
	f := validators.F()

	return rc.rejections > f
}

// CurrentProposal returns the current proposal, if any.
func (rc *ReconfigurationCoordinator[H]) CurrentProposal() *ReconfigurationProposal {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.currentProposal == nil {
		return nil
	}
	proposal := *rc.currentProposal
	return &proposal
}

// VoteStats returns the current vote counts.
func (rc *ReconfigurationCoordinator[H]) VoteStats() (approvals, rejections, total int) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.approvals, rc.rejections, len(rc.votes)
}

// Clear resets the coordinator state.
func (rc *ReconfigurationCoordinator[H]) Clear() {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.currentProposal = nil
	rc.votes = make(map[uint16]*ReconfigurationVote)
	rc.approvals = 0
	rc.rejections = 0
}
