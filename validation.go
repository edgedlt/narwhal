package narwhal

import (
	"fmt"
	"time"
)

// Security limits to prevent DoS attacks.
// These can be overridden via ValidationConfig.
const (
	// DefaultMaxBatchRefs is the maximum number of batch references in a header.
	// Prevents memory exhaustion from headers with too many refs.
	DefaultMaxBatchRefs = 1000

	// DefaultMaxParents is the maximum number of parent certificates in a header.
	// Limited by validator count in practice, but enforced for safety.
	DefaultMaxParents = 200

	// DefaultMaxSignatures is the maximum number of signatures in a certificate.
	// Should not exceed validator count.
	DefaultMaxSignatures = 200

	// DefaultMaxTransactionsPerBatch is the maximum transactions in a single batch.
	// Prevents memory exhaustion from oversized batches.
	DefaultMaxTransactionsPerBatch = 10000

	// DefaultMaxTransactionSize is the maximum size of a single transaction in bytes.
	DefaultMaxTransactionSize = 1024 * 1024 // 1 MB

	// DefaultMaxBatchSize is the maximum size of a batch in bytes.
	DefaultMaxBatchSize = 100 * 1024 * 1024 // 100 MB

	// DefaultMaxHeaderSize is the maximum size of a header in bytes.
	DefaultMaxHeaderSize = 1024 * 1024 // 1 MB

	// DefaultMaxCertificateSize is the maximum size of a certificate in bytes.
	DefaultMaxCertificateSize = 10 * 1024 * 1024 // 10 MB

	// DefaultMaxRoundSkip is the maximum rounds a header can be ahead of current.
	DefaultMaxRoundSkip = 100

	// DefaultMaxTimestampDrift is the maximum time a header timestamp can be in the future.
	DefaultMaxTimestampDrift = 60 * time.Second
)

// ValidationConfig configures validation limits.
type ValidationConfig struct {
	// MaxBatchRefs is the maximum number of batch references per header.
	MaxBatchRefs int

	// MaxParents is the maximum number of parent certificates per header.
	MaxParents int

	// MaxSignatures is the maximum number of signatures per certificate.
	MaxSignatures int

	// MaxTransactionsPerBatch is the maximum transactions per batch.
	MaxTransactionsPerBatch int

	// MaxTransactionSize is the maximum size of a single transaction in bytes.
	MaxTransactionSize int

	// MaxBatchSize is the maximum size of a batch in bytes.
	MaxBatchSize int

	// MaxHeaderSize is the maximum size of a header in bytes.
	MaxHeaderSize int

	// MaxCertificateSize is the maximum size of a certificate in bytes.
	MaxCertificateSize int

	// MaxRoundSkip is the maximum rounds a header can be ahead.
	MaxRoundSkip uint64

	// MaxTimestampDrift is the maximum time a header can be in the future.
	MaxTimestampDrift time.Duration
}

// DefaultValidationConfig returns a ValidationConfig with default limits.
func DefaultValidationConfig() ValidationConfig {
	return ValidationConfig{
		MaxBatchRefs:            DefaultMaxBatchRefs,
		MaxParents:              DefaultMaxParents,
		MaxSignatures:           DefaultMaxSignatures,
		MaxTransactionsPerBatch: DefaultMaxTransactionsPerBatch,
		MaxTransactionSize:      DefaultMaxTransactionSize,
		MaxBatchSize:            DefaultMaxBatchSize,
		MaxHeaderSize:           DefaultMaxHeaderSize,
		MaxCertificateSize:      DefaultMaxCertificateSize,
		MaxRoundSkip:            DefaultMaxRoundSkip,
		MaxTimestampDrift:       DefaultMaxTimestampDrift,
	}
}

// Validator provides comprehensive input validation for Narwhal messages.
// All methods are safe for concurrent use.
type Validator[H Hash, T Transaction[H]] struct {
	cfg        ValidationConfig
	validators ValidatorSet
	hashFunc   func([]byte) H
}

// NewValidator creates a new Validator with the given configuration.
func NewValidator[H Hash, T Transaction[H]](
	cfg ValidationConfig,
	validators ValidatorSet,
	hashFunc func([]byte) H,
) *Validator[H, T] {
	return &Validator[H, T]{
		cfg:        cfg,
		validators: validators,
		hashFunc:   hashFunc,
	}
}

// ValidateHeader validates a header before voting or accepting.
func (v *Validator[H, T]) ValidateHeader(header *Header[H], currentRound uint64) error {
	if header == nil {
		return fmt.Errorf("header is nil")
	}

	// Check author is valid
	if !v.validators.Contains(header.Author) {
		return fmt.Errorf("invalid author: %d (not in validator set)", header.Author)
	}

	// Check digest
	if !header.VerifyDigest(v.hashFunc) {
		return fmt.Errorf("header digest mismatch")
	}

	// Check round is not too far ahead
	if header.Round > currentRound+v.cfg.MaxRoundSkip {
		return fmt.Errorf("header round %d too far ahead (current: %d, max skip: %d)",
			header.Round, currentRound, v.cfg.MaxRoundSkip)
	}

	// Check timestamp is reasonable
	now := time.Now()
	headerTime := time.UnixMilli(int64(header.Timestamp))
	if headerTime.After(now.Add(v.cfg.MaxTimestampDrift)) {
		return fmt.Errorf("header timestamp too far in future: %v (max drift: %v)",
			headerTime.Sub(now), v.cfg.MaxTimestampDrift)
	}

	// Check batch refs count
	if len(header.BatchRefs) > v.cfg.MaxBatchRefs {
		return fmt.Errorf("too many batch refs: %d (max: %d)",
			len(header.BatchRefs), v.cfg.MaxBatchRefs)
	}

	// Check parents count
	if len(header.Parents) > v.cfg.MaxParents {
		return fmt.Errorf("too many parents: %d (max: %d)",
			len(header.Parents), v.cfg.MaxParents)
	}

	// Check minimum parents for non-genesis rounds
	if header.Round > 0 {
		minParents := 2*v.validators.F() + 1
		if len(header.Parents) < minParents {
			return fmt.Errorf("insufficient parents: %d (need at least %d for 2f+1)",
				len(header.Parents), minParents)
		}
	}

	// Check for duplicate batch refs
	// Use full hash bytes for comparison (String() may be truncated)
	seenBatchRefs := make(map[string]struct{})
	for _, ref := range header.BatchRefs {
		key := string(ref.Bytes())
		if _, seen := seenBatchRefs[key]; seen {
			return fmt.Errorf("duplicate batch ref: %s", ref.String())
		}
		seenBatchRefs[key] = struct{}{}
	}

	// Check for duplicate parents
	seenParents := make(map[string]struct{})
	for _, parent := range header.Parents {
		key := string(parent.Bytes())
		if _, seen := seenParents[key]; seen {
			return fmt.Errorf("duplicate parent: %s", parent.String())
		}
		seenParents[key] = struct{}{}
	}

	return nil
}

// ValidateCertificate performs comprehensive validation on a certificate.
//
// Checks performed:
//   - Certificate is not nil
//   - Header is valid (via ValidateHeader)
//   - Has quorum of signatures (2f+1)
//   - Signature count matches bitmap
//   - Number of signatures is within limits
//   - All signatures are valid (if verifySignatures is true)
func (v *Validator[H, T]) ValidateCertificate(cert *Certificate[H], currentRound uint64, verifySignatures bool) error {
	if cert == nil {
		return fmt.Errorf("certificate is nil")
	}

	if cert.Header == nil {
		return fmt.Errorf("certificate has no header")
	}

	// Validate header
	if err := v.ValidateHeader(cert.Header, currentRound); err != nil {
		return fmt.Errorf("invalid certificate header: %w", err)
	}

	// Check signature count limit
	if len(cert.Signatures) > v.cfg.MaxSignatures {
		return fmt.Errorf("too many signatures: %d (max: %d)",
			len(cert.Signatures), v.cfg.MaxSignatures)
	}

	// Validate quorum and signatures
	if verifySignatures {
		if err := cert.Validate(v.validators); err != nil {
			return fmt.Errorf("certificate validation failed: %w", err)
		}
	} else {
		// Just check quorum without signature verification
		signerCount := cert.SignerCount()
		quorum := 2*v.validators.F() + 1
		if signerCount < quorum {
			return fmt.Errorf("insufficient signatures: %d (need %d)", signerCount, quorum)
		}
		if len(cert.Signatures) != signerCount {
			return fmt.Errorf("signature count mismatch: %d signatures, %d signers",
				len(cert.Signatures), signerCount)
		}
	}

	return nil
}

// ValidateBatch performs comprehensive validation on a batch.
//
// Checks performed:
//   - Batch is not nil
//   - ValidatorID is valid
//   - Digest is correct
//   - Transaction count is within limits
//   - Individual transaction sizes are within limits (if checkTxSize is true)
func (v *Validator[H, T]) ValidateBatch(batch *Batch[H, T], checkTxSize bool) error {
	if batch == nil {
		return fmt.Errorf("batch is nil")
	}

	// Check validator is valid
	if !v.validators.Contains(batch.ValidatorID) {
		return fmt.Errorf("invalid validator: %d", batch.ValidatorID)
	}

	// Check digest
	if err := batch.Verify(v.hashFunc); err != nil {
		return fmt.Errorf("batch verification failed: %w", err)
	}

	// Check transaction count
	if len(batch.Transactions) > v.cfg.MaxTransactionsPerBatch {
		return fmt.Errorf("too many transactions: %d (max: %d)",
			len(batch.Transactions), v.cfg.MaxTransactionsPerBatch)
	}

	// Optionally check transaction sizes
	if checkTxSize {
		for i, tx := range batch.Transactions {
			txBytes := tx.Bytes()
			if len(txBytes) > v.cfg.MaxTransactionSize {
				return fmt.Errorf("transaction %d too large: %d bytes (max: %d)",
					i, len(txBytes), v.cfg.MaxTransactionSize)
			}
		}
	}

	return nil
}

// ValidateVote performs validation on a header vote.
//
// Checks performed:
//   - Vote is not nil
//   - Voter is a valid validator
//   - Signature is valid (if verifySignature is true)
func (v *Validator[H, T]) ValidateVote(vote *HeaderVote[H], verifySignature bool) error {
	if vote == nil {
		return fmt.Errorf("vote is nil")
	}

	// Check voter is valid
	if !v.validators.Contains(vote.ValidatorIndex) {
		return fmt.Errorf("invalid voter: %d", vote.ValidatorIndex)
	}

	// Verify signature if requested
	if verifySignature {
		pubKey, err := v.validators.GetByIndex(vote.ValidatorIndex)
		if err != nil {
			return fmt.Errorf("failed to get voter public key: %w", err)
		}
		if !pubKey.Verify(vote.HeaderDigest.Bytes(), vote.Signature) {
			return fmt.Errorf("invalid vote signature from validator %d", vote.ValidatorIndex)
		}
	}

	return nil
}

// ValidateMessageSize checks if a raw message is within size limits.
func (v *Validator[H, T]) ValidateMessageSize(msgType MessageType, data []byte) error {
	var limit int
	switch msgType {
	case MessageBatch:
		limit = v.cfg.MaxBatchSize
	case MessageHeader:
		limit = v.cfg.MaxHeaderSize
	case MessageCertificate:
		limit = v.cfg.MaxCertificateSize
	default:
		// Votes and requests are small, use header limit as default
		limit = v.cfg.MaxHeaderSize
	}

	if len(data) > limit {
		return fmt.Errorf("%s message too large: %d bytes (max: %d)",
			msgType, len(data), limit)
	}

	return nil
}

// ValidationError wraps a validation error with additional context.
type ValidationError struct {
	Type    string // "header", "certificate", "batch", "vote"
	Field   string // Specific field that failed validation
	Message string // Human-readable error message
	Cause   error  // Underlying error if any
}

func (e *ValidationError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("%s validation failed: %s: %s", e.Type, e.Field, e.Message)
	}
	return fmt.Sprintf("%s validation failed: %s", e.Type, e.Message)
}

func (e *ValidationError) Unwrap() error {
	return e.Cause
}

// IsValidationError returns true if err is a ValidationError.
func IsValidationError(err error) bool {
	_, ok := err.(*ValidationError)
	return ok
}
