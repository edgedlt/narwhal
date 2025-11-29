package narwhal

import (
	"encoding/binary"
	"fmt"
	"math/bits"
)

// Certificate methods for serialization and validation

// Validate verifies that the certificate has a valid quorum of signatures.
func (c *Certificate[H]) Validate(validators ValidatorSet) error {
	if c.Header == nil {
		return fmt.Errorf("certificate has no header")
	}

	// Count signers from bitmap
	signerCount := bits.OnesCount64(c.SignerBitmap)

	// Check quorum
	quorum := 2*validators.F() + 1
	if signerCount < quorum {
		return fmt.Errorf("insufficient signatures: got %d, need %d", signerCount, quorum)
	}

	// Verify signature count matches bitmap
	if len(c.Signatures) != signerCount {
		return fmt.Errorf("signature count mismatch: %d signatures, %d bits set", len(c.Signatures), signerCount)
	}

	// Verify each signature
	sigIdx := 0
	for i := 0; i < validators.Count(); i++ {
		if c.SignerBitmap&(1<<uint(i)) != 0 {
			pubKey, err := validators.GetByIndex(uint16(i))
			if err != nil {
				return fmt.Errorf("failed to get public key for validator %d: %w", i, err)
			}
			if !pubKey.Verify(c.Header.Digest.Bytes(), c.Signatures[sigIdx]) {
				return fmt.Errorf("invalid signature from validator %d", i)
			}
			sigIdx++
		}
	}

	return nil
}

// ValidateWithCrypto verifies that the certificate has a valid quorum of signatures
// using the provided CryptoProvider for optimized batch/parallel verification.
// If cache is provided, already-verified signatures are skipped.
func (c *Certificate[H]) ValidateWithCrypto(validators ValidatorSet, crypto CryptoProvider, cache *SignatureCache) error {
	if c.Header == nil {
		return fmt.Errorf("certificate has no header")
	}

	// Count signers from bitmap
	signerCount := bits.OnesCount64(c.SignerBitmap)

	// Check quorum
	quorum := 2*validators.F() + 1
	if signerCount < quorum {
		return fmt.Errorf("insufficient signatures: got %d, need %d", signerCount, quorum)
	}

	// Verify signature count matches bitmap
	if len(c.Signatures) != signerCount {
		return fmt.Errorf("signature count mismatch: %d signatures, %d bits set", len(c.Signatures), signerCount)
	}

	// Create batch verifier
	verifier := crypto.NewBatchVerifier(validators)
	defer verifier.Reset()

	// Optionally wrap with caching
	var v BatchVerifier = verifier
	if cache != nil {
		v = NewCachedBatchVerifier(verifier, cache, validators)
	}

	// Add all signatures to the batch
	message := c.Header.Digest.Bytes()
	sigIdx := 0
	for i := 0; i < validators.Count(); i++ {
		if c.SignerBitmap&(1<<uint(i)) != 0 {
			v.Add(message, c.Signatures[sigIdx], uint16(i))
			sigIdx++
		}
	}

	// Verify all signatures in batch
	return v.Verify()
}

// Digest returns the certificate's digest (same as header digest).
func (c *Certificate[H]) Digest() H {
	return c.Header.Digest
}

// Round returns the certificate's round.
func (c *Certificate[H]) Round() uint64 {
	return c.Header.Round
}

// Author returns the certificate's author.
func (c *Certificate[H]) Author() uint16 {
	return c.Header.Author
}

// SignerCount returns the number of validators who signed this certificate.
func (c *Certificate[H]) SignerCount() int {
	return bits.OnesCount64(c.SignerBitmap)
}

// HasSigner returns true if the given validator signed this certificate.
func (c *Certificate[H]) HasSigner(validatorIndex uint16) bool {
	return c.SignerBitmap&(1<<uint(validatorIndex)) != 0
}

// Bytes serializes the certificate to bytes.
// Format: [Header bytes][SignerBitmap:8][SigCount:4][Sig0Len:2][Sig0][Sig1Len:2][Sig1]...
func (c *Certificate[H]) Bytes() []byte {
	headerBytes := c.Header.Bytes()

	// Calculate total size
	size := len(headerBytes) + 8 + 4
	for _, sig := range c.Signatures {
		size += 2 + len(sig)
	}

	buf := make([]byte, size)
	offset := 0

	copy(buf[offset:], headerBytes)
	offset += len(headerBytes)

	binary.BigEndian.PutUint64(buf[offset:], c.SignerBitmap)
	offset += 8

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(c.Signatures)))
	offset += 4

	for _, sig := range c.Signatures {
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(sig)))
		offset += 2
		copy(buf[offset:], sig)
		offset += len(sig)
	}

	return buf
}

// CertificateFromBytes deserializes a certificate from bytes.
func CertificateFromBytes[H Hash](
	data []byte,
	hashFromBytes func([]byte) (H, error),
) (*Certificate[H], error) {
	if len(data) < 42 { // Minimum: header(30) + bitmap(8) + sigcount(4)
		return nil, fmt.Errorf("data too short for certificate: %d bytes", len(data))
	}

	// Parse header first - we need to know where it ends
	header, err := HeaderFromBytes(data, hashFromBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse certificate header: %w", err)
	}

	// Calculate header byte length by re-serializing
	// This is not ideal but necessary without tracking offsets in HeaderFromBytes
	headerBytes := header.Bytes()
	offset := len(headerBytes)

	if len(data) < offset+12 {
		return nil, fmt.Errorf("data too short for certificate signatures")
	}

	signerBitmap := binary.BigEndian.Uint64(data[offset:])
	offset += 8

	sigCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	signatures := make([][]byte, 0, sigCount)
	for i := 0; i < sigCount; i++ {
		if len(data) < offset+2 {
			return nil, fmt.Errorf("data too short for signature %d length", i)
		}
		sigLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2

		if len(data) < offset+sigLen {
			return nil, fmt.Errorf("data too short for signature %d", i)
		}
		sig := make([]byte, sigLen)
		copy(sig, data[offset:offset+sigLen])
		signatures = append(signatures, sig)
		offset += sigLen
	}

	return &Certificate[H]{
		Header:       header,
		SignerBitmap: signerBitmap,
		Signatures:   signatures,
	}, nil
}

// NewCertificate creates a new certificate from a header and votes.
func NewCertificate[H Hash](header *Header[H], votes map[uint16][]byte) *Certificate[H] {
	cert := &Certificate[H]{
		Header:       header,
		SignerBitmap: 0,
		Signatures:   make([][]byte, 0, len(votes)),
	}

	// Build bitmap and signatures in order
	// Iterate through possible validator indices to maintain order
	for i := uint16(0); i < 64; i++ {
		if sig, ok := votes[i]; ok {
			cert.SignerBitmap |= (1 << uint(i))
			cert.Signatures = append(cert.Signatures, sig)
		}
	}

	return cert
}
