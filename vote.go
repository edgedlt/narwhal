package narwhal

import (
	"encoding/binary"
	"fmt"
)

// HeaderVote methods for serialization

// Bytes serializes the vote to bytes.
// Format: [HeaderDigestLen:2][HeaderDigest][ValidatorIndex:2][SignatureLen:2][Signature]
func (v *HeaderVote[H]) Bytes() []byte {
	digestBytes := v.HeaderDigest.Bytes()

	size := 2 + len(digestBytes) + 2 + 2 + len(v.Signature)
	buf := make([]byte, size)
	offset := 0

	binary.BigEndian.PutUint16(buf[offset:], uint16(len(digestBytes)))
	offset += 2

	copy(buf[offset:], digestBytes)
	offset += len(digestBytes)

	binary.BigEndian.PutUint16(buf[offset:], v.ValidatorIndex)
	offset += 2

	binary.BigEndian.PutUint16(buf[offset:], uint16(len(v.Signature)))
	offset += 2

	copy(buf[offset:], v.Signature)

	return buf
}

// VoteFromBytes deserializes a vote from bytes.
func VoteFromBytes[H Hash](
	data []byte,
	hashFromBytes func([]byte) (H, error),
) (*HeaderVote[H], error) {
	if len(data) < 6 {
		return nil, fmt.Errorf("data too short for vote: %d bytes", len(data))
	}

	offset := 0

	digestLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2

	if len(data) < offset+digestLen+4 {
		return nil, fmt.Errorf("data too short for vote digest")
	}

	digest, err := hashFromBytes(data[offset : offset+digestLen])
	if err != nil {
		return nil, fmt.Errorf("failed to parse vote digest: %w", err)
	}
	offset += digestLen

	validatorIndex := binary.BigEndian.Uint16(data[offset:])
	offset += 2

	sigLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2

	if len(data) < offset+sigLen {
		return nil, fmt.Errorf("data too short for vote signature")
	}

	signature := make([]byte, sigLen)
	copy(signature, data[offset:offset+sigLen])

	return &HeaderVote[H]{
		HeaderDigest:   digest,
		ValidatorIndex: validatorIndex,
		Signature:      signature,
	}, nil
}

// NewVote creates a new vote for a header.
func NewVote[H Hash](headerDigest H, validatorIndex uint16, signer Signer) (*HeaderVote[H], error) {
	sig, err := signer.Sign(headerDigest.Bytes())
	if err != nil {
		return nil, fmt.Errorf("failed to sign header: %w", err)
	}

	return &HeaderVote[H]{
		HeaderDigest:   headerDigest,
		ValidatorIndex: validatorIndex,
		Signature:      sig,
	}, nil
}

// Verify verifies the vote signature.
func (v *HeaderVote[H]) Verify(pubKey PublicKey) bool {
	return pubKey.Verify(v.HeaderDigest.Bytes(), v.Signature)
}
