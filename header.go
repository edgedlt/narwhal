package narwhal

import (
	"encoding/binary"
	"fmt"
)

// Header methods for serialization and validation

// ComputeDigest computes and sets the digest for this header.
// The digest is computed over all header fields except the digest itself.
// This must be called before broadcasting the header.
func (h *Header[H]) ComputeDigest(hashFunc func([]byte) H) {
	h.Digest = hashFunc(h.digestBytes())
}

// digestBytes returns the bytes used to compute the digest.
// Format: [Author:2][Round:8][Epoch:8][Timestamp:8][BatchRefCount:4][BatchRef0]...[ParentCount:4][Parent0]...
func (h *Header[H]) digestBytes() []byte {
	var hashSize int
	if len(h.BatchRefs) > 0 {
		hashSize = len(h.BatchRefs[0].Bytes())
	} else if len(h.Parents) > 0 {
		hashSize = len(h.Parents[0].Bytes())
	}

	size := 2 + 8 + 8 + 8 + 4 + len(h.BatchRefs)*hashSize + 4 + len(h.Parents)*hashSize

	buf := make([]byte, size)
	offset := 0

	binary.BigEndian.PutUint16(buf[offset:], h.Author)
	offset += 2

	binary.BigEndian.PutUint64(buf[offset:], h.Round)
	offset += 8

	binary.BigEndian.PutUint64(buf[offset:], h.Epoch)
	offset += 8

	binary.BigEndian.PutUint64(buf[offset:], h.Timestamp)
	offset += 8

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(h.BatchRefs)))
	offset += 4

	for _, ref := range h.BatchRefs {
		copy(buf[offset:], ref.Bytes())
		offset += hashSize
	}

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(h.Parents)))
	offset += 4

	for _, parent := range h.Parents {
		copy(buf[offset:], parent.Bytes())
		offset += hashSize
	}

	return buf
}

// VerifyDigest verifies that the header digest matches its contents.
func (h *Header[H]) VerifyDigest(hashFunc func([]byte) H) bool {
	expected := hashFunc(h.digestBytes())
	return h.Digest.Equals(expected)
}

// Bytes serializes the header to bytes.
// Format: [Author:2][Round:8][Epoch:8][Timestamp:8][DigestLen:2][Digest][BatchRefCount:4][HashLen:2][BatchRef0]...[ParentCount:4][Parent0]...
func (h *Header[H]) Bytes() []byte {
	digestBytes := h.Digest.Bytes()

	var hashSize int
	if len(h.BatchRefs) > 0 {
		hashSize = len(h.BatchRefs[0].Bytes())
	} else if len(h.Parents) > 0 {
		hashSize = len(h.Parents[0].Bytes())
	} else if len(digestBytes) > 0 {
		hashSize = len(digestBytes)
	}

	// Calculate total size
	size := 2 + 8 + 8 + 8 + 2 + len(digestBytes) + 4 + 2 + len(h.BatchRefs)*hashSize + 4 + len(h.Parents)*hashSize

	buf := make([]byte, size)
	offset := 0

	binary.BigEndian.PutUint16(buf[offset:], h.Author)
	offset += 2

	binary.BigEndian.PutUint64(buf[offset:], h.Round)
	offset += 8

	binary.BigEndian.PutUint64(buf[offset:], h.Epoch)
	offset += 8

	binary.BigEndian.PutUint64(buf[offset:], h.Timestamp)
	offset += 8

	binary.BigEndian.PutUint16(buf[offset:], uint16(len(digestBytes)))
	offset += 2

	copy(buf[offset:], digestBytes)
	offset += len(digestBytes)

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(h.BatchRefs)))
	offset += 4

	binary.BigEndian.PutUint16(buf[offset:], uint16(hashSize))
	offset += 2

	for _, ref := range h.BatchRefs {
		copy(buf[offset:], ref.Bytes())
		offset += hashSize
	}

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(h.Parents)))
	offset += 4

	for _, parent := range h.Parents {
		copy(buf[offset:], parent.Bytes())
		offset += hashSize
	}

	return buf
}

// HeaderFromBytes deserializes a header from bytes.
func HeaderFromBytes[H Hash](
	data []byte,
	hashFromBytes func([]byte) (H, error),
) (*Header[H], error) {
	if len(data) < 30 { // Minimum header size
		return nil, fmt.Errorf("data too short for header: %d bytes", len(data))
	}

	offset := 0

	author := binary.BigEndian.Uint16(data[offset:])
	offset += 2

	round := binary.BigEndian.Uint64(data[offset:])
	offset += 8

	epoch := binary.BigEndian.Uint64(data[offset:])
	offset += 8

	timestamp := binary.BigEndian.Uint64(data[offset:])
	offset += 8

	digestLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2

	if len(data) < offset+digestLen {
		return nil, fmt.Errorf("data too short for header digest")
	}

	digest, err := hashFromBytes(data[offset : offset+digestLen])
	if err != nil {
		return nil, fmt.Errorf("failed to parse header digest: %w", err)
	}
	offset += digestLen

	if len(data) < offset+6 {
		return nil, fmt.Errorf("data too short for batch refs")
	}

	batchRefCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	hashSize := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2

	batchRefs := make([]H, 0, batchRefCount)
	for i := 0; i < batchRefCount; i++ {
		if len(data) < offset+hashSize {
			return nil, fmt.Errorf("data too short for batch ref %d", i)
		}
		ref, err := hashFromBytes(data[offset : offset+hashSize])
		if err != nil {
			return nil, fmt.Errorf("failed to parse batch ref %d: %w", i, err)
		}
		batchRefs = append(batchRefs, ref)
		offset += hashSize
	}

	if len(data) < offset+4 {
		return nil, fmt.Errorf("data too short for parent count")
	}

	parentCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	parents := make([]H, 0, parentCount)
	for i := 0; i < parentCount; i++ {
		if len(data) < offset+hashSize {
			return nil, fmt.Errorf("data too short for parent %d", i)
		}
		parent, err := hashFromBytes(data[offset : offset+hashSize])
		if err != nil {
			return nil, fmt.Errorf("failed to parse parent %d: %w", i, err)
		}
		parents = append(parents, parent)
		offset += hashSize
	}

	return &Header[H]{
		Author:    author,
		Round:     round,
		Epoch:     epoch,
		Timestamp: timestamp,
		Digest:    digest,
		BatchRefs: batchRefs,
		Parents:   parents,
	}, nil
}
