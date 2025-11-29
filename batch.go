package narwhal

import (
	"encoding/binary"
	"fmt"
)

// Batch methods for serialization and validation

// ComputeDigest computes and sets the digest for this batch.
// The digest is computed over the batch metadata and transaction hashes.
// This must be called before broadcasting the batch.
func (b *Batch[H, T]) ComputeDigest(hashFunc func([]byte) H) {
	b.Digest = hashFunc(b.digestBytes())
}

// digestBytes returns the bytes used to compute the digest.
// Format: [WorkerID:2][ValidatorID:2][Round:8][TxCount:4][TxHash0][TxHash1]...
func (b *Batch[H, T]) digestBytes() []byte {
	// Calculate size
	var hashSize int
	if len(b.Transactions) > 0 {
		hashSize = len(b.Transactions[0].Hash().Bytes())
	}
	size := 2 + 2 + 8 + 4 + len(b.Transactions)*hashSize

	buf := make([]byte, size)
	offset := 0

	binary.BigEndian.PutUint16(buf[offset:], b.WorkerID)
	offset += 2

	binary.BigEndian.PutUint16(buf[offset:], b.ValidatorID)
	offset += 2

	binary.BigEndian.PutUint64(buf[offset:], b.Round)
	offset += 8

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(b.Transactions)))
	offset += 4

	for _, tx := range b.Transactions {
		copy(buf[offset:], tx.Hash().Bytes())
		offset += hashSize
	}

	return buf
}

// Verify verifies that the batch digest matches its contents.
func (b *Batch[H, T]) Verify(hashFunc func([]byte) H) error {
	expected := hashFunc(b.digestBytes())
	if !b.Digest.Equals(expected) {
		return fmt.Errorf("batch digest mismatch")
	}
	return nil
}

// Bytes serializes the batch to bytes.
// Format: [WorkerID:2][ValidatorID:2][Round:8][DigestLen:2][Digest][TxCount:4][Tx0Len:4][Tx0][Tx1Len:4][Tx1]...
func (b *Batch[H, T]) Bytes() []byte {
	digestBytes := b.Digest.Bytes()

	// Calculate total size
	size := 2 + 2 + 8 + 2 + len(digestBytes) + 4
	for _, tx := range b.Transactions {
		size += 4 + len(tx.Bytes())
	}

	buf := make([]byte, size)
	offset := 0

	binary.BigEndian.PutUint16(buf[offset:], b.WorkerID)
	offset += 2

	binary.BigEndian.PutUint16(buf[offset:], b.ValidatorID)
	offset += 2

	binary.BigEndian.PutUint64(buf[offset:], b.Round)
	offset += 8

	binary.BigEndian.PutUint16(buf[offset:], uint16(len(digestBytes)))
	offset += 2

	copy(buf[offset:], digestBytes)
	offset += len(digestBytes)

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(b.Transactions)))
	offset += 4

	for _, tx := range b.Transactions {
		txBytes := tx.Bytes()
		binary.BigEndian.PutUint32(buf[offset:], uint32(len(txBytes)))
		offset += 4
		copy(buf[offset:], txBytes)
		offset += len(txBytes)
	}

	return buf
}

// BatchFromBytes deserializes a batch from bytes.
// hashFromBytes converts raw bytes to H.
// txFromBytes converts raw bytes to T.
func BatchFromBytes[H Hash, T Transaction[H]](
	data []byte,
	hashFromBytes func([]byte) (H, error),
	txFromBytes func([]byte) (T, error),
) (*Batch[H, T], error) {
	if len(data) < 16 { // Minimum: 2+2+8+2+0+4 = 18, but need at least digest
		return nil, fmt.Errorf("data too short for batch: %d bytes", len(data))
	}

	offset := 0

	workerID := binary.BigEndian.Uint16(data[offset:])
	offset += 2

	validatorID := binary.BigEndian.Uint16(data[offset:])
	offset += 2

	round := binary.BigEndian.Uint64(data[offset:])
	offset += 8

	digestLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2

	if len(data) < offset+digestLen+4 {
		return nil, fmt.Errorf("data too short for batch digest")
	}

	digest, err := hashFromBytes(data[offset : offset+digestLen])
	if err != nil {
		return nil, fmt.Errorf("failed to parse batch digest: %w", err)
	}
	offset += digestLen

	txCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	transactions := make([]T, 0, txCount)
	for i := 0; i < txCount; i++ {
		if len(data) < offset+4 {
			return nil, fmt.Errorf("data too short for transaction %d length", i)
		}
		txLen := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4

		if len(data) < offset+txLen {
			return nil, fmt.Errorf("data too short for transaction %d", i)
		}
		tx, err := txFromBytes(data[offset : offset+txLen])
		if err != nil {
			return nil, fmt.Errorf("failed to parse transaction %d: %w", i, err)
		}
		transactions = append(transactions, tx)
		offset += txLen
	}

	return &Batch[H, T]{
		WorkerID:     workerID,
		ValidatorID:  validatorID,
		Round:        round,
		Digest:       digest,
		Transactions: transactions,
	}, nil
}

// TransactionCount returns the number of transactions in the batch.
func (b *Batch[H, T]) TransactionCount() int {
	return len(b.Transactions)
}
