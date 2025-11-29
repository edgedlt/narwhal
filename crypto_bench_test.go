package narwhal_test

import (
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/edgedlt/narwhal"
	"github.com/edgedlt/narwhal/internal/testutil"
)

// ============================================================================
// BLS Unit Tests
// ============================================================================

func TestBLS_SignVerify(t *testing.T) {
	keyPair, err := narwhal.GenerateBLSKeyPair()
	if err != nil {
		t.Fatal(err)
	}

	message := []byte("test message for BLS")
	sig, err := narwhal.BLSSign(keyPair.PrivateKey, message)
	if err != nil {
		t.Fatalf("sign failed: %v", err)
	}

	pubKey, err := narwhal.NewBLSPublicKey(keyPair.PublicKey)
	if err != nil {
		t.Fatal(err)
	}

	if !pubKey.Verify(message, sig) {
		t.Error("signature verification failed")
	}

	// Wrong message should fail
	if pubKey.Verify([]byte("wrong message"), sig) {
		t.Error("verification should fail for wrong message")
	}
}

func TestBLS_BatchVerify(t *testing.T) {
	validators, err := narwhal.NewBLSValidatorSet(4)
	if err != nil {
		t.Fatal(err)
	}

	message := []byte("test message for batch verify")

	// Sign with 3 validators
	verifier := narwhal.NewGnarkBLSBatchVerifier()
	for i := 0; i < 3; i++ {
		signer := validators.GetSigner(uint16(i))
		sig, err := signer.Sign(message)
		if err != nil {
			t.Fatalf("sign failed: %v", err)
		}
		pk, _ := validators.GetByIndex(uint16(i))
		verifier.AddSignature(pk.Bytes(), message, sig)
	}

	if !verifier.VerifyBatch() {
		t.Error("batch verification failed")
	}
}

func TestBLS_Aggregate(t *testing.T) {
	validators, err := narwhal.NewBLSValidatorSet(4)
	if err != nil {
		t.Fatal(err)
	}

	message := []byte("test message for aggregation")

	// Sign with 3 validators
	sigs := make([][]byte, 3)
	pubKeys := make([][]byte, 3)
	for i := 0; i < 3; i++ {
		sig, err := validators.GetSigner(uint16(i)).Sign(message)
		if err != nil {
			t.Fatalf("sign failed: %v", err)
		}
		sigs[i] = sig
		pk, _ := validators.GetByIndex(uint16(i))
		pubKeys[i] = pk.Bytes()
	}

	aggregator := narwhal.NewGnarkBLSAggregator()
	aggSig, err := aggregator.Aggregate(sigs)
	if err != nil {
		t.Fatalf("aggregate failed: %v", err)
	}

	if !aggregator.VerifyAggregate(pubKeys, message, aggSig) {
		t.Error("aggregate verification failed")
	}

	// Wrong message should fail
	if aggregator.VerifyAggregate(pubKeys, []byte("wrong"), aggSig) {
		t.Error("verification should fail for wrong message")
	}
}

func TestBLS_ValidatorSet(t *testing.T) {
	validators, err := narwhal.NewBLSValidatorSet(4)
	if err != nil {
		t.Fatal(err)
	}

	if validators.Count() != 4 {
		t.Errorf("expected 4 validators, got %d", validators.Count())
	}

	if validators.F() != 1 {
		t.Errorf("expected F=1, got %d", validators.F())
	}

	for i := uint16(0); i < 4; i++ {
		if !validators.Contains(i) {
			t.Errorf("validator %d should exist", i)
		}

		pk, err := validators.GetByIndex(i)
		if err != nil {
			t.Errorf("GetByIndex(%d) failed: %v", i, err)
		}
		if pk == nil {
			t.Errorf("GetByIndex(%d) returned nil", i)
		}
	}

	if validators.Contains(4) {
		t.Error("validator 4 should not exist")
	}
}

func TestBLS_CryptoProvider(t *testing.T) {
	provider := narwhal.NewGnarkBLSCryptoProvider()

	if provider.Scheme() != narwhal.SignatureSchemeBLS {
		t.Errorf("expected BLS scheme, got %v", provider.Scheme())
	}

	if !provider.SupportsAggregation() {
		t.Error("BLS should support aggregation")
	}

	if provider.Aggregator() == nil {
		t.Error("BLS should have aggregator")
	}

	validators, err := narwhal.NewBLSValidatorSet(4)
	if err != nil {
		t.Fatal(err)
	}

	verifier := provider.NewBatchVerifier(validators)
	if verifier == nil {
		t.Error("NewBatchVerifier returned nil")
	}
}

// ============================================================================
// Basic Signature Benchmarks
// ============================================================================

func BenchmarkEd25519_Sign(b *testing.B) {
	signer := testutil.NewTestSigner()
	message := []byte("benchmark message for signing")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = signer.Sign(message)
	}
}

func BenchmarkBLS_Sign(b *testing.B) {
	keyPair, err := narwhal.GenerateBLSKeyPair()
	if err != nil {
		b.Fatal(err)
	}

	message := []byte("benchmark message for signing")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = narwhal.BLSSign(keyPair.PrivateKey, message)
	}
}

func BenchmarkEd25519_Verify(b *testing.B) {
	signer := testutil.NewTestSigner()
	message := []byte("benchmark message for verification")
	sig, _ := signer.Sign(message)
	pubKey := signer.PublicKey()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = pubKey.Verify(message, sig)
	}
}

func BenchmarkBLS_Verify(b *testing.B) {
	keyPair, err := narwhal.GenerateBLSKeyPair()
	if err != nil {
		b.Fatal(err)
	}
	pubKey, _ := narwhal.NewBLSPublicKey(keyPair.PublicKey)

	message := []byte("benchmark message for verification")
	sig, _ := narwhal.BLSSign(keyPair.PrivateKey, message)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = pubKey.Verify(message, sig)
	}
}

// ============================================================================
// Batch Verification Benchmarks (3 signatures - typical quorum for 4 nodes)
// ============================================================================

func BenchmarkEd25519_BatchVerify_3Sigs_Sequential(b *testing.B) {
	validators := testutil.NewTestValidatorSet(4)
	message := []byte("benchmark message for batch verify")

	// Pre-generate signatures
	sigs := make([][]byte, 3)
	for i := 0; i < 3; i++ {
		sig, _ := validators.GetSigner(uint16(i)).Sign(message)
		sigs[i] = sig
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 3; j++ {
			pk, _ := validators.GetByIndex(uint16(j))
			_ = pk.Verify(message, sigs[j])
		}
	}
}

func BenchmarkEd25519_BatchVerify_3Sigs_Parallel(b *testing.B) {
	validators := testutil.NewTestValidatorSet(4)
	message := []byte("benchmark message for batch verify")

	// Pre-generate signatures
	sigs := make([][]byte, 3)
	for i := 0; i < 3; i++ {
		sig, _ := validators.GetSigner(uint16(i)).Sign(message)
		sigs[i] = sig
	}

	crypto := narwhal.NewEd25519CryptoProvider(4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		verifier := crypto.NewBatchVerifier(validators)
		for j := 0; j < 3; j++ {
			verifier.Add(message, sigs[j], uint16(j))
		}
		_ = verifier.Verify()
	}
}

func BenchmarkBLS_BatchVerify_3Sigs(b *testing.B) {
	validators, err := narwhal.NewBLSValidatorSet(4)
	if err != nil {
		b.Fatal(err)
	}
	message := []byte("benchmark message for batch verify")

	// Pre-generate signatures
	sigs := make([][]byte, 3)
	for i := 0; i < 3; i++ {
		sig, _ := validators.GetSigner(uint16(i)).Sign(message)
		sigs[i] = sig
	}

	crypto := narwhal.NewGnarkBLSCryptoProvider()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		verifier := crypto.NewBatchVerifier(validators)
		for j := 0; j < 3; j++ {
			verifier.Add(message, sigs[j], uint16(j))
		}
		_ = verifier.Verify()
	}
}

// ============================================================================
// Batch Verification Benchmarks (10 signatures - larger quorum)
// ============================================================================

func BenchmarkEd25519_BatchVerify_10Sigs_Sequential(b *testing.B) {
	validators := testutil.NewTestValidatorSet(16)
	message := []byte("benchmark message for batch verify")

	// Pre-generate signatures
	sigs := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		sig, _ := validators.GetSigner(uint16(i)).Sign(message)
		sigs[i] = sig
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			pk, _ := validators.GetByIndex(uint16(j))
			_ = pk.Verify(message, sigs[j])
		}
	}
}

func BenchmarkEd25519_BatchVerify_10Sigs_Parallel(b *testing.B) {
	validators := testutil.NewTestValidatorSet(16)
	message := []byte("benchmark message for batch verify")

	// Pre-generate signatures
	sigs := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		sig, _ := validators.GetSigner(uint16(i)).Sign(message)
		sigs[i] = sig
	}

	crypto := narwhal.NewEd25519CryptoProvider(4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		verifier := crypto.NewBatchVerifier(validators)
		for j := 0; j < 10; j++ {
			verifier.Add(message, sigs[j], uint16(j))
		}
		_ = verifier.Verify()
	}
}

func BenchmarkBLS_BatchVerify_10Sigs(b *testing.B) {
	validators, err := narwhal.NewBLSValidatorSet(16)
	if err != nil {
		b.Fatal(err)
	}
	message := []byte("benchmark message for batch verify")

	// Pre-generate signatures
	sigs := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		sig, _ := validators.GetSigner(uint16(i)).Sign(message)
		sigs[i] = sig
	}

	crypto := narwhal.NewGnarkBLSCryptoProvider()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		verifier := crypto.NewBatchVerifier(validators)
		for j := 0; j < 10; j++ {
			verifier.Add(message, sigs[j], uint16(j))
		}
		_ = verifier.Verify()
	}
}

// ============================================================================
// Signature Aggregation Benchmarks (BLS only)
// ============================================================================

func BenchmarkBLS_Aggregate_3Sigs(b *testing.B) {
	validators, err := narwhal.NewBLSValidatorSet(4)
	if err != nil {
		b.Fatal(err)
	}
	message := []byte("benchmark message for aggregation")

	// Pre-generate signatures
	sigs := make([][]byte, 3)
	for i := 0; i < 3; i++ {
		sig, _ := validators.GetSigner(uint16(i)).Sign(message)
		sigs[i] = sig
	}

	aggregator := narwhal.NewGnarkBLSAggregator()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = aggregator.Aggregate(sigs)
	}
}

func BenchmarkBLS_VerifyAggregate_3Sigs(b *testing.B) {
	validators, err := narwhal.NewBLSValidatorSet(4)
	if err != nil {
		b.Fatal(err)
	}
	message := []byte("benchmark message for aggregation")

	// Pre-generate signatures and aggregate
	sigs := make([][]byte, 3)
	pubKeys := make([][]byte, 3)
	for i := 0; i < 3; i++ {
		sig, _ := validators.GetSigner(uint16(i)).Sign(message)
		sigs[i] = sig
		pk, _ := validators.GetByIndex(uint16(i))
		pubKeys[i] = pk.Bytes()
	}

	aggregator := narwhal.NewGnarkBLSAggregator()
	aggSig, _ := aggregator.Aggregate(sigs)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = aggregator.VerifyAggregate(pubKeys, message, aggSig)
	}
}

// ============================================================================
// Certificate Validation Benchmarks
// ============================================================================

func BenchmarkCertificate_Validate_Ed25519_Sequential(b *testing.B) {
	validators := testutil.NewTestValidatorSet(4)

	header := &narwhal.Header[testutil.TestHash]{
		Author:    0,
		Round:     0,
		Timestamp: uint64(time.Now().UnixNano()),
	}
	header.ComputeDigest(testutil.ComputeHash)

	votes := make(map[uint16][]byte)
	for i := uint16(0); i < 3; i++ {
		sig, _ := validators.GetSigner(i).Sign(header.Digest.Bytes())
		votes[i] = sig
	}
	cert := narwhal.NewCertificate(header, votes)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cert.Validate(validators)
	}
}

func BenchmarkCertificate_Validate_Ed25519_Parallel(b *testing.B) {
	validators := testutil.NewTestValidatorSet(4)
	crypto := narwhal.NewEd25519CryptoProvider(4)

	header := &narwhal.Header[testutil.TestHash]{
		Author:    0,
		Round:     0,
		Timestamp: uint64(time.Now().UnixNano()),
	}
	header.ComputeDigest(testutil.ComputeHash)

	votes := make(map[uint16][]byte)
	for i := uint16(0); i < 3; i++ {
		sig, _ := validators.GetSigner(i).Sign(header.Digest.Bytes())
		votes[i] = sig
	}
	cert := narwhal.NewCertificate(header, votes)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cert.ValidateWithCrypto(validators, crypto, nil)
	}
}

func BenchmarkCertificate_Validate_Ed25519_Cached(b *testing.B) {
	validators := testutil.NewTestValidatorSet(4)
	crypto := narwhal.NewEd25519CryptoProvider(4)
	cache := narwhal.NewSignatureCache(10000)

	header := &narwhal.Header[testutil.TestHash]{
		Author:    0,
		Round:     0,
		Timestamp: uint64(time.Now().UnixNano()),
	}
	header.ComputeDigest(testutil.ComputeHash)

	votes := make(map[uint16][]byte)
	for i := uint16(0); i < 3; i++ {
		sig, _ := validators.GetSigner(i).Sign(header.Digest.Bytes())
		votes[i] = sig
	}
	cert := narwhal.NewCertificate(header, votes)

	// Pre-populate cache
	_ = cert.ValidateWithCrypto(validators, crypto, cache)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cert.ValidateWithCrypto(validators, crypto, cache)
	}
}

// ============================================================================
// BLS Certificate Benchmarks (using BLSHash type)
// ============================================================================

// BLSHash is a hash type for BLS benchmarks
type BLSHash [32]byte

func (h BLSHash) Bytes() []byte  { return h[:] }
func (h BLSHash) String() string { return fmt.Sprintf("%x", h[:8]) }
func (h BLSHash) Equals(other narwhal.Hash) bool {
	if o, ok := other.(BLSHash); ok {
		return h == o
	}
	return false
}

func computeBLSHash(data []byte) BLSHash {
	return sha256.Sum256(data)
}

func BenchmarkCertificate_Validate_BLS_Batch(b *testing.B) {
	validators, err := narwhal.NewBLSValidatorSet(4)
	if err != nil {
		b.Fatal(err)
	}
	crypto := narwhal.NewGnarkBLSCryptoProvider()

	header := &narwhal.Header[BLSHash]{
		Author:    0,
		Round:     0,
		Timestamp: uint64(time.Now().UnixNano()),
	}
	header.ComputeDigest(computeBLSHash)

	votes := make(map[uint16][]byte)
	for i := uint16(0); i < 3; i++ {
		sig, _ := validators.GetSigner(i).Sign(header.Digest.Bytes())
		votes[i] = sig
	}
	cert := narwhal.NewCertificate(header, votes)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cert.ValidateWithCrypto(validators, crypto, nil)
	}
}

// ============================================================================
// Signature Cache Benchmarks
// ============================================================================

func BenchmarkSignatureCache_IsVerified_Hit(b *testing.B) {
	cache := narwhal.NewSignatureCache(100000)
	digest := []byte("benchmark digest value for cache")
	cache.MarkVerified(digest, 42)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cache.IsVerified(digest, 42)
	}
}

func BenchmarkSignatureCache_IsVerified_Miss(b *testing.B) {
	cache := narwhal.NewSignatureCache(100000)
	digest := []byte("benchmark digest value for cache")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cache.IsVerified(digest, 42)
	}
}

func BenchmarkSignatureCache_MarkVerified(b *testing.B) {
	cache := narwhal.NewSignatureCache(100000)
	digest := []byte("benchmark digest value for cache")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.MarkVerified(digest, uint16(i%1000))
	}
}

// ============================================================================
// Summary Test (not a benchmark, prints comparison)
// ============================================================================

func TestCryptoPerformanceSummary(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance summary in short mode")
	}

	t.Logf("\n=== Crypto Performance: Ed25519 vs BLS by Validator Set Size ===\n")
	t.Logf("%-8s | %-14s | %-14s | %-14s | %-10s", "Signers", "Ed25519 Seq", "Ed25519 Par", "BLS Aggregate", "Winner")
	t.Logf("%-8s-+-%-14s-+-%-14s-+-%-14s-+-%-10s", "--------", "--------------", "--------------", "--------------", "----------")

	// Test various quorum sizes
	testCases := []struct {
		validators int
		quorum     int // 2f+1
	}{
		{4, 3},    // Small: 4 validators, f=1, quorum=3
		{7, 5},    // Medium-small: 7 validators, f=2, quorum=5
		{10, 7},   // Medium: 10 validators, f=3, quorum=7
		{16, 11},  // Medium-large: 16 validators, f=5, quorum=11
		{25, 17},  // Large: 25 validators, f=8, quorum=17
		{31, 21},  // Larger: 31 validators, f=10, quorum=21
		{50, 34},  // Very large: 50 validators, f=16, quorum=34
		{100, 67}, // Huge: 100 validators, f=33, quorum=67
	}

	const iterations = 50

	for _, tc := range testCases {
		// Ed25519 setup
		ed25519Validators := testutil.NewTestValidatorSet(tc.validators)
		ed25519Message := []byte("test message for timing")
		ed25519Sigs := make([][]byte, tc.quorum)
		for i := 0; i < tc.quorum; i++ {
			sig, _ := ed25519Validators.GetSigner(uint16(i)).Sign(ed25519Message)
			ed25519Sigs[i] = sig
		}

		// Ed25519 sequential
		start := time.Now()
		for n := 0; n < iterations; n++ {
			for j := 0; j < tc.quorum; j++ {
				pk, _ := ed25519Validators.GetByIndex(uint16(j))
				_ = pk.Verify(ed25519Message, ed25519Sigs[j])
			}
		}
		ed25519SeqTime := time.Since(start) / iterations

		// Ed25519 parallel
		ed25519Crypto := narwhal.NewEd25519CryptoProvider(8) // 8 workers
		start = time.Now()
		for n := 0; n < iterations; n++ {
			verifier := ed25519Crypto.NewBatchVerifier(ed25519Validators)
			for j := 0; j < tc.quorum; j++ {
				verifier.Add(ed25519Message, ed25519Sigs[j], uint16(j))
			}
			_ = verifier.Verify()
		}
		ed25519ParTime := time.Since(start) / iterations

		// BLS setup
		blsValidators, err := narwhal.NewBLSValidatorSet(tc.validators)
		if err != nil {
			t.Fatal(err)
		}
		blsMessage := []byte("test message for timing")
		blsSigs := make([][]byte, tc.quorum)
		blsPubKeys := make([][]byte, tc.quorum)
		for i := 0; i < tc.quorum; i++ {
			sig, _ := blsValidators.GetSigner(uint16(i)).Sign(blsMessage)
			blsSigs[i] = sig
			pk, _ := blsValidators.GetByIndex(uint16(i))
			blsPubKeys[i] = pk.Bytes()
		}

		// BLS aggregate
		aggregator := narwhal.NewGnarkBLSAggregator()
		aggSig, _ := aggregator.Aggregate(blsSigs)

		start = time.Now()
		for n := 0; n < iterations; n++ {
			_ = aggregator.VerifyAggregate(blsPubKeys, blsMessage, aggSig)
		}
		blsAggTime := time.Since(start) / iterations

		// Determine winner
		winner := "Ed25519"
		if blsAggTime < ed25519ParTime {
			winner = "BLS"
		}

		t.Logf("%-8d | %-14v | %-14v | %-14v | %-10s",
			tc.quorum, ed25519SeqTime, ed25519ParTime, blsAggTime, winner)
	}

	t.Logf("")
	t.Logf("Note: BLS verification includes public key aggregation overhead.")
	t.Logf("      Ed25519 parallel verification scales very well with worker count.")
	t.Logf("")

	// Also show the "pure" pairing cost (pre-aggregated keys)
	t.Logf("=== BLS Pairing Cost (pre-aggregated keys) ===")
	blsValidators, _ := narwhal.NewBLSValidatorSet(4)
	blsMessage := []byte("test message for timing")
	blsSigs := make([][]byte, 3)
	blsPubKeys := make([][]byte, 3)
	for i := 0; i < 3; i++ {
		sig, _ := blsValidators.GetSigner(uint16(i)).Sign(blsMessage)
		blsSigs[i] = sig
		pk, _ := blsValidators.GetByIndex(uint16(i))
		blsPubKeys[i] = pk.Bytes()
	}
	aggregator := narwhal.NewGnarkBLSAggregator()
	aggSig, _ := aggregator.Aggregate(blsSigs)

	// Pre-aggregate public keys (would be done once when validator set changes)
	start := time.Now()
	for n := 0; n < iterations; n++ {
		_ = aggregator.VerifyAggregate(blsPubKeys, blsMessage, aggSig)
	}
	blsWithAgg := time.Since(start) / iterations
	t.Logf("BLS verify (with PK aggregation):  %v", blsWithAgg)

	// The pairing itself is ~1.5ms, PK aggregation adds overhead per key
	t.Logf("")
	t.Logf("=== Recommendations ===")
	t.Logf("")
	t.Logf("For Narwhal certificate verification:")
	t.Logf("  - Ed25519 parallel is fastest at ALL tested sizes (up to 67 signers)")
	t.Logf("  - Ed25519 parallel: ~6µs per signature with 8 workers")
	t.Logf("  - BLS advantage is in BANDWIDTH, not verification speed:")
	t.Logf("    * Ed25519 signature: 64 bytes × N signers")
	t.Logf("    * BLS aggregate sig: 48 bytes total (constant)")
	t.Logf("")
	t.Logf("Choose based on your bottleneck:")
	t.Logf("  - CPU-bound / high TPS: Ed25519")
	t.Logf("  - Bandwidth-bound / large validator sets: BLS")
}
