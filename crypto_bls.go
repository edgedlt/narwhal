// Package narwhal provides BLS signature verification using gnark-crypto.
// This implementation uses BLS12-381 with signatures in G1 and public keys in G2
// (minimal signature variant).

package narwhal

import (
	"fmt"
	"math/big"

	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
	"github.com/consensys/gnark-crypto/ecc/bls12-381/fr"
)

// BLS DST (Domain Separation Tag) for hash-to-curve
const blsDST = "BLS_SIG_BLS12381G1_XMD:SHA-256_SSWU_RO_NUL_"

// BLSKeyPair holds a BLS private/public key pair.
type BLSKeyPair struct {
	PrivateKey []byte // 32 bytes (fr.Element)
	PublicKey  []byte // 96 bytes (G2Affine compressed)
}

// GenerateBLSKeyPair generates a new BLS key pair.
func GenerateBLSKeyPair() (*BLSKeyPair, error) {
	// Generate random scalar for private key
	var scalar fr.Element
	_, err := scalar.SetRandom()
	if err != nil {
		return nil, fmt.Errorf("failed to generate random scalar: %w", err)
	}

	privBytes := scalar.Bytes()

	// Derive public key: pk = sk * G2
	_, _, _, g2Gen := bls12381.Generators()
	var pk bls12381.G2Affine
	pk.ScalarMultiplication(&g2Gen, scalar.BigInt(new(big.Int)))
	pubBytes := pk.Bytes()

	return &BLSKeyPair{
		PrivateKey: privBytes[:],
		PublicKey:  pubBytes[:],
	}, nil
}

// GnarkBLSBatchVerifier implements BLSBatchVerifier using gnark-crypto.
type GnarkBLSBatchVerifier struct {
	pubKeys    []bls12381.G2Affine
	messages   [][]byte
	signatures []bls12381.G1Affine
}

// NewGnarkBLSBatchVerifier creates a new BLS batch verifier.
func NewGnarkBLSBatchVerifier() *GnarkBLSBatchVerifier {
	return &GnarkBLSBatchVerifier{
		pubKeys:    make([]bls12381.G2Affine, 0, 16),
		messages:   make([][]byte, 0, 16),
		signatures: make([]bls12381.G1Affine, 0, 16),
	}
}

// AddSignature adds a signature to the batch.
func (v *GnarkBLSBatchVerifier) AddSignature(pubKey []byte, message []byte, signature []byte) {
	var pk bls12381.G2Affine
	if _, err := pk.SetBytes(pubKey); err != nil {
		return // Invalid public key
	}

	var sig bls12381.G1Affine
	if _, err := sig.SetBytes(signature); err != nil {
		return // Invalid signature
	}

	v.pubKeys = append(v.pubKeys, pk)
	v.messages = append(v.messages, message)
	v.signatures = append(v.signatures, sig)
}

// VerifyBatch verifies all signatures in the batch using random linear combination.
func (v *GnarkBLSBatchVerifier) VerifyBatch() bool {
	if len(v.signatures) == 0 {
		return true
	}

	if len(v.signatures) == 1 {
		// Single signature - use direct verification
		return v.verifySingle(0)
	}

	// Generate random scalars for linear combination (security against rogue key attacks)
	coeffs := make([]fr.Element, len(v.signatures))
	for i := range coeffs {
		_, _ = coeffs[i].SetRandom()
	}

	// Compute aggregate signature: Σ r_i * sig_i
	var aggSigJac bls12381.G1Jac
	for i, sig := range v.signatures {
		var sigJac bls12381.G1Jac
		sigJac.FromAffine(&sig)
		sigJac.ScalarMultiplication(&sigJac, coeffs[i].BigInt(new(big.Int)))
		aggSigJac.AddAssign(&sigJac)
	}
	var aggSig bls12381.G1Affine
	aggSig.FromJacobian(&aggSigJac)

	// Compute scaled hashes: r_i * H(m_i)
	scaledHashes := make([]bls12381.G1Affine, len(v.messages))
	for i, msg := range v.messages {
		hashPoint, err := bls12381.HashToG1(msg, []byte(blsDST))
		if err != nil {
			return false
		}
		var hashJac bls12381.G1Jac
		hashJac.FromAffine(&hashPoint)
		hashJac.ScalarMultiplication(&hashJac, coeffs[i].BigInt(new(big.Int)))
		scaledHashes[i].FromJacobian(&hashJac)
	}

	// Verify: e(Σ r_i * sig_i, G2) == e(r_1*H(m_1), pk_1) * ... * e(r_n*H(m_n), pk_n)
	_, _, _, g2Gen := bls12381.Generators()

	// LHS: e(aggSig, G2)
	lhs, err := bls12381.Pair([]bls12381.G1Affine{aggSig}, []bls12381.G2Affine{g2Gen})
	if err != nil {
		return false
	}

	// RHS: multi-pairing of scaled hashes with public keys
	rhs, err := bls12381.Pair(scaledHashes, v.pubKeys)
	if err != nil {
		return false
	}

	return lhs.Equal(&rhs)
}

// verifySingle verifies a single signature at index i.
func (v *GnarkBLSBatchVerifier) verifySingle(i int) bool {
	// Hash message to G1
	hashPoint, err := bls12381.HashToG1(v.messages[i], []byte(blsDST))
	if err != nil {
		return false
	}

	// Verify: e(H(m), pk) == e(sig, G2)
	_, _, _, g2Gen := bls12381.Generators()

	left, err := bls12381.Pair([]bls12381.G1Affine{hashPoint}, []bls12381.G2Affine{v.pubKeys[i]})
	if err != nil {
		return false
	}

	right, err := bls12381.Pair([]bls12381.G1Affine{v.signatures[i]}, []bls12381.G2Affine{g2Gen})
	if err != nil {
		return false
	}

	return left.Equal(&right)
}

// Reset clears the batch.
func (v *GnarkBLSBatchVerifier) Reset() {
	v.pubKeys = v.pubKeys[:0]
	v.messages = v.messages[:0]
	v.signatures = v.signatures[:0]
}

// GnarkBLSAggregator implements BLSAggregator using gnark-crypto.
type GnarkBLSAggregator struct{}

// NewGnarkBLSAggregator creates a new BLS aggregator.
func NewGnarkBLSAggregator() *GnarkBLSAggregator {
	return &GnarkBLSAggregator{}
}

// Aggregate combines multiple BLS signatures into one.
func (a *GnarkBLSAggregator) Aggregate(signatures [][]byte) ([]byte, error) {
	if len(signatures) == 0 {
		return nil, fmt.Errorf("no signatures to aggregate")
	}

	var aggSig bls12381.G1Jac
	for i, sigBytes := range signatures {
		var sig bls12381.G1Affine
		if _, err := sig.SetBytes(sigBytes); err != nil {
			return nil, fmt.Errorf("invalid signature at index %d: %w", i, err)
		}
		var sigJac bls12381.G1Jac
		sigJac.FromAffine(&sig)
		aggSig.AddAssign(&sigJac)
	}

	var result bls12381.G1Affine
	result.FromJacobian(&aggSig)
	bytes := result.Bytes()
	return bytes[:], nil
}

// VerifyAggregate verifies an aggregated signature (all signers signed the same message).
func (a *GnarkBLSAggregator) VerifyAggregate(pubKeys [][]byte, message []byte, aggSig []byte) bool {
	if len(pubKeys) == 0 {
		return false
	}

	// Parse aggregated signature
	var sig bls12381.G1Affine
	if _, err := sig.SetBytes(aggSig); err != nil {
		return false
	}

	// Aggregate public keys
	var aggPK bls12381.G2Jac
	for _, pkBytes := range pubKeys {
		var pk bls12381.G2Affine
		if _, err := pk.SetBytes(pkBytes); err != nil {
			return false
		}
		var pkJac bls12381.G2Jac
		pkJac.FromAffine(&pk)
		aggPK.AddAssign(&pkJac)
	}
	var aggPKAff bls12381.G2Affine
	aggPKAff.FromJacobian(&aggPK)

	// Hash message to G1
	hashPoint, err := bls12381.HashToG1(message, []byte(blsDST))
	if err != nil {
		return false
	}

	// Verify: e(H(m), aggPK) == e(sig, G2)
	_, _, _, g2Gen := bls12381.Generators()

	left, err := bls12381.Pair([]bls12381.G1Affine{hashPoint}, []bls12381.G2Affine{aggPKAff})
	if err != nil {
		return false
	}

	right, err := bls12381.Pair([]bls12381.G1Affine{sig}, []bls12381.G2Affine{g2Gen})
	if err != nil {
		return false
	}

	return left.Equal(&right)
}

// BLSSign signs a message using a BLS private key.
func BLSSign(privateKey []byte, message []byte) ([]byte, error) {
	// Parse private key
	var scalar fr.Element
	scalar.SetBytes(privateKey)

	// Hash message to G1
	hashPoint, err := bls12381.HashToG1(message, []byte(blsDST))
	if err != nil {
		return nil, fmt.Errorf("failed to hash message: %w", err)
	}

	// Compute signature: sig = sk * H(m)
	var sig bls12381.G1Affine
	sig.ScalarMultiplication(&hashPoint, scalar.BigInt(new(big.Int)))

	bytes := sig.Bytes()
	return bytes[:], nil
}

// GnarkBLSCryptoProvider implements CryptoProvider for BLS using gnark-crypto.
type GnarkBLSCryptoProvider struct {
	aggregator *GnarkBLSAggregator
}

// NewGnarkBLSCryptoProvider creates a BLS crypto provider.
func NewGnarkBLSCryptoProvider() *GnarkBLSCryptoProvider {
	return &GnarkBLSCryptoProvider{
		aggregator: NewGnarkBLSAggregator(),
	}
}

func (p *GnarkBLSCryptoProvider) Scheme() SignatureScheme {
	return SignatureSchemeBLS
}

func (p *GnarkBLSCryptoProvider) NewBatchVerifier(validators ValidatorSet) BatchVerifier {
	return NewBLSBatchVerifier(validators, NewGnarkBLSBatchVerifier())
}

func (p *GnarkBLSCryptoProvider) SupportsAggregation() bool {
	return true
}

func (p *GnarkBLSCryptoProvider) Aggregator() BLSAggregator {
	return p.aggregator
}

// BLSPublicKey wraps a BLS public key for the PublicKey interface.
type BLSPublicKey struct {
	bytes []byte
	point bls12381.G2Affine
}

// NewBLSPublicKey creates a BLS public key from bytes.
func NewBLSPublicKey(b []byte) (*BLSPublicKey, error) {
	pk := &BLSPublicKey{bytes: make([]byte, len(b))}
	copy(pk.bytes, b)
	if _, err := pk.point.SetBytes(b); err != nil {
		return nil, fmt.Errorf("invalid BLS public key: %w", err)
	}
	return pk, nil
}

func (k *BLSPublicKey) Bytes() []byte {
	return k.bytes
}

func (k *BLSPublicKey) Verify(message, signature []byte) bool {
	var sig bls12381.G1Affine
	if _, err := sig.SetBytes(signature); err != nil {
		return false
	}

	// Hash message to G1
	hashPoint, err := bls12381.HashToG1(message, []byte(blsDST))
	if err != nil {
		return false
	}

	// Verify: e(H(m), pk) == e(sig, G2)
	_, _, _, g2Gen := bls12381.Generators()

	left, err := bls12381.Pair([]bls12381.G1Affine{hashPoint}, []bls12381.G2Affine{k.point})
	if err != nil {
		return false
	}

	right, err := bls12381.Pair([]bls12381.G1Affine{sig}, []bls12381.G2Affine{g2Gen})
	if err != nil {
		return false
	}

	return left.Equal(&right)
}

func (k *BLSPublicKey) Equals(other interface{ Bytes() []byte }) bool {
	if other == nil {
		return false
	}
	// Try direct type assertion first for efficiency
	if o, ok := other.(*BLSPublicKey); ok {
		return k.point.Equal(&o.point)
	}
	// Fall back to byte comparison for cross-implementation compatibility
	otherBytes := other.Bytes()
	return len(otherBytes) == len(k.Bytes()) && string(otherBytes) == string(k.Bytes())
}

// BLSSigner implements the Signer interface for BLS.
type BLSSigner struct {
	privateKey []byte
	publicKey  *BLSPublicKey
}

// NewBLSSigner creates a new BLS signer from a key pair.
func NewBLSSigner(keyPair *BLSKeyPair) (*BLSSigner, error) {
	pk, err := NewBLSPublicKey(keyPair.PublicKey)
	if err != nil {
		return nil, err
	}
	return &BLSSigner{
		privateKey: keyPair.PrivateKey,
		publicKey:  pk,
	}, nil
}

func (s *BLSSigner) Sign(message []byte) ([]byte, error) {
	return BLSSign(s.privateKey, message)
}

func (s *BLSSigner) PublicKey() PublicKey {
	return s.publicKey
}

// BLSValidatorSet implements ValidatorSet for BLS validators.
type BLSValidatorSet struct {
	signers []*BLSSigner
}

// NewBLSValidatorSet creates a validator set with n BLS validators.
func NewBLSValidatorSet(n int) (*BLSValidatorSet, error) {
	signers := make([]*BLSSigner, n)
	for i := 0; i < n; i++ {
		keyPair, err := GenerateBLSKeyPair()
		if err != nil {
			return nil, err
		}
		signer, err := NewBLSSigner(keyPair)
		if err != nil {
			return nil, err
		}
		signers[i] = signer
	}
	return &BLSValidatorSet{signers: signers}, nil
}

func (v *BLSValidatorSet) Count() int {
	return len(v.signers)
}

func (v *BLSValidatorSet) GetByIndex(index uint16) (PublicKey, error) {
	if int(index) >= len(v.signers) {
		return nil, fmt.Errorf("index %d out of range", index)
	}
	return v.signers[index].PublicKey(), nil
}

func (v *BLSValidatorSet) Contains(index uint16) bool {
	return int(index) < len(v.signers)
}

func (v *BLSValidatorSet) F() int {
	return (len(v.signers) - 1) / 3
}

// GetSigner returns the signer for a validator index.
func (v *BLSValidatorSet) GetSigner(index uint16) *BLSSigner {
	if int(index) >= len(v.signers) {
		return nil
	}
	return v.signers[index]
}

// Ensure interfaces are implemented
var _ BLSBatchVerifier = (*GnarkBLSBatchVerifier)(nil)
var _ BLSAggregator = (*GnarkBLSAggregator)(nil)
var _ CryptoProvider = (*GnarkBLSCryptoProvider)(nil)
var _ PublicKey = (*BLSPublicKey)(nil)
var _ Signer = (*BLSSigner)(nil)
var _ ValidatorSet = (*BLSValidatorSet)(nil)
