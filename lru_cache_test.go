package narwhal_test

import (
	"crypto/sha256"
	"testing"

	"github.com/edgedlt/narwhal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testHash is a simple hash type for testing
type testHash [32]byte

func (h testHash) Bytes() []byte  { return h[:] }
func (h testHash) String() string { return string(h[:8]) }
func (h testHash) Equals(other narwhal.Hash) bool {
	if o, ok := other.(testHash); ok {
		return h == o
	}
	return false
}

func computeTestHash(data []byte) testHash {
	return sha256.Sum256(data)
}

// testTransaction is a simple transaction type for testing
type testTransaction struct {
	data []byte
	hash testHash
}

func (t *testTransaction) Hash() testHash { return t.hash }
func (t *testTransaction) Bytes() []byte  { return t.data }

func TestLRUCache_Basic(t *testing.T) {
	cache := narwhal.NewLRUCache[string, int](3)

	// Put and Get
	cache.Put("a", 1)
	cache.Put("b", 2)
	cache.Put("c", 3)

	v, ok := cache.Get("a")
	assert.True(t, ok)
	assert.Equal(t, 1, v)

	v, ok = cache.Get("b")
	assert.True(t, ok)
	assert.Equal(t, 2, v)

	assert.Equal(t, 3, cache.Len())
}

func TestLRUCache_Eviction(t *testing.T) {
	cache := narwhal.NewLRUCache[string, int](3)

	cache.Put("a", 1)
	cache.Put("b", 2)
	cache.Put("c", 3)

	// Access "a" to make it most recently used
	cache.Get("a")

	// Add "d" - should evict "b" (LRU)
	evictedKey, evictedValue, evicted := cache.Put("d", 4)
	assert.True(t, evicted)
	assert.Equal(t, "b", evictedKey)
	assert.Equal(t, 2, evictedValue)

	// "b" should be gone
	_, ok := cache.Get("b")
	assert.False(t, ok)

	// "a", "c", "d" should still be there
	_, ok = cache.Get("a")
	assert.True(t, ok)
	_, ok = cache.Get("c")
	assert.True(t, ok)
	_, ok = cache.Get("d")
	assert.True(t, ok)
}

func TestLRUCache_Update(t *testing.T) {
	cache := narwhal.NewLRUCache[string, int](3)

	cache.Put("a", 1)
	cache.Put("b", 2)
	cache.Put("c", 3)

	// Update "a"
	_, _, evicted := cache.Put("a", 100)
	assert.False(t, evicted) // No eviction on update

	v, ok := cache.Get("a")
	assert.True(t, ok)
	assert.Equal(t, 100, v)

	assert.Equal(t, 3, cache.Len())
}

func TestLRUCache_Remove(t *testing.T) {
	cache := narwhal.NewLRUCache[string, int](3)

	cache.Put("a", 1)
	cache.Put("b", 2)

	assert.True(t, cache.Remove("a"))
	assert.False(t, cache.Remove("x")) // Not present

	_, ok := cache.Get("a")
	assert.False(t, ok)

	assert.Equal(t, 1, cache.Len())
}

func TestLRUCache_Contains(t *testing.T) {
	cache := narwhal.NewLRUCache[string, int](3)

	cache.Put("a", 1)

	assert.True(t, cache.Contains("a"))
	assert.False(t, cache.Contains("b"))
}

func TestLRUCache_Peek(t *testing.T) {
	cache := narwhal.NewLRUCache[string, int](3)

	cache.Put("a", 1)
	cache.Put("b", 2)
	cache.Put("c", 3)

	// Peek "a" without updating LRU order
	v, ok := cache.Peek("a")
	assert.True(t, ok)
	assert.Equal(t, 1, v)

	// Add "d" - should evict "a" because Peek didn't update order
	evictedKey, _, evicted := cache.Put("d", 4)
	assert.True(t, evicted)
	assert.Equal(t, "a", evictedKey)
}

func TestLRUCache_Clear(t *testing.T) {
	cache := narwhal.NewLRUCache[string, int](3)

	cache.Put("a", 1)
	cache.Put("b", 2)

	cache.Clear()

	assert.Equal(t, 0, cache.Len())
	assert.False(t, cache.Contains("a"))
}

func TestLRUCache_Keys(t *testing.T) {
	cache := narwhal.NewLRUCache[string, int](3)

	cache.Put("a", 1)
	cache.Put("b", 2)
	cache.Put("c", 3)

	// Access "a" to make it most recent
	cache.Get("a")

	keys := cache.Keys()
	assert.Len(t, keys, 3)
	// Most recent first
	assert.Equal(t, "a", keys[0])
}

func TestLRUCache_Resize(t *testing.T) {
	cache := narwhal.NewLRUCache[string, int](5)

	cache.Put("a", 1)
	cache.Put("b", 2)
	cache.Put("c", 3)
	cache.Put("d", 4)
	cache.Put("e", 5)

	// Resize to smaller capacity - should evict LRU items
	evicted := cache.Resize(3)
	assert.Equal(t, 2, evicted)
	assert.Equal(t, 3, cache.Len())

	// "a" and "b" should be evicted (oldest)
	assert.False(t, cache.Contains("a"))
	assert.False(t, cache.Contains("b"))
	assert.True(t, cache.Contains("c"))
	assert.True(t, cache.Contains("d"))
	assert.True(t, cache.Contains("e"))
}

func TestLRUCache_Stats(t *testing.T) {
	cache := narwhal.NewLRUCache[string, int](3)

	cache.Put("a", 1)
	cache.Put("b", 2)

	cache.Get("a") // Hit
	cache.Get("x") // Miss
	cache.Get("y") // Miss

	cache.Put("c", 3)
	cache.Put("d", 4) // Causes eviction

	stats := cache.Stats()
	assert.Equal(t, 3, stats.Size)
	assert.Equal(t, 3, stats.Capacity)
	assert.Equal(t, uint64(1), stats.Hits)
	assert.Equal(t, uint64(2), stats.Misses)
	assert.Equal(t, uint64(1), stats.Evicts)
	assert.InDelta(t, 0.333, stats.HitRate, 0.01)
}

func TestLRUCache_MinCapacity(t *testing.T) {
	// Capacity should be at least 1
	cache := narwhal.NewLRUCache[string, int](0)
	assert.Equal(t, 1, cache.Stats().Capacity)

	cache = narwhal.NewLRUCache[string, int](-5)
	assert.Equal(t, 1, cache.Stats().Capacity)
}

func TestCertificateCache(t *testing.T) {
	cache := narwhal.NewCertificateCache[testHash](10)

	header := &narwhal.Header[testHash]{
		Author: 0,
		Round:  1,
	}
	header.ComputeDigest(computeTestHash)

	cert := narwhal.NewCertificate(header, map[uint16][]byte{0: {1, 2, 3}})

	// Put and Get
	cache.Put(cert)
	assert.True(t, cache.Contains(header.Digest))

	retrieved, ok := cache.Get(header.Digest)
	require.True(t, ok)
	assert.Equal(t, cert.Header.Round, retrieved.Header.Round)

	// Remove
	assert.True(t, cache.Remove(header.Digest))
	assert.False(t, cache.Contains(header.Digest))
}

func TestVertexCache(t *testing.T) {
	cache := narwhal.NewVertexCache[testHash, *testTransaction](10)

	header := &narwhal.Header[testHash]{
		Author: 0,
		Round:  1,
	}
	header.ComputeDigest(computeTestHash)

	cert := narwhal.NewCertificate(header, map[uint16][]byte{0: {1, 2, 3}})
	vertex := &narwhal.Vertex[testHash, *testTransaction]{
		Certificate: cert,
		Batches:     nil,
	}

	// Put and Get
	cache.Put(vertex)
	assert.True(t, cache.Contains(header.Digest))

	retrieved, ok := cache.Get(header.Digest)
	require.True(t, ok)
	assert.Equal(t, vertex.Certificate.Header.Round, retrieved.Certificate.Header.Round)

	// Stats
	stats := cache.Stats()
	assert.Equal(t, 1, stats.Size)
}

// Benchmark LRU cache operations
func BenchmarkLRUCache_Get(b *testing.B) {
	cache := narwhal.NewLRUCache[int, int](10000)
	for i := 0; i < 10000; i++ {
		cache.Put(i, i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(i % 10000)
	}
}

func BenchmarkLRUCache_Put(b *testing.B) {
	cache := narwhal.NewLRUCache[int, int](10000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Put(i%10000, i)
	}
}

func BenchmarkLRUCache_PutEviction(b *testing.B) {
	cache := narwhal.NewLRUCache[int, int](1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Put(i, i) // Will cause evictions after first 1000
	}
}
