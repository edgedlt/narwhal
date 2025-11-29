package narwhal

import (
	"container/list"
	"sync"
)

// LRUCache is a generic thread-safe LRU cache.
// It evicts the least recently used items when capacity is exceeded.
type LRUCache[K comparable, V any] struct {
	mu       sync.RWMutex
	capacity int
	items    map[K]*list.Element
	order    *list.List // Front = most recent, Back = least recent

	// Stats
	hits   uint64
	misses uint64
	evicts uint64
}

type lruEntry[K comparable, V any] struct {
	key   K
	value V
}

// NewLRUCache creates a new LRU cache with the given capacity.
// Capacity must be at least 1.
func NewLRUCache[K comparable, V any](capacity int) *LRUCache[K, V] {
	if capacity < 1 {
		capacity = 1
	}
	return &LRUCache[K, V]{
		capacity: capacity,
		items:    make(map[K]*list.Element),
		order:    list.New(),
	}
}

// Get retrieves a value from the cache.
// Returns the value and true if found, zero value and false otherwise.
// Accessing an item moves it to the front (most recently used).
func (c *LRUCache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.order.MoveToFront(elem)
		c.hits++
		return elem.Value.(*lruEntry[K, V]).value, true
	}

	c.misses++
	var zero V
	return zero, false
}

// Peek retrieves a value without updating its position in the LRU order.
func (c *LRUCache[K, V]) Peek(key K) (V, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if elem, ok := c.items[key]; ok {
		return elem.Value.(*lruEntry[K, V]).value, true
	}

	var zero V
	return zero, false
}

// Put adds or updates a value in the cache.
// If the cache is at capacity, the least recently used item is evicted.
// Returns the evicted key and value if eviction occurred.
func (c *LRUCache[K, V]) Put(key K, value V) (evictedKey K, evictedValue V, evicted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Update existing
	if elem, ok := c.items[key]; ok {
		c.order.MoveToFront(elem)
		elem.Value.(*lruEntry[K, V]).value = value
		return
	}

	// Evict if at capacity
	if c.order.Len() >= c.capacity {
		oldest := c.order.Back()
		if oldest != nil {
			entry := oldest.Value.(*lruEntry[K, V])
			delete(c.items, entry.key)
			c.order.Remove(oldest)
			c.evicts++
			evictedKey = entry.key
			evictedValue = entry.value
			evicted = true
		}
	}

	// Add new
	entry := &lruEntry[K, V]{key: key, value: value}
	elem := c.order.PushFront(entry)
	c.items[key] = elem
	return
}

// Remove removes an item from the cache.
// Returns true if the item was found and removed.
func (c *LRUCache[K, V]) Remove(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		delete(c.items, key)
		c.order.Remove(elem)
		return true
	}
	return false
}

// Contains checks if a key exists in the cache without updating LRU order.
func (c *LRUCache[K, V]) Contains(key K) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.items[key]
	return ok
}

// Len returns the current number of items in the cache.
func (c *LRUCache[K, V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.order.Len()
}

// Clear removes all items from the cache.
func (c *LRUCache[K, V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[K]*list.Element)
	c.order = list.New()
}

// LRUCacheStats contains cache statistics.
type LRUCacheStats struct {
	Size     int
	Capacity int
	Hits     uint64
	Misses   uint64
	Evicts   uint64
	HitRate  float64
}

// Stats returns cache statistics.
func (c *LRUCache[K, V]) Stats() LRUCacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	total := c.hits + c.misses
	var hitRate float64
	if total > 0 {
		hitRate = float64(c.hits) / float64(total)
	}

	return LRUCacheStats{
		Size:     c.order.Len(),
		Capacity: c.capacity,
		Hits:     c.hits,
		Misses:   c.misses,
		Evicts:   c.evicts,
		HitRate:  hitRate,
	}
}

// Keys returns all keys in the cache, from most to least recently used.
func (c *LRUCache[K, V]) Keys() []K {
	c.mu.RLock()
	defer c.mu.RUnlock()

	keys := make([]K, 0, c.order.Len())
	for elem := c.order.Front(); elem != nil; elem = elem.Next() {
		keys = append(keys, elem.Value.(*lruEntry[K, V]).key)
	}
	return keys
}

// Resize changes the cache capacity.
// If the new capacity is smaller, excess items are evicted (LRU order).
func (c *LRUCache[K, V]) Resize(newCapacity int) int {
	if newCapacity < 1 {
		newCapacity = 1
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	evicted := 0
	c.capacity = newCapacity

	// Evict excess items
	for c.order.Len() > c.capacity {
		oldest := c.order.Back()
		if oldest != nil {
			entry := oldest.Value.(*lruEntry[K, V])
			delete(c.items, entry.key)
			c.order.Remove(oldest)
			c.evicts++
			evicted++
		}
	}

	return evicted
}

// CertificateCache is a specialized LRU cache for certificates.
// It uses certificate digests as keys and provides convenient methods
// for certificate lookups.
type CertificateCache[H Hash] struct {
	cache *LRUCache[string, *Certificate[H]]
}

// NewCertificateCache creates a new certificate cache with the given capacity.
func NewCertificateCache[H Hash](capacity int) *CertificateCache[H] {
	return &CertificateCache[H]{
		cache: NewLRUCache[string, *Certificate[H]](capacity),
	}
}

// Get retrieves a certificate by its digest.
func (c *CertificateCache[H]) Get(digest H) (*Certificate[H], bool) {
	return c.cache.Get(digest.String())
}

// Put adds a certificate to the cache.
func (c *CertificateCache[H]) Put(cert *Certificate[H]) {
	c.cache.Put(cert.Header.Digest.String(), cert)
}

// Contains checks if a certificate exists in the cache.
func (c *CertificateCache[H]) Contains(digest H) bool {
	return c.cache.Contains(digest.String())
}

// Remove removes a certificate from the cache.
func (c *CertificateCache[H]) Remove(digest H) bool {
	return c.cache.Remove(digest.String())
}

// Len returns the number of certificates in the cache.
func (c *CertificateCache[H]) Len() int {
	return c.cache.Len()
}

// Stats returns cache statistics.
func (c *CertificateCache[H]) Stats() LRUCacheStats {
	return c.cache.Stats()
}

// Clear removes all certificates from the cache.
func (c *CertificateCache[H]) Clear() {
	c.cache.Clear()
}

// VertexCache is a specialized LRU cache for vertices (certificate + batches).
type VertexCache[H Hash, T Transaction[H]] struct {
	cache *LRUCache[string, *Vertex[H, T]]
}

// NewVertexCache creates a new vertex cache with the given capacity.
func NewVertexCache[H Hash, T Transaction[H]](capacity int) *VertexCache[H, T] {
	return &VertexCache[H, T]{
		cache: NewLRUCache[string, *Vertex[H, T]](capacity),
	}
}

// Get retrieves a vertex by its certificate digest.
func (c *VertexCache[H, T]) Get(digest H) (*Vertex[H, T], bool) {
	return c.cache.Get(digest.String())
}

// Put adds a vertex to the cache.
func (c *VertexCache[H, T]) Put(vertex *Vertex[H, T]) {
	c.cache.Put(vertex.Certificate.Header.Digest.String(), vertex)
}

// Contains checks if a vertex exists in the cache.
func (c *VertexCache[H, T]) Contains(digest H) bool {
	return c.cache.Contains(digest.String())
}

// Remove removes a vertex from the cache.
func (c *VertexCache[H, T]) Remove(digest H) bool {
	return c.cache.Remove(digest.String())
}

// Len returns the number of vertices in the cache.
func (c *VertexCache[H, T]) Len() int {
	return c.cache.Len()
}

// Stats returns cache statistics.
func (c *VertexCache[H, T]) Stats() LRUCacheStats {
	return c.cache.Stats()
}

// Clear removes all vertices from the cache.
func (c *VertexCache[H, T]) Clear() {
	c.cache.Clear()
}
