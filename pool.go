package narwhal

import (
	"sync"
)

// ByteSlicePool provides a pool of reusable byte slices.
type ByteSlicePool struct {
	pool sync.Pool
	size int
}

// NewByteSlicePool creates a new byte slice pool with the specified default size.
func NewByteSlicePool(defaultSize int) *ByteSlicePool {
	return &ByteSlicePool{
		size: defaultSize,
		pool: sync.Pool{
			New: func() interface{} {
				b := make([]byte, 0, defaultSize)
				return &b
			},
		},
	}
}

// Get retrieves a byte slice from the pool.
// The slice is reset to zero length but retains its capacity.
func (p *ByteSlicePool) Get() *[]byte {
	b := p.pool.Get().(*[]byte)
	*b = (*b)[:0]
	return b
}

// Put returns a byte slice to the pool.
// The slice should not be used after calling Put.
func (p *ByteSlicePool) Put(b *[]byte) {
	if cap(*b) >= p.size {
		p.pool.Put(b)
	}
}

// SlicePool provides a pool of reusable slices.
type SlicePool[T any] struct {
	pool    sync.Pool
	initCap int
}

// NewSlicePool creates a new slice pool with the specified initial capacity.
func NewSlicePool[T any](initCap int) *SlicePool[T] {
	return &SlicePool[T]{
		initCap: initCap,
		pool: sync.Pool{
			New: func() interface{} {
				s := make([]T, 0, initCap)
				return &s
			},
		},
	}
}

// Get retrieves a slice from the pool.
// The slice is reset to zero length but retains its capacity.
func (p *SlicePool[T]) Get() *[]T {
	s := p.pool.Get().(*[]T)
	*s = (*s)[:0]
	return s
}

// Put returns a slice to the pool.
func (p *SlicePool[T]) Put(s *[]T) {
	if cap(*s) >= p.initCap {
		p.pool.Put(s)
	}
}
