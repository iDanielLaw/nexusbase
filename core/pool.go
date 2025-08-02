package core

import (
	"bytes"
	"sync"
	"sync/atomic"
)

// GenericPool is a generic wrapper around sync.Pool
type GenericPool[T any] struct {
	pool sync.Pool
}

// NewGenericPool creates a new GenericPool with a function to create new items.
func NewGenericPool[T any](newItem func() T) *GenericPool[T] {
	return &GenericPool[T]{
		pool: sync.Pool{
			New: func() interface{} {
				return newItem()
			},
		},
	}
}

// Get retrieves an item from the pool.
func (p *GenericPool[T]) Get() T {
	return p.pool.Get().(T)
}

// Put returns an item to the pool.
func (p *GenericPool[T]) Put(item T) {
	p.pool.Put(item)
}

// bufferPool is a custom, GC-friendly pool implementation using a mutex-protected slice.
// Unlike sync.Pool, its contents are not cleared by the garbage collector, making it
// suitable for pooling larger, reusable objects like decompression buffers during
// long-running, memory-intensive operations like compaction.
type bufferPool struct {
	mu      sync.Mutex
	items   []*bytes.Buffer
	newFunc func() *bytes.Buffer

	// Metrics
	hits        atomic.Uint64 // Number of times a buffer was successfully retrieved from the pool.
	misses      atomic.Uint64 // Number of times a buffer was requested but the pool was empty.
	created     atomic.Uint64 // Total number of new buffers created.
	currentSize atomic.Int64  // Current number of items in the pool.
}

// DefaultBlockDecompressionSize is a reasonable default capacity for buffers
// used for decompressing SSTable blocks.
const DefaultBlockDecompressionSize = 4 * 1024 // 32KB

var BufferPool = NewBufferPool(DefaultBlockDecompressionSize)

// NewBufferPool creates a new buffer pool.
// initialCapacity is the pre-allocated capacity for each new buffer.
func NewBufferPool(initialCapacity ...int) *bufferPool {
	capacity := 0
	if len(initialCapacity) > 0 && initialCapacity[0] > 0 {
		capacity = initialCapacity[0]
	}
	// Pre-allocate the pool's internal slice to a reasonable size to reduce re-allocations.
	const initialPoolSize = 16384 // Increased pool size to better handle high concurrency during compaction/startup.
	bp := &bufferPool{
		items: make([]*bytes.Buffer, 0, initialPoolSize), // Start with len 0, cap initialPoolSize
	}
	bp.newFunc = func() *bytes.Buffer {
		bp.created.Add(1)
		return bytes.NewBuffer(make([]byte, 0, capacity))
	}

	// Pre-warm the pool by creating initial items.
	for i := 0; i < initialPoolSize; i++ {
		bp.items = append(bp.items, bp.newFunc())
	}
	// Initialize the size counter after pre-warming.
	// The created counter is already incremented by newFunc.
	bp.currentSize.Store(int64(initialPoolSize))

	return bp
}

// Get retrieves a buffer from the pool. If the pool is empty, it creates a new one.
func (bp *bufferPool) Get() *bytes.Buffer {
	bp.mu.Lock()
	if len(bp.items) == 0 {
		bp.mu.Unlock()
		bp.misses.Add(1)
		return bp.newFunc()
	}
	bp.hits.Add(1)
	bp.currentSize.Add(-1)
	item := bp.items[len(bp.items)-1]
	bp.items = bp.items[:len(bp.items)-1]
	bp.mu.Unlock()
	return item
}

// GetMetrics returns the current metrics for the pool.
func (bp *bufferPool) GetMetrics() (hits, misses, created uint64, currentSize int64) {
	return bp.hits.Load(), bp.misses.Load(), bp.created.Load(), bp.currentSize.Load()
}

// Put returns a buffer to the pool. It is never discarded.
func (bp *bufferPool) Put(buf *bytes.Buffer) {
	buf.Reset()
	bp.mu.Lock()
	bp.items = append(bp.items, buf)
	bp.currentSize.Add(1)
	bp.mu.Unlock()
}

// Generic Buffer Pool
type GenericBufferPoolOptions[T any] struct {
	newFunc    func() T
	removeFunc func(T)
}

type bufferGenericPool[T any] struct {
	opts    GenericBufferPoolOptions[T]
	mu      sync.Mutex
	items   []T
	newFunc func() T

	// Metrics
	hits        atomic.Uint64 // Number of times a buffer was successfully retrieved from the pool.
	misses      atomic.Uint64 // Number of times a buffer was requested but the pool was empty.
	created     atomic.Uint64 // Total number of new buffers created.
	currentSize atomic.Int64  // Current number of items in the pool.
}

func NewBufferGenericPool[T any](opts GenericBufferPoolOptions[T]) *bufferGenericPool[T] {
	// Pre-allocate the pool's internal slice to a reasonable size to reduce re-allocations.
	const initialPoolSize = 16384 // Increased pool size to better handle high concurrency during compaction/startup.
	bp := &bufferGenericPool[T]{
		opts:  opts,
		items: make([]T, 0, initialPoolSize), // Start with len 0, cap initialPoolSize
	}
	bp.newFunc = func() T {
		bp.created.Add(1)
		return opts.newFunc()
	}

	// Pre-warm the pool by creating initial items.
	for i := 0; i < initialPoolSize; i++ {
		bp.items = append(bp.items, bp.newFunc())
	}
	// Initialize the size counter after pre-warming.
	// The created counter is already incremented by newFunc.
	bp.currentSize.Store(int64(initialPoolSize))

	return bp
}

// Get retrieves a buffer from the pool. If the pool is empty, it creates a new one.
func (bp *bufferGenericPool[T]) Get() T {
	bp.mu.Lock()
	if len(bp.items) == 0 {
		bp.mu.Unlock()
		bp.misses.Add(1)
		return bp.newFunc()
	}
	bp.hits.Add(1)
	bp.currentSize.Add(-1)
	item := bp.items[len(bp.items)-1]
	bp.items = bp.items[:len(bp.items)-1]
	bp.mu.Unlock()
	return item
}

// GetMetrics returns the current metrics for the pool.
func (bp *bufferGenericPool[T]) GetMetrics() (hits, misses, created uint64, currentSize int64) {
	return bp.hits.Load(), bp.misses.Load(), bp.created.Load(), bp.currentSize.Load()
}

// Put returns a buffer to the pool. It is never discarded.
func (bp *bufferGenericPool[T]) Put(buf T) {
	bp.opts.removeFunc(buf)
	bp.mu.Lock()
	bp.items = append(bp.items, buf)
	bp.currentSize.Add(1)
	bp.mu.Unlock()
}
