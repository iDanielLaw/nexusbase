package memtable

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
)

// Pool provides object pooling for memory-intensive structures to reduce GC pressure.
// This implementation is safe for concurrent use.

const (
	// maxPoolSize limits the maximum number of items in each pool to prevent unbounded growth.
	// Set to a large value to handle burst traffic while preventing memory leaks.
	maxPoolSize = 32768
)

// keyPool manages a pool of MemtableKey objects to reduce allocations.
type keyPool struct {
	sp     sync.Pool
	hits   atomic.Uint64
	misses atomic.Uint64
}

// newKeyPool creates a new key pool backed by sync.Pool.
func newKeyPool(capacity int) *keyPool {
	p := &keyPool{}
	p.sp.New = func() any { return &MemtableKey{} }
	return p
}

// Get retrieves a MemtableKey from the sync.Pool or allocates a new one.
func (p *keyPool) Get() *MemtableKey {
	v := p.sp.Get()
	if v == nil {
		p.misses.Add(1)
		item := &MemtableKey{}
		slog.Default().Debug("KeyPool: new alloc", "ptr", fmt.Sprintf("%p", item))
		return item
	}
	p.hits.Add(1)
	item := v.(*MemtableKey)
	slog.Default().Debug("KeyPool: reuse", "ptr", fmt.Sprintf("%p", item))
	return item
}

// Put returns a MemtableKey to the pool for reuse.
func (p *keyPool) Put(k *MemtableKey) {
	if k == nil {
		return
	}
	k.Key = nil
	k.PointID = 0
	slog.Default().Debug("KeyPool: put back", "ptr", fmt.Sprintf("%p", k))
	p.sp.Put(k)
}

// GetMetrics returns approximate pool metrics (hits, misses, size unknown for sync.Pool).
func (p *keyPool) GetMetrics() (hits, misses uint64, size int) {
	return p.hits.Load(), p.misses.Load(), 0
}

// entryPool manages a pool of MemtableEntry objects to reduce allocations.
type entryPool struct {
	mu    sync.Mutex
	items []*MemtableEntry
	// Metrics for monitoring pool effectiveness
	hits   atomic.Uint64
	misses atomic.Uint64
}

// newEntryPool creates a new entry pool with the specified initial capacity.
func newEntryPool(capacity int) *entryPool {
	return &entryPool{
		items: make([]*MemtableEntry, 0, capacity),
	}
}

// Get retrieves a MemtableEntry from the pool or allocates a new one if the pool is empty.
func (p *entryPool) Get() *MemtableEntry {
	p.mu.Lock()
	if len(p.items) == 0 {
		p.mu.Unlock()
		p.misses.Add(1)
		item := &MemtableEntry{}
		slog.Default().Debug("EntryPool: new alloc", "ptr", fmt.Sprintf("%p", item))
		return item
	}

	item := p.items[len(p.items)-1]
	p.items = p.items[:len(p.items)-1]
	p.mu.Unlock()

	// Update metrics after releasing lock (atomic operation is thread-safe)
	p.hits.Add(1)
	slog.Default().Debug("EntryPool: reuse", "ptr", fmt.Sprintf("%p", item))
	return item
}

// Put returns a MemtableEntry to the pool for reuse.
// If the pool has reached maxPoolSize, the object is discarded to prevent unbounded growth.
func (p *entryPool) Put(e *MemtableEntry) {
	if e == nil {
		return
	}

	// Log the value backing pointer (if present) for debugging aliasing issues.
	slog.Default().Debug("EntryPool: put back", "ptr", fmt.Sprintf("%p", e), "value_ptr", func() string {
		if e != nil && len(e.Value) > 0 {
			return fmt.Sprintf("%p", &e.Value[0])
		}
		return ""
	}())

	// Reset fields to avoid holding onto old data
	e.Key = nil
	e.Value = nil
	e.EntryType = 0
	e.PointID = 0

	p.mu.Lock()
	// Only add to pool if below max capacity to prevent unbounded growth
	if len(p.items) < maxPoolSize {
		p.items = append(p.items, e)
	}
	p.mu.Unlock()
}

// GetMetrics returns pool usage statistics.
func (p *entryPool) GetMetrics() (hits, misses uint64, size int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.hits.Load(), p.misses.Load(), len(p.items)
}

// Global pools for MemtableKey and MemtableEntry objects.
// Pool size is set to 16384 to handle high ingestion rates with minimal allocations.
var (
	KeyPool   = newKeyPool(16384)
	EntryPool = newDelayedEntryPool(16384)
)
