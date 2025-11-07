package memtable

import (
	"sync"
	"sync/atomic"
)

// Pool provides object pooling for memory-intensive structures to reduce GC pressure.
// This implementation is safe for concurrent use.

// keyPool manages a pool of MemtableKey objects to reduce allocations.
type keyPool struct {
	mu    sync.Mutex
	items []*MemtableKey
	// Metrics for monitoring pool effectiveness
	hits   atomic.Uint64
	misses atomic.Uint64
}

// newKeyPool creates a new key pool with the specified initial capacity.
// A larger capacity reduces the likelihood of allocations during high ingestion rates.
func newKeyPool(capacity int) *keyPool {
	return &keyPool{
		items: make([]*MemtableKey, 0, capacity),
	}
}

// Get retrieves a MemtableKey from the pool or allocates a new one if the pool is empty.
// The returned object should be returned to the pool using Put() when no longer needed.
func (p *keyPool) Get() *MemtableKey {
	p.mu.Lock()
	if len(p.items) == 0 {
		p.mu.Unlock()
		p.misses.Add(1)
		return &MemtableKey{}
	}

	p.hits.Add(1)
	item := p.items[len(p.items)-1]
	p.items = p.items[:len(p.items)-1]
	p.mu.Unlock()

	return item
}

// Put returns a MemtableKey to the pool for reuse.
// The key is reset to prevent memory leaks and accidental data reuse.
func (p *keyPool) Put(k *MemtableKey) {
	if k == nil {
		return
	}

	// Reset to avoid holding onto old data (important for GC)
	k.Key = nil
	k.PointID = 0

	p.mu.Lock()
	p.items = append(p.items, k)
	p.mu.Unlock()
}

// GetMetrics returns pool usage statistics.
// Returns: hits (successful reuse), misses (new allocations), current pool size.
func (p *keyPool) GetMetrics() (hits, misses uint64, size int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.hits.Load(), p.misses.Load(), len(p.items)
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
		return &MemtableEntry{}
	}

	p.hits.Add(1)
	item := p.items[len(p.items)-1]
	p.items = p.items[:len(p.items)-1]
	p.mu.Unlock()

	return item
}

// Put returns a MemtableEntry to the pool for reuse.
func (p *entryPool) Put(e *MemtableEntry) {
	if e == nil {
		return
	}

	// Reset fields to avoid holding onto old data
	e.Key = nil
	e.Value = nil
	e.EntryType = 0
	e.PointID = 0

	p.mu.Lock()
	p.items = append(p.items, e)
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
	EntryPool = newEntryPool(16384)
)
