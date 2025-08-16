package memtable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time" // Import time

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/utils"
	"github.com/INLOpen/skiplist"
)

// --- Custom, GC-friendly pools for MemtableKey and MemtableEntry ---

type memtableKeyPool struct {
	mu    sync.Mutex
	items []*MemtableKey
	// Metrics
	hits   atomic.Uint64
	misses atomic.Uint64
}

func newMemtableKeyPool(size int) *memtableKeyPool {
	return &memtableKeyPool{
		items: make([]*MemtableKey, 0, size),
	}
}

func (p *memtableKeyPool) GetMetrics() (hits, misses uint64, currentSize int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.hits.Load(), p.misses.Load(), len(p.items)
}

func (p *memtableKeyPool) Get() *MemtableKey {
	p.mu.Lock()
	if len(p.items) == 0 {
		p.mu.Unlock()
		p.misses.Add(1)
		return &MemtableKey{} // Allocate new if empty
	}
	p.hits.Add(1)
	item := p.items[len(p.items)-1]
	p.items = p.items[:len(p.items)-1]
	p.mu.Unlock()
	return item
}

func (p *memtableKeyPool) Put(k *MemtableKey) {
	// Reset the key to avoid holding onto old data.
	// This is important for GC and to prevent accidental reuse of stale data.
	k.Key = nil   // The slice header is kept, but the backing array can be GC'd if not referenced elsewhere.
	k.PointID = 0 // Reset point id.
	p.mu.Lock()
	p.items = append(p.items, k)
	p.mu.Unlock()
}

type memtableEntryPool struct {
	mu    sync.Mutex
	items []*MemtableEntry
	// Metrics
	hits   atomic.Uint64
	misses atomic.Uint64
}

func newMemtableEntryPool(size int) *memtableEntryPool {
	return &memtableEntryPool{
		items: make([]*MemtableEntry, 0, size),
	}
}

func (p *memtableEntryPool) GetMetrics() (hits, misses uint64, currentSize int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.hits.Load(), p.misses.Load(), len(p.items)
}

func (p *memtableEntryPool) Get() *MemtableEntry {
	p.mu.Lock()
	if len(p.items) == 0 {
		p.mu.Unlock()
		p.misses.Add(1)
		return &MemtableEntry{} // Allocate new if empty
	}
	p.hits.Add(1)
	item := p.items[len(p.items)-1]
	p.items = p.items[:len(p.items)-1]
	p.mu.Unlock()
	return item
}

func (p *memtableEntryPool) Put(e *MemtableEntry) {
	// Reset fields to avoid holding onto old data.
	e.Key = nil     // The slice header is kept, but the backing array can be GC'd.
	e.Value = nil   // Same as above.
	e.EntryType = 0 // Reset entry type.
	e.PointID = 0   // Reset point id.
	p.mu.Lock()
	p.items = append(p.items, e)
	p.mu.Unlock()
}

// MemtableEntry represents a single key-value operation in the memtable.
// It implements skiplist.Comparable.

type MemtableKey struct {
	Key     []byte
	PointID uint64 // Point ID for this entry (FR5.3)
}

func (mk *MemtableKey) Bytes() []byte {
	buf := make([]byte, len(mk.Key)+binary.MaxVarintLen64)
	copy(buf, mk.Key)
	binary.BigEndian.PutUint64(buf[len(mk.Key):], mk.PointID)
	return buf
}

type MemtableEntry struct {
	Key       []byte
	Value     []byte
	EntryType core.EntryType
	PointID   uint64 // Point id for this entry (FR5.3)
}

// size returns the estimated memory size of the entry.
func (e *MemtableEntry) size() int64 {
	return int64(len(e.Key) + len(e.Value) + binary.MaxVarintLen64 + 1 /*EntryType*/)
}

var (
	KeyPool   = newMemtableKeyPool(16384)   // Pool for MemtableKey, increased size for high ingestion rates.
	EntryPool = newMemtableEntryPool(16384) // Pool for MemtableEntry, increased size for high ingestion rates.
)

func comparator(a, b *MemtableKey) int {
	cmp := bytes.Compare(a.Key, b.Key)
	if cmp != 0 {
		return cmp // Sort by key first
	}

	// Keys are equal, sort by point id descending.
	// If entryA.PointID > entryB.PointID, entryA comes before entryB, so entryA is "less".
	if a.PointID > b.PointID {
		return -1
	}
	if a.PointID < b.PointID {
		return 1
	}
	return 0 // Keys and PointID are equal
}

// Memtable is an in-memory, sorted data structure that buffers incoming writes.
// Corresponds to FR3.1.
type Memtable struct {
	mu                  sync.RWMutex
	data                *skiplist.SkipList[*MemtableKey, *MemtableEntry] // Use SkipList from huandu/skiplist
	sizeBytes           int64                                            // Estimated size of data in bytes
	threshold           int64                                            // Threshold for when the memtable is considered full
	FlushRetries        int                                              // Number of times this memtable has failed to flush (FR5.4 Error Handling)
	NextRetryDelay      time.Duration                                    // Delay before the next flush attempt (FR5.4 Error Handling)
	CreationTime        time.Time                                        // Time the memtable was created
	LastWALSegmentIndex uint64
	CompletionChan      chan error // For synchronous flush operations
	Err                 error      // Stores the last error encountered during a flush attempt
}

// NewMemtable creates a new Memtable with a given size threshold.
// Corresponds to FR3.1, FR3.2 (threshold part).
func NewMemtable(threshold int64, clock utils.Clock) *Memtable {
	return &Memtable{
		data:           skiplist.NewWithComparator[*MemtableKey, *MemtableEntry](comparator), // Pass an instance of the Comparable type
		threshold:      threshold,
		sizeBytes:      0, // Start with 0 size
		FlushRetries:   0, // Initialize flush retries
		CreationTime:   clock.Now(),
		CompletionChan: nil, // Default to nil, only set for synchronous flushes
		Err:            nil,
	}
}

// Put adds or updates a key-value pair or a tombstone in the memtable.
// Entries are kept sorted by key. If a key already exists, it's updated.
// Corresponds to FR1.1, FR1.2, FR3.1.
func (m *Memtable) Put(key, value []byte, entryType core.EntryType, pointID uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	newKey := KeyPool.Get()
	newKey.Key = key
	newKey.PointID = pointID

	newEntry := EntryPool.Get()
	newEntry.Key = key
	newEntry.Value = value
	newEntry.EntryType = entryType
	newEntry.PointID = pointID

	// Because our comparator includes the point id, inserting a key
	// with a new point id will always result in a new node being added
	// rather than an old one being updated. `Insert` will only return a non-nil
	// oldNode if we insert a key with an identical point id, which is
	// effectively a value update for the same version.
	oldNode := m.data.Insert(newKey, newEntry)

	if oldNode != nil {
		// An entry with the exact same key and point id was found.
		// The skiplist updates the node's value in-place and returns the old value.
		// The `newKey` object is now unused and must be returned to the pool.
		// The old value object must also be returned to the pool.
		KeyPool.Put(newKey)
		oldValue := oldNode.Value()
		EntryPool.Put(oldValue)

		m.sizeBytes -= oldValue.size()
	}
	// Add the size of the new or updated entry.
	m.sizeBytes += newEntry.size()
	return nil
}

// Get retrieves an entry from the memtable.
// Returns the value, entry type, and a boolean indicating if the key was found.
// If multiple versions of a key exist (due to PointID), this returns the one with the highest PointID
// because Put ensures it's sorted that way.
// Corresponds to FR1.3 (partially), FR3.1.
func (m *Memtable) Get(key []byte) (value []byte, entryType core.EntryType, found bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// To get the "latest" version (highest PointID for a given key), we need to
	// find the first entry with that key, because our comparator sorts by PointID
	// descending (a higher PointID is "less than" a lower one). We seek to the
	// smallest possible key for the given user key, which is (key, max_seq_num).
	searchKey := KeyPool.Get()
	searchKey.Key = key
	searchKey.PointID = ^uint64(0) // Max PointID to find the first entry for this key
	defer KeyPool.Put(searchKey)

	// Seek for the first element that is greater than or equal to our search key.
	// Because of our comparator (Key ASC, PointID DESC), this will find the
	// entry for the given key with the highest point id.
	node, ok := m.data.Seek(searchKey)

	if ok {
		// A node was found. Check if its key actually matches.
		foundKey := node.Key()
		if bytes.Equal(foundKey.Key, key) { // It's a match for our key.
			entry := node.Value()
			// A tombstone is a valid entry. The caller decides how to interpret it.
			if entry.EntryType == core.EntryTypeDelete {
				return nil, entry.EntryType, true
			}
			return entry.Value, entry.EntryType, true // Key found and it's a Put entry
		}
	}
	return nil, 0, false
}

// Size returns the estimated size of the data in the memtable in bytes.
// Corresponds to FR3.2.
func (m *Memtable) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sizeBytes
}

// IsFull checks if the memtable has reached its size threshold.
// Corresponds to FR3.2.
func (m *Memtable) IsFull() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sizeBytes >= m.threshold
}

// Len returns the number of entries in the memtable.
func (m *Memtable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.data.Len()
}

// NewIterator creates a new iterator for the Memtable.
// The iterator holds a read lock on the memtable for its lifetime.
// The caller MUST call Close() on the iterator to release the lock.
func (m *Memtable) NewIterator(startKey, endKey []byte, order core.SortOrder) core.IteratorInterface[*core.IteratorNode] {
	m.mu.RLock() // Acquire read lock
	opts := make([]skiplist.IteratorOption[*MemtableKey, *MemtableEntry], 0)
	if order == core.Descending {
		opts = append(opts, skiplist.WithReverse[*MemtableKey, *MemtableEntry]())
	}

	iter := m.data.NewIterator(opts...)

	return &MemtableIterator{
		mu:       &m.mu,
		iter:     iter,
		startKey: startKey,
		endKey:   endKey,
		order:    order,
		valid:    false, // valid is false so Next() knows it's the first call
	}
}

// FlushToSSTable writes all entries from the memtable to the given SSTableWriter.
// This is typically called when an immutable memtable is being flushed to disk.
// This method iterates over all entries, including older versions of keys, to ensure
// the full history is persisted.
// Corresponds to FR3.3.
func (m *Memtable) FlushToSSTable(writer core.SSTableWriterInterface) error {
	m.mu.RLock() // Use RLock as we are only reading
	defer m.mu.RUnlock()

	// The skiplist iterator will traverse all nodes in sorted order (Key ASC, PointID DESC).
	// This is exactly what we want to write to the SSTable, as it preserves all versions
	// of a key within the memtable's lifetime. The compaction process will later
	// handle the removal of older versions.
	iter := m.data.NewIterator()
	for iter.Next() {
		memKey := iter.Key()
		memEntry := iter.Value()

		// The key for SSTable is the user key, and the point id is passed separately.
		// Both MemtableKey and MemtableEntry contain the Key and PointID, we can use either.
		// Using memEntry for consistency.
		err := writer.Add(memEntry.Key, memEntry.Value, memEntry.EntryType, memKey.PointID)
		if err != nil {
			return fmt.Errorf("failed to add memtable entry to sstable writer (key: %s): %w", string(memEntry.Key), err)
		}
	}
	return nil
}

// Close releases the resources used by the memtable, returning its entries to the pool.
// This should be called when the memtable is no longer needed (e.g., after being flushed and compacted).
func (m *Memtable) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.data == nil {
		return
	}

	// Iterate through all nodes and return their keys and values to the respective pools.
	m.data.Range(func(key *MemtableKey, value *MemtableEntry) bool {
		// No need to reset fields as they will be overwritten when reused from the pool.
		KeyPool.Put(key)
		EntryPool.Put(value)
		return true
	})

	// After ranging over the data and returning the keys/values to our pools,
	// we just need to nil out the reference to the skiplist. The garbage collector
	// will handle the skiplist's internal nodes. Calling m.data.Clear() is
	// destructive to the objects we just put back into our pools.
	m.data = nil
	m.sizeBytes = 0
}
