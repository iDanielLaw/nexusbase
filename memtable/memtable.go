package memtable

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexuscore/types"
	"github.com/INLOpen/nexuscore/utils/clock"
	"github.com/INLOpen/skiplist"
)

// Memtable is an in-memory, sorted data structure that buffers incoming writes.
// It uses a skip list for O(log n) insertion and lookup performance.
//
// Key Features:
// - Thread-safe: All operations are protected by RWMutex
// - Sorted storage: Keys are ordered lexicographically with MVCC support
// - Size tracking: Monitors memory usage to determine when flushing is needed
// - Retry logic: Handles flush failures with exponential backoff
//
// Lifecycle:
// 1. Active: Accepts writes until size threshold is reached
// 2. Immutable: Marked for flushing, no longer accepts writes
// 3. Flushed: Written to SSTable and eligible for cleanup
//
// Corresponds to FR3.1 (In-Memory Buffer), FR3.2 (Size Threshold), FR3.3 (Flush to Disk)
type Memtable struct {
	mu        sync.RWMutex                                     // Protects all fields
	data      *skiplist.SkipList[*MemtableKey, *MemtableEntry] // Sorted key-value store
	sizeBytes int64                                            // Current memory usage in bytes
	threshold int64                                            // Flush threshold in bytes

	// Flush retry logic (FR5.4 Error Handling)
	FlushRetries   int           // Number of failed flush attempts
	NextRetryDelay time.Duration // Delay before next retry (exponential backoff)

	// Metadata
	CreationTime        time.Time // When memtable was created
	LastWALSegmentIndex uint64    // Last WAL segment index for recovery

	// Synchronization
	CompletionChan chan error // For synchronous flush operations
	Err            error      // Last error encountered during flush
}

// NewMemtable creates a new Memtable with the specified size threshold.
//
// Parameters:
//   - threshold: Maximum size in bytes before the memtable should be flushed
//   - clk: Clock instance for timestamp tracking
//
// The memtable uses a skip list for efficient sorted storage with O(log n)
// insertion and lookup complexity.
//
// Corresponds to FR3.1 (In-Memory Buffer), FR3.2 (Size Threshold)
func NewMemtable(threshold int64, clk clock.Clock) *Memtable {
	return &Memtable{
		data:           skiplist.NewWithComparator[*MemtableKey, *MemtableEntry](comparator),
		threshold:      threshold,
		sizeBytes:      0,
		FlushRetries:   0,
		CreationTime:   clk.Now(),
		CompletionChan: nil,
		Err:            nil,
	}
}

// Put adds or updates a key-value pair in the memtable.
//
// Parameters:
//   - key: User-provided key (must not be modified after passing to Put)
//   - value: Encoded value bytes (nil for tombstones)
//   - entryType: Type of entry (EntryTypePutEvent or EntryTypeDelete)
//   - pointID: Sequence number for MVCC
//
// Behavior:
//   - New keys are inserted in sorted order
//   - Duplicate keys with different pointIDs create new versions
//   - Duplicate keys with the same pointID update the existing entry
//   - Updates memtable size for flush threshold tracking
//
// Thread Safety: Safe for concurrent use (protected by mutex)
//
// Corresponds to FR1.1 (Write), FR1.2 (Delete), FR3.1 (Memtable Operations)
func (m *Memtable) Put(key, value []byte, entryType core.EntryType, pointID uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Acquire objects from pools to reduce allocations
	newKey := KeyPool.Get()
	newKey.Key = key
	newKey.PointID = pointID

	newEntry := EntryPool.Get()
	newEntry.Key = key
	newEntry.Value = value
	newEntry.EntryType = entryType
	newEntry.PointID = pointID

	// Insert into skip list
	// Due to our comparator (Key ASC, PointID DESC), inserting a key with a new
	// pointID creates a new node. Only identical key+pointID combinations update existing nodes.
	oldNode := m.data.Insert(newKey, newEntry)

	if oldNode != nil {
		// An entry with the exact same key and point id was found.
		// The skiplist updates the node's value in-place and returns the old value.
		// The `newKey` object is now unused and must be returned to the pool.
		// The old value object must also be returned to the pool.
		KeyPool.Put(newKey)
		oldValue := oldNode.Value()
		EntryPool.Put(oldValue)

		m.sizeBytes -= oldValue.Size()
	}
	// Add the size of the new or updated entry.
	m.sizeBytes += newEntry.Size()
	return nil
}

// Get retrieves the latest version of a key from the memtable.
//
// Parameters:
//   - key: The key to look up
//
// Returns:
//   - value: The value bytes (nil for tombstones or if not found)
//   - entryType: Type of entry (EntryTypePutEvent or EntryTypeDelete)
//   - found: true if the key exists, false otherwise
//
// Behavior:
//   - Returns the entry with the highest PointID for the given key
//   - Tombstones (EntryTypeDelete) are considered valid entries
//   - The caller must check entryType to distinguish tombstones from actual values
//
// Thread Safety: Safe for concurrent use (uses read lock)
//
// Corresponds to FR1.3 (Read), FR3.1 (Memtable Operations)
func (m *Memtable) Get(key []byte) (value []byte, entryType core.EntryType, found bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// To find the latest version (highest PointID), we seek with max PointID.
	// Our comparator sorts by (Key ASC, PointID DESC), so the first matching
	// key will have the highest PointID.
	searchKey := KeyPool.Get()
	searchKey.Key = key
	searchKey.PointID = ^uint64(0) // Maximum uint64 value
	defer KeyPool.Put(searchKey)

	// Seek to the first entry >= our search key
	node, ok := m.data.Seek(searchKey)
	if !ok {
		return nil, 0, false
	}

	// Verify the found key matches our search key
	foundKey := node.Key()
	if !bytes.Equal(foundKey.Key, key) {
		return nil, 0, false
	}

	// Found the latest version
	entry := node.Value()

	// Tombstones are valid entries - caller must check EntryType
	if entry.EntryType == core.EntryTypeDelete {
		return nil, entry.EntryType, true
	}

	return entry.Value, entry.EntryType, true
}

// Size returns the current memory usage of the memtable in bytes.
//
// This is an estimated size based on key+value+metadata overhead.
// Thread Safety: Safe for concurrent use (uses read lock)
//
// Corresponds to FR3.2 (Size Tracking)
func (m *Memtable) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sizeBytes
}

// IsFull returns true if the memtable has reached its size threshold.
//
// When a memtable is full, it should be marked as immutable and a new
// active memtable should be created to accept new writes.
// Thread Safety: Safe for concurrent use (uses read lock)
//
// Corresponds to FR3.2 (Size Threshold Check)
func (m *Memtable) IsFull() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sizeBytes >= m.threshold
}

// Len returns the number of entries (including all versions) in the memtable.
// Note: This counts all versions of keys, not unique keys.
// Thread Safety: Safe for concurrent use (uses read lock)
func (m *Memtable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.data.Len()
}

// NewIterator creates a new iterator for scanning the memtable.
//
// Parameters:
//   - startKey: Start of key range (inclusive), nil for beginning
//   - endKey: End of key range (exclusive), nil for end
//   - order: Iteration order (Ascending or Descending)
//
// Returns:
//   - Iterator that yields the latest version of each unique key in the range
//
// Important:
//   - The iterator holds a read lock on the memtable
//   - MUST call Close() on the iterator to release the lock
//   - Iterator is not safe for concurrent use
//
// Thread Safety: The iterator itself is not thread-safe, but acquiring it is safe.
//
// Corresponds to FR1.3 (Range Queries)
func (m *Memtable) NewIterator(startKey, endKey []byte, order types.SortOrder) core.IteratorInterface[*core.IteratorNode] {
	m.mu.RLock() // Acquire read lock - released by iterator.Close()

	opts := make([]skiplist.IteratorOption[*MemtableKey, *MemtableEntry], 0)
	if order == types.Descending {
		opts = append(opts, skiplist.WithReverse[*MemtableKey, *MemtableEntry]())
	}

	iter := m.data.NewIterator(opts...)

	return &MemtableIterator{
		mu:       &m.mu,
		iter:     iter,
		startKey: startKey,
		endKey:   endKey,
		order:    order,
		valid:    false, // Iterator starts before first element
	}
}

// FlushToSSTable writes all entries from the memtable to an SSTable.
//
// Parameters:
//   - writer: SSTable writer to receive the memtable entries
//
// Behavior:
//   - Writes ALL versions of ALL keys (complete history)
//   - Maintains sorted order (Key ASC, PointID DESC)
//   - Compaction will later remove obsolete versions
//   - Uses read lock (memtable remains readable during flush)
//
// Error Handling:
//   - Returns error immediately on first write failure
//   - Partial writes should be discarded by the caller
//
// Thread Safety: Safe for concurrent use (uses read lock)
//
// Corresponds to FR3.3 (Flush to Disk)
func (m *Memtable) FlushToSSTable(writer core.SSTableWriterInterface) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Iterate over all entries in sorted order
	iter := m.data.NewIterator()
	entriesWritten := 0

	for iter.Next() {
		memKey := iter.Key()
		memEntry := iter.Value()

		// Write each entry to SSTable
		err := writer.Add(memEntry.Key, memEntry.Value, memEntry.EntryType, memKey.PointID)
		if err != nil {
			return fmt.Errorf("failed to add memtable entry to sstable (key=%s, entries_written=%d): %w",
				string(memEntry.Key), entriesWritten, err)
		}
		entriesWritten++
	}

	return nil
}

// Close releases all resources held by the memtable.
//
// Behavior:
//   - Returns all pooled keys and entries to their respective pools
//   - Clears internal skip list reference
//   - Safe to call multiple times (idempotent)
//
// This should be called when the memtable is no longer needed, typically
// after it has been successfully flushed to disk and is no longer referenced
// by any queries.
//
// Thread Safety: Safe for concurrent use (uses write lock)
func (m *Memtable) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.data == nil {
		return // Already closed
	}

	// Return all keys and entries to pools for reuse
	m.data.Range(func(key *MemtableKey, value *MemtableEntry) bool {
		KeyPool.Put(key)
		EntryPool.Put(value)
		return true // Continue iteration
	})

	// Clear references
	m.data = nil
	m.sizeBytes = 0
}
