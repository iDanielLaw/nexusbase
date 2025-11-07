package memtable

import (
	"bytes"
	"sync"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexuscore/types"
	"github.com/INLOpen/skiplist"
)

// MemtableIterator provides ordered iteration over the latest version of each key.
//
// Key Features:
// - Yields only the latest version (highest PointID) of each unique key
// - Supports both ascending and descending iteration
// - Respects key range boundaries (startKey, endKey)
// - Holds a read lock on the parent memtable (released by Close)
//
// Lifecycle:
// 1. Created by Memtable.NewIterator()
// 2. Use Next() to advance, At() to read current entry
// 3. MUST call Close() to release memtable lock
//
// Thread Safety: Not safe for concurrent use by multiple goroutines.
// Each goroutine should create its own iterator.
type MemtableIterator struct {
	mu       *sync.RWMutex                                    // Parent memtable's lock
	iter     *skiplist.Iterator[*MemtableKey, *MemtableEntry] // Underlying skip list iterator
	startKey []byte                                           // Lower bound (inclusive)
	endKey   []byte                                           // Upper bound (exclusive)
	order    types.SortOrder                                  // Iteration direction
	valid    bool                                             // True if iterator points to valid entry
	err      error                                            // Last error encountered
}

// Compile-time interface verification
var _ core.IteratorInterface[*core.IteratorNode] = (*MemtableIterator)(nil)

// skipToLatestVersionOfCurrentKeyDescending advances the iterator past all older
// versions of the current key. Used only in descending iteration.
//
// In descending order, the skip list iterator traverses from newest to oldest
// PointIDs for each key. We want to yield only the newest (first encountered),
// so we skip past any older versions.
func (it *MemtableIterator) skipToLatestVersionOfCurrentKeyDescending() {
	currentKey := it.iter.Key().Key

	// Peek ahead to see if next entry is an older version of the same key
	for {
		peekIter := it.iter.Clone()
		if !peekIter.Next() {
			break // No more entries
		}
		if !bytes.Equal(peekIter.Key().Key, currentKey) {
			break // Different key, stop skipping
		}
		// Same key with older PointID, skip it
		it.iter.Next()
	}
}

// Next advances the iterator to the next unique key.
//
// Returns:
//   - true if a valid entry is available (use At() to read it)
//   - false if no more entries or error occurred (check Error())
//
// Behavior:
//   - First call: Positions iterator at first/last entry based on order
//   - Subsequent calls: Advances to next distinct key (skips older versions)
//   - Respects key range boundaries (startKey, endKey)
//
// Example:
//
//	for iter.Next() {
//	    node, err := iter.At()
//	    // process node
//	}
func (it *MemtableIterator) Next() bool {
	if !it.valid {
		// First call - position iterator at start
		return it.positionInitial()
	}

	// Subsequent calls - advance to next distinct key
	return it.advanceToNextKey()
}

// positionInitial positions the iterator at the start based on iteration order.
func (it *MemtableIterator) positionInitial() bool {
	it.valid = true // Tentatively set to true

	var found bool
	if it.order == types.Ascending {
		found = it.seekForwardStart()
	} else {
		found = it.seekReverseStart()
	}

	if !found {
		it.valid = false
		return false
	}

	// For descending, skip to latest version of initial key
	if it.order == types.Descending {
		it.skipToLatestVersionOfCurrentKeyDescending()
	}

	// Validate bounds
	return it.checkBoundsAndAdvance()
}

// seekForwardStart positions iterator for ascending iteration.
func (it *MemtableIterator) seekForwardStart() bool {
	if it.startKey != nil {
		// Seek to first key >= startKey
		seekKey := KeyPool.Get()
		seekKey.Key = it.startKey
		seekKey.PointID = ^uint64(0) // Max PointID to find first entry
		defer KeyPool.Put(seekKey)
		return it.iter.Seek(seekKey)
	}
	return it.iter.First()
}

// seekReverseStart positions iterator for descending iteration.
func (it *MemtableIterator) seekReverseStart() bool {
	if it.endKey != nil {
		// Seek to first key < endKey (exclusive)
		seekKey := KeyPool.Get()
		seekKey.Key = it.endKey
		seekKey.PointID = 0
		defer KeyPool.Put(seekKey)

		found := it.iter.Seek(seekKey)
		if !found {
			// All keys are less than endKey, start from last
			return it.iter.Last()
		}
		return true
	}
	return it.iter.Last()
}

// advanceToNextKey moves to the next distinct key, skipping older versions.
func (it *MemtableIterator) advanceToNextKey() bool {
	lastKey := it.iter.Key().Key

	// Skip all entries with the same key
	for {
		if !it.iter.Next() {
			it.valid = false
			return false
		}

		if !bytes.Equal(it.iter.Key().Key, lastKey) {
			break // Found a new key
		}
	}

	// For descending, skip to latest version of the new key
	if it.order == types.Descending {
		it.skipToLatestVersionOfCurrentKeyDescending()
	}

	// Validate bounds
	return it.checkBoundsAndAdvance()
}

// checkBoundsAndAdvance validates key bounds and advances if necessary.
func (it *MemtableIterator) checkBoundsAndAdvance() bool {
	for {
		currentKey := it.iter.Key().Key

		if it.order == types.Ascending {
			// Check upper bound
			if it.endKey != nil && bytes.Compare(currentKey, it.endKey) >= 0 {
				it.valid = false
				return false
			}
			return true // Valid entry within bounds
		}

		// Descending order
		// Check lower bound
		if it.startKey != nil && bytes.Compare(currentKey, it.startKey) < 0 {
			it.valid = false
			return false
		}

		// Check upper bound (shouldn't be >= endKey)
		if it.endKey != nil && bytes.Compare(currentKey, it.endKey) >= 0 {
			// Out of bounds, advance to next key
			if !it.skipToNextKeyInBounds() {
				return false
			}
			continue // Re-check bounds
		}

		return true // Valid entry within bounds
	}
}

// skipToNextKeyInBounds advances to the next key for descending iteration.
func (it *MemtableIterator) skipToNextKeyInBounds() bool {
	lastKey := it.iter.Key().Key

	for {
		if !it.iter.Next() {
			it.valid = false
			return false
		}

		if !bytes.Equal(it.iter.Key().Key, lastKey) {
			break
		}
	}

	it.skipToLatestVersionOfCurrentKeyDescending()
	return true
}

// At returns the current entry's data.
//
// Returns:
//   - node: Iterator node containing key, value, entry type, and sequence number
//   - error: Always nil (for interface compatibility)
//
// The returned node is only valid until the next Next() call.
// Returns empty node if iterator is not valid.
func (it *MemtableIterator) At() (*core.IteratorNode, error) {
	if !it.valid {
		return &core.IteratorNode{}, nil
	}

	return &core.IteratorNode{
		Key:       it.key(),
		Value:     it.value(),
		EntryType: it.entryType(),
		SeqNum:    it.pointID(),
	}, nil
}

// Error returns any error encountered during iteration.
// Currently always returns nil as errors are handled during iteration.
func (it *MemtableIterator) Error() error {
	return it.err
}

// Close releases the iterator's resources and the read lock on the memtable.
// Safe to call multiple times (idempotent).
// MUST be called when done with the iterator to prevent deadlocks.
func (it *MemtableIterator) Close() error {
	if it.mu == nil {
		return nil // Already closed
	}

	it.valid = false
	it.mu.RUnlock() // Release read lock on memtable
	it.mu = nil     // Mark as closed
	return nil
}

// Helper methods for accessing current entry data

func (it *MemtableIterator) key() []byte {
	if !it.valid {
		return nil
	}
	return it.iter.Key().Key
}

func (it *MemtableIterator) value() []byte {
	if !it.valid {
		return nil
	}
	return it.iter.Value().Value
}

func (it *MemtableIterator) entryType() core.EntryType {
	if !it.valid {
		return 0
	}
	return it.iter.Value().EntryType
}

func (it *MemtableIterator) pointID() uint64 {
	if !it.valid {
		return 0
	}
	return it.iter.Key().PointID
}
