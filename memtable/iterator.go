package memtable

import (
	"bytes"
	"sync"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/skiplist"
)

// MemtableIterator iterates over the latest version of each distinct key in the memtable.
// It is not safe for concurrent use by multiple goroutines.
type MemtableIterator struct {
	mu       *sync.RWMutex // The lock from the parent memtable. MUST be released by Close().
	iter     *skiplist.Iterator[*MemtableKey, *MemtableEntry]
	startKey []byte // For descending iteration check
	endKey   []byte
	order    core.SortOrder
	valid    bool // Indicates if the iterator is currently at a valid position.
	err      error
}

// skipToLatestVersionOfCurrentKeyDescending advances the iterator to the latest version
// (highest PointID) of the key it is currently on. This is only used for descending iteration.
func (it *MemtableIterator) skipToLatestVersionOfCurrentKeyDescending() {
	currentKey := it.iter.Key().Key
	for {
		peekIter := it.iter.Clone()
		if !peekIter.Next() || !bytes.Equal(peekIter.Key().Key, currentKey) {
			break
		}
		it.iter.Next()
	}
}

// Next moves the iterator to the next distinct key.
func (it *MemtableIterator) Next() bool {
	if !it.valid {
		// --- First Call: Initial Positioning ---
		it.valid = true // Tentatively set to true.
		var found bool
		if it.order == core.Ascending {
			if it.startKey != nil {
				found = it.iter.Seek(&MemtableKey{Key: it.startKey, PointID: ^uint64(0)})
			} else {
				found = it.iter.First()
			}
		} else { // Descending
			if it.endKey != nil {
				// For a descending scan, we want to start at the greatest key that is < endKey.
				// A reversed iterator's Seek should find the first element <= key.
				found = it.iter.Seek(&MemtableKey{Key: it.endKey, PointID: 0})
				// If Seek fails, it means all keys are less than endKey, so we should start from the very last element.
				if !found {
					found = it.iter.Last()
				}
			} else {
				found = it.iter.Last()
			}
		}

		if !found {
			it.valid = false // The initial positioning failed.
			return false
		}
		// If found, fall through to the validation loop for the first element.
		if it.order == core.Descending {
			it.skipToLatestVersionOfCurrentKeyDescending()
		}
	} else { // Subsequent calls
		lastKey := it.iter.Key().Key
		if it.order == core.Ascending {
			for {
				if !it.iter.Next() {
					it.valid = false
					return false
				}
				if !bytes.Equal(it.iter.Key().Key, lastKey) {
					break
				}
			}
		} else { // Descending
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
		}
	}

	// Loop to handle bounds checking, especially for descending scans that might start out of bounds.
	for {
		// We are at a new candidate key. Check its bounds.
		currentKey := it.iter.Key().Key
		if it.order == core.Ascending {
			if it.endKey != nil && bytes.Compare(currentKey, it.endKey) >= 0 {
				it.valid = false // Past the end key, so we are done.
				return false
			}
		} else { // Descending
			if it.startKey != nil && bytes.Compare(currentKey, it.startKey) < 0 {
				it.valid = false // Past the start key (lower bound), so we are done.
				return false
			}
			if it.endKey != nil && bytes.Compare(currentKey, it.endKey) >= 0 {
				// This key is out of bounds, but there might be other valid keys.
				// We need to advance to the next *distinct* key.
				lastKey := currentKey
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

				continue // Re-run the bounds check for the new key.
			}
		}

		// This key is valid and in bounds.
		return true
	}
}

// Value returns the value of the current entry.
func (it *MemtableIterator) value() []byte {
	if !it.valid {
		return nil
	}
	return it.iter.Value().Value
}

// Key returns the key of the current entry.
func (it *MemtableIterator) key() []byte {
	if !it.valid {
		return nil
	}
	return it.iter.Key().Key
}

// EntryType returns the type of the current entry.
func (it *MemtableIterator) entryType() core.EntryType {
	if !it.valid {
		return 0 // Return a zero value if not valid
	}
	return it.iter.Value().EntryType
}

// SequenceNumber returns the point id of the current entry.
// This is needed to implement iterator.Interface.
func (it *MemtableIterator) pointID() uint64 {
	if !it.valid {
		return 0 // Return a zero value if not valid
	}
	return it.iter.Key().PointID
}

func (it *MemtableIterator) At() ([]byte, []byte, core.EntryType, uint64) {
	if !it.valid {
		return nil, nil, 0, 0
	}
	return it.key(), it.value(), it.entryType(), it.pointID()
}

// Error returns the error.
func (it *MemtableIterator) Error() error {
	return it.err
}

// Close releases the iterator's resources, including the read lock on the memtable.
// It is safe to call Close multiple times.
func (it *MemtableIterator) Close() error {
	if it.mu == nil { // Prevent multiple unlocks
		return nil
	}
	it.valid = false
	it.mu.RUnlock() // Release the read lock on the memtable
	it.mu = nil     // Mark as closed
	return nil
}
