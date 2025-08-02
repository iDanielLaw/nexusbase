package sstable

// iterator.go: Defines SSTableIterator, Next, Key, Value
// Placeholder for SSTable iteration logic.

import (
	"bytes"
	"encoding/binary"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/iterator"
	// "testing" // Temporary for logging in tests
)

// decodedEntry holds a fully decoded key-value pair from a block.
// Used for descending iteration where we need to buffer a block's content.
type decodedEntry struct {
	key       []byte
	value     []byte
	entryType core.EntryType
	pointID   uint64
}

// Iterator is an interface for iterating over key-value pairs.
// Defined here and also in sstable.go for convenience, but this is the primary definition.

// sstableIterator implements the Iterator interface for an SSTable.
type sstableIterator struct {
	sstable *SSTable // Reference to the parent SSTable

	startKey []byte         // Start boundary (inclusive)
	endKey   []byte         // End boundary (exclusive)
	sem      chan struct{}  // Optional semaphore to limit block read concurrency
	order    core.SortOrder // The iteration order (ascending/descending)

	// State for iteration
	currentKey       []byte
	currentValue     []byte
	currentBlock     *bytes.Buffer // The buffer for the current decompressed block.
	currentEntryType core.EntryType
	currentPointID   uint64
	err              error // Stores any error encountered

	// Block-based iteration state (FR4.8)
	currentIndexEntry int            // Index of the current BlockIndexEntry in sstable.index.entries
	blockIter         *BlockIterator // Iterator for the current block (used for ascending)
	eof               bool

	// State for descending iteration
	decodedBlockEntries []decodedEntry // Holds all entries of the current block for reverse iteration
	currentDecodedIndex int            // The index into decodedBlockEntries for the current position
}

// NewSSTableIterator creates a new iterator for the given SSTable and key range.
// This function is typically called by SSTable.NewIterator.
// Corresponds to FR4.7.
func NewSSTableIterator(s *SSTable, startKey, endKey []byte, sem chan struct{}, order core.SortOrder) (iterator.Interface, error) {
	// TODO (FR4.7, FR4.8): Implementation details:
	it := &sstableIterator{
		sstable:           s,
		startKey:          startKey,
		endKey:            endKey,
		sem:               sem,
		order:             order,
		currentIndexEntry: -1,
		eof:               false,
	}

	if s.index == nil || len(s.index.entries) == 0 {
		it.eof = true
		return it, nil
	}

	if order == core.Ascending {
		// For ascending, find the block that could contain the startKey.
		it.currentIndexEntry = s.index.findFirstGreaterOrEqual(startKey)
		// If startKey is not nil and the found block's FirstKey is strictly greater than startKey,
		// and it's not the first block, then startKey *might* be in the previous block.
		if startKey != nil && it.currentIndexEntry > 0 && it.currentIndexEntry < len(s.index.entries) &&
			bytes.Compare(s.index.entries[it.currentIndexEntry].FirstKey, startKey) > 0 {
			it.currentIndexEntry--
		}
	} else { // Descending
		if endKey != nil {
			// For descending, we want to start from the block that contains keys <= endKey.
			// findFirstGreaterOrEqual gives us the first block with key >= endKey.
			idx := s.index.findFirstGreaterOrEqual(endKey)
			it.currentIndexEntry = idx
			// If idx is past the end, it means all keys are < endKey, so start from the last block.
			if it.currentIndexEntry >= len(s.index.entries) {
				it.currentIndexEntry = len(s.index.entries) - 1
			}
		} else {
			// If no endKey, start from the very last block.
			it.currentIndexEntry = len(s.index.entries) - 1
		}
	}

	// Load the first block. Next() will handle seeking within it.
	if !it.loadBlockAtIndex(it.currentIndexEntry) {
		// it.err would be set by loadBlockAtIndex if an error occurred, or eof if no blocks.
		// If eof is true, Next() will return false.
	}

	return it, it.err
}

func (it *sstableIterator) loadBlockAtIndex(blockIdx int) bool {
	if blockIdx < 0 || blockIdx >= len(it.sstable.index.entries) {
		it.eof = true
		return false
	}

	// Return the old buffer (if any) to the pool before getting a new one.
	// This is the key to fixing the memory leak.
	if it.currentBlock != nil {
		core.PutBuffer(it.currentBlock)
		it.currentBlock = nil
	}

	// Load the new block. readBlock now returns a *bytes.Buffer from the pool.
	it.sstable.mu.RLock()
	blockMeta := it.sstable.index.entries[blockIdx]
	newBlock, err := it.sstable.readBlock(blockMeta.BlockOffset, blockMeta.BlockLength, it.sem)
	it.sstable.mu.RUnlock()

	if err != nil {
		it.err = err
		it.eof = true
		// Ensure we don't leak the buffer if readBlock succeeded but something else failed.
		if newBlock != nil {
			core.PutBuffer(newBlock)
		}
		return false
	}

	fullBlockData := newBlock.Bytes()
	numRestartPointsOffset := len(fullBlockData) - 4
	numRestartPoints := binary.LittleEndian.Uint32(fullBlockData[numRestartPointsOffset:])
	trailerSize := (int(numRestartPoints) * 4) + 4
	entriesData := fullBlockData[:len(fullBlockData)-trailerSize]

	it.currentBlock = newBlock
	it.currentIndexEntry = blockIdx

	if it.order == core.Ascending {
		it.blockIter = NewBlockIterator(entriesData)
	} else { // Descending
		// For descending order, we decode the entire block into memory to iterate backwards.
		it.blockIter = nil           // Not used for descending
		it.decodedBlockEntries = nil // Clear previous block's entries

		tempBlockIter := NewBlockIterator(entriesData)
		for tempBlockIter.Next() {
			it.decodedBlockEntries = append(it.decodedBlockEntries, decodedEntry{
				key:       append([]byte(nil), tempBlockIter.Key()...),
				value:     append([]byte(nil), tempBlockIter.Value()...),
				entryType: tempBlockIter.EntryType(),
				pointID:   tempBlockIter.PointID(),
			})
		}
		// Set the index to the end of the decoded slice to start iterating backwards.
		it.currentDecodedIndex = len(it.decodedBlockEntries)
	}
	return true
}

// Next advances the iterator to the next key-value pair.
// Returns true if there is a next entry, false otherwise.
// Corresponds to FR4.7.
func (it *sstableIterator) Next() bool {
	if it.err != nil || it.eof {
		return false
	}

	if it.order == core.Ascending {
		for {
			if it.blockIter != nil && it.blockIter.Next() {
				key := it.blockIter.Key()

				// Check endKey boundary
				if it.endKey != nil && bytes.Compare(key, it.endKey) >= 0 {
					it.eof = true
					return false
				}

				// Check startKey boundary (skip if before startKey)
				if it.startKey != nil && bytes.Compare(key, it.startKey) < 0 {
					continue
				}

				it.currentKey = key
				it.currentValue = it.blockIter.Value()
				it.currentEntryType = it.blockIter.EntryType()
				it.currentPointID = it.blockIter.PointID()
				return true
			}

			if it.blockIter != nil && it.blockIter.Error() != nil {
				it.err = it.blockIter.Error()
				return false
			}

			if !it.loadBlockAtIndex(it.currentIndexEntry + 1) {
				return false
			}
		}
	} else { // Descending
		for {
			// Try to move backwards in the current decoded block
			if it.decodedBlockEntries != nil && it.currentDecodedIndex > 0 {
				it.currentDecodedIndex--
				entry := it.decodedBlockEntries[it.currentDecodedIndex]

				// Check startKey boundary (exclusive lower bound for descending)
				if it.startKey != nil && bytes.Compare(entry.key, it.startKey) < 0 {
					it.eof = true
					return false
				}

				// Check endKey boundary (exclusive upper bound for descending)
				if it.endKey != nil && bytes.Compare(entry.key, it.endKey) >= 0 {
					continue
				}

				it.currentKey = entry.key
				it.currentValue = entry.value
				it.currentEntryType = entry.entryType
				it.currentPointID = entry.pointID
				return true
			}

			// Current block exhausted, load the previous one.
			if !it.loadBlockAtIndex(it.currentIndexEntry - 1) {
				return false
			}
		}
	}
}

func (it *sstableIterator) At() ([]byte, []byte, core.EntryType, uint64) {
	return it.currentKey, it.currentValue, it.currentEntryType, it.currentPointID
}

// Error returns the last error encountered by the iterator.
func (it *sstableIterator) Error() error {
	return it.err
}

// Close releases any resources held by the iterator (e.g., file handle if not shared).
func (it *sstableIterator) Close() error {
	// Return the last used block buffer to the pool.
	if it.currentBlock != nil {
		core.PutBuffer(it.currentBlock)
		it.currentBlock = nil
	}
	// No specific resources to close for the iterator itself if SSTable manages the file.
	it.blockIter = nil // Help GC
	return nil
}
