package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"

	"github.com/INLOpen/nexusbase/core"
	// Import sort for binary search (though not used in the simplified Find)
)

// block.go: (Optional) Defines DataBlock structure if used within SSTables.
// Placeholder for SSTable data block logic.
// Details to be filled based on FR4.8 if block-based storage is implemented.

// Block represents a data block within an SSTable.
// It contains multiple key-value entries.
type Block struct {
	data        []byte // Raw byte data of the block
	entriesMeta []blockEntryMeta
}

// blockEntryMeta stores metadata for an entry within a block.
type blockEntryMeta struct {
	Key    []byte // The key of the entry
	Offset int    // The starting offset of the entry within the block's data
}

// NewBlock creates a new Block from raw byte data and builds its internal index.
func NewBlock(blockData []byte) *Block {
	return &Block{
		data: blockData,
		// Build the internal index (entriesMeta) when the block is created/loaded.
		// Note: The simplified Find method below does not use entriesMeta.
		entriesMeta: buildBlockIndex(blockData),
	}
}

// Find searches for a key within the block using restart points for efficiency. (FR4.8)
// Returns value, entryType, sequence number, and an error. If not found, returns ErrNotFound.
// Assumes entries within the block are sorted by Key, then by PointID descending.
func (b *Block) Find(keyToFind []byte) ([]byte, core.EntryType, uint64, error) {
	if len(b.data) < 4 { // Must have at least num_restart_points (uint32)
		return nil, 0, 0, ErrNotFound
	}
	// 1. Read the trailer to get restart points
	numRestartPointsOffset := len(b.data) - 4
	numRestartPoints := binary.LittleEndian.Uint32(b.data[numRestartPointsOffset:])
	trailerSize := (int(numRestartPoints) * 4) + 4
	if len(b.data) < trailerSize {
		return nil, 0, 0, fmt.Errorf("invalid block size %d, smaller than calculated trailer size %d: %w", len(b.data), trailerSize, ErrCorrupted)
	}
	entriesData := b.data[:len(b.data)-trailerSize]

	if numRestartPoints == 0 {
		// Block has no restart points, do a full scan from the beginning.
		// This might happen for very small blocks.
		blockIter := NewBlockIterator(entriesData)
		return findLinearScan(blockIter, keyToFind)
	}

	restartPointsStartOffset := numRestartPointsOffset - (int(numRestartPoints) * 4)
	if restartPointsStartOffset < 0 {
		return nil, 0, 0, fmt.Errorf("invalid restart points offset: %w", ErrCorrupted)
	}

	// 2. Binary search for the correct restart point.
	// We want to find the largest restart point whose key is <= keyToFind.
	// This means we are looking for the rightmost restart point to start our scan from.
	var searchStartOffset uint32
	// `sort.Search` is used to find the first restart point whose key is >= keyToFind.
	// This is the "insertion point" for keyToFind in the sorted list of restart point keys.
	searchIndex := sort.Search(int(numRestartPoints), func(i int) bool {
		offset := binary.LittleEndian.Uint32(b.data[restartPointsStartOffset+(i*4):])
		// Create a temporary iterator to read just the first key at this restart point.
		tempIter := NewBlockIterator(entriesData[offset:])
		if tempIter.Next() {
			// The predicate is true if the key at the restart point is >= the key we're looking for.
			return bytes.Compare(tempIter.Key(), keyToFind) >= 0
		}
		return false // Should not happen in a valid block
	})

	// After sort.Search, `searchIndex` is the index of the first restart point with a key >= keyToFind.
	// This means the block that could contain `keyToFind` must be the one starting at the *previous*
	// restart point (`searchIndex - 1`).
	// If `searchIndex` is 0, it means `keyToFind` is smaller than or equal to the first restart point's key,
	// so we must start our scan from the very beginning of the block (offset 0).
	if searchIndex > 0 {
		// The correct restart point to start scanning from is the one *before* the one found by sort.Search,
		// as that one is the first with a key >= keyToFind.
		searchStartOffset = binary.LittleEndian.Uint32(b.data[restartPointsStartOffset+((searchIndex-1)*4):])
	} else {
		// If searchIndex is 0, it means keyToFind is less than or equal to the first restart point's key.
		// We start from the very first restart point (which is always at offset 0).
		searchStartOffset = 0
	}

	// 3. Linear scan from the found restart point.
	blockIter := NewBlockIterator(entriesData[searchStartOffset:])
	return findLinearScan(blockIter, keyToFind)
}

// getEntriesData is a helper to extract the slice of block data that contains
// only the key-value entries, excluding the trailer.
func (b *Block) getEntriesData() []byte {
	if len(b.data) < 4 {
		return nil
	}
	numRestartPointsOffset := len(b.data) - 4
	numRestartPoints := binary.LittleEndian.Uint32(b.data[numRestartPointsOffset:])
	trailerSize := (int(numRestartPoints) * 4) + 4
	if len(b.data) < trailerSize {
		return nil // Corrupted block
	}
	return b.data[:len(b.data)-trailerSize]
}

// findLinearScan performs a linear scan for a key from a given iterator's current position.
func findLinearScan(blockIter *BlockIterator, keyToFind []byte) ([]byte, core.EntryType, uint64, error) {
	var latestFoundEntry *struct {
		value     []byte
		entryType core.EntryType
		pointID   uint64
	}

	for blockIter.Next() {
		currentKey := blockIter.Key()
		cmp := bytes.Compare(currentKey, keyToFind)

		if cmp == 0 { // Found an entry with the exact key
			currentPointID := blockIter.PointID()
			if latestFoundEntry == nil || currentPointID > latestFoundEntry.pointID {
				latestFoundEntry = &struct {
					value     []byte
					entryType core.EntryType
					pointID   uint64
				}{
					value:     blockIter.Value(),
					entryType: blockIter.EntryType(),
					pointID:   currentPointID,
				}
			}
		} else if cmp > 0 {
			// We've passed the key we're looking for.
			break
		}
	}

	if err := blockIter.Error(); err != nil {
		return nil, 0, 0, fmt.Errorf("block find: iterator error: %w", err)
	}

	if latestFoundEntry != nil {
		if latestFoundEntry.entryType == core.EntryTypeDelete {
			return nil, latestFoundEntry.entryType, latestFoundEntry.pointID, ErrNotFound
		}
		return latestFoundEntry.value, latestFoundEntry.entryType, latestFoundEntry.pointID, nil
	}

	return nil, 0, 0, ErrNotFound
}

// BlockIterator iterates over entries within a single data block.
type BlockIterator struct {
	reader *bytes.Reader
	// For prefix decompression
	previousKey      []byte
	currentKey       []byte
	currentValue     []byte
	currentEntryType core.EntryType
	currentPointID   uint64 // Unique ID of the current entry (FR5.3)
	err              error
}

// NewBlockIterator creates a new iterator for the given block data.
func NewBlockIterator(blockData []byte) *BlockIterator {
	return &BlockIterator{
		reader: bytes.NewReader(blockData),
	}
}

// Next advances the iterator to the next entry in the block.
// Returns false if there are no more entries or an error occurred.
func (bi *BlockIterator) Next() bool {
	if bi.err != nil || bi.reader.Len() == 0 {
		return false
	}

	// Read shared_key_len (varint)
	sharedLen, err := binary.ReadUvarint(bi.reader)
	if err != nil {
		if err == io.EOF {
			return false
		}
		bi.err = fmt.Errorf("block iterator: failed to read shared_key_len: %w", err)
		return false
	}

	// Read unshared_key_len (varint)
	unsharedLen, err := binary.ReadUvarint(bi.reader)
	if err != nil {
		bi.err = fmt.Errorf("block iterator: failed to read unshared_key_len: %w", err)
		return false
	}

	// Read value_len (varint)
	valueLen, err := binary.ReadUvarint(bi.reader)
	if err != nil {
		bi.err = fmt.Errorf("block iterator: failed to read value_len: %w", err)
		return false
	}

	// Read entry_type (1 byte)
	entryTypeByte, err := bi.reader.ReadByte()
	if err != nil {
		bi.err = fmt.Errorf("block iterator: failed to read entry type: %w", err)
		return false
	}

	// Read point_id (varint)
	pointID, err := binary.ReadUvarint(bi.reader)
	if err != nil {
		bi.err = fmt.Errorf("block iterator: failed to read point id: %w", err)
		return false
	}

	// Reconstruct key using prefix from previous key
	key := make([]byte, sharedLen+unsharedLen)
	copy(key, bi.previousKey[:sharedLen])
	if _, err := io.ReadFull(bi.reader, key[sharedLen:]); err != nil {
		bi.err = fmt.Errorf("block iterator: failed to read unshared key: %w", err)
		return false
	}

	// Read Value
	value := make([]byte, valueLen)
	if _, err := io.ReadFull(bi.reader, value); err != nil {
		bi.err = fmt.Errorf("block iterator: failed to read value for key %s: %w", string(key), err)
		return false
	}

	bi.currentKey = key
	bi.currentValue = value
	bi.currentEntryType = core.EntryType(entryTypeByte)
	bi.currentPointID = pointID
	bi.previousKey = append(bi.previousKey[:0], key...) // Update previous key for next iteration

	return true
}

// Key returns the key of the current entry.
func (bi *BlockIterator) Key() []byte { return bi.currentKey }

// Value returns the value of the current entry.
func (bi *BlockIterator) Value() []byte { return bi.currentValue }

// EntryType returns the type of the current entry.
func (bi *BlockIterator) EntryType() core.EntryType { return bi.currentEntryType }

// PointID returns the unique ID of the current entry.
func (bi *BlockIterator) PointID() uint64 { return bi.currentPointID }

// Error returns any error encountered during iteration.
func (bi *BlockIterator) Error() error { return bi.err }

// buildBlockIndex iterates through the raw block data and creates a simple index
// of entry keys and their offsets within the block.
// Note: This is now primarily for testing, as Block.Find uses restart points.
func buildBlockIndex(blockData []byte) []blockEntryMeta {
	if len(blockData) < 4 {
		return nil
	}
	// Read trailer first to find the end of the entry data
	numRestartPointsOffset := len(blockData) - 4
	numRestartPoints := binary.LittleEndian.Uint32(blockData[numRestartPointsOffset:])
	trailerSize := (int(numRestartPoints) * 4) + 4
	if len(blockData) < trailerSize {
		return nil // Corrupted block
	}
	entriesData := blockData[:len(blockData)-trailerSize]

	var entriesMeta []blockEntryMeta
	reader := bytes.NewReader(entriesData)
	var previousKey []byte

	for reader.Len() > 0 {
		entryStartOffset := len(entriesData) - reader.Len()
		// Format: shared_len(varint), unshared_len(varint), value_len(varint), type(1), point_id(varint), unshared_key, value

		sharedLen, err := binary.ReadUvarint(reader)
		if err != nil {
			return entriesMeta
		}
		unsharedLen, err := binary.ReadUvarint(reader)
		if err != nil {
			return entriesMeta
		}
		valueLen, err := binary.ReadUvarint(reader)
		if err != nil {
			return entriesMeta
		}

		// Skip entry_type (1 byte) and point_id (varint)
		if _, err := reader.Seek(1, io.SeekCurrent); err != nil {
			return entriesMeta
		}
		if _, err := binary.ReadUvarint(reader); err != nil {
			return entriesMeta
		}

		// Reconstruct key
		key := make([]byte, sharedLen+unsharedLen)
		copy(key, previousKey[:sharedLen])
		if _, err := io.ReadFull(reader, key[sharedLen:]); err != nil {
			return entriesMeta // Error or EOF
		}

		entriesMeta = append(entriesMeta, blockEntryMeta{Key: key, Offset: entryStartOffset})
		previousKey = append(previousKey[:0], key...) // Update for next iteration

		// Skip the rest of the entry: Value (valueLen)
		if _, err := reader.Seek(int64(valueLen), io.SeekCurrent); err != nil {
			return entriesMeta // Error or EOF
		}
	}
	return entriesMeta
}
