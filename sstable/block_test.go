package sstable

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"sort"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to create raw block data for testing.
// Copied from sstable_internal_test.go for use in this file.
func createTestBlockDataWithTime(t *testing.T, entries []testEntry, restartInterval int) []byte {
	t.Helper()
	var entriesBuf bytes.Buffer
	var lastKey []byte
	var restartPoints []uint32

	for i, entry := range entries {
		isRestartPoint := (i%restartInterval == 0)
		var sharedPrefixLen int

		if !isRestartPoint && lastKey != nil {
			limit := len(entry.Key)
			if len(lastKey) < limit {
				limit = len(lastKey)
			}
			for sharedPrefixLen < limit && entry.Key[sharedPrefixLen] == lastKey[sharedPrefixLen] {
				sharedPrefixLen++
			}
		}

		if isRestartPoint {
			restartPoints = append(restartPoints, uint32(entriesBuf.Len()))
		}

		unsharedKey := entry.Key[sharedPrefixLen:]

		varintBuf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(varintBuf, uint64(sharedPrefixLen))
		entriesBuf.Write(varintBuf[:n])
		n = binary.PutUvarint(varintBuf, uint64(len(unsharedKey)))
		entriesBuf.Write(varintBuf[:n])
		n = binary.PutUvarint(varintBuf, uint64(len(entry.Value)))
		entriesBuf.Write(varintBuf[:n])
		entriesBuf.WriteByte(byte(entry.EntryType))
		n = binary.PutUvarint(varintBuf, entry.PointID)
		entriesBuf.Write(varintBuf[:n])
		entriesBuf.Write(unsharedKey)
		entriesBuf.Write(entry.Value)

		lastKey = append(lastKey[:0], entry.Key...)
	}

	// Now write the trailer to the same buffer
	for _, offset := range restartPoints {
		binary.Write(&entriesBuf, binary.LittleEndian, offset)
	}
	binary.Write(&entriesBuf, binary.LittleEndian, uint32(len(restartPoints)))

	return entriesBuf.Bytes()
}

func TestBlockIterator(t *testing.T) {
	entries := []testEntry{
		{Key: []byte("apple"), Value: []byte("red"), EntryType: core.EntryTypePutEvent, PointID: 1},
		{Key: []byte("apricot"), Value: []byte("orange"), EntryType: core.EntryTypePutEvent, PointID: 2}, // restart point
		{Key: []byte("banana"), Value: []byte("yellow"), EntryType: core.EntryTypePutEvent, PointID: 3},
		{Key: []byte("cherry"), Value: []byte("sweet"), EntryType: core.EntryTypeDelete, PointID: 4}, // restart point
		{Key: []byte("date"), Value: []byte("brown"), EntryType: core.EntryTypePutEvent, PointID: 5},
	}
	// Ensure entries are sorted as per Block.Find assumption: Key ASC, SeqNum DESC
	sort.SliceStable(entries, func(i, j int) bool {
		cmp := bytes.Compare(entries[i].Key, entries[j].Key)
		if cmp == 0 {
			return entries[i].PointID > entries[j].PointID // Descending PointID
		}
		return cmp < 0 // Ascending Key
	})

	blockData := createTestBlockDataWithTime(t, entries, 2)
	// The iterator only gets the entry data, not the trailer.
	numRestartPointsOffset := len(blockData) - 4
	numRestartPoints := binary.LittleEndian.Uint32(blockData[numRestartPointsOffset:])
	trailerSize := (int(numRestartPoints) * 4) + 4
	entriesData := blockData[:len(blockData)-trailerSize]

	t.Run("FullIteration", func(t *testing.T) {
		iter := NewBlockIterator(entriesData)
		var actualEntries []testEntry
		for iter.Next() {
			actualEntries = append(actualEntries, testEntry{
				Key:       append([]byte(nil), iter.Key()...),
				Value:     append([]byte(nil), iter.Value()...),
				EntryType: iter.EntryType(),
				PointID:   iter.PointID(),
			})
		}

		require.NoError(t, iter.Error(), "Iteration should complete without error")
		assert.Equal(t, len(entries), len(actualEntries), "Should iterate over all entries")
		assert.True(t, reflect.DeepEqual(entries, actualEntries), "Iterated entries should match original entries")
	})

	t.Run("CorruptedData", func(t *testing.T) {
		// Create a valid entry first
		var validEntryBuf bytes.Buffer
		entry := testEntry{Key: []byte("apple"), Value: []byte("red"), EntryType: core.EntryTypePutEvent, PointID: 1}
		sharedPrefixLen := 0
		unsharedKey := entry.Key[sharedPrefixLen:]
		varintBuf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(varintBuf, uint64(sharedPrefixLen))
		validEntryBuf.Write(varintBuf[:n])
		n = binary.PutUvarint(varintBuf, uint64(len(unsharedKey)))
		validEntryBuf.Write(varintBuf[:n])
		n = binary.PutUvarint(varintBuf, uint64(len(entry.Value)))
		validEntryBuf.Write(varintBuf[:n])
		validEntryBuf.WriteByte(byte(entry.EntryType))
		n = binary.PutUvarint(varintBuf, entry.PointID)
		validEntryBuf.Write(varintBuf[:n])
		validEntryBuf.Write(unsharedKey)
		validEntryBuf.Write(entry.Value)
		validEntryBytes := validEntryBuf.Bytes()

		t.Run("TruncatedInHeader", func(t *testing.T) {
			// Truncate in the middle of a varint
			corruptedData := validEntryBytes[:2]
			iter := NewBlockIterator(corruptedData)
			assert.False(t, iter.Next())
			require.Error(t, iter.Error())
			assert.Contains(t, iter.Error().Error(), "failed to read")
		})

		t.Run("TruncatedInValue", func(t *testing.T) {
			// Truncate in the middle of the value data
			corruptedData := validEntryBytes[:len(validEntryBytes)-1]
			iter := NewBlockIterator(corruptedData)
			assert.False(t, iter.Next())
			require.Error(t, iter.Error())
			assert.Contains(t, iter.Error().Error(), "failed to read value")
		})

	})

	t.Run("EmptyBlock", func(t *testing.T) {
		iter := NewBlockIterator([]byte{})
		assert.False(t, iter.Next(), "Next() on empty block should return false")
		assert.NoError(t, iter.Error(), "Error() on empty block should be nil")
	})

	t.Run("CorruptedBlock_Truncated", func(t *testing.T) {
		if len(entriesData) < 10 {
			t.Skip("Block data too small to truncate for test")
		}
		corruptedData := entriesData[:len(entriesData)-10] // Truncate the data
		iter := NewBlockIterator(corruptedData)
		for iter.Next() {
			// Iterate until an error occurs
		}
		assert.Error(t, iter.Error(), "Iteration on corrupted block should produce an error")
	})
}

func TestBuildBlockIndex(t *testing.T) {
	entries := []testEntry{
		{Key: []byte("key01"), Value: []byte("v1"), EntryType: core.EntryTypePutEvent, PointID: 1},
		{Key: []byte("key02"), Value: []byte("v2"), EntryType: core.EntryTypePutEvent, PointID: 2},
		{Key: []byte("key03"), Value: []byte("v3"), EntryType: core.EntryTypePutEvent, PointID: 3},
	}
	sort.SliceStable(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].Key, entries[j].Key) < 0
	})

	blockData := createTestBlockDataWithTime(t, entries, 2)

	// Action
	meta := buildBlockIndex(blockData)

	// Verification
	require.Len(t, meta, len(entries), "Should create one metadata entry per data entry")

	// Check first entry
	assert.Equal(t, entries[0].Key, meta[0].Key, "First key in metadata should match")
	assert.Equal(t, 0, meta[0].Offset, "First entry offset should be 0")

	// Check second entry
	assert.Equal(t, entries[1].Key, meta[1].Key, "Second key in metadata should match")
	assert.Greater(t, meta[1].Offset, meta[0].Offset, "Second offset should be greater than first")

	// Check third entry
	assert.Equal(t, entries[2].Key, meta[2].Key, "Third key in metadata should match")
	assert.Greater(t, meta[2].Offset, meta[1].Offset, "Third offset should be greater than second")
}

func TestBlock_Find_WithRestartPoints(t *testing.T) {
	// Create a larger set of entries to ensure multiple restart points
	var entries []testEntry
	for i := 0; i < 10; i++ {
		entries = append(entries, testEntry{
			Key:       []byte{byte('a' + i)}, // "a", "b", "c", ...
			Value:     []byte{byte('v'), byte('0' + i)},
			EntryType: core.EntryTypePutEvent,
			PointID:   uint64(i + 1),
		})
	}
	// Add a multi-version key
	entries = append(entries, testEntry{Key: []byte("e"), Value: []byte("vE_old"), EntryType: core.EntryTypePutEvent, PointID: 50})
	entries = append(entries, testEntry{Key: []byte("e"), Value: []byte("vE_new"), EntryType: core.EntryTypePutEvent, PointID: 51})
	// Add a deleted key
	entries = append(entries, testEntry{Key: []byte("g"), Value: []byte("vG_old"), EntryType: core.EntryTypePutEvent, PointID: 60})
	entries = append(entries, testEntry{Key: []byte("g"), Value: nil, EntryType: core.EntryTypeDelete, PointID: 61})

	sort.SliceStable(entries, func(i, j int) bool {
		cmp := bytes.Compare(entries[i].Key, entries[j].Key)
		if cmp == 0 {
			return entries[i].PointID > entries[j].PointID
		}
		return cmp < 0
	})

	// Use a small restart interval to create several restart points
	const restartInterval = 3
	blockData := createTestBlockDataWithTime(t, entries, restartInterval)
	block := NewBlock(blockData)

	tests := []struct {
		name          string
		keyToFind     []byte
		wantValue     []byte
		wantEntryType core.EntryType
		wantErr       error
	}{
		{"find_first_key", []byte("a"), []byte("v0"), core.EntryTypePutEvent, nil},
		{"find_key_at_restart_point", []byte("d"), []byte("v3"), core.EntryTypePutEvent, nil}, // "d" is at index 3, which is a restart point
		{"find_key_between_restarts", []byte("b"), []byte("v1"), core.EntryTypePutEvent, nil},
		{"find_latest_version", []byte("e"), []byte("vE_new"), core.EntryTypePutEvent, nil},
		{"find_deleted_key", []byte("g"), nil, core.EntryTypeDelete, ErrNotFound},
		{"find_non_existent_key_middle", []byte("f_mid"), nil, 0, ErrNotFound},
		{"find_non_existent_key_before_all", []byte("0"), nil, 0, ErrNotFound},
		{"find_non_existent_key_after_all", []byte("z"), nil, 0, ErrNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, entryType, _, err := block.Find(tt.keyToFind)

			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr, "Expected specific error")
			} else {
				require.NoError(t, err, "Expected no error")
				assert.Equal(t, tt.wantValue, value, "Value mismatch")
				assert.Equal(t, tt.wantEntryType, entryType, "EntryType mismatch")
			}
		})
	}
}

func TestBlock_Find_Corrupted(t *testing.T) {
	t.Run("BlockTooSmallForTrailer", func(t *testing.T) {
		// A block must be at least 4 bytes to hold the num_restart_points trailer.
		corruptedData := []byte{1, 2, 3}
		block := NewBlock(corruptedData)
		_, _, _, err := block.Find([]byte("any"))
		// The current implementation returns ErrNotFound in this case, which is acceptable.
		assert.ErrorIs(t, err, ErrNotFound, "Find on a block too small for a trailer should fail")
	})

	t.Run("InvalidTrailerSize", func(t *testing.T) {
		// Create data where num_restart_points is impossibly large.
		// This will make `trailerSize` larger than `len(b.data)`.
		var buf bytes.Buffer
		buf.Write([]byte("some_entry_data"))
		// Write a huge number for num_restart_points
		binary.Write(&buf, binary.LittleEndian, uint32(1000))
		corruptedData := buf.Bytes()

		block := NewBlock(corruptedData)
		_, _, _, err := block.Find([]byte("any"))
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCorrupted, "Error should be ErrCorrupted")
		assert.Contains(t, err.Error(), "smaller than calculated trailer size")
	})

	t.Run("CorruptedBlockIteratorError", func(t *testing.T) {
		// Create block data that is valid in structure but will cause the block iterator to fail.
		// We create a block with a malformed varint (0x80) as its entry data,
		// followed by a valid trailer indicating 0 restart points.
		corruptedData := []byte{
			0x80,       // Malformed varint as entry data
			0, 0, 0, 0, // Trailer: 0 restart points
		}
		block := NewBlock(corruptedData)
		_, _, _, err := block.Find([]byte("any"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "block find: iterator error", "Error should propagate from the block iterator")
	})
}
