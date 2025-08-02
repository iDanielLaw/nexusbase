package sstable

import (
	"bytes"
	"encoding/binary"
	"sort"
	"testing"

	"github.com/INLOpen/nexusbase/core"
)

// Helper function to create raw block data for testing.
func createTestBlockData(t *testing.T, entries []testEntry) []byte {
	t.Helper()
	var entriesBuf bytes.Buffer
	var lastKey []byte
	var restartPoints []uint32
	const restartInterval = 2 // For testing, make every 2nd entry a restart point

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

func TestBlock_Find(t *testing.T) {
	entries := []testEntry{
		// Entries for the block must be sorted by Key ASC, then by SeqNum DESC
		{Key: []byte("apple"), Value: []byte("red_v2"), EntryType: core.EntryTypePutEvent, PointID: 3}, // Latest version of apple
		{Key: []byte("apple"), Value: []byte("red_v1"), EntryType: core.EntryTypePutEvent, PointID: 1}, // Older version of apple
		{Key: []byte("banana"), Value: []byte("yellow"), EntryType: core.EntryTypePutEvent, PointID: 2},
		{Key: []byte("cherry"), Value: []byte(""), EntryType: core.EntryTypeDelete, PointID: 5},        // Deleted cherry (latest version)
		{Key: []byte("cherry"), Value: []byte("sweet"), EntryType: core.EntryTypePutEvent, PointID: 4}, // Older version
	}
	// Ensure entries are sorted as per Block.Find assumption: Key ASC, SeqNum DESC
	sort.SliceStable(entries, func(i, j int) bool {
		cmp := bytes.Compare(entries[i].Key, entries[j].Key)
		if cmp == 0 {
			return entries[i].PointID > entries[j].PointID // Descending PointID
		}
		return cmp < 0 // Ascending Key
	})
	blockData := createTestBlockData(t, entries)
	block := NewBlock(blockData)

	tests := []struct {
		name          string
		keyToFind     []byte
		wantValue     []byte
		wantEntryType core.EntryType
		wantErr       error
	}{
		{
			name:          "find existing latest version",
			keyToFind:     []byte("apple"),
			wantValue:     []byte("red_v2"),
			wantEntryType: core.EntryTypePutEvent,
			wantErr:       nil,
		},
		{
			name:          "find existing single version",
			keyToFind:     []byte("banana"),
			wantValue:     []byte("yellow"),
			wantEntryType: core.EntryTypePutEvent,
			wantErr:       nil,
		},
		{
			name:      "find deleted key (tombstone)",
			keyToFind: []byte("cherry"),
			wantErr:   ErrNotFound, // Corrected: Expect ErrNotFound for deleted keys
		},
		{
			name:      "find non-existent key",
			keyToFind: []byte("date"),
			wantErr:   ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, entryType, _, err := block.Find(tt.keyToFind)

			if err != tt.wantErr {
				t.Fatalf("block.Find() error = %v, wantErr %v", err, tt.wantErr)
			}

			if tt.wantErr == nil {
				if !bytes.Equal(value, tt.wantValue) {
					t.Errorf("block.Find() value = %s, want %s", string(value), string(tt.wantValue))
				}
				if entryType != tt.wantEntryType {
					t.Errorf("block.Find() entryType = %v, want %v", entryType, tt.wantEntryType)
				}
			}
		})
	}
}

func TestIndex_Find(t *testing.T) {
	idx := &Index{
		entries: []BlockIndexEntry{
			{FirstKey: []byte("block_A_key01"), BlockOffset: 0, BlockLength: 100},
			{FirstKey: []byte("block_C_key01"), BlockOffset: 100, BlockLength: 100},
			{FirstKey: []byte("block_E_key01"), BlockOffset: 200, BlockLength: 100},
		},
	}

	tests := []struct {
		name            string
		keyToFind       []byte
		wantBlockOffset int64
		wantFound       bool
	}{
		{"exact_match_first_block", []byte("block_A_key01"), 0, true},
		{"exact_match_middle_block", []byte("block_C_key01"), 100, true},
		{"key_in_first_block", []byte("block_B_key50"), 0, true},
		{"key_in_middle_block", []byte("block_D_key99"), 100, true},
		{"key_in_last_block", []byte("block_F_key01"), 200, true},
		{"key_before_all", []byte("block_0_key01"), 0, true},
		{"key_after_all", []byte("block_Z_key01"), 200, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry, found := idx.Find(tt.keyToFind)
			if found != tt.wantFound {
				t.Fatalf("idx.Find() found = %v, want %v", found, tt.wantFound)
			}
			if found && entry.BlockOffset != tt.wantBlockOffset {
				t.Errorf("idx.Find() offset = %d, want %d", entry.BlockOffset, tt.wantBlockOffset)
			}
		})
	}

	t.Run("empty_index", func(t *testing.T) {
		emptyIdx := &Index{}
		_, found := emptyIdx.Find([]byte("anykey"))
		if found {
			t.Error("Find on empty index should return found=false")
		}
	})
}

func TestIndex_findFirstGreaterOrEqual(t *testing.T) {
	idx := &Index{
		entries: []BlockIndexEntry{
			{FirstKey: []byte("b")},
			{FirstKey: []byte("d")},
			{FirstKey: []byte("f")},
		},
	}

	tests := []struct {
		name      string
		keyToFind []byte
		wantIndex int
	}{
		{"find_exact", []byte("d"), 1},
		{"find_in_between", []byte("e"), 2},
		{"find_before_all", []byte("a"), 0},
		{"find_after_all", []byte("g"), 3},
		{"find_first", []byte("b"), 0},
		{"find_last", []byte("f"), 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotIndex := idx.findFirstGreaterOrEqual(tt.keyToFind)
			if gotIndex != tt.wantIndex {
				t.Errorf("findFirstGreaterOrEqual(%s) = %d, want %d", string(tt.keyToFind), gotIndex, tt.wantIndex)
			}
		})
	}
}
