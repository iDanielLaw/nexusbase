package sstable

import (
	"bytes"
	"log/slog"
	"os"
	"sort"
	"testing"

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/require"
)

// A more comprehensive set of entries for iterator testing
func createIteratorTestEntries() []testEntry {
	entries := []testEntry{
		// Block 1 candidates
		{Key: []byte("a"), Value: []byte("v1"), EntryType: core.EntryTypePutEvent, PointID: 1},
		{Key: []byte("b"), Value: []byte("v1"), EntryType: core.EntryTypePutEvent, PointID: 2},
		{Key: []byte("c"), Value: []byte("v1_old"), EntryType: core.EntryTypePutEvent, PointID: 3},
		{Key: []byte("c"), Value: []byte("v2_new"), EntryType: core.EntryTypePutEvent, PointID: 4}, // multi-version

		// Block 2 candidates
		{Key: []byte("d"), Value: []byte("v1"), EntryType: core.EntryTypePutEvent, PointID: 5},
		{Key: []byte("e"), Value: []byte("v1_old"), EntryType: core.EntryTypePutEvent, PointID: 6},
		{Key: []byte("e"), Value: nil, EntryType: core.EntryTypeDelete, PointID: 7}, // tombstone

		// Block 3 candidates
		{Key: []byte("f"), Value: []byte("v1"), EntryType: core.EntryTypePutEvent, PointID: 8},
		{Key: []byte("g"), Value: []byte("v1"), EntryType: core.EntryTypePutEvent, PointID: 9},
		{Key: []byte("h"), Value: []byte("v1"), EntryType: core.EntryTypePutEvent, PointID: 10},
	}
	// Sort them correctly: Key ASC, PointID DESC
	sort.SliceStable(entries, func(i, j int) bool {
		cmp := bytes.Compare(entries[i].Key, entries[j].Key)
		if cmp == 0 {
			return entries[i].PointID > entries[j].PointID
		}
		return cmp < 0
	})
	return entries
}

// writeTestSSTableForIterator is a helper to create an SSTable with specific options for iterator tests.
func writeTestSSTableForIterator(t *testing.T, dir string, entries []testEntry, fileID uint64, logger *slog.Logger) (*SSTable, string) {
	t.Helper()
	testBlockSize := 64 // Force small block size for testing multiple blocks
	compressor := &compressors.NoCompressionCompressor{}
	writerOpts := core.SSTableWriterOptions{
		DataDir:                      dir,
		ID:                           fileID,
		EstimatedKeys:                uint64(len(entries)),
		BloomFilterFalsePositiveRate: 0.01,
		BlockSize:                    testBlockSize,
		Tracer:                       nil,
		Compressor:                   compressor,
		Logger:                       logger,
	}
	writer, err := NewSSTableWriter(writerOpts)
	require.NoError(t, err)

	for _, entry := range entries {
		err := writer.Add(entry.Key, entry.Value, entry.EntryType, entry.PointID)
		require.NoError(t, err)
	}

	err = writer.Finish()
	require.NoError(t, err)

	filePath := writer.FilePath()

	loadOpts := LoadSSTableOptions{
		FilePath:   filePath,
		ID:         fileID,
		BlockCache: nil,
		Tracer:     nil,
		Logger:     logger,
	}
	sst, err := LoadSSTable(loadOpts)
	require.NoError(t, err)
	return sst, filePath
}

// setupIteratorTest creates a standard SSTable for iterator tests.
func setupIteratorTest(t *testing.T) (*SSTable, []testEntry) {
	t.Helper()
	tempDir := t.TempDir()
	logger := slog.Default()
	entries := createIteratorTestEntries()
	fileID := uint64(101)

	sst, sstPath := writeTestSSTableForIterator(t, tempDir, entries, fileID, logger)
	t.Cleanup(func() {
		sst.Close()
		os.Remove(sstPath)
	})

	return sst, entries
}

// collectIteratorResults drains an iterator and returns all its entries.
func collectIteratorResults(t *testing.T, it core.Interface) []testEntry {
	t.Helper()
	var results []testEntry
	for it.Next() {
		key, val, entryType, pointID := it.At()
		results = append(results, testEntry{
			Key:       append([]byte(nil), key...),
			Value:     append([]byte(nil), val...),
			EntryType: entryType,
			PointID:   pointID,
		})
	}
	require.NoError(t, it.Error(), "Iterator should not have an error")
	return results
}

func TestSSTableIterator_Ascending(t *testing.T) {
	sst, allEntries := setupIteratorTest(t)

	t.Run("FullScan", func(t *testing.T) {
		it, err := sst.NewIterator(nil, nil, nil, core.Ascending)
		require.NoError(t, err)
		defer it.Close()

		results := collectIteratorResults(t, it)
		require.Equal(t, allEntries, results, "Full ascending scan should return all entries in order")
	})

	t.Run("RangeScan_InclusiveStart_ExclusiveEnd", func(t *testing.T) {
		startKey := []byte("c")
		endKey := []byte("g") // Should not include 'g'

		it, err := sst.NewIterator(startKey, endKey, nil, core.Ascending)
		require.NoError(t, err)
		defer it.Close()

		results := collectIteratorResults(t, it)

		var expected []testEntry
		for _, e := range allEntries {
			if bytes.Compare(e.Key, startKey) >= 0 && bytes.Compare(e.Key, endKey) < 0 {
				expected = append(expected, e)
			}
		}
		require.Equal(t, expected, results, "Ascending range scan results mismatch")
	})

	t.Run("RangeScan_ToEnd", func(t *testing.T) {
		startKey := []byte("e")

		it, err := sst.NewIterator(startKey, nil, nil, core.Ascending)
		require.NoError(t, err)
		defer it.Close()

		results := collectIteratorResults(t, it)

		var expected []testEntry
		for _, e := range allEntries {
			if bytes.Compare(e.Key, startKey) >= 0 {
				expected = append(expected, e)
			}
		}
		require.Equal(t, expected, results)
	})
}

func TestSSTableIterator_Descending(t *testing.T) {
	sst, allEntries := setupIteratorTest(t)

	// Create expected descending results
	expectedDescending := make([]testEntry, len(allEntries))
	copy(expectedDescending, allEntries)
	// Reverse the slice
	for i, j := 0, len(expectedDescending)-1; i < j; i, j = i+1, j-1 {
		expectedDescending[i], expectedDescending[j] = expectedDescending[j], expectedDescending[i]
	}

	t.Run("FullScan", func(t *testing.T) {
		it, err := sst.NewIterator(nil, nil, nil, core.Descending)
		require.NoError(t, err)
		defer it.Close()

		results := collectIteratorResults(t, it)
		require.Equal(t, expectedDescending, results, "Full descending scan should return all entries in reverse order")
	})

	t.Run("RangeScan_InclusiveStart_ExclusiveEnd", func(t *testing.T) {
		startKey := []byte("c")
		endKey := []byte("g") // Should not include 'g'

		it, err := sst.NewIterator(startKey, endKey, nil, core.Descending)
		require.NoError(t, err)
		defer it.Close()

		results := collectIteratorResults(t, it)

		var expected []testEntry
		for _, e := range expectedDescending { // Iterate through reversed list
			if bytes.Compare(e.Key, startKey) >= 0 && bytes.Compare(e.Key, endKey) < 0 {
				expected = append(expected, e)
			}
		}
		require.Equal(t, expected, results, "Descending range scan results mismatch")
	})

	t.Run("RangeScan_FromStart", func(t *testing.T) {
		endKey := []byte("e") // Exclusive

		it, err := sst.NewIterator(nil, endKey, nil, core.Descending)
		require.NoError(t, err)
		defer it.Close()

		results := collectIteratorResults(t, it)

		var expected []testEntry
		for _, e := range expectedDescending {
			if bytes.Compare(e.Key, endKey) < 0 {
				expected = append(expected, e)
			}
		}
		require.Equal(t, expected, results)
	})
}

func TestSSTableIterator_EdgeCases(t *testing.T) {
	sst, _ := setupIteratorTest(t)

	t.Run("EmptySSTable", func(t *testing.T) {
		tempDir := t.TempDir()
		emptySST, path := writeTestSSTableForIterator(t, tempDir, []testEntry{}, 202, slog.Default())
		defer emptySST.Close()
		defer os.Remove(path)

		it, err := emptySST.NewIterator(nil, nil, nil, core.Ascending)
		require.NoError(t, err)
		defer it.Close()

		require.False(t, it.Next(), "Next() on empty SSTable should be false")
	})

	t.Run("InvalidRange_StartAfterEnd", func(t *testing.T) {
		startKey := []byte("g")
		endKey := []byte("c")

		it, err := sst.NewIterator(startKey, endKey, nil, core.Ascending)
		require.NoError(t, err)
		defer it.Close()

		results := collectIteratorResults(t, it)
		require.Empty(t, results, "Iterator with start > end should return no results")
	})

	t.Run("RangeWithNoData", func(t *testing.T) {
		startKey := []byte("b_intermediate")
		endKey := []byte("b_z")

		it, err := sst.NewIterator(startKey, endKey, nil, core.Ascending)
		require.NoError(t, err)
		defer it.Close()

		results := collectIteratorResults(t, it)
		require.Empty(t, results, "Iterator over a range with no data should return no results")
	})

	t.Run("SeekToNonExistentKey", func(t *testing.T) {
		// The iterator should start at the next key after the non-existent start key.
		startKey := []byte("c_nonexistent")

		it, err := sst.NewIterator(startKey, nil, nil, core.Ascending)
		require.NoError(t, err)
		defer it.Close()

		// The first result should be "d"
		require.True(t, it.Next(), "Iterator should find the next key")
		key, _, _, _ := it.At()
		require.Equal(t, []byte("d"), key, "Iterator should start at the first key >= startKey")
	})
}
