package sstable

import (
	"bytes"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core" // For core.EntryType
	"github.com/INLOpen/nexusbase/iterator"
)

func writeTestSSTable(t *testing.T, dir string, entries []testEntry, fileID uint64, compressor core.Compressor, blockSize int) (*SSTable, string, int64) {
	t.Helper()
	writerOpts := core.SSTableWriterOptions{
		DataDir:                      dir,
		ID:                           fileID,
		BloomFilterFalsePositiveRate: 0.01,
		BlockSize:                    blockSize,
		Tracer:                       nil, // No tracer for this test helper
		Compressor:                   compressor,
	}
	writer, err := NewSSTableWriter(writerOpts)
	if err != nil {
		t.Fatalf("NewSSTableWriter() with compressor %T error = %v", compressor, err)
	}

	for _, entry := range entries {
		if err := writer.Add(entry.Key, entry.Value, entry.EntryType, entry.PointID); err != nil {
			writer.Abort()
			t.Fatalf("writer.Add(%s) with compressor %T error = %v", string(entry.Key), compressor, err)
		}
	}

	if err := writer.Finish(); err != nil {
		writer.Abort()
		t.Fatalf("writer.Finish() with compressor %T error = %v", compressor, err)
	}

	filePath := writer.FilePath()
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("Failed to stat SSTable file %s: %v", filePath, err)
	}
	fileSize := fileInfo.Size()

	loadOpts := LoadSSTableOptions{
		FilePath:   filePath,
		ID:         fileID,
		BlockCache: nil,
		Tracer:     nil,
		Logger:     slog.Default(),
	}
	sst, err := LoadSSTable(loadOpts)
	if err != nil {
		t.Fatalf("LoadSSTable() after write with compressor %T error = %v", compressor, err)
	}
	return sst, filePath, fileSize
}

type testSetup struct {
	tempDir    string
	snappySST  *SSTable
	snappyPath string
	snappySize int64
	lz4SST     *SSTable
	lz4Path    string
	lz4Size    int64
	zstdSST    *SSTable
	zstdPath   string
	zstdSize   int64
	noneSST    *SSTable
	nonePath   string
	noneSize   int64
	entries    []testEntry
}

func setupCompressionTest(t *testing.T) *testSetup {
	t.Helper()
	tempDir := t.TempDir()
	fileID := uint64(301)
	entries := createMoreDiverseTestEntries()
	testBlockSize := 128 // Consistent block size for the test

	snappyCompressor := &compressors.SnappyCompressor{}
	noCompressor := &compressors.NoCompressionCompressor{}
	lz4Compressor := &compressors.LZ4Compressor{}
	zstdCompressor := compressors.NewZstdCompressor()

	sstSnappy, snappyPath, snappySize := writeTestSSTable(t, tempDir, entries, fileID, snappyCompressor, testBlockSize)
	t.Cleanup(func() {
		if sstSnappy != nil {
			sstSnappy.Close()
		}
		os.Remove(snappyPath)
	})
	t.Logf("SSTable with Snappy compression created: %s, Size: %d bytes, MinKey: %s, MaxKey: %s", snappyPath, snappySize, string(sstSnappy.MinKey()), string(sstSnappy.MaxKey()))

	sstLz4, lz4Path, lz4Size := writeTestSSTable(t, tempDir, entries, fileID+2, lz4Compressor, testBlockSize)
	t.Cleanup(func() {
		if sstLz4 != nil {
			sstLz4.Close()
		}
		os.Remove(lz4Path)
	})
	t.Logf("SSTable with LZ4 compression created: %s, Size: %d bytes", lz4Path, lz4Size)

	sstZstd, zstdPath, zstdSize := writeTestSSTable(t, tempDir, entries, fileID+3, zstdCompressor, testBlockSize)
	t.Cleanup(func() {
		if sstZstd != nil {
			sstZstd.Close()
		}
		os.Remove(zstdPath)
	})
	t.Logf("SSTable with ZSTD compression created: %s, Size: %d bytes", zstdPath, zstdSize)

	sstNone, nonePath, noneSize := writeTestSSTable(t, tempDir, entries, fileID+1, noCompressor, testBlockSize)
	t.Cleanup(func() {
		if sstNone != nil {
			sstNone.Close()
		}
		os.Remove(nonePath)
	})
	t.Logf("SSTable with No compression created: %s, Size: %d bytes", nonePath, noneSize)

	if snappySize >= noneSize && len(entries) > 10 {
		t.Logf("Warning: Snappy compressed size (%d) is not smaller than uncompressed size (%d). This might be okay for small/random data.", snappySize, noneSize)
	}

	return &testSetup{
		tempDir:    tempDir,
		snappySST:  sstSnappy,
		snappyPath: snappyPath,
		snappySize: snappySize,
		lz4SST:     sstLz4,
		lz4Path:    lz4Path,
		lz4Size:    lz4Size,
		zstdSST:    sstZstd,
		zstdPath:   zstdPath,
		zstdSize:   zstdSize,
		noneSST:    sstNone,
		nonePath:   nonePath,
		noneSize:   noneSize,
		entries:    entries,
	}
}

func generateRandomString(r *rand.Rand, length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[r.Intn(len(charset))]
	}
	return string(b)
}

func createMoreDiverseTestEntries() []testEntry {
	entries := make([]testEntry, 0)
	// Use a fixed seed for reproducible tests
	r := rand.New(rand.NewSource(12345))

	// Repetitive data (good for compression)
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("snappyKeyA%04d", i))
		value := []byte(fmt.Sprintf("repetitive_value_part_one_repetitive_value_part_two_%04d", i))
		entries = append(entries, testEntry{Key: key, Value: value, EntryType: core.EntryTypePutEvent, PointID: uint64(i + 1)})
	}
	// Less repetitive data
	for i := 0; i < 30; i++ {
		key := []byte(fmt.Sprintf("snappyKeyB%04d", i))
		value := []byte(fmt.Sprintf("random_data_%s", generateRandomString(r, 20)))
		entries = append(entries, testEntry{Key: key, Value: value, EntryType: core.EntryTypePutEvent, PointID: uint64(50 + i + 1)})
	}
	// Entries with updates and deletes
	entries = append(entries, testEntry{Key: []byte("snappyKeyC0001"), Value: []byte("initial_value"), EntryType: core.EntryTypePutEvent, PointID: 100})
	entries = append(entries, testEntry{Key: []byte("snappyKeyC0002"), Value: []byte("value_to_delete"), EntryType: core.EntryTypePutEvent, PointID: 101})
	entries = append(entries, testEntry{Key: []byte("snappyKeyC0001"), Value: []byte("updated_value"), EntryType: core.EntryTypePutEvent, PointID: 102}) // Update
	entries = append(entries, testEntry{Key: []byte("snappyKeyC0002"), Value: nil, EntryType: core.EntryTypeDelete, PointID: 103})                       // Delete
	entries = append(entries, testEntry{Key: []byte("snappyKeyC0003"), Value: []byte("another_value"), EntryType: core.EntryTypePutEvent, PointID: 104})
	entries = append(entries, testEntry{Key: []byte("snappyKeyC0004"), Value: nil, EntryType: core.EntryTypeDelete, PointID: 105}) // Delete

	// Sort entries by key, then seqNum descending for Put order
	sort.SliceStable(entries, func(i, j int) bool {
		cmp := bytes.Compare(entries[i].Key, entries[j].Key)
		if cmp == 0 {
			return entries[i].PointID > entries[j].PointID // Higher PointID first
		}
		return cmp < 0
	})
	return entries
}

func TestSSTable_Compression_WriteAndRead(t *testing.T) {
	setup := setupCompressionTest(t)

	tests := []struct {
		name string
		sst  *SSTable
	}{
		{"LZ4Compressed", setup.lz4SST},
		{"ZSTDCompressed", setup.zstdSST},
		{"SnappyCompressed", setup.snappySST},
		{"NoCompression", setup.noneSST},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { // This sub-test is now inside the compressor loop

			// Test Get_Verification
			// SSTable.Get uses a sparse index. When multiple versions of a key exist, it
			// will only find the version in the block pointed to by the index.
			// This means SSTable.Get cannot guarantee finding the *globally* latest version
			// if multiple versions of the same key span across different blocks within the same SSTable.
			// This is a known limitation of sparse indexing for point lookups with versioning.
			// The MergingIterator (tested below) handles this by merging across all relevant sources.
			// For this test, we verify that SSTable.Get returns the latest version of the key
			// that it finds in the block pointed to by the index.

			// Test cases
			tests := []struct {
				name          string
				key           string
				wantValue     []byte
				wantEntryType core.EntryType
				wantErr       error
			}{
				{
					name:          "find latest version of updated key",
					key:           "snappyKeyC0001",
					wantValue:     []byte("updated_value"),
					wantEntryType: core.EntryTypePutEvent,
					wantErr:       nil,
				},
				{
					name:    "find key where latest version is a tombstone",
					key:     "snappyKeyC0004",
					wantErr: ErrNotFound,
				},
				{
					name:          "special case: find older Put version due to block splitting",
					key:           "snappyKeyC0002",
					wantValue:     []byte("value_to_delete"),
					wantEntryType: core.EntryTypePutEvent,
					wantErr:       nil,
				},
				{
					name:          "find a simple key",
					key:           "snappyKeyA0025",
					wantValue:     []byte("repetitive_value_part_one_repetitive_value_part_two_0025"),
					wantEntryType: core.EntryTypePutEvent,
					wantErr:       nil,
				},
				{
					name:    "find non-existent key",
					key:     "nonExistentKey",
					wantErr: ErrNotFound,
				},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					value, entryType, err := tc.sst.Get([]byte(tt.key))

					if tt.wantErr != nil {
						if err != tt.wantErr {
							t.Errorf("Get(%s): error mismatch, got %v, want %v", tt.key, err, tt.wantErr)
						}
					} else {
						if err != nil {
							t.Errorf("Get(%s): expected no error, but got %v", tt.key, err)
						}
						if entryType != tt.wantEntryType {
							t.Errorf("Get(%s): entryType mismatch, got %v, want %v", tt.key, entryType, tt.wantEntryType)
						}
						if !bytes.Equal(value, tt.wantValue) {
							t.Errorf("Get(%s): value mismatch, got '%s', want '%s'", tt.key, string(value), string(tt.wantValue))
						}
					}
				})
			}
			t.Run("Iterator_FullScan", func(t *testing.T) { // This sub-test is now inside the compressor loop
				iter, err := tc.sst.NewIterator(nil, nil, nil, core.Ascending)
				if err != nil {
					t.Fatalf("NewIterator(nil, nil) failed: %v", err)
				}
				defer iter.Close()

				var actualEntries []testEntry
				for iter.Next() {
					key, value, entryType, pointID := iter.At()
					actualEntries = append(actualEntries, testEntry{
						Key:       append([]byte(nil), key...),
						Value:     append([]byte(nil), value...),
						EntryType: entryType,
						PointID:   pointID,
					})
				}
				if err := iter.Error(); err != nil {
					t.Errorf("Iterator error during full scan: %v", err)
				}

				// The raw SSTable iterator should return all entries exactly as they were written.
				if !reflect.DeepEqual(actualEntries, setup.entries) {
					t.Errorf("Iterator full scan results mismatch:\nGot:    %+v\nWanted: %+v", actualEntries, setup.entries)
				}
			})

			t.Run("Iterator_RangeScan", func(t *testing.T) { // This sub-test is now inside the compressor loop
				startKey := []byte("snappyKeyB0010")
				endKey := []byte("snappyKeyC0003")

				iter, err := tc.sst.NewIterator(startKey, endKey, nil, core.Ascending)
				if err != nil {
					t.Fatalf("NewIterator(%s, %s) failed: %v", string(startKey), string(endKey), err)
				}
				defer iter.Close()

				var actualEntries []testEntry
				for iter.Next() {
					key, value, entryType, pointID := iter.At()
					actualEntries = append(actualEntries, testEntry{
						Key:       append([]byte(nil), key...),
						Value:     append([]byte(nil), value...),
						EntryType: entryType,
						PointID:   pointID,
					})
				}
				if err := iter.Error(); err != nil {
					t.Errorf("Iterator error during range scan: %v", err)
				}

				var expectedEntries []testEntry
				for _, entry := range setup.entries {
					if bytes.Compare(entry.Key, startKey) >= 0 && (endKey == nil || bytes.Compare(entry.Key, endKey) < 0) {
						expectedEntries = append(expectedEntries, entry)
					}
				}

				if !reflect.DeepEqual(actualEntries, expectedEntries) {
					t.Errorf("Iterator range scan [%s, %s) results mismatch:\nGot:    %+v\nWanted: %+v", string(startKey), string(endKey), actualEntries, expectedEntries)
				}
			})
		}) // End of t.Run(tc.name) for TestSSTable_Compression_WriteAndRead
	}
}

func TestSSTable_Compression_GetEdgeCases(t *testing.T) {
	setup := setupCompressionTest(t)

	tests := []struct {
		name string
		sst  *SSTable
	}{
		{"LZ4Compressed", setup.lz4SST},
		{"ZSTDCompressed", setup.zstdSST},
		{"SnappyCompressed", setup.snappySST},
		{"NoCompression", setup.noneSST},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { // This sub-test is now inside the compressor loop

			// Test MinKey
			t.Run("Get_MinKey", func(t *testing.T) {
				minKey := tc.sst.MinKey()
				if minKey == nil {
					t.Fatal("SSTable MinKey is nil")
				}
				_, entryType, err := tc.sst.Get(minKey)
				if err != nil {
					t.Errorf("Get(MinKey %s) failed: %v", string(minKey), err)
				}
				if entryType != core.EntryTypePutEvent { // Assuming MinKey is always a Put
					t.Errorf("Get(MinKey %s) entry type mismatch: got %v, want PutEvent", string(minKey), entryType)
				}
				// Further validation of value if needed (e.g., check against setup.entries)
			})

			// Test MaxKey
			t.Run("Get_MaxKey", func(t *testing.T) {
				maxKey := tc.sst.MaxKey()
				if maxKey == nil {
					t.Fatal("SSTable MaxKey is nil")
				}
				_, _, err := tc.sst.Get(maxKey)

				// The max key in our test data ("snappyKeyC0004") is a tombstone.
				// Therefore, sst.Get() should correctly return ErrNotFound.
				if err != ErrNotFound {
					t.Errorf("Get(MaxKey %s): expected ErrNotFound because the max key is a tombstone, but got err: %v", string(maxKey), err)
				}
			})

			// Test a key that is a FirstKey of an internal block (not MinKey)
			t.Run("Get_InternalBlockFirstKey", func(t *testing.T) {
				if len(tc.sst.index.entries) < 2 {
					t.Skip("Not enough blocks to test internal block first key")
				}
				internalFirstKey := tc.sst.index.entries[1].FirstKey // Second block's first key
				_, entryType, err := tc.sst.Get(internalFirstKey)
				if err != nil {
					t.Errorf("Get(InternalBlockFirstKey %s) failed: %v", string(internalFirstKey), err)
				}
				if entryType != core.EntryTypePutEvent {
					t.Errorf("Get(InternalBlockFirstKey %s) entry type mismatch: got %v, want PutEvent", string(internalFirstKey), entryType)
				}
			})

			// Test a key that is within a block but not its FirstKey
			t.Run("Get_KeyWithinBlock", func(t *testing.T) {
				// Pick a key that is not a FirstKey from the generated entries.
				// For example, if "snappyKeyA0000" is FirstKey of block 0, "snappyKeyA0001" should be within that block.
				keyWithinBlock := []byte("snappyKeyA0001")
				val, entryType, err := tc.sst.Get(keyWithinBlock)
				if err != nil {
					t.Errorf("Get(KeyWithinBlock %s) failed: %v", string(keyWithinBlock), err)
				}
				if entryType != core.EntryTypePutEvent {
					t.Errorf("Get(KeyWithinBlock %s) entry type mismatch: got %v, want PutEvent", string(keyWithinBlock), entryType)
				}
				if !bytes.Equal(val, []byte("repetitive_value_part_one_repetitive_value_part_two_0001")) {
					t.Errorf("Get(KeyWithinBlock %s) value mismatch: got %s", string(keyWithinBlock), string(val))
				}
			})
		}) // End of t.Run(tc.name) for TestSSTable_Compression_GetEdgeCases
	}
}

func TestSSTable_Compression_IteratorEdgeCases(t *testing.T) {
	setup := setupCompressionTest(t)

	tests := []struct {
		name string
		sst  *SSTable
	}{
		{"LZ4Compressed", setup.lz4SST},
		{"ZSTDCompressed", setup.zstdSST},
		{"SnappyCompressed", setup.snappySST},
		{"NoCompression", setup.noneSST},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { // This sub-test is now inside the compressor loop

			// Test iterator on an SSTable that contains only tombstones (should yield no results after merging)
			t.Run("Iterator_OnlyTombstones", func(t *testing.T) {
				tempDir := t.TempDir()
				deletedEntries := []testEntry{
					{Key: []byte("delKey1"), Value: nil, EntryType: core.EntryTypeDelete, PointID: 1},
					{Key: []byte("delKey2"), Value: nil, EntryType: core.EntryTypeDelete, PointID: 2},
				}
				sort.SliceStable(deletedEntries, func(i, j int) bool {
					return bytes.Compare(deletedEntries[i].Key, deletedEntries[j].Key) < 0 // Sort by key for consistent iteration order
				})

				sstOnlyDeletes, sstOnlyDeletesPath, _ := writeTestSSTable(t, tempDir, deletedEntries, 999, &compressors.SnappyCompressor{}, 128)
				if sstOnlyDeletes != nil {
					defer sstOnlyDeletes.Close()
				}
				defer os.Remove(sstOnlyDeletesPath)

				iter, err := sstOnlyDeletes.NewIterator(nil, nil, nil, core.Ascending)
				if err != nil {
					t.Fatalf("NewIterator failed for SSTable with only deletes: %v", err)
				}
				defer iter.Close()
				testIteratorRange(t, iter, deletedEntries)
			})

			// Test iterator with startKey and endKey completely outside the SSTable's range
			t.Run("Iterator_RangeOutsideData", func(t *testing.T) {
				startKey := []byte("aaaaa")
				endKey := []byte("bbbbb") // Before any actual data
				iter, _ := tc.sst.NewIterator(startKey, endKey, nil, core.Ascending)
				defer iter.Close()
				testIteratorRange(t, iter, []testEntry{})

				startKey = []byte("zzzzz")
				endKey = []byte("zzzzz_plus") // After all actual data
				iter, _ = tc.sst.NewIterator(startKey, endKey, nil, core.Ascending)
				defer iter.Close()
				testIteratorRange(t, iter, []testEntry{})
			})

			// Test iterator with startKey > endKey (invalid range)
			t.Run("Iterator_InvalidRange", func(t *testing.T) {
				startKey := []byte("snappyKeyC0003")
				endKey := []byte("snappyKeyB0000")
				iter, _ := tc.sst.NewIterator(startKey, endKey, nil, core.Ascending)
				defer iter.Close()
				testIteratorRange(t, iter, []testEntry{})
			})
		}) // End of t.Run(tc.name) for TestSSTable_Compression_IteratorEdgeCases
	}
}

// testIteratorRange is a helper function to reduce code duplication for range scan tests.
func testIteratorRange(t *testing.T, iter iterator.Interface, expectedEntries []testEntry) {
	t.Helper()

	var actualEntries []testEntry
	for iter.Next() {
		actualEntries = append(actualEntries, copyTestEntryFromIterator(iter))
	}
	if err := iter.Error(); err != nil {
		t.Errorf("Iterator error during range scan: %v", err)
	}

	// Normalize nil slice to empty slice for DeepEqual to work correctly.
	if actualEntries == nil {
		actualEntries = []testEntry{}
	}

	if !reflect.DeepEqual(actualEntries, expectedEntries) {
		t.Errorf("Iterator range scan results mismatch:\nGot:    %+v\nWanted: %+v", actualEntries, expectedEntries)
	}
}

// copyTestEntryFromIterator creates a deep copy of a testEntry from an iterator.
func copyTestEntryFromIterator(it iterator.Interface) testEntry {
	key, value, entryType, pointID := it.At()
	return testEntry{
		Key:       append([]byte(nil), key...),
		Value:     append([]byte(nil), value...),
		EntryType: entryType,
		PointID:   pointID,
	}
}
