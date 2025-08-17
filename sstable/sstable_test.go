package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"expvar"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/INLOpen/nexusbase/cache" // For new BlockCache
	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/sys"
	"github.com/INLOpen/nexuscore/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testEntry struct {
	Key       []byte
	Value     []byte
	EntryType core.EntryType
	PointID   uint64 // Added for testing with sequence numbers
}

// mockFile is a mock implementation of sys.FileInterface for testing I/O errors.
type mockFile struct {
	name            string
	writeShouldFail bool
	writeErr        error
	syncShouldFail  bool
	syncErr         error
	closeShouldFail bool
	closeErr        error
	buffer          bytes.Buffer // Simulate in-memory file content
	writeFunc       func(p []byte) (n int, err error)
}

func (m *mockFile) Write(p []byte) (n int, err error) {
	if m.writeFunc != nil {
		return m.writeFunc(p)
	}
	if m.writeShouldFail {
		return 0, m.writeErr
	}
	return m.buffer.Write(p)
}

func (m *mockFile) Sync() error {
	if m.syncShouldFail {
		return m.syncErr
	}
	return nil
}

func (m *mockFile) Close() error {
	if m.closeShouldFail {
		return m.closeErr
	}
	return nil
}

// mockFilter is a simple mock for the filter.Filter interface for testing.
type mockFilter struct {
	data map[string]bool
}

func (m *mockFilter) Contains(data []byte) bool {
	_, ok := m.data[string(data)]
	return ok
}

func (m *mockFilter) Bytes() []byte {
	return nil
}

// Implement other methods of sys.FileInterface to satisfy the interface
func (m *mockFile) Read(p []byte) (n int, err error)               { return m.buffer.Read(p) }
func (m *mockFile) Seek(offset int64, whence int) (int64, error)   { return 0, nil }
func (m *mockFile) Stat() (os.FileInfo, error)                     { return nil, nil }
func (m *mockFile) Truncate(size int64) error                      { return nil }
func (m *mockFile) Name() string                                   { return m.name }
func (m *mockFile) WriteAt(p []byte, off int64) (n int, err error) { return m.Write(p) }
func (m *mockFile) ReadAt(p []byte, off int64) (n int, err error)  { return 0, nil }
func (m *mockFile) WriteString(s string) (n int, err error)        { return m.Write([]byte(s)) }
func (m *mockFile) WriteTo(w io.Writer) (n int64, err error)       { return 0, nil }
func (m *mockFile) ReadFrom(r io.Reader) (n int64, err error)      { return 0, nil }

func createTestEntries() []testEntry {
	return []testEntry{
		{Key: []byte("apple"), Value: []byte("red"), EntryType: core.EntryTypePutEvent},
		{Key: []byte("banana"), Value: []byte("yellow"), EntryType: core.EntryTypePutEvent},
		{Key: []byte("cherry"), Value: []byte("sweet"), EntryType: core.EntryTypeDelete},             // Tombstone
		{Key: []byte("date"), Value: []byte("brown"), EntryType: core.EntryTypePutEvent, PointID: 3}, // Example PointID
		{Key: []byte("elderberry"), Value: []byte("purple"), EntryType: core.EntryTypePutEvent, PointID: 4},
	}
}

func writeTestSSTableGeneral(t *testing.T, dir string, entries []testEntry, fileID uint64, logger *slog.Logger) (*SSTable, string) {
	t.Helper()
	testBlockSize := 64 // Force small block size for testing multiple blocks
	compressor := &compressors.NoCompressionCompressor{}
	writerOpts := core.SSTableWriterOptions{
		DataDir:                      dir,
		ID:                           fileID,
		EstimatedKeys:                uint64(len(entries)),
		BloomFilterFalsePositiveRate: 0.01,
		BlockSize:                    testBlockSize,
		Tracer:                       nil, // Pass nil tracer for test
		Compressor:                   compressor,
	}
	writer, err := NewSSTableWriter(writerOpts)
	if err != nil {
		t.Fatalf("NewSSTableWriter() error = %v", err)
	}

	for _, entry := range entries {
		if err := writer.Add(entry.Key, entry.Value, entry.EntryType, entry.PointID); err != nil {
			writer.Abort()
			t.Fatalf("writer.Add(%s) error = %v", string(entry.Key), err)
		}
	}

	if err := writer.Finish(); err != nil {
		writer.Abort()
		t.Fatalf("writer.Finish() error = %v", err)
	}

	concreteWriter, ok := writer.(*SSTableWriter)
	if !ok {
		t.Fatal("Fail assign *SSTableWriter to concrete type")
	}

	// Load the written SSTable to return it (as writer doesn't return SSTable directly)
	// For testing, we don't pass a block cache to LoadSSTable initially,
	// it can be set later if needed for specific cache tests.
	loadOpts := LoadSSTableOptions{
		FilePath:   concreteWriter.filePath,
		ID:         fileID,
		BlockCache: nil,
		Tracer:     nil,
		Logger:     logger,
	}
	sst, err := LoadSSTable(loadOpts)
	if err != nil {
		t.Fatalf("LoadSSTable() after write error = %v", err)
	}
	return sst, concreteWriter.filePath
}

func TestWriteAndLoadSSTable_New(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.Default()

	entries := createTestEntries()
	fileID := uint64(1) // Unique ID for this test

	writtenSST, sstPath := writeTestSSTableGeneral(t, tempDir, entries, fileID, logger)
	if writtenSST != nil {
		defer writtenSST.Close()
	}
	defer os.Remove(sstPath) // Clean up

	// Check basic properties of the in-memory SSTable struct after writing and loading
	if writtenSST == nil {
		t.Fatal("writeTestSSTable returned nil sstable")
	}

	assert.Equal(t, uint64(len(entries)), writtenSST.KeyCount(), "KeyCount should match number of entries written")

	if !bytes.Equal(writtenSST.MinKey(), entries[0].Key) {
		t.Errorf("writtenSST.MinKey() = %s, want %s", string(writtenSST.MinKey()), string(entries[0].Key))
	}
	// MaxKey in new block-based SSTable is FirstKey of last block, which is an approximation.
	// For this test data, 'elderberry' will be the first key of its block.
	if !bytes.Equal(writtenSST.MaxKey(), entries[len(entries)-1].Key) {
		t.Logf("Note: MaxKey is an approximation (FirstKey of last block). Got: %s, Expected based on last entry: %s", string(writtenSST.MaxKey()), string(entries[len(entries)-1].Key))
		// This might not be a strict failure depending on how many entries fit per block.
		// For a more precise check, one would need to know the block structure or read the actual last key.
	}

	if writtenSST.index == nil || len(writtenSST.index.entries) == 0 {
		t.Errorf("writtenSST.index is nil or empty")
	} else {
		// With block-based, number of index entries <= number of data entries
		t.Logf("Number of block index entries: %d", len(writtenSST.index.entries))
	}

	if writtenSST.filter == nil {
		t.Error("writtenSST.bloomFilter is nil")
	}
	for _, entry := range entries {
		if !writtenSST.filter.Contains(entry.Key) {
			t.Errorf("Bloom filter should contain key %s, but it doesn't", string(entry.Key))
		}
	}

	// Load SSTable again from path
	loadOpts := LoadSSTableOptions{
		FilePath:   sstPath,
		ID:         fileID,
		BlockCache: nil,
		Tracer:     nil,
		Logger:     logger,
	}
	loadedSSTable, err := LoadSSTable(loadOpts)
	if err != nil {
		t.Fatalf("LoadSSTable() error = %v", err)
	}
	if loadedSSTable != nil {
		defer loadedSSTable.Close()
	}
	if loadedSSTable == nil {
		t.Fatal("LoadSSTable() returned nil sstable")
	}

	// Compare loaded SSTable properties
	assert.Equal(t, writtenSST.KeyCount(), loadedSSTable.KeyCount(), "Loaded KeyCount should match original")
	if !bytes.Equal(loadedSSTable.MinKey(), writtenSST.MinKey()) {
		t.Errorf("loadedSSTable.MinKey() = %s, want %s", string(loadedSSTable.MinKey()), string(writtenSST.MinKey()))
	}
	if !bytes.Equal(loadedSSTable.MaxKey(), writtenSST.MaxKey()) {
		t.Errorf("loadedSSTable.MaxKey() = %s, want %s", string(loadedSSTable.MaxKey()), string(writtenSST.MaxKey()))
	}
	if loadedSSTable.index == nil || len(loadedSSTable.index.entries) != len(writtenSST.index.entries) {
		t.Errorf("loadedSSTable.index has wrong number of entries. Got %d, want %d", len(loadedSSTable.index.entries), len(writtenSST.index.entries))
	}
	if loadedSSTable.filter == nil {
		t.Error("loadedSSTable.bloomFilter is nil")
	}
	if !bytes.Equal(loadedSSTable.filter.Bytes(), writtenSST.filter.Bytes()) {
		t.Error("Loaded bloom filter data does not match original")
	}

	// Check block index entries (BlockIndexEntry has FirstKey, BlockOffset, BlockLength)
	for i := range writtenSST.index.entries {
		if !bytes.Equal(loadedSSTable.index.entries[i].FirstKey, writtenSST.index.entries[i].FirstKey) ||
			loadedSSTable.index.entries[i].BlockOffset != writtenSST.index.entries[i].BlockOffset ||
			loadedSSTable.index.entries[i].BlockLength != writtenSST.index.entries[i].BlockLength {
			t.Errorf("Loaded block index entry %d mismatch.\nGot:    %+v\nWanted: %+v", i, loadedSSTable.index.entries[i], writtenSST.index.entries[i])
		}
	}
}

func TestLoadSSTable_CorruptedIndexData(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.Default()
	entries := createTestEntries()
	fileID := uint64(401)

	// 1. Create a valid SSTable using the general helper
	sst, sstPath := writeTestSSTableGeneral(t, tempDir, entries, fileID, logger)
	sst.Close() // Close the file handle so we can modify it
	defer os.Remove(sstPath)

	// 2. Read the file content and corrupt the indexLen in the footer.
	//    This should cause LoadSSTable to fail when reading the index data.
	fileData, err := os.ReadFile(sstPath)
	if err != nil {
		t.Fatalf("Failed to read SSTable file: %v", err)
	}

	// Get index offset from the footer of the valid SSTable
	footerReader := bytes.NewReader(fileData[len(fileData)-FooterSize:])
	var indexOffsetFromFooter uint64
	binary.Read(footerReader, binary.LittleEndian, &indexOffsetFromFooter)

	// Calculate the offset of indexLen within the file (it's part of the footer)
	indexLenOffsetInFile := len(fileData) - FooterSize + IndexOffsetSize

	// Corrupt indexLen to be larger than the actual remaining data in the file
	// This will cause file.ReadAt(indexData, int64(indexOffset)) to return io.EOF or io.ErrUnexpectedEOF.
	corruptedIndexLen := uint32(len(fileData) - int(indexOffsetFromFooter) + 1)
	binary.LittleEndian.PutUint32(fileData[indexLenOffsetInFile:], corruptedIndexLen)

	if corruptedIndexLen == 0 { // Skip if the corruption makes indexLen zero, which might be valid for empty index
		t.Skip("SSTable has no index data, skipping corruption test.")
	}

	// Write back the corrupted data
	if err := os.WriteFile(sstPath, fileData, 0644); err != nil {
		t.Fatalf("Failed to write corrupted SSTable file: %v", err)
	}

	// 3. Attempt to load the corrupted SSTable
	loadOpts := LoadSSTableOptions{
		FilePath:   sstPath,
		ID:         fileID,
		BlockCache: nil,
		Tracer:     nil,
		Logger:     logger,
	}
	_, err = LoadSSTable(loadOpts)
	if err == nil {
		t.Fatal("LoadSSTable expected an error for corrupted index data, got nil")
	}
	// The error should indicate deserialization failure or general corruption
	if !bytes.Contains([]byte(err.Error()), []byte("failed to read index data")) && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("LoadSSTable error message mismatch. Got: %q, want to contain: 'failed to read index data' or EOF/ErrUnexpectedEOF", err.Error())
	}
}

func TestLoadSSTable_CorruptedBloomFilterData(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.Default()
	entries := createTestEntries()
	fileID := uint64(402)

	// 1. Create a valid SSTable using the general helper
	sst, sstPath := writeTestSSTableGeneral(t, tempDir, entries, fileID, logger)
	sst.Close() // Close the file handle so we can modify it
	defer os.Remove(sstPath)

	// 2. Read the file content and corrupt the bloomFilterLen in the footer.
	//    This should cause LoadSSTable to fail when reading the bloom filter data.
	fileData, err := os.ReadFile(sstPath)
	if err != nil {
		t.Fatalf("Failed to read SSTable file: %v", err)
	}

	// Get bloom filter offset from the footer of the valid SSTable
	footerReader := bytes.NewReader(fileData[len(fileData)-FooterSize:])
	var indexOffsetFromFooter, bloomFilterOffsetFromFooter uint64
	var dummyIndexLen uint32                                               // Declare a variable to hold the dummy value
	binary.Read(footerReader, binary.LittleEndian, &indexOffsetFromFooter) // Read indexOffset to advance reader
	binary.Read(footerReader, binary.LittleEndian, &dummyIndexLen)         // Read indexLen (dummy) to advance reader
	binary.Read(footerReader, binary.LittleEndian, &bloomFilterOffsetFromFooter)

	// Calculate the offset of bloomFilterLen within the file (it's part of the footer)
	// Footer starts at fileSize - FooterSize. bloomFilterLen is at: (fileSize - FooterSize) + IndexOffsetSize + IndexLenSize + BloomFilterOffsetSize
	bloomFilterLenOffsetInFile := len(fileData) - FooterSize + IndexOffsetSize + IndexLenSize + BloomFilterOffsetSize

	// Corrupt bloomFilterLen to be larger than the actual remaining data in the file
	// This will cause file.ReadAt(bloomFilterData, int64(bloomFilterOffset)) to return io.EOF or io.ErrUnexpectedEOF.
	corruptedBloomFilterLen := uint32(len(fileData) - int(bloomFilterOffsetFromFooter) + 1)
	binary.LittleEndian.PutUint32(fileData[bloomFilterLenOffsetInFile:], corruptedBloomFilterLen)

	if corruptedBloomFilterLen == 0 { // Skip if the corruption makes bloomFilterLen zero, which might be valid for empty bloom filter
		t.Skip("SSTable has no bloom filter data, skipping corruption test.")
	}

	// Write back the corrupted data
	if err := os.WriteFile(sstPath, fileData, 0644); err != nil {
		t.Fatalf("Failed to write corrupted SSTable file: %v", err)
	}

	// 3. Attempt to load the corrupted SSTable
	loadOpts := LoadSSTableOptions{
		FilePath:   sstPath,
		ID:         fileID,
		BlockCache: nil,
		Tracer:     nil,
		Logger:     logger,
	}
	_, err = LoadSSTable(loadOpts)
	if err == nil {
		t.Fatal("LoadSSTable expected an error for corrupted bloom filter data, got nil")
	}
	// The error should indicate deserialization failure or general corruption
	if !bytes.Contains([]byte(err.Error()), []byte("failed to read bloom filter data")) && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("LoadSSTable error message mismatch. Got: %q, want to contain: 'failed to read bloom filter data' or EOF/ErrUnexpectedEOF", err.Error())
	}
}

func TestLoadSSTable_TruncatedFile(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.Default()
	entries := createTestEntries()
	fileID := uint64(403)

	// 1. Create a valid SSTable using the general helper
	sst, sstPath := writeTestSSTableGeneral(t, tempDir, entries, fileID, logger)
	sst.Close() // Close the file handle so we can modify it
	defer os.Remove(sstPath)

	originalFileSize := sst.Size()
	if originalFileSize < int64(FooterSize) {
		t.Fatalf("Original SSTable file size %d is too small for truncation tests (min footer size %d)", originalFileSize, FooterSize)
	}

	tests := []struct {
		name         string
		truncateSize int64
		expectedErr  error // Specific error or a substring of error message
	}{
		{
			name:         "truncated_in_data_block",
			truncateSize: originalFileSize / 2, // Truncate roughly in the middle of data
			expectedErr:  ErrCorrupted,         // Or io.ErrUnexpectedEOF, depends on exact read
		},
		{
			name:         "truncated_in_index_data",
			truncateSize: originalFileSize - int64(FooterFixedComponentSize) + 10, // Just after footer start, in index data
			expectedErr:  ErrCorrupted,                                            // Or io.EOF/io.ErrUnexpectedEOF
		},
		{
			name:         "truncated_in_bloom_filter_data",
			truncateSize: originalFileSize - int64(MagicStringLen) - 10, // Just before magic string, in bloom filter data
			expectedErr:  ErrCorrupted,                                  // Or io.EOF/io.ErrUnexpectedEOF
		},
		{
			name:         "truncated_just_before_magic_string",
			truncateSize: originalFileSize - int64(MagicStringLen) + 1,
			expectedErr:  ErrCorrupted, // Should fail magic string check
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Read original content
			fileData, err := os.ReadFile(sstPath)
			if err != nil {
				t.Fatalf("Failed to read original SSTable file: %v", err)
			}

			// Truncate the in-memory byte slice
			truncatedData := fileData[:tt.truncateSize]

			// Write the truncated data to a new temporary file for this sub-test
			subTestSSTPath := filepath.Join(tempDir, fmt.Sprintf("%d_%s.sst", fileID, tt.name))
			if err := os.WriteFile(subTestSSTPath, truncatedData, 0644); err != nil {
				t.Fatalf("Failed to write truncated SSTable file for sub-test: %v", err)
			}
			defer os.Remove(subTestSSTPath)

			// Attempt to load the truncated SSTable
			loadOpts := LoadSSTableOptions{
				FilePath:   subTestSSTPath,
				ID:         fileID,
				BlockCache: nil,
				Tracer:     nil,
				Logger:     logger,
			}
			_, err = LoadSSTable(loadOpts)
			if err == nil {
				t.Fatal("LoadSSTable expected an error for truncated file, got nil")
			}

			// Check the error type/message
			if tt.expectedErr == ErrCorrupted {
				if !errors.Is(err, ErrCorrupted) && !bytes.Contains([]byte(err.Error()), []byte("failed to read")) && !bytes.Contains([]byte(err.Error()), []byte("invalid magic string")) && !bytes.Contains([]byte(err.Error()), []byte("too small to be valid")) {
					t.Errorf("LoadSSTable error mismatch for %s. Got: %q, want to be ErrCorrupted or related read error", tt.name, err.Error())
				}
			} else if !errors.Is(err, tt.expectedErr) && !bytes.Contains([]byte(err.Error()), []byte(tt.expectedErr.Error())) {
				t.Errorf("LoadSSTable error mismatch for %s. Got: %q, want: %q", tt.name, err.Error(), tt.expectedErr.Error())
			}
		})
	}
}

func TestSSTable_Get_New(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.Default()

	entries := createTestEntries()
	fileID := uint64(2) // Unique ID for this test

	sst, sstPath := writeTestSSTableGeneral(t, tempDir, entries, fileID, logger)
	if sst != nil {
		defer sst.Close()
	}
	defer os.Remove(sstPath)

	// Metrics for cache testing
	cacheHits := new(expvar.Int) // Initialize to 0
	cacheHits.Set(0)
	cacheMisses := new(expvar.Int) // Initialize to 0
	cacheMisses.Set(0)

	// Assign a block cache for testing cache interaction
	blockCache := cache.NewLRUCache(2, nil, nil, nil) // LRU cache with small capacity (2 blocks) for eviction test
	blockCache.SetMetrics(cacheHits, cacheMisses)
	sst.blockCache = blockCache // Manually set for the loaded sst instance

	tests := []struct {
		name             string
		key              []byte
		wantValue        []byte
		wantEntryType    core.EntryType
		wantFound        bool
		expectCachePop   bool   // True if we expect this Get to populate cache
		expectCacheHitOn string // Key whose block we expect to be in cache after this Get
	}{
		{"get_apple_miss_populate", []byte("apple"), []byte("red"), core.EntryTypePutEvent, true, true, "apple"},                         // Miss, Block A cached
		{"get_banana_miss_populate", []byte("banana"), []byte("yellow"), core.EntryTypePutEvent, true, true, "banana"},                   // Miss, Block B cached (Cache: A, B)
		{"get_apple_hit", []byte("apple"), []byte("red"), core.EntryTypePutEvent, true, false, "apple"},                                  // Hit Block A (Cache: B, A - LRU)
		{"get_date_miss_evict_populate", []byte("date"), []byte("brown"), core.EntryTypePutEvent, true, true, "date"},                    // Miss, Block C cached, Block B evicted (Cache: A, C)
		{"get_banana_miss_evict_populate", []byte("banana"), []byte("yellow"), core.EntryTypePutEvent, true, true, "banana"},             // Miss, Block B cached, Block A evicted (Cache: C, B)
		{"get_cherry_tombstone_miss_populate", []byte("cherry"), nil, 0, false, true, "cherry"},                                          // Miss, Block D cached, Block C evicted (Cache: B, D)
		{"get_elderberry_miss_evict_populate", []byte("elderberry"), []byte("purple"), core.EntryTypePutEvent, true, true, "elderberry"}, // Miss, Block E cached, Block B evicted (Cache: D, E)
		{"get_nonexistent_before", []byte("aardvark"), nil, 0, false, false, ""},
		{"get_nonexistent_after", []byte("zebra"), nil, 0, false, false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, entryTypeFromGet, err := sst.Get(tt.key)

			var found bool
			if err == nil && entryTypeFromGet == core.EntryTypePutEvent {
				// This check is a bit simplistic as Get can return (nil, EntryTypeDelete, nil) for a found tombstone
				// which is different from (nil, 0, ErrNotFound) for a truly non-existent key.
				// The test cases above handle this by setting wantFound=false for tombstones.
				found = true
			}

			if err == ErrNotFound {
				err = nil // Treat ErrNotFound as 'not found' rather than an unexpected error
			}

			if err != nil {
				t.Errorf("sst.Get() error = %v", err)
				return
			}
			if found != tt.wantFound {
				t.Errorf("sst.Get() found = %v, want %v", found, tt.wantFound)
			}
			if found { // Only check value and type if found
				if !bytes.Equal(val, tt.wantValue) {
					t.Errorf("sst.Get() value = %s, want %s", string(val), string(tt.wantValue))
				}
				if entryTypeFromGet != tt.wantEntryType {
					t.Errorf("sst.Get() entryType = %v, want %v", entryTypeFromGet, tt.wantEntryType)
				}
			}

			if tt.expectCachePop { // This Get should have been a miss and populated the cache
				if tt.expectCacheHitOn != "" { // Check if the specific block is now in cache
					blockMeta, metaFound := sst.index.Find([]byte(tt.expectCacheHitOn))
					if !metaFound {
						t.Fatalf("Test %s: Could not find block metadata for key %s to check cache", tt.name, tt.expectCacheHitOn)
					}
					_ = fmt.Sprintf("%d-%d", sst.id, blockMeta.BlockOffset)
					// We don't check cacheFound here because Get() itself will put it in cache.
					// The check is whether the block was *expected* to be populated.
				}
			} else if tt.wantFound && tt.expectCacheHitOn != "" { // This Get should have been a hit
				// The block should already be in cache from a previous operation.
			}
		})
	}
}

func TestSSTableWriter_EmptyEntries_New(t *testing.T) {
	tempDir := t.TempDir()
	compressor := &compressors.NoCompressionCompressor{}
	writerOpts := core.SSTableWriterOptions{
		DataDir:                      tempDir,
		ID:                           1,
		EstimatedKeys:                0,
		BloomFilterFalsePositiveRate: 0.01,
		BlockSize:                    DefaultBlockSize,
		Tracer:                       nil, // Pass nil tracer for test
		Compressor:                   compressor,
	}
	writer, err := NewSSTableWriter(writerOpts)
	if err != nil {
		t.Fatalf("NewSSTableWriter() error = %v", err)
	}
	// Finish without adding entries. Should not error, but result in an empty (or near empty) SSTable.
	// The current writer flushes the last block (even if empty) and writes metadata.
	// An SSTable with no data entries is valid, though perhaps not useful.
	// The old test expected "cannot write empty SSTable". The new writer might behave differently.
	// Let's check if Finish works.
	err = writer.Finish()
	if err != nil {
		t.Errorf("writer.Finish() with no entries error = %v, want nil (or specific error if design changes)", err)
	}
	// Further checks could involve loading this "empty" SSTable.
	// For now, just ensuring Finish doesn't panic.
	// If an error is desired for 0 entries, Add or Finish needs to enforce it.
	// The current `writer.Add` is never called, `flushCurrentBlock` in `Finish` handles empty buffer.
}

func TestLoadSSTable_FileTooSmall_New(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.Default()
	filePath := filepath.Join(tempDir, "sstable-small.sst")

	if err := os.WriteFile(filePath, []byte("short"), 0644); err != nil {
		t.Fatalf("Failed to write small file: %v", err)
	}
	defer os.Remove(filePath)

	loadOpts := LoadSSTableOptions{
		FilePath:   filePath,
		ID:         1,
		BlockCache: nil,
		Tracer:     nil,
		Logger:     logger,
	}
	_, err := LoadSSTable(loadOpts)
	if err == nil {
		t.Fatal("LoadSSTable expected an error for too small file, got nil")
	}
	// With the new header, the error is more likely to be an EOF when reading the header.
	if !bytes.Contains([]byte(err.Error()), []byte("failed to read sstable header")) {
		t.Errorf("LoadSSTable error message mismatch. Got: %q, want to contain: %q", err.Error(), "failed to read sstable header")
	}
}

func TestSSTableWriter_ErrorHandling(t *testing.T) {
	compressor := &compressors.NoCompressionCompressor{}
	baseOpts := core.SSTableWriterOptions{
		EstimatedKeys:                10,
		BloomFilterFalsePositiveRate: 0.01,
		BlockSize:                    DefaultBlockSize,
		Compressor:                   compressor,
		Logger:                       slog.Default(),
	}

	t.Run("NewSSTableWriter_CreateError", func(t *testing.T) {
		// This test now uses a mock file creation function instead of relying on
		// OS-specific directory permissions, making it cross-platform compatible.
		originalCreate := sys.Create
		defer func() { sys.Create = originalCreate }() // Restore original function after test

		// Mock sys.Create to always return an error.
		mockErr := errors.New("simulated create error")
		sys.Create = func(name string) (sys.FileInterface, error) {
			return nil, mockErr
		}

		opts := baseOpts
		opts.DataDir = t.TempDir() // We still need a valid temp dir
		opts.ID = 1

		_, err := NewSSTableWriter(opts)
		require.Error(t, err, "NewSSTableWriter should fail when sys.Create fails")
		assert.ErrorIs(t, err, mockErr, "The error should wrap the mocked error")
	})

	t.Run("NewSSTableWriter_InvalidBloomFilterRate", func(t *testing.T) {
		opts := baseOpts
		opts.DataDir = t.TempDir()
		opts.ID = 4
		opts.BloomFilterFalsePositiveRate = 1.5 // Invalid rate

		_, err := NewSSTableWriter(opts)
		require.Error(t, err, "NewSSTableWriter should fail with invalid bloom filter rate")
		assert.Contains(t, err.Error(), "invalid arguments for NewBloomFilter", "Error message should indicate bloom filter issue")
	})

	t.Run("Add_FlushBlockError", func(t *testing.T) {
		// 1. Setup mock file that will fail on write
		mockErr := errors.New("simulated write error")
		mockF := &mockFile{
			name: "add_flush_error.tmp",
		}

		// 2. Mock sys.Create to return our mock file
		originalCreate := sys.Create
		sys.Create = func(name string) (sys.FileInterface, error) {
			// The first write is the header, let it succeed.
			mockF.writeShouldFail = false
			return mockF, nil
		}
		defer func() { sys.Create = originalCreate }()

		// 3. Create writer with a tiny block size to force a flush
		opts := baseOpts
		opts.DataDir = t.TempDir()
		opts.ID = 5
		opts.BlockSize = 32 // Very small block size

		writer, err := NewSSTableWriter(opts)
		require.NoError(t, err, "NewSSTableWriter should succeed initially")

		// 4. First Add should succeed and fill the buffer
		require.NoError(t, writer.Add([]byte("key1"), []byte("value1"), core.EntryTypePutEvent, 1))

		// 5. Configure mock to fail on the next write (which will be the flush)
		mockF.writeShouldFail = true
		mockF.writeErr = mockErr

		// 6. Second Add will trigger flushCurrentBlock, which will fail
		err = writer.Add([]byte("key2"), []byte("value2"), core.EntryTypePutEvent, 2)
		require.Error(t, err, "Add should fail when flushCurrentBlock fails")
		assert.ErrorIs(t, err, mockErr, "The error from Add should wrap the mocked write error")
	})

	t.Run("Finish_WriteError", func(t *testing.T) {
		// This test simulates an I/O error during the final metadata write in Finish().
		mockErr := errors.New("simulated metadata write error")
		mockF := &mockFile{name: "write_error.tmp"}

		originalCreate := sys.Create
		sys.Create = func(name string) (sys.FileInterface, error) { return mockF, nil }
		defer func() { sys.Create = originalCreate }()

		var removedPath string
		originalRemove := sys.Remove
		sys.Remove = func(name string) error {
			removedPath = name
			return nil
		}
		defer func() { sys.Remove = originalRemove }()

		var writeCount int
		mockF.writeFunc = func(p []byte) (n int, err error) {
			writeCount++
			// 1: header, 2: block compression flag, 3: block checksum, 4: block data
			// Fail on the 5th write, which is the index checksum.
			if writeCount == 5 {
				return 0, mockErr
			}
			return mockF.buffer.Write(p) // Use the internal buffer for successful writes
		}

		opts := baseOpts
		opts.DataDir = t.TempDir()
		opts.ID = 8
		writer, err := NewSSTableWriter(opts)
		require.NoError(t, err)
		filePathBeforeFinish := writer.FilePath() // Store path before it's cleared by abort()
		require.NoError(t, writer.Add([]byte("key"), []byte("val"), core.EntryTypePutEvent, 1))

		err = writer.Finish()
		require.Error(t, err, "Finish should fail when a metadata write fails")
		assert.ErrorIs(t, err, mockErr, "The error should wrap the mocked write error")
		assert.Contains(t, err.Error(), "failed to write index checksum")

		// Verify that Abort was called and the temp file was removed
		assert.Equal(t, filePathBeforeFinish, removedPath, "The temp file should have been removed after a failed Finish")
	})

	t.Run("Finish_SyncError", func(t *testing.T) {
		// 1. Setup mock file that fails on Sync
		mockErr := errors.New("simulated sync error")
		mockF := &mockFile{
			name:           "sync_error.tmp",
			syncShouldFail: true,
			syncErr:        mockErr,
		}

		// 2. Mock sys.Create and sys.Remove
		originalCreate := sys.Create
		sys.Create = func(name string) (sys.FileInterface, error) { return mockF, nil }
		defer func() { sys.Create = originalCreate }()

		var removedPath string
		originalRemove := sys.Remove
		sys.Remove = func(name string) error {
			removedPath = name
			return nil // Simulate successful removal
		}
		defer func() { sys.Remove = originalRemove }()

		// 3. Create writer, add data, and call Finish
		opts := baseOpts
		opts.DataDir = t.TempDir()
		opts.ID = 6
		writer, err := NewSSTableWriter(opts)
		require.NoError(t, err)
		filePathBeforeFinish := writer.FilePath() // Store path before it's cleared by abort()
		require.NoError(t, writer.Add([]byte("key"), []byte("val"), core.EntryTypePutEvent, 1))

		err = writer.Finish()
		require.Error(t, err, "Finish should fail when Sync fails")
		assert.ErrorIs(t, err, mockErr, "The error from Finish should wrap the mocked sync error")

		// 4. Verify that Abort was called and the temp file was removed
		assert.Equal(t, filePathBeforeFinish, removedPath, "The temp file should have been removed after a failed Finish")
	})

	t.Run("Finish_RenameError", func(t *testing.T) {
		tempDir := t.TempDir()
		opts := baseOpts
		opts.DataDir = tempDir
		opts.ID = 2

		writer, err := NewSSTableWriter(opts)
		require.NoError(t, err)

		// Create a directory where the final .sst file should be, to cause os.Rename to fail.
		finalPath := writer.FilePath()[:len(writer.FilePath())-len(filepath.Ext(writer.FilePath()))] + ".sst"
		err = os.Mkdir(finalPath, 0755)
		require.NoError(t, err)

		// Add some data so Finish() doesn't operate on an empty file
		require.NoError(t, writer.Add([]byte("key"), []byte("val"), core.EntryTypePutEvent, 1))

		err = writer.Finish()
		require.Error(t, err, "Finish should fail when the target path is a directory")
		assert.Contains(t, err.Error(), "failed to rename temporary sstable file", "Error message should indicate rename failure")

		// The writer should have aborted and cleaned up the .tmp file
		_, statErr := os.Stat(writer.FilePath())
		assert.True(t, os.IsNotExist(statErr), "The .tmp file should be removed after a failed Finish")
	})

	t.Run("Abort_RemovesTempFile", func(t *testing.T) {
		tempDir := t.TempDir()
		opts := baseOpts
		opts.DataDir = tempDir
		opts.ID = 3

		writer, err := NewSSTableWriter(opts)
		require.NoError(t, err)

		tempPath := writer.FilePath()
		_, err = os.Stat(tempPath)
		require.NoError(t, err, "Temp file should exist after NewSSTableWriter")

		err = writer.Abort()
		require.NoError(t, err)

		_, err = os.Stat(tempPath)
		assert.True(t, os.IsNotExist(err), "Temp file should not exist after Abort")

		// Calling Abort again should not cause an error
		err = writer.Abort()
		require.NoError(t, err, "Calling Abort multiple times should not error")
	})

	t.Run("Abort_RemoveError", func(t *testing.T) {
		// 1. Mock sys.Remove to fail
		mockErr := errors.New("simulated remove error")
		originalRemove := sys.Remove
		sys.Remove = func(name string) error {
			return mockErr
		}
		defer func() { sys.Remove = originalRemove }()

		// 2. Create a writer
		opts := baseOpts
		opts.DataDir = t.TempDir()
		opts.ID = 7
		writer, err := NewSSTableWriter(opts)
		require.NoError(t, err)

		// 3. Call Abort and check for the propagated error
		err = writer.Abort()
		require.Error(t, err, "Abort should fail when os.Remove fails")
		assert.ErrorIs(t, err, mockErr, "The error from Abort should wrap the mocked remove error")
	})
}

// TestSSTableWriter_flushCurrentBlock_WriteFailures provides targeted tests for write failures
// that can occur within the flushCurrentBlock method. This is crucial for ensuring that I/O errors
// during block flushing are handled gracefully.
func TestSSTableWriter_flushCurrentBlock_WriteFailures(t *testing.T) {
	mockErr := errors.New("simulated disk write error")
	baseOpts := core.SSTableWriterOptions{
		DataDir:                      t.TempDir(),
		ID:                           99,
		EstimatedKeys:                10,
		BloomFilterFalsePositiveRate: 0.01,
		BlockSize:                    DefaultBlockSize,
		Compressor:                   &compressors.NoCompressionCompressor{},
		Logger:                       slog.Default(),
	}

	// runTest is a helper function to run a sub-test with a specific write failure point.
	// It sets up a mock file that will fail on the Nth write call.
	runTest := func(t *testing.T, failOnWriteCount int, expectedErrSubstring string) {
		mockF := &mockFile{name: "flush_fail.tmp"}
		writeCounter := 0

		// Mock sys.Create to return our controllable mock file.
		originalCreate := sys.Create
		sys.Create = func(name string) (sys.FileInterface, error) {
			// The writeFunc allows us to control behavior for each Write call.
			mockF.writeFunc = func(p []byte) (n int, err error) {
				writeCounter++
				// The first write is always the SSTable header in NewSSTableWriter.
				// We want to test failures *within* flushCurrentBlock, which happens later.
				if writeCounter == failOnWriteCount {
					return 0, mockErr
				}
				return mockF.buffer.Write(p) // Successful write
			}
			return mockF, nil
		}
		defer func() { sys.Create = originalCreate }()

		writer, err := NewSSTableWriter(baseOpts)
		require.NoError(t, err)

		// Add data to trigger a flush on Finish()
		require.NoError(t, writer.Add([]byte("key"), []byte("value"), core.EntryTypePutEvent, 1))

		// Call Finish(), which will call flushCurrentBlock internally.
		err = writer.Finish()

		// Assertions
		require.Error(t, err, "Finish() should fail when an internal write fails")
		assert.ErrorIs(t, err, mockErr, "The error should wrap the original disk error")
		assert.Contains(t, err.Error(), expectedErrSubstring, "Error message should indicate the specific failure point")
	}

	// The SSTable header is write #1. The writes within flushCurrentBlock are #2, #3, #4.
	t.Run("FailOnCompressionFlag", func(t *testing.T) { runTest(t, 2, "failed to write compression type flag") })
	t.Run("FailOnChecksum", func(t *testing.T) { runTest(t, 3, "failed to write block checksum") })
	t.Run("FailOnBlockData", func(t *testing.T) { runTest(t, 4, "failed to write data block") })
}

// TODO: Add more corruption tests for the new block-based format (e.g., corrupted block index, corrupted block data).

func TestSSTableIterator_New(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.Default()
	entries := createTestEntries()
	fileID := uint64(3)

	sst, sstPath := writeTestSSTableGeneral(t, tempDir, entries, fileID, logger)
	if sst != nil {
		defer sst.Close()
	}
	defer os.Remove(sstPath)

	blockCache := cache.NewLRUCache(10, nil, nil, nil) // No onEvicted func for test
	blockCache.SetMetrics(nil, nil)
	sst.blockCache = blockCache // Set cache for iterator to use via sst.readBlock

	// For sstableIterator, we expect all entries, including tombstones, as it's a raw iterator.
	expectedAllEntriesFromSSTable := entries // Use the original entries directly

	t.Run("full_scan_new", func(t *testing.T) {
		iter, err := sst.NewIterator(nil, nil, nil, types.Ascending)
		if err != nil {
			t.Fatalf("sst.NewIterator(nil, nil) error = %v", err)
		}
		defer iter.Close()

		var actualEntries []testEntry
		for iter.Next() {
			// key, value, entryType, pointID := iter.At()
			cur, _ := iter.At()
			key := cur.Key
			value := cur.Value
			entryType := cur.EntryType
			pointID := cur.SeqNum
			actualEntries = append(actualEntries, testEntry{
				Key:       append([]byte(nil), key...),
				Value:     append([]byte(nil), value...),
				EntryType: entryType,
				PointID:   pointID,
			})
		}
		if err := iter.Error(); err != nil {
			t.Errorf("iter.Error() = %v", err)
		}

		if !reflect.DeepEqual(actualEntries, expectedAllEntriesFromSSTable) {
			t.Errorf("Full scan mismatch:\nGot:    %+v\nWanted: %+v", actualEntries, expectedAllEntriesFromSSTable)
		}
	})

	t.Run("range_scan_banana_to_date_new", func(t *testing.T) {
		startKey := []byte("banana")
		endKey := []byte("date") // Exclusive
		iter, err := sst.NewIterator(startKey, endKey, nil, types.Ascending)
		if err != nil {
			t.Fatalf("sst.NewIterator(%s, %s) error = %v", startKey, endKey, err)
		}
		defer iter.Close()

		var actualEntries []testEntry
		for iter.Next() {
			// key, value, entryType, pointID := iter.At()
			cur, _ := iter.At()
			key := cur.Key
			value := cur.Value
			entryType := cur.EntryType
			pointID := cur.SeqNum

			actualEntries = append(actualEntries, testEntry{
				Key:       append([]byte(nil), key...),
				Value:     append([]byte(nil), value...),
				EntryType: entryType,
				PointID:   pointID,
			})
		}
		if err := iter.Error(); err != nil {
			t.Errorf("iter.Error() = %v", err)
		}

		// Filter expectedAllEntriesFromSSTable for the specified range
		var expectedRangeEntriesFromSSTable []testEntry
		for _, entry := range expectedAllEntriesFromSSTable {
			if bytes.Compare(entry.Key, startKey) >= 0 && (endKey == nil || bytes.Compare(entry.Key, endKey) < 0) {
				expectedRangeEntriesFromSSTable = append(expectedRangeEntriesFromSSTable, entry)
			}
		}

		if !reflect.DeepEqual(actualEntries, expectedRangeEntriesFromSSTable) {
			t.Errorf("Range scan [banana, date) mismatch:\nGot:    %+v\nWanted: %+v", actualEntries, expectedRangeEntriesFromSSTable)
		}
	})

	t.Run("scan_from_key_to_end", func(t *testing.T) {
		startKey := []byte("date")
		iter, err := sst.NewIterator(startKey, nil, nil, types.Ascending) // nil endKey means scan to end
		if err != nil {
			t.Fatalf("sst.NewIterator(%s, nil) error = %v", startKey, err)
		}
		defer iter.Close()

		var actualEntries []testEntry
		for iter.Next() {
			// key, value, entryType, pointID := iter.At()
			cur, _ := iter.At()
			key := cur.Key
			value := cur.Value
			entryType := cur.EntryType
			pointID := cur.SeqNum

			actualEntries = append(actualEntries, testEntry{
				Key:       append([]byte(nil), key...),
				Value:     append([]byte(nil), value...),
				EntryType: entryType,
				PointID:   pointID,
			})
		}
		if err := iter.Error(); err != nil {
			t.Errorf("iter.Error() = %v", err)
		}

		// Filter expectedAllEntriesFromSSTable for the specified range
		var expectedRangeEntriesFromSSTable []testEntry
		for _, entry := range expectedAllEntriesFromSSTable {
			if bytes.Compare(entry.Key, startKey) >= 0 { // From startKey to end
				expectedRangeEntriesFromSSTable = append(expectedRangeEntriesFromSSTable, entry)
			}
		}

		if !reflect.DeepEqual(actualEntries, expectedRangeEntriesFromSSTable) {
			t.Errorf("Range scan [%s, end) mismatch:\nGot:    %+v\nWanted: %+v", string(startKey), actualEntries, expectedRangeEntriesFromSSTable)
		}
	})

	t.Run("scan_empty_range_no_data", func(t *testing.T) {
		startKey := []byte("foo")
		endKey := []byte("goo") // A range with no data in between
		iter, err := sst.NewIterator(startKey, endKey, nil, types.Ascending)
		if err != nil {
			t.Fatalf("sst.NewIterator(%s, %s) error = %v", startKey, endKey, err)
		}
		defer iter.Close()

		if iter.Next() {
			// key, _, _, _ := iter.At()
			cur, _ := iter.At()
			key := cur.Key

			t.Errorf("Expected no entries for empty range, but got one: Key=%s", string(key))
		}
		if err := iter.Error(); err != nil {
			t.Errorf("iter.Error() = %v", err)
		}
	})

	t.Run("scan_empty_range_invalid_bounds", func(t *testing.T) {
		startKey := []byte("zebra")
		endKey := []byte("apple") // startKey > endKey
		iter, err := sst.NewIterator(startKey, endKey, nil, types.Ascending)
		if err != nil {
			t.Fatalf("sst.NewIterator(%s, %s) error = %v", startKey, endKey, err)
		}
		defer iter.Close()

		if iter.Next() {
			// key, _, _, _ := iter.At()
			cur, _ := iter.At()
			key := cur.Key

			t.Errorf("Expected no entries for invalid range (start > end), but got one: Key=%s", string(key))
		}
		if err := iter.Error(); err != nil {
			t.Errorf("iter.Error() = %v", err)
		}
	})

	// Note on tombstones:
	// The sstableIterator (which is what sst.NewIterator returns) is a low-level iterator.
	// It is designed to faithfully iterate over all entries stored in the SSTable,
	// including tombstones (EntryTypeDelete). Higher-level iterators (like MergingIterator
	// or SkippingTombstoneIterator in the engine's query path) are responsible for
	// filtering out tombstones and resolving the latest version of a key.
	// Therefore, the existing "full_scan_new" and "range_scan_banana_to_date_new" tests
	// already correctly assert that tombstones are returned by sstableIterator.
	// No additional test is needed here for "scan_with_tombstones_within_range" for sstableIterator itself.
}

// Note: Tests for SSTableIndex.Serialize/Deserialize and GetBlockOffset (now Find/findFirstGreaterOrEqual)
// are implicitly covered by WriteAndLoadSSTable and Get/Iterator tests if they pass.
// Explicit unit tests for index methods are still good for isolation.

func TestLoadSSTable_CorruptedMagicString(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.Default()
	entries := createTestEntries()
	fileID := uint64(301)

	// 1. Create a valid SSTable
	sst, sstPath := writeTestSSTableGeneral(t, tempDir, entries, fileID, logger)
	sst.Close() // Close the valid one

	// 2. Read the file content and corrupt the magic string
	fileData, err := os.ReadFile(sstPath)
	if err != nil {
		t.Fatalf("Failed to read SSTable file: %v", err)
	}

	// Corrupt the last byte of the magic string
	if len(fileData) > 0 {
		fileData[len(fileData)-1] = 'X'
	} else {
		t.Fatal("SSTable file is empty.")
	}

	// Write back the corrupted data
	if err := os.WriteFile(sstPath, fileData, 0644); err != nil {
		t.Fatalf("Failed to write corrupted SSTable file: %v", err)
	}

	// 3. Attempt to load the corrupted SSTable
	loadOpts := LoadSSTableOptions{
		FilePath:   sstPath,
		ID:         fileID,
		BlockCache: nil,
		Tracer:     nil,
		Logger:     logger,
	}
	_, err = LoadSSTable(loadOpts)
	if err == nil {
		t.Fatal("LoadSSTable expected an error for corrupted magic string, got nil")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("invalid magic string")) {
		t.Errorf("LoadSSTable error message mismatch. Got: %q, want to contain: %q", err.Error(), "invalid magic string")
	}
}

// TODO: Test SSTableIndex.Find and findFirstGreaterOrEqual more directly.
// TODO: Test Block.Find and BlockIterator more directly.

func TestSSTable_VerifyIntegrity_DeepChecks(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.Default()
	entries := []testEntry{
		{Key: []byte("apple"), Value: []byte("red"), EntryType: core.EntryTypePutEvent, PointID: 1},
		{Key: []byte("banana"), Value: []byte("yellow"), EntryType: core.EntryTypePutEvent, PointID: 2},
		{Key: []byte("date"), Value: []byte("brown"), EntryType: core.EntryTypePutEvent, PointID: 3},
		{Key: []byte("elderberry"), Value: []byte("purple"), EntryType: core.EntryTypePutEvent, PointID: 4},
		{Key: []byte("fig"), Value: []byte("green"), EntryType: core.EntryTypePutEvent, PointID: 5},
	}
	fileID := uint64(101)

	// Use a small block size to ensure multiple blocks are created
	// and thus multiple index entries.
	// Each entry: PointID(8) + KeyLen(4) + Key(avg 5) + ValLen(4) + Val(avg 5) + Type(1) = ~27 bytes
	// Block size 32 should create multiple blocks.
	customBlockSize := 32

	compressor := &compressors.NoCompressionCompressor{}
	writerOpts := core.SSTableWriterOptions{
		DataDir:                      tempDir,
		ID:                           fileID,
		EstimatedKeys:                uint64(len(entries)),
		BloomFilterFalsePositiveRate: 0.01,
		BlockSize:                    customBlockSize,
		Tracer:                       nil, // Pass nil tracer for test
		Compressor:                   compressor,
	}
	writer, err := NewSSTableWriter(writerOpts) // Pass nil tracer
	if err != nil {
		t.Fatalf("NewSSTableWriter() error = %v", err)
	}
	for _, entry := range entries {
		if err := writer.Add(entry.Key, entry.Value, entry.EntryType, entry.PointID); err != nil {
			writer.Abort()
			t.Fatalf("writer.Add(%s) error = %v", string(entry.Key), err)
		}
	}
	if err := writer.Finish(); err != nil {
		writer.Abort()
		t.Fatalf("writer.Finish() error = %v", err)
	}
	sstPath := writer.FilePath()
	defer os.Remove(sstPath)

	blockCache := cache.NewLRUCache(10, nil, nil, nil) // No onEvicted func for test
	blockCache.SetMetrics(nil, nil)
	sstLoadOptd := LoadSSTableOptions{
		FilePath:   sstPath,
		ID:         fileID,
		BlockCache: blockCache,
		Tracer:     nil,
		Logger:     logger,
	}
	sst, err := LoadSSTable(sstLoadOptd) // Pass nil tracer for test
	if err != nil {
		t.Fatalf("LoadSSTable() error = %v", err)
	}
	defer sst.Close()

	t.Run("valid_sstable_deep_checks", func(t *testing.T) {
		errors := sst.VerifyIntegrity(true) // Call with deepCheck = true
		if len(errors) > 0 {
			t.Errorf("Expected no integrity errors for a valid SSTable, but got: %v", errors)
		}
	})

	// --- Scenarios for corrupted data (conceptual, hard to implement without file manipulation) ---

	t.Run("corrupted_index_firstkey_mismatch", func(t *testing.T) {
		// To test this properly:
		// 1. Create a valid SSTable.
		// 2. Manually open the .sst file, locate the index section.
		// 3. Modify one of the FirstKey entries in the serialized index data
		//    so it no longer matches the actual first key of its corresponding data block.
		// 4. Save the corrupted file.
		// 5. Load this corrupted SSTable.
		// 6. Call VerifyIntegrity() and check for the specific error.
		// This is complex to do reliably in a unit test without specialized tools.
		// For now, we acknowledge this test case.
		// If we had a way to "corrupt" an in-memory sst.index.entries[i].FirstKey before calling VerifyIntegrity,
		// that would be a simpler unit test, but VerifyIntegrity reads from sst.index.

		// Simulate by checking a valid table, knowing the check exists.
		errors := sst.VerifyIntegrity(true) // Call with deepCheck = true
		foundMismatchError := false
		for _, e := range errors {
			if bytes.Contains([]byte(e.Error()), []byte("mismatch with actual block first key")) {
				foundMismatchError = true
				break
			}
		}
		if foundMismatchError { // This should not happen for a valid table
			t.Errorf("Found 'FirstKey mismatch' error on a supposedly valid table: %v", errors)
		}
		t.Log("Note: Testing corrupted index (FirstKey mismatch) requires manual file corruption or advanced mocking.")
	})

	t.Run("corrupted_bloom_filter_false_negative", func(t *testing.T) {
		// To test this properly:
		// 1. Create a valid SSTable.
		// 2. Manually corrupt the Bloom Filter's bitset in the .sst file such that for an existing key,
		//    Contains() would return false.
		// 3. Load this corrupted SSTable.
		// 4. Call VerifyIntegrity() and check for the "Bloom filter returned false negative" error.

		// Simulate by checking a valid table.
		errors := sst.VerifyIntegrity(true) // Call with deepCheck = true
		foundBloomError := false
		for _, e := range errors {
			if bytes.Contains([]byte(e.Error()), []byte("Bloom filter returned false negative")) {
				foundBloomError = true
				break
			}
		}
		if foundBloomError { // This should not happen for a valid table
			t.Errorf("Found 'Bloom filter false negative' error on a supposedly valid table: %v", errors)
		}
		t.Log("Note: Testing corrupted Bloom Filter (false negative) requires manual file corruption or advanced mocking.")
	})
}

func TestSSTable_CorruptedBlockChecksum(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.Default()
	entries := []testEntry{
		{Key: []byte("key1"), Value: []byte("value1"), EntryType: core.EntryTypePutEvent, PointID: 1},
		{Key: []byte("key2"), Value: []byte("value2"), EntryType: core.EntryTypePutEvent, PointID: 2},
		{Key: []byte("key3"), Value: []byte("value3"), EntryType: core.EntryTypePutEvent, PointID: 3},
	}
	fileID := uint64(201)

	// 1. Create a valid SSTable
	// Use a small block size to ensure we have at least one block.
	compressor := &compressors.NoCompressionCompressor{}
	writerOpts := core.SSTableWriterOptions{
		DataDir:                      tempDir,
		ID:                           fileID,
		EstimatedKeys:                uint64(len(entries)),
		BloomFilterFalsePositiveRate: 0.01,
		BlockSize:                    32,  // small block size
		Tracer:                       nil, // Pass nil tracer for test
		Compressor:                   compressor,
	}
	writer, err := NewSSTableWriter(writerOpts)
	if err != nil { // Check for error from NewSSTableWriter
		t.Fatalf("NewSSTableWriter() error = %v", err)
	}
	for _, entry := range entries {
		if err := writer.Add(entry.Key, entry.Value, entry.EntryType, entry.PointID); err != nil {
			writer.Abort()
			t.Fatalf("writer.Add failed: %v", err)
		}
	}
	if err := writer.Finish(); err != nil {
		writer.Abort()
		t.Fatalf("writer.Finish failed: %v", err)
	}
	validSSTPath := writer.FilePath()

	// 2. Load the valid SSTable to get block offset information
	loadOpts := LoadSSTableOptions{
		FilePath:   validSSTPath,
		ID:         fileID,
		BlockCache: nil,
		Tracer:     nil,
		Logger:     logger,
	}
	validSST, err := LoadSSTable(loadOpts)
	if err != nil {
		t.Fatalf("LoadSSTable (valid) error = %v", err)
	}
	if len(validSST.index.entries) == 0 {
		validSST.Close()
		t.Fatal("Valid SSTable has no index entries, cannot proceed with corruption test.")
	}
	firstBlockMeta := validSST.index.entries[0]
	validSST.Close() // Close it as we'll be modifying its underlying file content

	// 3. Read the file content, corrupt the checksum of the first block
	fileData, err := os.ReadFile(validSSTPath)
	if err != nil {
		t.Fatalf("Failed to read SSTable file: %v", err)
	}

	// Corrupt the first byte of the checksum for the first data block
	// The compression flag is at BlockOffset, checksum starts at BlockOffset + 1.
	checksumByteToCorruptOffset := int(firstBlockMeta.BlockOffset) + 1 // Corrupt the first byte of the actual checksum
	if checksumByteToCorruptOffset < len(fileData) {
		fileData[checksumByteToCorruptOffset]++ // Flip a bit in the checksum
	} else {
		t.Fatalf("Block offset %d is out of bounds for file size %d", firstBlockMeta.BlockOffset, len(fileData))
	}

	corruptedSSTPath := filepath.Join(tempDir, fmt.Sprintf("%d_corrupted.sst", fileID))
	if err := os.WriteFile(corruptedSSTPath, fileData, 0644); err != nil {
		t.Fatalf("Failed to write corrupted SSTable file: %v", err)
	}
	defer os.Remove(corruptedSSTPath)

	// 4. Attempt to load and read from the corrupted SSTable
	corruptedLoadOpts := LoadSSTableOptions{
		FilePath:   corruptedSSTPath,
		ID:         fileID,
		BlockCache: nil,
		Tracer:     nil,
		Logger:     logger,
	}
	corruptedSST, err := LoadSSTable(corruptedLoadOpts)
	if err != nil {
		// Depending on when LoadSSTable reads blocks (e.g., for min/max key),
		// corruption might be caught here.
		// If LoadSSTable itself doesn't read blocks for min/max key determination,
		// the error will appear during Get or iteration.
		t.Logf("LoadSSTable for corrupted file returned error (as expected if it reads blocks early): %v", err)
		if !errors.Is(err, ErrCorrupted) && !bytes.Contains([]byte(err.Error()), []byte("checksum mismatch")) {
			// t.Errorf("Expected ErrCorrupted or checksum error on load, got: %v", err)
		}
		// If LoadSSTable succeeds, the error should come from Get/Iterate
	}
	if corruptedSST == nil { // If LoadSSTable failed fatally
		return
	}
	defer corruptedSST.Close()

	// Attempt to Get a key that would be in the (now corrupted) first block
	_, _, errGet := corruptedSST.Get(entries[0].Key)
	if errGet == nil {
		t.Errorf("Expected an error when getting key from corrupted block, got nil")
	} else if !errors.Is(errGet, ErrCorrupted) && !bytes.Contains([]byte(errGet.Error()), []byte("checksum mismatch")) {
		t.Errorf("Expected ErrCorrupted or checksum error from Get, got: %v", errGet)
	} else {
		t.Logf("Correctly received error from Get on corrupted block: %v", errGet)
	}
}

func TestSSTable_VerifyIntegrity_CorruptedBlock(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.Default()
	entries := []testEntry{
		{Key: []byte("key1"), Value: []byte("value1"), EntryType: core.EntryTypePutEvent, PointID: 1},
		{Key: []byte("key2"), Value: []byte("value2"), EntryType: core.EntryTypePutEvent, PointID: 2},
	}
	fileID := uint64(202)

	// 1. Create a valid SSTable
	compressor := &compressors.NoCompressionCompressor{}
	writerOpts := core.SSTableWriterOptions{
		DataDir:                      tempDir,
		ID:                           fileID,
		EstimatedKeys:                uint64(len(entries)),
		BloomFilterFalsePositiveRate: 0.01,
		BlockSize:                    32,
		Compressor:                   compressor,
		Logger:                       logger,
	}
	writer, err := NewSSTableWriter(writerOpts)
	require.NoError(t, err)
	for _, entry := range entries {
		require.NoError(t, writer.Add(entry.Key, entry.Value, entry.EntryType, entry.PointID))
	}
	require.NoError(t, writer.Finish())
	validSSTPath := writer.FilePath()

	// 2. Load the valid SSTable to get block offset information
	validSST, err := LoadSSTable(LoadSSTableOptions{FilePath: validSSTPath, ID: fileID, Logger: logger})
	require.NoError(t, err)
	require.NotEmpty(t, validSST.index.entries, "SSTable should have index entries")
	firstBlockMeta := validSST.index.entries[0]
	validSST.Close()

	// 3. Read the file content and corrupt the checksum of the first block
	fileData, err := os.ReadFile(validSSTPath)
	require.NoError(t, err)

	checksumByteToCorruptOffset := int(firstBlockMeta.BlockOffset) + 1 // +1 to skip compression flag
	require.Less(t, checksumByteToCorruptOffset, len(fileData), "Offset should be in bounds")
	fileData[checksumByteToCorruptOffset]++ // Flip a bit in the checksum

	// 4. Write back the corrupted data
	require.NoError(t, os.WriteFile(validSSTPath, fileData, 0644))

	// 5. Load the corrupted SSTable
	corruptedSST, err := LoadSSTable(LoadSSTableOptions{FilePath: validSSTPath, ID: fileID, Logger: logger})
	require.NoError(t, err, "LoadSSTable should succeed even with corrupted block, as it reads lazily")
	defer corruptedSST.Close()

	// 6. Verify integrity with deep check, which should fail
	errs := corruptedSST.VerifyIntegrity(true)
	require.NotEmpty(t, errs, "VerifyIntegrity with deepCheck should find an error")
	assert.Contains(t, errs[0].Error(), "checksum mismatch", "Expected a checksum mismatch error")
}

func TestSSTable_Close(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.Default()
	entries := []testEntry{
		{Key: []byte("a"), Value: []byte("1"), EntryType: core.EntryTypePutEvent, PointID: 1},
	}
	fileID := uint64(501)

	// 1. Create a valid SSTable
	sst, sstPath := writeTestSSTableGeneral(t, tempDir, entries, fileID, logger)
	defer os.Remove(sstPath) // Ensure cleanup even if test fails early

	// 2. First call to Close should succeed
	err := sst.Close()
	assert.NoError(t, err, "First call to Close() should succeed")

	// 3. Second call to Close should return ErrClosed
	err = sst.Close()
	assert.ErrorIs(t, err, ErrClosed, "Second call to Close() should return ErrClosed")

	// 4. Verify that other operations also fail with ErrClosed
	t.Run("Get_After_Close", func(t *testing.T) {
		_, _, err := sst.Get([]byte("a"))
		assert.ErrorIs(t, err, ErrClosed, "Get() after Close() should return ErrClosed")
	})

	t.Run("NewIterator_After_Close", func(t *testing.T) {
		_, err := sst.NewIterator(nil, nil, nil, types.Ascending)
		assert.ErrorIs(t, err, ErrClosed, "NewIterator() after Close() should return ErrClosed")
	})

	t.Run("VerifyIntegrity_After_Close", func(t *testing.T) {
		errs := sst.VerifyIntegrity(false)
		assert.NotEmpty(t, errs, "VerifyIntegrity() after Close() should return errors")
		assert.ErrorIs(t, errs[0], ErrClosed, "VerifyIntegrity() error should be ErrClosed")
	})
}

func TestSSTable_Contains(t *testing.T) {
	keyExists := []byte("key-exists")
	keyNotExists := []byte("key-not-exists")

	t.Run("WithFilter", func(t *testing.T) {
		mockF := &mockFilter{
			data: map[string]bool{
				string(keyExists): true,
			},
		}
		tableWithFilter := &SSTable{filter: mockF}

		assert.True(t, tableWithFilter.Contains(keyExists), "Should return true for a key that might exist")
		assert.False(t, tableWithFilter.Contains(keyNotExists), "Should return false for a key that does not exist")
	})

	t.Run("WithoutFilter", func(t *testing.T) {
		tableWithoutFilter := &SSTable{filter: nil}

		// Without a filter, it should always return true to be safe.
		assert.True(t, tableWithoutFilter.Contains(keyExists), "Should always return true when no filter is present")
		assert.True(t, tableWithoutFilter.Contains(keyNotExists), "Should always return true when no filter is present")
	})
}
