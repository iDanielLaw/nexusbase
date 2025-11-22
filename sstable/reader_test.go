package sstable

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/INLOpen/nexusbase/sys"

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper to create a valid SSTable and then allow a corruption function to modify its raw bytes.
func createAndCorruptSSTable(t *testing.T, corruptionFunc func(data []byte) []byte) string {
	t.Helper()
	tempDir := t.TempDir()
	entries := []testEntry{
		{Key: []byte("key01"), Value: []byte("value01"), EntryType: core.EntryTypePutEvent, PointID: 1},
		{Key: []byte("key02"), Value: []byte("value02"), EntryType: core.EntryTypePutEvent, PointID: 2},
	}

	// 1. Create a valid SSTable
	writerOpts := core.SSTableWriterOptions{
		DataDir:                      tempDir,
		ID:                           1,
		BloomFilterFalsePositiveRate: 0.01,
		Compressor:                   &compressors.NoCompressionCompressor{},
		Logger:                       slog.Default(),
	}
	writer, err := NewSSTableWriter(writerOpts)
	require.NoError(t, err)
	for _, entry := range entries {
		require.NoError(t, writer.Add(entry.Key, entry.Value, entry.EntryType, entry.PointID))
	}
	require.NoError(t, writer.Finish())
	validPath := writer.FilePath()

	// 2. Read the valid data
	validData, err := os.ReadFile(validPath)
	require.NoError(t, err)

	// 3. Apply corruption
	corruptedData := corruptionFunc(validData)

	// 4. Write corrupted data to a new file
	corruptedPath := filepath.Join(tempDir, "corrupted.sst")
	err = os.WriteFile(corruptedPath, corruptedData, 0644)
	require.NoError(t, err)

	return corruptedPath
}

func TestLoadSSTable_ErrorPaths(t *testing.T) {
	t.Run("CorruptedMagicNumber", func(t *testing.T) {
		corruptedPath := createAndCorruptSSTable(t, func(data []byte) []byte {
			// The FileHeader struct layout is: Magic (4 bytes), ...
			// Corrupt the magic number.
			if len(data) > 4 {
				copy(data[0:4], []byte("BAD!"))
			}
			return data
		})

		_, err := LoadSSTable(LoadSSTableOptions{FilePath: corruptedPath, ID: 1, Logger: slog.Default()})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid sstable magic number", "Error should indicate an invalid magic number")
	})

	t.Run("CorruptedBloomFilterDeserialization", func(t *testing.T) {
		corruptedPath := createAndCorruptSSTable(t, func(data []byte) []byte {
			// Find bloom filter offset and length from footer
			footerReader := bytes.NewReader(data[len(data)-FooterSize:])
			var indexOffset, bloomFilterOffset uint64
			var indexLen, bloomFilterLen uint32
			binary.Read(footerReader, binary.LittleEndian, &indexOffset)
			binary.Read(footerReader, binary.LittleEndian, &indexLen)
			binary.Read(footerReader, binary.LittleEndian, &bloomFilterOffset)
			binary.Read(footerReader, binary.LittleEndian, &bloomFilterLen)

			// Corrupt the bloom filter data itself by flipping some bits.
			// This should cause the size check inside DeserializeBloomFilter to fail.
			// For example, we can corrupt the numBits metadata inside the bloom filter data.
			if bloomFilterLen > 1 {
				data[bloomFilterOffset+1]++ // Corrupt the serialized numBits
			}
			return data
		})

		_, err := LoadSSTable(LoadSSTableOptions{FilePath: corruptedPath, ID: 1, Logger: slog.Default()})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to deserialize filter", "Error should indicate bloom filter deserialization failure")
	})

	t.Run("InvalidHeaderVersion", func(t *testing.T) {
		corruptedPath := createAndCorruptSSTable(t, func(data []byte) []byte {
			// The FileHeader struct layout is: Magic (4 bytes), Version (1 byte), ...
			// Therefore, the version is at offset 4.
			if len(data) > 4 {
				data[4] = 255 // Corrupt the version byte to an invalid value.
			}
			return data
		})

		_, err := LoadSSTable(LoadSSTableOptions{FilePath: corruptedPath, ID: 1, Logger: slog.Default()})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported sstable version", "Error should indicate an unsupported version")
	})

	t.Run("FooterReadError", func(t *testing.T) {
		// This is simulated by truncating the file so the footer read fails.
		// This is already covered by TestLoadSSTable_TruncatedFile, but we can have a specific one.
		corruptedPath := createAndCorruptSSTable(t, func(data []byte) []byte {
			// Truncate the file to be smaller than the footer itself, ensuring the size check fails.
			return data[:FooterSize-1]
		})

		_, err := LoadSSTable(LoadSSTableOptions{FilePath: corruptedPath, ID: 1, Logger: slog.Default()})
		require.Error(t, err, "LoadSSTable should fail on a truncated file")
		// The file is large enough for the header, but not for the footer, so the size check should fail.
		assert.Contains(t, err.Error(), "is too small to be valid", "Error should be about file size validity")
	})
}

func TestLoadSSTable_CorruptedIndexChecksum(t *testing.T) {
	corruptedPath := createAndCorruptSSTable(t, func(data []byte) []byte {
		// 1. Find index offset from footer
		footerReader := bytes.NewReader(data[len(data)-FooterSize:])
		var indexOffset uint64
		err := binary.Read(footerReader, binary.LittleEndian, &indexOffset)
		require.NoError(t, err)

		// 2. Find checksum location (right before the index data)
		checksumOffset := int64(indexOffset) - int64(core.ChecksumSize)
		require.GreaterOrEqual(t, checksumOffset, int64(0), "Checksum offset should be non-negative")
		require.Less(t, checksumOffset, int64(len(data)), "Checksum offset should be within file bounds")

		// 3. Corrupt the checksum by incrementing the first byte
		data[checksumOffset]++

		return data
	})

	_, err := LoadSSTable(LoadSSTableOptions{FilePath: corruptedPath, ID: 1, Logger: slog.Default()})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCorrupted, "Error should be ErrCorrupted for checksum mismatch")
	assert.Contains(t, err.Error(), "index checksum mismatch", "Error message should indicate index checksum mismatch")
}

func TestSSTable_readBlock_DecompressionError(t *testing.T) {
	tempDir := t.TempDir()
	entries := []testEntry{
		{Key: []byte("key1"), Value: []byte("value1"), EntryType: core.EntryTypePutEvent, PointID: 1},
	}
	fileID := uint64(99)

	// 1. Create a valid SSTable with Snappy compression
	writerOpts := core.SSTableWriterOptions{
		DataDir:                      tempDir,
		ID:                           fileID,
		BloomFilterFalsePositiveRate: 0.01,
		Compressor:                   &compressors.SnappyCompressor{},
		Logger:                       slog.Default(),
	}
	writer, err := NewSSTableWriter(writerOpts)
	require.NoError(t, err)
	require.NoError(t, writer.Add(entries[0].Key, entries[0].Value, entries[0].EntryType, entries[0].PointID))
	require.NoError(t, writer.Finish())
	validPath := writer.FilePath()

	// 2. Load it to get block metadata
	sst, err := LoadSSTable(LoadSSTableOptions{FilePath: validPath, ID: fileID, Logger: slog.Default()})
	require.NoError(t, err)
	blockMeta := sst.index.GetEntries()[0]
	sst.Close()

	// 3. Corrupt the compressed data but fix the checksum
	fileData, err := os.ReadFile(validPath)
	require.NoError(t, err)

	// Block layout: [compression_flag (1)] [checksum (4)] [compressed_data (N)]
	dataStartOffset := int(blockMeta.BlockOffset) + 1 + 4
	dataEndOffset := int(blockMeta.BlockOffset) + int(blockMeta.BlockLength)
	compressedData := fileData[dataStartOffset:dataEndOffset]

	// Corrupt the compressed data
	if len(compressedData) > 0 {
		compressedData[0]++ // Flip a bit
	}

	// Recalculate checksum for the corrupted data
	newChecksum := crc32.ChecksumIEEE(compressedData)
	binary.LittleEndian.PutUint32(fileData[int(blockMeta.BlockOffset)+1:dataStartOffset], newChecksum)

	// 4. Write back and try to read
	require.NoError(t, os.WriteFile(validPath, fileData, 0644))

	corruptedSST, err := LoadSSTable(LoadSSTableOptions{FilePath: validPath, ID: fileID, Logger: slog.Default()})
	require.NoError(t, err) // Load should succeed
	defer corruptedSST.Close()

	_, _, err = corruptedSST.Get([]byte("key1"))
	require.Error(t, err, "Get should fail due to decompression error")
	assert.Contains(t, err.Error(), "failed to decompress block", "Error message should indicate decompression failure")
	assert.Contains(t, err.Error(), "corrupt input", "Error message should indicate corrupt snappy input")
}

func TestSSTable_readBlock_ErrorPaths(t *testing.T) {
	t.Run("BlockLengthTooSmall", func(t *testing.T) {
		// Create a valid SSTable. Add BloomFilterFalsePositiveRate to prevent error in NewSSTableWriter.
		tempDir := t.TempDir()
		writer, err := NewSSTableWriter(core.SSTableWriterOptions{DataDir: tempDir, ID: 1, Compressor: &compressors.NoCompressionCompressor{}, Logger: slog.Default(), BloomFilterFalsePositiveRate: 0.01})
		require.NoError(t, err)
		require.NoError(t, writer.Add([]byte("key"), []byte("val"), core.EntryTypePutEvent, 1))
		require.NoError(t, writer.Finish())
		sst, err := LoadSSTable(LoadSSTableOptions{FilePath: writer.FilePath(), ID: 1, Logger: slog.Default()})
		require.NoError(t, err)
		defer sst.Close()

		blockMeta := sst.index.GetEntries()[0]

		// Attempt to read with an invalid length
		_, err = sst.readBlock(blockMeta.BlockOffset, 2, nil) // Length 2 is < header size
		require.Error(t, err)
		assert.Contains(t, err.Error(), "block length 2 is too small")
	})

	t.Run("UnknownCompressionType", func(t *testing.T) {
		corruptedPath := createAndCorruptSSTable(t, func(data []byte) []byte {
			// Find the first block's offset
			footerReader := bytes.NewReader(data[len(data)-FooterSize:])
			var indexOffset uint64
			binary.Read(footerReader, binary.LittleEndian, &indexOffset)
			indexDataBytes := data[indexOffset:]
			var keyLen uint32
			binary.Read(bytes.NewReader(indexDataBytes), binary.LittleEndian, &keyLen)
			var blockOffset int64
			binary.Read(bytes.NewReader(indexDataBytes[4+keyLen:]), binary.LittleEndian, &blockOffset)

			// Corrupt the compression flag byte at the start of the block
			data[blockOffset] = 99 // An unknown compression type
			return data
		})

		sst, err := LoadSSTable(LoadSSTableOptions{FilePath: corruptedPath, ID: 1, Logger: slog.Default()})
		require.NoError(t, err) // Load succeeds, error is on read
		defer sst.Close()

		// Attempt to read the corrupted block
		_, _, err = sst.Get([]byte("key01"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown compression type: 99")
	})
}

// getBaseSSTForIntegrityTest creates a new base SSTable instance for integrity tests.
func getBaseSSTForIntegrityTest() *SSTable {
	return &SSTable{
		minKey: []byte("b"),
		maxKey: []byte("d"),
		index: &Index{
			entries: []BlockIndexEntry{
				{FirstKey: []byte("b"), BlockOffset: 100, BlockLength: 50},
				{FirstKey: []byte("d"), BlockOffset: 150, BlockLength: 50},
			},
		},
		logger: slog.Default(),
	}
}

func TestSSTable_VerifyIntegrity(t *testing.T) {
	t.Run("MinKeyGreaterThanMaxKey", func(t *testing.T) {
		sst := getBaseSSTForIntegrityTest()
		sst.minKey = []byte("e") // "e" > "d"
		sst.maxKey = []byte("d")

		errs := sst.VerifyIntegrity(false)
		require.NotEmpty(t, errs, "Expected at least one integrity error")

		// Check that the specific error we are testing for is present.
		var foundExpectedError bool
		for _, e := range errs {
			if strings.Contains(e.Error(), "MinKey e > MaxKey d") {
				foundExpectedError = true
				break
			}
		}
		assert.True(t, foundExpectedError, "Expected to find 'MinKey > MaxKey' error in the list of errors")
	})

	t.Run("UnsortedIndex", func(t *testing.T) {
		// Create a fresh instance for the test to avoid copying the lock.
		sst := getBaseSSTForIntegrityTest()
		// Create a new index with unsorted entries
		sst.index = &Index{
			entries: []BlockIndexEntry{
				{FirstKey: []byte("d"), BlockOffset: 150, BlockLength: 50},
				{FirstKey: []byte("b"), BlockOffset: 100, BlockLength: 50},
			},
		}

		errs := sst.VerifyIntegrity(false)
		require.NotEmpty(t, errs, "Expected at least one integrity error")

		// Check that the specific error we are testing for is present.
		var foundExpectedError bool
		for _, e := range errs {
			if strings.Contains(e.Error(), "Index not sorted correctly") {
				foundExpectedError = true
				break
			}
		}
		assert.True(t, foundExpectedError, "Expected to find 'Index not sorted correctly' error in the list of errors")
	})

	t.Run("MinKeyMismatchWithIndex", func(t *testing.T) {
		sst := getBaseSSTForIntegrityTest()
		sst.minKey = []byte("a") // "a" is not equal to the first index key "b"

		errs := sst.VerifyIntegrity(false)
		require.NotEmpty(t, errs, "Expected at least one integrity error")

		var foundExpectedError bool
		for _, e := range errs {
			if strings.Contains(e.Error(), "does not match first key in index") {
				foundExpectedError = true
				break
			}
		}
		assert.True(t, foundExpectedError, "Expected to find 'MinKey does not match first key in index' error")
	})
}

func TestSSTable_VerifyIntegrity_BloomFilterFalseNegative(t *testing.T) {
	// 1. Create a valid SSTable with some keys
	tempDir := t.TempDir()
	entries := []testEntry{
		{Key: []byte("key-exists"), Value: []byte("v1"), EntryType: core.EntryTypePutEvent, PointID: 1},
	}
	writerOpts := core.SSTableWriterOptions{
		DataDir:                      tempDir,
		ID:                           1,
		BloomFilterFalsePositiveRate: 0.01,
		Compressor:                   &compressors.NoCompressionCompressor{},
		Logger:                       slog.Default(),
	}
	writer, err := NewSSTableWriter(writerOpts)
	require.NoError(t, err)
	require.NoError(t, writer.Add(entries[0].Key, entries[0].Value, entries[0].EntryType, entries[0].PointID))
	require.NoError(t, writer.Finish())
	validPath := writer.FilePath()
	defer sys.Remove(validPath)

	// 2. Read the valid data, corrupt it in memory, and write it back.
	validData, err := os.ReadFile(validPath)
	require.NoError(t, err)

	// Corrupt the bloom filter by zeroing out its bit array.
	footerReader := bytes.NewReader(validData[len(validData)-FooterSize:])
	var indexOffset, bloomFilterOffset uint64
	var indexLen, bloomFilterLen uint32
	binary.Read(footerReader, binary.LittleEndian, &indexOffset)
	binary.Read(footerReader, binary.LittleEndian, &indexLen)
	binary.Read(footerReader, binary.LittleEndian, &bloomFilterOffset)
	binary.Read(footerReader, binary.LittleEndian, &bloomFilterLen)

	bloomFilterData := validData[bloomFilterOffset : int(bloomFilterOffset)+int(bloomFilterLen)]
	// The bloom filter data itself starts after its metadata (numBits, numHashes).
	// Zeroing out the bit array guarantees a false negative.
	if len(bloomFilterData) > 12 {
		for i := 12; i < len(bloomFilterData); i++ {
			bloomFilterData[i] = 0
		}
	}
	require.NoError(t, os.WriteFile(validPath, validData, 0644))

	// 3. Load the corrupted table and verify integrity
	corruptedSST, err := LoadSSTable(LoadSSTableOptions{FilePath: validPath, ID: 1, Logger: slog.Default()})
	require.NoError(t, err)
	defer corruptedSST.Close()

	// The bloom filter should now give a false negative for "key-exists"
	require.False(t, corruptedSST.Contains([]byte("key-exists")), "Corrupted bloom filter should not contain the key")

	// VerifyIntegrity with deep check should detect this
	errs := corruptedSST.VerifyIntegrity(true)
	require.NotEmpty(t, errs, "VerifyIntegrity should find a false negative error")
	assert.Contains(t, errs[0].Error(), "Bloom filter returned false negative", "Error should be about bloom filter false negative")
}

func TestSSTable_Get_BloomFilterFalsePositive(t *testing.T) {
	// This test ensures that if the bloom filter returns true, but the key
	// is not actually in the block, Get correctly returns ErrNotFound.

	// 1. Create a block with "key1" and "key3"
	tempDir := t.TempDir()
	writer, err := NewSSTableWriter(core.SSTableWriterOptions{DataDir: tempDir, ID: 1, Compressor: &compressors.NoCompressionCompressor{}, Logger: slog.Default(), BloomFilterFalsePositiveRate: 0.01})
	require.NoError(t, err)
	require.NoError(t, writer.Add([]byte("key1"), []byte("v1"), core.EntryTypePutEvent, 1))
	require.NoError(t, writer.Add([]byte("key3"), []byte("v3"), core.EntryTypePutEvent, 3))
	require.NoError(t, writer.Finish())

	// 2. Load the SSTable
	sst, err := LoadSSTable(LoadSSTableOptions{FilePath: writer.FilePath(), ID: 1, Logger: slog.Default(), BlockCache: nil})
	require.NoError(t, err)
	defer sst.Close()

	// 3. Find a key ("key2") that is *not* in the table, but *might* cause a bloom filter false positive.
	keyToFind := []byte("key2")

	// Manually check bloom filter for logging purposes
	if sst.Contains(keyToFind) {
		t.Logf("Bloom filter for 'key2' returned true (a potential false positive). This is expected for the test to proceed.")
	} else {
		t.Logf("Bloom filter for 'key2' returned false. The test will still verify the Get logic.")
	}

	// 4. Call Get and verify it returns ErrNotFound
	_, _, err = sst.Get(keyToFind)
	assert.ErrorIs(t, err, ErrNotFound, "Get should return ErrNotFound even if bloom filter has a false positive")
}
