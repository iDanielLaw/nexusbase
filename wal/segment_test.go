package wal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSegmentFileNameFormat(t *testing.T) {
	tests := []struct {
		index    uint64
		expected string
	}{
		{1, "00000001.wal"},
		{12345, "00012345.wal"},
		{99999999, "99999999.wal"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			fileName := core.FormatSegmentFileName(tt.index)
			assert.Equal(t, tt.expected, fileName)

			parsedIndex, err := core.ParseSegmentFileName(fileName)
			require.NoError(t, err)
			assert.Equal(t, tt.index, parsedIndex)
		})
	}

	t.Run("ParseError", func(t *testing.T) {
		_, err := core.ParseSegmentFileName("not_a_segment.log")
		assert.Error(t, err)
		_, err = core.ParseSegmentFileName("00000001.wal_backup")
		assert.Error(t, err)
	})
}

func TestCreateSegment(t *testing.T) {
	tempDir := t.TempDir()

	t.Run("SuccessfulCreation", func(t *testing.T) {
		sw, err := CreateSegment(tempDir, 1)
		require.NoError(t, err)
		require.NotNil(t, sw)
		defer sw.Close()

		// Check if file exists
		_, err = os.Stat(sw.path)
		assert.NoError(t, err, "Segment file should be created")

		// Check file size is just the header
		size, err := sw.Size()
		require.NoError(t, err)
		assert.Equal(t, int64(binary.Size(core.FileHeader{})), size, "Initial size should be just the header")
	})

	t.Run("CreationInNonExistentDir", func(t *testing.T) {
		// CreateSegment does not create the directory, os.MkdirAll in WAL.Open does.
		// So this should fail.
		nonExistentDir := filepath.Join(tempDir, "nonexistent")
		_, err := CreateSegment(nonExistentDir, 1)
		require.Error(t, err)
		assert.ErrorIs(t, err, os.ErrNotExist)
	})
}

func TestSegment_WriteAndReadRecord(t *testing.T) {
	tempDir := t.TempDir()

	// 1. Create and write to a segment
	sw, err := CreateSegment(tempDir, 1)
	require.NoError(t, err)

	record1 := []byte("hello world")
	err = sw.WriteRecord(record1)
	require.NoError(t, err)

	record2 := []byte("another record")
	err = sw.WriteRecord(record2)
	require.NoError(t, err)

	err = sw.Close()
	require.NoError(t, err)

	// 2. Open the segment for reading
	sr, err := OpenSegmentForRead(sw.path)
	require.NoError(t, err)
	defer sr.Close()

	// 3. Read and verify records
	readRecord1, err := sr.ReadRecord()
	require.NoError(t, err)
	assert.Equal(t, record1, readRecord1)

	readRecord2, err := sr.ReadRecord()
	require.NoError(t, err)
	assert.Equal(t, record2, readRecord2)

	// 4. Check for EOF
	_, err = sr.ReadRecord()
	assert.ErrorIs(t, err, io.EOF, "Should return EOF after reading all records")
}

func TestSegmentReader_CorruptedData(t *testing.T) {
	tempDir := t.TempDir()
	segmentPath := filepath.Join(tempDir, core.FormatSegmentFileName(1)) // Use a valid filename format

	// Helper to write a valid record and then corrupt the file
	writeAndCorrupt := func(t *testing.T, corruption func([]byte) []byte) {
		t.Helper()
		// Write a valid record first
		var buf bytes.Buffer
		validRecord := []byte("this is a valid record")
		require.NoError(t, binary.Write(&buf, binary.LittleEndian, uint32(len(validRecord))))
		buf.Write(validRecord)
		checksum := crc32.ChecksumIEEE(validRecord)
		require.NoError(t, binary.Write(&buf, binary.LittleEndian, checksum))

		// Apply corruption
		corruptedBytes := corruption(buf.Bytes())

		// Write to file with a valid header
		file, err := os.Create(segmentPath)
		require.NoError(t, err)
		header := core.NewFileHeader(core.WALMagicNumber, core.CompressionNone)
		require.NoError(t, binary.Write(file, binary.LittleEndian, &header))
		_, err = file.Write(corruptedBytes)
		require.NoError(t, err)
		file.Close()
	}

	t.Run("CorruptedChecksum", func(t *testing.T) {
		writeAndCorrupt(t, func(data []byte) []byte {
			// Flip a bit in the checksum
			data[len(data)-1] ^= 0xFF
			return data
		})

		sr, err := OpenSegmentForRead(segmentPath)
		require.NoError(t, err)
		defer sr.Close()

		_, err = sr.ReadRecord()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "checksum mismatch")
	})

	t.Run("TruncatedData", func(t *testing.T) {
		writeAndCorrupt(t, func(data []byte) []byte {
			// Truncate the data part
			return data[:len(data)-10]
		})

		sr, err := OpenSegmentForRead(segmentPath)
		require.NoError(t, err)
		defer sr.Close()

		_, err = sr.ReadRecord()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read record data")
	})
}

func TestOpenSegmentForRead_ErrorCases(t *testing.T) {
	tempDir := t.TempDir()

	t.Run("FileNotExist", func(t *testing.T) {
		_, err := OpenSegmentForRead(filepath.Join(tempDir, "nonexistent.wal"))
		require.Error(t, err)
		assert.ErrorIs(t, err, os.ErrNotExist)
	})

	t.Run("InvalidMagicNumber", func(t *testing.T) {
		path := filepath.Join(tempDir, core.FormatSegmentFileName(2)) // Use a valid filename format
		file, err := os.Create(path)
		require.NoError(t, err)
		// Write a bad header
		header := core.NewFileHeader(0xAECDCDAE, core.CompressionNone)
		require.NoError(t, binary.Write(file, binary.LittleEndian, &header))
		file.Close()

		_, err = OpenSegmentForRead(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid magic number")
	})

	t.Run("TruncatedHeader", func(t *testing.T) {
		path := filepath.Join(tempDir, core.FormatSegmentFileName(3)) // Use a valid filename format
		err := os.WriteFile(path, []byte{0x01, 0x02, 0x03}, 0644)
		require.NoError(t, err)

		_, err = OpenSegmentForRead(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read segment header")
	})
}

func TestSegmentWriter_Close(t *testing.T) {
	tempDir := t.TempDir()
	sw, err := CreateSegment(tempDir, 1)
	require.NoError(t, err)

	err = sw.Close()
	require.NoError(t, err)

	// Second close should be a no-op
	err = sw.Close()
	require.NoError(t, err)

	// Writing after close should fail
	err = sw.WriteRecord([]byte("test"))
	require.Error(t, err)
	assert.ErrorIs(t, err, os.ErrClosed)
}
