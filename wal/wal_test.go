package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWAL_AppendAndRead(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	tempDir := t.TempDir()
	opts := Options{
		Dir:      tempDir,
		Logger:   logger,
		SyncMode: SyncAlways,
	}

	// --- Phase 1: Write entries ---
	w1, initialEntries, err := Open(opts)
	require.NoError(t, err, "Open on a new directory should not have a recovery error")
	require.Empty(t, initialEntries, "A new WAL should have no recovered entries")

	entry1 := core.WALEntry{EntryType: core.EntryTypePutEvent, Key: []byte("key1"), Value: []byte("val1"), SeqNum: 1}
	entry2 := core.WALEntry{EntryType: core.EntryTypeDelete, Key: []byte("key2"), Value: nil, SeqNum: 2}

	require.NoError(t, w1.Append(entry1))
	require.NoError(t, w1.Append(entry2))
	require.NoError(t, w1.Close())

	// --- Phase 2: Reopen and recover ---
	// The second WAL instance is only to trigger recovery. We don't use it after.
	w2, recoveredEntries, recoveryErr := Open(opts)
	require.NoError(t, w2.Close()) // Close the handle to prevent leaks
	// The error from NewWAL is the recovery error, which should be EOF for a clean read.
	require.NoError(t, recoveryErr)

	// --- Phase 3: Verify ---
	require.Len(t, recoveredEntries, 2, "Should recover 2 entries")
	assert.Equal(t, entry1, recoveredEntries[0], "Entry 1 mismatch")
	assert.Equal(t, entry2, recoveredEntries[1], "Entry 2 mismatch")
}

func TestWAL_AppendBatchAndRead(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	tempDir := t.TempDir()
	opts := Options{
		Dir:      tempDir,
		Logger:   logger,
		SyncMode: SyncAlways,
	}

	// --- Phase 1: Write batch ---
	w1, _, err := Open(opts)
	require.NoError(t, err)

	batchEntries := []core.WALEntry{
		{EntryType: core.EntryTypePutEvent, Key: []byte("batch_key1"), Value: []byte("batch_val1"), SeqNum: 10},
		{EntryType: core.EntryTypePutEvent, Key: []byte("batch_key2"), Value: []byte("batch_val2"), SeqNum: 11},
		{EntryType: core.EntryTypeDelete, Key: []byte("batch_key3"), Value: nil, SeqNum: 12},
	}
	require.NoError(t, w1.AppendBatch(batchEntries))
	require.NoError(t, w1.Close())

	// --- Phase 2: Reopen and recover ---
	w2, recoveredEntries, recoveryErr := Open(opts)
	require.NoError(t, w2.Close())
	require.NoError(t, recoveryErr)

	// --- Phase 3: Verify ---
	require.Len(t, recoveredEntries, 3, "Should recover 3 entries from batch")
	assert.Equal(t, batchEntries, recoveredEntries, "Batch entries mismatch")
}

func TestWAL_AppendBatch_Empty(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	tempDir := t.TempDir()
	opts := Options{
		Dir:    tempDir,
		Logger: logger,
	}

	w1, _, err := Open(opts)
	require.NoError(t, err)

	require.NoError(t, w1.AppendBatch([]core.WALEntry{}), "AppendBatch with empty slice should not return an error")
	require.NoError(t, w1.Close())

	w2, recoveredEntries, recoveryErr := Open(opts)
	require.NoError(t, w2.Close())
	require.NoError(t, recoveryErr)

	require.Empty(t, recoveredEntries, "Expected 0 entries after appending empty batch")
}

// TestWAL_Recovery_Corrupted tests the WAL's ability to recover as much data as possible
// from a corrupted file.
func TestWAL_Recovery_Corrupted(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	validEntries := []core.WALEntry{
		{EntryType: core.EntryTypePutEvent, Key: []byte("key1"), Value: []byte("value1"), SeqNum: 1},
		{EntryType: core.EntryTypePutEvent, Key: []byte("key2"), Value: []byte("value2_long_value_to_ensure_truncation_is_possible"), SeqNum: 2},
		{EntryType: core.EntryTypePutEvent, Key: []byte("key3"), Value: []byte("value3"), SeqNum: 3},
	}

	// Helper to create a valid WAL directory with multiple records for corruption tests
	createValidWAL := func(t *testing.T) string {
		t.Helper()
		walDir := t.TempDir()
		opts := Options{Dir: walDir, Logger: logger, SyncMode: SyncAlways}

		w, initialEntries, recoveryErr := Open(opts)
		require.NoError(t, recoveryErr, "Recovery error should be nil when creating a fresh WAL")
		require.Empty(t, initialEntries, "A fresh WAL file should have no initial entries")

		// Use Append to create separate records for each entry
		for _, entry := range validEntries {
			err := w.Append(entry)
			require.NoError(t, err)
		}
		err := w.Close()
		require.NoError(t, err)
		return walDir
	}

	// --- Test Cases for different corruption types ---

	t.Run("TruncatedInMiddleOfRecord", func(t *testing.T) {
		walDir := createValidWAL(t)
		segmentPath := filepath.Join(walDir, "00000001.wal") // Assume first segment
		originalData, err := os.ReadFile(segmentPath)
		require.NoError(t, err)

		// Calculate offset to truncate. After the header and the first full record.
		headerSize := binary.Size(core.FileHeader{})
		firstRecordSize := getEncodedRecordSize(t, &validEntries[0])
		truncateOffset := headerSize + firstRecordSize + 10 // Truncate 10 bytes into the second record

		require.Greater(t, len(originalData), truncateOffset, "File is too small to test truncation at the desired offset")

		// Truncate the file
		err = os.WriteFile(segmentPath, originalData[:truncateOffset], 0644)
		require.NoError(t, err)

		// Attempt recovery
		opts := Options{Dir: walDir, Logger: logger}
		w, recoveredEntries, recoveryErr := Open(opts)
		if w != nil { // The WAL instance must be closed to release the file handle.
			w.Close()
		}
		require.Error(t, recoveryErr, "Expected an error from reading a truncated WAL")
		assert.True(t, errors.Is(recoveryErr, io.ErrUnexpectedEOF) || strings.Contains(recoveryErr.Error(), "failed to read"), "Expected an unexpected EOF or read error")

		// Verification
		require.Len(t, recoveredEntries, 1, "Should have recovered only the first valid entry")
		assert.Equal(t, validEntries[0], recoveredEntries[0], "The recovered entry should match the first valid entry")
	})

	t.Run("BadChecksum", func(t *testing.T) {
		walDir := createValidWAL(t)
		segmentPath := filepath.Join(walDir, "00000001.wal")
		originalData, err := os.ReadFile(segmentPath)
		require.NoError(t, err)

		// Find the offset of the second record's data and corrupt it
		headerSize := binary.Size(core.FileHeader{})
		firstRecordSize := getEncodedRecordSize(t, &validEntries[0])
		secondRecordDataOffset := headerSize + firstRecordSize + 4 // After header, first record, and second record's length prefix

		require.Greater(t, len(originalData), secondRecordDataOffset, "File is too small to test checksum corruption")

		// Corrupt a byte in the second record's data
		originalData[secondRecordDataOffset]++

		err = os.WriteFile(segmentPath, originalData, 0644)
		require.NoError(t, err)

		// Attempt recovery
		w, recoveredEntries, recoveryErr := Open(Options{Dir: walDir, Logger: logger})
		if w != nil {
			w.Close()
		}
		require.Error(t, recoveryErr, "Expected an error from reading a WAL with a bad checksum")
		assert.Contains(t, recoveryErr.Error(), "checksum mismatch", "Error message should indicate a checksum mismatch")

		// Verification
		require.Len(t, recoveredEntries, 1, "Should have recovered only the first valid entry")
		assert.Equal(t, validEntries[0], recoveredEntries[0], "The recovered entry should match the first valid entry")
	})

	t.Run("BadLengthPrefix", func(t *testing.T) {
		walDir := createValidWAL(t)
		segmentPath := filepath.Join(walDir, "00000001.wal")
		originalData, err := os.ReadFile(segmentPath)
		require.NoError(t, err)

		// Find the offset of the second record's length prefix and corrupt it
		headerSize := binary.Size(core.FileHeader{})
		firstRecordSize := getEncodedRecordSize(t, &validEntries[0])
		secondRecordLengthOffset := headerSize + firstRecordSize

		require.Greater(t, len(originalData), secondRecordLengthOffset+4, "File is too small to test length prefix corruption")

		// Set an impossibly large length
		binary.LittleEndian.PutUint32(originalData[secondRecordLengthOffset:], 0xFFFFFFFF)

		err = os.WriteFile(segmentPath, originalData, 0644)
		require.NoError(t, err)

		// Attempt recovery
		w, recoveredEntries, recoveryErr := Open(Options{Dir: walDir, Logger: logger})
		if w != nil {
			w.Close()
		}
		require.Error(t, recoveryErr, "Expected an error from reading a WAL with a bad length prefix")
		assert.Contains(t, recoveryErr.Error(), "exceeds sanity limit", "Error message should indicate a bad length")

		// Verification
		require.Len(t, recoveredEntries, 1, "Should have recovered only the first valid entry")
		assert.Equal(t, validEntries[0], recoveredEntries[0], "The recovered entry should match the first valid entry")
	})
}

func TestWAL_Rotation(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	tempDir := t.TempDir()

	// Set a very small segment size to force rotation
	entry1 := core.WALEntry{EntryType: core.EntryTypePutEvent, Key: []byte("key1"), Value: []byte("val1"), SeqNum: 1}
	recordSize := getEncodedRecordSize(t, &entry1)
	headerSize := binary.Size(core.FileHeader{})

	opts := Options{
		Dir:            tempDir,
		Logger:         logger,
		SyncMode:       SyncAlways,
		MaxSegmentSize: int64(headerSize + recordSize), // Rotate after 1 record
	}

	// --- Phase 1: Write entries to trigger rotation ---
	w, _, err := Open(opts)
	require.NoError(t, err)

	// First write should fit in segment 1
	require.NoError(t, w.Append(entry1))

	// Check that only one segment exists
	files, err := os.ReadDir(tempDir)
	require.NoError(t, err)
	require.Len(t, files, 1, "Should have 1 segment file before rotation")
	assert.Equal(t, "00000001.wal", files[0].Name())

	// Second write should trigger rotation
	entry2 := core.WALEntry{EntryType: core.EntryTypePutEvent, Key: []byte("key2"), Value: []byte("val2"), SeqNum: 2}
	require.NoError(t, w.Append(entry2))

	// Check that a new segment was created
	files, err = os.ReadDir(tempDir)
	require.NoError(t, err)
	require.Len(t, files, 2, "Should have 2 segment files after rotation")
	assert.Equal(t, "00000001.wal", files[0].Name())
	assert.Equal(t, "00000002.wal", files[1].Name())

	require.NoError(t, w.Close())

	// --- Phase 2: Reopen and recover ---
	w2, recoveredEntries, recoveryErr := Open(opts)
	require.NoError(t, w2.Close())
	require.NoError(t, recoveryErr, "A clean recovery across multiple segments should not return an error")

	// --- Phase 3: Verify ---
	require.Len(t, recoveredEntries, 2, "Should recover 2 entries from two segments")
	assert.Equal(t, entry1, recoveredEntries[0])
	assert.Equal(t, entry2, recoveredEntries[1])
}

func TestWAL_Purge(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	tempDir := t.TempDir()

	// Set a small segment size to create multiple segments easily
	entry := core.WALEntry{EntryType: core.EntryTypePutEvent, Key: []byte("k"), Value: []byte("v"), SeqNum: 1}
	recordSize := getEncodedRecordSize(t, &entry)
	headerSize := binary.Size(core.FileHeader{})

	opts := Options{
		Dir:            tempDir,
		Logger:         logger,
		SyncMode:       SyncAlways,
		MaxSegmentSize: int64(headerSize + recordSize), // Rotate after 1 record
	}

	// --- Phase 1: Create 3 segments ---
	w, _, err := Open(opts)
	require.NoError(t, err)

	entry.SeqNum = 1
	require.NoError(t, w.Append(entry)) // Writes to seg 1
	entry.SeqNum = 2
	require.NoError(t, w.Append(entry)) // Rotates, writes to seg 2
	entry.SeqNum = 3
	require.NoError(t, w.Append(entry)) // Rotates, writes to seg 3

	// Check that 3 segments exist
	files, err := os.ReadDir(tempDir)
	require.NoError(t, err)
	require.Len(t, files, 3, "Should have 3 segment files")

	// --- Phase 2: Purge first segment ---
	err = w.Purge(1)
	require.NoError(t, err)

	// Check files
	files, err = os.ReadDir(tempDir)
	require.NoError(t, err)
	require.Len(t, files, 2, "Should have 2 segment files after purging index 1")
	assert.Equal(t, "00000002.wal", files[0].Name())
	assert.Equal(t, "00000003.wal", files[1].Name())

	// --- Phase 3: Purge active segment (should be skipped) ---
	// Active segment is 3
	err = w.Purge(3)
	require.NoError(t, err)

	files, err = os.ReadDir(tempDir)
	require.NoError(t, err)
	require.Len(t, files, 1, "Should have 1 segment file after attempting to purge active segment")
	assert.Equal(t, "00000003.wal", files[0].Name(), "Active segment should not be purged")

	require.NoError(t, w.Close())
}

// getEncodedRecordSize is a test helper to calculate the on-disk size of a record.
func getEncodedRecordSize(t *testing.T, entry *core.WALEntry) int {
	t.Helper()
	var data bytes.Buffer
	// For simplicity, we'll just encode to get the length.
	err := encodeEntryData(&data, entry)
	require.NoError(t, err)
	return 4 + data.Len() + 4 // length + data + checksum
}
