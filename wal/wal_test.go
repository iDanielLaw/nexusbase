package wal

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper to create WAL options for testing.
func testWALOptions(t *testing.T, dir string) Options {
	t.Helper()
	return Options{
		Dir:            dir,
		SyncMode:       SyncDisabled, // Use SyncDisabled for performance in tests
		MaxSegmentSize: 64 * 1024,    // 64KB, small for testing rotation
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// Helper to create a slice of test WAL entries.
func createTestWALEntries(count int) []core.WALEntry {
	entries := make([]core.WALEntry, count)
	for i := 0; i < count; i++ {
		entries[i] = core.WALEntry{
			EntryType: core.EntryTypePutEvent,
			Key:       []byte(fmt.Sprintf("key-%d", i)),
			Value:     []byte(fmt.Sprintf("value-%d", i)),
			SeqNum:    uint64(i + 1),
		}
	}
	return entries
}

func TestOpenWAL_New(t *testing.T) {
	tempDir := t.TempDir()
	opts := testWALOptions(t, tempDir)

	wal, recovered, err := Open(opts)
	require.NoError(t, err, "Opening a new WAL should not fail")
	require.NotNil(t, wal)
	defer wal.Close()

	assert.Empty(t, recovered, "A new WAL should have no recovered entries")
	assert.Equal(t, uint64(1), wal.ActiveSegmentIndex(), "A new WAL should start with segment index 1")
}

func TestWAL_AppendAndRecover(t *testing.T) {
	tempDir := t.TempDir()
	opts := testWALOptions(t, tempDir)

	// 1. Open WAL and write some entries
	wal, _, err := Open(opts)
	require.NoError(t, err)

	entries := createTestWALEntries(5)
	err = wal.AppendBatch(entries)
	require.NoError(t, err)

	// Append a single entry
	singleEntry := core.WALEntry{Key: []byte("single"), Value: []byte("entry"), SeqNum: 6, EntryType: core.EntryTypePutEvent}
	err = wal.Append(singleEntry)
	require.NoError(t, err)

	err = wal.Close()
	require.NoError(t, err)

	// 2. Re-open the WAL and check recovered entries
	wal2, recovered, err := Open(opts)
	require.NoError(t, err, "Re-opening WAL should succeed")
	require.NotNil(t, wal2)
	defer wal2.Close()

	expectedEntries := append(entries, singleEntry)
	require.Len(t, recovered, len(expectedEntries), "Should recover all written entries")

	// Compare recovered entries with expected
	for i := range expectedEntries {
		assert.Equal(t, expectedEntries[i].SeqNum, recovered[i].SeqNum)
		assert.Equal(t, expectedEntries[i].Key, recovered[i].Key)
		assert.Equal(t, expectedEntries[i].Value, recovered[i].Value)
		assert.Equal(t, expectedEntries[i].EntryType, recovered[i].EntryType)
	}
}

func TestWAL_Rotation(t *testing.T) {
	tempDir := t.TempDir()
	opts := testWALOptions(t, tempDir)
	opts.MaxSegmentSize = 256 // Very small size to force rotation

	wal, _, err := Open(opts)
	require.NoError(t, err)
	defer wal.Close()

	assert.Equal(t, uint64(1), wal.ActiveSegmentIndex(), "Initial segment index should be 1")

	// Write entries until rotation occurs
	var totalEntries []core.WALEntry
	for i := 0; i < 100; i++ {
		entry := core.WALEntry{
			Key:       []byte(fmt.Sprintf("key-for-rotation-%d", i)),
			Value:     []byte("a somewhat long value to ensure we fill the segment"),
			SeqNum:    uint64(i + 1),
			EntryType: core.EntryTypePutEvent,
		}
		err := wal.Append(entry)
		require.NoError(t, err)
		totalEntries = append(totalEntries, entry)
	}

	assert.Greater(t, wal.ActiveSegmentIndex(), uint64(1), "WAL should have rotated to a new segment")
	rotatedIndex := wal.ActiveSegmentIndex()

	// Append one more entry after rotation
	finalEntry := core.WALEntry{Key: []byte("final"), Value: []byte("entry"), SeqNum: 11, EntryType: core.EntryTypePutEvent}
	err = wal.Append(finalEntry)
	require.NoError(t, err)
	totalEntries = append(totalEntries, finalEntry)

	assert.Equal(t, rotatedIndex, wal.ActiveSegmentIndex(), "Segment index should not change after one more append")

	// Close and recover to verify all data is intact
	err = wal.Close()
	require.NoError(t, err)

	wal2, recovered, err := Open(opts)
	require.NoError(t, err)
	defer wal2.Close()

	require.Len(t, recovered, len(totalEntries), "Should recover all entries across rotated segments")
	// Simple check on first and last entry
	assert.Equal(t, totalEntries[0].Key, recovered[0].Key)
	assert.Equal(t, totalEntries[len(totalEntries)-1].Key, recovered[len(recovered)-1].Key)
}

func TestWAL_Purge(t *testing.T) {
	tempDir := t.TempDir()
	opts := testWALOptions(t, tempDir)
	opts.MaxSegmentSize = 128 // Small size to force rotation

	wal, _, err := Open(opts)
	require.NoError(t, err)

	// Create a few segments
	require.NoError(t, wal.Append(core.WALEntry{Key: []byte("a"), Value: []byte("long value to trigger rotation maybe"), SeqNum: 1}))
	require.NoError(t, wal.Rotate()) // Manual rotate to segment 2
	require.NoError(t, wal.Append(core.WALEntry{Key: []byte("b"), Value: []byte("long value to trigger rotation maybe"), SeqNum: 2}))
	require.NoError(t, wal.Rotate()) // Manual rotate to segment 3
	require.NoError(t, wal.Append(core.WALEntry{Key: []byte("c"), Value: []byte("long value to trigger rotation maybe"), SeqNum: 3}))
	require.NoError(t, wal.Rotate()) // Manual rotate to segment 4
	require.NoError(t, wal.Append(core.WALEntry{Key: []byte("d"), Value: []byte("long value to trigger rotation maybe"), SeqNum: 4}))

	activeSegmentIdx := wal.ActiveSegmentIndex()
	assert.Equal(t, uint64(4), activeSegmentIdx, "Should be on segment 4")

	// Purge up to segment 2
	err = wal.Purge(2)
	require.NoError(t, err)

	// Check that segment files 1 and 2 are gone, but 3 and 4 remain
	_, err = os.Stat(filepath.Join(tempDir, formatSegmentFileName(1)))
	assert.True(t, os.IsNotExist(err), "Segment 1 should be purged")
	_, err = os.Stat(filepath.Join(tempDir, formatSegmentFileName(2)))
	assert.True(t, os.IsNotExist(err), "Segment 2 should be purged")
	_, err = os.Stat(filepath.Join(tempDir, formatSegmentFileName(3)))
	assert.NoError(t, err, "Segment 3 should not be purged")
	_, err = os.Stat(filepath.Join(tempDir, formatSegmentFileName(4)))
	assert.NoError(t, err, "Segment 4 (active) should not be purged")

	// Try to purge the active segment - it should be skipped
	err = wal.Purge(activeSegmentIdx)
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(tempDir, formatSegmentFileName(activeSegmentIdx)))
	assert.NoError(t, err, "Active segment should not be purged even if requested")

	wal.Close()
}

func TestWAL_Recovery_Corrupted(t *testing.T) {
	tempDir := t.TempDir()
	opts := testWALOptions(t, tempDir)

	// 1. Create a WAL with good data
	wal, _, err := Open(opts)
	require.NoError(t, err)
	goodEntries := createTestWALEntries(3)
	require.NoError(t, wal.AppendBatch(goodEntries))
	segmentPath := wal.activeSegment.path
	require.NoError(t, wal.Close())

	// 2. Corrupt the segment file by truncating it
	fileData, err := os.ReadFile(segmentPath)
	require.NoError(t, err)
	corruptedData := fileData[:len(fileData)-5] // Truncate last 5 bytes (part of checksum/record)
	err = os.WriteFile(segmentPath, corruptedData, 0644)
	require.NoError(t, err)

	// 3. Attempt to recover
	wal2, recovered, err := Open(opts)
	require.Error(t, err, "Open should return an error for a corrupted WAL")
	assert.NotNil(t, wal2, "WAL object should still be returned on non-fatal recovery error")
	if wal2 != nil {
		defer wal2.Close()
	}

	// The recovery should stop at the corruption but return the entries it successfully read.
	// The key is that an error indicating corruption/truncation is returned.
	assert.Contains(t, err.Error(), "unexpected EOF", "Error should indicate truncation")
	t.Logf("Recovered %d entries from corrupted WAL", len(recovered))
}

func TestWAL_StartRecoveryIndex(t *testing.T) {
	tempDir := t.TempDir()
	opts := testWALOptions(t, tempDir)
	opts.MaxSegmentSize = 128 // Small size to force rotation

	// 1. Create a WAL with 3 segments
	wal, _, err := Open(opts)
	require.NoError(t, err)
	// Segment 1
	require.NoError(t, wal.Append(core.WALEntry{Key: []byte("a"), SeqNum: 1}))
	require.NoError(t, wal.Rotate())
	// Segment 2
	require.NoError(t, wal.Append(core.WALEntry{Key: []byte("b"), SeqNum: 2}))
	require.NoError(t, wal.Rotate())
	// Segment 3
	require.NoError(t, wal.Append(core.WALEntry{Key: []byte("c"), SeqNum: 3}))
	require.NoError(t, wal.Close())

	// 2. Recover, but start after segment 1
	opts.StartRecoveryIndex = 1
	wal2, recovered, err := Open(opts)
	require.NoError(t, err)
	defer wal2.Close()

	// Should only recover entries from segments > 1 (i.e., segments 2 and 3)
	require.Len(t, recovered, 2, "Should only recover entries from segments 2 and 3")
	assert.Equal(t, uint64(2), recovered[0].SeqNum)
	assert.Equal(t, []byte("b"), recovered[0].Key)
	assert.Equal(t, uint64(3), recovered[1].SeqNum)
	assert.Equal(t, []byte("c"), recovered[1].Key)
}
