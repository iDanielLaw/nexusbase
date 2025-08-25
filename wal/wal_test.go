package wal

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper to create WAL options for testing.
func testWALOptions(t *testing.T, dir string) Options {
	t.Helper()
	return Options{
		Dir:            dir,
		SyncMode:       core.WALSyncDisabled, // Use SyncDisabled for performance in tests
		MaxSegmentSize: 64 * 1024,            // 64KB, small for testing rotation
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// Helper to create a slice of test WAL entries.
func createTestWALEntries(count int, startSeqNum uint64) []core.WALEntry {
	entries := make([]core.WALEntry, count)
	for i := 0; i < count; i++ {
		entries[i] = core.WALEntry{
			EntryType: core.EntryTypePutEvent,
			Key:       []byte(fmt.Sprintf("key-%d", startSeqNum+uint64(i))),
			Value:     []byte(fmt.Sprintf("value-%d", startSeqNum+uint64(i))),
			SeqNum:    startSeqNum + uint64(i),
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

	entries := createTestWALEntries(5, 1)
	err = wal.AppendBatch(entries)
	require.NoError(t, err)

	// Append a single entry
	singleEntry := core.WALEntry{Key: []byte("single"), Value: []byte("entry"), SeqNum: 6, EntryType: core.EntryTypePutEvent}
	err = wal.Append(singleEntry)
	require.NoError(t, err)

	err = wal.Close() // This will wait for the committer to finish
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
	t.Run("RotationOnMultipleSmallWrites", func(t *testing.T) {
		tempDir := t.TempDir()
		opts := testWALOptions(t, tempDir)
		opts.MaxSegmentSize = 256 // Very small size to force rotation

		wal, _, err := Open(opts)
		require.NoError(t, err)

		assert.Equal(t, uint64(1), wal.ActiveSegmentIndex(), "Initial segment index should be 1")

		// Write entries until rotation occurs
		var totalEntries []core.WALEntry
		var seqNum uint64 = 0
		for i := 0; i < 10; i++ {
			seqNum++
			entry := core.WALEntry{
				Key:       []byte(fmt.Sprintf("key-for-rotation-%d", i)),
				Value:     []byte("a somewhat long value to ensure we fill the segment"),
				SeqNum:    seqNum,
				EntryType: core.EntryTypePutEvent,
			}
			err := wal.Append(entry)
			require.NoError(t, err)
			totalEntries = append(totalEntries, entry)
		}

		// Force a sync to make sure the committer runs and rotates if needed
		require.NoError(t, wal.Sync())

		assert.Greater(t, wal.ActiveSegmentIndex(), uint64(1), "WAL should have rotated to a new segment")
		rotatedIndex := wal.ActiveSegmentIndex()

		// Append one more entry after rotation
		seqNum++
		finalEntry := core.WALEntry{Key: []byte("final"), Value: []byte("entry"), SeqNum: seqNum, EntryType: core.EntryTypePutEvent}
		err = wal.Append(finalEntry)
		require.NoError(t, err)
		totalEntries = append(totalEntries, finalEntry)

		require.NoError(t, wal.Sync())
		assert.Equal(t, rotatedIndex, wal.ActiveSegmentIndex(), "Segment index should not change after one more append")

		// Close and recover to verify all data is intact
		require.NoError(t, wal.Close())

		wal2, recovered, err := Open(opts)
		require.NoError(t, err)

		require.Len(t, recovered, len(totalEntries), "Should recover all entries across rotated segments")
		// Simple check on first and last entry
		assert.Equal(t, totalEntries[0].Key, recovered[0].Key)
		assert.Equal(t, totalEntries[len(totalEntries)-1].Key, recovered[len(recovered)-1].Key)
		wal2.Close()
	})
}

func TestWAL_GroupCommit(t *testing.T) {
	tempDir := t.TempDir()
	opts := testWALOptions(t, tempDir)
	opts.SyncMode = core.WALSyncAlways // Use SyncAlways to test fsync behavior

	wal, _, err := Open(opts)
	require.NoError(t, err)

	numGoroutines := 10
	numEntriesPerGoroutine := 10
	var wg sync.WaitGroup

	// All goroutines will append concurrently.
	// The group commit mechanism should batch these writes together.
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			startSeq := uint64(1 + gID*numEntriesPerGoroutine)
			entries := createTestWALEntries(numEntriesPerGoroutine, startSeq)
			err := wal.AppendBatch(entries)
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// Close the WAL to ensure all pending commits are flushed.
	require.NoError(t, wal.Close())

	// Re-open and verify that all entries were written correctly.
	wal2, recovered, err := Open(opts)
	require.NoError(t, err)
	defer wal2.Close()

	assert.Len(t, recovered, numGoroutines*numEntriesPerGoroutine, "Should recover all entries from all goroutines")
}

func TestWAL_Close(t *testing.T) {
	tempDir := t.TempDir()
	opts := testWALOptions(t, tempDir)
	wal, _, err := Open(opts)
	require.NoError(t, err)

	// Append some data
	require.NoError(t, wal.Append(core.WALEntry{Key: []byte("a"), SeqNum: 1}))

	// Close should not return an error and should shutdown the committer.
	require.NoError(t, wal.Close())

	// A second close should be a no-op and not cause a panic.
	require.NotPanics(t, func() {
		assert.NoError(t, wal.Close())
	})
}

func TestWAL_GroupCommit_ConfigurableOptions(t *testing.T) {
	t.Run("CommitMaxBatchSize", func(t *testing.T) {
		tempDir := t.TempDir()
		opts := testWALOptions(t, tempDir)
		opts.CommitMaxBatchSize = 5 // Force commit after 5 records

		wal, _, err := Open(opts)
		require.NoError(t, err)

		// Append 4 entries, they should be held pending by the committer
		// We use a wait group to know when the goroutines are done.
		var wg sync.WaitGroup
		for i := 0; i < opts.CommitMaxBatchSize-1; i++ {
			wg.Add(1)
			go func(num int) {
				defer wg.Done()
				// These appends will block until the batch is committed.
				err := wal.Append(core.WALEntry{Key: []byte(fmt.Sprintf("key-%d", num)), SeqNum: uint64(num + 1)})
				assert.NoError(t, err)
			}(i)
		}

		// The 5th entry should trigger the batch commit, unblocking the other goroutines.
		err = wal.Append(core.WALEntry{Key: []byte("key-trigger"), SeqNum: uint64(opts.CommitMaxBatchSize)})
		require.NoError(t, err)

		// Wait for the initial goroutines to complete.
		wg.Wait()

		// Close the WAL to ensure everything is flushed.
		require.NoError(t, wal.Close())

		// Re-open and verify.
		wal2, recovered, err := Open(opts)
		require.NoError(t, err)
		defer wal2.Close()
		assert.Len(t, recovered, opts.CommitMaxBatchSize, "Should recover all entries from the batch")
	})

	t.Run("CommitMaxDelay", func(t *testing.T) {
		tempDir := t.TempDir()
		opts := testWALOptions(t, tempDir)
		opts.CommitMaxDelay = 5 * time.Millisecond // Force commit after a short delay

		wal, _, err := Open(opts)
		require.NoError(t, err)

		start := time.Now()
		// This append will block until the ticker forces the commit.
		err = wal.Append(core.WALEntry{Key: []byte("key-delay"), SeqNum: 1})
		duration := time.Since(start)

		require.NoError(t, err)
		// The duration should be slightly longer than the delay, accounting for processing time.
		assert.GreaterOrEqual(t, duration, opts.CommitMaxDelay)
		// It shouldn't be excessively long either.
		assert.Less(t, duration, opts.CommitMaxDelay*20) // Increased multiplier for CI

		require.NoError(t, wal.Close())

		// Re-open and verify.
		wal2, recovered, err := Open(opts)
		require.NoError(t, err)
		defer wal2.Close()
		assert.Len(t, recovered, 1, "Should recover the single entry")
	})
}

func TestWAL_Purge(t *testing.T) {
	// Setup: Create a WAL with 4 segments
	tempDir := t.TempDir()
	opts := testWALOptions(t, tempDir)
	// Use a small segment size to make rotation easy
	opts.MaxSegmentSize = 128

	wal, _, err := Open(opts)
	require.NoError(t, err)

	// Write some data to create segment 1
	require.NoError(t, wal.Append(core.WALEntry{Key: []byte("a"), Value: []byte("a"), SeqNum: 1}))
	// Rotate to segment 2
	require.NoError(t, wal.Rotate())
	require.Equal(t, uint64(2), wal.ActiveSegmentIndex())
	require.NoError(t, wal.Append(core.WALEntry{Key: []byte("b"), Value: []byte("b"), SeqNum: 2}))
	// Rotate to segment 3
	require.NoError(t, wal.Rotate())
	require.Equal(t, uint64(3), wal.ActiveSegmentIndex())
	require.NoError(t, wal.Append(core.WALEntry{Key: []byte("c"), Value: []byte("c"), SeqNum: 3}))
	// Rotate to segment 4
	require.NoError(t, wal.Rotate())
	require.Equal(t, uint64(4), wal.ActiveSegmentIndex())
	require.NoError(t, wal.Append(core.WALEntry{Key: []byte("d"), Value: []byte("d"), SeqNum: 4}))

	// At this point, we have 4 segment files: 000001.wal, 000002.wal, 000003.wal, 000004.wal
	// Active segment is 4.
	require.Len(t, wal.segmentIndexes, 4, "Should have 4 segments before purge")

	t.Run("Purge old segments", func(t *testing.T) {
		// Purge segments up to index 2. This should delete 1 and 2.
		err := wal.Purge(2)
		require.NoError(t, err)

		// Check internal state
		assert.Len(t, wal.segmentIndexes, 2, "Should have 2 segments remaining")
		assert.Equal(t, []uint64{3, 4}, wal.segmentIndexes, "Remaining segments should be 3 and 4")

		// Check filesystem
		_, err = os.Stat(filepath.Join(tempDir, core.FormatSegmentFileName(1)))
		assert.True(t, os.IsNotExist(err), "Segment 1 should be deleted")
		_, err = os.Stat(filepath.Join(tempDir, core.FormatSegmentFileName(2)))
		assert.True(t, os.IsNotExist(err), "Segment 2 should be deleted")
		_, err = os.Stat(filepath.Join(tempDir, core.FormatSegmentFileName(3)))
		assert.NoError(t, err, "Segment 3 should still exist")
		_, err = os.Stat(filepath.Join(tempDir, core.FormatSegmentFileName(4)))
		assert.NoError(t, err, "Segment 4 (active) should still exist")
	})

	t.Run("Try to purge active segment", func(t *testing.T) {
		// Try to purge up to index 4. This should delete 3, but spare 4 (the active one).
		err := wal.Purge(4)
		require.NoError(t, err)

		// Check internal state
		assert.Len(t, wal.segmentIndexes, 1, "Should have 1 segment remaining")
		assert.Equal(t, []uint64{4}, wal.segmentIndexes, "Only segment 4 should remain")

		// Check filesystem
		_, err = os.Stat(filepath.Join(tempDir, core.FormatSegmentFileName(3)))
		assert.True(t, os.IsNotExist(err), "Segment 3 should be deleted")
		_, err = os.Stat(filepath.Join(tempDir, core.FormatSegmentFileName(4)))
		assert.NoError(t, err, "Segment 4 (active) should still exist")
	})

	t.Run("Purge with no matching segments", func(t *testing.T) {
		// At this point, only segment 4 exists. Purging up to 3 should do nothing.
		err := wal.Purge(3)
		require.NoError(t, err)
		assert.Len(t, wal.segmentIndexes, 1, "Length should not change")
		assert.Equal(t, []uint64{4}, wal.segmentIndexes)
	})

	require.NoError(t, wal.Close())
}

func TestWAL_Rotate_EdgeCases(t *testing.T) {
	t.Run("Rapid repeated rotations", func(t *testing.T) {
		tempDir := t.TempDir()
		opts := testWALOptions(t, tempDir)
		wal, _, err := Open(opts)
		require.NoError(t, err)

		require.Equal(t, uint64(1), wal.ActiveSegmentIndex())

		// Rotate 3 times in a row
		require.NoError(t, wal.Rotate())
		require.Equal(t, uint64(2), wal.ActiveSegmentIndex())

		require.NoError(t, wal.Rotate())
		require.Equal(t, uint64(3), wal.ActiveSegmentIndex())

		require.NoError(t, wal.Rotate())
		require.Equal(t, uint64(4), wal.ActiveSegmentIndex())

		// Check internal state
		assert.Equal(t, []uint64{1, 2, 3, 4}, wal.segmentIndexes)

		// Check filesystem
		for i := uint64(1); i <= 4; i++ {
			_, err := os.Stat(filepath.Join(tempDir, core.FormatSegmentFileName(i)))
			assert.NoError(t, err, "Segment %d should exist", i)
		}

		require.NoError(t, wal.Close())
	})

	t.Run("Rotation fails if cannot create new segment", func(t *testing.T) {
		tempDir := t.TempDir()
		opts := testWALOptions(t, tempDir)
		wal, _, err := Open(opts)
		require.NoError(t, err)

		originalActiveIndex := wal.ActiveSegmentIndex()
		require.Equal(t, uint64(1), originalActiveIndex)

		// Manually create a directory where the next segment file should be.
		// This will cause the call to os.OpenFile in CreateSegment to fail.
		nextSegmentPath := filepath.Join(tempDir, core.FormatSegmentFileName(originalActiveIndex+1))
		require.NoError(t, os.MkdirAll(nextSegmentPath, 0755))

		err = wal.Rotate()
		require.Error(t, err, "Rotate should fail when the next segment path is a directory")

		// Check that the state has not changed
		assert.Equal(t, originalActiveIndex, wal.ActiveSegmentIndex(), "Active segment index should not change on failure")
		assert.Len(t, wal.segmentIndexes, 1, "Segment indexes should not change on failure")

		require.NoError(t, wal.Close())
	})
}