package wal

import (
	"fmt"
	"io"
	"log/slog"
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