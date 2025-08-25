package wal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStreamReader_Next_Success tests reading all entries from a multi-segment WAL.
func TestStreamReader_Next_Success(t *testing.T) {
	tempDir := t.TempDir()
	opts := testWALOptions(t, tempDir)
	opts.MaxSegmentSize = 256 // Small size to force rotation

	// 1. Create a WAL and write entries across multiple segments
	wal, _, err := Open(opts)
	require.NoError(t, err)

	var allEntries []core.WALEntry
	for i := 1; i <= 10; i++ {
		entry := core.WALEntry{
			EntryType: core.EntryTypePutEvent,
			Key:       []byte(fmt.Sprintf("key-%d", i)),
			Value:     []byte("some-value-to-ensure-rotation-happens-eventually"),
			SeqNum:    uint64(i),
		}
		require.NoError(t, wal.Append(entry))
		allEntries = append(allEntries, entry)
	}
	require.NoError(t, wal.Close())

	// 2. Re-open the WAL to test reading from the created files
	wal, _, err = Open(opts)
	require.NoError(t, err)
	defer wal.Close()

	// 3. Create a stream reader starting from the beginning
	reader, err := wal.NewStreamReader(0)
	require.NoError(t, err)
	defer reader.Close()

	// 4. Read all entries and verify
	for i, expected := range allEntries {
		// To make the test robust, we must rotate the WAL to ensure the last segment is closed and readable.
		wal.Rotate()
		entry, err := reader.Next()
		require.NoError(t, err, "Next() should not fail for entry %d", i+1)
		require.NotNil(t, entry)
		assert.Equal(t, expected.SeqNum, entry.SeqNum)
		assert.Equal(t, expected.Key, entry.Key)
		assert.Equal(t, expected.Value, entry.Value)
	}

	// 5. Verify that the next call returns ErrNoNewEntries
	_, err = reader.Next()
	assert.ErrorIs(t, err, ErrNoNewEntries)
}

// TestStreamReader_Next_StartFromMiddle tests starting a stream from a specific sequence number.
func TestStreamReader_Next_StartFromMiddle(t *testing.T) {
	tempDir := t.TempDir()
	opts := testWALOptions(t, tempDir)

	// 1. Create a WAL and write 10 entries
	wal, _, err := Open(opts)
	require.NoError(t, err)
	allEntries := createTestWALEntries(10, 1)
	require.NoError(t, wal.AppendBatch(allEntries))
	require.NoError(t, wal.Close())

	// 2. Re-open the WAL
	wal, _, err = Open(opts)
	require.NoError(t, err)
	defer wal.Close()

	// 3. Create a stream reader starting from sequence number 5
	// It should start reading from SeqNum 6.
	startSeqNum := uint64(5)
	reader, err := wal.NewStreamReader(startSeqNum)
	require.NoError(t, err)
	defer reader.Close()

	// 4. Read the remaining entries and verify
	for i := int(startSeqNum); i < len(allEntries); i++ {
		// To make the test robust, we must rotate the WAL to ensure the last segment is closed and readable.
		wal.Rotate()
		expected := allEntries[i]
		entry, err := reader.Next()
		require.NoError(t, err, "Next() should not fail for entry %d", i+1)
		require.NotNil(t, entry)
		assert.Equal(t, expected.SeqNum, entry.SeqNum)
		assert.Equal(t, expected.Key, entry.Key)
	}

	// 5. Verify that the next call returns ErrNoNewEntries
	_, err = reader.Next()
	assert.ErrorIs(t, err, ErrNoNewEntries)
}

// TestStreamReader_ReadsFromActiveSegment tests that the reader can stream entries
// after a segment is rotated.
func TestStreamReader_ReadsFromActiveSegment(t *testing.T) {
	tempDir := t.TempDir()
	opts := testWALOptions(t, tempDir)

	// 1. Create a WAL
	wal, _, err := Open(opts)
	require.NoError(t, err)
	defer wal.Close()

	// 2. Create a stream reader
	reader, err := wal.NewStreamReader(0)
	require.NoError(t, err)
	defer reader.Close()

	// 3. Initially, there should be no entries
	_, err = reader.Next()
	require.ErrorIs(t, err, ErrNoNewEntries, "Should be no entries in a new WAL")

	// 4. Append an entry to the active WAL
	entry1 := core.WALEntry{Key: []byte("key1"), SeqNum: 1}
	require.NoError(t, wal.Append(entry1))

	// 5. The reader should still see no new entries because the segment is active
	_, err = reader.Next()
	require.ErrorIs(t, err, ErrNoNewEntries, "Should not read from active segment")

	// 6. Rotate the WAL to make the segment readable
	require.NoError(t, wal.Rotate())

	// 7. Now, the reader should be able to read the new entry
	readEntry1, err := reader.Next()
	require.NoError(t, err, "Should be able to read after rotation")
	require.NotNil(t, readEntry1)
	assert.Equal(t, entry1.SeqNum, readEntry1.SeqNum)
	assert.Equal(t, entry1.Key, readEntry1.Key)

	// 8. There should be no more entries
	_, err = reader.Next()
	require.ErrorIs(t, err, ErrNoNewEntries, "Should be no more entries after reading the first one")
}

// TestStreamReader_Next_BlocksAndResumes tests the "tailing" functionality of the stream reader.
func TestStreamReader_Next_BlocksAndResumes(t *testing.T) {
	tempDir := t.TempDir()
	opts := testWALOptions(t, tempDir)

	// 1. Create a WAL and write some initial entries
	wal, _, err := Open(opts)
	require.NoError(t, err)
	defer wal.Close()

	initialEntries := createTestWALEntries(3, 1)
	require.NoError(t, wal.AppendBatch(initialEntries))

	// Force a rotation so the initial entries are in a closed segment that the reader can access.
	require.NoError(t, wal.Rotate())

	// 2. Create a stream reader
	reader, err := wal.NewStreamReader(0)
	require.NoError(t, err)
	defer reader.Close()

	// 3. Read all initial entries
	for i := 0; i < len(initialEntries); i++ {
		entry, err := reader.Next()
		require.NoError(t, err, "Failed to read initial entry %d", i)
		require.NotNil(t, entry)
		assert.Equal(t, initialEntries[i].SeqNum, entry.SeqNum)
	}

	// 4. The next call should indicate no new entries
	_, err = reader.Next()
	require.ErrorIs(t, err, ErrNoNewEntries)

	// 5. Append a new entry to the WAL while the reader is active
	newEntry := core.WALEntry{
		EntryType: core.EntryTypePutEvent,
		Key:       []byte("new-live-key"),
		Value:     []byte("live-value"),
		SeqNum:    uint64(len(initialEntries) + 1),
	}
	require.NoError(t, wal.Append(newEntry))

	// Force another rotation to make the newly appended entry readable by closing the active segment.
	require.NoError(t, wal.Rotate())

	// 6. The next call to Next() should now return the new entry
	entry, err := reader.Next()
	require.NoError(t, err, "Next() should have found the new entry after rotation")
	require.NotNil(t, entry)
	assert.Equal(t, newEntry.SeqNum, entry.SeqNum)
	assert.Equal(t, newEntry.Key, entry.Key)

	// 7. And now it should be empty again
	_, err = reader.Next()
	require.ErrorIs(t, err, ErrNoNewEntries)
}

// TestStreamReader_ConcurrentRotation tests the reader's ability to handle
// WAL rotations that happen concurrently while it is reading.
func TestStreamReader_ConcurrentRotation(t *testing.T) {
	tempDir := t.TempDir()
	opts := testWALOptions(t, tempDir)
	opts.MaxSegmentSize = 1024 // Small size to force rotations

	// 1. Setup WAL and initial data
	wal, _, err := Open(opts)
	require.NoError(t, err)
	defer wal.Close()

	// Create a stream reader before any writes
	reader, err := wal.NewStreamReader(0)
	require.NoError(t, err)
	defer reader.Close()

	var wg sync.WaitGroup
	var mu sync.Mutex // To protect access to readEntries
	var readEntries []*core.WALEntry
	var writerErr, readerErr error
	totalEntriesToWrite := 50

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 2. Start the reader goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < totalEntriesToWrite; i++ {
			entry, err := reader.Next()
			// Check for cancellation signal from the main test goroutine.
			select {
			case <-ctx.Done():
				return // Stop reading if context is cancelled.
			default:
			}
			if err != nil {
				if errors.Is(err, ErrNoNewEntries) {
					// This is expected, wait and retry
					time.Sleep(10 * time.Millisecond)
					i-- // Decrement counter to retry reading this entry index
					continue
				}
				// A real error occurred
				mu.Lock()
				readerErr = err
				mu.Unlock()
				cancel() // Stop the writer
				return
			}
			mu.Lock()
			readEntries = append(readEntries, entry)
			mu.Unlock()
		}
	}()

	// 3. Start the writer logic in the main test goroutine
	var seqNum uint64 = 0
WriterLoop:
	for i := 0; i < totalEntriesToWrite; i++ {
		// Check for cancellation signal from the reader goroutine.
		select {
		case <-ctx.Done():
			writerErr = fmt.Errorf("writer stopped due to context cancellation: %w", ctx.Err())
			break WriterLoop
		default:
			// Continue with the write operation.
		}

		seqNum++
		entry := core.WALEntry{
			EntryType: core.EntryTypePutEvent,
			Key:       []byte(fmt.Sprintf("key-%d", seqNum)),
			Value:     []byte("some-value-to-ensure-rotation-happens-eventually-and-this-is-it"),
			SeqNum:    seqNum,
		}
		writerErr = wal.Append(entry)
		require.NoError(t, writerErr)
		time.Sleep(2 * time.Millisecond) // Give the reader a chance to process
	}
	require.NoError(t, wal.Rotate()) // Final rotation to close the last segment

	// 4. Wait for the reader to finish
	wg.Wait()

	// 5. Verification
	require.NoError(t, writerErr, "Writer should not have failed")
	require.NoError(t, readerErr, "Reader should not have failed")
	require.Len(t, readEntries, totalEntriesToWrite, "Reader should have read all written entries")
	for i := 0; i < totalEntriesToWrite; i++ {
		expectedSeqNum := uint64(i + 1)
		assert.Equal(t, expectedSeqNum, readEntries[i].SeqNum, "Sequence number mismatch at index %d", i)
	}
}

// TestStreamReader_EmptyWAL tests behavior with an empty WAL.
func TestStreamReader_EmptyWAL(t *testing.T) {
	tempDir := t.TempDir()
	opts := testWALOptions(t, tempDir)

	wal, _, err := Open(opts)
	require.NoError(t, err)
	defer wal.Close()

	reader, err := wal.NewStreamReader(0)
	require.NoError(t, err)
	defer reader.Close()

	_, err = reader.Next()
	assert.ErrorIs(t, err, ErrNoNewEntries)
}

// TestStreamReader_ContextCancellation tests that the reader stops when the context is cancelled.
// This is a bit tricky to test directly as the current `Next()` implementation doesn't take a context.
// The check happens in the gRPC server loop. This test will simulate that loop.
func TestStreamReader_ContextCancellation(t *testing.T) {
	tempDir := t.TempDir()
	opts := testWALOptions(t, tempDir)

	wal, _, err := Open(opts)
	require.NoError(t, err)
	defer wal.Close()

	reader, err := wal.NewStreamReader(0)
	require.NoError(t, err)
	defer reader.Close()

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				// Context was cancelled, exit the loop
				return
			default:
				_, err := reader.Next()
				if errors.Is(err, ErrNoNewEntries) {
					// In a real scenario, we'd wait before retrying.
					// For the test, a short sleep is fine.
					time.Sleep(10 * time.Millisecond)
					continue
				}
				// If we get a real error or a value, something is wrong with the test setup.
				if err != nil {
					t.Errorf("Unexpected error from reader.Next(): %v", err)
				} else {
					t.Error("Unexpectedly received an entry from the reader")
				}
				return
			}
		}
	}()

	// Let the goroutine run for a bit
	time.Sleep(50 * time.Millisecond)

	// Cancel the context
	cancel()

	// Wait for the goroutine to finish
	wg.Wait()

	// If we reach here without the test timing out or erroring, it means
	// the goroutine successfully exited upon context cancellation.
}

func TestStreamReader_OpenSegmentError(t *testing.T) {
	// Setup: Create a WAL with 2 segments
	tempDir := t.TempDir()
	opts := testWALOptions(t, tempDir)
	wal, _, err := Open(opts)
	require.NoError(t, err)
	defer wal.Close()

	// Write to segment 1 and rotate to create segment 2
	require.NoError(t, wal.Append(core.WALEntry{Key: []byte("a"), SeqNum: 1}))
	require.NoError(t, wal.Rotate())
	require.NoError(t, wal.Append(core.WALEntry{Key: []byte("b"), SeqNum: 2}))
	require.Equal(t, uint64(2), wal.ActiveSegmentIndex(), "Active segment should be 2")

	// Create a stream reader. It will know about segments 1 and 2.
	sr, err := wal.NewStreamReader(0)
	require.NoError(t, err)
	defer sr.Close()

	// Corrupt the setup AFTER the reader has been created:
	// Replace the first segment file with a directory.
	segment1Path := filepath.Join(tempDir, core.FormatSegmentFileName(1))
	require.NoError(t, os.Remove(segment1Path))
	require.NoError(t, os.Mkdir(segment1Path, 0755))

	// Act: Call Next(), which should trigger openNextAvailableSegmentLocked and fail.
	_, err = sr.Next()

	// Assert: Check for a genuine error, not ErrNoNewEntries
	require.Error(t, err, "Next() should return an error")
	assert.NotErrorIs(t, err, ErrNoNewEntries, "Error should be a file system error, not ErrNoNewEntries")
}