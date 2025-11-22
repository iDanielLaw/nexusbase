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

	"github.com/INLOpen/nexusbase/sys"

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

	allEntries := createTestWALEntries(10, 1)
	require.NoError(t, wal.AppendBatch(allEntries))
	require.NoError(t, wal.Close())

	// 2. Re-open the WAL to test reading from the created files
	wal, _, err = Open(opts)
	require.NoError(t, err)
	defer wal.Close()

	// 3. Create a stream reader starting from the beginning (SeqNum 1)
	reader, err := wal.NewStreamReader(1)
	require.NoError(t, err)
	defer reader.Close()

	// 4. Read all entries and verify
	for i, expected := range allEntries {
		entry, err := reader.Next(context.Background())
		require.NoError(t, err, "Next() should not fail for entry %d", i+1)
		require.NotNil(t, entry)
		assert.Equal(t, expected.SeqNum, entry.SeqNum)
		assert.Equal(t, expected.Key, entry.Key)
		assert.Equal(t, expected.Value, entry.Value)
	}

	// 5. Verify that the next call returns ErrNoNewEntries
	_, err = reader.Next(context.Background())
	assert.ErrorIs(t, err, ErrNoNewEntries)
}

// TestStreamReader_Next_StartFromMiddle tests starting a stream from a specific sequence number.
func TestStreamReader_Next_StartFromMiddle(t *testing.T) {
	tempDir := t.TempDir()
	opts := testWALOptions(t, tempDir)

	// 1. Create a WAL and write 10 entries, then close it to make them non-active
	wal, _, err := Open(opts)
	require.NoError(t, err)
	allEntries := createTestWALEntries(10, 1)
	require.NoError(t, wal.AppendBatch(allEntries))
	require.NoError(t, wal.Close())

	// 2. Re-open the WAL
	wal, _, err = Open(opts)
	require.NoError(t, err)
	defer wal.Close()

	// 3. Create a stream reader starting from sequence number 5.
	// It should return entries 5, 6, 7, 8, 9, 10.
	startSeqNum := uint64(5)
	reader, err := wal.NewStreamReader(startSeqNum)
	require.NoError(t, err)
	defer reader.Close()

	// 4. Read the remaining entries and verify
	for i := int(startSeqNum) - 1; i < len(allEntries); i++ {
		expected := allEntries[i]
		entry, err := reader.Next(context.Background())
		require.NoError(t, err, "Next() should not fail for expected entry %d", expected.SeqNum)
		require.NotNil(t, entry)
		assert.Equal(t, expected.SeqNum, entry.SeqNum)
		assert.Equal(t, expected.Key, entry.Key)
	}

	// 5. Verify that the next call returns ErrNoNewEntries
	_, err = reader.Next(context.Background())
	assert.ErrorIs(t, err, ErrNoNewEntries)
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

	// 2. Create a stream reader starting from the beginning.
	reader, err := wal.NewStreamReader(1)
	require.NoError(t, err)
	defer reader.Close()

	// 3. Read all initial entries
	for i := 0; i < len(initialEntries); i++ {
		entry, err := reader.Next(context.Background())
		require.NoError(t, err, "Failed to read initial entry %d", i)
		require.NotNil(t, entry)
		assert.Equal(t, initialEntries[i].SeqNum, entry.SeqNum)
	}

	// 4. The next call should indicate no new entries, and the reader should switch to tailing mode internally.
	_, err = reader.Next(context.Background())
	require.ErrorIs(t, err, ErrNoNewEntries)

	// 5. Append a new entry to the WAL while the reader is active
	newEntry := core.WALEntry{
		EntryType: core.EntryTypePutEvent,
		Key:       []byte("new-live-key"),
		Value:     []byte("live-value"),
		SeqNum:    uint64(len(initialEntries) + 1),
	}

	// Use a WaitGroup to ensure the reader is blocking before we write
	var wg sync.WaitGroup
	wg.Add(1)

	var readEntry *core.WALEntry
	var readErr error

	go func() {
		defer wg.Done()
		// This call will block until the Append below happens.
		readEntry, readErr = reader.Next(context.Background())
	}()

	// Give the reader goroutine a moment to start and block in Next()
	time.Sleep(50 * time.Millisecond)

	require.NoError(t, wal.Append(newEntry))

	// Wait for the reader to finish processing the new entry
	wg.Wait()

	// 6. Check the results from the reader goroutine
	require.NoError(t, readErr, "Next() should have found the new entry from notification")
	require.NotNil(t, readEntry)
	assert.Equal(t, newEntry.SeqNum, readEntry.SeqNum)
	assert.Equal(t, newEntry.Key, readEntry.Key)

	// 7. And now it should be waiting for the next notification.
	// We can test this with a timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = reader.Next(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded, "Next() should block waiting for new entries")
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
	reader, err := wal.NewStreamReader(1)
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
			entry, err := reader.Next(ctx)
			// Check for cancellation signal from the main test goroutine.
			select {
			case <-ctx.Done():
				return // Stop reading if context is cancelled.
			default:
			}
			if err != nil {
				if errors.Is(err, ErrNoNewEntries) || errors.Is(err, context.DeadlineExceeded) {
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

	_, err = reader.Next(context.Background())
	assert.ErrorIs(t, err, ErrNoNewEntries)
}

// TestStreamReader_ContextCancellation tests that the reader stops when the context is cancelled.
func TestStreamReader_ContextCancellation(t *testing.T) {
	tempDir := t.TempDir()
	opts := testWALOptions(t, tempDir)

	wal, _, err := Open(opts)
	require.NoError(t, err)

	reader, err := wal.NewStreamReader(0)
	require.NoError(t, err)
	defer reader.Close()

	// To properly test cancellation of a *blocking* call, we must first
	// get the reader into a state where it will block. This happens when it
	// enters "tailing" mode. The current logic requires it to have read at least
	// one segment before it will switch to tailing.

	// 1. Write an entry and rotate to create a closed segment.
	require.NoError(t, wal.Append(core.WALEntry{SeqNum: 1, Key: []byte("setup")}))
	require.NoError(t, wal.Rotate())

	// 2. Read the first entry to advance the reader past the closed segment.
	_, err = reader.Next(context.Background())
	require.NoError(t, err)

	// 3. The next call should return ErrNoNewEntries and switch the reader to tailing mode.
	_, err = reader.Next(context.Background())
	require.ErrorIs(t, err, ErrNoNewEntries)

	// 4. Now, the reader is in tailing mode. The next call to Next() will block.
	// We can test cancellation on this blocking call.
	ctx, cancelFunc := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)

	// This goroutine will call Next() and block, waiting for an entry.
	go func() {
		defer wg.Done()
		_, err = reader.Next(ctx)
		assert.ErrorIs(t, err, context.Canceled, "Next() should return context.Canceled")
	}()

	// Let the goroutine start and block inside Next()
	time.Sleep(50 * time.Millisecond)

	cancelFunc()
	wg.Wait()
	wal.Close()
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
	require.NoError(t, sys.Remove(segment1Path))
	require.NoError(t, os.Mkdir(segment1Path, 0755))

	// Act: Call Next(), which should trigger openNextAvailableSegmentLocked and fail.
	_, err = sr.Next(context.Background())

	// Assert: Check for a genuine error, not ErrNoNewEntries
	require.Error(t, err, "Next() should return an error")
	assert.NotErrorIs(t, err, ErrNoNewEntries, "Error should be a file system error, not ErrNoNewEntries")
}
