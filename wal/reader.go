package wal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/INLOpen/nexusbase/core"
)

// walReader implements the WALReader interface for streaming WAL entries.
type walReader struct {
	wal         *WAL
	nextSeqNum  uint64
	entryBuffer []core.WALEntry

	currentSegReader *SegmentReader
	currentSegIndex  uint64

	mu         sync.Mutex
	closeCh    chan struct{}
	cancelOnce sync.Once
}

// OpenReader creates a new WAL reader starting from a given sequence number.
// The reader will start from the oldest available segment and scan forward.
func (w *WAL) OpenReader(fromSeqNum uint64) (WALReader, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.segmentIndexes) == 0 {
		// This can happen if the WAL is new and no data has been written yet.
		// The reader will just wait for the first entry.
		w.logger.Info("Opening WAL reader on an empty WAL, will wait for data", "from_seq_num", fromSeqNum)
	}

	r := &walReader{
		wal:        w,
		nextSeqNum: fromSeqNum,
		closeCh:    make(chan struct{}),
	}

	return r, nil
}

// Next returns the next available WAL entry. It blocks if no new entries are
// available, until an entry is written, the context is cancelled, or the reader is closed.
func (r *walReader) Next(ctx context.Context) (*core.WALEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for {
		// Check for cancellation or closed reader first.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-r.closeCh:
			return nil, io.EOF
		default:
		}

		// 1. Process buffered entries first.
		if len(r.entryBuffer) > 0 {
			entry := r.entryBuffer[0]
			r.entryBuffer = r.entryBuffer[1:] // Consume entry
			if entry.SeqNum >= r.nextSeqNum {
				r.nextSeqNum = entry.SeqNum + 1
				return &entry, nil
			}
			// Stale entry, loop to get the next one from the buffer.
			continue
		}

		// 2. Buffer is empty, ensure we have a segment reader.
		if r.currentSegReader == nil {
			err := r.openNextSegment()
			if err != nil {
				return nil, err // Propagate error (e.g., segment not found)
			}
			if r.currentSegReader == nil { // No error, but no segment opened (at the tip)
				if err := r.waitForData(ctx); err != nil {
					return nil, err
				}
				continue // After waiting, loop again to try opening a segment.
			}
		}

		// 3. Read a new record from the current segment.
		recordData, err := r.currentSegReader.ReadRecord()
		if err == nil {
			entries, decErr := decodeBatchRecord(recordData)
			if decErr != nil {
				r.wal.logger.Error("Failed to decode WAL batch record during streaming", "error", decErr, "segment", r.currentSegIndex)
				// This is a corruption error. We should stop.
				return nil, decErr
			}
			// Add segment index to all entries in the batch
			for i := range entries {
				entries[i].SegmentIndex = r.currentSegIndex
			}
			r.entryBuffer = entries
			continue // Loop to process the newly filled buffer.
		}

		// 4. Handle read error.
		if errors.Is(err, io.EOF) {
			// If we hit EOF on the *active* segment, wait for new data.
			if r.wal.activeSegment != nil && r.currentSegReader != nil && r.currentSegReader.index == r.wal.activeSegment.index {
				if waitErr := r.waitForData(ctx); waitErr != nil {
					return nil, waitErr // Context cancelled or reader closed
				}
				continue // After waiting, loop again to retry reading from the same active segment.
			}
			// Otherwise, it's a non-active segment. Close it and try to open the next one.
			r.currentSegReader.Close()
			r.currentSegReader = nil
			continue
		}

		// Any other error is fatal for this reader.
		r.wal.logger.Error("Unrecoverable error reading from WAL segment", "error", err, "segment", r.currentSegIndex)
		return nil, err
	}
}

// openNextSegment tries to open the next available WAL segment for reading.
// It returns true if a new segment was successfully opened.
// It returns an error if a segment is expected but not found.
// MUST be called with the reader's lock held.
func (r *walReader) openNextSegment() error {
	r.wal.mu.Lock()
	defer r.wal.mu.Unlock()

	var nextSegIndex uint64
	if r.currentSegIndex == 0 {
		// First time opening, start from the oldest segment.
		if len(r.wal.segmentIndexes) > 0 {
			nextSegIndex = r.wal.segmentIndexes[0]
		}
	} else {
		// Find the segment that comes after the current one.
		for i, index := range r.wal.segmentIndexes {
			if index == r.currentSegIndex && i+1 < len(r.wal.segmentIndexes) {
				nextSegIndex = r.wal.segmentIndexes[i+1]
				break
			}
		}
	}

	if nextSegIndex == 0 {
		return nil // No more segments to open, we are at the tip.
	}

	path := filepath.Join(r.wal.dir, core.FormatSegmentFileName(nextSegIndex))
	segReader, err := OpenSegmentForRead(path)
	if err != nil {
		if os.IsNotExist(err) {
			// This is a critical error for the reader. The segment is gone.
			return fmt.Errorf("segment %d not found, likely purged: %w", nextSegIndex, err)
		}
		r.wal.logger.Error("Failed to open next segment for reading", "segment", nextSegIndex, "error", err)
		return err
	}

	r.wal.logger.Debug("WAL reader opened new segment", "segment", nextSegIndex)
	r.currentSegReader = segReader
	r.currentSegIndex = nextSegIndex
	return nil
}

// waitForData blocks until new data is written to the WAL or the context is cancelled.
// MUST be called WITHOUT the reader's lock held.
func (r *walReader) waitForData(ctx context.Context) error {
	// This is a pattern to wait on a sync.Cond with context cancellation.
	waitDone := make(chan struct{})
	go func() {
		r.wal.mu.Lock()
		r.wal.readerCond.Wait()
		r.wal.mu.Unlock()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		return nil // Woken up by a new write.
	case <-ctx.Done():
		// Context was cancelled. We need to wake up our waiting goroutine.
		r.wal.readerCond.Broadcast() // This wakes up all waiters, including ours.
		<-waitDone                   // Wait for the goroutine to exit.
		return ctx.Err()
	case <-r.closeCh:
		// Reader was closed.
		r.wal.readerCond.Broadcast()
		<-waitDone
		return io.EOF
	}
}

// Close stops the reader and releases its resources.
func (r *walReader) Close() error {
	r.cancelOnce.Do(func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		close(r.closeCh)
		if r.currentSegReader != nil {
			r.currentSegReader.Close()
			r.currentSegReader = nil
		}
	})
	return nil
}