package wal

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/INLOpen/nexusbase/core"
)

// ErrNoNewEntries is returned by StreamReader.Next() when it reaches the end
// of the current active segment and no new entries have been written yet.
// The caller should wait and retry.
var ErrNoNewEntries = errors.New("no new WAL entries available")

// streamReader implements the StreamReader interface.
type streamReader struct {
	wal *WAL // Reference to the parent WAL to access segments and lock

	currentSegmentReader *SegmentReader
	currentSegmentIndex  uint64
	lastReadSeqNum       uint64

	// entryBuffer holds entries from the last physical read to serve them one by one.
	entryBuffer []core.WALEntry
	bufferIndex int

	logger *slog.Logger
}

// Next returns the next WAL entry from the stream.
func (sr *streamReader) Next() (*core.WALEntry, error) {
	// This method needs to be protected by the WAL's mutex because it accesses
	// shared segment information and file handles.
	sr.wal.mu.Lock()
	defer sr.wal.mu.Unlock()

	for {
		// 1. Try to serve the next valid entry from the current buffer.
		if sr.bufferIndex < len(sr.entryBuffer) {
			entry := &sr.entryBuffer[sr.bufferIndex]
			sr.bufferIndex++
			if entry.SeqNum > sr.lastReadSeqNum {
				sr.lastReadSeqNum = entry.SeqNum
				return entry, nil
			}
			// This entry was already seen (e.g., when starting from a specific SeqNum),
			// so try the next one in the buffer.
			continue
		}

		// 2. If the buffer is exhausted, reset it and prepare to read from disk.
		sr.entryBuffer = nil
		sr.bufferIndex = 0

		// If there's no current segment reader, try to open one.
		if sr.currentSegmentReader == nil {
			err := sr.openNextAvailableSegmentLocked()
			if err != nil {
				// If the helper function signals no new entries, propagate it.
				if errors.Is(err, ErrNoNewEntries) {
					return nil, ErrNoNewEntries
				}
				// Otherwise, it's a real error.
				return nil, fmt.Errorf("stream reader failed to open next segment: %w", err)
			}
		}

		// Try to read the next record from the current segment.
		recordData, err := sr.currentSegmentReader.ReadRecord()
		if err != nil {
			// If we hit the end of the current segment file...
			if err == io.EOF {
				sr.currentSegmentReader.Close()
				// On Windows, file handles might not be released immediately, which can cause
				// locking issues if another part of the system (like the WAL writer) holds
				// a handle to the same file. Calling a GC cycle can help expedite the
				// release of the underlying OS handle, preventing a timeout on Close().
				// sys.GC()
				sr.currentSegmentReader = nil
				// ...loop again to try opening the next segment.
				continue
			}
			// For any other read error, it's fatal for this stream.
			return nil, fmt.Errorf("error reading WAL record from segment %d: %w", sr.currentSegmentIndex, err)
		}

		// We have a record, now decode the batch.
		decodedEntries, err := decodeBatchRecord(recordData)
		if err != nil {
			return nil, fmt.Errorf("error decoding batch record from segment %d: %w", sr.currentSegmentIndex, err)
		}
		sr.entryBuffer = decodedEntries
		// The loop will now restart and serve entries from the newly populated buffer.
	}
}

// openNextSegmentLocked finds and opens the next segment file in sequence for reading.
// Must be called with the WAL lock held.
func (sr *streamReader) openNextAvailableSegmentLocked() error {
	var segmentToOpen uint64

	if sr.currentSegmentIndex == 0 {
		// First time opening. Find the first segment to read.
		if len(sr.wal.segmentIndexes) > 0 {
			segmentToOpen = sr.wal.segmentIndexes[0]
		} else {
			return ErrNoNewEntries // No segments exist at all.
		}
	} else {
		// We have finished a previous segment, find the next one in the list.
		nextKnownIndex := sr.findNextSegmentIndexLocked(sr.currentSegmentIndex)
		if nextKnownIndex == 0 {
			// We've read all known segments. There are no more closed segments to read.
			return ErrNoNewEntries
		}
		segmentToOpen = nextKnownIndex
	}

	// CRITICAL CHECK: Do not attempt to open the segment that is currently active for writing.
	// If the segment we are about to open is the active one, it means we have caught up
	// to the writer. We should wait for it to be rotated and closed.
	if segmentToOpen >= sr.wal.activeSegmentIndexLocked() {
		return ErrNoNewEntries
	}

	path := filepath.Join(sr.wal.dir, core.FormatSegmentFileName(segmentToOpen))
	reader, err := OpenSegmentForRead(path)
	if err != nil {
		// This could happen if a segment was purged between listing and opening.
		// Treat it as if there are no new entries for now.
		if os.IsNotExist(err) {
			return ErrNoNewEntries
		}
		return err
	}

	sr.logger.Debug("Stream reader opening segment", "index", segmentToOpen)
	sr.currentSegmentReader = reader
	sr.currentSegmentIndex = segmentToOpen
	return nil
}

// findNextSegmentIndexLocked finds the index of the next segment to read.
func (sr *streamReader) findNextSegmentIndexLocked(currentIndex uint64) uint64 {
	if currentIndex == 0 { // First call
		if len(sr.wal.segmentIndexes) > 0 {
			return sr.wal.segmentIndexes[0]
		}
		return 0
	}
	// Find the next index in the sorted list
	for i, idx := range sr.wal.segmentIndexes {
		if idx == currentIndex && i+1 < len(sr.wal.segmentIndexes) {
			return sr.wal.segmentIndexes[i+1]
		}
	}
	return 0 // No next segment found
}

// Close releases resources held by the stream reader.
func (sr *streamReader) Close() error {
	if sr.currentSegmentReader != nil {
		return sr.currentSegmentReader.Close()
	}
	return nil
}
