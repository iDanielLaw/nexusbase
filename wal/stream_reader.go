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

	logger *slog.Logger
}

// Next returns the next WAL entry from the stream.
func (sr *streamReader) Next() (*core.WALEntry, error) {
	// This method needs to be protected by the WAL's mutex because it accesses
	// shared segment information and file handles.
	sr.wal.mu.Lock()
	defer sr.wal.mu.Unlock()

	for {
		// If there's no current segment reader, try to open one.
		if sr.currentSegmentReader == nil {
			err := sr.openNextSegmentLocked()
			if err != nil {
				// If we can't open the next segment and it's the active one,
				// it might just not have any new data yet.
				if errors.Is(err, os.ErrNotExist) && sr.currentSegmentIndex >= sr.wal.ActiveSegmentIndex() {
					return nil, ErrNoNewEntries
				}
				return nil, fmt.Errorf("stream reader failed to open next segment: %w", err)
			}
		}

		// Try to read the next record from the current segment.
		recordData, err := sr.currentSegmentReader.ReadRecord()
		if err != nil {
			// If we hit the end of the current segment file...
			if err == io.EOF {
				sr.currentSegmentReader.Close()
				sr.currentSegmentReader = nil
				// ...loop again to try opening the next segment.
				continue
			}
			// For any other read error, it's fatal for this stream.
			return nil, fmt.Errorf("error reading WAL record from segment %d: %w", sr.currentSegmentIndex, err)
		}

		// We have a record, now decode the batch.
		entries, err := decodeBatchRecord(recordData)
		if err != nil {
			return nil, fmt.Errorf("error decoding batch record from segment %d: %w", sr.currentSegmentIndex, err)
		}

		// Find the first entry in the batch that we haven't processed yet.
		for _, entry := range entries {
			if entry.SeqNum > sr.lastReadSeqNum {
				sr.lastReadSeqNum = entry.SeqNum
				return &entry, nil
			}
		}

		// If all entries in this batch were already processed, continue to the next record.
	}
}

// openNextSegmentLocked finds and opens the next segment file in sequence for reading.
// Must be called with the WAL lock held.
func (sr *streamReader) openNextSegmentLocked() error {
	// TODO: Implement logic to find the correct starting segment based on sr.lastReadSeqNum.
	// For now, we just advance to the next available segment file.
	nextIndex := sr.findNextSegmentIndexLocked(sr.currentSegmentIndex)
	if nextIndex == 0 {
		return os.ErrNotExist // No more segments to read.
	}

	path := filepath.Join(sr.wal.dir, core.FormatSegmentFileName(nextIndex))
	reader, err := OpenSegmentForRead(path)
	if err != nil {
		return err
	}

	sr.logger.Debug("Stream reader opening new segment", "index", nextIndex)
	sr.currentSegmentReader = reader
	sr.currentSegmentIndex = nextIndex
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
