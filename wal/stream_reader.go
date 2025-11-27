package wal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/INLOpen/nexusbase/core"
)

// ErrNoNewEntries is returned by StreamReader.Next() during catch-up mode when it reaches the end
// of all closed segments. This is a signal to the caller to switch to tailing mode.
var ErrNoNewEntries = errors.New("no new WAL entries available in closed segments")

// streamReader implements the StreamReader interface.
type streamReader struct {
	wal *WAL // Reference to the parent WAL to access segments and lock

	currentSegmentReader *SegmentReader
	currentSegmentIndex  uint64
	lastReadSeqNum       uint64

	// entryBuffer holds entries from the last physical read or notification to serve them one by one.
	entryBuffer []core.WALEntry
	bufferIndex int

	logger       *slog.Logger
	registration *streamerRegistration
	isTailing    bool // Flag to indicate if we are reading live entries via notification
	lastNotifyID uint64
}

// Next returns the next WAL entry from the stream.
func (sr *streamReader) Next(ctx context.Context) (*core.WALEntry, error) {
	sr.logger.Debug("Next called", "isTailing", sr.isTailing, "bufferLen", len(sr.entryBuffer), "bufferIdx", sr.bufferIndex, "lastReadSeq", sr.lastReadSeqNum)
	for {
		// FIX: Check for cancellation at the start of every loop iteration.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			// Continue with the rest of the logic
		}

		// 1. Try to serve the next valid entry from the current buffer.
		if sr.bufferIndex < len(sr.entryBuffer) {
			entry := &sr.entryBuffer[sr.bufferIndex]
			sr.bufferIndex++
			sr.logger.Debug("Checking entry from buffer", "entrySeq", entry.SeqNum, "lastReadSeq", sr.lastReadSeqNum, "notify_id", sr.lastNotifyID)
			if entry.SeqNum > sr.lastReadSeqNum {
				sr.lastReadSeqNum = entry.SeqNum
				sr.logger.Info("Serving entry from buffer", "seqNum", entry.SeqNum, "notify_id", sr.lastNotifyID)
				return entry, nil
			}
			sr.logger.Debug("Skipping already seen entry in buffer", "seqNum", entry.SeqNum)
			continue // Skip already seen entry
		}

		// 2. If the buffer is exhausted, reset it.
		sr.entryBuffer = nil
		sr.bufferIndex = 0

		// 3. Decide whether to read from disk (catch-up) or wait for notification (tailing).
		if sr.isTailing {
			sr.logger.Debug("Stream reader is in tailing mode, waiting for notification...")
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case payload, ok := <-sr.registration.notifyC:
				if !ok {
					sr.logger.Info("Stream reader notification channel closed, ending stream.")
					return nil, io.EOF
				}
				batch := payload.entries
				sr.logger.Info("Stream reader received notification", "entries", len(batch), "notify_id", payload.notifyID)
				// Log the sequence numbers included for easier correlation with sender logs.
				if len(batch) > 0 {
					seqs := make([]uint64, 0, len(batch))
					for i := 0; i < len(batch) && i < 8; i++ {
						seqs = append(seqs, batch[i].SeqNum)
					}
					sr.logger.Info("Stream reader notification seqs (first N)", "seqs", seqs, "lastReadSeq", sr.lastReadSeqNum, "notify_id", payload.notifyID)
				}
				sr.entryBuffer = batch
				sr.lastNotifyID = payload.notifyID
				continue // Restart loop to process the new buffer
			}
		}

		// --- Catch-up Mode ---
		sr.wal.mu.Lock()
		sr.logger.Debug("In catch-up mode", "currentSegmentReader", sr.currentSegmentReader != nil)
		if sr.currentSegmentReader == nil {
			err := sr.openNextAvailableSegmentLocked()
			if err != nil {
				sr.wal.mu.Unlock()
				if errors.Is(err, ErrNoNewEntries) {
					// If we have successfully read from at least one segment before (i.e., currentSegmentIndex > 0),
					// it means we've finished the catch-up phase and should switch to tailing.
					// If we can't open a segment, it means we've read all the closed segments
					// and should switch to tailing mode to wait for new entries from the active segment.
					sr.logger.Debug("Stream reader finished catch-up, switching to tailing mode.")
					sr.isTailing = true
					return nil, ErrNoNewEntries
				}
				return nil, fmt.Errorf("stream reader failed to open next segment: %w", err)
			}
		}

		recordData, err := sr.currentSegmentReader.ReadRecord()
		sr.wal.mu.Unlock()

		if err != nil {
			if err == io.EOF {
				// We've successfully read a whole segment. This is a key part of the catch-up phase.
				// The next iteration will attempt to open the next segment, and if that fails with
				// ErrNoNewEntries, we'll know for sure that catch-up is complete.
				sr.logger.Debug("EOF on segment, closing and moving to next", "segmentIndex", sr.currentSegmentIndex)
				sr.wal.mu.Lock()
				sr.currentSegmentReader.Close()
				sr.currentSegmentReader = nil
				sr.wal.mu.Unlock()
				continue
			}
			return nil, fmt.Errorf("error reading WAL record from segment %d: %w", sr.currentSegmentIndex, err)
		}

		decodedEntries, err := decodeBatchRecord(recordData)
		sr.logger.Debug("Decoded batch from segment", "segmentIndex", sr.currentSegmentIndex, "entryCount", len(decodedEntries), "error", err)
		if err != nil {
			return nil, fmt.Errorf("error decoding batch record from segment %d: %w", sr.currentSegmentIndex, err)
		}
		sr.entryBuffer = decodedEntries
	}
}

// openNextAvailableSegmentLocked finds and opens the next segment file for reading.
// It will not open the currently active segment.
// Must be called with the WAL lock held.
func (sr *streamReader) openNextAvailableSegmentLocked() error {
	var segmentToOpen uint64

	if sr.currentSegmentIndex == 0 {
		if len(sr.wal.segmentIndexes) > 0 {
			segmentToOpen = sr.wal.segmentIndexes[0]
		} else {
			return ErrNoNewEntries
		}
	} else {
		nextKnownIndex := sr.findNextSegmentIndexLocked(sr.currentSegmentIndex)
		if nextKnownIndex == 0 {
			return ErrNoNewEntries
		}
		segmentToOpen = nextKnownIndex
	}

	if segmentToOpen >= sr.wal.activeSegmentIndexLocked() {
		return ErrNoNewEntries
	}

	path := filepath.Join(sr.wal.dir, core.FormatSegmentFileName(segmentToOpen))
	reader, err := OpenSegmentForRead(path)
	if err != nil {
		if os.IsNotExist(err) {
			return ErrNoNewEntries
		}
		return err
	}

	sr.logger.Debug("Stream reader opening segment for catch-up", "index", segmentToOpen)
	sr.currentSegmentReader = reader
	sr.currentSegmentIndex = segmentToOpen
	return nil
}

// findNextSegmentIndexLocked finds the index of the next segment to read.
func (sr *streamReader) findNextSegmentIndexLocked(currentIndex uint64) uint64 {
	for i, idx := range sr.wal.segmentIndexes {
		if idx == currentIndex && i+1 < len(sr.wal.segmentIndexes) {
			return sr.wal.segmentIndexes[i+1]
		}
	}
	return 0 // No next segment found
}

// Close releases resources held by the stream reader and unregisters it from the WAL.
func (sr *streamReader) Close() error {
	sr.wal.unregisterStreamer(sr.registration)

	sr.wal.mu.Lock()
	defer sr.wal.mu.Unlock()

	if sr.currentSegmentReader != nil {
		return sr.currentSegmentReader.Close()
	}
	return nil
}
