package wal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"expvar"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
)

// WALSyncMode defines how frequently the WAL is synced to disk.
type WALSyncMode string

const (
	SyncAlways   WALSyncMode = "always"   // Sync after every append (highest durability, lowest performance)
	SyncInterval WALSyncMode = "interval" // Sync periodically (not handled by WAL anymore, but by engine)
	SyncDisabled WALSyncMode = "disabled" // No sync (for testing/benchmarking, high risk of data loss)
)

// WAL (Write-Ahead Log) provides durability by logging operations before they are applied to memtable.
// It manages a directory of segment files.
type WAL struct {
	dir  string
	mu   sync.Mutex
	opts Options

	activeSegment  *SegmentWriter
	segmentIndexes []uint64

	metricsBytesWritten   *expvar.Int
	metricsEntriesWritten *expvar.Int

	logger      *slog.Logger
	hookManager hooks.HookManager

	testingOnlyInjectCloseError  error
	testingOnlyInjectAppendError error
}

var _ WALInterface = (*WAL)(nil)

// Options holds configuration for the WAL.
type Options struct {
	Dir            string
	SyncMode       WALSyncMode
	MaxSegmentSize int64
	BytesWritten   *expvar.Int
	EntriesWritten *expvar.Int
	Logger         *slog.Logger
	// StartRecoveryIndex tells the WAL to only recover entries from segments with an index greater than this value.
	StartRecoveryIndex uint64
	HookManager        hooks.HookManager
}

// Open creates or opens a WAL directory.
// It recovers entries from existing segments and prepares for appending.
func Open(opts Options) (*WAL, []core.WALEntry, error) {
	if opts.Logger == nil {
		opts.Logger = slog.Default().With("component", "WAL_default")
	} else {
		opts.Logger = opts.Logger.With("component", "WAL")
	}
	if opts.MaxSegmentSize == 0 {
		opts.MaxSegmentSize = MaxSegmentSize
	}

	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, nil, fmt.Errorf("failed to create WAL directory %s: %w", opts.Dir, err)
	}

	w := &WAL{
		dir:                   opts.Dir,
		opts:                  opts,
		logger:                opts.Logger,
		metricsBytesWritten:   opts.BytesWritten,
		metricsEntriesWritten: opts.EntriesWritten,
		hookManager:           opts.HookManager,
	}

	// 1. Discover existing segments
	if err := w.loadSegments(); err != nil {
		return nil, nil, fmt.Errorf("failed to load WAL segments: %w", err)
	}

	// 2. Perform recovery
	recoveredEntries, recoveryErr := w.recover(opts.StartRecoveryIndex)
	// We will return recoveryErr at the end, but we continue with initialization.
	// The caller (StorageEngine) will decide if the error is fatal.
	// An io.EOF error means a clean end of all segments was reached.
	// Other errors (e.g., io.ErrUnexpectedEOF) indicate potential truncation.

	// 3. Prepare for appending
	if err := w.openForAppend(); err != nil {
		w.Close()
		return nil, nil, fmt.Errorf("failed to open WAL for appending: %w", err)
	}

	// The recovery process returns io.EOF for a clean, full read of all segments,
	// which is not an error for the Open operation. Other errors (like UnexpectedEOF
	// on a non-last segment) are real problems.
	if recoveryErr == io.EOF {
		return w, recoveredEntries, nil
	}
	return w, recoveredEntries, recoveryErr
}

// loadSegments scans the WAL directory and populates the segmentIndexes slice.
func (w *WAL) loadSegments() error {
	files, err := os.ReadDir(w.dir)
	if err != nil {
		return fmt.Errorf("failed to read WAL directory %s: %w", w.dir, err)
	}

	w.segmentIndexes = make([]uint64, 0, len(files))
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		index, err := parseSegmentFileName(file.Name())
		if err == nil {
			w.segmentIndexes = append(w.segmentIndexes, index)
		}
	}
	sort.Slice(w.segmentIndexes, func(i, j int) bool {
		return w.segmentIndexes[i] < w.segmentIndexes[j]
	})
	return nil
}

// SetTestingOnlyInjectCloseError sets an error that will be returned by the Close() method.
func (w *WAL) SetTestingOnlyInjectCloseError(err error) {
	w.testingOnlyInjectCloseError = err
}

func (w *WAL) SetTestingOnlyInjectAppendError(err error) {
	w.testingOnlyInjectAppendError = err
}

// Append writes a single WALEntry to the log. It's a convenience wrapper around AppendBatch.
func (w *WAL) Append(entry core.WALEntry) error {
	return w.AppendBatch([]core.WALEntry{entry})
}

// AppendBatch writes a slice of WAL entries as a single, atomic record.
func (w *WAL) AppendBatch(entries []core.WALEntry) error {
	if len(entries) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.testingOnlyInjectAppendError != nil {
		return w.testingOnlyInjectAppendError // ถ้ามี error ถูก inject ให้คืนค่านั้นไปเลย
	}

	var batchPayload bytes.Buffer
	if len(entries) == 1 {
		// Write as a single entry record for efficiency
		if err := encodeEntryData(&batchPayload, &entries[0]); err != nil {
			return fmt.Errorf("failed to encode single entry for batch append: %w", err)
		}
	} else {
		// Write as a batch record
		if err := batchPayload.WriteByte(byte(core.EntryTypePutBatch)); err != nil {
			return fmt.Errorf("failed to write batch entry type: %w", err)
		}
		if err := binary.Write(&batchPayload, binary.LittleEndian, uint32(len(entries))); err != nil {
			return fmt.Errorf("failed to write batch entry count: %w", err)
		}
		for i := range entries {
			if err := encodeEntryData(&batchPayload, &entries[i]); err != nil {
				return fmt.Errorf("failed to encode entry %d for batch: %w", i, err)
			}
		}
	}

	payloadBytes := batchPayload.Bytes()
	newRecordSize := int64(len(payloadBytes) + 8) // +4 for length, +4 for checksum

	if w.activeSegment == nil {
		return errors.New("wal is closed or not open for writing")
	}

	// Check if we need to rotate the segment BEFORE writing the new record.
	// Rotate if the current file already contains data and adding the new record would exceed the max size.
	currentSize, err := w.activeSegment.Size()
	if err != nil {
		return fmt.Errorf("could not get active segment size: %w", err)
	}
	// The check `currentSize > int64(binary.Size(core.FileHeader{}))` ensures we only rotate
	// if the segment already contains at least one record. This allows a single large
	// record to be written to an empty segment, even if it exceeds the max size.
	if currentSize > int64(binary.Size(core.FileHeader{})) && (currentSize+newRecordSize) > w.opts.MaxSegmentSize {
		w.logger.Debug("Rotating WAL segment due to size", "current_size", currentSize, "new_record_size", newRecordSize, "max_size", w.opts.MaxSegmentSize)
		if err := w.rotateLocked(); err != nil {
			return fmt.Errorf("failed to rotate WAL segment: %w", err)
		}
	}

	if w.metricsBytesWritten != nil {
		w.metricsBytesWritten.Add(int64(len(payloadBytes) + 8)) // +8 for length and checksum
	}
	if w.metricsEntriesWritten != nil {
		w.metricsEntriesWritten.Add(int64(len(entries)))
	}

	if err := w.activeSegment.WriteRecord(payloadBytes); err != nil {
		return err
	}

	if w.opts.SyncMode == SyncAlways {
		return w.activeSegment.Sync()
	}
	return nil
}

// Sync flushes data to the active segment file.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.activeSegment.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL file: %w", err)
	}
	return nil
}

// Rotate manually triggers a segment rotation.
// It closes the current segment and opens a new one for writing.
func (w *WAL) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.rotateLocked()
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.testingOnlyInjectCloseError != nil {
		return w.testingOnlyInjectCloseError
	}

	if w.activeSegment == nil {
		return nil // Already closed
	}

	closeErr := w.activeSegment.Close()
	w.activeSegment = nil

	if closeErr != nil {
		w.logger.Error("Error during WAL close.", "error", closeErr)
	} else {
		w.logger.Info("WAL closed.")
	}
	return closeErr
}

// Purge deletes segment files with index less than or equal to the given index.
func (w *WAL) Purge(upToIndex uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var remainingIndexes []uint64
	var purgedCount int
	for _, index := range w.segmentIndexes {
		if index <= upToIndex {
			// Don't delete the active segment
			if w.activeSegment != nil && w.activeSegment.index == index {
				w.logger.Warn("Skipping purge of active WAL segment", "index", index)
				remainingIndexes = append(remainingIndexes, index)
				continue
			}
			path := filepath.Join(w.dir, formatSegmentFileName(index))
			if err := os.Remove(path); err != nil {
				// Log error but continue trying to delete others
				w.logger.Error("Failed to purge WAL segment", "path", path, "error", err)
			} else {
				purgedCount++
			}
		} else {
			remainingIndexes = append(remainingIndexes, index)
		}
	}
	w.segmentIndexes = remainingIndexes
	if purgedCount > 0 {
		w.logger.Info("Purged WAL segments", "count", purgedCount, "up_to_index", upToIndex)
	}
	return nil
}

// Path returns the directory path of the WAL.
func (w *WAL) Path() string {
	return w.dir
}

// ActiveSegmentIndex returns the index of the current active segment file.
// It returns 0 if there is no active segment.
func (w *WAL) ActiveSegmentIndex() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.activeSegment == nil {
		return 0
	}
	return w.activeSegment.index
}

// rotate creates a new segment file for writing. Must be called with lock held.
func (w *WAL) rotateLocked() error {
	var nextIndex uint64 = 1
	if len(w.segmentIndexes) > 0 {
		nextIndex = w.segmentIndexes[len(w.segmentIndexes)-1] + 1
	}

	newSegment, err := CreateSegment(w.dir, nextIndex)
	if err != nil {
		return err
	}

	var oldIndex uint64
	if w.activeSegment != nil {
		oldIndex = w.activeSegment.index
		if err := w.activeSegment.Close(); err != nil {
			w.logger.Error("failed to close active segment during rotation", "path", w.activeSegment.path, "error", err)
			// Continue anyway, we need a new segment
		}
	}

	w.activeSegment = newSegment
	w.segmentIndexes = append(w.segmentIndexes, nextIndex)
	w.logger.Info("Rotated to new WAL segment", "index", nextIndex, "path", newSegment.path)
	// --- Post-WAL-Rotate Hook ---
	if w.hookManager != nil && oldIndex > 0 {
		payload := hooks.PostWALRotatePayload{
			OldSegmentIndex: oldIndex,
			NewSegmentIndex: newSegment.index,
			NewSegmentPath:  newSegment.path,
		}
		// Use background context as this is an internal, non-request-driven event.
		w.hookManager.Trigger(context.Background(), hooks.NewPostWALRotateEvent(payload))
	}
	return nil
}

// encodeEntryData serializes a single WALEntry's data part into a writer.
func encodeEntryData(w io.Writer, entry *core.WALEntry) error {
	if err := binary.Write(w, binary.LittleEndian, entry.EntryType); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, entry.SeqNum); err != nil {
		return err
	}

	keyLenBuf := make([]byte, binary.MaxVarintLen32)
	keyLenBytes := binary.PutUvarint(keyLenBuf, uint64(len(entry.Key)))
	if _, err := w.Write(keyLenBuf[:keyLenBytes]); err != nil {
		return err
	}
	if _, err := w.Write(entry.Key); err != nil {
		return err
	}

	valLenBuf := make([]byte, binary.MaxVarintLen32)
	valLenBytes := binary.PutUvarint(valLenBuf, uint64(len(entry.Value)))
	if _, err := w.Write(valLenBuf[:valLenBytes]); err != nil {
		return err
	}
	_, err := w.Write(entry.Value)
	return err
}

// decodeEntryData deserializes a single WALEntry's data part from a reader.
func decodeEntryData(r io.Reader) (*core.WALEntry, error) {
	entry := &core.WALEntry{}
	if err := binary.Read(r, binary.LittleEndian, &entry.EntryType); err != nil {
		return nil, fmt.Errorf("failed to read entry type: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &entry.SeqNum); err != nil {
		return nil, fmt.Errorf("failed to read sequence number: %w", err)
	}

	keyLen, err := binary.ReadUvarint(r.(io.ByteReader))
	if err != nil {
		return nil, fmt.Errorf("failed to read key length: %w", err)
	}
	entry.Key = make([]byte, keyLen)
	if _, err := io.ReadFull(r, entry.Key); err != nil {
		return nil, fmt.Errorf("failed to read key: %w", err)
	}

	valLen, err := binary.ReadUvarint(r.(io.ByteReader))
	if err != nil {
		return nil, fmt.Errorf("failed to read value length: %w", err)
	}
	if valLen > 0 {
		entry.Value = make([]byte, valLen)
		if _, err := io.ReadFull(r, entry.Value); err != nil {
			return nil, fmt.Errorf("failed to read value: %w", err)
		}
	}

	return entry, nil
}

// recover reads all entries from all known segments.
func (w *WAL) recover(startRecoveryIndex uint64) ([]core.WALEntry, error) {
	var allEntries []core.WALEntry
	for _, index := range w.segmentIndexes {
		if index <= startRecoveryIndex {
			continue // Skip segments that are already covered by a checkpoint
		}
		path := filepath.Join(w.dir, formatSegmentFileName(index))
		entries, err := recoverFromSegment(path, w.logger)
		if len(entries) > 0 {
			allEntries = append(allEntries, entries...)
		}
		if err != nil {
			if err == io.EOF {
				// Cleanly read all records in this segment, continue to the next.
				continue
			}
			// For other errors (e.g., corruption, unexpected EOF), we stop recovery.
			// The caller receives the partially recovered entries and the error,
			// and can decide how to proceed.
			w.logger.Warn("Recovery stopped on segment due to error", "index", index, "path", path, "error", err)
			return allEntries, err
		}
	}
	// If we successfully read all segments without error, return EOF to signal a clean recovery.
	return allEntries, io.EOF
}

// recoverFromSegment reads all valid entries from a single WAL segment file.
// It is an unexported helper function.
// It returns all entries read successfully before an error was encountered,
// along with the error itself (which can be io.EOF for a clean read).
func recoverFromSegment(filePath string, logger *slog.Logger) ([]core.WALEntry, error) {
	reader, err := OpenSegmentForRead(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Info("WAL segment does not exist, nothing to recover.", "path", filePath)
			return nil, nil // Not an error, just no entries to recover.
		}
		return nil, fmt.Errorf("failed to open WAL segment for reading %s: %w", filePath, err)
	}
	defer reader.Close()

	var entries []core.WALEntry
	for {
		recordData, err := reader.ReadRecord()
		if err != nil {
			// This is the important part: return successfully read entries along with the error.
			// The caller can then decide if the error (e.g., io.EOF, io.ErrUnexpectedEOF) is fatal.
			return entries, err
		}

		payloadReader := bytes.NewReader(recordData)
		var entryTypeByte byte
		entryTypeByte, err = payloadReader.ReadByte()
		if err != nil {
			return entries, fmt.Errorf("error reading entry type from WAL record: %w", err)
		}
		entryType := core.EntryType(entryTypeByte)

		if entryType == core.EntryTypePutBatch {
			var numEntries uint32
			if err := binary.Read(payloadReader, binary.LittleEndian, &numEntries); err != nil {
				return entries, fmt.Errorf("error reading batch entry count: %w", err)
			}
			for i := 0; i < int(numEntries); i++ {
				entry, err := decodeEntryData(payloadReader)
				if err != nil {
					return entries, fmt.Errorf("error decoding entry %d in batch: %w", i, err)
				}
				entries = append(entries, *entry)
			}
		} else {
			// The payloadReader already had its first byte read, so we need to construct a new reader
			// for decodeEntryData which expects to read the type byte itself.
			entry, err := decodeEntryData(bytes.NewReader(recordData))
			if err != nil {
				return entries, fmt.Errorf("error decoding single WAL entry: %w", err)
			}
			entries = append(entries, *entry)
		}
	}
}

func (w *WAL) openForAppend() error {
	if len(w.segmentIndexes) == 0 {
		// No segments exist, create the first one.
		return w.rotateLocked()
	}

	// Open the last known segment for writing.
	lastIndex := w.segmentIndexes[len(w.segmentIndexes)-1]
	path := filepath.Join(w.dir, formatSegmentFileName(lastIndex))

	// To avoid appending to a potentially corrupt/partially written file after a crash,
	// we start a new segment. A more advanced implementation could truncate the last
	// record and continue, but starting a new segment is safer and simpler.
	stat, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to stat last segment %s: %w", path, err)
	}

	if stat.Size() > int64(binary.Size(core.FileHeader{})) {
		// If the last segment has more than just a header, rotate to a new one.
		return w.rotateLocked()
	}

	// If the last segment is empty or only has a header, reuse it.
	// We need to truncate it and rewrite the header to be safe.
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("failed to remove incomplete segment %s for reuse: %w", path, err)
	}

	seg, err := CreateSegment(w.dir, lastIndex)
	if err != nil {
		return fmt.Errorf("failed to reuse segment %d: %w", lastIndex, err)
	}
	w.activeSegment = seg
	return nil
}
