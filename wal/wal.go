package wal

import (
	"bufio"
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

// WAL (Write-Ahead Log) provides durability by logging operations before they are applied to memtable.
// It manages a directory of segment files.
type WAL struct {
	dir  string
	mu   sync.Mutex
	opts Options

	activeSegment  *SegmentWriter
	segmentIndexes []uint64
	readerCond     *sync.Cond

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
	SyncMode       core.WALSyncMode
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
		opts.MaxSegmentSize = core.WALMaxSegmentSize
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
		readerCond:            sync.NewCond(&sync.Mutex{}),
	}

	// 1. Discover existing segments
	if err := w.loadSegments(); err != nil {
		return nil, nil, fmt.Errorf("failed to load WAL segments: %w", err)
	}

	// Use the WAL's mutex for the condition variable
	w.readerCond.L = &w.mu

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
		index, err := core.ParseSegmentFileName(file.Name())
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
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.testingOnlyInjectAppendError != nil {
		return w.testingOnlyInjectAppendError // ถ้ามี error ถูก inject ให้คืนค่านั้นไปเลย
	}

	if len(entries) == 0 {
		return nil
	}

	var batchPayload bytes.Buffer
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

	// Signal any waiting readers that new data is available.
	w.readerCond.Broadcast()

	if w.opts.SyncMode == core.WALSyncAlways {
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
			path := filepath.Join(w.dir, core.FormatSegmentFileName(index))
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
	// Write fixed-size fields first.
	if err := binary.Write(w, binary.LittleEndian, entry.EntryType); err != nil {
		return fmt.Errorf("failed to write entry type: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, entry.SeqNum); err != nil {
		return fmt.Errorf("failed to write sequence number: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, entry.SegmentIndex); err != nil {
		return fmt.Errorf("failed to write segment index: %w", err)
	}

	// Write variable-size fields with length prefixes.
	if err := writeUvarintPrefixed(w, entry.Key); err != nil {
		return fmt.Errorf("failed to write key: %w", err)
	}
	if err := writeUvarintPrefixed(w, entry.Value); err != nil {
		return fmt.Errorf("failed to write value: %w", err)
	}
	return nil
}

// decodeEntryData deserializes a single WALEntry's data part from a reader.
func decodeEntryData(r io.Reader) (*core.WALEntry, error) {
	entry := &core.WALEntry{}

	// Explicitly read the EntryType as a single byte.
	byteReader, ok := r.(io.ByteReader)
	if !ok {
		// Wrap the reader if it doesn't implement io.ByteReader, which is needed for ReadUvarint.
		byteReader = bufio.NewReader(r)
	}

	if err := binary.Read(r, binary.LittleEndian, &entry.EntryType); err != nil {
		return nil, fmt.Errorf("failed to read entry type: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &entry.SeqNum); err != nil {
		return nil, fmt.Errorf("failed to read sequence number: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &entry.SegmentIndex); err != nil {
		return nil, fmt.Errorf("failed to read segment index: %w", err)
	}

	var err error
	entry.Key, err = readUvarintPrefixed(byteReader)
	if err != nil {
		return nil, fmt.Errorf("failed to read key: %w", err)
	}
	entry.Value, err = readUvarintPrefixed(byteReader)
	if err != nil {
		return nil, fmt.Errorf("failed to read value: %w", err)
	}

	return entry, nil
}

// recover reads all entries from all known segments.
func (w *WAL) recover(startRecoveryIndex uint64) ([]core.WALEntry, error) {
	var allEntries []core.WALEntry
	for _, index := range w.segmentIndexes {
		if index <= startRecoveryIndex {
			w.logger.Debug("Skipping WAL segment for recovery (covered by checkpoint)", "index", index)
			continue // Skip segments that are already covered by a checkpoint
		}
		path := filepath.Join(w.dir, core.FormatSegmentFileName(index))
		entries, err := w.recoverFromSegment(path)
		if len(entries) > 0 {
			allEntries = append(allEntries, entries...)
			// Update the last known sequence number from the recovered entries
			// This part is actually handled by the engine applying the entries,
			// so we don't need to track it here during recovery.
			// w.lastKnownSeqNum = entries[len(entries)-1].SeqNum
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
// It is a method on WAL to access its logger.
// It returns all entries read successfully before an error was encountered,
// along with the error itself (which can be io.EOF for a clean read).
func (w *WAL) recoverFromSegment(filePath string) ([]core.WALEntry, error) {
	reader, err := OpenSegmentForRead(filePath)
	if err != nil {
		return nil, err
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

		// The entire record is a batch. Decode it.
		batchEntries, err := decodeBatchRecord(recordData)
		if len(batchEntries) > 0 {
			entries = append(entries, batchEntries...)
		}
		if err != nil {
			return entries, fmt.Errorf("failed to decode batch record from segment: %w", err) // Return entries collected so far, along with the error
		}
	}
}

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
			// Clean end of segment, close it and try to open the next one in the next loop iteration.
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

func (w *WAL) openForAppend() error {
	if len(w.segmentIndexes) == 0 {
		// No segments exist, create the first one.
		return w.rotateLocked()
	}

	// Open the last known segment for writing.
	lastIndex := w.segmentIndexes[len(w.segmentIndexes)-1]
	path := filepath.Join(w.dir, core.FormatSegmentFileName(lastIndex))

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
	// CreateSegment will truncate the file and write a new header, making it safe for reuse.

	seg, err := CreateSegment(w.dir, lastIndex)
	if err != nil {
		return fmt.Errorf("failed to reuse segment %d: %w", lastIndex, err)
	}
	w.activeSegment = seg
	return nil
}

// writeUvarintPrefixed writes a uvarint length prefix followed by the data slice.
func writeUvarintPrefixed(w io.Writer, data []byte) error {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, uint64(len(data)))
	if _, err := w.Write(buf[:n]); err != nil {
		return err
	}
	if len(data) > 0 {
		if _, err := w.Write(data); err != nil {
			return err
		}
	}
	return nil
}

// readUvarintPrefixed reads a uvarint length prefix and then the data slice.
func readUvarintPrefixed(r io.ByteReader) ([]byte, error) {
	length, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	if length > 0 {
		data := make([]byte, length)
		// The reader might not be an io.Reader, so we need to cast it.
		if _, err := io.ReadFull(r.(io.Reader), data); err != nil {
			return nil, err
		}
		return data, nil
	}
	return nil, nil
}

// decodeBatchRecord decodes a byte slice that represents a batch of WAL entries.
func decodeBatchRecord(recordData []byte) ([]core.WALEntry, error) {
	reader := bytes.NewReader(recordData)
	var entryTypeByte byte
	entryTypeByte, err := reader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("error reading entry type from WAL record: %w", err)
	}
	if core.EntryType(entryTypeByte) != core.EntryTypePutBatch {
		return nil, fmt.Errorf("unexpected WAL record type: got %d, want %d (EntryTypePutBatch)", entryTypeByte, core.EntryTypePutBatch)
	}

	var numEntries uint32
	if err := binary.Read(reader, binary.LittleEndian, &numEntries); err != nil {
		return nil, fmt.Errorf("error reading batch entry count: %w", err)
	}

	entries := make([]core.WALEntry, 0, numEntries)
	for i := 0; i < int(numEntries); i++ {
		entry, err := decodeEntryData(reader)
		if err != nil {
			return entries, fmt.Errorf("error decoding entry %d in batch: %w", i+1, err)
		}
		entries = append(entries, *entry)
	}
	return entries, nil
}
