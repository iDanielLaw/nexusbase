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
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
)

// commitRecord represents a single commit request to the WAL.
// It bundles the entries to be written with a channel to signal completion.
type commitRecord struct {
	entries []core.WALEntry
	done    chan error
}

// WAL (Write-Ahead Log) provides durability by logging operations before they are applied to memtable.
// It manages a directory of segment files.
type WAL struct {
	dir  string
	mu   sync.Mutex
	opts Options

	activeSegment  *SegmentWriter
	segmentIndexes []uint64

	// Group Commit fields
	commitChan   chan *commitRecord
	shutdownChan chan struct{}
	committerWg  sync.WaitGroup
	closeOnce    sync.Once
	isClosing    atomic.Bool

	metricsBytesWritten   *expvar.Int
	metricsEntriesWritten *expvar.Int

	logger      *slog.Logger
	hookManager hooks.HookManager

	testingOnlyInjectCloseError  error
	testingOnlyInjectAppendError error
}

var _ WALInterface = (*WAL)(nil)

// NewStreamReader creates a new reader for streaming WAL entries.
// It's designed for replication followers to tail the leader's WAL.
func (w *WAL) NewStreamReader(fromSeqNum uint64) (StreamReader, error) {
	// The stream reader needs a consistent view of the segment list, so we lock.
	w.mu.Lock()
	defer w.mu.Unlock()

	// TODO: Implement logic to find the correct starting segment based on fromSeqNum.
	// For now, the filtering of already-seen sequence numbers is handled
	// within the stream reader's Next() method by checking sr.lastReadSeqNum.
	sr := &streamReader{
		wal:            w,
		lastReadSeqNum: fromSeqNum,
		logger:         w.logger.With("component", "wal_stream_reader"),
	}
	return sr, nil
}

// Options holds configuration for the WAL.
type Options struct {
	Dir                string
	SyncMode           core.WALSyncMode
	MaxSegmentSize     int64
	BytesWritten       *expvar.Int
	EntriesWritten     *expvar.Int
	Logger             *slog.Logger
	CommitMaxDelay     time.Duration // Max delay before a pending batch is committed.
	CommitMaxBatchSize int           // Max number of records in a batch before a commit is forced.
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
	if opts.CommitMaxDelay == 0 {
		opts.CommitMaxDelay = 10 * time.Millisecond
	}
	if opts.CommitMaxBatchSize == 0 {
		opts.CommitMaxBatchSize = 64
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
	w.isClosing.Store(false)

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
		// Close is not called here because the committer goroutine hasn't been started.
		return nil, nil, fmt.Errorf("failed to open WAL for appending: %w", err)
	}

	// 4. Start the committer goroutine for group commits.
	w.commitChan = make(chan *commitRecord, 128) // Buffer size can be tuned.
	w.shutdownChan = make(chan struct{})
	w.committerWg.Add(1)
	go w.runCommitter()

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

// AppendBatch submits a slice of WAL entries to be written by the committer goroutine.
// It blocks until the entries have been written and synced to disk.
func (w *WAL) AppendBatch(entries []core.WALEntry) error {
	if w.isClosing.Load() {
		return errors.New("wal is closed")
	}
	if len(entries) == 0 {
		return nil
	}

	if w.testingOnlyInjectAppendError != nil {
		return w.testingOnlyInjectAppendError
	}

	rec := &commitRecord{
		entries: entries,
		done:    make(chan error, 1),
	}

	w.commitChan <- rec
	return <-rec.done
}

// Sync forces a commit of all pending entries and waits for it to complete.
func (w *WAL) Sync() error {
	if w.isClosing.Load() {
		return errors.New("wal is closed")
	}
	// With group commit, Sync can be implemented by sending a special empty record
	// and waiting for it to complete. This ensures all prior writes are flushed.
	rec := &commitRecord{
		entries: []core.WALEntry{},
		done:    make(chan error, 1),
	}
	w.commitChan <- rec
	return <-rec.done
}

// Rotate manually triggers a segment rotation.
func (w *WAL) Rotate() error {
	if w.isClosing.Load() {
		return errors.New("wal is closed")
	}
	// To rotate, we send a nil-entry record. The committer interprets this as a rotation request.
	rec := &commitRecord{
		entries: nil, // A nil slice of entries signals a rotation request.
		done:    make(chan error, 1),
	}
	w.commitChan <- rec
	return <-rec.done
}

// Close shuts down the committer goroutine and closes the WAL file.
func (w *WAL) Close() (closeErr error) {
	w.closeOnce.Do(func() {
		w.isClosing.Store(true)

		// Signal the committer to shut down.
		if w.shutdownChan != nil {
			close(w.shutdownChan)
		}

		// Wait for the committer to finish processing.
		w.committerWg.Wait()

		w.mu.Lock()
		defer w.mu.Unlock()

		if w.testingOnlyInjectCloseError != nil {
			closeErr = w.testingOnlyInjectCloseError
			return
		}

		if w.activeSegment == nil {
			return // Already closed
		}

		closeErr = w.activeSegment.Close()
		w.activeSegment = nil

		if closeErr != nil {
			w.logger.Error("Error during WAL close.", "error", closeErr)
		} else {
			w.logger.Info("WAL closed.")
		}
	})
	return
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

// activeSegmentIndexLocked returns the index of the current active segment file.
// It assumes the caller holds the WAL's mutex.
func (w *WAL) activeSegmentIndexLocked() uint64 {
	if w.activeSegment == nil {
		return 0
	}
	return w.activeSegment.index
}

// ActiveSegmentIndex returns the index of the current active segment file.
// It returns 0 if there is no active segment.
func (w *WAL) ActiveSegmentIndex() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.activeSegmentIndexLocked()
}

// rotateLocked creates a new segment file for writing. Must be called with lock held.
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
	segmentsToRecover := make([]uint64, 0)
	for _, index := range w.segmentIndexes {
		if index <= startRecoveryIndex {
			continue // Skip segments that are already covered by a checkpoint
		}
		segmentsToRecover = append(segmentsToRecover, index)
	}

	if len(segmentsToRecover) == 0 {
		return nil, nil // Nothing to recover
	}

	// --- Parallel Recovery ---
	numWorkers := runtime.NumCPU()
	if len(segmentsToRecover) < numWorkers {
		numWorkers = len(segmentsToRecover)
	}

	w.logger.Info("Starting parallel WAL recovery", "segments_to_recover", len(segmentsToRecover), "workers", numWorkers)

	var wg sync.WaitGroup
	segmentChan := make(chan uint64, len(segmentsToRecover))
	resultsChan := make(chan []core.WALEntry, len(segmentsToRecover))
	errChan := make(chan error, numWorkers)

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for index := range segmentChan {
				path := filepath.Join(w.dir, core.FormatSegmentFileName(index))
				entries, err := recoverFromSegment(path, w.logger)
				if len(entries) > 0 {
					resultsChan <- entries
				}
				if err != nil && err != io.EOF {
					w.logger.Error("Error recovering from segment", "worker_id", workerID, "segment_index", index, "error", err)
					errChan <- err
					return // Stop this worker on a hard error
				}
			}
		}(i)
	}

	// Feed segments to workers
	for _, index := range segmentsToRecover {
		segmentChan <- index
	}
	close(segmentChan)

	wg.Wait()
	close(resultsChan)
	close(errChan)

	// Collect and sort all results
	var allEntries []core.WALEntry
	for entries := range resultsChan {
		allEntries = append(allEntries, entries...)
	}

	// Check for the first error that occurred.
	// We do this after collecting results so that we can return partially recovered data.
	firstErr := <-errChan

	sort.Slice(allEntries, func(i, j int) bool {
		return allEntries[i].SeqNum < allEntries[j].SeqNum
	})

	// If there was an error, return the recovered entries along with it.
	// Otherwise, firstErr will be nil.
	return allEntries, firstErr
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

// runCommitter is the heart of the group commit mechanism.
// It runs in a dedicated goroutine, collecting write requests (commitRecords)
// and committing them in batches.
func (w *WAL) runCommitter() {
	defer w.committerWg.Done()

	// We use a ticker to set a maximum delay for commits, ensuring that even
	// low-traffic periods have bounded latency.
	ticker := time.NewTicker(w.opts.CommitMaxDelay)
	defer ticker.Stop()

	var pending []*commitRecord

	for {
		select {
		case rec := <-w.commitChan:
			pending = append(pending, rec)
			// If we have a large enough batch, commit immediately without waiting for the ticker.
			if len(pending) >= w.opts.CommitMaxBatchSize {
				w.commit(pending)
				pending = nil
			}

		case <-ticker.C:
			if len(pending) > 0 {
				w.commit(pending)
				pending = nil
			}

		case <-w.shutdownChan:
			if len(pending) > 0 {
				// On shutdown, commit any remaining pending writes.
				w.commit(pending)
			}
			return
		}
	}
}

// commit performs the actual writing and syncing of a batch of commit records.
func (w *WAL) commit(records []*commitRecord) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.activeSegment == nil || w.isClosing.Load() {
		// This can happen if Close() is called concurrently.
		// Notify waiters that the write failed because the WAL is closed.
		for _, rec := range records {
			rec.done <- errors.New("wal is closed")
		}
		return
	}

	// 1. Combine all entries from all records into one giant batch.
	var allEntries []core.WALEntry
	var shouldRotate bool
	for _, rec := range records {
		if rec.entries == nil {
			// A nil entry list is a signal for rotation.
			shouldRotate = true
		} else {
			allEntries = append(allEntries, rec.entries...)
		}
	}

	// A request with an empty entry slice is a sync request.
	// A request with a nil entry slice is a rotation request.
	if len(allEntries) == 0 {
		var err error
		if shouldRotate {
			err = w.rotateLocked()
		} else if w.opts.SyncMode != core.WALSyncDisabled {
			err = w.activeSegment.Sync()
		}

		// Notify all waiters.
		for _, rec := range records {
			rec.done <- err
		}
		return
	}

	// --- Actual Write Logic ---
	var batchPayload bytes.Buffer
	if err := batchPayload.WriteByte(byte(core.EntryTypePutBatch)); err != nil {
		// This is a non-recoverable error, notify all waiters and return.
		for _, rec := range records {
			rec.done <- err
		}
		return
	}
	if err := binary.Write(&batchPayload, binary.LittleEndian, uint32(len(allEntries))); err != nil {
		for _, rec := range records {
			rec.done <- err
		}
		return
	}
	for i := range allEntries {
		if err := encodeEntryData(&batchPayload, &allEntries[i]); err != nil {
			for _, rec := range records {
				rec.done <- err
			}
			return
		}
	}
	payloadBytes := batchPayload.Bytes()
	newRecordSize := int64(len(payloadBytes) + 8) // +4 for length, +4 for checksum

	// Check for rotation before writing.
	currentSize, err := w.activeSegment.Size()
	if err != nil {
		for _, rec := range records {
			rec.done <- err
		}
		return
	}
	if currentSize > int64(binary.Size(core.FileHeader{})) && (currentSize+newRecordSize) > w.opts.MaxSegmentSize {
		if err := w.rotateLocked(); err != nil {
			for _, rec := range records {
				rec.done <- err
			}
			return
		}
	}

	// Write the single large batch record.
	writeErr := w.activeSegment.WriteRecord(payloadBytes)

	// Sync if required.
	var syncErr error
	if writeErr == nil && w.opts.SyncMode != core.WALSyncDisabled {
		syncErr = w.activeSegment.Sync()
	}

	// Update metrics if the write was successful.
	if writeErr == nil && syncErr == nil {
		if w.metricsBytesWritten != nil {
			w.metricsBytesWritten.Add(int64(len(payloadBytes) + 8))
		}
		if w.metricsEntriesWritten != nil {
			w.metricsEntriesWritten.Add(int64(len(allEntries)))
		}
	}

	// Notify all waiting goroutines.
	finalErr := writeErr
	if finalErr == nil {
		finalErr = syncErr
	}

	for _, rec := range records {
		rec.done <- finalErr
	}
}