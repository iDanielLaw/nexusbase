package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/INLOpen/nexusbase/checkpoint"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/memtable"
	"github.com/INLOpen/nexusbase/sstable"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// maxFlushRetries is the maximum number of times a memtable flush will be retried.
const maxFlushRetries = 3

// initialFlushRetryDelay is the initial delay before retrying a failed flush.
const initialFlushRetryDelay = 1 * time.Second

// maxFlushRetryDelay is the maximum delay between flush retries.
const maxFlushRetryDelay = 30 * time.Second

// processImmutableMemtables is the background goroutine logic. It handles retries and DLQ.
// It takes one memtable from the immutable queue and flushes it to an SSTable.
func (e *storageEngine) processImmutableMemtables(writeCheckpoint bool) {
	e.mu.Lock()
	if len(e.immutableMemtables) == 0 {
		e.mu.Unlock()
		return
	}

	// Create a span only if there's work to do.
	ctx, span := e.tracer.Start(context.Background(), "StorageEngine.processImmutableMemtables")
	defer span.End()
	span.SetAttributes(attribute.Int("immutable_memtables.count", len(e.immutableMemtables)))

	memToFlush := e.immutableMemtables[0]
	e.immutableMemtables = e.immutableMemtables[1:]
	e.mu.Unlock()

	// This function now contains the retry logic.
	for {
		err := e.flushMemtableToSSTable(ctx, memToFlush)
		if err == nil {
			// Success: Memtable has been flushed to an SSTable.
			// Now, we can release the memtable's resources back to the pools.
			memToFlush.Close()

			// Now, we can create a checkpoint and purge old WAL segments.
			e.logger.Info("Memtable flush to SSTable completed. Proceeding with checkpoint.", "memtable_size", memToFlush.Size())

			if writeCheckpoint {
				// NOTE: This implementation assumes that the `memtable.Memtable` struct has a field
				// or method (e.g., `LastWALSegmentIndex() uint64`) that returns the index of the
				// latest WAL segment containing data for that memtable. This value should be
				// recorded when the mutable memtable is swapped to the immutable list.
				lastActiveSegment := memToFlush.LastWALSegmentIndex

				// We can only safely checkpoint the segment *before* the one that was active
				// when this memtable was rotated. This ensures we don't checkpoint a segment
				// that might still be receiving writes from other operations.
				safeSegmentToCheckpoint := lastActiveSegment - 1

				if safeSegmentToCheckpoint > 0 {
					cp := checkpoint.Checkpoint{LastSafeSegmentIndex: safeSegmentToCheckpoint}
					if writeErr := checkpoint.Write(e.opts.DataDir, cp); writeErr != nil {
						e.logger.Error("Failed to write checkpoint after memtable flush. WAL files will not be purged.", "error", writeErr)
					} else {
						e.logger.Info("Checkpoint written successfully.", "last_safe_segment_index", safeSegmentToCheckpoint)
						e.purgeWALSegments(safeSegmentToCheckpoint)
					}
				}
			}

			e.mu.Lock() // Acquire lock to persist manifest
			e.persistManifest()
			e.mu.Unlock()
			return
		}

		// Failure
		memToFlush.FlushRetries++
		if memToFlush.FlushRetries >= maxFlushRetries {
			e.logger.Error("Memtable flush failed after max retries. Attempting to move to DLQ.", "memtable_size", memToFlush.Size(), "retries", memToFlush.FlushRetries, "error", err)
			if dlqErr := e.moveToDLQ(memToFlush); dlqErr != nil {
				e.logger.Error("CRITICAL: Failed to move memtable to DLQ. Data might be lost.", "error", dlqErr, "memtable_size", memToFlush.Size())
			}
			return // Give up on this memtable
		}

		// Calculate next retry delay
		if memToFlush.NextRetryDelay == 0 {
			memToFlush.NextRetryDelay = initialFlushRetryDelay
		} else {
			memToFlush.NextRetryDelay *= 2
			if memToFlush.NextRetryDelay > maxFlushRetryDelay {
				memToFlush.NextRetryDelay = maxFlushRetryDelay
			}
		}

		e.logger.Warn("Flush error, will retry memtable.", "memtable_size", memToFlush.Size(), "retry_count", memToFlush.FlushRetries, "next_delay", memToFlush.NextRetryDelay.String(), "error", err)

		// Wait for the retry delay or shutdown signal
		select {
		case <-time.After(memToFlush.NextRetryDelay):
			// continue to next iteration of the loop
		case <-e.shutdownChan:
			// Shutdown signaled. We should not retry anymore.
			// Re-queue the memtable so the final synchronous flush in Close() can handle it.
			e.logger.Warn("Shutdown signaled during flush retry. Re-queuing memtable for final flush.", "memtable_size", memToFlush.Size())
			e.mu.Lock()
			e.immutableMemtables = append([]*memtable.Memtable{memToFlush}, e.immutableMemtables...)
			e.mu.Unlock()
			return
		}
	}
}

// triggerPeriodicFlush checks if the mutable memtable should be flushed based on time.
// It moves the current mutable memtable to the immutable list if it's not empty
// and the flush process is not already backlogged.
func (e *storageEngine) triggerPeriodicFlush() {
	_, span := e.tracer.Start(context.Background(), "StorageEngine.triggerPeriodicFlush")
	defer span.End()

	e.mu.Lock()
	// We only trigger a periodic flush if the mutable memtable has data
	// and if the immutable queue is empty, to avoid piling up work
	// if the system is already under heavy load.
	if e.mutableMemtable != nil && e.mutableMemtable.Size() > 0 && len(e.immutableMemtables) == 0 {
		span.SetAttributes(attribute.Bool("flush_triggered", true), attribute.Int64("memtable.size_bytes", e.mutableMemtable.Size()))
		e.logger.Info("Triggering periodic memtable flush.", "size_bytes", e.mutableMemtable.Size())
		// Swap mutable memtable
		// Before moving the memtable, record which WAL segment it belongs to.
		// This is crucial for checkpointing.
		e.mutableMemtable.LastWALSegmentIndex = e.wal.ActiveSegmentIndex()

		e.immutableMemtables = append(e.immutableMemtables, e.mutableMemtable)
		e.mutableMemtable = memtable.NewMemtable(e.opts.MemtableThreshold, e.clock)
		e.mu.Unlock()

		// Signal the flush loop to process the newly added immutable memtable.
		// This is non-blocking. If the flush loop is busy, this signal is dropped,
		// which is fine because the loop will eventually process the queue anyway.
		select {
		case e.flushChan <- struct{}{}:
		default:
		}
	} else {
		span.SetAttributes(attribute.Bool("flush_triggered", false))
		e.mu.Unlock()
	}
}

// hasImmutableMemtables checks if there are any memtables waiting to be flushed.
func (e *storageEngine) hasImmutableMemtables() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.immutableMemtables) > 0
}

// moveToDLQ attempts to write the contents of a memtable to a Dead Letter Queue file.
func (e *storageEngine) moveToDLQ(mem *memtable.Memtable) error {
	if e.dlqDir == "" {
		e.logger.Error("DLQ directory path is empty in engine state. Cannot save problematic memtable.", "memtable_size", mem.Size())
		return fmt.Errorf("DLQ directory not configured in engine state")
	}
	dlqFileName := fmt.Sprintf("memtable-failed-%d.dlq", e.clock.Now().UnixNano())
	dlqFilePath := filepath.Join(e.dlqDir, dlqFileName)
	file, err := os.Create(dlqFilePath)
	if err != nil {
		return fmt.Errorf("failed to create DLQ file %s: %w", dlqFilePath, err)
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	iter := mem.NewIterator(nil, nil, core.Ascending)
	defer iter.Close()
	for iter.Next() {
		cur, err := iter.At()
		if err != nil {
			return fmt.Errorf("error iterating memtable for DLQ: %w", err)
		}
		key := cur.Key
		value := cur.Value
		entryType := cur.EntryType
		seqNum := cur.SeqNum

		entry := struct { // Define struct locally for DLQ entry
			Key       []byte
			Value     []byte
			EntryType core.EntryType
			SeqNum    uint64
		}{key, value, entryType, seqNum}
		if err := encoder.Encode(entry); err != nil {
			e.logger.Error("Failed to encode memtable entry to DLQ file.", "dlq_file", dlqFilePath, "key", string(entry.Key), "error", err)
			return fmt.Errorf("failed to encode memtable entry to DLQ file %s (key: %s): %w", dlqFilePath, string(entry.Key), err)
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("error iterating memtable for DLQ: %w", err)
	}
	e.logger.Info("Successfully moved memtable to DLQ.", "dlq_file", dlqFilePath, "memtable_size", mem.Size())
	return nil
}

// flushMemtableToSSTable flushes a given memtable to a new SSTable on disk.
// It handles the creation of the SSTable, writing entries, and adding it to L0.
// This method is called by background flush processes and during shutdown.
func (e *storageEngine) flushMemtableToSSTable(parentCtx context.Context, memToFlush *memtable.Memtable) error {
	ctx, span := e.tracer.Start(parentCtx, "StorageEngine.flushMemtableToSSTable")
	defer span.End()
	span.SetAttributes(attribute.Int64("memtable.size_bytes", memToFlush.Size()), attribute.Int("memtable.len", memToFlush.Len()))
	startTime := e.clock.Now()

	// For testing purposes, simulate a flush error
	if e.opts.TestingOnlyFailFlushCount != nil {
		if e.opts.TestingOnlyFailFlushCount.Load() > 0 {
			e.opts.TestingOnlyFailFlushCount.Add(-1)
			span.RecordError(fmt.Errorf("simulated flush error"))
			span.SetStatus(codes.Error, "simulated_flush_error")
			return fmt.Errorf("simulated flush error")
		}
	}
	e.metrics.FlushTotal.Add(1)
	// --- Pre-Flush Hook ---
	preFlushPayload := hooks.PreFlushMemtablePayload{} // SSTable is not created yet
	preHookEvent := hooks.NewPreFlushMemtableEvent(preFlushPayload)
	e.hookManager.Trigger(ctx, preHookEvent)

	// Call the core flush logic
	newSST, err := e._flushMemtableToL0SSTable(memToFlush, ctx)
	if err != nil {
		return err // The helper function already logged and traced the error
	}
	if newSST == nil {
		return nil // Memtable was empty, nothing was flushed.
	}

	e.levelsManager.AddL0Table(newSST)

	e.logger.Info("Successfully flushed memtable to SSTable.", "path", newSST.FilePath())

	// --- Post-Flush Hook ---
	postFlushPayload := hooks.PostFlushMemtablePayload{SSTable: newSST}
	postHookEvent := hooks.NewPostFlushMemtableEvent(postFlushPayload)
	e.hookManager.Trigger(context.Background(), postHookEvent) // Use background context for background processes

	span.SetAttributes(attribute.String("sstable.path", newSST.FilePath()))
	// Record metrics on success
	flushDuration := e.clock.Now().Sub(startTime).Seconds()                  // Time taken for the flush operation itself
	memtableLifetime := e.clock.Now().Sub(memToFlush.CreationTime).Seconds() // Time from creation to flush completion
	observeLatency(e.metrics.FlushLatencyHist, flushDuration)
	e.metrics.FlushDataPointsFlushedTotal.Add(int64(memToFlush.Len()))
	e.metrics.FlushBytesFlushedTotal.Add(memToFlush.Size())
	span.SetAttributes(attribute.Float64("flush_duration_seconds", flushDuration), attribute.Float64("memtable_lifetime_seconds", memtableLifetime))
	e.logger.Info("Successfully flushed memtable to SSTable.", "path", newSST.FilePath(), "flush_duration_s", flushDuration, "memtable_lifetime_s", memtableLifetime)

	return nil
}

// _flushMemtableToL0SSTable is the core logic for flushing a memtable to a new L0 SSTable.
// It handles writer creation, flushing, and loading the new table.
// It does NOT handle metrics, hooks, or adding the table to the levels manager.
func (e *storageEngine) _flushMemtableToL0SSTable(memToFlush *memtable.Memtable, parentCtx context.Context) (*sstable.SSTable, error) {
	if memToFlush == nil || memToFlush.Size() == 0 {
		return nil, nil
	}

	_, span := e.tracer.Start(parentCtx, "StorageEngine._flushMemtableToL0SSTable")
	defer span.End()

	fileID := e.GetNextSSTableID()
	estimatedKeys := uint64(memToFlush.Size() / 100)
	if estimatedKeys == 0 {
		estimatedKeys = 1
	}

	writerOpts := core.SSTableWriterOptions{
		DataDir:                      e.sstDir,
		ID:                           fileID,
		EstimatedKeys:                estimatedKeys,
		BloomFilterFalsePositiveRate: e.opts.BloomFilterFalsePositiveRate,
		BlockSize:                    e.opts.SSTableDefaultBlockSize,
		Tracer:                       e.tracer,
		Compressor:                   e.opts.SSTableCompressor,
		Logger:                       e.logger,
	}
	writer, err := sstable.NewSSTableWriter(writerOpts)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed_to_create_sstable_writer")
		return nil, fmt.Errorf("failed to create SSTable writer: %w", err)
	}

	if err := memToFlush.FlushToSSTable(writer); err != nil {
		writer.Abort()
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed_to_flush_memtable_to_sstable")
		return nil, fmt.Errorf("failed to flush memtable to SSTable: %w", err)
	}

	if err := writer.Finish(); err != nil {
		writer.Abort()
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed_to_finish_sstable")
		return nil, fmt.Errorf("failed to finish SSTable: %w", err)
	}

	loadOpts := sstable.LoadSSTableOptions{
		FilePath:   writer.FilePath(),
		ID:         fileID,
		BlockCache: e.blockCache,
		Tracer:     e.tracer,
		Logger:     e.logger,
	}
	newSST, err := sstable.LoadSSTable(loadOpts)
	if err != nil {
		os.Remove(writer.FilePath())
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed_to_load_new_sstable")
		return nil, fmt.Errorf("failed to load newly created SSTable %s: %w", writer.FilePath(), err)
	}

	e.logger.Debug("SSTable flushed and loaded (helper)",
		"id", newSST.ID(),
		"path", newSST.FilePath())

	// --- Post-SSTable-Create Hook ---
	// This hook is triggered after a new SSTable is successfully created from a flush.
	postSSTableCreatePayload := hooks.SSTablePayload{
		ID:    newSST.ID(),
		Level: 0, // Flushes always create L0 tables.
		Path:  newSST.FilePath(),
		Size:  newSST.Size(),
	}
	// This is a post-hook, so it's typically async and we don't handle the error.
	e.hookManager.Trigger(context.Background(), hooks.NewPostSSTableCreateEvent(postSSTableCreatePayload))

	return newSST, nil
}

// syncMetadata persists the manifest and syncs the string/series stores.
func (e *storageEngine) syncMetadata() {
	e.logger.Debug("Starting periodic metadata sync.")

	// Persist manifest (which includes tag index). This needs a lock.
	e.mu.Lock()
	err := e.persistManifest()
	e.mu.Unlock()
	if err != nil {
		e.logger.Error("Error during periodic manifest persistence.", "error", err)
	}

	// Sync string and series stores to ensure their append-only logs are flushed to disk.
	// These operations are assumed to be thread-safe internally.
	e.stringStore.Sync()
	e.seriesIDStore.Sync()

	e.logger.Debug("Periodic metadata sync finished.")
}

// flushRemainingMemtables flushes all remaining immutable memtables and the mutable memtable.
// This is called during shutdown to ensure all in-memory data is persisted.
func (e *storageEngine) flushRemainingMemtables() error {
	// This is called during shutdown, so we create a new context.
	ctx, span := e.tracer.Start(context.Background(), "StorageEngine.flushRemainingMemtables")
	defer span.End()

	e.mu.Lock()
	defer e.mu.Unlock()

	// Flush immutable memtables first
	var firstErr error
	if len(e.immutableMemtables) > 0 {
		e.logger.Info("Processing remaining immutable memtables before final close...", "count", len(e.immutableMemtables))
		span.SetAttributes(attribute.Int("immutable_memtables.count", len(e.immutableMemtables)))
		for _, mem := range e.immutableMemtables {
			if mem == nil {
				continue
			}
			e.logger.Info("Synchronously flushing immutable memtable during shutdown.", "memtable_size", mem.Size())
			if err := e.flushMemtableToSSTable(ctx, mem); err != nil {
				flushErr := fmt.Errorf("failed to flush immutable memtable during shutdown (size: %d): %w", mem.Size(), err)
				e.logger.Error(flushErr.Error())
				if firstErr == nil {
					firstErr = flushErr
				}
				// Attempt to move to DLQ if flush fails during shutdown
				if dlqErr := e.moveToDLQ(mem); dlqErr != nil {
					e.logger.Error("CRITICAL: Failed to move immutable memtable to DLQ during shutdown.", "error", dlqErr)
				}
			} else {
				mem.Close() // Release resources on successful flush
			}
		}
		e.immutableMemtables = nil // Clear the list after processing
	}

	// Flush the mutable memtable
	if e.mutableMemtable != nil && e.mutableMemtable.Size() > 0 {
		e.logger.Info("Flushing final mutable memtable during shutdown.", "size_bytes", e.mutableMemtable.Size())
		span.SetAttributes(attribute.Int64("mutable_memtable.size_bytes", e.mutableMemtable.Size()))
		memToFlush := e.mutableMemtable
		if err := e.flushMemtableToSSTable(ctx, memToFlush); err != nil {
			flushErr := fmt.Errorf("failed to flush mutable memtable during shutdown (size: %d): %w", memToFlush.Size(), err)
			e.logger.Error(flushErr.Error())
			if firstErr == nil {
				firstErr = flushErr
			}
			// Attempt to move to DLQ if flush fails during shutdown
			if dlqErr := e.moveToDLQ(memToFlush); dlqErr != nil {
				e.logger.Error("CRITICAL: Failed to move mutable memtable to DLQ during shutdown.", "error", dlqErr)
			}
		} else {
			// Release resources on successful flush before creating a new one.
			memToFlush.Close()
		}
		e.mutableMemtable = memtable.NewMemtable(e.opts.MemtableThreshold, e.clock) // Create a new empty one
	}
	e.logger.Info("All remaining memtables processed.")
	if err := e.persistManifest(); err != nil { // Persist manifest after all remaining memtables are flushed
		manifestErr := fmt.Errorf("failed to persist manifest after final flush: %w", err)
		if firstErr == nil {
			firstErr = manifestErr
		}
	}
	return firstErr
}

// purgeWALSegments safely purges WAL segments up to a certain index,
// respecting the configured safety margin (WALPurgeKeepSegments).
func (e *storageEngine) purgeWALSegments(lastFlushedSegment uint64) {
	if lastFlushedSegment == 0 {
		return
	}

	keepCount := uint64(e.opts.WALPurgeKeepSegments)
	if e.opts.WALPurgeKeepSegments < 1 {
		e.logger.Debug("WALPurgeKeepSegments is less than 1, defaulting to 1 for safety.", "configured_value", e.opts.WALPurgeKeepSegments)
		keepCount = 1 // Always keep at least the active segment.
	}

	if lastFlushedSegment <= keepCount {
		e.logger.Debug("Skipping WAL purge, not enough segments to meet safety margin.", "last_flushed_segment", lastFlushedSegment, "keep_count", keepCount)
		return
	}

	purgeUpToIndex := lastFlushedSegment - keepCount
	if err := e.wal.Purge(purgeUpToIndex); err != nil {
		e.logger.Warn("Failed to purge old WAL segments after checkpoint.", "purge_up_to_index", purgeUpToIndex, "error", err)
	} else {
		e.logger.Info("Successfully purged WAL segments.", "purge_up_to_index", purgeUpToIndex)
	}
}
