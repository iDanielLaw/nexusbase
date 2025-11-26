package engine

import (
	"context"
	"fmt"
	"sort"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/memtable"
	pb "github.com/INLOpen/nexusbase/replication/proto"
)

// ApplyReplicatedEntry applies a change from the leader without writing to its own WAL.
// This is the core of the follower's write path.
func (e *storageEngine) ApplyReplicatedEntry(ctx context.Context, entry *pb.WALEntry) (err error) {
	if err := e.CheckStarted(); err != nil {
		return err
	}

	// This method should only be called on a follower.
	if e.replicationMode != "follower" {
		return fmt.Errorf("ApplyReplicatedEntry called on a non-follower node (mode: %s)", e.replicationMode)
	}

	// Defer a function to capture any error and increment the error metric.
	defer func() {
		if err != nil && e.metrics.ReplicationErrorsTotal != nil {
			e.metrics.ReplicationErrorsTotal.Add(1)
		}
	}()

	e.mu.Lock()
	defer e.mu.Unlock()

	// Process based on entry type
	switch entry.GetEntryType() {
	case pb.WALEntry_PUT_EVENT:
		err = e.applyPutEvent(ctx, entry)
	case pb.WALEntry_DELETE_SERIES:
		err = e.applyDeleteSeries(ctx, entry)
	case pb.WALEntry_DELETE_RANGE:
		err = e.applyDeleteRange(ctx, entry)
	default:
		err = fmt.Errorf("unknown replicated entry type: %v", entry.GetEntryType())
	}

	if err != nil {
		return fmt.Errorf("failed to apply replicated entry with seq_num %d: %w", entry.GetSequenceNumber(), err)
	}

	// Atomically update the engine's sequence number to track the leader's state.
	e.sequenceNumber.Store(entry.GetSequenceNumber())

	// Handle memtable flush if it's full. This logic is the same as in PutBatch.
	if e.mutableMemtable.IsFull() {
		// Only the leader needs to associate the memtable with a WAL segment for checkpointing.
		if e.replicationMode != "follower" {
			e.mutableMemtable.LastWALSegmentIndex = e.wal.ActiveSegmentIndex()
		}
		e.immutableMemtables = append(e.immutableMemtables, e.mutableMemtable)
		e.mutableMemtable = memtable.NewMemtable2(e.opts.MemtableThreshold, e.clock)
		select {
		case e.flushChan <- struct{}{}:
		default:
		}
	}

	return nil
}

// GetLatestAppliedSeqNum returns the latest sequence number that has been successfully applied from the leader.
func (e *storageEngine) GetLatestAppliedSeqNum() uint64 {
	return e.sequenceNumber.Load()
}

// ReplaceWithSnapshot is a destructive operation that replaces the engine's entire state
// with a snapshot from the leader. It assumes the engine is already closed.
func (e *storageEngine) ReplaceWithSnapshot(snapshotDir string) error {
	e.logger.Info("Replacing engine state with snapshot", "snapshot_dir", snapshotDir)

	// 1. Wipe the current data directory clean.
	if err := e.wipeDataDirectory(); err != nil {
		return fmt.Errorf("failed to wipe data directory for snapshot restore: %w", err)
	}

	// 2. Use the snapshot manager to copy and restore the state.
	if err := e.snapshotManager.RestoreFrom(context.Background(), snapshotDir); err != nil {
		return fmt.Errorf("snapshot manager failed to restore from snapshot: %w", err)
	}

	e.logger.Info("Successfully replaced engine state with snapshot. Engine is ready to be started.")
	return nil
}

// --- Internal apply helpers ---

func (e *storageEngine) applyPutEvent(ctx context.Context, entry *pb.WALEntry) error {
	// 1. Get string IDs for metric and tags, creating them if they don't exist.
	// Batch-create missing IDs to reduce per-string write+sync overhead.
	var idMap map[string]uint64
	if e.stringStore != nil {
		unique := make(map[string]struct{})
		unique[entry.GetMetric()] = struct{}{}
		for k, v := range entry.GetTags() {
			unique[k] = struct{}{}
			unique[v] = struct{}{}
		}
		list := make([]string, 0, len(unique))
		for s := range unique {
			list = append(list, s)
		}
		idMap = make(map[string]uint64)
		e.ensureIDs(idMap, list)
	}

	metricID, err := e.getOrCreateIDFromMap(idMap, entry.GetMetric())
	if err != nil {
		return fmt.Errorf("failed to encode metric '%s': %w", entry.GetMetric(), err)
	}

	tagsSlicePtr := encodedTagsPool.Get().(*[]core.EncodedSeriesTagPair)
	defer func() {
		*tagsSlicePtr = (*tagsSlicePtr)[:0]
		encodedTagsPool.Put(tagsSlicePtr)
	}()
	encodedTags := *tagsSlicePtr

	tags := entry.GetTags()
	for k, v := range tags {
		keyID, err_k := e.getOrCreateIDFromMap(idMap, k)
		if err_k != nil {
			return fmt.Errorf("failed to encode tag key '%s': %w", k, err_k)
		}
		valueID, err_v := e.getOrCreateIDFromMap(idMap, v)
		if err_v != nil {
			return fmt.Errorf("failed to encode tag value '%s': %w", v, err_v)
		}
		encodedTags = append(encodedTags, core.EncodedSeriesTagPair{KeyID: keyID, ValueID: valueID})
	}
	sort.Slice(encodedTags, func(i, j int) bool {
		return encodedTags[i].KeyID < encodedTags[j].KeyID
	})
	*tagsSlicePtr = encodedTags

	// 2. Encode keys
	keyBuf := core.GetBuffer()
	defer core.PutBuffer(keyBuf)
	core.EncodeTSDBKeyToBuffer(keyBuf, metricID, encodedTags, entry.GetTimestamp())
	encodedKey := keyBuf.Bytes()

	// 3. Encode value
	fields, err := core.NewFieldValuesFromMap(entry.GetFields().AsMap())
	if err != nil {
		return fmt.Errorf("failed to create fields from replicated data: %w", err)
	}
	valueBytes, err := fields.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode fields: %w", err)
	}

	// 4. Put into memtable. We must copy the key/value slices as the memtable will hold them.
	keyCopy := make([]byte, len(encodedKey))
	copy(keyCopy, encodedKey)
	valueCopy := make([]byte, len(valueBytes))
	copy(valueCopy, valueBytes)

	if err := e.mutableMemtable.PutRaw(keyCopy, valueCopy, core.EntryTypePutEvent, entry.GetSequenceNumber()); err != nil {
		e.logger.Error("CRITICAL: Failed to put replicated entry into mutable memtable.", "key", string(keyCopy), "error", err)
		return fmt.Errorf("CRITICAL INCONSISTENCY: failed to put replicated data into memtable: %w", err)
	}

	// 5. Update series tracking and tag index
	seriesKeyBuf := core.GetBuffer()
	defer core.PutBuffer(seriesKeyBuf)
	core.EncodeSeriesKeyToBuffer(seriesKeyBuf, metricID, encodedTags)
	seriesKeyStr := string(seriesKeyBuf.Bytes())

	e.addActiveSeries(seriesKeyStr)
	seriesID, err := e.seriesIDStore.GetOrCreateID(seriesKeyStr)
	if err != nil {
		return fmt.Errorf("failed to get series ID for replicated entry: %w", err)
	}

	e.tagIndexManagerMu.Lock()
	if err := e.tagIndexManager.Add(seriesID, tags); err != nil {
		e.logger.Error("Failed to update tag index for replicated entry", "seriesID", seriesID, "error", err)
	}
	e.tagIndexManagerMu.Unlock()

	if e.metrics.ReplicationPutTotal != nil {
		e.metrics.ReplicationPutTotal.Add(1)
	}

	return nil
}

func (e *storageEngine) applyDeleteSeries(ctx context.Context, entry *pb.WALEntry) error {
	seriesKey, err := e.encodeSeriesKeyFromProto(entry.GetMetric(), entry.GetTags())
	if err != nil {
		return fmt.Errorf("could not encode series key for replicated delete: %w", err)
	}
	seriesKeyStr := string(seriesKey)

	// To prevent deadlock, acquire locks in the same order as other operations.
	e.activeSeriesMu.Lock()
	defer e.activeSeriesMu.Unlock()

	seriesID, found := e.seriesIDStore.GetID(seriesKeyStr)
	if !found {
		// If the series doesn't exist on the follower, there's nothing to delete.
		e.logger.Info("Replicated DeleteSeries for a non-existent series, skipping.", "seriesKey", seriesKeyStr)
		return nil
	}

	// Add to in-memory deleted series map
	e.deletedSeriesMu.Lock()
	e.deletedSeries[seriesKeyStr] = entry.GetSequenceNumber()
	e.deletedSeriesMu.Unlock()

	// Remove from active series tracking
	delete(e.activeSeries, seriesKeyStr)

	// Remove from the in-memory tag index
	e.tagIndexManager.RemoveSeries(seriesID)

	e.logger.Info("Applied replicated DeleteSeries", "seriesKey", seriesKeyStr, "seqNum", entry.GetSequenceNumber())
	if e.metrics.ReplicationDeleteSeriesTotal != nil {
		e.metrics.ReplicationDeleteSeriesTotal.Add(1)
	}
	return nil
}

func (e *storageEngine) applyDeleteRange(ctx context.Context, entry *pb.WALEntry) error {
	seriesKey, err := e.encodeSeriesKeyFromProto(entry.GetMetric(), entry.GetTags())
	if err != nil {
		return fmt.Errorf("could not encode series key for replicated range delete: %w", err)
	}

	// Add to in-memory range tombstones map
	e.rangeTombstonesMu.Lock()
	keyStr := string(seriesKey)
	e.rangeTombstones[keyStr] = append(e.rangeTombstones[keyStr], core.RangeTombstone{
		MinTimestamp: entry.GetStartTime(),
		MaxTimestamp: entry.GetEndTime(),
		SeqNum:       entry.GetSequenceNumber(),
	})
	e.rangeTombstonesMu.Unlock()

	e.logger.Info("Applied replicated DeleteRange", "seriesKey", keyStr, "start", entry.GetStartTime(), "end", entry.GetEndTime(), "seqNum", entry.GetSequenceNumber())
	if e.metrics.ReplicationDeleteRangeTotal != nil {
		e.metrics.ReplicationDeleteRangeTotal.Add(1)
	}
	return nil
}

// encodeSeriesKeyFromProto is a helper to derive the binary series key from proto fields.
func (e *storageEngine) encodeSeriesKeyFromProto(metric string, tags map[string]string) ([]byte, error) {
	// Batch-create IDs for metric and tags to reduce write+sync overhead.
	var idMap map[string]uint64
	if e.stringStore != nil {
		unique := make(map[string]struct{})
		unique[metric] = struct{}{}
		for k, v := range tags {
			unique[k] = struct{}{}
			unique[v] = struct{}{}
		}
		list := make([]string, 0, len(unique))
		for s := range unique {
			list = append(list, s)
		}
		idMap = make(map[string]uint64)
		e.ensureIDs(idMap, list)
	}

	metricID, err := e.getOrCreateIDFromMap(idMap, metric)
	if err != nil {
		return nil, fmt.Errorf("failed to encode metric '%s': %w", metric, err)
	}

	tagsSlicePtr := encodedTagsPool.Get().(*[]core.EncodedSeriesTagPair)
	defer func() {
		*tagsSlicePtr = (*tagsSlicePtr)[:0]
		encodedTagsPool.Put(tagsSlicePtr)
	}()
	encodedTags := *tagsSlicePtr

	for k, v := range tags {
		keyID, err_k := e.getOrCreateIDFromMap(idMap, k)
		if err_k != nil {
			return nil, fmt.Errorf("failed to encode tag key '%s': %w", k, err_k)
		}
		valueID, err_v := e.getOrCreateIDFromMap(idMap, v)
		if err_v != nil {
			return nil, fmt.Errorf("failed to encode tag value '%s': %w", v, err_v)
		}
		encodedTags = append(encodedTags, core.EncodedSeriesTagPair{KeyID: keyID, ValueID: valueID})
	}
	sort.Slice(encodedTags, func(i, j int) bool {
		return encodedTags[i].KeyID < encodedTags[j].KeyID
	})
	*tagsSlicePtr = encodedTags

	seriesKeyBuf := core.GetBuffer()
	defer core.PutBuffer(seriesKeyBuf)
	core.EncodeSeriesKeyToBuffer(seriesKeyBuf, metricID, encodedTags)

	seriesKeyCopy := make([]byte, seriesKeyBuf.Len())
	copy(seriesKeyCopy, seriesKeyBuf.Bytes())
	return seriesKeyCopy, nil
}
