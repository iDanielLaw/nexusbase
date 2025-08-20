package engine

import (
	"context"
	"fmt"
	"sort"

	"github.com/INLOpen/nexusbase/api/tsdb"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"go.opentelemetry.io/otel/codes"
)

// DeleteSeries marks an entire series for deletion.
func (e *storageEngine) DeleteSeries(ctx context.Context, metric string, tags map[string]string) error {
	if err := e.CheckStarted(); err != nil {
		return err
	}
	_, span := e.tracer.Start(ctx, "StorageEngine.DeleteSeries")
	defer span.End()

	if err := core.ValidateMetricAndTags(e.validator, metric, tags); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid_arguments")
		return err
	}

	// --- Pre-Delete Hook ---
	preDeletePayload := hooks.PreDeleteSeriesPayload{
		Metric: &metric,
		Tags:   &tags,
	}
	preHookEvent := hooks.NewPreDeleteSeriesEvent(preDeletePayload)
	if err := e.hookManager.Trigger(ctx, preHookEvent); err != nil {
		// A pre-hook listener cancelled the operation.
		e.logger.Info("DeleteSeries operation cancelled by PreDeleteSeries hook", "error", err)
		return fmt.Errorf("operation cancelled by pre-hook: %w", err)
	}

	// --- Dictionary Encoding ---
	metricID, ok := e.stringStore.GetID(metric)
	if !ok {
		return nil // Series does not exist, nothing to delete.
	}

	tagsSlicePtr := encodedTagsPool.Get().(*[]core.EncodedSeriesTagPair)
	defer func() {
		*tagsSlicePtr = (*tagsSlicePtr)[:0]
		encodedTagsPool.Put(tagsSlicePtr)
	}()
	encodedTags := *tagsSlicePtr

	for k, v := range tags {
		keyID, ok := e.stringStore.GetID(k)
		if !ok {
			return nil
		}
		valueID, ok := e.stringStore.GetID(v)
		if !ok {
			return nil
		}
		encodedTags = append(encodedTags, core.EncodedSeriesTagPair{KeyID: keyID, ValueID: valueID})
	}
	*tagsSlicePtr = encodedTags
	sort.Slice(encodedTags, func(i, j int) bool {
		return encodedTags[i].KeyID < encodedTags[j].KeyID
	})
	// --- End Dictionary Encoding ---

	seriesKeyBuf := core.GetBuffer()
	defer core.PutBuffer(seriesKeyBuf)
	core.EncodeSeriesKeyToBuffer(seriesKeyBuf, metricID, encodedTags)
	seriesKeyBytes := seriesKeyBuf.Bytes()
	seriesKeyStr := string(seriesKeyBytes)

	// To prevent deadlock with Put (which locks activeSeries then seriesIDStore),
	// we must acquire locks in the same order.
	e.activeSeriesMu.Lock()
	defer e.activeSeriesMu.Unlock()

	seriesID, found := e.seriesIDStore.GetID(seriesKeyStr) // This now happens under the activeSeriesMu lock
	if !found {
		return nil // Series does not exist, nothing to do.
	}

	currentSeqNum := e.sequenceNumber.Add(1)

	// Write tombstone to WAL
	walEntry := core.WALEntry{
		EntryType:    core.EntryTypeDeleteSeries,
		Key:          seriesKeyBytes,
		SeqNum:       currentSeqNum,
		SegmentIndex: e.wal.ActiveSegmentIndex(),
	}
	if err := e.wal.Append(walEntry); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "wal_append_failed")
		return fmt.Errorf("failed to write series tombstone to WAL: %w", err)
	}

	// Add to in-memory deleted series map
	e.deletedSeriesMu.Lock()
	e.deletedSeries[seriesKeyStr] = currentSeqNum
	e.deletedSeriesMu.Unlock()

	// Remove from active series tracking (we already hold the lock)
	delete(e.activeSeries, seriesKeyStr)

	// Also remove it from the in-memory tag index immediately for query consistency.
	e.tagIndexManager.RemoveSeries(seriesID)

	// Publish the update
	e.pubsub.Publish(&tsdb.DataPointUpdate{
		UpdateType: tsdb.DataPointUpdate_DELETE_SERIES,
		Metric:     metric,
		Tags:       tags,
	})

	// --- Post-Delete Hook ---
	postDeletePayload := hooks.PostDeleteSeriesPayload{
		Metric:    metric,
		Tags:      tags,
		SeriesKey: seriesKeyStr,
	}
	postHookEvent := hooks.NewPostDeleteSeriesEvent(postDeletePayload)
	e.hookManager.Trigger(ctx, postHookEvent)
	return nil
}

// DeletesByTimeRange marks a range of data points within a series for deletion.
func (e *storageEngine) DeletesByTimeRange(ctx context.Context, metric string, tags map[string]string, startTime, endTime int64) error {
	if err := e.CheckStarted(); err != nil {
		return err
	}
	_, span := e.tracer.Start(ctx, "StorageEngine.DeletesByTimeRange")
	defer span.End()

	if err := core.ValidateMetricAndTags(e.validator, metric, tags); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid_arguments")
		return err
	}
	if startTime > endTime {
		return fmt.Errorf("start time cannot be after end time")
	}

	// --- Pre-Delete Hook ---
	preDeletePayload := hooks.PreDeleteRangePayload{
		Metric:    &metric,
		Tags:      &tags,
		StartTime: &startTime,
		EndTime:   &endTime,
	}
	preHookEvent := hooks.NewPreDeleteRangeEvent(preDeletePayload)
	if err := e.hookManager.Trigger(ctx, preHookEvent); err != nil {
		// A pre-hook listener cancelled the operation.
		e.logger.Info("DeletesByTimeRange operation cancelled by PreDeleteRange hook", "error", err)
		return fmt.Errorf("operation cancelled by pre-hook: %w", err)
	}

	// --- Dictionary Encoding ---
	metricID, ok := e.stringStore.GetID(metric)
	if !ok {
		return nil // Series does not exist, nothing to delete.
	}

	tagsSlicePtr := encodedTagsPool.Get().(*[]core.EncodedSeriesTagPair)
	defer func() {
		*tagsSlicePtr = (*tagsSlicePtr)[:0]
		encodedTagsPool.Put(tagsSlicePtr)
	}()
	encodedTags := *tagsSlicePtr

	for k, v := range tags {
		keyID, ok := e.stringStore.GetID(k)
		if !ok {
			return nil
		}
		valueID, ok := e.stringStore.GetID(v)
		if !ok {
			return nil
		}
		encodedTags = append(encodedTags, core.EncodedSeriesTagPair{KeyID: keyID, ValueID: valueID})
	}
	*tagsSlicePtr = encodedTags
	sort.Slice(encodedTags, func(i, j int) bool {
		return encodedTags[i].KeyID < encodedTags[j].KeyID
	})
	// --- End Dictionary Encoding ---

	seriesKeyBuf := core.GetBuffer()
	defer core.PutBuffer(seriesKeyBuf)
	core.EncodeSeriesKeyToBuffer(seriesKeyBuf, metricID, encodedTags)
	seriesKeyBytes := seriesKeyBuf.Bytes()
	seriesKeyStr := string(seriesKeyBytes)

	currentSeqNum := e.sequenceNumber.Add(1)

	// Write tombstone to WAL
	walEntry := core.WALEntry{
		EntryType:    core.EntryTypeDeleteRange,
		Key:          seriesKeyBytes,
		Value:        core.EncodeRangeTombstoneValue(startTime, endTime),
		SeqNum:       currentSeqNum,
		SegmentIndex: e.wal.ActiveSegmentIndex(),
	}
	if err := e.wal.Append(walEntry); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "wal_append_failed")
		return fmt.Errorf("failed to write range tombstone to WAL: %w", err)
	}

	// Add to in-memory range tombstones map
	e.rangeTombstonesMu.Lock()
	e.rangeTombstones[seriesKeyStr] = append(e.rangeTombstones[seriesKeyStr], core.RangeTombstone{
		MinTimestamp: startTime,
		MaxTimestamp: endTime,
		SeqNum:       currentSeqNum,
	})
	e.rangeTombstonesMu.Unlock()

	// Publish the update
	e.pubsub.Publish(&tsdb.DataPointUpdate{
		UpdateType: tsdb.DataPointUpdate_DELETE_RANGE,
		Metric:     metric,
		Tags:       tags,
		Timestamp:  startTime,        // Use start time as the representative timestamp
		Value:      float64(endTime), // Use end time as the representative value
	})

	// --- Post-Delete Hook ---
	postDeletePayload := hooks.PostDeleteRangePayload{
		Metric:    metric,
		Tags:      tags,
		SeriesKey: seriesKeyStr,
		StartTime: startTime,
		EndTime:   endTime,
	}
	e.hookManager.Trigger(ctx, hooks.NewPostDeleteRangeEvent(postDeletePayload))

	return nil
}

// isSeriesDeleted checks if a series key is marked as deleted.
// It checks if the data point's sequence number is older than or equal to the deletion sequence number.
func (e *storageEngine) isSeriesDeleted(seriesKey []byte, dataPointSeqNum uint64) bool {
	e.deletedSeriesMu.RLock()
	defer e.deletedSeriesMu.RUnlock()
	if deletionSeqNum, ok := e.deletedSeries[string(seriesKey)]; ok {
		return dataPointSeqNum <= deletionSeqNum
	}
	return false
}

// isCoveredByRangeTombstone checks if a data point at a given timestamp is covered by any range tombstone for its series.
// It checks if the data point's sequence number is older than or equal to the deletion sequence number.
func (e *storageEngine) isCoveredByRangeTombstone(seriesKey []byte, timestamp int64, dataPointSeqNum uint64) bool {
	e.rangeTombstonesMu.RLock()
	defer e.rangeTombstonesMu.RUnlock()
	if tombstones, ok := e.rangeTombstones[string(seriesKey)]; ok {
		for _, ts := range tombstones {
			if timestamp >= ts.MinTimestamp && timestamp <= ts.MaxTimestamp {
				if dataPointSeqNum <= ts.SeqNum {
					return true
				}
			}
		}
	}
	return false
}
