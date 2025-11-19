package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/INLOpen/nexusbase/api/tsdb"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexuscore/types"

	"path/filepath"

	"github.com/INLOpen/nexusbase/core" // Import core package
	"github.com/INLOpen/nexusbase/iterator"
	"github.com/INLOpen/nexusbase/memtable" // Required for memtable.NewMemtable
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/RoaringBitmap/roaring/roaring64"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var (
	ErrWritesForbiddenInFollowerMode = errors.New("writes are forbidden in follower mode")
)

// --- Query Helpers ---

// parseDuration extends time.ParseDuration to support days (d), weeks (w), and years (y).
func parseDuration(s string) (time.Duration, error) {
	d, originalErr := time.ParseDuration(s)
	if originalErr == nil {
		return d, nil
	}
	if len(s) < 2 {
		return 0, originalErr
	}
	unit := s[len(s)-1]
	if unit != 'd' && unit != 'w' && unit != 'y' {
		return 0, originalErr
	}
	valueStr := s[:len(s)-1]
	value, err := strconv.ParseInt(valueStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid duration value in %q: %w", s, err)
	}
	var customDuration time.Duration
	switch unit {
	case 'd':
		customDuration = time.Hour * 24 * time.Duration(value)
	case 'w':
		customDuration = time.Hour * 24 * 7 * time.Duration(value)
	case 'y':
		customDuration = time.Hour * 24 * 365 * time.Duration(value)
	}
	return customDuration, nil
}

// getRoundingDuration determines the appropriate rounding duration for caching.
func (e *storageEngine) getRoundingDuration(queryDuration time.Duration) time.Duration {
	// Default rules, sorted from smallest to largest threshold
	defaultRules := []RoundingRule{
		{QueryDurationThreshold: 10 * time.Minute, RoundingDuration: 10 * time.Second},
		{QueryDurationThreshold: 2 * time.Hour, RoundingDuration: 1 * time.Minute},
	}
	// Default rounding for queries longer than any threshold
	const defaultLongerQueryRounding = 5 * time.Minute

	rules := e.opts.RelativeQueryRoundingRules
	if rules == nil {
		rules = defaultRules
	}

	for _, rule := range rules {
		if queryDuration <= rule.QueryDurationThreshold {
			return rule.RoundingDuration
		}
	}
	return defaultLongerQueryRounding
}

// TimeFilterIterator wraps another iterator and filters out items
// that are outside the specified exact time range.
// It implements the low-level iterator.Interface.
type TimeFilterIterator struct {
	underlying     core.IteratorInterface[*core.IteratorNode] // Changed from core.QueryResultIteratorInterface
	exactStartTime int64
	exactEndTime   int64

	// Cached values for the current valid item
	valid     bool
	key       []byte
	value     []byte
	entryType core.EntryType
	seqNum    uint64

	// Pooled buffers to hold the copied key/value, reducing allocations.
	keyBuf *bytes.Buffer
	valBuf *bytes.Buffer
}

var _ core.IteratorInterface[*core.IteratorNode] = (*TimeFilterIterator)(nil)

// NewTimeFilterIterator creates a new TimeFilterIterator.
// It now takes and returns an iterator.Interface.
func NewTimeFilterIterator(iter core.IteratorInterface[*core.IteratorNode], startTime, endTime int64) core.IteratorInterface[*core.IteratorNode] {
	return &TimeFilterIterator{
		underlying:     iter,
		exactStartTime: startTime,
		exactEndTime:   endTime,
	}
}

// Next advances the iterator to the next item that falls within the time range.
func (it *TimeFilterIterator) Next() bool {
	// Return previously used buffers to the pool before getting new ones.
	if it.keyBuf != nil {
		core.PutBuffer(it.keyBuf)
		it.keyBuf = nil
	}
	if it.valBuf != nil {
		core.PutBuffer(it.valBuf)
		it.valBuf = nil
	}

	for it.underlying.Next() {
		// key, value, entryType, seqNum := it.underlying.At()
		cur, err := it.underlying.At()
		if err != nil {
			it.valid = false
			return false
		}
		key, value, entryType, seqNum := cur.Key, cur.Value, cur.EntryType, cur.SeqNum

		if len(key) < 8 {
			continue
		}
		ts, err := core.DecodeTimestamp(key[len(key)-8:])
		if err != nil {
			continue
		}
		if ts >= it.exactStartTime && ts <= it.exactEndTime {
			// Found a valid item, cache it and return true.
			// We must copy the key and value, as the underlying iterator's
			// buffer might be reused on the next call to Next(). This ensures
			// our cached version is stable.
			it.valid = true
			it.keyBuf = core.GetBuffer()
			it.keyBuf.Write(key)
			it.key = it.keyBuf.Bytes()
			it.valBuf = core.GetBuffer()
			it.valBuf.Write(value)
			it.value = it.valBuf.Bytes()
			it.entryType = entryType
			it.seqNum = seqNum
			return true
		}
	}
	// No more valid items found.
	it.valid = false
	return false
}

// At returns the raw data from the underlying iterator.
// It now matches the iterator.Interface signature.
func (it *TimeFilterIterator) At() (*core.IteratorNode, error) {
	if !it.valid {
		return &core.IteratorNode{}, fmt.Errorf("iterator not valid")
	}
	return &core.IteratorNode{
		Key:       it.key,
		Value:     it.value,
		EntryType: it.entryType,
		SeqNum:    it.seqNum,
	}, nil
	// return it.key, it.value, it.entryType, it.seqNum
}

// Error returns any error from the underlying iterator.
func (it *TimeFilterIterator) Error() error {
	return it.underlying.Error()
}

// Close closes the underlying iterator.
func (it *TimeFilterIterator) Close() error {
	// Ensure any held buffers are returned to the pool when the iterator is closed.
	if it.keyBuf != nil {
		core.PutBuffer(it.keyBuf)
		it.keyBuf = nil
	}
	if it.valBuf != nil {
		core.PutBuffer(it.valBuf)
		it.valBuf = nil
	}
	return it.underlying.Close()
}

var (
	// encodedTagsPool provides reusable slices for holding encoded tag pairs,
	// reducing allocations during key creation in high-throughput scenarios like Get/Put.
	encodedTagsPool = sync.Pool{
		New: func() interface{} {
			// Pre-allocate with a reasonable capacity for common tag counts
			slice := make([]core.EncodedSeriesTagPair, 0, 10)
			return &slice
		},
	}
)

// get retrieves a value for a given key from the engine.
// It searches memtables and then SSTables.
func (e *storageEngine) get(ctx context.Context, key []byte) ([]byte, error) {
	_, span := e.tracer.Start(ctx, "StorageEngine.Get")
	startTime := e.clock.Now() // Use the clock interface for time measurement
	defer func() {
		duration := e.clock.Now().Sub(startTime).Seconds()
		observeLatency(e.metrics.GetLatencyHist, duration) // Assumes observeLatency is accessible
		span.SetAttributes(attribute.Float64("duration_seconds", duration))
		span.End()
	}()
	span.SetAttributes(
		attribute.String("db.system", "tsdb-prototype"),
		attribute.String("db.operation", "get"),
		attribute.String("db.key", string(key)),
	)
	e.metrics.GetTotal.Add(1)

	// 1. Check mutable memtable
	e.mu.RLock()
	if e.mutableMemtable != nil {
		if val, entryType, found := e.mutableMemtable.Get(key); found {
			e.mu.RUnlock()
			if entryType == core.EntryTypeDelete {
				span.SetAttributes(attribute.Bool("db.found", false), attribute.String("db.found_reason", "tombstone_memtable"))
				return nil, sstable.ErrNotFound
			}
			span.SetAttributes(attribute.Bool("db.found", true), attribute.String("db.found_in", "mutable_memtable"))
			return val, nil
		}
	}

	// 2. Check immutable memtables (newest to oldest)
	for i := len(e.immutableMemtables) - 1; i >= 0; i-- {
		if val, entryType, found := e.immutableMemtables[i].Get(key); found {
			e.mu.RUnlock()
			if entryType == core.EntryTypeDelete {
				span.SetAttributes(attribute.Bool("db.found", false), attribute.String("db.found_reason", "tombstone_immutable_memtable"))
				return nil, sstable.ErrNotFound
			}
			span.SetAttributes(attribute.Bool("db.found", true), attribute.String("db.found_in", "immutable_memtable"))
			return val, nil
		}
	}
	e.mu.RUnlock()

	// 3. Check SSTables (L0, then L1, L2, ...)
	levelStates, unlockFunc := e.levelsManager.GetSSTablesForRead()
	defer unlockFunc() // Ensure the read lock is always released
	for levelIdx := 0; levelIdx < len(levelStates); levelIdx++ {
		levelTables := levelStates[levelIdx].GetTables()
		if levelIdx == 0 { // L0 is still scanned linearly (as keys can overlap)
			for _, table := range levelTables {
				e.metrics.BloomFilterChecksTotal.Add(1)
				if table.Contains(key) {
					val, entryType, err := table.Get(key)
					if err == nil { // Found in SSTable
						if entryType == core.EntryTypeDelete {
							span.SetAttributes(attribute.Bool("db.found", false), attribute.String("db.found_reason", fmt.Sprintf("tombstone_sstable_L%d_id%d", levelIdx, table.ID())))
							return nil, sstable.ErrNotFound
						}
						span.SetAttributes(attribute.Bool("db.found", true), attribute.String("db.found_in", fmt.Sprintf("sstable_L%d_id%d", levelIdx, table.ID())))
						return val, nil
					}
					if err != sstable.ErrNotFound { // Actual error reading SSTable
						span.RecordError(err)
						span.SetStatus(codes.Error, "sstable_get_error")
						return nil, fmt.Errorf("error getting key %s from sstable %d: %w", string(key), table.ID(), err)
					}
					// If we are here, it means Get returned ErrNotFound, which is a false positive
					e.metrics.BloomFilterFalsePositivesTotal.Add(1)
					span.SetAttributes(attribute.Bool("db.bloom_filter_false_positive", true), attribute.Int64("db.sstable_id", int64(table.ID())))
				}
			}
		} else { // L1+ uses Binary Search (as keys are non-overlapping and sorted by minKey)
			// Find the SSTable that might contain the key using binary search
			// sort.Search returns the smallest index i such that f(i) is true.
			// Here, f(i) is bytes.Compare(levelTables[i].MaxKey(), key) >= 0, meaning
			// we are looking for the first table whose MaxKey is greater than or equal to the search key.
			// This table (or the one before it, if the key is within its range) is the candidate.
			idx := sort.Search(len(levelTables), func(i int) bool {
				return bytes.Compare(levelTables[i].MaxKey(), key) >= 0
			})

			// Check if the found index is valid and the key is within the candidate table's range.
			// The key must be >= candidate.MinKey and <= candidate.MaxKey.
			if idx < len(levelTables) && bytes.Compare(levelTables[idx].MinKey(), key) <= 0 {
				// Key might be in this table
				table := levelTables[idx]
				e.metrics.BloomFilterChecksTotal.Add(1)
				if table.Contains(key) {
					val, entryType, err := table.Get(key)
					if err == nil { // Found in SSTable
						if entryType == core.EntryTypeDelete {
							span.SetAttributes(attribute.Bool("db.found", false), attribute.String("db.found_reason", fmt.Sprintf("tombstone_sstable_L%d_id%d", levelIdx, table.ID())))
							return nil, sstable.ErrNotFound
						}
						span.SetAttributes(attribute.Bool("db.found", true), attribute.String("db.found_in", fmt.Sprintf("sstable_L%d_id%d", levelIdx, table.ID())))
						return val, nil
					}
					if err != sstable.ErrNotFound { // Actual error reading SSTable
						span.RecordError(err)
						span.SetStatus(codes.Error, "sstable_get_error")
						return nil, fmt.Errorf("error getting key %s from sstable %d: %w", string(key), table.ID(), err)
					}
					// If we are here, it means Get returned ErrNotFound, which is a false positive
					e.metrics.BloomFilterFalsePositivesTotal.Add(1)
					span.SetAttributes(attribute.Bool("db.bloom_filter_false_positive", true), attribute.Int64("db.sstable_id", int64(table.ID())))
				}
			}
		}
	}

	span.SetAttributes(attribute.Bool("db.found", false), attribute.String("db.found_reason", "not_found_anywhere"))
	return nil, sstable.ErrNotFound
}

// delete marks a key for deletion in the engine.
// It writes a tombstone to WAL and then to the mutable memtable.
func (e *storageEngine) delete(ctx context.Context, key []byte) error {
	_, span := e.tracer.Start(ctx, "StorageEngine.Delete")
	startTime := e.clock.Now() // Use the clock interface for time measurement
	defer func() {
		duration := e.clock.Now().Sub(startTime).Seconds()
		observeLatency(e.metrics.DeleteLatencyHist, duration) // Assumes observeLatency is accessible
		span.SetAttributes(attribute.Float64("duration_seconds", duration))
		span.End()
	}()
	span.SetAttributes(
		attribute.String("db.system", "tsdb-prototype"),
		attribute.String("db.operation", "delete"),
		attribute.String("db.key", string(key)),
	)

	// Make a copy of the key to ensure it's not pointing to a reusable buffer.
	// This is crucial because the memtable will hold onto this slice.
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	currentSeqNum := e.sequenceNumber.Add(1)
	e.metrics.DeleteTotal.Add(1)

	walEntry := core.WALEntry{EntryType: core.EntryTypeDelete, Key: keyCopy, Value: nil, SeqNum: currentSeqNum}
	if err := e.wal.Append(walEntry); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "wal_append_failed")
		return fmt.Errorf("failed to write to WAL before Delete: %w", err)
	}

	e.mu.Lock()
	if err := e.mutableMemtable.Put(keyCopy, nil, core.EntryTypeDelete, currentSeqNum); err != nil {
		e.mu.Unlock()
		span.RecordError(err)
		span.SetStatus(codes.Error, "memtable_delete_failed")
		return fmt.Errorf("failed to delete (put tombstone) into mutable memtable: %w", err)
	}

	if e.mutableMemtable.IsFull() {
		e.immutableMemtables = append(e.immutableMemtables, e.mutableMemtable)
		e.mutableMemtable = memtable.NewMemtable(e.opts.MemtableThreshold, e.clock)
		select {
		case e.flushChan <- struct{}{}:
		default:
		}
	}
	e.mu.Unlock()
	return nil
}

// rangeScanParams holds parameters for the internal rangeScan method.
type rangeScanParams struct {
	StartKey []byte
	EndKey   []byte
	Order    types.SortOrder
}

// rangeScan provides an iterator over a range of keys.
func (e *storageEngine) rangeScan(ctx context.Context, params rangeScanParams) (core.IteratorInterface[*core.IteratorNode], error) {
	_, span := e.tracer.Start(ctx, "StorageEngine.RangeScan")
	startTime := e.clock.Now()
	defer func() {
		duration := e.clock.Now().Sub(startTime).Seconds()
		if e.metrics != nil && e.metrics.RangeScanLatencyHist != nil {
			observeLatency(e.metrics.RangeScanLatencyHist, duration)
		}
		span.SetAttributes(attribute.Float64("duration_seconds", duration))
		span.End()
	}()

	span.SetAttributes(
		attribute.String("db.system", "tsdb-prototype"),
		attribute.String("db.operation", "rangescan"),
		attribute.String("db.rangescan.start_key", string(params.StartKey)),
		attribute.String("db.rangescan.end_key", string(params.EndKey)),
	)

	var iterators []core.IteratorInterface[*core.IteratorNode]

	e.mu.RLock()
	if e.mutableMemtable != nil {
		// Assuming memtable.NewIterator now accepts an order parameter.
		mutableIter := e.mutableMemtable.NewIterator(params.StartKey, params.EndKey, params.Order)
		iterators = append(iterators, mutableIter)
	}
	for i := len(e.immutableMemtables) - 1; i >= 0; i-- {
		// Assuming memtable.NewIterator now accepts an order parameter.
		immutableIter := e.immutableMemtables[i].NewIterator(params.StartKey, params.EndKey, params.Order)
		iterators = append(iterators, immutableIter)
	}
	e.mu.RUnlock()

	levelStates, unlockFunc := e.levelsManager.GetSSTablesForRead()
	defer unlockFunc()
	for _, levelState := range levelStates {
		levelTables := levelState.GetTables()
		for _, table := range levelTables {
			if (params.EndKey != nil && bytes.Compare(table.MinKey(), params.EndKey) >= 0) ||
				(params.StartKey != nil && bytes.Compare(table.MaxKey(), params.StartKey) < 0 && len(table.MaxKey()) > 0) {
				continue
			}
			// Assuming sstable.NewIterator now accepts an order parameter.
			sstIter, err := table.NewIterator(params.StartKey, params.EndKey, nil, params.Order) // Pass nil semaphore for user queries
			if err != nil {
				for _, it := range iterators {
					it.Close()
				}
				span.RecordError(err)
				span.SetStatus(codes.Error, "sstable_iterator_creation_failed")
				return nil, fmt.Errorf("failed to create iterator for sstable %d: %w", table.ID(), err)
			}
			iterators = append(iterators, sstIter)
		}
	}

	mergeParams := iterator.MergingIteratorParams{
		Iters:                iterators,
		StartKey:             params.StartKey,
		EndKey:               params.EndKey,
		Order:                params.Order,
		IsSeriesDeleted:      e.isSeriesDeleted,
		IsRangeDeleted:       e.isCoveredByRangeTombstone,
		ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
		DecodeTsFunc:         core.DecodeTimestamp,
	}
	mergedIter, err := iterator.NewMergingIteratorWithTombstones(mergeParams)
	if err != nil {
		for _, it := range iterators {
			it.Close()
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, "merging_iterator_creation_failed")
		return nil, fmt.Errorf("failed to create merging iterator with tombstones: %w", err)
	}

	return mergedIter, nil // Return the mergedIter directly after it handles skipping
}

// prepareDataPoint handles all pre-processing for a single data point before it's
// committed to the WAL and memtable. This includes hooks, validation, and encoding.
func (e *storageEngine) prepareDataPoint(ctx context.Context, p core.DataPoint) (core.WALEntry, *tsdb.DataPointUpdate, hooks.PostPutDataPointPayload, error) {
	// Use local variables for the data, which can be modified by hooks.
	metric, tags, timestamp, fields := p.Metric, p.Tags, p.Timestamp, p.Fields

	// --- Pre-Put Hook ---
	prePutPayload := hooks.PrePutDataPointPayload{
		Metric:    &metric,
		Tags:      &tags,
		Timestamp: &timestamp,
		Fields:    &fields,
	}
	if err := e.hookManager.Trigger(ctx, hooks.NewPrePutDataPointEvent(prePutPayload)); err != nil {
		return core.WALEntry{}, nil, hooks.PostPutDataPointPayload{}, fmt.Errorf("operation cancelled by pre-hook: %w", err)
	}

	// --- Validation ---
	if err := core.ValidateMetricAndTags(e.validator, metric, tags); err != nil {
		return core.WALEntry{}, nil, hooks.PostPutDataPointPayload{}, err
	}

	// --- Dictionary & Key Encoding ---
	metricID, err := e.stringStore.GetOrCreateID(metric)
	if err != nil {
		return core.WALEntry{}, nil, hooks.PostPutDataPointPayload{}, fmt.Errorf("failed to encode metric '%s': %w", metric, err)
	}

	// Get a pooled slice for encoded tags

	// Get a pooled slice for encoded tags
	tagsSlicePtr := encodedTagsPool.Get().(*[]core.EncodedSeriesTagPair)
	defer func() {
		*tagsSlicePtr = (*tagsSlicePtr)[:0]
		encodedTagsPool.Put(tagsSlicePtr)
	}()
	encodedTags := *tagsSlicePtr

	for k, v := range tags {
		keyID, err_k := e.stringStore.GetOrCreateID(k)
		if err_k != nil {
			return core.WALEntry{}, nil, hooks.PostPutDataPointPayload{}, fmt.Errorf("failed to encode tag key '%s': %w", k, err_k)
		}
		valueID, err_v := e.stringStore.GetOrCreateID(v)
		if err_v != nil {
			return core.WALEntry{}, nil, hooks.PostPutDataPointPayload{}, fmt.Errorf("failed to encode tag value '%s': %w", v, err_v)
		}
		encodedTags = append(encodedTags, core.EncodedSeriesTagPair{KeyID: keyID, ValueID: valueID})
	}
	*tagsSlicePtr = encodedTags

	sort.Slice(encodedTags, func(i, j int) bool {
		return encodedTags[i].KeyID < encodedTags[j].KeyID
	})

	seriesKeyBuf := core.GetBuffer()
	defer core.PutBuffer(seriesKeyBuf)
	core.EncodeSeriesKeyToBuffer(seriesKeyBuf, metricID, encodedTags)
	seriesKeyBytes := seriesKeyBuf.Bytes()
	seriesKeyStr := string(seriesKeyBytes)

	keyBuf := core.GetBuffer()
	defer core.PutBuffer(keyBuf)
	core.EncodeTSDBKeyToBuffer(keyBuf, metricID, encodedTags, timestamp)
	encodedKey := keyBuf.Bytes()

	// --- Series Tracking & Indexing ---

	e.addActiveSeries(seriesKeyStr)
	seriesID, err := e.seriesIDStore.GetOrCreateID(seriesKeyStr)
	if err != nil {
		return core.WALEntry{}, nil, hooks.PostPutDataPointPayload{}, fmt.Errorf("failed to get series ID: %w", err)
	}

	// Update tag index via the new manager
	e.tagIndexManagerMu.Lock()
	if err := e.tagIndexManager.Add(seriesID, tags); err != nil {
		e.logger.Error("Failed to update tag index", "seriesID", seriesID, "error", err)
	}
	e.tagIndexManagerMu.Unlock()

	// --- Prepare WAL Entry ---

	bufFields, err := fields.Encode()
	if err != nil {
		return core.WALEntry{}, nil, hooks.PostPutDataPointPayload{}, fmt.Errorf("failed to encode fields: %w", err)
	}
	keyCopy := make([]byte, len(encodedKey))
	copy(keyCopy, encodedKey)
	valueCopy := make([]byte, len(bufFields))
	copy(valueCopy, bufFields)

	walEntry := core.WALEntry{
		EntryType: core.EntryTypePutEvent,
		Key:       keyCopy,
		Value:     valueCopy,
		// SeqNum is assigned later, just before writing to WAL.
	}

	// --- Prepare Post-operation data ---
	// For pub/sub, we need to decide which field to publish.
	// A common convention is to publish a field named "value" if it exists.
	var pubsubValue float64
	if fv, ok := fields["value"]; ok {
		if floatVal, isFloat := fv.ValueFloat64(); isFloat {
			pubsubValue = floatVal
		}
	}
	pubsubUpdate := &tsdb.DataPointUpdate{
		UpdateType: tsdb.DataPointUpdate_PUT,
		Metric:     metric,
		Tags:       tags,
		Timestamp:  timestamp,
		Value:      pubsubValue,
	}
	// Create the payload for the post-write hook using the final data.
	postHookPayload := hooks.PostPutDataPointPayload{
		Metric:    metric,
		Tags:      tags,
		Timestamp: timestamp,
		Fields:    fields,
	}

	return walEntry, pubsubUpdate, postHookPayload, nil
}

// Put is a TSDB-specific API to put a single data point.
// It's a convenience wrapper around PutBatch.
func (e *storageEngine) Put(ctx context.Context, point core.DataPoint) error {
	if err := e.CheckStarted(); err != nil {
		return err
	}
	if e.replicationMode == "follower" {
		return ErrWritesForbiddenInFollowerMode
	}
	return e.PutBatch(ctx, []core.DataPoint{point})
}

// PutBatch puts multiple data points into the engine in a single call.
// It iterates through the points and calls the single Put method for each.
// This is the primary write path; single Put calls are wrapped by this method.
func (e *storageEngine) PutBatch(ctx context.Context, points []core.DataPoint) (err error) {
	if e.putBatchInterceptor != nil {
		return e.putBatchInterceptor(ctx, points)
	}
	// Defer a function to capture any error and increment the error metric.
	defer func() {
		if err != nil && e.metrics.PutErrorsTotal != nil {
			e.metrics.PutErrorsTotal.Add(1)
		}
	}()

	if errCheck := e.CheckStarted(); errCheck != nil {
		return errCheck
	}
	if e.replicationMode == "follower" {
		return ErrWritesForbiddenInFollowerMode
	}
	startTime := e.clock.Now()
	defer func() {
		duration := e.clock.Now().Sub(startTime).Seconds()
		if e.metrics != nil && e.metrics.PutLatencyHist != nil {
			observeLatency(e.metrics.PutLatencyHist, duration)
		}
	}()
	_, span := e.tracer.Start(ctx, "StorageEngine.PutBatch")
	defer span.End()
	span.SetAttributes(attribute.Int("batch.size", len(points)))

	if len(points) == 0 {
		return nil
	}

	// --- Pre-Batch Hook ---
	// The payload contains a pointer to the slice, allowing hooks to modify the batch.
	preBatchPayload := hooks.PrePutBatchPayload{Points: &points}
	if hookErr := e.hookManager.Trigger(ctx, hooks.NewPrePutBatchEvent(preBatchPayload)); hookErr != nil {
		return fmt.Errorf("put batch cancelled by pre-hook: %w", hookErr)
	}

	// Defer the Post-Batch hook. It will run after the function completes and
	// will have access to the final error state via the named return value `err`.
	defer func() {
		postBatchPayload := hooks.PostPutBatchPayload{
			Points: points, // Use the (potentially modified) points slice
			Error:  err,
		}
		// Post-hooks are typically async and don't return errors.
		e.hookManager.Trigger(ctx, hooks.NewPostPutBatchEvent(postBatchPayload))
	}()

	// 1. Prepare all data points for commit. This includes running hooks and encoding.
	walEntries := make([]core.WALEntry, 0, len(points))
	pubsubUpdates := make([]*tsdb.DataPointUpdate, 0, len(points))
	postHookPayloads := make([]hooks.PostPutDataPointPayload, 0, len(points))

	for i, p := range points {
		walEntry, pubsubUpdate, postHookPayload, err := e.prepareDataPoint(ctx, p)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "put_batch_failed")
			span.SetAttributes(attribute.Int("batch.failed_index", i))
			err = fmt.Errorf("failed to prepare point %d in batch (metric: %s): %w", i, p.Metric, err)
			return err
		}
		walEntries = append(walEntries, walEntry)
		pubsubUpdates = append(pubsubUpdates, pubsubUpdate)
		postHookPayloads = append(postHookPayloads, postHookPayload)
	}

	// --- Pre-WAL Append Hook ---
	preWALPayload := hooks.WALAppendPayload{Entries: &walEntries}
	if hookErr := e.hookManager.Trigger(ctx, hooks.NewPreWALAppendEvent(preWALPayload)); hookErr != nil {
		err = fmt.Errorf("put batch cancelled by pre-wal-append hook: %w", hookErr)
		span.RecordError(err)
		span.SetStatus(codes.Error, "pre_wal_hook_failed")
		return err
	}

	// 2. Assign sequence numbers to all entries in the batch.
	// This must be done before writing to the WAL.
	for i := range walEntries {
		walEntries[i].SeqNum = e.sequenceNumber.Add(1)
	}

	// 3. Write the entire batch to the WAL as a single, atomic operation.
	// This requires the WAL to support an `AppendBatch` method.
	walErr := e.wal.AppendBatch(walEntries)

	// --- Post-WAL Append Hook ---
	postWALPayload := hooks.PostWALAppendPayload{Entries: walEntries, Error: walErr}
	e.hookManager.Trigger(ctx, hooks.NewPostWALAppendEvent(postWALPayload))

	if walErr != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "wal_append_batch_failed")
		// If the atomic WAL write fails, we can safely return. No state has been changed in the memtable.
		err = fmt.Errorf("failed to write batch to WAL: %w", walErr)
		return err
	}

	// 4. Write all entries to memtable under a single lock.
	// This part is now much safer because the WAL write was atomic. If this fails,
	// the WAL is ahead of the memtable, which is a critical state that must be handled.
	// A common strategy is to panic, as the in-memory state is now inconsistent with the durable log.
	e.mu.Lock()
	defer e.mu.Unlock()

	for i, entry := range walEntries {
		if err := e.mutableMemtable.Put(entry.Key, entry.Value, entry.EntryType, entry.SeqNum); err != nil {
			// This is a critical, unrecoverable state. The WAL contains entries that are not in the memtable.
			// The application should probably panic to force a restart, where it will recover from the WAL.
			e.logger.Error("CRITICAL: In-memory state is now inconsistent with WAL. A memtable write failed after a successful batch WAL write. Manual intervention or a restart is required.", "key", string(entry.Key), "error", err)
			// For now, we return a very explicit error. In a production system, a panic might be more appropriate.
			return fmt.Errorf("CRITICAL INCONSISTENCY: failed to put point %d into mutable memtable after successful WAL batch write: %w", i, err)
		}
	}
	e.metrics.PutTotal.Add(int64(len(points)))

	// 5. Check if memtable needs to be flushed.
	if e.mutableMemtable.IsFull() {
		// Before moving the memtable, record which WAL segment it belongs to.
		// This is crucial for checkpointing.
		e.mutableMemtable.LastWALSegmentIndex = e.wal.ActiveSegmentIndex()

		e.immutableMemtables = append(e.immutableMemtables, e.mutableMemtable)
		e.mutableMemtable = memtable.NewMemtable(e.opts.MemtableThreshold, e.clock)
		select {
		case e.flushChan <- struct{}{}:
		default:
		}
	}

	// 6. After successful memtable write, publish updates and trigger post-hooks asynchronously.
	e.publishAndHook(ctx, pubsubUpdates, postHookPayloads)
	return nil
}

// publishAndHook handles the asynchronous post-write tasks (pub/sub and hooks).
func (e *storageEngine) publishAndHook(ctx context.Context, pubsubUpdates []*tsdb.DataPointUpdate, postHookPayloads []hooks.PostPutDataPointPayload) {
	go func() {
		for _, update := range pubsubUpdates {
			e.pubsub.Publish(update)
		}
		for _, payload := range postHookPayloads {
			e.hookManager.Trigger(ctx, hooks.NewPostPutDataPointEvent(payload))
		}
	}()
}

// Get is a TSDB-specific API to get a single data point.
func (e *storageEngine) Get(ctx context.Context, metric string, tags map[string]string, timestamp int64) (result core.FieldValues, err error) {
	if errCheck := e.CheckStarted(); errCheck != nil {
		err = errCheck
		return
	}

	// --- Pre-Get Hook ---
	preGetPayload := hooks.PreGetPointPayload{
		Metric:    &metric,
		Tags:      &tags,
		Timestamp: &timestamp,
	}
	if hookErr := e.hookManager.Trigger(ctx, hooks.NewPreGetPointEvent(preGetPayload)); hookErr != nil {
		err = fmt.Errorf("get operation cancelled by pre-hook: %w", hookErr)
		return
	}

	// --- Post-Get Hook (deferred) ---
	defer func() {
		postGetPayload := hooks.PostGetPointPayload{
			Metric:    metric, // Use potentially modified values
			Tags:      tags,
			Timestamp: timestamp,
			Result:    &result, // Pointer to the named return value
			Error:     err,     // The final error state
		}
		// Post-hooks are typically async and don't return errors, but they can modify the result.
		e.hookManager.Trigger(ctx, hooks.NewPostGetPointEvent(postGetPayload))
	}()

	if errVal := core.ValidateMetricAndTags(e.validator, metric, tags); errVal != nil {
		err = errVal
		return
	}

	// --- Dictionary Encoding ---
	metricID, ok := e.stringStore.GetID(metric)
	if !ok {
		err = sstable.ErrNotFound
		return
	}

	// Get a pooled slice for encoded tags
	tagsSlicePtr := encodedTagsPool.Get().(*[]core.EncodedSeriesTagPair)
	defer func() {
		// Reset slice to zero length before returning to the pool
		*tagsSlicePtr = (*tagsSlicePtr)[:0]
		encodedTagsPool.Put(tagsSlicePtr)
	}()
	encodedTags := *tagsSlicePtr

	for k, v := range tags {
		keyID, ok := e.stringStore.GetID(k)
		if !ok {
			err = sstable.ErrNotFound
			return
		}
		valueID, ok := e.stringStore.GetID(v)
		if !ok {
			err = sstable.ErrNotFound
			return
		}
		encodedTags = append(encodedTags, core.EncodedSeriesTagPair{KeyID: keyID, ValueID: valueID})
	}
	// Update the pointer in the pool in case the slice was re-allocated by append
	*tagsSlicePtr = encodedTags

	sort.Slice(encodedTags, func(i, j int) bool {
		return encodedTags[i].KeyID < encodedTags[j].KeyID
	})
	// --- End Dictionary Encoding ---

	// Use pooled buffers for key encoding to avoid allocations
	keyBuf := core.GetBuffer()
	defer core.PutBuffer(keyBuf)
	core.EncodeTSDBKeyToBuffer(keyBuf, metricID, encodedTags, timestamp)
	encodedKey := keyBuf.Bytes()

	seriesKeyBuf := core.GetBuffer()
	defer core.PutBuffer(seriesKeyBuf)
	core.EncodeSeriesKeyToBuffer(seriesKeyBuf, metricID, encodedTags)
	seriesKeyBytesForCheck := seriesKeyBuf.Bytes()

	if e.isSeriesDeleted(seriesKeyBytesForCheck, 0) {
		err = sstable.ErrNotFound
		return
	}
	if e.isCoveredByRangeTombstone(seriesKeyBytesForCheck, timestamp, 0) {
		err = sstable.ErrNotFound
		return
	}

	var encodedValue []byte
	encodedValue, err = e.get(ctx, encodedKey) // Call unexported get
	if err != nil {
		return
	}

	result, err = core.DecodeFields(bytes.NewBuffer(encodedValue))
	if err != nil {
		err = fmt.Errorf("failed to decode value for data point (metric: %s, ts: %d): %w", metric, timestamp, err)
		return
	}
	return
}

// Delete is a TSDB-specific API to delete a single data point (creates a point tombstone).
func (e *storageEngine) Delete(ctx context.Context, metric string, tags map[string]string, timestamp int64) (err error) {
	if errCheck := e.CheckStarted(); errCheck != nil {
		err = errCheck
		return
	}
	if e.replicationMode == "follower" {
		err = ErrWritesForbiddenInFollowerMode
		return
	}

	// --- Pre-Delete Hook ---
	preDeletePayload := hooks.PreDeletePointPayload{
		Metric:    &metric,
		Tags:      &tags,
		Timestamp: &timestamp,
	}
	if hookErr := e.hookManager.Trigger(ctx, hooks.NewPreDeletePointEvent(preDeletePayload)); hookErr != nil {
		err = fmt.Errorf("delete operation cancelled by pre-hook: %w", hookErr)
		return
	}

	// --- Post-Delete Hook (deferred) ---
	defer func() {
		postDeletePayload := hooks.PostDeletePointPayload{
			Metric:    metric,
			Tags:      tags,
			Timestamp: timestamp,
			Error:     err,
		}
		e.hookManager.Trigger(ctx, hooks.NewPostDeletePointEvent(postDeletePayload))
	}()

	if errVal := core.ValidateMetricAndTags(e.validator, metric, tags); errVal != nil {
		err = errVal
		return
	}

	// --- Dictionary Encoding ---
	var metricID uint64
	metricID, err = e.stringStore.GetOrCreateID(metric)
	if err != nil {
		err = fmt.Errorf("failed to get or create ID for metric '%s' during delete: %w", metric, err)
		return
	}

	// Get a pooled slice for encoded tags
	tagsSlicePtr := encodedTagsPool.Get().(*[]core.EncodedSeriesTagPair)
	defer func() {
		// Reset slice to zero length before returning to the pool
		*tagsSlicePtr = (*tagsSlicePtr)[:0]
		encodedTagsPool.Put(tagsSlicePtr)
	}()
	encodedTags := *tagsSlicePtr

	for k, v := range tags {
		var keyID, valueID uint64
		keyID, err = e.stringStore.GetOrCreateID(k)
		if err != nil {
			err = fmt.Errorf("failed to get or create ID for tag key '%s' during delete: %w", k, err)
			return
		}
		valueID, err = e.stringStore.GetOrCreateID(v)
		if err != nil {
			err = fmt.Errorf("failed to get or create ID for tag value '%s' during delete: %w", v, err)
			return
		}
		encodedTags = append(encodedTags, core.EncodedSeriesTagPair{KeyID: keyID, ValueID: valueID})
	}
	*tagsSlicePtr = encodedTags // Update pointer in case of reallocation
	sort.Slice(encodedTags, func(i, j int) bool {
		return encodedTags[i].KeyID < encodedTags[j].KeyID
	})
	// --- End Dictionary Encoding ---

	keyBuf := core.GetBuffer()
	defer core.PutBuffer(keyBuf)
	core.EncodeTSDBKeyToBuffer(keyBuf, metricID, encodedTags, timestamp)
	encodedKey := keyBuf.Bytes()

	err = e.delete(ctx, encodedKey) // Call unexported delete with the original err variable
	if err != nil {
		err = fmt.Errorf("failed to delete data point (metric: %s, ts: %d): %w", metric, timestamp, err)
		return
	}

	// Publish the update
	e.pubsub.Publish(&tsdb.DataPointUpdate{
		UpdateType: tsdb.DataPointUpdate_DELETE,
		Metric:     metric,
		Tags:       tags,
		Timestamp:  timestamp,
	})
	return
}

var queryResultItemPool = sync.Pool{
	New: func() interface{} {
		// Pre-allocate maps to reduce allocations during use
		return &core.QueryResultItem{}
	},
}

// getMatchingBinarySeriesKeys finds all series matching the given metric and tags
// and returns their binary-encoded series keys. This is an internal helper for Query.
func (e *storageEngine) getMatchingBinarySeriesKeys(metric string, tags map[string]string) ([][]byte, error) {
	e.tagIndexManagerMu.RLock()
	defer e.tagIndexManagerMu.RUnlock()
	// 1. Query the tag index manager to get a bitmap of matching series based on tags.
	resultBitmap, err := e.tagIndexManager.Query(tags)
	if err != nil {
		return nil, fmt.Errorf("failed to query tag index: %w", err)
	}

	// If tags were provided but no series matched, return early.
	if len(tags) > 0 && (resultBitmap == nil || resultBitmap.IsEmpty()) {
		return nil, nil
	}

	// 2. If a metric is specified, filter the bitmap further.
	if metric != "" {
		metricID, ok := e.stringStore.GetID(metric)
		if !ok {
			return nil, nil // Metric doesn't exist, so no series can match.
		}

		filteredBitmap := roaring64.NewBitmap()

		// If tags were provided, filter the existing bitmap.
		if resultBitmap != nil && !resultBitmap.IsEmpty() {
			iter := resultBitmap.Iterator()
			for iter.HasNext() {
				seriesID := iter.Next()
				seriesKeyBytes, found := e.seriesIDStore.GetKey(seriesID)
				if found {
					mID, _, decodeErr := core.DecodeSeriesKey([]byte(seriesKeyBytes))
					if decodeErr == nil && mID == metricID {
						filteredBitmap.Add(seriesID)
					}
				}
			}
		} else { // No tags were provided, so we need to find all series for this metric.
			// This is the slow path. In a real system, we might have a metric->series index.
			// For now, we iterate all active series.
			e.activeSeriesMu.RLock()
			for seriesKeyStr := range e.activeSeries {
				mID, _, decodeErr := core.DecodeSeriesKey([]byte(seriesKeyStr))
				if decodeErr == nil && mID == metricID {
					if seriesID, found := e.seriesIDStore.GetID(seriesKeyStr); found {
						filteredBitmap.Add(seriesID)
					}
				}
			}
			e.activeSeriesMu.RUnlock()
		}
		resultBitmap = filteredBitmap
	}

	// 3. If no filters were applied at all (no metric, no tags), we should not return everything.
	// The caller (Query) should handle this case. Here we assume at least one filter is present
	// or the caller wants all series matching the (empty) filters.
	// Let's handle the "return all" case here for clarity.
	if metric == "" && len(tags) == 0 {
		// This is a "get all" query. It can be very expensive.
		resultBitmap = roaring64.NewBitmap()
		e.activeSeriesMu.RLock()
		for seriesKeyStr := range e.activeSeries {
			if seriesID, found := e.seriesIDStore.GetID(seriesKeyStr); found {
				resultBitmap.Add(seriesID)
			}
		}
		e.activeSeriesMu.RUnlock()
	}

	if resultBitmap == nil || resultBitmap.IsEmpty() {
		return nil, nil
	}

	// 4. Convert the final list of SeriesIDs to binary series keys.
	finalSeriesKeys := make([][]byte, 0, resultBitmap.GetCardinality())
	iter := resultBitmap.Iterator()
	for iter.HasNext() {
		seriesID := iter.Next()
		seriesKeyStr, found := e.seriesIDStore.GetKey(uint64(seriesID))
		if found {
			// Check if the series is marked as deleted before adding it to the results.
			// This ensures that GetSeriesByTags does not return series that have been deleted.
			e.deletedSeriesMu.RLock()
			_, isDeleted := e.deletedSeries[seriesKeyStr]
			e.deletedSeriesMu.RUnlock()

			if !isDeleted {
				finalSeriesKeys = append(finalSeriesKeys, []byte(seriesKeyStr))
			}
		}
	}

	return finalSeriesKeys, nil
}

// The signature is updated to use core.QueryParams struct.
func (e *storageEngine) Query(ctx context.Context, params core.QueryParams) (iter core.QueryResultIteratorInterface, err error) {
	// Increment active queries gauge
	if e.metrics.ActiveQueries != nil {
		e.metrics.ActiveQueries.Add(1)
		defer e.metrics.ActiveQueries.Add(-1)
	}
	// Defer a function to capture any error and increment the error metric.
	defer func() {
		if err != nil && e.metrics.QueryErrorsTotal != nil {
			e.metrics.QueryErrorsTotal.Add(1)
		}
	}()

	if err := e.CheckStarted(); err != nil {
		return nil, err
	}
	if err := core.ValidateMetricAndTags(e.validator, params.Metric, params.Tags); err != nil {
		return nil, err
	}

	// --- Time Resolution & Rounding ---
	var exactStartTime, exactEndTime int64
	var isCacheableQuery bool

	// Resolve relative time to absolute time here, inside the engine.
	if params.IsRelative {
		isCacheableQuery = true
		duration, err := parseDuration(params.RelativeDuration)
		if err != nil {
			return nil, fmt.Errorf("invalid relative duration: %w", err)
		}

		now := e.clock.Now()
		exactEndTime = now.UnixNano()
		exactStartTime = now.Add(-duration).UnixNano()

		// Apply rounding for caching (over-fetching).
		roundingDuration := e.getRoundingDuration(duration)
		params.StartTime = now.Add(-duration).Truncate(roundingDuration).UnixNano()
		params.EndTime = now.Truncate(roundingDuration).Add(roundingDuration).UnixNano()
	} else if params.EndTime == 0 {
		// For absolute queries, resolve NOW() if EndTime is 0.
		params.EndTime = e.clock.Now().UnixNano()
	}

	// --- Pre-Query Hook ---
	preQueryPayload := hooks.PreQueryPayload{Params: &params}
	if hookErr := e.hookManager.Trigger(ctx, hooks.NewPreQueryEvent(preQueryPayload)); hookErr != nil {
		return nil, fmt.Errorf("query cancelled by pre-hook: %w", hookErr)
	}

	// Defer the PostQuery hook. It will execute after the function returns.
	// 'err' is a named return value, so the deferred function can access its final state.
	overallQueryStartTime := e.clock.Now()
	defer func() {
		postQueryPayload := hooks.PostQueryPayload{ //nolint:govet
			Params:   params, // Use the potentially modified params
			Duration: e.clock.Now().Sub(overallQueryStartTime),
			Error:    err,
		}
		e.hookManager.Trigger(ctx, hooks.NewPostQueryEvent(postQueryPayload))
	}()

	// Increment the query count metric.
	if e.metrics != nil && e.metrics.QueryTotal != nil {
		e.metrics.QueryTotal.Add(1)
	}

	// If a cursor (AfterKey) is provided, we can optimize the query by updating
	// the StartTime to the timestamp of the cursor. This helps narrow down the
	// initial scan range in SSTables. The actual skipping of the cursor key itself
	// is handled by the executor's SkippingIterator.
	if len(params.AfterKey) > 0 {
		ts, err := core.DecodeTimestamp(params.AfterKey[len(params.AfterKey)-8:])
		if err != nil {
			return nil, fmt.Errorf("invalid cursor: failed to decode timestamp: %w", err)
		}
		params.StartTime = ts
	}

	// --- Dictionary Encoding for Query Params ---
	var metricID uint64 // Used for synthetic key in final aggregation
	if params.Metric != "" {
		var ok bool
		metricID, ok = e.stringStore.GetID(params.Metric)
		if !ok {
			// If metric doesn't exist, no series can match. Return an empty iterator.
			return &QueryResultIterator{underlying: iterator.NewEmptyIterator(), engine: e}, nil
		}
	}
	// --- End Dictionary Encoding ---

	_, span := e.tracer.Start(ctx, "StorageEngine.Query") // Span for the Query setup
	defer span.End()

	// 1. Find all matching series keys (binary format).
	var effectiveIter core.IteratorInterface[*core.IteratorNode]
	isFinalAgg := false

	// Check if this is a multi-field aggregation query (and not a downsampling one)
	if len(params.AggregationSpecs) > 0 && params.DownsampleInterval == "" {
		isFinalAgg = true
		// 1. Find all matching series keys (binary format).
		binarySeriesKeys, err := e.getMatchingBinarySeriesKeys(params.Metric, params.Tags)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed_to_find_matching_series")
			return nil, fmt.Errorf("failed to find matching series for query: %w", err)
		}
		if len(binarySeriesKeys) == 0 {
			return &QueryResultIterator{underlying: iterator.NewEmptyIterator(), engine: e}, nil
		}
		span.SetAttributes(attribute.Int("query.matching_series_count", len(binarySeriesKeys)))

		// 2. Create a rangeScan iterator for each matching series.
		var iteratorsToMerge []core.IteratorInterface[*core.IteratorNode]
		for _, seriesKey := range binarySeriesKeys {
			startKeyBytes := make([]byte, len(seriesKey)+8)
			copy(startKeyBytes, seriesKey)
			binary.BigEndian.PutUint64(startKeyBytes[len(seriesKey):], uint64(params.StartTime))

			endKeyBytes := make([]byte, len(seriesKey)+8) // The end key is exclusive in some contexts, so +1 might be needed.
			copy(endKeyBytes, seriesKey)
			binary.BigEndian.PutUint64(endKeyBytes[len(seriesKey):], uint64(params.EndTime+1))

			scanParams := rangeScanParams{StartKey: startKeyBytes, EndKey: endKeyBytes, Order: params.Order}
			seriesBaseIter, err := e.rangeScan(ctx, scanParams)
			if err != nil {
				// Cleanup previously created iterators
				for _, it := range iteratorsToMerge {
					it.Close()
				}
				return nil, fmt.Errorf("failed to create iterator for series %x: %w", seriesKey, err)
			}
			iteratorsToMerge = append(iteratorsToMerge, seriesBaseIter)
		}

		// 3. Merge the raw data iterators from all series.
		mergeParams := iterator.MergingIteratorParams{
			Iters:                iteratorsToMerge,
			Order:                params.Order,
			IsSeriesDeleted:      e.isSeriesDeleted,
			IsRangeDeleted:       e.isCoveredByRangeTombstone,
			ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
			DecodeTsFunc:         core.DecodeTimestamp,
		}
		baseIter, err := iterator.NewMergingIteratorWithTombstones(mergeParams)
		if err != nil {
			// Cleanup previously created iterators
			for _, it := range iteratorsToMerge {
				it.Close()
			}
			return nil, fmt.Errorf("failed to merge series iterators: %w", err)
		}

		// If this was a relative query, we over-fetched data for caching.
		// Apply the exact time filter BEFORE aggregation.
		if isCacheableQuery {
			baseIter = NewTimeFilterIterator(baseIter, exactStartTime, exactEndTime)
		}

		// For a final aggregation over multiple series, the result key should be synthetic
		// and represent the query, not a specific series. We'll use just the metric.
		syntheticSeriesKey := core.EncodeSeriesKey(metricID, nil)
		const maxSeriesKeyLen = 64 * 1024 // 64KB, adjust as appropriate
		if len(syntheticSeriesKey) > maxSeriesKeyLen {
			return nil, fmt.Errorf("series key too large (%d bytes), max allowed is %d", len(syntheticSeriesKey), maxSeriesKeyLen)
		}
		syntheticQueryStartKey := make([]byte, len(syntheticSeriesKey)+8)
		copy(syntheticQueryStartKey, syntheticSeriesKey)
		binary.BigEndian.PutUint64(syntheticQueryStartKey[len(syntheticSeriesKey):], uint64(params.StartTime))

		// Use the new MultiFieldAggregatingIterator
		aggIter, aggErr := iterator.NewMultiFieldAggregatingIterator(baseIter, params.AggregationSpecs, syntheticQueryStartKey)
		if aggErr != nil {
			if aggIter != nil {
				aggIter.Close()
			}
			baseIter.Close()
			span.RecordError(aggErr)
			span.SetStatus(codes.Error, "aggregating_iterator_creation_failed")
			return nil, fmt.Errorf("failed to create aggregating iterator: %w", aggErr)
		}
		effectiveIter = aggIter
	} else {
		// This is the path for raw data queries or downsampling queries.
		// The logic for this path remains largely the same as before.
		// We need to find matching series and create a merged iterator.
		binarySeriesKeys, err := e.getMatchingBinarySeriesKeys(params.Metric, params.Tags)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed_to_find_matching_series")
			return nil, fmt.Errorf("failed to find matching series for query: %w", err)
		}

		if len(binarySeriesKeys) == 0 {
			return &QueryResultIterator{underlying: iterator.NewEmptyIterator(), engine: e}, nil
		}
		span.SetAttributes(attribute.Int("query.matching_series_count", len(binarySeriesKeys)))

		var iteratorsToMerge []core.IteratorInterface[*core.IteratorNode]
		for _, seriesKey := range binarySeriesKeys {
			startKeyBytes := make([]byte, len(seriesKey)+8)
			copy(startKeyBytes, seriesKey)
			binary.BigEndian.PutUint64(startKeyBytes[len(seriesKey):], uint64(params.StartTime))

			endKeyBytes := make([]byte, len(seriesKey)+8) // The end key is exclusive in some contexts, so +1 might be needed.
			copy(endKeyBytes, seriesKey)
			binary.BigEndian.PutUint64(endKeyBytes[len(seriesKey):], uint64(params.EndTime+1))

			scanParams := rangeScanParams{
				StartKey: startKeyBytes,
				EndKey:   endKeyBytes,
				Order:    params.Order,
			}
			seriesBaseIter, err := e.rangeScan(ctx, scanParams)
			if err != nil {
				for _, it := range iteratorsToMerge {
					it.Close()
				}
				return nil, fmt.Errorf("failed to create iterator for series %x: %w", seriesKey, err)
			}
			iteratorsToMerge = append(iteratorsToMerge, seriesBaseIter)
		}

		mergeParams := iterator.MergingIteratorParams{
			Iters:                iteratorsToMerge,
			Order:                params.Order,
			IsSeriesDeleted:      e.isSeriesDeleted,
			IsRangeDeleted:       e.isCoveredByRangeTombstone,
			ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
			DecodeTsFunc:         core.DecodeTimestamp,
		}
		baseIter, err := iterator.NewMergingIteratorWithTombstones(mergeParams)
		if err != nil {
			for _, it := range iteratorsToMerge {
				it.Close()
			}
			return nil, fmt.Errorf("failed to merge series iterators: %w", err)
		}
		effectiveIter = baseIter

		if params.DownsampleInterval != "" {
			interval, err := time.ParseDuration(params.DownsampleInterval)
			if err != nil {
				baseIter.Close() // Clean up the underlying iterator
				return nil, fmt.Errorf("invalid downsample interval: %w", err)
			}

			// For relative queries, we need to use the exact time range for the downsampler
			// to generate the correct windows. The baseIter already fetched a wider range for caching.
			downsampleStartTime := params.StartTime
			downsampleEndTime := params.EndTime
			if isCacheableQuery {
				downsampleStartTime = exactStartTime
				downsampleEndTime = exactEndTime
			}

			// Wrap the merged iterator with the new multi-field downsampling iterator.
			downsampleIter, err := iterator.NewMultiFieldDownsamplingIterator(baseIter, params.AggregationSpecs, interval, downsampleStartTime, downsampleEndTime, params.EmitEmptyWindows)
			if err != nil {
				baseIter.Close()
				return nil, fmt.Errorf("failed to create downsampling iterator: %w", err)
			}
			effectiveIter = downsampleIter
		}

	}

	// Apply cursor-based skipping at the lowest level before wrapping in QueryResultIterator
	if len(params.AfterKey) > 0 {
		effectiveIter = iterator.NewSkippingIterator(effectiveIter, params.AfterKey)
	}

	// For non-final-aggregation queries, apply the time filter at the end.
	// For final aggregations, this is handled before the aggregation step.
	if isCacheableQuery && !isFinalAgg {
		effectiveIter = NewTimeFilterIterator(effectiveIter, exactStartTime, exactEndTime)
	}
	span.SetAttributes(
		attribute.String("db.system", "tsdb-prototype"),
		attribute.String("db.operation", "query"),
		attribute.String("db.metric", params.Metric),
		attribute.Int64("db.query.start_time", params.StartTime),
		attribute.Int64("db.query.end_time", params.EndTime),
		attribute.Float64("iterator_setup_duration_seconds", time.Since(overallQueryStartTime).Seconds()),
	)

	// Wrap the final iterator in our new QueryResultIterator
	resultIterator := &QueryResultIterator{
		underlying:     effectiveIter,
		isFinalAgg:     isFinalAgg,
		queryReqInfo:   &params,
		engine:         e,
		startTime:      overallQueryStartTime,
		limit:          params.Limit,
		count:          0,
		exactStartTime: exactStartTime,
		exactEndTime:   exactEndTime,
	}

	return resultIterator, nil
}

// GetSeriesByTags allows querying for series based on metric and/or tags.
// This method demonstrates the use of the new tag index.
// It returns a slice of series keys (string) that match the criteria.
// If both metric and tags are provided, it will filter by both.
// If only metric is provided, it will return all series for that metric.
// If only tags are provided, it will return all series matching those tags across all metrics.
// If neither is provided, it returns all active series (potentially very large).
func (e *storageEngine) GetSeriesByTags(metric string, tags map[string]string) ([]string, error) {
	if err := e.CheckStarted(); err != nil {
		return nil, err
	}
	// Custom validation for optional metric
	if metric != "" {
		if err := e.validator.ValidateMetricName(metric); err != nil {
			return nil, err
		}
	}
	for k := range tags {
		if err := e.validator.ValidateLabelName(k); err != nil {
			return nil, err
		}
	}

	// Use the internal helper to get the binary keys
	binarySeriesKeys, err := e.getMatchingBinarySeriesKeys(metric, tags)
	if err != nil {
		return nil, fmt.Errorf("error getting matching series keys: %w", err)
	}

	if len(binarySeriesKeys) == 0 {
		return []string{}, nil
	}

	// Convert final list of binary series keys to human-readable strings.
	finalSeriesKeys := make([]string, 0, len(binarySeriesKeys))
	for _, seriesKeyBytes := range binarySeriesKeys {
		mID, tagPairs, decodeErr := core.DecodeSeriesKey(seriesKeyBytes)
		if decodeErr != nil {
			e.logger.Warn("Failed to decode series key from store", "binary_key", seriesKeyBytes, "error", decodeErr)
			continue
		}
		mStr, _ := e.stringStore.GetString(mID)
		tagsMap := make(map[string]string, len(tagPairs))
		for _, p := range tagPairs {
			k, _ := e.stringStore.GetString(p.KeyID)
			v, _ := e.stringStore.GetString(p.ValueID)
			tagsMap[k] = v
		}
		finalSeriesKeys = append(finalSeriesKeys, string(core.EncodeSeriesKeyWithString(mStr, tagsMap)))
	}

	// Sort for consistent output (optional, but good for testing)
	sort.Strings(finalSeriesKeys)
	return finalSeriesKeys, nil
}

// GetMetrics returns a sorted list of all unique metric names in the engine.
func (e *storageEngine) GetMetrics() ([]string, error) {
	if err := e.CheckStarted(); err != nil {
		return nil, err
	}
	e.activeSeriesMu.RLock()
	defer e.activeSeriesMu.RUnlock()

	uniqueMetrics := make(map[string]struct{})
	for seriesKeyStr := range e.activeSeries {
		metricID, _, err := core.DecodeSeriesKey([]byte(seriesKeyStr))
		if err != nil {
			// This indicates a corrupted key in the activeSeries map, which is serious.
			e.logger.Warn("Failed to decode series key from active series map", "key", seriesKeyStr, "error", err)
			continue
		}
		metricName, ok := e.stringStore.GetString(metricID)
		if !ok {
			e.logger.Warn("Metric ID from active series not found in string store", "metric_id", metricID)
			continue
		}
		uniqueMetrics[metricName] = struct{}{}
	}

	metrics := make([]string, 0, len(uniqueMetrics))
	for m := range uniqueMetrics {
		metrics = append(metrics, m)
	}
	sort.Strings(metrics)
	return metrics, nil
}

// GetTagsForMetric returns a sorted list of all unique tag keys for a given metric.
func (e *storageEngine) GetTagsForMetric(metric string) ([]string, error) {
	if err := e.CheckStarted(); err != nil {
		return nil, err
	}
	binarySeriesKeys, err := e.getMatchingBinarySeriesKeys(metric, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get series for metric '%s': %w", metric, err)
	}

	uniqueTagKeys := make(map[string]struct{})
	for _, seriesKeyBytes := range binarySeriesKeys {
		_, encodedTags, err := core.DecodeSeriesKey(seriesKeyBytes)
		if err != nil {
			e.logger.Warn("Failed to decode series key while getting tags", "key", string(seriesKeyBytes), "error", err)
			continue
		}
		for _, pair := range encodedTags {
			tagKey, ok := e.stringStore.GetString(pair.KeyID)
			if ok {
				uniqueTagKeys[tagKey] = struct{}{}
			}
		}
	}

	tagKeys := make([]string, 0, len(uniqueTagKeys))
	for k := range uniqueTagKeys {
		tagKeys = append(tagKeys, k)
	}
	sort.Strings(tagKeys)
	return tagKeys, nil
}

// GetTagValues returns a sorted list of all unique tag values for a given metric and tag key.
func (e *storageEngine) GetTagValues(metric, tagKey string) ([]string, error) {
	if err := e.CheckStarted(); err != nil {
		return nil, err
	}
	// This implementation is a placeholder and can be optimized significantly
	// with a dedicated index (e.g., metric+tagKey -> roaring bitmap of valueIDs).
	// For now, it iterates through all series for the metric.
	allTags, err := e.GetTagsForMetric(metric)
	if err != nil {
		return nil, err
	}
	return allTags, nil
}

// ForceFlush manually triggers a flush of the current mutable memtable to disk.
// This is a synchronous operation for the caller but the actual file writing
// happens in the background. It returns an error if the engine is not in a state
// where it can accept a flush request.
func (e *storageEngine) ForceFlush(ctx context.Context, wait bool) error {
	_, span := e.tracer.Start(ctx, "StorageEngine.ForceFlush", trace.WithAttributes(attribute.Bool("wait", wait)))
	defer span.End()

	if err := e.CheckStarted(); err != nil {
		return err
	}

	if !wait { // Asynchronous flush (FLUSH MEMTABLE)
		e.mu.Lock()
		// Only rotate if there's something to flush
		if e.mutableMemtable != nil && e.mutableMemtable.Size() > 0 {
			e.logger.Info("Asynchronously rotating mutable memtable for flush.")
			// Before moving the memtable, record which WAL segment it belongs to.
			e.mutableMemtable.LastWALSegmentIndex = e.wal.ActiveSegmentIndex()
			e.immutableMemtables = append(e.immutableMemtables, e.mutableMemtable)
			e.mutableMemtable = memtable.NewMemtable(e.opts.MemtableThreshold, e.clock)

			// Signal the background flush loop to process the queue.
			// This is non-blocking.
			select {
			case e.flushChan <- struct{}{}:
			default:
				// Flush loop is already busy, which is fine. It will get to it.
			}
		}
		e.mu.Unlock()
		return nil
	}

	// Synchronous flush (FLUSH ALL)
	completionChan := make(chan error, 1)

	// Try to send to the channel, but don't block if another sync flush is in progress.
	// The flush loop can only handle one sync request at a time.
	// If the channel is full (i.e., no receiver is ready), we return an error.
	select {
	case e.forceFlushChan <- completionChan:
		// Wait for the flush loop to finish processing all immutable memtables.
	case <-ctx.Done():
		return ctx.Err()
	case <-e.shutdownChan:
		return ErrEngineClosed
	default:
		return ErrFlushInProgress
	}

	// Wait for the flush to complete or the context to be cancelled.
	select {
	case err := <-completionChan:
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "flush_failed_in_background")
			e.logger.Error("Synchronous flush completed with an error.", "error", err)
			return err // Return the error from the flush loop
		}
		e.logger.Info("Synchronous flush of all memtables completed successfully.")
	case <-ctx.Done():
		return ctx.Err() // The request was cancelled or timed out
	case <-e.shutdownChan:
		return ErrEngineClosed
	}

	return nil
}

// CreateSnapshot creates a full, consistent, point-in-time snapshot of the database.
// It flushes all in-memory data, creates a manifest of the current state, and copies
// all necessary data files to a new snapshot directory.
func (e *storageEngine) CreateSnapshot(ctx context.Context) (string, error) {
	if err := e.CheckStarted(); err != nil {
		return "", err
	}

	ctx, span := e.tracer.Start(ctx, "StorageEngine.CreateSnapshot")
	defer span.End()

	e.logger.Info("Starting snapshot creation process...")

	// 1. Flush all in-memory data to SSTables to ensure a consistent state on disk.
	// This is a blocking call that ensures all pending writes are persisted.
	e.logger.Info("Flushing all memtables for snapshot consistency...")
	if err := e.ForceFlush(ctx, true); err != nil {
		e.logger.Error("Failed to flush memtables during snapshot creation.", "error", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "memtable_flush_failed")
		return "", fmt.Errorf("failed to flush memtables for snapshot: %w", err)
	}

	// 2. Now that the state is stable on disk, acquire a lock to prevent further
	// changes while we read the state for the manifest.
	e.mu.Lock()
	defer e.mu.Unlock()

	// 3. Create a unique directory for the snapshot.
	snapshotID := fmt.Sprintf("snapshot-%s", e.clock.Now().UTC().Format("20060102T150405Z"))
	snapshotDir := filepath.Join(e.snapshotsBaseDir, snapshotID)
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		e.logger.Error("Failed to create snapshot directory.", "path", snapshotDir, "error", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "create_snapshot_dir_failed")
		return "", fmt.Errorf("failed to create snapshot directory %s: %w", snapshotDir, err)
	}
	e.logger.Info("Snapshot directory created.", "path", snapshotDir)
	span.SetAttributes(attribute.String("snapshot.path", snapshotDir))

	// 4. Create and persist a new manifest file *inside the snapshot directory*.
	// This captures the state of the LSM tree at this point in time.
	if err := e.snapshotManager.CreateFull(ctx, snapshotDir); err != nil {
		e.logger.Error("Failed to create snapshot via manager.", "error", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "snapshot_manager_create_failed")
		// Attempt to clean up the partially created snapshot directory
		_ = os.RemoveAll(snapshotDir)
		return "", fmt.Errorf("failed to create snapshot: %w", err)
	}

	e.logger.Info("Snapshot created successfully.", "path", snapshotDir)
	return snapshotDir, nil
}

// RestoreFromSnapshot restores the database state from a given snapshot directory.
// This is a destructive operation. It shuts down the engine, wipes the current data,
// copies the snapshot data, and restarts the engine.
func (e *storageEngine) RestoreFromSnapshot(ctx context.Context, snapshotPath string, overwrite bool) error {
	if err := e.CheckStarted(); err != nil {
		return err
	}

	_, span := e.tracer.Start(ctx, "StorageEngine.RestoreFromSnapshot")
	defer span.End()
	span.SetAttributes(
		attribute.String("snapshot.path", snapshotPath),
		attribute.Bool("snapshot.overwrite", overwrite),
	)

	e.logger.Info("Starting restore from snapshot process...", "path", snapshotPath)

	// 1. Validate snapshot directory
	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		e.logger.Error("Snapshot validation failed: directory does not exist.", "path", snapshotPath)
		span.SetStatus(codes.Error, "invalid_snapshot_dir_not_exist")
		return fmt.Errorf("invalid snapshot directory: path does not exist: %s", snapshotPath)
	}
	// The snapshot manager itself will validate the contents (e.g., CURRENT file).
	// 2. Safety check for overwrite
	if !overwrite {
		if e.levelsManager.GetTotalTableCount() > 0 || e.mutableMemtable.Size() > 0 {
			err := fmt.Errorf("database is not empty and OVERWRITE is not specified")
			e.logger.Error("Restore aborted.", "error", err)
			span.SetStatus(codes.Error, "restore_aborted_db_not_empty")
			return err
		}
	}

	// 3. Gracefully shut down the current engine instance.
	e.logger.Info("Shutting down current engine instance for restore...")
	if err := e.Close(); err != nil {
		return fmt.Errorf("failed to shut down engine before restore: %w", err)
	}

	// At this point, the engine is stopped. We can now manipulate files.
	// The caller (e.g., a server process) would be responsible for re-initializing
	// and starting the engine after this method returns successfully.
	// This implementation will perform the file operations and leave the engine in a "ready to start" state.
	return e.snapshotManager.RestoreFrom(ctx, snapshotPath)
}

// CreateIncrementalSnapshot creates a snapshot containing only changes since the last one.
// This method now delegates the call to the dedicated snapshot manager.
func (e *storageEngine) CreateIncrementalSnapshot(snapshotsBaseDir string) error {
	if err := e.CheckStarted(); err != nil {
		return err
	}
	return e.snapshotManager.CreateIncremental(context.Background(), snapshotsBaseDir)
}
