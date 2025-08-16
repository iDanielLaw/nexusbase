package engine

import (
	"bytes"
	"fmt"
	"time"

	"github.com/INLOpen/nexusbase/core"
)

var _ core.IteratorInterface[*core.QueryResultItem] = (*QueryResultIterator)(nil)
var _ core.IteratorPoolInterface[*core.QueryResultItem] = (*QueryResultIterator)(nil)

// QueryResultIterator is a specialized iterator returned by the engine's Query method.
// It wraps the low-level iterator.Interface and provides methods to get fully decoded results.
type QueryResultIterator struct {
	underlying     core.Interface
	isFinalAgg     bool // True if the query was a final aggregation over a time range
	queryReqInfo   *core.QueryParams
	engine         *storageEngine // For metrics
	limit          int64          // The maximum number of items to return. 0 means no limit.
	count          int64          // The number of items already returned.
	startTime      time.Time      // For latency calculation
	lastRawKey     []byte         // The raw key of the last item returned by At()
	exactStartTime int64
	exactEndTime   int64
}

// Next advances the iterator. It delegates to the underlying iterator.
// It stops if the configured limit has been reached.
func (it *QueryResultIterator) Next() bool {
	// If a limit is set and we've reached it, stop.
	if it.limit > 0 && it.count >= it.limit {
		return false
	}
	if it.underlying.Next() {
		it.count++
		return true
	}
	return false
}

// Error returns any error encountered during iteration.
func (it *QueryResultIterator) Error() error {
	return it.underlying.Error()
}

// UnderlyingAt exposes the raw key/value from the underlying iterator.
func (it *QueryResultIterator) UnderlyingAt() ([]byte, []byte, core.EntryType, uint64) {
	return it.underlying.At()
}

// Close closes the underlying iterator.
func (it *QueryResultIterator) Close() error {
	if it.engine != nil && it.engine.clock != nil {
		duration := it.engine.clock.Now().Sub(it.startTime).Seconds()
		// Observe general query latency for all query types.
		if it.engine.metrics.QueryLatencyHist != nil {
			observeLatency(it.engine.metrics.QueryLatencyHist, duration)
		}
		// Observe specific latency for ANY aggregation query (final or downsampling).
		if it.queryReqInfo != nil && len(it.queryReqInfo.AggregationSpecs) > 0 && it.engine.metrics.AggregationQueryLatencyHist != nil {
			observeLatency(it.engine.metrics.AggregationQueryLatencyHist, duration)
		}
	}

	return it.underlying.Close()
}

// Put releases the core.QueryResultItem back to the pool for reuse.
// The caller is responsible for calling this after they are done with the item.
func (it *QueryResultIterator) Put(item *core.QueryResultItem) {
	// Reset fields to avoid leaking data between uses
	item.Metric = ""
	item.Tags = nil
	item.AggregatedValues = nil
	queryResultItemPool.Put(item)
}

// At decodes the current iterator position into a structured core.QueryResultItem.
// It handles raw data, downsampled data, and final aggregation results,
// and applies tag filtering.
func (it *QueryResultIterator) At() (*core.QueryResultItem, error) {
	key, value, _, _ := it.underlying.At()
	// Store the raw key for cursor creation
	it.lastRawKey = make([]byte, len(key))
	copy(it.lastRawKey, key)
	// Get an item from the pool
	result := queryResultItemPool.Get().(*core.QueryResultItem)

	// The key is now binary encoded with IDs.
	// The series part is the key minus the last 8 bytes (timestamp).
	if len(key) < 8 {
		return nil, fmt.Errorf("invalid key length in iterator: %d", len(key))
	}
	seriesKeyBytes := key[:len(key)-8]

	metricID, encodedTags, err := core.DecodeSeriesKey(seriesKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to decode series key from iterator key: %w", err)
	}

	// Convert IDs back to strings
	metric, ok := it.engine.stringStore.GetString(metricID)
	if !ok {
		return nil, fmt.Errorf("metric ID %d not found in string store", metricID)
	}

	allTags := make(map[string]string, len(encodedTags))
	for _, pair := range encodedTags {
		tagK, _ := it.engine.stringStore.GetString(pair.KeyID)
		tagV, _ := it.engine.stringStore.GetString(pair.ValueID)
		allTags[tagK] = tagV
	}

	result.Metric = metric
	result.Tags = allTags
	result.IsAggregated = false // Default

	if it.isFinalAgg {
		aggValues, err := core.DecodeAggregationResult(value)
		if err != nil {
			return nil, fmt.Errorf("failed to decode final aggregation result: %w", err)
		}
		result.IsAggregated = true
		// For relative queries, the user expects the window to match the exact duration,
		// not the potentially larger, rounded window used for caching.
		if it.queryReqInfo.IsRelative {
			result.WindowStartTime = it.exactStartTime
			result.WindowEndTime = it.exactEndTime
		} else {
			result.WindowStartTime = it.queryReqInfo.StartTime
			result.WindowEndTime = it.queryReqInfo.EndTime
		}
		result.AggregatedValues = aggValues
	} else if len(it.queryReqInfo.DownsampleInterval) > 0 {
		aggValues, err := core.DecodeAggregationResult(value)
		if err != nil {
			return nil, fmt.Errorf("failed to decode downsampled window result: %w", err)
		}
		result.IsAggregated = true
		result.WindowStartTime, _ = core.DecodeTimestamp(key[len(key)-8:])
		result.AggregatedValues = aggValues
		// Calculate WindowEndTime based on the interval
		if it.queryReqInfo.DownsampleInterval != "" {
			interval, err := time.ParseDuration(it.queryReqInfo.DownsampleInterval)
			if err == nil {
				result.WindowEndTime = result.WindowStartTime + interval.Nanoseconds()
			}
		}
	} else {
		// Raw data point or event
		// entryType := it.underlying.EntryType()
		_, _, entryType, _ := it.underlying.At()
		if entryType == core.EntryTypePutEvent {
			result.IsEvent = true
			fields, decodeErr := core.DecodeFields(bytes.NewBuffer(value))
			if decodeErr != nil {
				return nil, fmt.Errorf("failed to decode event fields: %w", decodeErr)
			}
			result.Fields = fields
		} else {
			// This branch might be for legacy single-value points.
			// For now, we assume all non-aggregated points can have fields.
			fields, _ := core.DecodeFields(bytes.NewBuffer(value))
			result.Fields = fields
		}
		result.Timestamp, _ = core.DecodeTimestamp(key[len(key)-8:])
	}
	return result, nil
}
