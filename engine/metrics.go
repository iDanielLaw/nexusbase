package engine

import (
	"expvar"
	"fmt"
)

// EngineMetrics holds all expvar variables for a StorageEngine instance.
// When injected into StorageEngine, these instances are used directly.
// When StorageEngine creates its own metrics (if not injected), it will
// initialize these fields with globally published expvar variables.
type EngineMetrics struct {
	PutTotal              *expvar.Int
	PutErrorsTotal        *expvar.Int // New
	GetTotal              *expvar.Int
	QueryTotal            *expvar.Int
	QueryErrorsTotal      *expvar.Int // New
	DeleteTotal           *expvar.Int
	FlushTotal            *expvar.Int
	CompactionTotal       *expvar.Int
	CompactionErrorsTotal *expvar.Int // New
	SSTablesCreatedTotal  *expvar.Int // New: Total number of SSTables created

	FlushDataPointsFlushedTotal *expvar.Int // New: Total data points flushed
	FlushBytesFlushedTotal      *expvar.Int // New: Total bytes flushed

	PutLatencyHist              *expvar.Map
	GetLatencyHist              *expvar.Map
	DeleteLatencyHist           *expvar.Map
	QueryLatencyHist            *expvar.Map
	RangeScanLatencyHist        *expvar.Map
	AggregationQueryLatencyHist *expvar.Map

	FlushLatencyHist      *expvar.Map // New: Histogram for flush durations
	CompactionLatencyHist *expvar.Map // New: Replaces LastDuration floats

	BloomFilterChecksTotal         *expvar.Int // New: Total times Bloom Filter was checked
	BloomFilterFalsePositivesTotal *expvar.Int // New: Times Bloom Filter said true, but key was not found

	WALBytesWrittenTotal   *expvar.Int
	WALEntriesWrittenTotal *expvar.Int

	WALRecoveryDurationSeconds *expvar.Float
	WALRecoveredEntriesTotal   *expvar.Int

	CompactionDataReadBytesTotal    *expvar.Int
	CompactionDataWrittenBytesTotal *expvar.Int
	CompactionTablesMergedTotal     *expvar.Int

	CacheHits   *expvar.Int
	CacheMisses *expvar.Int

	SeriesCreatedTotal *expvar.Int // New

	// Gauges (published as Func)
	ActiveQueries         *expvar.Int // New
	CompactionsInProgress *expvar.Int // New

	// Func providers - these are populated by StorageEngine when metrics are injected.
	// Tests can call these functions directly on their EngineMetrics instance.
	// When metrics are NOT injected, NewStorageEngine will create expvar.Funcs
	// wrapping closures that capture the engine instance and publish them globally.
	// The fields below will hold the closures themselves, not the expvar.Func.
	// This allows tests to access the underlying function logic directly.

	activeSeriesCountFunc                func() interface{}
	mutableMemtableSizeFunc              func() interface{}
	immutableMemtablesCountFunc          func() interface{}
	immutableMemtablesTotalSizeBytesFunc func() interface{}
	uptimeSecondsFunc                    func() interface{} // New
	flushQueueLengthFunc                 func() interface{} // New
	diskUsageBytesFunc                   func() interface{} // New
	// Note: Level-specific funcs (levelTableCountFuncs, levelSizeBytesFuncs)
	// are harder to manage in a flat struct. They might need to remain
	// as globally published expvar.Funcs or accessed via engine methods.
	// If needed for tests, they could be added here as a map of funcs.
	// levelTableCountFuncs map[int]func() interface{}
	// levelSizeBytesFuncs map[int]func() interface{}
}

// NewEngineMetrics creates and initializes a new EngineMetrics struct with expvar variables.
// If publishGlobally is true, it uses helper functions that publish to the global expvar registry.
// If publishGlobally is false, it creates expvar variables but does NOT publish them globally.
// prefix is used for metric names when publishGlobally is false, to ensure uniqueness in tests.
func NewEngineMetrics(publishGlobally bool, prefix string) *EngineMetrics {
	var newIntFunc func(string) *expvar.Int
	var newFloatFunc func(string) *expvar.Float
	var newMapFunc func(string) *expvar.Map

	if publishGlobally {
		// Use the helper functions that publish globally and handle reuse/reset
		newIntFunc = publishExpvarInt
		newFloatFunc = publishExpvarFloat
		newMapFunc = publishExpvarMap // This helper handles global publishing and reuse
	} else {
		// For tests (not publishing globally), just create new instances.
		// These instances are not registered with the global expvar handler.
		newIntFunc = func(_ string) *expvar.Int { return new(expvar.Int) }       // Name is irrelevant as it's not published
		newFloatFunc = func(_ string) *expvar.Float { return new(expvar.Float) } // Name is irrelevant
		newMapFunc = func(_ string) *expvar.Map {                                // Name is irrelevant
			// expvar.Map needs to be initialized for use, even if not published.
			m := new(expvar.Map)
			m.Init() // Initializes the map for use (e.g., for Set calls).
			return m
		}
	}

	em := &EngineMetrics{
		PutTotal:              newIntFunc(prefix + "put_total"),
		PutErrorsTotal:        newIntFunc(prefix + "put_errors_total"),
		GetTotal:              newIntFunc(prefix + "get_total"),
		QueryTotal:            newIntFunc(prefix + "query_total"),
		QueryErrorsTotal:      newIntFunc(prefix + "query_errors_total"),
		DeleteTotal:           newIntFunc(prefix + "delete_total"),
		FlushTotal:            newIntFunc(prefix + "flush_total"),
		CompactionTotal:       newIntFunc(prefix + "compaction_total"),
		CompactionErrorsTotal: newIntFunc(prefix + "compaction_errors_total"),
		SSTablesCreatedTotal:  newIntFunc(prefix + "sstables_created_total"),

		FlushDataPointsFlushedTotal:    newIntFunc(prefix + "flush_data_points_flushed_total"),
		FlushBytesFlushedTotal:         newIntFunc(prefix + "flush_bytes_flushed_total"),
		BloomFilterChecksTotal:         newIntFunc(prefix + "bloom_filter_checks_total"),
		BloomFilterFalsePositivesTotal: newIntFunc(prefix + "bloom_filter_false_positives_total"),

		PutLatencyHist:              newMapFunc(prefix + "put_latency_seconds"),
		GetLatencyHist:              newMapFunc(prefix + "get_latency_seconds"),
		DeleteLatencyHist:           newMapFunc(prefix + "delete_latency_seconds"),
		QueryLatencyHist:            newMapFunc(prefix + "query_latency_seconds"),
		RangeScanLatencyHist:        newMapFunc(prefix + "range_scan_latency_seconds"),
		AggregationQueryLatencyHist: newMapFunc(prefix + "aggregation_query_latency_seconds"),

		FlushLatencyHist:      newMapFunc(prefix + "flush_latency_seconds"),
		CompactionLatencyHist: newMapFunc(prefix + "compaction_latency_seconds"),

		WALBytesWrittenTotal:   newIntFunc(prefix + "wal_bytes_written_total"),
		WALEntriesWrittenTotal: newIntFunc(prefix + "wal_entries_written_total"),

		WALRecoveryDurationSeconds: newFloatFunc(prefix + "wal_recovery_duration_seconds"),
		WALRecoveredEntriesTotal:   newIntFunc(prefix + "wal_recovered_entries_total"),

		CompactionTablesMergedTotal: newIntFunc(prefix + "compaction_tables_merged_total"),

		CacheHits:   newIntFunc(prefix + "cache_hits"),
		CacheMisses: newIntFunc(prefix + "cache_misses"),

		SeriesCreatedTotal: newIntFunc(prefix + "series_created_total"),

		ActiveQueries:         newIntFunc(prefix + "active_queries"),
		CompactionsInProgress: newIntFunc(prefix + "compactions_in_progress"),
	}

	// Initialize histogram maps with their sub-metrics (count, sum, buckets)
	// This should use the actual latencyBuckets defined in engine.go
	// For simplicity, assuming latencyBuckets is accessible or passed here.
	// If not, this initialization needs to happen where latencyBuckets is defined.
	histMaps := []*expvar.Map{
		em.FlushLatencyHist,
		em.CompactionLatencyHist,
		em.PutLatencyHist, em.GetLatencyHist, em.DeleteLatencyHist, em.QueryLatencyHist,
		em.RangeScanLatencyHist, em.AggregationQueryLatencyHist,
	}
	for _, m := range histMaps {
		m.Set("count", new(expvar.Int))
		m.Set("sum", new(expvar.Float))
		for _, b := range latencyBuckets { // Use the package-level latencyBuckets
			m.Set(fmt.Sprintf("le_%.3f", b), new(expvar.Int))
		}
		m.Set("le_inf", new(expvar.Int))
	}
	return em
}

// Helper methods to get values from Func providers (useful for tests)
// These methods assume the corresponding func fields in EngineMetrics have been
// populated by NewStorageEngine when metrics were injected.

func (em *EngineMetrics) GetActiveSeriesCount() (int, error) {
	if em.activeSeriesCountFunc == nil {
		// This indicates the metrics were not injected, or the func provider wasn't set.
		// In a test using injected metrics, this is an error.
		// In production (not injecting), the global expvar.Func should be used via expvar.Get.
		return 0, fmt.Errorf("activeSeriesCountFunc not initialized in injected metrics")
	}
	val := em.activeSeriesCountFunc()
	if count, ok := val.(int); ok {
		return count, nil
	}
	// Handle int64 case for robustness if the func returns int64
	if count64, ok64 := val.(int64); ok64 {
		return int(count64), nil
	}
	return 0, fmt.Errorf("activeSeriesCountFunc did not return int or int64, got %T", val)
}

// TODO: Add similar helper methods for other func-based metrics if needed by tests.
// e.g., GetMutableMemtableSizeBytes(), GetImmutableMemtablesCount(), etc.
