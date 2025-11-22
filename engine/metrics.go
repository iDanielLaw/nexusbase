package engine

import (
	"expvar"
	"fmt"
)

// EngineMetrics holds all expvar variables for a StorageEngine instance.
type EngineMetrics struct {
	PublishedGlobally bool // Indicates if the metrics are published to the global expvar namespace.

	PutTotal              *expvar.Int
	PutErrorsTotal        *expvar.Int
	GetTotal              *expvar.Int
	QueryTotal            *expvar.Int
	QueryErrorsTotal      *expvar.Int
	DeleteTotal           *expvar.Int
	FlushTotal            *expvar.Int
	CompactionTotal       *expvar.Int
	CompactionErrorsTotal *expvar.Int
	SSTablesCreatedTotal  *expvar.Int
	SSTablesDeletedTotal  *expvar.Int

	FlushDataPointsFlushedTotal *expvar.Int
	FlushBytesFlushedTotal      *expvar.Int

	PutLatencyHist              *expvar.Map
	GetLatencyHist              *expvar.Map
	DeleteLatencyHist           *expvar.Map
	QueryLatencyHist            *expvar.Map
	RangeScanLatencyHist        *expvar.Map
	AggregationQueryLatencyHist *expvar.Map

	FlushLatencyHist      *expvar.Map
	CompactionLatencyHist *expvar.Map

	BloomFilterChecksTotal         *expvar.Int
	BloomFilterFalsePositivesTotal *expvar.Int

	WALBytesWrittenTotal   *expvar.Int
	WALEntriesWrittenTotal *expvar.Int

	WALRecoveryDurationSeconds *expvar.Float
	WALRecoveredEntriesTotal   *expvar.Int

	CompactionDataReadBytesTotal    *expvar.Int
	CompactionDataWrittenBytesTotal *expvar.Int
	CompactionTablesMergedTotal     *expvar.Int

	CacheHits   *expvar.Int
	CacheMisses *expvar.Int

	SeriesCreatedTotal *expvar.Int

	ActiveQueries         *expvar.Int
	CompactionsInProgress *expvar.Int

	ReplicationErrorsTotal       *expvar.Int
	ReplicationPutTotal          *expvar.Int
	ReplicationDeleteSeriesTotal *expvar.Int
	ReplicationDeleteRangeTotal  *expvar.Int

	PreallocateEnabled *expvar.Int

	PreallocSuccesses   *expvar.Int
	PreallocFailures    *expvar.Int
	PreallocUnsupported *expvar.Int

	activeSeriesCountFunc                func() interface{}
	mutableMemtableSizeFunc              func() interface{}
	immutableMemtablesCountFunc          func() interface{}
	immutableMemtablesTotalSizeBytesFunc func() interface{}
	uptimeSecondsFunc                    func() interface{}
	flushQueueLengthFunc                 func() interface{}
	diskUsageBytesFunc                   func() interface{}
}

// NewEngineMetrics creates and initializes a new EngineMetrics struct with expvar variables.
func NewEngineMetrics(publishGlobally bool, prefix string) *EngineMetrics {
	var newIntFunc func(string) *expvar.Int
	var newFloatFunc func(string) *expvar.Float
	var newMapFunc func(string) *expvar.Map

	if publishGlobally {
		newIntFunc = publishExpvarInt
		newFloatFunc = publishExpvarFloat
		newMapFunc = publishExpvarMap
	} else {
		newIntFunc = func(_ string) *expvar.Int { return new(expvar.Int) }
		newFloatFunc = func(_ string) *expvar.Float { return new(expvar.Float) }
		newMapFunc = func(_ string) *expvar.Map {
			m := new(expvar.Map)
			m.Init()
			return m
		}
	}

	em := &EngineMetrics{
		PublishedGlobally:     publishGlobally,
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
		SSTablesDeletedTotal:  newIntFunc(prefix + "sstables_deleted_total"),

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

		ReplicationErrorsTotal:       newIntFunc(prefix + "replication_errors_total"),
		ReplicationPutTotal:          newIntFunc(prefix + "replication_put_total"),
		ReplicationDeleteSeriesTotal: newIntFunc(prefix + "replication_delete_series_total"),
		ReplicationDeleteRangeTotal:  newIntFunc(prefix + "replication_delete_range_total"),

		PreallocateEnabled: newIntFunc(prefix + "preallocate_enabled"),

		PreallocSuccesses:   newIntFunc(prefix + "prealloc_successes_total"),
		PreallocFailures:    newIntFunc(prefix + "prealloc_failures_total"),
		PreallocUnsupported: newIntFunc(prefix + "prealloc_unsupported_total"),
	}

	histMaps := []*expvar.Map{
		em.FlushLatencyHist,
		em.CompactionLatencyHist,
		em.PutLatencyHist, em.GetLatencyHist, em.DeleteLatencyHist, em.QueryLatencyHist,
		em.RangeScanLatencyHist, em.AggregationQueryLatencyHist,
	}
	for _, m := range histMaps {
		m.Set("count", new(expvar.Int))
		m.Set("sum", new(expvar.Float))
		for _, b := range latencyBuckets {
			m.Set(fmt.Sprintf("le_%.3f", b), new(expvar.Int))
		}
		m.Set("le_inf", new(expvar.Int))
	}
	return em
}

func (em *EngineMetrics) GetActiveSeriesCount() (int, error) {
	if em.activeSeriesCountFunc == nil {
		return 0, fmt.Errorf("activeSeriesCountFunc not initialized in injected metrics")
	}
	val := em.activeSeriesCountFunc()
	if count, ok := val.(int); ok {
		return count, nil
	}
	if count64, ok64 := val.(int64); ok64 {
		return int(count64), nil
	}
	return 0, fmt.Errorf("activeSeriesCountFunc did not return int or int64, got %T", val)
}
