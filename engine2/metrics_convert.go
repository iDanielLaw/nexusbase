package engine2

import "github.com/INLOpen/nexusbase/engine"

// ToEngine converts an engine2 EngineMetrics2 into the legacy engine.EngineMetrics.
// This copies pointer references; both objects will reference the same expvar
// variables when published globally.
func (m *EngineMetrics2) ToEngine() *engine.EngineMetrics {
	if m == nil {
		return nil
	}
	return &engine.EngineMetrics{
		PublishedGlobally:     m.PublishedGlobally,
		PutTotal:              m.PutTotal,
		PutErrorsTotal:        m.PutErrorsTotal,
		GetTotal:              m.GetTotal,
		QueryTotal:            m.QueryTotal,
		QueryErrorsTotal:      m.QueryErrorsTotal,
		DeleteTotal:           m.DeleteTotal,
		FlushTotal:            m.FlushTotal,
		CompactionTotal:       m.CompactionTotal,
		CompactionErrorsTotal: m.CompactionErrorsTotal,

		SSTablesCreatedTotal: m.SSTablesCreatedTotal,
		SSTablesDeletedTotal: m.SSTablesDeletedTotal,

		FlushDataPointsFlushedTotal: m.FlushDataPointsFlushedTotal,
		FlushBytesFlushedTotal:      m.FlushBytesFlushedTotal,

		PutLatencyHist:              m.PutLatencyHist,
		GetLatencyHist:              m.GetLatencyHist,
		DeleteLatencyHist:           m.DeleteLatencyHist,
		QueryLatencyHist:            m.QueryLatencyHist,
		RangeScanLatencyHist:        m.RangeScanLatencyHist,
		AggregationQueryLatencyHist: m.AggregationQueryLatencyHist,

		FlushLatencyHist:      m.FlushLatencyHist,
		CompactionLatencyHist: m.CompactionLatencyHist,

		BloomFilterChecksTotal:         m.BloomFilterChecksTotal,
		BloomFilterFalsePositivesTotal: m.BloomFilterFalsePositivesTotal,

		WALBytesWrittenTotal:   m.WALBytesWrittenTotal,
		WALEntriesWrittenTotal: m.WALEntriesWrittenTotal,

		WALRecoveryDurationSeconds: m.WALRecoveryDurationSeconds,
		WALRecoveredEntriesTotal:   m.WALRecoveredEntriesTotal,

		CompactionDataReadBytesTotal:    m.CompactionDataReadBytesTotal,
		CompactionDataWrittenBytesTotal: m.CompactionDataWrittenBytesTotal,
		CompactionTablesMergedTotal:     m.CompactionTablesMergedTotal,

		CacheHits:   m.CacheHits,
		CacheMisses: m.CacheMisses,

		SeriesCreatedTotal: m.SeriesCreatedTotal,

		ActiveQueries:         m.ActiveQueries,
		CompactionsInProgress: m.CompactionsInProgress,

		ReplicationErrorsTotal:       m.ReplicationErrorsTotal,
		ReplicationPutTotal:          m.ReplicationPutTotal,
		ReplicationDeleteSeriesTotal: m.ReplicationDeleteSeriesTotal,
		ReplicationDeleteRangeTotal:  m.ReplicationDeleteRangeTotal,

		PreallocateEnabled: m.PreallocateEnabled,

		PreallocSuccesses:   m.PreallocSuccesses,
		PreallocFailures:    m.PreallocFailures,
		PreallocUnsupported: m.PreallocUnsupported,

		// Note: unexported function hooks (activeSeriesCountFunc, etc.) are
		// intentionally not set here because they are unexported in the
		// legacy `engine.EngineMetrics` type and not addressable from this
		// package. Callers that need to inject those hooks should do so via
		// legacy `engine` helpers or by directly using engine.NewEngineMetrics
		// in combination with engine2.FromEngine.
	}
}

// FromEngine builds an EngineMetrics2 from the legacy engine.EngineMetrics.
func FromEngine(e *engine.EngineMetrics) *EngineMetrics2 {
	if e == nil {
		return nil
	}
	return &EngineMetrics2{
		PublishedGlobally:     e.PublishedGlobally,
		PutTotal:              e.PutTotal,
		PutErrorsTotal:        e.PutErrorsTotal,
		GetTotal:              e.GetTotal,
		QueryTotal:            e.QueryTotal,
		QueryErrorsTotal:      e.QueryErrorsTotal,
		DeleteTotal:           e.DeleteTotal,
		FlushTotal:            e.FlushTotal,
		CompactionTotal:       e.CompactionTotal,
		CompactionErrorsTotal: e.CompactionErrorsTotal,

		SSTablesCreatedTotal: e.SSTablesCreatedTotal,
		SSTablesDeletedTotal: e.SSTablesDeletedTotal,

		FlushDataPointsFlushedTotal: e.FlushDataPointsFlushedTotal,
		FlushBytesFlushedTotal:      e.FlushBytesFlushedTotal,

		PutLatencyHist:              e.PutLatencyHist,
		GetLatencyHist:              e.GetLatencyHist,
		DeleteLatencyHist:           e.DeleteLatencyHist,
		QueryLatencyHist:            e.QueryLatencyHist,
		RangeScanLatencyHist:        e.RangeScanLatencyHist,
		AggregationQueryLatencyHist: e.AggregationQueryLatencyHist,

		FlushLatencyHist:      e.FlushLatencyHist,
		CompactionLatencyHist: e.CompactionLatencyHist,

		BloomFilterChecksTotal:         e.BloomFilterChecksTotal,
		BloomFilterFalsePositivesTotal: e.BloomFilterFalsePositivesTotal,

		WALBytesWrittenTotal:   e.WALBytesWrittenTotal,
		WALEntriesWrittenTotal: e.WALEntriesWrittenTotal,

		WALRecoveryDurationSeconds: e.WALRecoveryDurationSeconds,
		WALRecoveredEntriesTotal:   e.WALRecoveredEntriesTotal,

		CompactionDataReadBytesTotal:    e.CompactionDataReadBytesTotal,
		CompactionDataWrittenBytesTotal: e.CompactionDataWrittenBytesTotal,
		CompactionTablesMergedTotal:     e.CompactionTablesMergedTotal,

		CacheHits:   e.CacheHits,
		CacheMisses: e.CacheMisses,

		SeriesCreatedTotal: e.SeriesCreatedTotal,

		ActiveQueries:         e.ActiveQueries,
		CompactionsInProgress: e.CompactionsInProgress,

		ReplicationErrorsTotal:       e.ReplicationErrorsTotal,
		ReplicationPutTotal:          e.ReplicationPutTotal,
		ReplicationDeleteSeriesTotal: e.ReplicationDeleteSeriesTotal,
		ReplicationDeleteRangeTotal:  e.ReplicationDeleteRangeTotal,

		PreallocateEnabled: e.PreallocateEnabled,

		PreallocSuccesses:   e.PreallocSuccesses,
		PreallocFailures:    e.PreallocFailures,
		PreallocUnsupported: e.PreallocUnsupported,

		// Unexported hook functions from engine.EngineMetrics are not
		// accessible here; leave engine2 hooks nil so callers can set them
		// explicitly on the EngineMetrics2 instance when needed.
	}
}
