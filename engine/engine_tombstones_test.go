package engine

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsSeriesDeleted(t *testing.T) {
	eng := newEngineForTombstoneTests(t)

	concreteEng, ok := eng.(*storageEngine)
	if !ok {
		t.Fatal("Failed to assert StorageEngineInterface to *storageEngine")
	}
	require.True(t, ok)
	// Setup: Create a series and its ID-based key
	metricID, _ := concreteEng.stringStore.GetOrCreateID("test.metric")
	tagKeyID, _ := concreteEng.stringStore.GetOrCreateID("host")
	tagValID, _ := concreteEng.stringStore.GetOrCreateID("server1")
	encodedTags := []core.EncodedSeriesTagPair{{KeyID: tagKeyID, ValueID: tagValID}}
	seriesKeyBytes := core.EncodeSeriesKey(metricID, encodedTags)

	// Mark series as deleted at seqNum 100
	concreteEng.deletedSeries[string(seriesKeyBytes)] = 100

	testCases := []struct {
		name            string
		keyToCheck      []byte
		dataPointSeqNum uint64
		expectedDeleted bool
	}{
		{"SeriesNotMarkedAsDeleted", []byte("other.key"), 10, false},
		{"DataPointOlderThanDeletion", seriesKeyBytes, 50, true},
		{"DataPointSameAsDeletion", seriesKeyBytes, 100, true},
		{"DataPointNewerThanDeletion", seriesKeyBytes, 150, false},
		{"DataPointSeqNumIsZero (considered old)", seriesKeyBytes, 0, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectedDeleted, concreteEng.isSeriesDeleted(tc.keyToCheck, tc.dataPointSeqNum))
		})
	}
}

func TestIsCoveredByRangeTombstone(t *testing.T) {
	eng := newEngineForTombstoneTests(t)
	concreteEng, ok := eng.(*storageEngine)
	if !ok {
		t.Fatal("Failed to assert StorageEngineInterface to *storageEngine")
	}
	require.True(t, ok)
	// Setup: Create a series and its ID-based key
	metricID, _ := concreteEng.stringStore.GetOrCreateID("range.metric")
	tagKeyID, _ := concreteEng.stringStore.GetOrCreateID("zone")
	tagValID, _ := concreteEng.stringStore.GetOrCreateID("north")
	encodedTags := []core.EncodedSeriesTagPair{{KeyID: tagKeyID, ValueID: tagValID}}
	seriesKeyBytes := core.EncodeSeriesKey(metricID, encodedTags)

	// Setup range tombstones
	concreteEng.rangeTombstones[string(seriesKeyBytes)] = []core.RangeTombstone{
		{MinTimestamp: 100, MaxTimestamp: 200, SeqNum: 500}, // RT1
		{MinTimestamp: 180, MaxTimestamp: 220, SeqNum: 550}, // RT2 (Overlapping, newer)
		{MinTimestamp: 300, MaxTimestamp: 400, SeqNum: 600}, // RT3 (Non-overlapping)
	}

	testCases := []struct {
		name            string
		keyToCheck      []byte
		timestamp       int64
		dataPointSeqNum uint64
		expectedCovered bool
	}{
		{"NoRangeTombstonesForThisSeries", []byte("other.key"), 150, 10, false},
		{"TimestampBeforeAllRanges", seriesKeyBytes, 50, 10, false},
		{"TimestampAfterAllRanges", seriesKeyBytes, 500, 10, false},
		{"TimestampInRT1_OlderData", seriesKeyBytes, 150, 400, true},
		{"TimestampInRT1_SameData", seriesKeyBytes, 150, 500, true},
		{"TimestampInRT1_NewerData", seriesKeyBytes, 150, 600, false},
		{"TimestampInRT1_ZeroSeqNum", seriesKeyBytes, 150, 0, true},
		{"TimestampInOverlappingRegion_CoveredByNewerRT2", seriesKeyBytes, 190, 520, true}, // ts=190, seq=520. In RT1(500) & RT2(550). Covered by RT2.
		{"TimestampInOverlappingRegion_NewerThanBoth", seriesKeyBytes, 190, 560, false},    // ts=190, seq=560. Newer than RT1(500) & RT2(550). Not covered.
		{"TimestampInRT3", seriesKeyBytes, 350, 580, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectedCovered, concreteEng.isCoveredByRangeTombstone(tc.keyToCheck, tc.timestamp, tc.dataPointSeqNum))
		})
	}
}

func TestDeleteSeries(t *testing.T) {
	// This test now uses a real engine, so we don't need the mock helper.
	tempDir := t.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            1024 * 1024,
		CompactionIntervalSeconds:    3600,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		Metrics:                      NewEngineMetrics(false, "delseries_tsdb_"),
	}
	eng, err := NewStorageEngine(opts)
	require.NoError(t, err)
	err = eng.Start()
	require.NoError(t, err)
	defer eng.Close()

	metric := "test.delete.series"
	tags := map[string]string{"env": "dev"}

	// Put some data first
	eng.Put(context.Background(), HelperDataPoint(t, metric, tags, time.Now().UnixNano(), map[string]interface{}{"value": 1.0}))
	eng.Put(context.Background(), HelperDataPoint(t, metric, tags, time.Now().UnixNano()+1, map[string]interface{}{"value": 2.0}))

	// Call DeleteSeries
	err = eng.DeleteSeries(context.Background(), metric, tags)
	require.NoError(t, err)

	// Verify series is marked as deleted by trying to get a point.
	// We use the original timestamp to ensure we are checking for a point that existed.
	// Note: The actual timestamp doesn't matter much as the whole series is deleted.
	_, err = eng.Get(context.Background(), metric, tags, time.Now().UnixNano()) // Use a timestamp within the original range
	require.ErrorIs(t, err, sstable.ErrNotFound, "Get after DeleteSeries should return ErrNotFound")

	// Verify that GetSeriesByTags no longer finds the series key.
	retrievedKeys, err := eng.GetSeriesByTags(metric, tags)
	require.NoError(t, err)
	assert.Empty(t, retrievedKeys, "GetSeriesByTags should return 0 keys for a deleted series")

	// Verify that a Query for the series returns no data points.
	// This is the most important check, as it verifies the tombstone is effective.
	iter, err := eng.Query(context.Background(), core.QueryParams{Metric: metric, Tags: tags, StartTime: 0, EndTime: time.Now().UnixNano() * 2})
	require.NoError(t, err)
	defer iter.Close()

	assert.False(t, iter.Next(), "Query should return no data for a deleted series")
	assert.NoError(t, iter.Error(), "Iterator should have no error after iterating a deleted series")
}

func TestDeletesByTimeRange_WithHooks(t *testing.T) {
	ctx := context.Background()

	t.Run("PreHook_CancelOperation", func(t *testing.T) {
		// Setup
		eng := newEngineForTombstoneTests(t)
		concreteEngine := eng.(*storageEngine)

		metric := "hook.range_delete.cancel"
		tags := map[string]string{"test": "pre_range_delete_hook"}
		ts1, ts2, ts3 := int64(100), int64(200), int64(300)

		// Put data so the series exists
		eng.Put(ctx, HelperDataPoint(t, metric, tags, ts1, map[string]interface{}{"value": 1.0}))
		eng.Put(ctx, HelperDataPoint(t, metric, tags, ts2, map[string]interface{}{"value": 2.0}))
		eng.Put(ctx, HelperDataPoint(t, metric, tags, ts3, map[string]interface{}{"value": 3.0}))

		// Register a hook that will cancel the deletion
		expectedErr := errors.New("range deletion cancelled by pre-hook")
		listener := &mockListener{
			priority:  1,
			isAsync:   false,
			returnErr: expectedErr,
		}
		concreteEngine.hookManager.Register(hooks.EventPreDeleteRange, listener)

		// Action: Attempt to delete a range
		err := eng.DeletesByTimeRange(ctx, metric, tags, ts2, ts2)

		// Assertions
		require.Error(t, err, "DeletesByTimeRange should have returned an error")
		require.ErrorIs(t, err, expectedErr, "Error should be the one from the hook")

		// Verify data was NOT deleted
		_, err = eng.Get(ctx, metric, tags, ts2)
		require.NoError(t, err, "Get should find data for a cancelled range deletion")
	})

	t.Run("PostHook_AsyncNotification", func(t *testing.T) {
		// Setup
		eng := newEngineForTombstoneTests(t)
		concreteEngine := eng.(*storageEngine)

		metric := "hook.range_delete.post"
		tags := map[string]string{"test": "post_range_delete_hook"}
		ts1, ts2, ts3 := int64(100), int64(200), int64(300)

		// Put data so the series exists
		eng.Put(ctx, HelperDataPoint(t, metric, tags, ts1, map[string]interface{}{"value": 1.0}))
		eng.Put(ctx, HelperDataPoint(t, metric, tags, ts2, map[string]interface{}{"value": 2.0}))
		eng.Put(ctx, HelperDataPoint(t, metric, tags, ts3, map[string]interface{}{"value": 3.0}))

		// Get the internal series key for verification
		metricID, _ := concreteEngine.stringStore.GetOrCreateID(metric)
		tagKeyID, _ := concreteEngine.stringStore.GetOrCreateID("test")
		tagValID, _ := concreteEngine.stringStore.GetOrCreateID("post_range_delete_hook")
		expectedSeriesKey := core.EncodeSeriesKey(metricID, []core.EncodedSeriesTagPair{{KeyID: tagKeyID, ValueID: tagValID}})

		// Register a listener to capture the post-delete event
		signalChan := make(chan hooks.HookEvent, 1)
		listener := &mockListener{
			priority:   1,
			isAsync:    true,
			callSignal: signalChan,
		}
		concreteEngine.hookManager.Register(hooks.EventPostDeleteRange, listener)

		// Action: Delete a range
		deleteStartTime, deleteEndTime := ts2-50, ts2+50
		err := eng.DeletesByTimeRange(ctx, metric, tags, deleteStartTime, deleteEndTime)
		require.NoError(t, err, "DeletesByTimeRange returned an unexpected error")

		// Assertions
		// 1. Data should be deleted immediately from a query perspective
		_, err = eng.Get(ctx, metric, tags, ts2)
		require.ErrorIs(t, err, sstable.ErrNotFound, "Get should fail for deleted point in range")

		// 2. Wait for the async hook to be called
		select {
		case event := <-signalChan:
			payload, ok := event.Payload().(hooks.PostDeleteRangePayload)
			require.True(t, ok, "Post-hook received payload of wrong type: %T", event.Payload())

			assert.Equal(t, metric, payload.Metric, "Post-hook payload metric mismatch")
			assert.Equal(t, tags, payload.Tags, "Post-hook payload tags mismatch")
			assert.Equal(t, string(expectedSeriesKey), payload.SeriesKey, "Post-hook payload SeriesKey mismatch")
			assert.Equal(t, deleteStartTime, payload.StartTime, "Post-hook payload StartTime mismatch")
			assert.Equal(t, deleteEndTime, payload.EndTime, "Post-hook payload EndTime mismatch")

		case <-time.After(200 * time.Millisecond):
			t.Fatal("Timed out waiting for post-range-delete-hook to be called")
		}
	})
}

func TestDeleteSeries_WithHooks(t *testing.T) {
	ctx := context.Background()

	t.Run("PreHook_CancelOperation", func(t *testing.T) {
		// Setup
		eng := newEngineForTombstoneTests(t)
		concreteEngine := eng.(*storageEngine)

		metric := "hook.delete.cancel"
		tags := map[string]string{"test": "pre_delete_hook"}
		ts := time.Now().UnixNano()

		// Put data so the series exists
		require.NoError(t, eng.Put(ctx, HelperDataPoint(t, metric, tags, ts, map[string]interface{}{"value": 1.0})))

		// Register a hook that will cancel the deletion
		expectedErr := errors.New("deletion cancelled by pre-hook")
		listener := &mockListener{
			priority:  1,
			isAsync:   false,
			returnErr: expectedErr,
		}
		concreteEngine.hookManager.Register(hooks.EventPreDeleteSeries, listener)

		// Action: Attempt to delete the series
		err := eng.DeleteSeries(ctx, metric, tags)

		// Assertions
		require.Error(t, err, "DeleteSeries should have returned an error")
		require.ErrorIs(t, err, expectedErr, "Error should be the one from the hook")

		// Verify data was NOT deleted
		_, err = eng.Get(ctx, metric, tags, ts)
		require.NoError(t, err, "Get should find data for a cancelled deletion")
	})

	t.Run("PostHook_AsyncNotification", func(t *testing.T) {
		// Setup
		eng := newEngineForTombstoneTests(t)
		concreteEngine := eng.(*storageEngine)

		metric := "hook.delete.post"
		tags := map[string]string{"test": "post_delete_hook"}
		ts := time.Now().UnixNano()

		// Put data so the series exists
		require.NoError(t, eng.Put(ctx, HelperDataPoint(t, metric, tags, ts, map[string]interface{}{"value": 1.0})))

		// Get the internal series key for verification
		metricID, _ := concreteEngine.stringStore.GetOrCreateID(metric)
		tagKeyID, _ := concreteEngine.stringStore.GetOrCreateID("test")
		tagValID, _ := concreteEngine.stringStore.GetOrCreateID("post_delete_hook")
		expectedSeriesKey := core.EncodeSeriesKey(metricID, []core.EncodedSeriesTagPair{{KeyID: tagKeyID, ValueID: tagValID}})

		// Register a listener to capture the post-delete event
		signalChan := make(chan hooks.HookEvent, 1)
		listener := &mockListener{
			priority:   1,
			isAsync:    true,
			callSignal: signalChan,
		}
		concreteEngine.hookManager.Register(hooks.EventPostDeleteSeries, listener)

		// Action: Delete the series
		err := eng.DeleteSeries(ctx, metric, tags)
		require.NoError(t, err, "DeleteSeries returned an unexpected error")

		// Assertions
		// 1. Data should be deleted immediately from a query perspective
		_, err = eng.Get(ctx, metric, tags, ts)
		require.ErrorIs(t, err, sstable.ErrNotFound, "Get should fail for deleted series")

		// 2. Wait for the async hook to be called
		select {
		case event := <-signalChan:
			payload, ok := event.Payload().(hooks.PostDeleteSeriesPayload)
			require.True(t, ok, "Post-hook received payload of wrong type: %T", event.Payload())
			assert.Equal(t, metric, payload.Metric, "Post-hook payload metric mismatch")
			assert.Equal(t, tags, payload.Tags, "Post-hook payload tags mismatch")
			assert.Equal(t, string(expectedSeriesKey), payload.SeriesKey, "Post-hook payload SeriesKey mismatch")

		case <-time.After(200 * time.Millisecond):
			t.Fatal("Timed out waiting for post-delete-hook to be called")
		}
	})

	t.Run("PreHook_ModifyPayload", func(t *testing.T) {
		// Setup: Create two distinct series. The hook will redirect the deletion from one to the other.
		eng := newEngineForTombstoneTests(t)
		concreteEngine := eng.(*storageEngine)

		metric := "hook.delete.modify"
		tagsOriginal := map[string]string{"host": "A"}
		tagsTarget := map[string]string{"host": "B"}
		tsOriginal := time.Now().UnixNano()
		tsTarget := tsOriginal + 1

		// Put data for both series
		require.NoError(t, eng.Put(ctx, HelperDataPoint(t, metric, tagsOriginal, tsOriginal, map[string]interface{}{"value": 1.0})))
		require.NoError(t, eng.Put(ctx, HelperDataPoint(t, metric, tagsTarget, tsTarget, map[string]interface{}{"value": 2.0})))

		// Register a hook that modifies the tags to be deleted
		listener := &mockListener{
			priority: 1,
			isAsync:  false,
			onEventFunc: func(event hooks.HookEvent) {
				if p, ok := event.Payload().(hooks.PreDeleteSeriesPayload); ok {
					// Redirect the deletion to the target tags
					*p.Tags = tagsTarget
				}
			},
		}
		concreteEngine.hookManager.Register(hooks.EventPreDeleteSeries, listener)

		// Action: Attempt to delete the original series
		err := eng.DeleteSeries(ctx, metric, tagsOriginal)
		require.NoError(t, err, "DeleteSeries with modifying hook should not fail")

		// Assertions
		// 1. The original series should NOT be deleted.
		_, err = eng.Get(ctx, metric, tagsOriginal, tsOriginal)
		assert.NoError(t, err, "Original series should not be deleted after hook modified the target")

		// 2. The target series (modified by the hook) SHOULD be deleted.
		_, err = eng.Get(ctx, metric, tagsTarget, tsTarget)
		assert.ErrorIs(t, err, sstable.ErrNotFound, "Target series should be deleted by the hook")
	})
}

func TestDeletesByTimeRange(t *testing.T) {
	// This test now uses a real engine.
	tempDir := t.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            1024 * 1024,
		CompactionIntervalSeconds:    3600,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		Metrics:                      NewEngineMetrics(false, "delrange_tsdb_"),
	}
	eng, err := NewStorageEngine(opts)
	require.NoError(t, err)
	err = eng.Start()
	require.NoError(t, err)
	defer eng.Close()

	metric := "test.range.delete"
	tags := map[string]string{"loc": "lab"}

	startTime := int64(100)
	endTime := int64(200)

	// Put points inside and outside the range
	eng.Put(context.Background(), HelperDataPoint(t, metric, tags, startTime-1, map[string]interface{}{"value": 1.0})) // Before
	eng.Put(context.Background(), HelperDataPoint(t, metric, tags, startTime, map[string]interface{}{"value": 2.0}))   // Inside
	eng.Put(context.Background(), HelperDataPoint(t, metric, tags, endTime, map[string]interface{}{"value": 3.0}))     // Inside
	eng.Put(context.Background(), HelperDataPoint(t, metric, tags, endTime+1, map[string]interface{}{"value": 4.0}))   // After

	// Call DeletesByTimeRange
	err = eng.DeletesByTimeRange(context.Background(), metric, tags, startTime, endTime)
	require.NoError(t, err)

	// Verify points inside the range are deleted
	_, err = eng.Get(context.Background(), metric, tags, startTime)
	assert.ErrorIs(t, err, sstable.ErrNotFound, "Point at start of range should be deleted")
	_, err = eng.Get(context.Background(), metric, tags, endTime)
	assert.ErrorIs(t, err, sstable.ErrNotFound, "Point at end of range should be deleted")

	// Verify points outside the range still exist
	_, err = eng.Get(context.Background(), metric, tags, startTime-1)
	assert.NoError(t, err, "Point before range should exist")
	_, err = eng.Get(context.Background(), metric, tags, endTime+1)
	assert.NoError(t, err, "Point after range should exist")

	// Test invalid time range
	err = eng.DeletesByTimeRange(context.Background(), metric, tags, 200, 100)
	assert.Error(t, err, "Expected error for invalid time range (startTime > endTime)")
}

// newEngineForBenchmark is a helper to create a StorageEngine for benchmarks.
func newEngineForBenchmark(b *testing.B) StorageEngineInterface {
	b.Helper()
	tempDir := b.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            1024 * 1024 * 100, // Large memtable to avoid flushes
		CompactionIntervalSeconds:    3600,              // Disable auto-compaction
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		Metrics:                      NewEngineMetrics(false, "bench_delseries_"),
		Logger:                       slog.New(slog.NewTextHandler(io.Discard, nil)), // Discard logs
	}
	eng, err := NewStorageEngine(opts)
	require.NoError(b, err, "NewStorageEngine failed")

	err = eng.Start()
	require.NoError(b, err, "Failed to start setup engine")

	b.Cleanup(func() {
		require.NoError(b, eng.Close())
	})
	return eng
}

/*
func BenchmarkDeleteSeries(b *testing.B) {
	ctx := context.Background()
	eng := newEngineForBenchmark(b)

	numSeries := 1000
	points := make([]core.DataPoint, numSeries)
	metric := "bench.delete.series"

	for i := 0; i < numSeries; i++ {
		tags := map[string]string{"host": fmt.Sprintf("server-%d", i)}
		point := HelperDataPoint(b, metric, tags, int64(i), map[string]interface{}{"value": float64(i)})
		points[i] = point
		if err := eng.Put(ctx, point); err != nil {
			b.Fatalf("Failed to put data for benchmark setup: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Cycle through the points to delete different series in each iteration.
		pointToDelete := points[i%len(points)]
		if err := eng.DeleteSeries(ctx, pointToDelete.Metric, pointToDelete.Tags); err != nil {
			b.Fatalf("DeleteSeries failed during benchmark: %v", err)
		}
	}
}
*/
