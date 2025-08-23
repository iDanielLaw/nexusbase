package engine

import (
	"context"
	"errors"
	"expvar"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core" // Import core package
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/internal"
	"github.com/INLOpen/nexusbase/sstable" // For sstable.DefaultBlockSize
	"github.com/INLOpen/nexusbase/sys"
	"github.com/INLOpen/nexuscore/utils/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	sys.SetDebugMode(true)
}

func TestStorageEngine_PutAndGet(t *testing.T) {
	tempDir := t.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            1024 * 1024, // Large enough
		IndexMemtableThreshold:       1024 * 1024, // Prevent index flushes during this test
		CompactionIntervalSeconds:    3600,        // Disable compaction
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		Metrics:                      NewEngineMetrics(false, "putget_tsdb_"), // Inject metrics
	}

	engine, err := NewStorageEngine(opts)
	if err != nil || engine == nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err = engine.Start(); err != nil {
		t.Fatalf("Failed to start setup engine: %v", err)
	}
	defer engine.Close()

	concreteEngine, ok := engine.(*storageEngine)
	if !ok {
		t.Fatal("Failed to cast engine to concrete type")
	}

	metric1 := "cpu.temp"
	tags1 := map[string]string{"host": "serverA", "core": "0"}
	ts1 := time.Now().UnixNano()
	val1 := 35.5

	metric2 := "cpu.temp"                                      // Same metric
	tags2 := map[string]string{"host": "serverB", "core": "0"} // Different host
	ts2 := time.Now().UnixNano() + 1000                        // Slightly different timestamp
	val2 := 38.0

	metric3 := "memory.usage" // Different metric
	tags3 := map[string]string{"host": "serverA"}
	ts3 := time.Now().UnixNano() + 2000
	val3 := 75.2

	getActiveSeriesCount := func(metrics *EngineMetrics) int {
		count, err := metrics.GetActiveSeriesCount()
		if err != nil {
			t.Fatalf("Error getting active series count: %v", err)
		}
		return count
	}

	// Test Put for metric1
	if err := engine.Put(context.Background(), HelperDataPoint(t, metric1, tags1, ts1, map[string]interface{}{"value": val1})); err != nil {
		t.Fatalf("Put(metric1) failed: %v", err)
	}
	if count := getActiveSeriesCount(concreteEngine.metrics); count != 1 {
		t.Errorf("After Put(metric1), active series count = %d, want 1", count)
	}

	// Test Put for metric2 (new series)
	if err := engine.Put(context.Background(), HelperDataPoint(t, metric2, tags2, ts2, map[string]interface{}{"value": val2})); err != nil {
		t.Fatalf("Put(metric2) failed: %v", err)
	}
	if count := getActiveSeriesCount(concreteEngine.metrics); count != 2 {
		t.Errorf("After Put(metric2), active series count = %d, want 2", count)
	}

	// Test Put for metric3 (new series)
	if err := engine.Put(context.Background(), HelperDataPoint(t, metric3, tags3, ts3, map[string]interface{}{"value": val3})); err != nil {
		t.Fatalf("Put(metric3) failed: %v", err)
	}
	if count := getActiveSeriesCount(concreteEngine.metrics); count != 3 {
		t.Errorf("After Put(metric3), active series count = %d, want 3", count)
	}

	// --- Test Get ---

	// Test Get for metric1
	retrievedVal1, err := engine.Get(context.Background(), metric1, tags1, ts1)
	if err != nil {
		t.Fatalf("Get(metric1) failed: %v", err)
	}
	if val, ok := retrievedVal1["value"].ValueFloat64(); !ok || !ApproximatelyEqual(val, val1) {
		t.Errorf("Get(metric1) value mismatch: got %f, want %f", val, val1)
	}

	// Test Get for metric2
	retrievedVal2, err := engine.Get(context.Background(), metric2, tags2, ts2)
	if err != nil {
		t.Fatalf("Get(metric2) failed: %v", err)
	}
	if val, ok := retrievedVal2["value"].ValueFloat64(); !ok || !ApproximatelyEqual(val, val2) {
		t.Errorf("Get(metric2) value mismatch: got %f, want %f", val, val2)
	}

	// Test Get for metric3
	retrievedVal3, err := engine.Get(context.Background(), metric3, tags3, ts3)
	if err != nil {
		t.Fatalf("Get(metric3) failed: %v", err)
	}
	if val, ok := retrievedVal3["value"].ValueFloat64(); !ok || !ApproximatelyEqual(val, val3) {
		t.Errorf("Get(metric3) value mismatch: got %f, want %f", val, val3)
	}

	// Test Get for a non-existent timestamp
	_, err = engine.Get(context.Background(), metric1, tags1, ts1+12345) // Different timestamp
	if err != sstable.ErrNotFound {
		t.Errorf("Get for non-existent timestamp: expected ErrNotFound, got %v", err)
	}

	// Test Get for a non-existent series (different tags)
	_, err = engine.Get(context.Background(), metric1, map[string]string{"host": "serverC"}, ts1)
	if err != sstable.ErrNotFound {
		t.Errorf("Get for non-existent series: expected ErrNotFound, got %v", err)
	}

	// --- Test Delete ---

	// Delete metric1's data point
	if err := engine.Delete(context.Background(), metric1, tags1, ts1); err != nil {
		t.Fatalf("Delete(metric1) failed: %v", err)
	}

	// Try to Get for the deleted metric1
	_, err = engine.Get(context.Background(), metric1, tags1, ts1)
	if err != sstable.ErrNotFound {
		t.Errorf("Get(metric1) after delete: expected ErrNotFound, got %v", err)
	}

	// Ensure metric2 (which was not deleted) can still be retrieved
	retrievedVal2AfterDelete, err := engine.Get(context.Background(), metric2, tags2, ts2)
	if err != nil {
		t.Fatalf("Get(metric2) after deleting metric1 failed: %v", err)
	}
	if val, ok := retrievedVal2AfterDelete["value"].ValueFloat64(); !ok || !ApproximatelyEqual(val, val2) {
		t.Errorf("Get(metric2) after deleting metric1 value mismatch: got %f, want %f", val, val2)
	}

	// Active series count should remain the same after deleting a point (as series still exists)
	if count := getActiveSeriesCount(concreteEngine.metrics); count != 3 { // Active series count is not affected by point deletes
		t.Errorf("After Delete(metric1), active series count = %d, want 3", count)
	}
}

func TestStorageEngine_PutBatch(t *testing.T) {
	ctx := context.Background()

	t.Run("SuccessfulBatchPut", func(t *testing.T) { // Re-use helper from flush test
		opts := getBaseOptsForFlushTest(t)
		engine, err := NewStorageEngine(opts)
		if err != nil {
			t.Fatalf("NewStorageEngine failed: %v", err)
		}
		if err = engine.Start(); err != nil {
			t.Fatalf("Failed to start setup engine: %v", err)
		}
		defer engine.Close()

		points := []core.DataPoint{
			HelperDataPoint(t, "batch.metric.a", map[string]string{"host": "server1"}, 100, map[string]interface{}{"value": 10.0}),
			HelperDataPoint(t, "batch.metric.a", map[string]string{"host": "server1"}, 101, map[string]interface{}{"value": 11.0}),
			HelperDataPoint(t, "batch.metric.b", map[string]string{"host": "server2"}, 200, map[string]interface{}{"value": 20.0}),
			HelperDataPoint(t, "batch.metric.a", map[string]string{"host": "server2"}, 102, map[string]interface{}{"value": 12.0}), // Different series
		}

		err = engine.PutBatch(ctx, points)
		if err != nil {
			t.Fatalf("PutBatch failed unexpectedly: %v", err)
		}

		// Verify all points were written
		for _, p := range points {
			val, getErr := engine.Get(ctx, p.Metric, p.Tags, p.Timestamp)
			if getErr != nil {
				t.Errorf("Failed to get point after batch put (metric: %s, ts: %d): %v", p.Metric, p.Timestamp, getErr)
				continue
			}
			v1, ok1 := val["value"].ValueFloat64()
			v2, ok2 := p.Fields["value"].ValueFloat64()
			if !ok1 || !ok2 {
				t.Errorf("Failed to get value point")
			}

			if !ApproximatelyEqual(v1, v2) {
				t.Errorf("Value mismatch for point (metric: %s, ts: %d): got %f, want %f", p.Metric, p.Timestamp, v1, v2)
			}
		}
	})

	t.Run("EmptyBatch", func(t *testing.T) {
		opts := getBaseOptsForFlushTest(t)
		engine, err := NewStorageEngine(opts)
		if err != nil {
			t.Fatalf("NewStorageEngine failed: %v", err)
		}
		if err = engine.Start(); err != nil {
			t.Fatalf("Failed to start setup engine: %v", err)
		}
		defer engine.Close()

		err = engine.PutBatch(ctx, []core.DataPoint{})
		if err != nil {
			t.Errorf("PutBatch with empty slice should not return an error, but got: %v", err)
		}
	})

	t.Run("BatchWithInvalidPoint", func(t *testing.T) {
		opts := getBaseOptsForFlushTest(t)
		engine, err := NewStorageEngine(opts)
		if err != nil {
			t.Fatalf("NewStorageEngine failed: %v", err)
		}
		if err = engine.Start(); err != nil {
			t.Fatalf("Failed to start setup engine: %v", err)
		}
		defer engine.Close()
		points := []core.DataPoint{}
		point1, err := core.NewSimpleDataPoint("batch.valid.1", map[string]string{"host": "server1"}, 300, map[string]interface{}{"value": 30.0})
		if err != nil {
			t.Fatalf("Failed to create valid point: %v", err)
		}
		points = append(points, *point1)

		points = append(points, core.DataPoint{}) // test point invalid

		point3, err := core.NewSimpleDataPoint("batch.valid.2", map[string]string{"host": "server1"}, 302, map[string]interface{}{"value": 32.0})
		if err != nil {
			t.Fatalf("Failed to create valid point: %v", err)
		}
		points = append(points, *point3)

		err = engine.PutBatch(ctx, points)
		if err == nil {
			t.Fatal("PutBatch with an invalid point should have returned an error, but got nil")
		}

		// Check that the error is a validation error
		var validationErr *core.ValidationError
		if !errors.As(err, &validationErr) {
			t.Errorf("Expected a validation error, but got a different type: %T, %v", err, err)
		}

		// Verify that the first valid point was NOT written because the batch is atomic (or at least stops on first error)
		_, getErr := engine.Get(ctx, "batch.valid.1", map[string]string{"host": "server1"}, 300)
		if getErr != sstable.ErrNotFound {
			t.Errorf("Expected point before invalid one to NOT be written, but it was found (err: %v)", getErr)
		}
	})

	t.Run("BatchWithHooks", func(t *testing.T) {
		opts := getBaseOptsForFlushTest(t)
		engine, err := NewStorageEngine(opts)
		if err != nil {
			t.Fatalf("NewStorageEngine failed: %v", err)
		}
		if err = engine.Start(); err != nil {
			t.Fatalf("Failed to start setup engine: %v", err)
		}
		defer engine.Close()

		concreteEngine, ok := engine.(*storageEngine)
		if !ok {
			t.Fatal("Failed to cast engine to concrete type")
		}

		// Reset hooks for this subtest - this is safe now because the engine is not shared.
		concreteEngine.hookManager = hooks.NewHookManager(nil)

		signalChan := make(chan hooks.HookEvent, 2) // Expect 2 calls
		listener := &mockListener{
			priority:   1,
			isAsync:    true, // Post-hooks are async
			callSignal: signalChan,
		}
		concreteEngine.hookManager.Register(hooks.EventPostPutDataPoint, listener)

		points := []core.DataPoint{
			HelperDataPoint(t, "batch.hook.a", map[string]string{"id": "1"}, 400, map[string]interface{}{"value": 40.0}),
			HelperDataPoint(t, "batch.hook.b", map[string]string{"id": "2"}, 401, map[string]interface{}{"value": 41.0}),
		}

		err = engine.PutBatch(ctx, points)
		if err != nil {
			t.Fatalf("PutBatch for hook test failed: %v", err)
		}

		// Wait for the async hooks to be called
		for i := 0; i < len(points); i++ {
			select {
			case <-signalChan:
				// Received a signal, continue
			case <-time.After(200 * time.Millisecond):
				t.Fatalf("Timed out waiting for post-put hooks. Received %d of %d.", i, len(points))
			}
		}
	})

	t.Run("BatchWithWALError", func(t *testing.T) {
		opts := getBaseOptsForFlushTest(t)
		engine, err := NewStorageEngine(opts)
		if err != nil {
			t.Fatalf("NewStorageEngine failed: %v", err)
		}
		if err = engine.Start(); err != nil {
			t.Fatalf("Failed to start setup engine: %v", err)
		}
		defer engine.Close()

		// Get concrete engine to access the WAL
		concreteEngine, ok := engine.(*storageEngine)
		if !ok {
			t.Fatal("Failed to cast engine to concrete type")
		}

		// Inject the error into the WAL
		expectedErr := errors.New("simulated WAL append error")

		concreteWAL := concreteEngine.wal.(internal.PrivateWAL)
		if concreteWAL == nil {
			t.Fatal("Failed to cast WAL to concrete type")
		}

		concreteWAL.SetTestingOnlyInjectAppendError(expectedErr)

		points := []core.DataPoint{
			HelperDataPoint(t, "batch.wal.fail", map[string]string{"id": "1"}, 500, map[string]interface{}{"value": 50.0}),
		}

		// Action: This call should fail because the WAL write fails.
		err = engine.PutBatch(ctx, points)

		// Assertions
		if err == nil {
			t.Fatal("PutBatch should have returned an error due to WAL failure, but got nil")
		}
		if !errors.Is(err, expectedErr) {
			t.Errorf("Expected error to wrap '%v', but got '%v'", expectedErr, err)
		}

		// Verify that the data was NOT written to the memtable
		_, getErr := engine.Get(ctx, points[0].Metric, points[0].Tags, points[0].Timestamp)
		if getErr != sstable.ErrNotFound {
			t.Errorf("Expected point to NOT be written to memtable after WAL failure, but it was found (err: %v)", getErr)
		}
	})
}

func TestStorageEngine_DeleteSeries(t *testing.T) {
	// This test now uses a real engine, so we don't need the mock helper.
	opts := getBaseOptsForFlushTest(t)
	eng, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err = eng.Start(); err != nil {
		t.Fatalf("Failed to start setup engine: %v", err)
	}
	defer eng.Close()

	metric := "test.delete.series"
	tags := map[string]string{"env": "dev"}

	// Put some data first
	eng.Put(context.Background(), HelperDataPoint(t, metric, tags, time.Now().UnixNano(), map[string]interface{}{"value": 1.0}))
	eng.Put(context.Background(), HelperDataPoint(t, metric, tags, time.Now().UnixNano()+1, map[string]interface{}{"value": 2.0}))

	// Call DeleteSeries
	err = eng.DeleteSeries(context.Background(), metric, tags)
	if err != nil {
		t.Fatalf("DeleteSeries failed: %v", err)
	}
	// Verify series is marked as deleted by trying to get a point.
	// The actual timestamp doesn't matter much as the whole series is deleted.
	_, err = eng.Get(context.Background(), metric, tags, time.Now().UnixNano())
	if err != sstable.ErrNotFound {
		t.Errorf("Expected Get to return ErrNotFound after DeleteSeries, but got %v", err)
	}

	// Verify that GetSeriesByTags no longer finds the series key.
	retrievedKeys, err := eng.GetSeriesByTags(metric, tags)
	if err != nil {
		t.Fatalf("GetSeriesByTags failed after DeleteSeries: %v", err)
	}
	if len(retrievedKeys) != 0 {
		t.Errorf("Expected GetSeriesByTags to return 0 keys for a deleted series, but got %d", len(retrievedKeys))
	}

	// Verify that a Query for the series returns no data points.
	// This is the most important check, as it verifies the tombstone is effective.
	iter, err := eng.Query(context.Background(), core.QueryParams{Metric: metric, Tags: tags, StartTime: 0, EndTime: time.Now().UnixNano() * 2})
	if err != nil || iter.Next() {
		t.Errorf("Expected Query to return no data for a deleted series, but it did. Iterator error: %v", iter.Error())
	}
}

func TestStorageEngine_DeletesByTimeRange(t *testing.T) {
	opts := getBaseOptsForFlushTest(t)
	engine, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err = engine.Start(); err != nil {
		t.Fatalf("Failed to start setup engine: %v", err)
	}
	defer engine.Close()

	metric := "sensor.temp"
	tags := map[string]string{"location": "room1"}

	ts := []int64{
		time.Now().UnixNano(),
		time.Now().UnixNano() + 1000,
		time.Now().UnixNano() + 2000,
		time.Now().UnixNano() + 3000,
		time.Now().UnixNano() + 4000,
	}
	vals := []float64{20.0, 20.5, 21.0, 21.5, 22.0}

	for i := 0; i < len(ts); i++ {
		if err := engine.Put(context.Background(), HelperDataPoint(t, metric, tags, ts[i], map[string]interface{}{"value": vals[i]})); err != nil {
			t.Fatalf("Put failed for ts[%d]: %v", i, err)
		}
	}

	// Put data for another series to ensure it's not affected
	otherMetric := "sensor.humidity"
	otherTags := map[string]string{"location": "room1"}
	otherTs := ts[0]
	otherVal := 60.0
	engine.Put(context.Background(), HelperDataPoint(t, otherMetric, otherTags, otherTs, map[string]interface{}{"value": otherVal}))

	// Delete points from ts[1] to ts[3] (inclusive) for the "sensor.temp" series
	deleteStartTime := ts[1]
	deleteEndTime := ts[3]

	if err := engine.DeletesByTimeRange(context.Background(), metric, tags, deleteStartTime, deleteEndTime); err != nil {
		t.Fatalf("DeletesByTimeRange failed: %v", err)
	}

	// Verify data
	expectedResults := map[int64]bool{ // timestamp -> shouldBeFound
		ts[0]: true,  // Before range
		ts[1]: false, // In range, deleted
		ts[2]: false, // In range, deleted
		ts[3]: false, // In range, deleted
		ts[4]: true,  // After range
	}

	for timestamp, shouldBeFound := range expectedResults {
		_, err := engine.Get(context.Background(), metric, tags, timestamp)
		if shouldBeFound && err != nil {
			t.Errorf("Get for ts %d: expected to find value, got error %v", timestamp, err)
		}
		if !shouldBeFound && err != sstable.ErrNotFound {
			t.Errorf("Get for ts %d: expected ErrNotFound, got %v", timestamp, err)
		}
	}
	// Verify other series is not affected
	retrievedOtherVal, err := engine.Get(context.Background(), otherMetric, otherTags, otherTs)
	if err != nil {
		t.Errorf("Get for otherMetric failed: %v", err)
	}
	if v, ok := retrievedOtherVal["value"].ValueFloat64(); !ok || !ApproximatelyEqual(v, otherVal) {
		t.Errorf("Get for otherMetric value mismatch: got %f, want %f", v, otherVal)
	}

	// Verify using Query that the deleted points are not returned
	queryRangeDelParams := core.QueryParams{
		Metric:             metric,
		StartTime:          ts[0],
		EndTime:            ts[len(ts)-1] + 1,
		Tags:               tags,
		AggregationSpecs:   nil,
		DownsampleInterval: "",
		EmitEmptyWindows:   false,
	}
	iter, queryErr := engine.Query(context.Background(), queryRangeDelParams)
	if queryErr != nil {
		t.Fatalf("Query after DeletesByTimeRange failed: %v", queryErr)
	}
	defer iter.Close()

	var itemsCollected []*core.QueryResultItem
	for iter.Next() {
		item, err := iter.At()
		if err != nil {
			t.Fatalf("iter.At() failed: %v", err)
		}
		itemsCollected = append(itemsCollected, item)
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("Iterator error after DeletesByTimeRange: %v", err)
	}

	for _, item := range itemsCollected {
		keyTimestamp := item.Timestamp
		if _, shouldBeFound := expectedResults[keyTimestamp]; !shouldBeFound {
			t.Errorf("Query returned data point with timestamp %d which should have been deleted by range", keyTimestamp)
		}
	}
}

func TestStorageEngine_GetSeriesByTags(t *testing.T) {
	opts := getBaseOptsForFlushTest(t)
	opts.SelfMonitoringEnabled = false // Disable for this test to avoid interference
	engine, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err = engine.Start(); err != nil {
		t.Fatalf("Failed to start setup engine: %v", err)
	}
	defer engine.Close()

	// Define some data points to put
	type testPoint struct {
		metric    string
		tags      map[string]string
		timestamp int64
		value     float64
		seriesKey string // Pre-calculated series key for verification
	}

	points := []testPoint{
		{
			metric:    "cpu.usage",
			tags:      map[string]string{"host": "serverA", "region": "us-east"},
			timestamp: 1000,
			value:     50.0,
			seriesKey: string(core.EncodeSeriesKeyWithString("cpu.usage", map[string]string{"host": "serverA", "region": "us-east"})),
		},
		{
			metric:    "cpu.usage",
			tags:      map[string]string{"host": "serverB", "region": "us-east"},
			timestamp: 1001,
			value:     60.0,
			seriesKey: string(core.EncodeSeriesKeyWithString("cpu.usage", map[string]string{"host": "serverB", "region": "us-east"})),
		},
		{
			metric:    "memory.free",
			tags:      map[string]string{"host": "serverA", "region": "us-west"},
			timestamp: 1002,
			value:     1024.0,
			seriesKey: string(core.EncodeSeriesKeyWithString("memory.free", map[string]string{"host": "serverA", "region": "us-west"})),
		},
		{
			metric:    "network.in",
			tags:      map[string]string{"host": "serverC", "env": "prod"},
			timestamp: 1003,
			value:     100.0,
			seriesKey: string(core.EncodeSeriesKeyWithString("network.in", map[string]string{"host": "serverC", "env": "prod"})),
		},
	}

	for _, p := range points {
		if err := engine.Put(context.Background(), HelperDataPoint(t, p.metric, p.tags, p.timestamp, map[string]interface{}{"value": p.value})); err != nil {
			t.Fatalf("Put failed for %s: %v", p.seriesKey, err)
		}
	}

	// Test Cases for GetSeriesByTags
	tests := []struct {
		name         string
		metric       string
		tags         map[string]string
		expectedKeys []string
		expectError  bool
	}{
		{
			name:         "query by metric only (cpu.usage)",
			metric:       "cpu.usage",
			tags:         nil,
			expectedKeys: []string{points[0].seriesKey, points[1].seriesKey},
		},
		{
			name:         "query by metric only (memory.free)",
			metric:       "memory.free",
			tags:         nil,
			expectedKeys: []string{points[2].seriesKey},
		},
		{
			name:         "query by single tag (host=serverA)",
			metric:       "",
			tags:         map[string]string{"host": "serverA"},
			expectedKeys: []string{points[0].seriesKey, points[2].seriesKey},
		},
		{
			name:         "query by single tag (region=us-east)",
			metric:       "",
			tags:         map[string]string{"region": "us-east"},
			expectedKeys: []string{points[0].seriesKey, points[1].seriesKey},
		},
		{
			name:         "query by multiple tags (host=serverA, region=us-east)",
			metric:       "",
			tags:         map[string]string{"host": "serverA", "region": "us-east"},
			expectedKeys: []string{points[0].seriesKey},
		},
		{
			name:         "query by metric and tag (cpu.usage, region=us-east)",
			metric:       "cpu.usage",
			tags:         map[string]string{"region": "us-east"},
			expectedKeys: []string{points[0].seriesKey, points[1].seriesKey},
		},
		{
			name:         "query by non-existent metric",
			metric:       "non.existent",
			tags:         nil,
			expectedKeys: []string{},
		},
		{
			name:         "query by non-existent tag",
			metric:       "",
			tags:         map[string]string{"nonexistent": "tag"},
			expectedKeys: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualKeys, err := engine.GetSeriesByTags(tt.metric, tt.tags)
			if (err != nil) != tt.expectError {
				t.Fatalf("GetSeriesByTags() error = %v, expectError %v", err, tt.expectError)
			}
			sort.Strings(actualKeys) // Ensure sorted for comparison
			if !sortAndCompare(actualKeys, tt.expectedKeys) {
				t.Errorf("GetSeriesByTags() mismatch for %s.\nGot:    %x\nWanted: %x", tt.name, actualKeys, tt.expectedKeys)
			}
		})
	}

	// Test DeleteSeries and verify it's removed from tag index
	t.Run("GetSeriesByTags_DoesNotReturnDeletedSeries", func(t *testing.T) {
		seriesToDelete := points[0] // cpu.usage, host=serverA, region=us-east
		if err := engine.DeleteSeries(context.Background(), seriesToDelete.metric, seriesToDelete.tags); err != nil {
			t.Fatalf("DeleteSeries failed: %v", err)
		}

		// Verify that GetSeriesByTags no longer finds the deleted series key.
		actualKeys, err := engine.GetSeriesByTags(seriesToDelete.metric, seriesToDelete.tags)
		if err != nil {
			t.Fatalf("GetSeriesByTags after DeleteSeries failed: %v", err)
		}
		if len(actualKeys) != 0 {
			t.Errorf("Expected GetSeriesByTags to return 0 keys for a deleted series, but got %d", len(actualKeys))
		}

		// Verify that a Query for the series returns no data points.
		// This is the most important check, as it verifies the tombstone is effective.
		iter, err := engine.Query(context.Background(), core.QueryParams{Metric: seriesToDelete.metric, Tags: seriesToDelete.tags, StartTime: 0, EndTime: time.Now().UnixNano() * 2})
		if err != nil {
			t.Fatalf("Query after DeleteSeries failed: %v", err)
		}
		defer iter.Close()

		if iter.Next() {
			t.Errorf("Expected Query to return no data for a deleted series, but it did. Iterator error: %v", iter.Error())
		}

		// Verify other series are still present
		actualKeysAfterDelete, err := engine.GetSeriesByTags("cpu.usage", nil) // Should only return serverB now
		if err != nil {
			t.Fatalf("GetSeriesByTags for other cpu.usage series failed: %v", err)
		}
		expectedRemaining := []string{points[1].seriesKey} // Only serverB should remain
		if !sortAndCompare(actualKeysAfterDelete, expectedRemaining) {
			t.Errorf("Expected only %v after deleting %s, but got: %v", expectedRemaining, seriesToDelete.seriesKey, actualKeysAfterDelete)
		}
	})

	t.Run("Query_AllActiveSeries_NoFilters", func(t *testing.T) {
		// This test should be run after all puts and deletes to get the final state
		actualKeys, err := engine.GetSeriesByTags("", nil)
		if err != nil {
			t.Fatalf("GetSeriesByTags with no filters failed: %v", err)
		}

		// Expected active series after all operations:
		// points[1] (cpu.usage, host=serverB, region=us-east) - not deleted
		// points[2] (memory.free, host=serverA, region=us-west) - not deleted
		// points[3] (network.in, host=serverC, env=prod) - not deleted
		expectedActive := []string{
			points[1].seriesKey,
			points[2].seriesKey,
			points[3].seriesKey,
		}
		if !sortAndCompare(actualKeys, expectedActive) {
			t.Errorf("GetSeriesByTags (all active series) mismatch:\nGot:    %x\nWanted: %x", actualKeys, expectedActive)
		}
	})

	t.Run("Query_NonExistentMetric_ExistingTags", func(t *testing.T) {
		actualKeys, err := engine.GetSeriesByTags("non.existent.metric", map[string]string{"host": "serverA"})
		if err != nil {
			t.Fatalf("GetSeriesByTags failed: %v", err)
		}
		if len(actualKeys) != 0 {
			t.Errorf("Expected no series for non-existent metric with existing tags, but got: %v", actualKeys)
		}
	})

	t.Run("Query_ExistingMetric_NonExistentTags", func(t *testing.T) {
		actualKeys, err := engine.GetSeriesByTags("memory.free", map[string]string{"host": "serverX"})
		if err != nil {
			t.Fatalf("GetSeriesByTags failed: %v", err)
		}
		if len(actualKeys) != 0 {
			t.Errorf("Expected no series for existing metric with non-existent tags, but got: %v", actualKeys)
		}
	})

	t.Run("Put_ExistingSeries_NoDuplicateInTagIndex", func(t *testing.T) {
		// Put another data point for an existing series (points[2])
		concreteEngine, ok := engine.(*storageEngine)
		if !ok {
			t.Fatal("Failed to cast engine to concrete type")
		}
		existingSeries := points[2]
		newTimestamp := existingSeries.timestamp + 10000
		newValue := existingSeries.value + 5.0

		if err := engine.Put(context.Background(), HelperDataPoint(t, existingSeries.metric, existingSeries.tags, newTimestamp, map[string]interface{}{"value": newValue})); err != nil {
			t.Fatalf("Put for existing series failed: %v", err)
		}

		// Query for this series again, the count should not increase
		actualKeys, err := engine.GetSeriesByTags(existingSeries.metric, existingSeries.tags) // This will use the new GetSeriesByTags logic
		if err != nil {
			t.Fatalf("GetSeriesByTags failed after putting data for existing series: %v", err)
		}
		expectedKeys := []string{existingSeries.seriesKey}
		if !sortAndCompare(actualKeys, expectedKeys) {
			t.Errorf("Expected series count to remain 1 for existing series after new data point, but got: %v", actualKeys)
		}

		// Verify overall active series count is still correct
		if count, err := concreteEngine.metrics.GetActiveSeriesCount(); err != nil {
			t.Errorf("Error getting active series count: %v", err)
		} else if count != 3 { // Still 3 active series
			t.Errorf("Overall active series count mismatch: got %d, want 3", count)
		}
	})
}

func TestStorageEngine_Query(t *testing.T) {
	opts := getBaseOptsForFlushTest(t)
	engine, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err = engine.Start(); err != nil {
		t.Fatalf("Failed to start setup engine: %v", err)
	}
	defer engine.Close()

	metric := "temp.cpu"
	tags := map[string]string{"dc": "nyc"}

	// Insert some data points
	ts1 := time.Now().UnixNano()
	ts2 := ts1 + 1000
	ts3 := ts1 + 2000

	engine.Put(context.Background(), HelperDataPoint(t, metric, tags, ts1, map[string]interface{}{"value": 25.0}))
	engine.Put(context.Background(), HelperDataPoint(t, metric, tags, ts2, map[string]interface{}{"value": 26.5}))
	engine.Put(context.Background(), HelperDataPoint(t, metric, tags, ts3, map[string]interface{}{"value": 27.0}))

	// Define the query parameters
	startTime := ts1 + 500                  // Just after ts1
	endTime := ts3 - 500                    // Just before ts3
	var aggregations []core.AggregationSpec // Empty for now
	downsampleInterval := ""                // Empty for now
	emitEmpty := false                      // For non-downsampling, this is effectively false

	// Perform the query
	querySimpleParams := core.QueryParams{
		Metric:             metric,
		StartTime:          startTime,
		EndTime:            endTime,
		Tags:               tags,
		AggregationSpecs:   aggregations,
		DownsampleInterval: downsampleInterval,
		EmitEmptyWindows:   emitEmpty,
	}
	resultIter, err := engine.Query(context.Background(), querySimpleParams)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer resultIter.Close()

	// Collect the results
	var collectedResults []struct {
		ts  int64
		val float64
	}

	for resultIter.Next() {
		item, err := resultIter.At()
		if err != nil {
			t.Fatalf("resultIter.At() failed: %v", err)
		}
		if !item.IsAggregated {
			fv, ok := item.Fields["value"]
			if !ok {
				t.Fatalf("Field 'value' not found in result item: %v", item.Fields)
			}

			v, ok := fv.ValueFloat64()
			if !ok {
				t.Fatalf("Failed to convert value to float64: %v", fv)
			}

			collectedResults = append(collectedResults, struct {
				ts  int64
				val float64
			}{ts: item.Timestamp, val: v})
		}
	}
	if err := resultIter.Error(); err != nil {
		t.Fatalf("Iterator error: %v", err)
	}

	// Assert the results
	if len(collectedResults) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(collectedResults))
	}

	if collectedResults[0].ts != ts2 {
		t.Errorf("Expected timestamp %d, got %d", ts2, collectedResults[0].ts)
	}
	if collectedResults[0].val != 26.5 {
		t.Errorf("Expected value 26.5, got %f", collectedResults[0].val)
	}
}

func TestStorageEngine_Query_WithAggregation(t *testing.T) {
	opts := getBaseOptsForFlushTest(t)
	engine, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err = engine.Start(); err != nil {
		t.Fatalf("Failed to start setup engine: %v", err)
	}
	defer engine.Close()

	metric := "system.load"
	tags := map[string]string{"region": "us-west"}

	ts := []int64{
		time.Now().UnixNano(),
		time.Now().UnixNano() + 1000,
		time.Now().UnixNano() + 2000,
		time.Now().UnixNano() + 3000,
	}
	vals := []float64{10.0, 20.0, 5.0, 15.0} // Sum=50, Count=4, Avg=12.5, Min=5, Max=20

	for i := 0; i < len(ts); i++ {
		if err := engine.Put(context.Background(), HelperDataPoint(t, metric, tags, ts[i], map[string]interface{}{"value": vals[i]})); err != nil {
			t.Fatalf("Put failed for ts[%d]: %v", i, err)
		}
	}

	// Query parameters
	startTime := ts[0]
	endTime := ts[len(ts)-1] + 1 // Ensure endTime is inclusive for the last point in key encoding

	tests := []struct {
		name               string
		aggregations       []core.AggregationSpec
		expectedAggResults map[string]float64 // Using string for AggregationType for easier map key
	}{
		{
			name: "count_sum_avg",
			aggregations: []core.AggregationSpec{
				{Function: core.AggCount, Field: "value"},
				{Function: core.AggSum, Field: "value"},
				{Function: core.AggAvg, Field: "value"},
			},
			expectedAggResults: map[string]float64{
				"count_value": 4.0,
				"sum_value":   50.0,
				"avg_value":   12.5,
			},
		},
		{
			name: "min_max",
			aggregations: []core.AggregationSpec{
				{Function: core.AggMin, Field: "value"},
				{Function: core.AggMax, Field: "value"},
			},
			expectedAggResults: map[string]float64{
				"min_value": 5.0,
				"max_value": 20.0,
			},
		},
		{
			name: "all_aggregations",
			aggregations: []core.AggregationSpec{
				{Function: core.AggCount, Field: "value"},
				{Function: core.AggSum, Field: "value"},
				{Function: core.AggAvg, Field: "value"},
				{Function: core.AggMin, Field: "value"},
				{Function: core.AggMax, Field: "value"},
			},
			expectedAggResults: map[string]float64{
				"count_value": 4.0,
				"sum_value":   50.0,
				"avg_value":   12.5,
				"min_value":   5.0,
				"max_value":   20.0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { // No downsampling, so emitEmptyWindows is false
			queryAggParams := core.QueryParams{
				Metric:             metric,
				StartTime:          startTime,
				EndTime:            endTime,
				Tags:               tags,
				AggregationSpecs:   tt.aggregations,
				DownsampleInterval: "", // No downsampling for these aggregation tests
				EmitEmptyWindows:   false,
			}
			resultIter, err := engine.Query(context.Background(), queryAggParams)
			if err != nil {
				t.Fatalf("Query with aggregations %v failed: %v", tt.aggregations, err)
			}
			defer resultIter.Close()

			var resultValues map[string]float64
			count := 0
			for resultIter.Next() {
				item, err := resultIter.At()
				if err != nil {
					t.Fatalf("resultIter.At() failed: %v", err)
				}
				resultValues = item.AggregatedValues

				count++
			}

			if count != 1 {
				t.Fatalf("AggregatingIterator.Next() returned false, expected one result")
			}

			for aggType, expectedVal := range tt.expectedAggResults {
				if actualVal, ok := resultValues[aggType]; !ok {
					t.Errorf("Aggregation type %s missing from result", aggType)
				} else if actualVal != expectedVal {
					t.Errorf("Aggregation %s: got %f, want %f", aggType, actualVal, expectedVal)
				}
			}
		})
	}
}

func TestStorageEngine_AggregationQueryLatencyMetric(t *testing.T) {
	opts := getBaseOptsForFlushTest(t)
	opts.Metrics = NewEngineMetrics(false, "agglatency_tsdb_")

	engine, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err = engine.Start(); err != nil {
		t.Fatalf("Failed to start setup engine: %v", err)
	}
	defer engine.Close()

	concreteEngine, ok := engine.(*storageEngine)
	if !ok {
		t.Fatal("Failed to cast engine to concrete type")
	}

	metricName := "latency.test.metric"
	tags := map[string]string{"test": "agg_latency"}
	numPoints := 1000 // Reduced number of points to prevent timeout
	startTime := time.Now().UnixNano()
	timestamps := make([]int64, numPoints) // Declare ts slice here

	// Using PutBatch for efficiency
	points := make([]core.DataPoint, numPoints)
	for i := 0; i < numPoints; i++ {
		currentTs := startTime + int64(i*1000)
		timestamps[i] = currentTs // Store each timestamp
		val := float64(i)
		points[i] = HelperDataPoint(t, metricName, tags, currentTs, map[string]interface{}{"value": val})
	}
	if err := engine.PutBatch(context.Background(), points); err != nil {
		t.Fatalf("PutBatch failed: %v", err)
	}

	// Get initial metric values
	// Since metrics are injected, we access them via engine.metrics
	// The names used in NewEngineMetrics (e.g., "agglatency_tsdb_aggregation_query_latency_seconds")
	// are for the expvar.Map instance itself if it were globally published.
	// Here, we access the *expvar.Map instance directly.
	aggLatencyHist := concreteEngine.metrics.AggregationQueryLatencyHist
	if aggLatencyHist == nil {
		t.Fatal("engine.metrics.AggregationQueryLatencyHist is nil")
	}
	initialCount := aggLatencyHist.Get("count").(*expvar.Int).Value() // Assuming "count" is initialized
	initialSum := aggLatencyHist.Get("sum").(*expvar.Float).Value()   // Assuming "sum" is initialized

	// Perform an aggregation query
	aggregations := []core.AggregationSpec{
		{Function: core.AggCount, Field: "value"}, // Corrected field to "value"
		{Function: core.AggSum, Field: "value"},   // Corrected field to "value"
	}

	queryStartTime := timestamps[0]
	queryEndTime := timestamps[len(timestamps)-1] + 1

	queryLatencyParams := core.QueryParams{
		Metric:             metricName,
		StartTime:          queryStartTime,
		EndTime:            queryEndTime,
		Tags:               tags,
		AggregationSpecs:   aggregations,
		DownsampleInterval: "",
		EmitEmptyWindows:   false,
	}
	resultIter, err := engine.Query(context.Background(), queryLatencyParams)
	if err != nil {
		t.Fatalf("Query with aggregation failed: %v", err)
	}
	// Drain the iterator
	for resultIter.Next() {
	}
	resultIter.Close()

	// Check if metrics were updated
	if count := aggLatencyHist.Get("count").(*expvar.Int).Value(); count != initialCount+1 {
		t.Errorf("Aggregation query latency count: got %d, want %d", count, initialCount+1)
	}
	if sum := aggLatencyHist.Get("sum").(*expvar.Float).Value(); sum <= initialSum {
		t.Errorf("Aggregation query latency sum: got %f, want > %f", sum, initialSum)
	}
}

// TestStorageEngine_Query_MultiSeries tests querying data that spans multiple series matching the query tags.
func TestStorageEngine_Query_MultiSeries(t *testing.T) {
	opts := getBaseOptsForFlushTest(t)
	engine, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err = engine.Start(); err != nil {
		t.Fatalf("Failed to start setup engine: %v", err)
	}
	defer engine.Close()

	ctx := context.Background()
	metric := "multi.query"

	// Series A: region=east, host=A
	tagsA := map[string]string{"region": "east", "host": "A"}
	engine.Put(ctx, HelperDataPoint(t, metric, tagsA, 100, map[string]interface{}{"value": 10.0}))
	engine.Put(ctx, HelperDataPoint(t, metric, tagsA, 300, map[string]interface{}{"value": 30.0}))

	// Series B: region=east, host=B
	tagsB := map[string]string{"region": "east", "host": "B"}
	engine.Put(ctx, HelperDataPoint(t, metric, tagsB, 150, map[string]interface{}{"value": 15.0}))
	engine.Put(ctx, HelperDataPoint(t, metric, tagsB, 250, map[string]interface{}{"value": 25.0}))

	// Series C: region=west, host=C (should not be in result)
	tagsC := map[string]string{"region": "west", "host": "C"}
	engine.Put(ctx, HelperDataPoint(t, metric, tagsC, 200, map[string]interface{}{"value": 20.0}))

	// Query for all series in region=east
	queryParams := core.QueryParams{
		Metric:    metric,
		Tags:      map[string]string{"region": "east"},
		StartTime: 0,
		EndTime:   1000,
	}

	iter, err := engine.Query(ctx, queryParams)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer iter.Close()

	// Expected results, interleaved by timestamp
	expected := []struct {
		ts   int64
		val  float64
		tags map[string]string
	}{
		{ts: 100, val: 10.0, tags: tagsA},
		{ts: 150, val: 15.0, tags: tagsB},
		{ts: 250, val: 25.0, tags: tagsB},
		{ts: 300, val: 30.0, tags: tagsA},
	}

	var actual []struct {
		ts   int64
		val  float64
		tags map[string]string
	}
	for iter.Next() {
		item, err := iter.At()
		if err != nil {
			t.Fatalf("iter.At() failed: %v", err)
		}
		fv, ok := item.Fields["value"]
		if !ok {
			t.Fatalf("Field 'value' not found in item.Fields")
		}

		val, ok := fv.ValueFloat64()
		if !ok {
			t.Fatalf("Failed to convert value to float64")
		}

		actual = append(actual, struct {
			ts   int64
			val  float64
			tags map[string]string
		}{ts: item.Timestamp, val: val, tags: item.Tags})
		iter.Put(item)
	}

	if err := iter.Error(); err != nil {
		t.Fatalf("Iterator error: %v", err)
	}

	if len(actual) != len(expected) {
		t.Fatalf("Expected %d results, got %d", len(expected), len(actual))
	}

	for i, exp := range expected {
		act := actual[i]
		if act.ts != exp.ts || act.val != exp.val || !reflect.DeepEqual(act.tags, exp.tags) {
			t.Errorf("Result %d mismatch:\nGot:    ts=%d, val=%.1f, tags=%v\nWanted: ts=%d, val=%.1f, tags=%v",
				i, act.ts, act.val, act.tags, exp.ts, exp.val, exp.tags)
		}
	}
}

func TestStorageEngine_Query_MultiSeries_WithAggregation(t *testing.T) {
	opts := getBaseOptsForFlushTest(t)
	engine, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err = engine.Start(); err != nil {
		t.Fatalf("Failed to start setup engine: %v", err)
	}
	defer engine.Close()

	ctx := context.Background()
	metric := "multi.agg"

	// Series A: region=east, type=prod
	tagsA := map[string]string{"region": "east", "type": "prod"}
	engine.Put(ctx, HelperDataPoint(t, metric, tagsA, 100, map[string]interface{}{"value": 10.0})) // val=10
	engine.Put(ctx, HelperDataPoint(t, metric, tagsA, 200, map[string]interface{}{"value": 20.0})) // val=20

	// Series B: region=east, type=dev
	tagsB := map[string]string{"region": "east", "type": "dev"}
	engine.Put(ctx, HelperDataPoint(t, metric, tagsB, 150, map[string]interface{}{"value": 5.0})) // val=5

	// Series C: region=west, type=prod
	tagsC := map[string]string{"region": "west", "type": "prod"}
	engine.Put(ctx, HelperDataPoint(t, metric, tagsC, 250, map[string]interface{}{"value": 100.0})) // Should not be included

	// Query for all series in region=east and aggregate
	queryParams := core.QueryParams{
		Metric:    metric,
		Tags:      map[string]string{"region": "east"},
		StartTime: 0,
		EndTime:   1000,
		AggregationSpecs: []core.AggregationSpec{
			{Function: core.AggCount, Field: "value"},
			{Function: core.AggSum, Field: "value"},
			{Function: core.AggAvg, Field: "value"},
			{Function: core.AggMin, Field: "value"},
			{Function: core.AggMax, Field: "value"},
		},
	}

	iter, err := engine.Query(ctx, queryParams)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer iter.Close()

	// Expected results: sum=35, count=3, avg=11.666..., min=5, max=20
	expected := map[string]float64{
		"sum_value":   35.0,
		"count_value": 3.0,
		"avg_value":   35.0 / 3.0,
		"min_value":   5.0,
		"max_value":   20.0,
	}

	if !iter.Next() {
		t.Fatal("Expected one aggregated result, but got none")
	}

	item, err := iter.At()
	if err != nil {
		t.Fatalf("iter.At() failed: %v", err)
	}

	if !item.IsAggregated {
		t.Error("Expected result to be aggregated")
	}

	t.Log(item.AggregatedValues, expected)

	compareFloatMaps(t, item.AggregatedValues, expected, 0)
	iter.Put(item)

	if iter.Next() {
		t.Fatal("Expected only one result from aggregation query")
	}

	if err := iter.Error(); err != nil {
		t.Fatalf("Iterator error: %v", err)
	}
}

func TestStorageEngine_Put_WithHooks(t *testing.T) {
	ctx := context.Background()

	t.Run("PreHook_CancelOperation", func(t *testing.T) {
		// Setup
		opts := getBaseOptsForFlushTest(t)
		engine, err := NewStorageEngine(opts)
		if err != nil {
			t.Fatalf("NewStorageEngine failed: %v", err)
		}
		require.NoError(t, engine.Start(), "Failed to start setup engine")
		defer engine.Close()

		concreteEngine := engine.(*storageEngine)
		expectedErr := errors.New("cancelled by pre-hook")
		listener := &mockListener{
			priority:  1,
			isAsync:   false,
			returnErr: expectedErr,
		}
		concreteEngine.hookManager.Register(hooks.EventPrePutDataPoint, listener)

		// Action
		metric := "hook.test.cancel"
		tags := map[string]string{"test": "prehook"}
		ts := time.Now().UnixNano()
		err = engine.Put(ctx, HelperDataPoint(t, metric, tags, ts, map[string]interface{}{"value": 1.0}))

		// Assertions for Put
		require.Error(t, err, "Put should have returned an error, but got nil")
		require.ErrorIs(t, err, expectedErr, "Expected error to wrap '%v', but got '%v'", expectedErr, err)

		// Verify data was not written
		_, getErr := engine.Get(ctx, metric, tags, ts)
		require.ErrorIs(t, getErr, sstable.ErrNotFound, "Get should return ErrNotFound for cancelled put")
	})

	t.Run("PreHook_ModifyPayload", func(t *testing.T) {
		// Setup
		opts := getBaseOptsForFlushTest(t)
		engine, err := NewStorageEngine(opts)
		if err != nil {
			t.Fatalf("NewStorageEngine failed: %v", err)
		}
		require.NoError(t, engine.Start(), "Failed to start setup engine")
		defer engine.Close()

		concreteEngine := engine.(*storageEngine)
		modifiedValue := 999.9
		listener := &mockListener{
			priority: 1,
			isAsync:  false,
			onEventFunc: func(event hooks.HookEvent) {
				if p, ok := event.Payload().(hooks.PrePutDataPointPayload); ok {
					// To modify a field, we create a new FieldValues map with the desired value
					// and then extract the single FieldValue from it. This respects the core package's API.
					newFields, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": modifiedValue})
					require.NoError(t, err, "hook failed to create new field values")
					(*p.Fields)["value"] = newFields["value"]
				}
			},
		}
		concreteEngine.hookManager.Register(hooks.EventPrePutDataPoint, listener)

		// Action
		metric := "hook.test.modify"
		tags := map[string]string{"test": "prehook"}
		ts := time.Now().UnixNano()
		err = engine.Put(ctx, HelperDataPoint(t, metric, tags, ts, map[string]interface{}{"value": 1.0}))
		require.NoError(t, err, "Put returned an unexpected error")

		// Assertions
		retrievedVal, err := engine.Get(ctx, metric, tags, ts)
		require.NoError(t, err, "Get failed after modification")

		v, ok := retrievedVal["value"].ValueFloat64()
		require.True(t, ok, "retrieved value is not a float64")
		assert.True(t, ApproximatelyEqual(v, modifiedValue), "Expected value to be modified by hook. Got %f, want %f", v, modifiedValue)
	})

	t.Run("PostHook_AsyncNotification", func(t *testing.T) {
		// Setup
		opts := getBaseOptsForFlushTest(t)
		engine, err := NewStorageEngine(opts)
		if err != nil {
			t.Fatalf("NewStorageEngine failed: %v", err)
		}
		require.NoError(t, engine.Start(), "Failed to start setup engine")
		defer engine.Close()

		concreteEngine := engine.(*storageEngine)
		signalChan := make(chan hooks.HookEvent, 1)
		listener := &mockListener{
			priority:   1,
			isAsync:    true,
			callSignal: signalChan,
		}
		concreteEngine.hookManager.Register(hooks.EventPostPutDataPoint, listener)

		// Action
		metric := "hook.test.post"
		tags := map[string]string{"test": "posthook"}
		ts := time.Now().UnixNano()
		val := 123.45
		err = engine.Put(ctx, HelperDataPoint(t, metric, tags, ts, map[string]interface{}{"value": val}))
		require.NoError(t, err, "Put returned an unexpected error")

		// Assertions
		// 1. Data should be readable immediately
		_, err = engine.Get(ctx, metric, tags, ts)
		require.NoError(t, err, "Get failed for post-hook test")

		// 2. Wait for the async hook to be called
		select {
		case event := <-signalChan:
			payload, ok := event.Payload().(hooks.PostPutDataPointPayload)
			require.True(t, ok, "Post-hook received payload of wrong type: %T", event.Payload())

			hookVal, ok := payload.Fields["value"].ValueFloat64()
			require.True(t, ok, "Payload 'value' field is not a float64")

			assert.Equal(t, metric, payload.Metric, "Metric mismatch in post-hook payload")
			assert.Equal(t, ts, payload.Timestamp, "Timestamp mismatch in post-hook payload")
			assert.True(t, ApproximatelyEqual(hookVal, val), "Value mismatch in post-hook payload. Got %f, want %f", hookVal, val)

		case <-time.After(200 * time.Millisecond):
			t.Fatal("Timed out waiting for post-hook to be called")
		}
	})
}

func TestStorageEngine_StartedStateChecks(t *testing.T) {
	ctx := context.Background()
	point := HelperDataPoint(t, "metric", map[string]string{"tag": "val"}, 1, map[string]interface{}{"value": 1.0})

	t.Run("Put_On_Not_Started_Engine", func(t *testing.T) {
		opts := getBaseOptsForFlushTest(t)
		engine, err := NewStorageEngine(opts)
		require.NoError(t, err)
		// Do NOT call engine.Start()

		err = engine.Put(ctx, point)
		require.ErrorIs(t, err, ErrEngineClosed, "Put on a non-started engine should return ErrEngineClosed")
	})

	t.Run("Get_On_Not_Started_Engine", func(t *testing.T) {
		opts := getBaseOptsForFlushTest(t)
		engine, err := NewStorageEngine(opts)
		require.NoError(t, err)
		// Do NOT call engine.Start()

		_, err = engine.Get(ctx, "metric", map[string]string{"tag": "val"}, 1)
		require.ErrorIs(t, err, ErrEngineClosed, "Get on a non-started engine should return ErrEngineClosed")
	})

	t.Run("Put_On_Closed_Engine", func(t *testing.T) {
		opts := getBaseOptsForFlushTest(t)
		engine, err := NewStorageEngine(opts)
		require.NoError(t, err)
		err = engine.Start()
		require.NoError(t, err)
		err = engine.Close()
		require.NoError(t, err)

		err = engine.Put(ctx, point)
		require.ErrorIs(t, err, ErrEngineClosed, "Put on a closed engine should return ErrEngineClosed")
	})

	t.Run("Get_On_Closed_Engine", func(t *testing.T) {
		opts := getBaseOptsForFlushTest(t)
		engine, err := NewStorageEngine(opts)
		require.NoError(t, err)
		err = engine.Start()
		require.NoError(t, err)

		// Put data while it's open
		err = engine.Put(ctx, point)
		require.NoError(t, err)

		err = engine.Close()
		require.NoError(t, err)

		_, err = engine.Get(ctx, "metric", map[string]string{"tag": "val"}, 1)
		require.ErrorIs(t, err, ErrEngineClosed, "Get on a closed engine should return ErrEngineClosed")
	})

	t.Run("Query_On_Not_Started_Engine", func(t *testing.T) {
		opts := getBaseOptsForFlushTest(t)
		engine, err := NewStorageEngine(opts)
		require.NoError(t, err)
		// Do NOT call engine.Start()

		_, err = engine.Query(ctx, core.QueryParams{Metric: "metric"})
		require.ErrorIs(t, err, ErrEngineClosed, "Query on a non-started engine should return ErrEngineClosed")
	})

	t.Run("Query_On_Closed_Engine", func(t *testing.T) {
		opts := getBaseOptsForFlushTest(t)
		engine, err := NewStorageEngine(opts)
		require.NoError(t, err)
		err = engine.Start()
		require.NoError(t, err)
		err = engine.Close()
		require.NoError(t, err)

		_, err = engine.Query(ctx, core.QueryParams{Metric: "metric"})
		require.ErrorIs(t, err, ErrEngineClosed, "Query on a closed engine should return ErrEngineClosed")
	})

	t.Run("DeleteSeries_On_Not_Started_Engine", func(t *testing.T) {
		opts := getBaseOptsForFlushTest(t)
		engine, err := NewStorageEngine(opts)
		require.NoError(t, err)
		// Do NOT call engine.Start()

		err = engine.DeleteSeries(ctx, "metric", map[string]string{"tag": "val"})
		require.ErrorIs(t, err, ErrEngineClosed, "DeleteSeries on a non-started engine should return ErrEngineClosed")
	})

	t.Run("DeleteSeries_On_Closed_Engine", func(t *testing.T) {
		opts := getBaseOptsForFlushTest(t)
		engine, err := NewStorageEngine(opts)
		require.NoError(t, err)
		err = engine.Start()
		require.NoError(t, err)
		// Put some data so the series exists
		err = engine.Put(ctx, point)
		require.NoError(t, err)
		err = engine.Close()
		require.NoError(t, err)

		err = engine.DeleteSeries(ctx, "metric", map[string]string{"tag": "val"})
		require.ErrorIs(t, err, ErrEngineClosed, "DeleteSeries on a closed engine should return ErrEngineClosed")
	})

	t.Run("Other_APIs_On_Not_Started_Engine", func(t *testing.T) {
		opts := getBaseOptsForFlushTest(t)
		engine, err := NewStorageEngine(opts)
		require.NoError(t, err)
		// Do NOT call engine.Start()
		assert.PanicsWithError(t, "GetNextSSTableID called on a non-running engine: engine is closed or not started", func() {
			engine.GetNextSSTableID()
		}, "GetNextSSTableID on non-started engine should panic")

		assert.Equal(t, "", engine.GetWALPath(), "GetWALPath on non-started engine should return empty string")

		_, err = engine.Metrics()
		assert.ErrorIs(t, err, ErrEngineClosed, "Metrics on non-started engine should return ErrEngineClosed")

		_, err = engine.GetPubSub()
		assert.ErrorIs(t, err, ErrEngineClosed, "GetPubSub on non-started engine should return ErrEngineClosed")

	})

	t.Run("Other_APIs_On_Closed_Engine", func(t *testing.T) {
		opts := getBaseOptsForFlushTest(t)
		engine, err := NewStorageEngine(opts)
		require.NoError(t, err)
		err = engine.Start()
		require.NoError(t, err)
		err = engine.Close()
		require.NoError(t, err)

		assert.PanicsWithError(t, "GetNextSSTableID called on a non-running engine: engine is closed or not started", func() {
			engine.GetNextSSTableID()
		}, "GetNextSSTableID on closed engine should panic")

		assert.Equal(t, "", engine.GetWALPath(), "GetWALPath on closed engine should return empty string")

		_, err = engine.Metrics()
		assert.ErrorIs(t, err, ErrEngineClosed, "Metrics on closed engine should return ErrEngineClosed")

		_, err = engine.GetPubSub()
		assert.ErrorIs(t, err, ErrEngineClosed, "GetPubSub on closed engine should return ErrEngineClosed")

	})
}

func TestStorageEngine_Get_WithHooks(t *testing.T) {
	ctx := context.Background()
	metric := "hook.get.test"
	tagsA := map[string]string{"id": "A"}
	tsA := int64(100)
	valA := 10.0

	tagsB := map[string]string{"id": "B"}
	tsB := int64(200)
	valB := 20.0

	// Helper to set up a clean engine for each subtest, preventing data races on shared state like the hook manager.
	setupTest := func(t *testing.T) (*storageEngine, func()) {
		opts := getBaseOptsForFlushTest(t)
		engine, err := NewStorageEngine(opts)
		require.NoError(t, err)
		require.NoError(t, engine.Start())

		// Setup data for this specific engine instance
		require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric, tagsA, tsA, map[string]interface{}{"value": valA})))
		require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric, tagsB, tsB, map[string]interface{}{"value": valB})))

		return engine.(*storageEngine), func() { engine.Close() }
	}

	t.Run("PreHook_CancelOperation", func(t *testing.T) {
		engine, cleanup := setupTest(t)
		defer cleanup()

		expectedErr := errors.New("get cancelled by pre-hook")
		listener := &mockListener{returnErr: expectedErr}
		engine.hookManager.Register(hooks.EventPreGetPoint, listener)

		_, err := engine.Get(ctx, metric, tagsA, tsA)
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("PreHook_ModifyPayload", func(t *testing.T) {
		engine, cleanup := setupTest(t)
		defer cleanup()

		listener := &mockListener{
			onEventFunc: func(event hooks.HookEvent) {
				if p, ok := event.Payload().(hooks.PreGetPointPayload); ok {
					*p.Tags = tagsB // Redirect Get from A to B
					*p.Timestamp = tsB
				}
			},
		}
		engine.hookManager.Register(hooks.EventPreGetPoint, listener)

		// Attempt to get A, but hook should redirect to B
		result, err := engine.Get(ctx, metric, tagsA, tsA)
		require.NoError(t, err)
		retrievedVal := HelperFieldValueValidateFloat64(t, result, "value")
		assert.InDelta(t, valB, retrievedVal, 1e-9, "Expected to get value from series B due to hook modification")
	})

	t.Run("PostHook_ModifyResult", func(t *testing.T) {
		engine, cleanup := setupTest(t)
		defer cleanup()

		modifiedValue := 999.9
		listener := &mockListener{
			isAsync: false, // Must be sync to modify result
			onEventFunc: func(event hooks.HookEvent) {
				if p, ok := event.Payload().(hooks.PostGetPointPayload); ok && p.Error == nil {
					// Modify the result in place
					newFields, _ := core.NewFieldValuesFromMap(map[string]interface{}{"value": modifiedValue})
					*p.Result = newFields
				}
			},
		}
		engine.hookManager.Register(hooks.EventPostGetPoint, listener)

		result, err := engine.Get(ctx, metric, tagsA, tsA)
		require.NoError(t, err)
		retrievedVal := HelperFieldValueValidateFloat64(t, result, "value")
		assert.InDelta(t, modifiedValue, retrievedVal, 1e-9, "Expected result to be modified by post-hook")
	})

	t.Run("PostHook_AsyncNotification", func(t *testing.T) {
		engine, cleanup := setupTest(t)
		defer cleanup()

		signalChan := make(chan hooks.HookEvent, 1)
		listener := &mockListener{isAsync: true, callSignal: signalChan}
		engine.hookManager.Register(hooks.EventPostGetPoint, listener)

		_, err := engine.Get(ctx, metric, tagsA, tsA)
		require.NoError(t, err)

		select {
		case event := <-signalChan:
			payload, ok := event.Payload().(hooks.PostGetPointPayload)
			require.True(t, ok)
			assert.NoError(t, payload.Error)
			assert.Equal(t, metric, payload.Metric)
			retrievedVal := HelperFieldValueValidateFloat64(t, *payload.Result, "value")
			assert.InDelta(t, valA, retrievedVal, 1e-9)
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Timed out waiting for post-get-hook to be called")
		}
	})
}

func TestStorageEngine_Delete_WithHooks(t *testing.T) {
	ctx := context.Background()
	metric := "hook.delete.test"
	tagsA := map[string]string{"id": "A"}
	tsA := int64(100)
	tagsB := map[string]string{"id": "B"}
	tsB := int64(200)

	// Helper to set up a clean engine for each subtest
	setupTest := func(t *testing.T) (*storageEngine, func()) {
		opts := getBaseOptsForFlushTest(t)
		engine, err := NewStorageEngine(opts)
		require.NoError(t, err)
		require.NoError(t, engine.Start())

		// Setup data
		require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric, tagsA, tsA, map[string]interface{}{"value": 10.0})))
		require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric, tagsB, tsB, map[string]interface{}{"value": 20.0})))

		return engine.(*storageEngine), func() { engine.Close() }
	}

	t.Run("PreHook_CancelOperation", func(t *testing.T) {
		engine, cleanup := setupTest(t)
		defer cleanup()

		expectedErr := errors.New("delete cancelled by pre-hook")
		listener := &mockListener{returnErr: expectedErr}
		engine.hookManager.Register(hooks.EventPreDeletePoint, listener)

		// Attempt to delete point A
		err := engine.Delete(ctx, metric, tagsA, tsA)
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)

		// Verify point A was NOT deleted
		_, getErr := engine.Get(ctx, metric, tagsA, tsA)
		assert.NoError(t, getErr, "Point A should not have been deleted")
	})

	t.Run("PreHook_ModifyPayload", func(t *testing.T) {
		engine, cleanup := setupTest(t)
		defer cleanup()

		listener := &mockListener{
			onEventFunc: func(event hooks.HookEvent) {
				if p, ok := event.Payload().(hooks.PreDeletePointPayload); ok {
					*p.Tags = tagsB // Redirect Delete from A to B
					*p.Timestamp = tsB
				}
			},
		}
		engine.hookManager.Register(hooks.EventPreDeletePoint, listener)

		// Attempt to delete A, but hook should redirect to B
		err := engine.Delete(ctx, metric, tagsA, tsA)
		require.NoError(t, err)

		// Verify A was NOT deleted
		_, getErrA := engine.Get(ctx, metric, tagsA, tsA)
		assert.NoError(t, getErrA, "Point A should not have been deleted")

		// Verify B WAS deleted
		_, getErrB := engine.Get(ctx, metric, tagsB, tsB)
		assert.ErrorIs(t, getErrB, sstable.ErrNotFound, "Point B should have been deleted by the hook")
	})

	t.Run("PostHook_AsyncNotification", func(t *testing.T) {
		engine, cleanup := setupTest(t)
		defer cleanup()

		signalChan := make(chan hooks.HookEvent, 1)
		listener := &mockListener{isAsync: true, callSignal: signalChan}
		engine.hookManager.Register(hooks.EventPostDeletePoint, listener)

		err := engine.Delete(ctx, metric, tagsA, tsA)
		require.NoError(t, err)

		select {
		case event := <-signalChan:
			payload, ok := event.Payload().(hooks.PostDeletePointPayload)
			require.True(t, ok)
			assert.NoError(t, payload.Error)
			assert.Equal(t, metric, payload.Metric)
			assert.Equal(t, tsA, payload.Timestamp)
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Timed out waiting for post-delete-hook to be called")
		}
	})
}

func TestStorageEngine_Query_RelativeTime(t *testing.T) {
	// 1. Setup
	// Use a mock clock for predictable "now"
	mockNow := time.Date(2024, 7, 16, 12, 0, 0, 0, time.UTC)
	mockClock := clock.NewMockClock(mockNow)

	opts := getBaseOptsForFlushTest(t)
	opts.Clock = mockClock // Inject the mock clock
	engine, err := NewStorageEngine(opts)
	require.NoError(t, err)
	err = engine.Start()
	require.NoError(t, err)
	defer engine.Close()

	ctx := context.Background()
	metric := "test.query.relative"
	tags := map[string]string{"host": "relativehost"}

	// 2. Data Population
	// This point is 30 minutes ago, should be included in a RELATIVE(1h) query
	tsInside := mockNow.Add(-30 * time.Minute).UnixNano()
	valInside := 30.0
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric, tags, tsInside, map[string]interface{}{"value": valInside})))

	// This point is 90 minutes ago, should be excluded
	tsOutside := mockNow.Add(-90 * time.Minute).UnixNano()
	valOutside := 90.0
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric, tags, tsOutside, map[string]interface{}{"value": valOutside})))

	// This point is in the future, should be excluded
	tsFuture := mockNow.Add(10 * time.Minute).UnixNano()
	valFuture := -10.0
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric, tags, tsFuture, map[string]interface{}{"value": valFuture})))

	// 3. Query Execution
	params := core.QueryParams{
		Metric:           metric,
		Tags:             tags,
		IsRelative:       true,
		RelativeDuration: "1h",
	}

	iter, err := engine.Query(ctx, params)
	require.NoError(t, err)
	require.NotNil(t, iter)
	defer iter.Close()

	// 4. Verification
	var results []*core.QueryResultItem
	for iter.Next() {
		item, errAt := iter.At()
		require.NoError(t, errAt)
		results = append(results, item)
	}
	require.NoError(t, iter.Error())

	// We expect only one result: the point at tsInside
	require.Len(t, results, 1, "Expected exactly one data point to be returned")

	resultItem := results[0]
	assert.Equal(t, tsInside, resultItem.Timestamp, "Timestamp of the returned point is incorrect")

	retrievedVal := HelperFieldValueValidateFloat64(t, resultItem.Fields, "value")
	assert.InDelta(t, valInside, retrievedVal, 1e-9, "Value of the returned point is incorrect")
}

func TestStorageEngine_Query_RelativeTime_WithAggregation(t *testing.T) {
	// 1. Setup
	// Use a mock clock for predictable "now"
	mockNow := time.Date(2024, 7, 16, 12, 0, 0, 0, time.UTC)
	mockClock := clock.NewMockClock(mockNow)

	opts := getBaseOptsForFlushTest(t)
	opts.Clock = mockClock // Inject the mock clock
	engine, err := NewStorageEngine(opts)
	require.NoError(t, err)
	err = engine.Start()
	require.NoError(t, err)
	defer engine.Close()

	ctx := context.Background()
	metric := "test.query.relative.agg"
	tags := map[string]string{"host": "relativeagg-host"}

	// 2. Data Population
	// Points within the last hour
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric, tags, mockNow.Add(-10*time.Minute).UnixNano(), map[string]interface{}{"value": 10.0})))
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric, tags, mockNow.Add(-20*time.Minute).UnixNano(), map[string]interface{}{"value": 20.0})))
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric, tags, mockNow.Add(-30*time.Minute).UnixNano(), map[string]interface{}{"value": 30.0})))

	// Point outside the last hour
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric, tags, mockNow.Add(-90*time.Minute).UnixNano(), map[string]interface{}{"value": 99.0})))

	// 3. Query Execution
	params := core.QueryParams{
		Metric:           metric,
		Tags:             tags,
		IsRelative:       true,
		RelativeDuration: "1h",
		AggregationSpecs: []core.AggregationSpec{
			{Function: core.AggSum, Field: "value"},
			{Function: core.AggCount, Field: "value"},
			{Function: core.AggAvg, Field: "value"},
			{Function: core.AggMin, Field: "value"},
			{Function: core.AggMax, Field: "value"},
		},
	}

	iter, err := engine.Query(ctx, params)
	require.NoError(t, err)
	require.NotNil(t, iter)
	defer iter.Close()

	// 4. Verification
	require.True(t, iter.Next(), "Expected one aggregated result")

	item, err := iter.At()
	require.NoError(t, err)

	assert.True(t, item.IsAggregated, "Result item should be aggregated")
	assert.False(t, iter.Next(), "Expected only one result item")
	require.NoError(t, iter.Error())

	// Expected results: sum=60, count=3, avg=20, min=10, max=30
	expected := map[string]float64{
		"sum_value":   60.0,
		"count_value": 3.0,
		"avg_value":   20.0,
		"min_value":   10.0,
		"max_value":   30.0,
	}

	compareFloatMaps(t, item.AggregatedValues, expected, 0)

	// Verify window times for final aggregation. With the fix, it should report the exact window.
	expectedStartTime := mockNow.Add(-1 * time.Hour).UnixNano()
	expectedEndTime := mockNow.UnixNano()
	assert.Equal(t, expectedStartTime, item.WindowStartTime, "WindowStartTime should match the start of the relative range")
	assert.Equal(t, expectedEndTime, item.WindowEndTime, "WindowEndTime should match 'now'")
}

func TestStorageEngine_Query_RelativeTime_WithDownsampling(t *testing.T) {
	// 1. Setup
	// Use a mock clock for predictable "now"
	mockNow := time.Date(2024, 7, 16, 12, 0, 0, 0, time.UTC)
	mockClock := clock.NewMockClock(mockNow)

	opts := getBaseOptsForFlushTest(t)
	opts.Clock = mockClock // Inject the mock clock
	engine, err := NewStorageEngine(opts)
	require.NoError(t, err)
	err = engine.Start()
	require.NoError(t, err)
	defer engine.Close()

	ctx := context.Background()
	metric := "test.query.relative.downsample"
	tags := map[string]string{"host": "relativeds-host"}

	// 2. Data Population
	// Window 1 (11:45:00 - 11:59:59)
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric, tags, mockNow.Add(-5*time.Minute).UnixNano(), map[string]interface{}{"value": 10.0})))
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric, tags, mockNow.Add(-10*time.Minute).UnixNano(), map[string]interface{}{"value": 20.0})))
	// Window 2 (11:30:00 - 11:44:59)
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric, tags, mockNow.Add(-25*time.Minute).UnixNano(), map[string]interface{}{"value": 30.0})))
	// Window 3 (11:15:00 - 11:29:59) - Empty
	// Window 4 (11:00:00 - 11:14:59)
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric, tags, mockNow.Add(-55*time.Minute).UnixNano(), map[string]interface{}{"value": 40.0})))
	// Point outside the 1h relative query range
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric, tags, mockNow.Add(-65*time.Minute).UnixNano(), map[string]interface{}{"value": 99.0})))

	// 3. Query Execution
	params := core.QueryParams{
		Metric:             metric,
		Tags:               tags,
		IsRelative:         true,
		RelativeDuration:   "1h",
		DownsampleInterval: "15m",
		EmitEmptyWindows:   true,
		AggregationSpecs: []core.AggregationSpec{
			{Function: core.AggSum, Field: "value"},
			{Function: core.AggCount, Field: "value"},
		},
	}

	iter, err := engine.Query(ctx, params)
	require.NoError(t, err)
	require.NotNil(t, iter)
	defer iter.Close()

	// 4. Verification
	var results []*core.QueryResultItem
	for iter.Next() {
		item, errAt := iter.At()
		require.NoError(t, errAt)
		results = append(results, item)
	}
	require.NoError(t, iter.Error())

	// The query is for the last hour [11:00, 12:00).
	// The downsampler will create windows for 11:00, 11:15, 11:30, 11:45.
	require.Len(t, results, 4, "Expected 4 downsampled windows")

	// Expected results are ordered from oldest to newest window
	expectedWindows := []struct {
		startTime time.Time
		values    map[string]float64
	}{
		{startTime: time.Date(2024, 7, 16, 11, 0, 0, 0, time.UTC), values: map[string]float64{"sum_value": 40.0, "count_value": 1.0}},
		{startTime: time.Date(2024, 7, 16, 11, 15, 0, 0, time.UTC), values: map[string]float64{"sum_value": 0.0, "count_value": 0.0}},
		{startTime: time.Date(2024, 7, 16, 11, 30, 0, 0, time.UTC), values: map[string]float64{"sum_value": 30.0, "count_value": 1.0}},
		{startTime: time.Date(2024, 7, 16, 11, 45, 0, 0, time.UTC), values: map[string]float64{"sum_value": 30.0, "count_value": 2.0}},
	}

	for i, expected := range expectedWindows {
		actual := results[i]
		assert.Equal(t, expected.startTime.UnixNano(), actual.WindowStartTime, "Window %d start time mismatch", i)
		compareFloatMaps(t, actual.AggregatedValues, expected.values, i)
	}
}

func TestStorageEngine_Query_FromMemtableAndSSTable(t *testing.T) {
	// 1. Setup
	opts := getBaseOptsForFlushTest(t)
	engine, err := NewStorageEngine(opts)
	require.NoError(t, err)
	err = engine.Start()
	require.NoError(t, err)
	defer engine.Close()

	ctx := context.Background()
	metric := "test.query.mixed.source"
	tags := map[string]string{"host": "mixed-host"}

	// 2. Data Population - Phase 1 (to be flushed)
	// These points will end up in an SSTable.
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric, tags, 100, map[string]interface{}{"value": 10.0})))
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric, tags, 300, map[string]interface{}{"value": 30.0})))

	// Force a flush to move the above data to an L0 SSTable.
	// The 'true' argument makes it a synchronous wait.
	err = engine.ForceFlush(ctx, true)
	require.NoError(t, err, "ForceFlush failed")

	// 3. Data Population - Phase 2 (to remain in memtable)
	// These points will be in the mutable memtable during the query.
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric, tags, 50, map[string]interface{}{"value": 5.0})))
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric, tags, 200, map[string]interface{}{"value": 20.0})))

	// 4. Query Execution
	// Query a range that covers data from both the SSTable and the memtable.
	params := core.QueryParams{
		Metric:    metric,
		Tags:      tags,
		StartTime: 0,
		EndTime:   1000,
	}

	iter, err := engine.Query(ctx, params)
	require.NoError(t, err)
	require.NotNil(t, iter)
	defer iter.Close()

	// 5. Verification
	// Collect results and verify they are merged and sorted correctly.
	var results []*core.QueryResultItem
	for iter.Next() {
		item, errAt := iter.At()
		require.NoError(t, errAt)
		results = append(results, item)
	}
	require.NoError(t, iter.Error())

	// Expected results, ordered by timestamp (ascending is default)
	expected := []struct {
		ts  int64
		val float64
	}{
		{ts: 50, val: 5.0},
		{ts: 100, val: 10.0},
		{ts: 200, val: 20.0},
		{ts: 300, val: 30.0},
	}

	require.Len(t, results, len(expected), "Expected %d results, but got %d", len(expected), len(results))

	for i, exp := range expected {
		actual := results[i]
		assert.Equal(t, exp.ts, actual.Timestamp, "Timestamp mismatch at index %d", i)
		retrievedVal := HelperFieldValueValidateFloat64(t, actual.Fields, "value")
		assert.InDelta(t, exp.val, retrievedVal, 1e-9, "Value mismatch at index %d", i)
	}
}
