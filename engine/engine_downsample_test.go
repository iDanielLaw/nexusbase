package engine

import (
	"context"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/sys"
	"github.com/stretchr/testify/require"
)

func init() {
	sys.SetDebugMode(false)
}

func TestStorageEngine_Query_MultiFieldDownsample(t *testing.T) {
	opts := GetBaseOptsForTest(t, "test")
	engine, err := NewStorageEngine(opts)
	require.NoError(t, err)
	err = engine.Start()
	require.NoError(t, err)
	defer engine.Close()

	ctx := context.Background()
	metric := "http.requests"
	tags := map[string]string{"service": "api-gateway", "region": "us-east-1"}
	baseTime := time.Date(2024, 7, 15, 10, 0, 0, 0, time.UTC)

	// Data points spread over 3 minutes
	points := []struct {
		offsetSeconds int
		fields        map[string]interface{}
	}{
		// Window 1 (10:00:00 - 10:00:59)
		{10, map[string]interface{}{"latency_ms": 100.0, "status_code": int64(200), "bytes_out": int64(1024)}},
		{30, map[string]interface{}{"latency_ms": 150.0, "status_code": int64(200), "bytes_out": int64(2048)}},
		{50, map[string]interface{}{"latency_ms": 125.0, "status_code": int64(503), "bytes_out": int64(512)}},
		// Window 2 (10:01:00 - 10:01:59) - EMPTY
		// Window 3 (10:02:00 - 10:02:59)
		{130, map[string]interface{}{"latency_ms": 200.0, "status_code": int64(200), "bytes_out": int64(4096)}},
		{170, map[string]interface{}{"latency_ms": 220.0, "status_code": int64(404), "bytes_out": int64(256)}},
	}

	for _, p := range points {
		ts := baseTime.Add(time.Duration(p.offsetSeconds) * time.Second).UnixNano()
		dp := HelperDataPoint(t, metric, tags, ts, p.fields)
		require.NoError(t, engine.Put(ctx, dp))
	}

	// Flush to make sure data is in SSTables
	concreteEngine := engine.(*storageEngine)
	require.NoError(t, concreteEngine.flushMemtableToSSTable(context.Background(), concreteEngine.mutableMemtable))
	queryParams := core.QueryParams{
		Metric:    metric,
		Tags:      tags,
		StartTime: baseTime.UnixNano(),
		EndTime:   baseTime.Add(3*time.Minute).UnixNano() - 1,
		Limit:     10,
		AggregationSpecs: []core.AggregationSpec{
			{Function: core.AggSum, Field: "latency_ms"},
			{Function: core.AggCount, Field: "latency_ms"},
			{Function: core.AggAvg, Field: "latency_ms"},
			{Function: core.AggMin, Field: "latency_ms"},
			{Function: core.AggMax, Field: "latency_ms"},
			{Function: core.AggSum, Field: "bytes_out"},
			{Function: core.AggCount, Field: "status_code"}, // Count on a different field
		},
		DownsampleInterval: "1m",
		EmitEmptyWindows:   true,
	}

	iter, err := engine.Query(ctx, queryParams)
	require.NoError(t, err)
	defer iter.Close()

	var results []*core.QueryResultItem
	for iter.Next() {
		item, errAt := iter.At()
		require.NoError(t, errAt)
		results = append(results, item)
	}
	require.NoError(t, iter.Error())
	t.Log(results)
	require.Len(t, results, 3, "should have 3 windows (1 empty)")

	// --- Verify Window 1 ---
	win1 := results[0]
	require.Equal(t, baseTime.UnixNano(), win1.WindowStartTime)
	expected1 := map[string]float64{
		"sum_latency_ms":    375.0,
		"count_latency_ms":  3.0,
		"avg_latency_ms":    125.0,
		"min_latency_ms":    100.0,
		"max_latency_ms":    150.0,
		"sum_bytes_out":     3584.0,
		"count_status_code": 3.0,
	}
	compareFloatMaps(t, win1.AggregatedValues, expected1, 0)

	// --- Verify Window 2 (Empty) ---
	win2 := results[1]
	require.Equal(t, baseTime.Add(1*time.Minute).UnixNano(), win2.WindowStartTime)
	// For empty windows, we expect 0 for sum/count and NaN for avg/min/max.
	// This requires the aggregator implementations to handle the "no data" case correctly.
	// The provided `iterator/aggregator.go` already does this.

	// --- Verify Window 3 ---
	win3 := results[2]
	require.Equal(t, baseTime.Add(2*time.Minute).UnixNano(), win3.WindowStartTime)
	expected3 := map[string]float64{
		"sum_latency_ms":    420.0,
		"count_latency_ms":  2.0,
		"avg_latency_ms":    210.0,
		"min_latency_ms":    200.0,
		"max_latency_ms":    220.0,
		"sum_bytes_out":     4352.0,
		"count_status_code": 2.0,
	}
	compareFloatMaps(t, win3.AggregatedValues, expected3, 2)
}
