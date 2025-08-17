package engine

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexuscore/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupEngineForTest is a helper to create a fully initialized storage engine for testing.
func setupEngineForTest(t *testing.T) *storageEngine {
	t.Helper()
	opts := StorageEngineOptions{
		DataDir:           t.TempDir(),
		MemtableThreshold: 1 * 1024 * 1024, // 1MB
		// Use a logger that discards output to keep test logs clean
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}
	// We need to cast the interface to the concrete type for testing internal state.
	eng, err := initializeStorageEngine(opts)
	require.NoError(t, err)

	err = eng.Start()
	require.NoError(t, err)

	return eng
}

func TestStorageEngine_Query_Order(t *testing.T) {
	// 1. Setup
	eng := setupEngineForTest(t)
	defer eng.Close()

	ctx := context.Background()
	metric := "test.query.order"
	tags := map[string]string{"host": "testhost"}
	baseTime := time.Now().Truncate(time.Minute)

	fields1, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": 1.0})
	require.NoError(t, err)
	fields2, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": 2.0})
	require.NoError(t, err)
	fields3, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": 3.0})
	require.NoError(t, err)

	points := []core.DataPoint{
		{Metric: metric, Tags: tags, Timestamp: baseTime.Add(1 * time.Minute).UnixNano(), Fields: fields1},
		{Metric: metric, Tags: tags, Timestamp: baseTime.Add(2 * time.Minute).UnixNano(), Fields: fields2},
		{Metric: metric, Tags: tags, Timestamp: baseTime.Add(3 * time.Minute).UnixNano(), Fields: fields3},
	}

	// 2. Put data
	err = eng.PutBatch(ctx, points)
	require.NoError(t, err)

	// --- Test Case 1: Descending Order ---
	t.Run("Descending", func(t *testing.T) {
		params := core.QueryParams{
			Metric:    metric,
			Tags:      tags,
			StartTime: baseTime.UnixNano(),
			EndTime:   baseTime.Add(5 * time.Minute).UnixNano(),
			Order:     types.Descending,
		}

		iter, err := eng.Query(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, iter)
		defer iter.Close()

		var results []*core.QueryResultItem
		for iter.Next() {
			item, err := iter.At()
			require.NoError(t, err)
			results = append(results, item)
		}
		require.NoError(t, iter.Error())
		require.Len(t, results, 3, "Expected 3 data points")

		// Assert descending order
		assert.Equal(t, points[2].Timestamp, results[0].Timestamp, "First result should be the newest point")
		assert.Equal(t, points[1].Timestamp, results[1].Timestamp, "Second result should be the middle point")
		assert.Equal(t, points[0].Timestamp, results[2].Timestamp, "Third result should be the oldest point")
	})

	// --- Test Case 2: Ascending Order ---
	t.Run("Ascending", func(t *testing.T) {
		params := core.QueryParams{
			Metric:    metric,
			Tags:      tags,
			StartTime: baseTime.UnixNano(),
			EndTime:   baseTime.Add(5 * time.Minute).UnixNano(),
			Order:     types.Ascending, // Explicitly test ascending
		}

		iter, err := eng.Query(ctx, params)
		require.NoError(t, err)
		defer iter.Close()

		var results []*core.QueryResultItem
		for iter.Next() {
			item, err := iter.At()
			require.NoError(t, err)
			results = append(results, item)
		}
		require.NoError(t, iter.Error())
		require.Len(t, results, 3, "Expected 3 data points")

		// Assert ascending order
		assert.Equal(t, points[0].Timestamp, results[0].Timestamp, "First result should be the oldest point")
		assert.Equal(t, points[1].Timestamp, results[1].Timestamp, "Second result should be the middle point")
		assert.Equal(t, points[2].Timestamp, results[2].Timestamp, "Third result should be the newest point")
	})
}
