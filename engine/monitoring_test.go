package engine

import (
	"context"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/levels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

// mockLevelsManager for testing monitoring.
type mockLevelsManager struct {
	levels.Manager // Embed the real manager to satisfy the interface easily
	tableCounts    map[int]int
}

// Override the method we want to mock.
func (m *mockLevelsManager) GetLevelTableCounts() (map[int]int, error) {
	return m.tableCounts, nil
}

func TestCollectAndStoreMetrics(t *testing.T) {
	// 1. Setup
	opts := getBaseOptsForFlushTest(t)
	opts.SelfMonitoringEnabled = false // Disable the automatic loop to test the function in isolation.
	opts.SelfMonitoringPrefix = "__test."

	// Create a real engine
	eng, err := NewStorageEngine(opts)
	require.NoError(t, err)

	// We need to start the engine to initialize all components like WAL, etc.
	// that PutBatch might depend on.
	err = eng.Start()
	require.NoError(t, err)
	defer eng.Close()

	// Cast to concrete type to replace the levels manager
	concreteEngine, ok := eng.(*storageEngine)
	require.True(t, ok)

	// Replace the real levels manager with our mock
	mockLM := &mockLevelsManager{
		tableCounts: map[int]int{
			0: 5,
			1: 12,
		},
	}
	// The embedded levels.Manager needs to be initialized
	lm, err := levels.NewLevelsManager(opts.MaxLevels, opts.MaxL0Files, opts.TargetSSTableSize, trace.NewNoopTracerProvider().Tracer("test"))
	require.NoError(t, err)
	mockLM.Manager = lm
	concreteEngine.levelsManager = mockLM

	// 2. Action
	// Call the function under test
	concreteEngine.collectAndStoreMetrics(context.Background())

	// 3. Verification
	// The metrics are now in the memtable. We can use the Query API to verify.

	// Verify a runtime metric
	t.Run("VerifyRuntimeMetric", func(t *testing.T) {
		iter, err := eng.Query(context.Background(), core.QueryParams{
			Metric:    "__test.runtime.goroutines.count",
			StartTime: 0,
			EndTime:   time.Now().UnixNano() * 2,
		})
		require.NoError(t, err)
		defer iter.Close()

		require.True(t, iter.Next(), "Expected to find the goroutine count metric")
		item, err := iter.At()
		require.NoError(t, err)

		val, ok := item.Fields["value"].ValueFloat64()
		require.True(t, ok)
		assert.Greater(t, val, 0.0, "Goroutine count should be greater than 0")

		require.False(t, iter.Next(), "Expected only one data point for goroutine count")
	})

	// Verify an LSM metric
	t.Run("VerifyLsmMetric", func(t *testing.T) {
		iter, err := eng.Query(context.Background(), core.QueryParams{
			Metric:    "__test.lsm.level.sstable.count",
			StartTime: 0,
			EndTime:   time.Now().UnixNano() * 2,
		})
		require.NoError(t, err)
		defer iter.Close()

		results := make(map[string]float64)
		for iter.Next() {
			item, err := iter.At()
			require.NoError(t, err)
			levelTag := item.Tags["level"]
			val, ok := item.Fields["value"].ValueFloat64()
			require.True(t, ok)
			results[levelTag] = val
		}

		assert.Equal(t, 2, len(results), "Expected metrics for 2 levels")
		assert.Equal(t, 5.0, results["0"], "SSTable count for level 0 is incorrect")
		assert.Equal(t, 12.0, results["1"], "SSTable count for level 1 is incorrect")
	})
}

func TestStartMetrics_Loop(t *testing.T) {
	opts := getBaseOptsForFlushTest(t)
	opts.SelfMonitoringEnabled = true
	opts.SelfMonitoringIntervalMs = 50 // Use a short interval for the test
	opts.SelfMonitoringPrefix = "__test_start."

	eng, err := NewStorageEngine(opts)
	require.NoError(t, err)

	// The engine's Start() method calls startMetrics(), which immediately calls collectAndStoreMetrics.
	err = eng.Start()
	require.NoError(t, err)
	defer eng.Close()

	// Use require.Eventually to wait for the metric to appear, making the test robust against timing issues.
	require.Eventually(t, func() bool {
		iter, err := eng.Query(context.Background(), core.QueryParams{
			Metric:    "__test_start.runtime.goroutines.count",
			StartTime: 0,
			EndTime:   time.Now().UnixNano() * 2,
		})
		if err != nil {
			return false
		}
		defer iter.Close()
		return iter.Next() // Return true if a point is found
	}, 2*time.Second, 10*time.Millisecond, "timed out waiting for self-monitoring metric to be written")
}

func TestStartMetrics_Disabled(t *testing.T) {
	opts := getBaseOptsForFlushTest(t)
	opts.SelfMonitoringEnabled = false // Explicitly disable
	opts.SelfMonitoringPrefix = "__test_disabled."

	eng, err := NewStorageEngine(opts)
	require.NoError(t, err)

	err = eng.Start()
	require.NoError(t, err)
	defer eng.Close()

	time.Sleep(20 * time.Millisecond)

	iter, err := eng.Query(context.Background(), core.QueryParams{
		Metric:    "__test_disabled.runtime.goroutines.count",
		StartTime: 0,
		EndTime:   time.Now().UnixNano() * 2,
	})
	require.NoError(t, err)
	defer iter.Close()

	assert.False(t, iter.Next(), "No metrics should be collected when self-monitoring is disabled")
}
