package engine

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexusbase/sys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	sys.SetDebugMode(false)
}

func TestWALRecovery_Successful(t *testing.T) {
	tempDir := t.TempDir()
	opts := GetBaseOptsForTest(t, "test")
	opts.DataDir = tempDir

	// Data points to be written
	dp1 := HelperDataPoint(t, "metric.a", map[string]string{"host": "server1"}, 100, map[string]interface{}{"value": 10.0})
	dp2 := HelperDataPoint(t, "metric.b", map[string]string{"host": "server2"}, 200, map[string]interface{}{"value": 20.0})
	dp3_event := HelperDataPoint(t, "app.log", map[string]string{"level": "error"}, 300, map[string]interface{}{"status": int64(500), "msg": "failed to connect"})
	dp4_todelete := HelperDataPoint(t, "metric.c", map[string]string{"host": "server3"}, 400, map[string]interface{}{"value": 40.0})
	series_todelete := HelperDataPoint(t, "metric.d", map[string]string{"host": "server4"}, 500, map[string]interface{}{"value": 50.0})

	// --- Phase 1: Create an engine, write data, and simulate a crash ---
	crashEngine(t, opts, func(e StorageEngineInterface) {
		// Write data
		require.NoError(t, e.Put(context.Background(), dp1))
		require.NoError(t, e.Put(context.Background(), dp2))
		require.NoError(t, e.Put(context.Background(), dp3_event))
		require.NoError(t, e.Put(context.Background(), dp4_todelete))
		require.NoError(t, e.Put(context.Background(), series_todelete))

		// Perform deletions
		require.NoError(t, e.Delete(context.Background(), dp4_todelete.Metric, dp4_todelete.Tags, dp4_todelete.Timestamp))
		require.NoError(t, e.DeleteSeries(context.Background(), series_todelete.Metric, series_todelete.Tags))
	})

	// --- Phase 2: Create a new engine instance in the same directory to trigger recovery ---
	engine2, err := NewStorageEngine(opts)
	require.NoError(t, err)
	err = engine2.Start()
	require.NoError(t, err)
	defer engine2.Close()

	concreteEngine2 := engine2.(*storageEngine)

	// --- Phase 3: Verification ---

	// 1. Verify sequence number
	// 5 Puts + 1 Delete + 1 DeleteSeries = 7 operations
	assert.Equal(t, uint64(7), concreteEngine2.sequenceNumber.Load(), "Sequence number should be restored to the max from WAL")

	// 2. Verify data in memtable
	// Point 1 should exist
	val1, err := engine2.Get(context.Background(), dp1.Metric, dp1.Tags, dp1.Timestamp)
	require.NoError(t, err, "Point 1 should be found after recovery")
	assert.Equal(t, 10.0, HelperFieldValueValidateFloat64(t, val1, "value"), "Value of point 1 is incorrect")

	// Point 2 should exist
	val2, err := engine2.Get(context.Background(), dp2.Metric, dp2.Tags, dp2.Timestamp)
	require.NoError(t, err, "Point 2 should be found after recovery")
	assert.Equal(t, 20.0, HelperFieldValueValidateFloat64(t, val2, "value"), "Value of point 2 is incorrect")

	// Point 3 (event) should exist
	val3, err := engine2.Get(context.Background(), dp3_event.Metric, dp3_event.Tags, dp3_event.Timestamp)
	require.NoError(t, err, "Event 3 should be found after recovery")
	assert.Equal(t, int64(500), HelperFieldValueValidateInt64(t, val3, "status"), "Field 'status' of event 3 is incorrect")
	assert.Equal(t, "failed to connect", HelperFieldValueValidateString(t, val3, "msg"), "Field 'msg' of event 3 is incorrect")

	// 3. Verify deletions
	// Point 4 should be deleted
	_, err = engine2.Get(context.Background(), dp4_todelete.Metric, dp4_todelete.Tags, dp4_todelete.Timestamp)
	assert.ErrorIs(t, err, sstable.ErrNotFound, "Point 4 should be marked as deleted after recovery")

	// Series D should be deleted
	// We must construct the key using the same ID-based encoding the engine uses internally.
	metricID_D, ok := concreteEngine2.stringStore.GetID(series_todelete.Metric)
	require.True(t, ok, "metric for deleted series should exist in string store")
	var encodedTags_D []core.EncodedSeriesTagPair
	for k, v := range series_todelete.Tags {
		keyID, ok1 := concreteEngine2.stringStore.GetID(k)
		valID, ok2 := concreteEngine2.stringStore.GetID(v)
		require.True(t, ok1 && ok2, "tags for deleted series should exist in string store")
		encodedTags_D = append(encodedTags_D, core.EncodedSeriesTagPair{KeyID: keyID, ValueID: valID})
	}
	sort.Slice(encodedTags_D, func(i, j int) bool { return encodedTags_D[i].KeyID < encodedTags_D[j].KeyID })
	seriesKeyD_bytes := core.EncodeSeriesKey(metricID_D, encodedTags_D)
	seriesKeyD := string(seriesKeyD_bytes)

	concreteEngine2.deletedSeriesMu.RLock()
	_, isDeleted := concreteEngine2.deletedSeries[seriesKeyD]
	concreteEngine2.deletedSeriesMu.RUnlock()
	assert.True(t, isDeleted, "Series D should be in the deletedSeries map after recovery")

	// 4. Verify active series count
	// metric.a, metric.b, app.log, metric.c, metric.d = 5 series were touched.
	// Even though c and d were deleted, they are still "active" in the sense that they exist in the DB state.
	assert.Equal(t, 5, len(concreteEngine2.activeSeries), "Active series count should be restored")

	// 5. Verify no SSTables were created (recovery only populates memtable)
	sstDir := filepath.Join(tempDir, "sst")
	files, _ := os.ReadDir(sstDir)
	assert.Empty(t, files, "No SSTables should be created during WAL recovery")
}
