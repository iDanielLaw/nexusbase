package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/INLOpen/nexusbase/sstable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngine_SnapshotAndRestore(t *testing.T) {
	ctx := context.Background()

	// --- Phase 1: Setup a source engine and create a snapshot ---
	sourceDir := t.TempDir()
	sourceOpts := getBaseOptsForFlushTest(t)
	sourceOpts.DataDir = sourceDir

	sourceEngine, err := NewStorageEngine(sourceOpts)
	require.NoError(t, err)
	err = sourceEngine.Start()
	require.NoError(t, err)

	// Put some data into the source engine
	dp1 := HelperDataPoint(t, "metric.snap", map[string]string{"id": "a"}, 100, map[string]interface{}{"value": 10.0})
	dp2 := HelperDataPoint(t, "metric.snap", map[string]string{"id": "b"}, 200, map[string]interface{}{"value": 20.0})
	require.NoError(t, sourceEngine.Put(ctx, dp1))
	require.NoError(t, sourceEngine.Put(ctx, dp2))

	// Create the snapshot
	snapshotPath, err := sourceEngine.CreateSnapshot(ctx)
	require.NoError(t, err)
	require.DirExists(t, snapshotPath)
	require.FileExists(t, filepath.Join(snapshotPath, "MANIFEST"))

	// Verify snapshot contents (simple check)
	files, err := os.ReadDir(filepath.Join(snapshotPath, "sst"))
	require.NoError(t, err)
	assert.NotEmpty(t, files, "Snapshot sst directory should contain files")

	// Close the source engine
	require.NoError(t, sourceEngine.Close())

	// --- Phase 2: Setup a destination engine with different data ---
	destDir := t.TempDir()
	destOpts := getBaseOptsForFlushTest(t)
	destOpts.DataDir = destDir

	destEngine, err := NewStorageEngine(destOpts)
	require.NoError(t, err)
	err = destEngine.Start()
	require.NoError(t, err)

	// Put some conflicting/different data
	dp3 := HelperDataPoint(t, "metric.other", map[string]string{"id": "c"}, 300, map[string]interface{}{"value": 30.0})
	require.NoError(t, destEngine.Put(ctx, dp3))

	// --- Phase 3: Restore from the snapshot ---
	err = destEngine.RestoreFromSnapshot(ctx, snapshotPath, true) // Use overwrite=true
	require.NoError(t, err)

	// After restore, the engine is stopped and needs to be restarted to load the new state.
	// In a real server, the server process would handle this. In the test, we do it manually.
	restartedEngine, err := NewStorageEngine(destOpts)
	require.NoError(t, err)
	err = restartedEngine.Start()
	require.NoError(t, err)
	defer restartedEngine.Close()

	// --- Phase 4: Verification ---
	// 1. Original data from the snapshot should exist.
	val1, err := restartedEngine.Get(ctx, dp1.Metric, dp1.Tags, dp1.Timestamp)
	require.NoError(t, err, "Data from snapshot (dp1) should be found after restore")
	assert.Equal(t, 10.0, HelperFieldValueValidateFloat64(t, val1, "value"))

	val2, err := restartedEngine.Get(ctx, dp2.Metric, dp2.Tags, dp2.Timestamp)
	require.NoError(t, err, "Data from snapshot (dp2) should be found after restore")
	assert.Equal(t, 20.0, HelperFieldValueValidateFloat64(t, val2, "value"))

	// 2. Data that was in the destination engine before restore should be gone.
	_, err = restartedEngine.Get(ctx, dp3.Metric, dp3.Tags, dp3.Timestamp)
	assert.ErrorIs(t, err, sstable.ErrNotFound, "Original data from destination engine should be gone after restore")
}

func TestEngine_RestoreFromSnapshot_NoOverwrite(t *testing.T) {
	ctx := context.Background()

	// --- Phase 1: Create a snapshot ---
	sourceDir := t.TempDir()
	sourceOpts := getBaseOptsForFlushTest(t)
	sourceOpts.DataDir = sourceDir
	sourceEngine, err := NewStorageEngine(sourceOpts)
	require.NoError(t, err)
	err = sourceEngine.Start()
	require.NoError(t, err)
	require.NoError(t, sourceEngine.Put(ctx, HelperDataPoint(t, "metric.a", nil, 1, map[string]interface{}{"value": 1.0})))
	snapshotPath, err := sourceEngine.CreateSnapshot(ctx)
	require.NoError(t, err)
	require.NoError(t, sourceEngine.Close())

	// --- Phase 2: Attempt to restore to a non-empty DB without overwrite ---
	destDir := t.TempDir()
	destOpts := getBaseOptsForFlushTest(t)
	destOpts.DataDir = destDir
	destEngine, err := NewStorageEngine(destOpts)
	require.NoError(t, err)
	err = destEngine.Start()
	require.NoError(t, err)
	defer destEngine.Close()

	// Make the destination non-empty
	dpDest := HelperDataPoint(t, "metric.dest", nil, 2, map[string]interface{}{"value": 2.0})
	require.NoError(t, destEngine.Put(ctx, dpDest))

	// Action: Attempt restore
	err = destEngine.RestoreFromSnapshot(ctx, snapshotPath, false) // overwrite=false

	// Verification
	require.Error(t, err, "Expected restore to fail on non-empty DB without overwrite flag")
	assert.Contains(t, err.Error(), "database is not empty")

	// Verify original data in destination is untouched
	val, getErr := destEngine.Get(ctx, dpDest.Metric, dpDest.Tags, dpDest.Timestamp)
	require.NoError(t, getErr, "Original data in destination should be untouched after failed restore")
	assert.Equal(t, 2.0, HelperFieldValueValidateFloat64(t, val, "value"))
}
