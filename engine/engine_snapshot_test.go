package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexusbase/sys"
	"github.com/INLOpen/nexusbase/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	sys.SetDebugMode(true)
}

// setupTestEngine creates a new engine instance for testing.
func setupTestEngine(t *testing.T, dir string) (StorageEngineInterface, StorageEngineOptions) {
	t.Helper()
	opts := StorageEngineOptions{
		DataDir:                      dir,
		MemtableThreshold:            1024, // Small threshold to encourage flushing
		BlockCacheCapacity:           10,
		MaxL0Files:                   2,
		TargetSSTableSize:            2048,
		LevelsTargetSizeMultiplier:   2,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		CompactionIntervalSeconds:    3600, // Disable auto-compaction for predictable tests
		Metrics:                      NewEngineMetrics(false, "snapshot_test_"),
		WALSyncMode:                  wal.SyncAlways,
	}
	eng, err := NewStorageEngine(opts)
	require.NoError(t, err, "NewStorageEngine should not fail")
	err = eng.Start()
	require.NoError(t, err, "Start should not fail")
	return eng, opts
}

// TestSnapshot_Lifecycle covers the full end-to-end process of creating a snapshot
// from a live database and restoring it into a new one, verifying data integrity.
func TestSnapshot_Lifecycle(t *testing.T) {
	// --- Setup ---
	baseDir := t.TempDir()
	originalDataDir := filepath.Join(baseDir, "original_data")
	snapshotDir := filepath.Join(baseDir, "snapshot")
	restoredDataDir := filepath.Join(baseDir, "restored_data")

	engine1, _ := setupTestEngine(t, originalDataDir)

	// --- Populate Data ---
	ctx := context.Background()
	liveMetric := "live.metric"
	liveTags := map[string]string{"status": "active"}
	liveTs := time.Now().UnixNano()
	require.NoError(t, engine1.Put(ctx, HelperDataPoint(t, liveMetric, liveTags, liveTs, map[string]interface{}{"value": 123.45})), "Put failed")

	pointDelMetric := "point.delete.metric"
	pointDelTags := map[string]string{"status": "deleted"}
	pointDelTs := time.Now().UnixNano()
	require.NoError(t, engine1.Put(ctx, HelperDataPoint(t, pointDelMetric, pointDelTags, pointDelTs, map[string]interface{}{"value": 99.0})), "Put failed")
	require.NoError(t, engine1.Delete(ctx, pointDelMetric, pointDelTags, pointDelTs), "Delete failed")

	seriesDelMetric := "series.delete.metric"
	seriesDelTags := map[string]string{"status": "gone"}
	require.NoError(t, engine1.Put(ctx, HelperDataPoint(t, seriesDelMetric, seriesDelTags, time.Now().UnixNano(), map[string]interface{}{"value": 1.0})), "Put failed")
	require.NoError(t, engine1.DeleteSeries(ctx, seriesDelMetric, seriesDelTags), "DeleteSeries failed")

	rangeDelMetric := "range.delete.metric"
	rangeDelTags := map[string]string{"status": "partial"}
	rangeDelTs1 := time.Now().UnixNano()
	rangeDelTs2 := rangeDelTs1 + 1000 // This one will be deleted
	rangeDelTs3 := rangeDelTs1 + 3000
	require.NoError(t, engine1.Put(ctx, HelperDataPoint(t, rangeDelMetric, rangeDelTags, rangeDelTs1, map[string]interface{}{"value": 1.0})), "Put failed")
	require.NoError(t, engine1.Put(ctx, HelperDataPoint(t, rangeDelMetric, rangeDelTags, rangeDelTs2, map[string]interface{}{"value": 2.0})), "Put failed")
	require.NoError(t, engine1.Put(ctx, HelperDataPoint(t, rangeDelMetric, rangeDelTags, rangeDelTs3, map[string]interface{}{"value": 3.0})), "Put failed")
	require.NoError(t, engine1.DeletesByTimeRange(ctx, rangeDelMetric, rangeDelTags, rangeDelTs2-500, rangeDelTs2+500), "DeletesByTimeRange failed")

	// --- Create Snapshot ---
	require.NoError(t, engine1.CreateSnapshot(snapshotDir), "CreateSnapshot should succeed")
	require.NoError(t, engine1.Close(), "Closing original engine should succeed")

	// --- Verify Snapshot Contents ---
	t.Run("VerifySnapshotDirectory", func(t *testing.T) {
		// Check for CURRENT file
		currentFilePath := filepath.Join(snapshotDir, CURRENT_FILE_NAME)
		manifestFileNameBytes, err := os.ReadFile(currentFilePath)
		require.NoError(t, err, "CURRENT file should exist in snapshot")
		manifestFileName := string(manifestFileNameBytes)
		require.NotEmpty(t, manifestFileName, "CURRENT file should not be empty")

		// Check for MANIFEST file
		manifestPath := filepath.Join(snapshotDir, manifestFileName)
		manifestFile, err := os.Open(manifestPath)
		require.NoError(t, err, "MANIFEST file should exist in snapshot")
		defer manifestFile.Close()

		manifest, err := readManifestBinary(manifestFile)
		require.NoError(t, err, "MANIFEST should be a valid binary file")

		// Check for auxiliary files listed in manifest
		require.NotEmpty(t, manifest.WALFile, "Manifest should list a WAL file")
		//assert.FileExists(t, filepath.Join(snapshotDir, manifest.WALFile))
		walInSnapshot := filepath.Join(snapshotDir, manifest.WALFile)
		info, err := os.Stat(walInSnapshot)
		require.NoError(t, err)
		require.True(t, info.IsDir())

		require.NotEmpty(t, manifest.StringMappingFile, "Manifest should list a string mapping file")
		assert.FileExists(t, filepath.Join(snapshotDir, manifest.StringMappingFile))

		require.NotEmpty(t, manifest.SeriesMappingFile, "Manifest should list a series mapping file")
		assert.FileExists(t, filepath.Join(snapshotDir, manifest.SeriesMappingFile))

		require.NotEmpty(t, manifest.DeletedSeriesFile, "Manifest should list a deleted series file")
		assert.FileExists(t, filepath.Join(snapshotDir, manifest.DeletedSeriesFile))

		require.NotEmpty(t, manifest.RangeTombstonesFile, "Manifest should list a range tombstones file")
		assert.FileExists(t, filepath.Join(snapshotDir, manifest.RangeTombstonesFile))

		// Check for SSTables
		require.NotEmpty(t, manifest.Levels, "Manifest should contain level information")
		for _, level := range manifest.Levels {
			for _, tableMeta := range level.Tables {
				assert.FileExists(t, filepath.Join(snapshotDir, tableMeta.FileName))
			}
		}
	})

	// --- Restore from Snapshot ---
	restoreOpts := StorageEngineOptions{DataDir: restoredDataDir} // Minimal opts needed for restore
	require.NoError(t, RestoreFromSnapshot(restoreOpts, snapshotDir), "RestoreFromSnapshot should succeed")

	// --- Verify Restored Data ---
	t.Run("VerifyRestoredDatabase", func(t *testing.T) {
		engine2, _ := setupTestEngine(t, restoredDataDir)
		defer engine2.Close() // Ensure the restored engine is closed

		// Verify live data
		val, err := engine2.Get(ctx, liveMetric, liveTags, liveTs)
		require.NoError(t, err, "Live data point should exist in restored DB")
		retrievedVal := HelperFieldValueValidateFloat64(t, val, "value")
		assert.InDelta(t, 123.45, retrievedVal, 1e-9, "Live data point value should match")

		// Verify point delete
		_, err = engine2.Get(ctx, pointDelMetric, pointDelTags, pointDelTs)
		assert.ErrorIs(t, err, sstable.ErrNotFound, "Point-deleted data should not be found")

		// Verify series delete
		_, err = engine2.Get(ctx, seriesDelMetric, seriesDelTags, time.Now().UnixNano())
		assert.ErrorIs(t, err, sstable.ErrNotFound, "Series-deleted data should not be found")

		// Verify range delete
		_, err = engine2.Get(ctx, rangeDelMetric, rangeDelTags, rangeDelTs1)
		assert.NoError(t, err, "Point before range delete should exist")
		_, err = engine2.Get(ctx, rangeDelMetric, rangeDelTags, rangeDelTs2)
		assert.ErrorIs(t, err, sstable.ErrNotFound, "Point inside range delete should not be found")
		_, err = engine2.Get(ctx, rangeDelMetric, rangeDelTags, rangeDelTs3)
		assert.NoError(t, err, "Point after range delete should exist")
	})
}

// TestSnapshot_Overwrite verifies that creating a snapshot into an existing directory
// correctly replaces the old snapshot with the new one.
func TestSnapshot_Overwrite(t *testing.T) {
	// --- Setup ---
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")
	snapshotDir := filepath.Join(baseDir, "snapshot") // Same dir for both snapshots
	restoredDataDir := filepath.Join(baseDir, "restored")

	engine, opts := setupTestEngine(t, dataDir)
	ctx := context.Background()

	// --- Snapshot v1 ---
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, "metric.v1", nil, 100, map[string]interface{}{"value": 1.0})), "Put v1 failed")
	require.NoError(t, engine.CreateSnapshot(snapshotDir), "CreateSnapshot v1 should succeed")

	// --- Snapshot v2 (Overwrite) ---
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, "metric.v2", nil, 200, map[string]interface{}{"value": 2.0})), "Put v2 failed") // Add new data
	require.NoError(t, engine.CreateSnapshot(snapshotDir), "CreateSnapshot v2 (overwrite) should succeed")                                // Overwrite
	require.NoError(t, engine.Close(), "Engine close failed")

	// --- Restore and Verify ---
	restoreOpts := StorageEngineOptions{DataDir: restoredDataDir}
	require.NoError(t, RestoreFromSnapshot(restoreOpts, snapshotDir), "RestoreFromSnapshot should succeed")

	restoredOpts := opts
	restoredOpts.DataDir = restoredDataDir
	restoredOpts.Metrics = NewEngineMetrics(false, "overwrite_restored_")
	restoredEngine, err := NewStorageEngine(restoredOpts)
	require.NoError(t, err, "Should open restored engine")
	err = restoredEngine.Start()
	require.NoError(t, err, "Should start restored engine")
	defer restoredEngine.Close()

	// Check for v2 data (must exist)
	val2, err := restoredEngine.Get(ctx, "metric.v2", nil, 200)
	require.NoError(t, err, "Data from v2 snapshot should exist")
	retrievedVal2 := HelperFieldValueValidateFloat64(t, val2, "value")
	assert.InDelta(t, 2.0, retrievedVal2, 1e-9, "Value mismatch for v2 data")

	// Check for v1 data (it SHOULD exist, as the v2 snapshot captures the full state of the engine)
	val1, err := restoredEngine.Get(ctx, "metric.v1", nil, 100)
	require.NoError(t, err, "Data from v1 snapshot should exist in the v2 snapshot")
	retrievedVal1 := HelperFieldValueValidateFloat64(t, val1, "value")
	assert.InDelta(t, 1.0, retrievedVal1, 1e-9, "Value mismatch for v1 data")
}

// TestRestoreFromSnapshot_ErrorCases tests various failure scenarios for the restore process.
func TestRestoreFromSnapshot_ErrorCases(t *testing.T) {
	baseDir := t.TempDir()
	snapshotDir := filepath.Join(baseDir, "snapshot")
	targetDir := filepath.Join(baseDir, "target")
	require.NoError(t, os.MkdirAll(snapshotDir, 0755))
	require.NoError(t, os.MkdirAll(targetDir, 0755))

	restoreOpts := StorageEngineOptions{DataDir: targetDir}

	t.Run("SnapshotDir_NotExists", func(t *testing.T) {
		err := RestoreFromSnapshot(restoreOpts, filepath.Join(baseDir, "non_existent_snapshot"))
		require.Error(t, err, "Should fail if snapshot dir does not exist")
		assert.Contains(t, err.Error(), "does not exist")
	})

	t.Run("TargetDir_NotEmpty_SucceedsByCleaning", func(t *testing.T) {
		// This subtest verifies that RestoreFromSnapshot correctly cleans a non-empty
		// target directory before restoring, which is the intended behavior of the
		// engine-level function. The CLI wrapper adds a pre-check for safety.

		// Create a valid snapshot to restore from.
		snapshotDirForThisTest := t.TempDir()
		engineForSnap, _ := setupTestEngine(t, t.TempDir())
		require.NoError(t, engineForSnap.Put(context.Background(), HelperDataPoint(t, "metric", nil, 100, map[string]interface{}{"value": 1})))
		require.NoError(t, engineForSnap.CreateSnapshot(snapshotDirForThisTest), "Failed to create snapshot for TargetDir_NotEmpty test")
		engineForSnap.Close()

		// Create a non-empty target directory.
		nonEmptyTargetDir := t.TempDir()
		dummyFile := filepath.Join(nonEmptyTargetDir, "dummy.txt")
		require.NoError(t, os.WriteFile(dummyFile, []byte("this should be deleted"), 0644))

		// Run the restore. It should succeed.
		restoreOpts := StorageEngineOptions{DataDir: nonEmptyTargetDir}
		err := RestoreFromSnapshot(restoreOpts, snapshotDirForThisTest)
		require.NoError(t, err, "RestoreFromSnapshot should succeed even if target dir is not empty")

		// Verify the dummy file is gone.
		_, err = os.Stat(dummyFile)
		assert.True(t, os.IsNotExist(err), "Dummy file should have been removed by RestoreFromSnapshot")

		// Verify the restored data is present by trying to open an engine on it.
		// Use setupTestEngine to get a properly configured engine.
		verifyEngine, _ := setupTestEngine(t, nonEmptyTargetDir)
		// We must close this engine to release file handles before the test's temp dir is cleaned up.
		defer verifyEngine.Close()
	})

	t.Run("Snapshot_Missing_CURRENT", func(t *testing.T) {
		// snapshotDir is empty, so CURRENT is missing
		err := RestoreFromSnapshot(restoreOpts, snapshotDir)
		require.Error(t, err, "Should fail if CURRENT file is missing")
		assert.Contains(t, err.Error(), "failed to read CURRENT file")
	})

	t.Run("Snapshot_Missing_MANIFEST", func(t *testing.T) {
		// Create CURRENT but no MANIFEST
		currentFile := filepath.Join(snapshotDir, "CURRENT")
		require.NoError(t, os.WriteFile(currentFile, []byte("MANIFEST-123.bin"), 0644))
		defer os.Remove(currentFile)

		err := RestoreFromSnapshot(restoreOpts, snapshotDir)
		require.Error(t, err, "Should fail if MANIFEST file is missing")
		assert.Contains(t, err.Error(), "not found in")
	})

	t.Run("Snapshot_Corrupted_MANIFEST", func(t *testing.T) {
		// Create CURRENT and a corrupted MANIFEST
		currentFile := filepath.Join(snapshotDir, CURRENT_FILE_NAME)
		manifestPath := filepath.Join(snapshotDir, "MANIFEST-corrupt.bin")
		require.NoError(t, os.WriteFile(currentFile, []byte("MANIFEST-corrupt.bin"), 0644))
		// Write a file with an invalid magic number
		require.NoError(t, os.WriteFile(manifestPath, []byte("this is not a valid binary manifest"), 0644))
		defer os.Remove(currentFile)
		defer os.Remove(manifestPath)

		err := RestoreFromSnapshot(restoreOpts, snapshotDir) // restoreOpts is defined at the top of the test function
		require.Error(t, err, "Should fail if MANIFEST is corrupted")
		assert.Contains(t, err.Error(), "invalid binary manifest magic number")
	})
}
