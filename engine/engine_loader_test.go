package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupLoaderForTest creates a new engine and its associated StateLoader for testing.
// It does not start the engine's background services.
func setupLoaderForTest(t *testing.T, opts StorageEngineOptions) (*storageEngine, *StateLoader) {
	t.Helper()
	// initializeStorageEngine creates a "bare" engine without LSM components.
	eng, err := initializeStorageEngine(opts)
	require.NoError(t, err)

	// Manually initialize components that StateLoader depends on,
	// because we are not calling eng.Start() in this test setup.
	eng.initializeMetrics()
	err = eng.initializeLSMTreeComponents()
	require.NoError(t, err)
	err = eng.initializeTagIndexManager()
	require.NoError(t, err)

	// The NewStorageEngine already creates the loader, but we can access it.
	require.NotNil(t, eng.stateLoader, "StateLoader should be initialized by NewStorageEngine")

	// The engine is initialized but not started. The loader has not been run yet.
	return eng, eng.stateLoader
}

func TestStateLoader_Load_FreshStart(t *testing.T) {
	opts := getBaseOptsForFlushTest(t)
	eng, loader := setupLoaderForTest(t, opts)

	// Action: Load state into the fresh engine.
	err := loader.Load()
	require.NoError(t, err, "Load on a fresh directory should succeed")

	// Verification
	assert.Equal(t, uint64(0), eng.sequenceNumber.Load(), "Sequence number should be 0 on fresh start")
	assert.NotNil(t, eng.wal, "WAL should be initialized")
	assert.Equal(t, 0, eng.levelsManager.GetTotalTableCount(), "There should be no SSTables")
	assert.Equal(t, int64(0), eng.mutableMemtable.Size(), "Mutable memtable should be empty")
}

func TestStateLoader_Load_FromManifest(t *testing.T) {
	opts := getBaseOptsForFlushTest(t)

	// --- Phase 1: Create a valid engine state with a manifest ---
	{
		engine1, err := NewStorageEngine(opts)
		require.NoError(t, err)
		err = engine1.Start()
		require.NoError(t, err)

		// Put some data and flush it to create an SSTable and a manifest
		dp1 := HelperDataPoint(t, "metric.manifest", map[string]string{"id": "a"}, 100, map[string]interface{}{"value": 1.0})
		require.NoError(t, engine1.Put(context.Background(), dp1))

		// Put more data that will also be flushed on close
		dp2 := HelperDataPoint(t, "metric.wal", map[string]string{"id": "b"}, 200, map[string]interface{}{"value": 2.0})
		require.NoError(t, engine1.Put(context.Background(), dp2))

		// Close cleanly to write the final manifest and flush all memtables
		require.NoError(t, engine1.Close())
	}

	// --- Phase 2: Create a new engine and use StateLoader to load the state ---
	engine2, loader2 := setupLoaderForTest(t, opts)

	// Action
	err := loader2.Load()
	require.NoError(t, err, "Load from a valid manifest should succeed")

	// Manually set engine to "started" for verification purposes
	engine2.isStarted.Store(true)

	// Verification
	// After a clean close, all data is in SSTables, and the WAL is empty.
	// The sequence number is persisted in the manifest.
	// Put(dp1) -> seq=1, Put(dp2) -> seq=2. Close flushes and persists manifest with seq=2.
	assert.Equal(t, uint64(2), engine2.sequenceNumber.Load(), "Sequence number should be restored from manifest")

	// Both data points should be in SSTables and queryable
	val1, err := engine2.Get(context.Background(), "metric.manifest", map[string]string{"id": "a"}, 100)
	require.NoError(t, err, "Data from first SSTable should be found")
	assert.Equal(t, 1.0, HelperFieldValueValidateFloat64(t, val1, "value"))

	val2, err := engine2.Get(context.Background(), "metric.wal", map[string]string{"id": "b"}, 200)
	require.NoError(t, err, "Data from second flush (on close) should be found")
	assert.Equal(t, 2.0, HelperFieldValueValidateFloat64(t, val2, "value"))

	assert.Equal(t, int64(0), engine2.mutableMemtable.Size(), "Mutable memtable should be empty after clean recovery")
}

func TestStateLoader_Load_FallbackScan(t *testing.T) {
	opts := getBaseOptsForFlushTest(t)
	dataDir := opts.DataDir

	// --- Phase 1: Create a valid engine state with SSTables but no manifest ---
	{
		engine1, err := NewStorageEngine(opts)
		require.NoError(t, err)
		err = engine1.Start()
		require.NoError(t, err)

		require.NoError(t, engine1.Put(context.Background(), HelperDataPoint(t, "metric.fallback", map[string]string{"id": "a"}, 100, map[string]interface{}{"value": 1.0})))
		require.NoError(t, engine1.Put(context.Background(), HelperDataPoint(t, "metric.fallback", map[string]string{"id": "b"}, 200, map[string]interface{}{"value": 2.0})))
		require.NoError(t, engine1.Close())

		// Now, remove the manifest to force a fallback scan
		require.NoError(t, os.Remove(filepath.Join(dataDir, CURRENT_FILE_NAME)))
		files, _ := os.ReadDir(dataDir)
		for _, f := range files {
			if strings.HasPrefix(f.Name(), "MANIFEST") {
				os.Remove(filepath.Join(dataDir, f.Name()))
			}
		}
	}

	// --- Phase 2: Load using StateLoader, expecting fallback ---
	engine2, loader2 := setupLoaderForTest(t, opts)
	err := loader2.Load()
	require.NoError(t, err, "Load with fallback scan should succeed")

	// Manually set engine to "started" for verification purposes
	engine2.isStarted.Store(true)

	// Verification
	assert.Equal(t, uint64(0), engine2.sequenceNumber.Load(), "Sequence number should be reset to 0 on fallback")
	assert.Greater(t, engine2.levelsManager.GetTotalTableCount(), 0, "SSTables should be loaded into levels manager")
	assert.Equal(t, 1, len(engine2.levelsManager.GetTablesForLevel(0)), "All tables should be loaded into L0 on fallback")

	val1, err := engine2.Get(context.Background(), "metric.fallback", map[string]string{"id": "a"}, 100)
	require.NoError(t, err, "Data point 1 should be found after fallback")
	assert.Equal(t, 1.0, HelperFieldValueValidateFloat64(t, val1, "value"))
}

func TestStateLoader_Load_WithWALRecovery(t *testing.T) {
	opts := getBaseOptsForFlushTest(t)

	// --- Phase 1: Write to WAL and then crash ---
	crashEngine(t, opts, func(e StorageEngineInterface) {
		require.NoError(t, e.Put(context.Background(), HelperDataPoint(t, "metric.wal.recovery", map[string]string{"id": "a"}, 100, map[string]interface{}{"value": 1.0})))
	})

	// --- Phase 2: Load and recover ---
	engine2, loader2 := setupLoaderForTest(t, opts)
	err := loader2.Load()
	require.NoError(t, err, "Load with WAL recovery should succeed")

	// Manually set engine to "started" for verification purposes
	engine2.isStarted.Store(true)

	// Verification
	assert.Equal(t, uint64(1), engine2.sequenceNumber.Load(), "Sequence number should be restored from WAL")
	assert.Greater(t, engine2.mutableMemtable.Size(), int64(0), "Mutable memtable should contain recovered data")

	val1, err := engine2.Get(context.Background(), "metric.wal.recovery", map[string]string{"id": "a"}, 100)
	require.NoError(t, err, "Data from WAL should be found after recovery")
	assert.Equal(t, 1.0, HelperFieldValueValidateFloat64(t, val1, "value"))
}
