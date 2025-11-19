package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/internal"
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
	opts := GetBaseOptsForTest(t, "test")
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
	opts := GetBaseOptsForTest(t, "test")

	// --- Phase 1: Create a valid engine state with a manifest ---

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

	// --- Phase 2: Create a new engine and use StateLoader to load the state ---
	engine2, loader2 := setupLoaderForTest(t, opts)

	// Action
	err2 := loader2.Load()
	require.NoError(t, err2, "Load from a valid manifest should succeed")

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
	opts := GetBaseOptsForTest(t, "test")
	dataDir := opts.DataDir

	// --- Phase 1: Create a valid engine state with SSTables but no manifest ---

	engine1, err := NewStorageEngine(opts)
	require.NoError(t, err)
	err = engine1.Start()
	require.NoError(t, err)

	require.NoError(t, engine1.Put(context.Background(), HelperDataPoint(t, "metric.fallback", map[string]string{"id": "a"}, 100, map[string]interface{}{"value": 1.0})))
	require.NoError(t, engine1.Put(context.Background(), HelperDataPoint(t, "metric.fallback", map[string]string{"id": "b"}, 200, map[string]interface{}{"value": 2.0})))
	require.NoError(t, engine1.Close())

	// Now, remove the manifest to force a fallback scan
	require.NoError(t, os.Remove(filepath.Join(dataDir, core.CurrentFileName)))
	files, _ := os.ReadDir(dataDir)
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "MANIFEST") {
			os.Remove(filepath.Join(dataDir, f.Name()))
		}
	}

	// --- Phase 2: Load using StateLoader, expecting fallback ---
	engine2, loader2 := setupLoaderForTest(t, opts)
	err2 := loader2.Load()
	require.NoError(t, err2, "Load with fallback scan should succeed")

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
	opts := GetBaseOptsForTest(t, "test")

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

// crashEngine simulates a server crash by closing the WAL file handle without
// performing a graceful engine shutdown.
// It creates an engine, runs the provided function `fn` to manipulate its state,
// and then simulates a crash without a clean shutdown.
func crashEngine(t *testing.T, opts StorageEngineOptions, fn func(e StorageEngineInterface)) {
	t.Helper()

	engine, err := NewStorageEngine(opts)
	require.NoError(t, err)
	err = engine.Start()
	require.NoError(t, err)

	// Run the user-provided function to populate/manipulate the engine
	if fn != nil {
		fn(engine)
	}

	concreteEngine, ok := engine.(*storageEngine)
	if !ok {
		t.Fatalf("Failed to cast to *storageEngine for crash simulation")
	}
	// 1. Signal all background loops to stop.
	if concreteEngine.tagIndexManager != nil {
		concreteTagMgr, ok := concreteEngine.tagIndexManager.(internal.PrivateTagIndexManager)
		if !ok {
			t.Fatal("Failed to assert TagIndexManager to PrivateTagIndexManager")
		} else {
			shutdownChan := concreteTagMgr.GetShutdownChain()
			select {
			case <-shutdownChan:
			default:
				close(shutdownChan)
			}
			concreteTagMgr.GetWaitGroup().Wait()
			if concreteTagMgr.GetLevelsManager() != nil {
				_ = concreteTagMgr.GetLevelsManager().Close()
			}
		}
	}
	select {
	case <-concreteEngine.shutdownChan:
	default:
		close(concreteEngine.shutdownChan)
	}

	// 2. Wait for all background loops to finish.
	concreteEngine.wg.Wait()

	// 3. Manually close the underlying file stores to release file locks.
	if err := concreteEngine.wal.Sync(); err != nil {
		t.Logf("Warning: WAL sync during crash simulation failed: %v", err)
	}
	if concreteEngine.wal != nil {
		_ = concreteEngine.wal.Close()
	}
	if concreteEngine.stringStore != nil {
		_ = concreteEngine.stringStore.Close()
	}
	if concreteEngine.seriesIDStore != nil {
		_ = concreteEngine.seriesIDStore.Close()
	}
	if concreteEngine.levelsManager != nil {
		_ = concreteEngine.closeSSTables() // This closes the main levels manager
	}
}

func TestStateLoader_Load_CorruptedManifest(t *testing.T) {
	opts := GetBaseOptsForTest(t, "test")
	dataDir := opts.DataDir

	// --- Phase 1: Create a valid engine state with SSTables and a manifest ---
	var manifestFileName string

	engine1, err := NewStorageEngine(opts)
	require.NoError(t, err)
	err = engine1.Start()
	require.NoError(t, err)

	require.NoError(t, engine1.Put(context.Background(), HelperDataPoint(t, "metric.corrupt.manifest", map[string]string{"id": "a"}, 100, map[string]interface{}{"value": 1.0})))
	require.NoError(t, engine1.Close())

	// Get the name of the manifest file
	currentBytes, err := os.ReadFile(filepath.Join(dataDir, core.CurrentFileName))
	require.NoError(t, err)
	manifestFileName = string(currentBytes)

	// --- Phase 2: Corrupt the manifest file ---
	manifestPath := filepath.Join(dataDir, manifestFileName)
	require.NoError(t, os.WriteFile(manifestPath, []byte("this is not valid binary data"), 0644))

	// --- Phase 3: Load using StateLoader, expecting fallback scan ---
	engine2, loader2 := setupLoaderForTest(t, opts)
	err2 := loader2.Load()
	require.NoError(t, err2, "Load with corrupted manifest should succeed by falling back")

	// Manually set engine to "started" for verification purposes
	engine2.isStarted.Store(true)

	// Verification
	assert.Equal(t, uint64(0), engine2.sequenceNumber.Load(), "Sequence number should be reset to 0 on fallback")
	assert.Equal(t, 1, len(engine2.levelsManager.GetTablesForLevel(0)), "SSTable should be loaded into L0 on fallback")

	val1, err := engine2.Get(context.Background(), "metric.corrupt.manifest", map[string]string{"id": "a"}, 100)
	require.NoError(t, err, "Data point should be found after fallback")
	assert.Equal(t, 1.0, HelperFieldValueValidateFloat64(t, val1, "value"))
}

func TestStateLoader_Load_WithCheckpoint(t *testing.T) {
	opts := GetBaseOptsForTest(t, "test")
	opts.WALMaxSegmentSize = 512 // Small segment size to force rotation

	// --- Phase 1: Create state with multiple WAL segments and a checkpoint ---
	crashEngine(t, opts, func(e StorageEngineInterface) {
		// Write data to fill the first WAL segment
		for i := 0; i < 10; i++ {
			require.NoError(t, e.Put(context.Background(), HelperDataPoint(t, "metric.checkpoint.old", map[string]string{"id": "a"}, int64(100+i), map[string]interface{}{"value": float64(i)})))
		}

		// Force a flush. This will create an SSTable and a checkpoint.
		// The checkpoint will point to the last segment that was fully flushed.
		require.NoError(t, e.ForceFlush(context.Background(), true))

		// Write more data. This will go into a new WAL segment. This is the data we expect to recover.
		require.NoError(t, e.Put(context.Background(), HelperDataPoint(t, "metric.checkpoint.new", map[string]string{"id": "b"}, 200, map[string]interface{}{"value": 20.0})))
	})

	// --- Phase 2: Load and recover ---
	engine2, loader2 := setupLoaderForTest(t, opts)
	err := loader2.Load()
	require.NoError(t, err, "Load with checkpoint should succeed")

	// Manually set engine to "started" for verification purposes
	engine2.isStarted.Store(true)

	// Verification
	// The old data should be in an SSTable
	valOld, err := engine2.Get(context.Background(), "metric.checkpoint.old", map[string]string{"id": "a"}, 100)
	require.NoError(t, err, "Old data from SSTable should be found")
	assert.Equal(t, 0.0, HelperFieldValueValidateFloat64(t, valOld, "value"))

	// The new data should have been recovered from the later WAL segment
	valNew, err := engine2.Get(context.Background(), "metric.checkpoint.new", map[string]string{"id": "b"}, 200)
	require.NoError(t, err, "New data from WAL should be found after recovery")
	assert.Equal(t, 20.0, HelperFieldValueValidateFloat64(t, valNew, "value"))
}

func TestStateLoader_Load_PopulateActiveSeriesFromLog(t *testing.T) {
	opts := GetBaseOptsForTest(t, "test")
	// dataDir := opts.DataDir

	// --- Phase 1: Create a state with some series ---

	engine1, err := NewStorageEngine(opts)
	require.NoError(t, err)
	err = engine1.Start()
	require.NoError(t, err)

	require.NoError(t, engine1.Put(context.Background(), HelperDataPoint(t, "metric.serieslog", map[string]string{"id": "a"}, 100, map[string]interface{}{"value": 1.0})))
	require.NoError(t, engine1.Put(context.Background(), HelperDataPoint(t, "metric.serieslog", map[string]string{"id": "b"}, 200, map[string]interface{}{"value": 2.0})))
	require.NoError(t, engine1.Close())

	// --- Phase 2: Load the state, which should populate activeSeries from the log ---
	engine2, loader2 := setupLoaderForTest(t, opts)
	err2 := loader2.Load()
	require.NoError(t, err2)

	// Verification
	assert.Equal(t, 2, len(engine2.activeSeries), "Should have 2 active series from log")

	// Verify the specific series keys are present
	metricID, _ := engine2.stringStore.GetID("metric.serieslog")
	tagKeyID, _ := engine2.stringStore.GetID("id")
	tagValA_ID, _ := engine2.stringStore.GetID("a")
	tagValB_ID, _ := engine2.stringStore.GetID("b")

	seriesKeyA := string(core.EncodeSeriesKey(metricID, []core.EncodedSeriesTagPair{{KeyID: tagKeyID, ValueID: tagValA_ID}}))
	seriesKeyB := string(core.EncodeSeriesKey(metricID, []core.EncodedSeriesTagPair{{KeyID: tagKeyID, ValueID: tagValB_ID}}))

	_, existsA := engine2.activeSeries[seriesKeyA]
	_, existsB := engine2.activeSeries[seriesKeyB]
	assert.True(t, existsA, "Series A should be in activeSeries map")
	assert.True(t, existsB, "Series B should be in activeSeries map")
}

func TestStateLoader_Load_ErrorReadingCheckpoint(t *testing.T) {
	opts := GetBaseOptsForTest(t, "test")
	dataDir := opts.DataDir

	// Create a corrupted checkpoint file
	checkpointPath := filepath.Join(dataDir, "CHECKPOINT")
	require.NoError(t, os.MkdirAll(dataDir, 0755))
	require.NoError(t, os.WriteFile(checkpointPath, []byte("invalid data"), 0644))

	// Attempt to load
	_, loader := setupLoaderForTest(t, opts)
	err := loader.Load()

	// Verification
	require.Error(t, err, "Load should fail with a corrupted checkpoint file")
	assert.Contains(t, err.Error(), "failed to read checkpoint file", "Error message should indicate checkpoint read failure")
}
