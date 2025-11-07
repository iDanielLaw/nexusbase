package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStorageEngine_ForceFlush_Async tests the non-blocking (wait=false) flush.
func TestStorageEngine_ForceFlush_Async(t *testing.T) {
	opts := GetBaseOptsForTest(t, "test")
	// Disable background loops to control the flush manually
	opts.MemtableFlushIntervalMs = 0
	opts.CompactionIntervalSeconds = 0
	opts.CheckpointIntervalSeconds = 0

	eng, err := NewStorageEngine(opts)
	require.NoError(t, err)

	// Manually initialize the engine without starting background services
	// to prevent the flush loop from consuming the signal we want to test.
	concreteEngine := eng.(*storageEngine)
	concreteEngine.initializeMetrics()
	require.NoError(t, concreteEngine.initializeLSMTreeComponents())
	require.NoError(t, concreteEngine.initializeTagIndexManager())
	require.NoError(t, concreteEngine.stateLoader.Load())
	concreteEngine.isStarted.Store(true) // Manually set started flag
	defer concreteEngine.Close()         // Ensure cleanup

	// Put some data into the mutable memtable
	dp := HelperDataPoint(t, "metric.async.flush", nil, 1, map[string]interface{}{"value": 1.0})
	require.NoError(t, eng.Put(context.Background(), dp))

	require.Equal(t, 1, concreteEngine.mutableMemtable.Len(), "Mutable memtable should have 1 item before flush")
	require.Empty(t, concreteEngine.immutableMemtables, "Immutable memtables should be empty before flush")

	// Action: Call ForceFlush with wait=false
	err = eng.ForceFlush(context.Background(), false)
	require.NoError(t, err)

	// Verification:
	// 1. The function should return immediately.
	// 2. The mutable memtable should have been moved to the immutable list.
	// 3. A new, empty mutable memtable should exist.
	// 4. The flushChan should have been signaled.

	concreteEngine.mu.RLock()
	assert.Len(t, concreteEngine.immutableMemtables, 1, "Should be 1 immutable memtable after async flush signal")
	assert.Equal(t, 0, concreteEngine.mutableMemtable.Len(), "New mutable memtable should be empty")
	concreteEngine.mu.RUnlock()

	// Check if the flush channel was signaled
	select {
	case <-concreteEngine.flushChan:
		// Good, the signal was sent
	default:
		t.Error("flushChan was not signaled after async ForceFlush")
	}

	// The actual flush to disk hasn't happened yet because we disabled the background loops.
	// We can verify this by checking the number of SSTables.
	l0Tables := concreteEngine.levelsManager.GetTablesForLevel(0)
	assert.Empty(t, l0Tables, "No SSTables should be created yet for async flush")
}

// TestStorageEngine_ForceFlush_Sync tests the blocking (wait=true) flush.
func TestStorageEngine_ForceFlush_Sync(t *testing.T) {
	opts := GetBaseOptsForTest(t, "test")
	// Disable background loops to control the flush manually, but we will run the flush loop manually.
	opts.MemtableFlushIntervalMs = 0
	opts.CompactionIntervalSeconds = 0
	opts.CheckpointIntervalSeconds = 0

	eng, err := NewStorageEngine(opts)
	require.NoError(t, err)
	// We don't call eng.Start() because we want to control the background loops ourselves.
	// We need to manually initialize the components that Start() would.
	concreteEngine := eng.(*storageEngine)
	concreteEngine.initializeMetrics()
	require.NoError(t, concreteEngine.initializeLSMTreeComponents())
	require.NoError(t, concreteEngine.initializeTagIndexManager())
	require.NoError(t, concreteEngine.stateLoader.Load())
	concreteEngine.isStarted.Store(true) // Manually set started flag

	// Start the flush loop in a goroutine so our ForceFlush call can interact with it.
	go concreteEngine.serviceManager.startFlushLoop()
	defer concreteEngine.serviceManager.Stop() // Ensure it's stopped at the end

	// Put some data
	dp := HelperDataPoint(t, "metric.sync.flush", nil, 1, map[string]interface{}{"value": 1.0})
	require.NoError(t, eng.Put(context.Background(), dp))

	// Action: Call ForceFlush with wait=true. This should block until the flush is complete.
	// There's a potential race condition between starting the flushLoop goroutine
	// and it being ready to receive on the forceFlushChan. We retry a few times
	// to give the goroutine time to schedule and start listening.
	var flushErr error
	for i := 0; i < 10; i++ { // Retry for up to ~500ms
		flushErr = eng.ForceFlush(context.Background(), true)
		if flushErr == nil || !errors.Is(flushErr, ErrFlushInProgress) {
			break // Success or a different error occurred
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.NoError(t, flushErr, "ForceFlush failed even after retries")

	// Verification:
	// 1. The immutable queue should be empty.
	// 2. An SSTable should have been created in L0.
	// 3. A checkpoint should have been written.

	concreteEngine.mu.RLock()
	assert.Empty(t, concreteEngine.immutableMemtables, "Immutable memtables should be empty after sync flush")
	concreteEngine.mu.RUnlock()

	l0Tables := concreteEngine.levelsManager.GetTablesForLevel(0)
	assert.Len(t, l0Tables, 1, "One SSTable should be created in L0 after sync flush")
}

// TestStorageEngine_ForceFlush_Sync_Idle tests that a synchronous flush on an idle engine
// (no data in memtable) completes successfully and creates a checkpoint.
func TestStorageEngine_ForceFlush_Sync_Idle(t *testing.T) {
	opts := GetBaseOptsForTest(t, "test")
	opts.MemtableFlushIntervalMs = 0
	opts.CompactionIntervalSeconds = 0
	opts.CheckpointIntervalSeconds = 0
	opts.SelfMonitoringEnabled = false // Disable self-monitoring to prevent concurrent writes

	eng, err := NewStorageEngine(opts)
	require.NoError(t, err)
	// Do not call eng.Start() to avoid race conditions with background loops.
	// Manually initialize and start only the necessary components for this test.
	concreteEngine := eng.(*storageEngine)
	concreteEngine.initializeMetrics()
	require.NoError(t, concreteEngine.initializeLSMTreeComponents())
	require.NoError(t, concreteEngine.initializeTagIndexManager())
	require.NoError(t, concreteEngine.stateLoader.Load())
	concreteEngine.isStarted.Store(true) // Manually set started flag

	// Start only the flush loop in a goroutine.
	go concreteEngine.serviceManager.startFlushLoop()
	defer concreteEngine.serviceManager.Stop() // This will stop the flush loop.

	// Ensure memtable is empty
	require.Equal(t, int64(0), concreteEngine.mutableMemtable.Size())

	// Action: Call ForceFlush with wait=true on an idle engine.
	// There's a potential race condition between starting the flushLoop goroutine
	// and it being ready to receive on the forceFlushChan. We retry a few times
	// to give the goroutine time to schedule and start listening.
	var flushErr error
	for i := 0; i < 10; i++ { // Retry for up to ~500ms
		flushErr = eng.ForceFlush(context.Background(), true)
		if flushErr == nil || !errors.Is(flushErr, ErrFlushInProgress) {
			break // Success or a different error occurred
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.NoError(t, flushErr, "ForceFlush failed even after retries")

	// Verification:
	// 1. No new SSTables should be created.
	// 2. A checkpoint file should exist (or have been updated).
	l0Tables := concreteEngine.levelsManager.GetTablesForLevel(0)
	assert.Empty(t, l0Tables, "No SSTables should be created for idle flush")

	// Because the WAL is empty (no writes), the lastFlushedSegment will be 0,
	// so the checkpoint logic inside ForceFlush will not actually write a new file.
	// This is correct behavior. If we had written to the WAL, a checkpoint would be created.
	dp := HelperDataPoint(t, "metric.idle.flush", nil, 1, map[string]interface{}{"value": 1.0})
	require.NoError(t, eng.Put(context.Background(), dp))

	// Now call ForceFlush again. This time it will flush the point and create a checkpoint.
	// The loop is already running, so a race is less likely, but we retry for robustness.
	for i := 0; i < 10; i++ { // Retry for up to ~500ms
		flushErr = eng.ForceFlush(context.Background(), true)
		if flushErr == nil || !errors.Is(flushErr, ErrFlushInProgress) {
			break // Success or a different error occurred
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.NoError(t, flushErr, "ForceFlush after put failed even after retries")

	l0TablesAfterPut := concreteEngine.levelsManager.GetTablesForLevel(0)
	assert.Len(t, l0TablesAfterPut, 1, "One SSTable should be created after putting data and flushing")

	// Check for checkpoint file
	checkpointPath := filepath.Join(opts.DataDir, "CHECKPOINT")
	_, err = os.Stat(checkpointPath)
	assert.NoError(t, err, "CHECKPOINT file should exist after a synchronous flush with data")
}