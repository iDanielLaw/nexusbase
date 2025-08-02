package engine

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/memtable"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexusbase/sys"
	"github.com/INLOpen/nexusbase/utils"
	"github.com/INLOpen/nexusbase/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
)

func init() {
	sys.SetDebugMode(true)
}

// TestStorageEngine_PeriodicFlush_Success verifies that a non-full memtable is flushed
// after the specified time interval.
func TestStorageEngine_PeriodicFlush_Success(t *testing.T) {
	tempDir := t.TempDir()
	testMetrics := NewEngineMetrics(false, "periodic_flush_success_")
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            1024 * 1024, // 1MB, large enough to not be triggered by size
		MemtableFlushIntervalMs:      50,          // 50ms, very short for testing
		CompactionIntervalSeconds:    3600,        // Disable compaction
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		Metrics:                      testMetrics,
		SSTableCompressor:            &compressors.NoCompressionCompressor{},
		WALSyncMode:                  wal.SyncDisabled, // Disable for speed in test
	}

	engine, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err = engine.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer engine.Close()

	// Check initial state
	if flushCount := testMetrics.FlushTotal.Value(); flushCount != 0 {
		t.Fatalf("Initial flush count should be 0, got %d", flushCount)
	}

	point := HelperDataPoint(
		t,
		"metric.periodic",
		map[string]string{"test": "flush"},
		1,
		map[string]interface{}{"value": float64(1.0)},
	)

	// Put one data point, which should not trigger a size-based flush
	if err := engine.Put(context.Background(), point); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Wait for a duration longer than the flush interval
	time.Sleep(100 * time.Millisecond)

	// Verify that a flush has occurred
	if flushCount := testMetrics.FlushTotal.Value(); flushCount != 1 {
		t.Errorf("Expected flush count to be 1 after interval, got %d", flushCount)
	}

	// Verify that an SSTable file was created in L0
	sstDir := filepath.Join(tempDir, "sst")
	files, err := os.ReadDir(sstDir)
	if err != nil {
		t.Fatalf("Could not read sst directory: %v", err)
	}
	if len(files) == 0 {
		t.Error("Expected at least one SSTable file to be created by periodic flush, but found none")
	}
}

// TestStorageEngine_PeriodicFlush_NoData verifies that the periodic flush is not
// triggered if the mutable memtable is empty.
func TestStorageEngine_PeriodicFlush_NoData(t *testing.T) {
	tempDir := t.TempDir()
	testMetrics := NewEngineMetrics(false, "periodic_flush_nodata_")
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            1024 * 1024, // 1MB
		MemtableFlushIntervalMs:      50,          // 50ms
		CompactionIntervalSeconds:    3600,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		Metrics:                      testMetrics,
		SSTableCompressor:            &compressors.NoCompressionCompressor{},
		WALSyncMode:                  wal.SyncDisabled,
	}

	engine, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err = engine.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer engine.Close()

	// Wait for a duration longer than the flush interval
	time.Sleep(100 * time.Millisecond)

	// Verify that no flush has occurred because no data was written
	if flushCount := testMetrics.FlushTotal.Value(); flushCount != 0 {
		t.Errorf("Expected flush count to be 0 when no data is written, got %d", flushCount)
	}
}

// TestStorageEngine_PeriodicFlush_SizeTriggerFirst verifies that a size-based flush
// happens immediately and resets the state, not waiting for the time-based trigger.
func TestStorageEngine_PeriodicFlush_SizeTriggerFirst(t *testing.T) {
	tempDir := t.TempDir()
	testMetrics := NewEngineMetrics(false, "periodic_flush_size_")
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            1024, // 1KB, very small to trigger by size
		MemtableFlushIntervalMs:      5000, // 5s, very long
		CompactionIntervalSeconds:    3600,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		Metrics:                      testMetrics,
		SSTableCompressor:            &compressors.NoCompressionCompressor{},
		WALSyncMode:                  wal.SyncDisabled,
	}

	engine, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err = engine.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer engine.Close()

	// Put enough data to exceed the memtable threshold
	for i := 0; i < 30; i++ {
		metric := "metric.size.trigger"
		tags := map[string]string{"i": fmt.Sprintf("%d", i)}
		fields := map[string]interface{}{"value": float64(i)}
		point := HelperDataPoint(
			t,
			metric,
			tags,
			int64(i),
			fields,
		)
		if err := engine.Put(context.Background(), point); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// The size-based flush should be triggered almost immediately by the Put call.
	// We wait a very short time to allow the background flush goroutine to run.
	time.Sleep(50 * time.Millisecond)

	// Verify that a flush has occurred due to size
	if flushCount := testMetrics.FlushTotal.Value(); flushCount != 1 {
		t.Errorf("Expected flush count to be 1 after size trigger, got %d", flushCount)
	}
}

// TestStorageEngine_TriggerPeriodicFlush unit tests the triggerPeriodicFlush function directly.
func TestStorageEngine_TriggerPeriodicFlush(t *testing.T) {
	// Common setup for a "bare" engine, without starting background loops.
	opts := getBaseOptsForFlushTest(t)

	t.Run("Success_MutableHasData_ImmutableIsEmpty", func(t *testing.T) {
		// Create a minimal engine for this test, without calling NewStorageEngine
		// to avoid starting the background flush loop.
		concreteEngine := &storageEngine{
			opts:               opts,
			mu:                 sync.RWMutex{},
			immutableMemtables: make([]*memtable.Memtable, 0),
			mutableMemtable:    memtable.NewMemtable(opts.MemtableThreshold, &utils.SystemClock{}),
			flushChan:          make(chan struct{}, 1), // Buffered channel to avoid blocking
			logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
			metrics:            NewEngineMetrics(false, "trigger_flush_isempty_success_"),
			clock:              &utils.SystemClock{},
			tracer:             noop.NewTracerProvider().Tracer("test"),
		}

		// Manually initialize the WAL, which is what the now-private initializeWALAndRecover used to do.
		// This is necessary because triggerPeriodicFlush depends on the WAL being present.
		walDir := filepath.Join(opts.DataDir, "wal")
		require.NoError(t, os.MkdirAll(walDir, 0755))
		walOpts := wal.Options{
			Dir:      walDir,
			Logger:   concreteEngine.logger,
			SyncMode: wal.SyncDisabled,
		}
		testWal, _, err := wal.Open(walOpts)
		require.NoError(t, err)
		concreteEngine.wal = testWal
		defer testWal.Close()

		// Manually put data into the memtable to simulate a write.
		// We don't use Put to avoid the complexity of the full write path.
		concreteEngine.mutableMemtable.Put([]byte("key1"), makeTestEventValue(t, "val1"), core.EntryTypePutEvent, 1)
		originalMutable := concreteEngine.mutableMemtable
		if originalMutable.Size() == 0 {
			t.Fatal("Mutable memtable should have data after Put")
		}

		// Action
		concreteEngine.triggerPeriodicFlush()

		// Assertions
		if len(concreteEngine.immutableMemtables) != 1 {
			t.Errorf("Expected 1 immutable memtable, got %d", len(concreteEngine.immutableMemtables))
		}
		if concreteEngine.immutableMemtables[0] != originalMutable {
			t.Error("The original mutable memtable was not moved to the immutable list")
		}
		if concreteEngine.mutableMemtable.Size() != 0 {
			t.Errorf("Expected new mutable memtable to be empty, but size is %d", concreteEngine.mutableMemtable.Size())
		}
		if concreteEngine.mutableMemtable == originalMutable {
			t.Error("Expected mutable memtable to be a new instance, but it's the same")
		}

		// Check if flushChan was signaled. Since there's no background loop consuming it,
		// we should be able to receive the signal.
		select {
		case <-concreteEngine.flushChan:
			// Success
		default:
			t.Error("Expected flushChan to be signaled, but it was not")
		}
	})

	t.Run("Skip_MutableIsEmpty", func(t *testing.T) {
		concreteEngine := &storageEngine{
			opts:               opts,
			mu:                 sync.RWMutex{},
			immutableMemtables: make([]*memtable.Memtable, 0),
			mutableMemtable:    memtable.NewMemtable(opts.MemtableThreshold, &utils.SystemClock{}),
			flushChan:          make(chan struct{}, 1),
			logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
			metrics:            NewEngineMetrics(false, "skip_mutableempty_"),
			clock:              &utils.SystemClock{},
			tracer:             noop.NewTracerProvider().Tracer("test"),
		}
		originalMutable := concreteEngine.mutableMemtable

		// Action
		concreteEngine.triggerPeriodicFlush()

		// Assertions
		if len(concreteEngine.immutableMemtables) != 0 {
			t.Errorf("Expected 0 immutable memtables, got %d", len(concreteEngine.immutableMemtables))
		}
		if concreteEngine.mutableMemtable != originalMutable {
			t.Error("Mutable memtable should not have been replaced")
		}
	})

	t.Run("Skip_ImmutableIsNotEmpty", func(t *testing.T) {
		concreteEngine := &storageEngine{
			opts:               opts,
			mu:                 sync.RWMutex{},
			immutableMemtables: make([]*memtable.Memtable, 0),
			mutableMemtable:    memtable.NewMemtable(opts.MemtableThreshold, &utils.SystemClock{}),
			flushChan:          make(chan struct{}, 1),
			logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
			metrics:            NewEngineMetrics(false, "skip_immutable_is_not_empty_"),
			clock:              &utils.SystemClock{},
			tracer:             noop.NewTracerProvider().Tracer("test"),
		}

		// Manually create a backlogged state
		backloggedMemtable := memtable.NewMemtable(opts.MemtableThreshold, &utils.SystemClock{})
		backloggedMemtable.Put([]byte("backlog_key"), makeTestEventValue(t, "backlog_val"), core.EntryTypePutEvent, 1)
		concreteEngine.immutableMemtables = append(concreteEngine.immutableMemtables, backloggedMemtable)

		// Put more data into the new mutable memtable
		concreteEngine.mutableMemtable.Put([]byte("new_key"), makeTestEventValue(t, "new_val"), core.EntryTypePutEvent, 2)
		originalMutable := concreteEngine.mutableMemtable

		// Action
		concreteEngine.triggerPeriodicFlush()

		// Assertions
		if len(concreteEngine.immutableMemtables) != 1 {
			t.Errorf("Expected immutable memtable count to remain 1, got %d", len(concreteEngine.immutableMemtables))
		}
		if concreteEngine.mutableMemtable != originalMutable {
			t.Error("Mutable memtable should not have been replaced when backlogged")
		}
	})
}

// TestStorageEngine_MoveToDLQ tests the functionality of moving a memtable to the Dead Letter Queue.
func TestStorageEngine_MoveToDLQ(t *testing.T) {
	t.Run("Success_WithData", func(t *testing.T) {
		// Setup
		opts := getBaseOptsForFlushTest(t)
		eng := setupEngineForFlushTest(t, opts)

		mem := memtable.NewMemtable(opts.MemtableThreshold, &utils.SystemClock{})
		// Use realistic TSDB keys
		metricID1, _ := eng.stringStore.GetOrCreateID("dlq.metric.1")
		tsdbKey1 := core.EncodeTSDBKey(metricID1, nil, 100)
		mem.Put(tsdbKey1, makeTestEventValue(t, "value1"), core.EntryTypePutEvent, 10)

		metricID2, _ := eng.stringStore.GetOrCreateID("dlq.metric.2")
		tsdbKey2 := core.EncodeTSDBKey(metricID2, nil, 200)
		mem.Put(tsdbKey2, nil, core.EntryTypeDelete, 11) // Tombstone value is nil

		// Action
		err := eng.moveToDLQ(mem)
		if err != nil {
			t.Fatalf("moveToDLQ failed unexpectedly: %v", err)
		}

		// Verification
		files, err := os.ReadDir(eng.dlqDir)
		if err != nil {
			t.Fatalf("Failed to read DLQ directory: %v", err)
		}
		if len(files) != 1 {
			t.Fatalf("Expected 1 file in DLQ directory, got %d", len(files))
		}

		// Verify content of the DLQ file
		dlqFilePath := filepath.Join(eng.dlqDir, files[0].Name())
		file, err := os.Open(dlqFilePath)
		if err != nil {
			t.Fatalf("Failed to open DLQ file: %v", err)
		}
		defer file.Close()

		// Simplified verification: just check that the file is not empty.
		// A full content verification would require re-implementing the JSON decoding logic here.
		stat, err := file.Stat()
		require.NoError(t, err)
		require.Greater(t, stat.Size(), int64(0), "DLQ file should not be empty")
	})

	t.Run("Success_EmptyMemtable", func(t *testing.T) {
		// Setup
		opts := getBaseOptsForFlushTest(t)
		eng := setupEngineForFlushTest(t, opts)
		mem := memtable.NewMemtable(opts.MemtableThreshold, &utils.SystemClock{})

		// Action
		err := eng.moveToDLQ(mem)
		if err != nil {
			t.Fatalf("moveToDLQ with empty memtable failed unexpectedly: %v", err)
		}

		// Verification
		files, err := os.ReadDir(eng.dlqDir)
		if err != nil {
			t.Fatalf("Failed to read DLQ directory: %v", err)
		}
		if len(files) != 1 {
			t.Fatalf("Expected 1 file in DLQ directory, got %d", len(files))
		}

		// Verify the file is empty
		dlqFilePath := filepath.Join(eng.dlqDir, files[0].Name())
		stat, err := os.Stat(dlqFilePath)
		if err != nil {
			t.Fatalf("Failed to stat DLQ file: %v", err)
		}
		if stat.Size() != 0 {
			t.Errorf("Expected empty DLQ file for empty memtable, but size is %d", stat.Size())
		}
	})

	t.Run("Failure_DLQDirNotConfigured", func(t *testing.T) {
		// Setup
		opts := getBaseOptsForFlushTest(t)
		eng := setupEngineForFlushTest(t, opts)
		eng.dlqDir = "" // Manually un-configure the DLQ directory

		mem := memtable.NewMemtable(opts.MemtableThreshold, &utils.SystemClock{})
		mem.Put([]byte("key"), makeTestEventValue(t, "value"), core.EntryTypePutEvent, 1)

		// Action
		err := eng.moveToDLQ(mem)

		// Verification
		if err == nil {
			t.Fatal("Expected moveToDLQ to fail when dlqDir is not configured, but it succeeded")
		}
		if !strings.Contains(err.Error(), "DLQ directory not configured") {
			t.Errorf("Expected error message to contain 'DLQ directory not configured', but got: %v", err)
		}
	})
}

// setupEngineForFlushTest creates a new engine instance for testing.
// It calls NewStorageEngine and then immediately stops the background loops,
// providing a fully initialized but quiescent engine for direct method calls.
// NOTE: This helper does not register a t.Cleanup(eng.Close()) because some tests
// manually manipulate the shutdown channel, which would cause a panic on double-close.
// This may lead to resource leaks (e.g., file handles) if the test process panics.
func setupEngineForFlushTest(t *testing.T, opts StorageEngineOptions) *storageEngine {
	t.Helper()
	if opts.Metrics == nil {
		opts.Metrics = NewEngineMetrics(false, "flush_test_")
	}

	// Disable background processes for more controlled testing of specific functions.
	// We set them to 0 to prevent the ticker from even starting in NewStorageEngine.
	opts.CompactionIntervalSeconds = 0
	opts.MemtableFlushIntervalMs = 0
	opts.MetadataSyncIntervalSeconds = 0

	eng, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}

	if err = eng.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	concreteEngine, ok := eng.(*storageEngine)
	if !ok {
		t.Fatalf("Failed to cast StorageEngineInterface to *storageEngine")
	}

	// Stop all background loops that were started by NewStorageEngine.
	// This gives us a fully initialized but quiescent engine for testing.
	close(concreteEngine.shutdownChan)
	concreteEngine.wg.Wait()

	// Re-create the shutdown channel so that tests which signal shutdown can work correctly,
	// and so that the final Close() in t.Cleanup doesn't panic.
	concreteEngine.shutdownChan = make(chan struct{})

	// The engine is now ready for direct method calls without background interference.
	t.Cleanup(func() {
		eng.Close()
	})

	return concreteEngine
}

func TestStorageEngine_ProcessImmutableMemtables(t *testing.T) {
	t.Run("Success_FirstTry", func(t *testing.T) {
		// Setup
		opts := getBaseOptsForFlushTest(t)
		eng := setupEngineForFlushTest(t, opts)
		// Create a valid TSDB key using the engine's own string store.
		// This makes the test more realistic and ensures internal key parsing works.
		metricID, _ := eng.stringStore.GetOrCreateID("metric.test")
		tsdbKey := core.EncodeTSDBKey(metricID, nil, 12345) // A simple key with no tags

		mem := memtable.NewMemtable(eng.opts.MemtableThreshold, &utils.SystemClock{})
		mem.Put(tsdbKey, makeTestEventValue(t, "val1"), core.EntryTypePutEvent, 1)
		eng.immutableMemtables = append(eng.immutableMemtables, mem)

		// Action
		eng.processImmutableMemtables()

		// Assertions
		if len(eng.immutableMemtables) != 0 {
			t.Errorf("Expected immutable memtables queue to be empty, but has %d items", len(eng.immutableMemtables))
		}
		files, _ := os.ReadDir(eng.sstDir)
		if len(files) != 1 {
			t.Errorf("Expected 1 SSTable file to be created, but found %d", len(files))
		}
	})

	t.Run("Success_AfterOneRetry", func(t *testing.T) {
		// Setup
		opts := getBaseOptsForFlushTest(t)
		opts.TestingOnlyFailFlushCount = new(atomic.Int32)
		opts.TestingOnlyFailFlushCount.Store(1) // Fail once
		eng := setupEngineForFlushTest(t, opts)

		metricID, _ := eng.stringStore.GetOrCreateID("metric.retry")
		tsdbKey := core.EncodeTSDBKey(metricID, nil, 67890)

		mem := memtable.NewMemtable(eng.opts.MemtableThreshold, &utils.SystemClock{})
		mem.Put(tsdbKey, makeTestEventValue(t, "val1"), core.EntryTypePutEvent, 1)
		eng.immutableMemtables = append(eng.immutableMemtables, mem)

		// Action
		eng.processImmutableMemtables()

		// Assertions
		if len(eng.immutableMemtables) != 0 {
			t.Errorf("Expected immutable memtables queue to be empty, but has %d items", len(eng.immutableMemtables))
		}
		files, _ := os.ReadDir(eng.sstDir)
		if len(files) != 1 {
			t.Errorf("Expected 1 SSTable file to be created after retry, but found %d", len(files))
		}
		if mem.FlushRetries != 1 {
			t.Errorf("Expected memtable FlushRetries to be 1, got %d", mem.FlushRetries)
		}
	})

	t.Run("Failure_MovesToDLQ", func(t *testing.T) {
		// Setup
		opts := getBaseOptsForFlushTest(t)
		opts.TestingOnlyFailFlushCount = new(atomic.Int32)
		opts.TestingOnlyFailFlushCount.Store(maxFlushRetries) // Fail all 3 times
		eng := setupEngineForFlushTest(t, opts)

		metricID, _ := eng.stringStore.GetOrCreateID("metric.dlq")
		tsdbKey := core.EncodeTSDBKey(metricID, nil, 11111)

		mem := memtable.NewMemtable(eng.opts.MemtableThreshold, &utils.SystemClock{})
		mem.Put(tsdbKey, makeTestEventValue(t, "val1"), core.EntryTypePutEvent, 1)
		eng.immutableMemtables = append(eng.immutableMemtables, mem)

		// Action
		eng.processImmutableMemtables()

		// Assertions
		if len(eng.immutableMemtables) != 0 {
			t.Errorf("Expected immutable memtables queue to be empty, but has %d items", len(eng.immutableMemtables))
		}
		// No SSTable should be created
		sstFiles, _ := os.ReadDir(eng.sstDir)
		if len(sstFiles) != 0 {
			t.Errorf("Expected 0 SSTable files to be created, but found %d", len(sstFiles))
		}
		// A DLQ file should be created
		dlqFiles, _ := os.ReadDir(eng.dlqDir)
		if len(dlqFiles) != 1 {
			t.Errorf("Expected 1 DLQ file to be created, but found %d", len(dlqFiles))
		}
		if mem.FlushRetries != maxFlushRetries {
			t.Errorf("Expected memtable FlushRetries to be %d, got %d", maxFlushRetries, mem.FlushRetries)
		}
	})

	t.Run("Failure_RequeuedOnShutdown", func(t *testing.T) {
		// Setup
		opts := getBaseOptsForFlushTest(t)
		opts.TestingOnlyFailFlushCount = new(atomic.Int32)
		opts.TestingOnlyFailFlushCount.Store(5) // Fail more than max retries to ensure it stays in retry loop
		eng := setupEngineForFlushTest(t, opts)

		metricID, _ := eng.stringStore.GetOrCreateID("metric.shutdown")
		tsdbKey := core.EncodeTSDBKey(metricID, nil, 22222)

		mem := memtable.NewMemtable(eng.opts.MemtableThreshold, &utils.SystemClock{})
		mem.Put(tsdbKey, makeTestEventValue(t, "val1"), core.EntryTypePutEvent, 1)
		eng.immutableMemtables = append(eng.immutableMemtables, mem)

		// Action
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			eng.processImmutableMemtables()
		}()

		// Wait a moment for the first failure, then signal shutdown
		time.Sleep(10 * time.Millisecond)
		close(eng.shutdownChan)
		wg.Wait() // Wait for the goroutine to exit

		// Assertions
		if len(eng.immutableMemtables) != 1 {
			t.Errorf("Expected memtable to be re-queued on shutdown, but queue has %d items", len(eng.immutableMemtables))
		} else if eng.immutableMemtables[0] != mem {
			t.Error("The re-queued memtable is not the original one")
		}
	})
}

// Test_flushMemtableToL0SSTable_Helper tests the internal _flushMemtableToL0SSTable helper function.
func Test_flushMemtableToL0SSTable_Helper(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		opts := getBaseOptsForFlushTest(t)
		eng := setupEngineForFlushTest(t, opts) // This helper gives us a quiescent engine

		// Create and populate a memtable
		mem := memtable.NewMemtable(opts.MemtableThreshold, eng.clock)
		dp1 := HelperDataPoint(t, "metric.flush.helper", map[string]string{"id": "a"}, 100, map[string]interface{}{"value": 1.0})
		dp2 := HelperDataPoint(t, "metric.flush.helper", map[string]string{"id": "b"}, 200, map[string]interface{}{"value": 2.0})

		// Manually put data into the memtable using the engine's string store
		metricID, _ := eng.stringStore.GetOrCreateID(dp1.Metric)
		tags1 := encodeTags(eng, dp1.Tags)
		key1 := core.EncodeTSDBKey(metricID, tags1, dp1.Timestamp)
		val1, _ := dp1.Fields.Encode()
		mem.Put(key1, val1, core.EntryTypePutEvent, 1)

		tags2 := encodeTags(eng, dp2.Tags)
		key2 := core.EncodeTSDBKey(metricID, tags2, dp2.Timestamp)
		val2, _ := dp2.Fields.Encode()
		mem.Put(key2, val2, core.EntryTypePutEvent, 2)

		// Action
		newSST, err := eng._flushMemtableToL0SSTable(mem, ctx)

		// Assertions
		require.NoError(t, err)
		require.NotNil(t, newSST, "A new SSTable should have been created")
		defer newSST.Close()

		// Verify file exists
		_, statErr := os.Stat(newSST.FilePath())
		require.NoError(t, statErr, "SSTable file should exist on disk")

		// Verify content
		retrievedVal1, _, err := newSST.Get(key1)
		require.NoError(t, err, "Should find key1 in the new SSTable")
		assert.Equal(t, val1, retrievedVal1, "Value for key1 should match")

		retrievedVal2, _, err := newSST.Get(key2)
		require.NoError(t, err, "Should find key2 in the new SSTable")
		assert.Equal(t, val2, retrievedVal2, "Value for key2 should match")
	})

	t.Run("EmptyMemtable", func(t *testing.T) {
		opts := getBaseOptsForFlushTest(t)
		eng := setupEngineForFlushTest(t, opts)

		mem := memtable.NewMemtable(opts.MemtableThreshold, eng.clock)

		// Action
		newSST, err := eng._flushMemtableToL0SSTable(mem, ctx)

		// Assertions
		require.NoError(t, err)
		require.Nil(t, newSST, "No SSTable should be created for an empty memtable")

		// Verify no SSTable file was created
		files, readErr := os.ReadDir(eng.sstDir)
		require.NoError(t, readErr)
		assert.Empty(t, files, "SSTable directory should be empty")
	})
}
