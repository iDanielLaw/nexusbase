package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/sstable" // For sstable.DefaultBlockSize
	"github.com/INLOpen/nexusbase/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageEngine_WALMetrics(t *testing.T) {
	testMetrics := NewEngineMetrics(false, "wal_test_") // Create test-specific metrics with a unique prefix, no global publish

	tempDir := t.TempDir()
	opts := StorageEngineOptions{
		Metrics:                      testMetrics, // Inject metrics
		DataDir:                      tempDir,
		MemtableThreshold:            1024 * 1024, // Large enough
		CompactionIntervalSeconds:    3600,        // Disable compaction
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		WALSyncMode:                  wal.SyncAlways, // Ensure WAL is written for the test
		WALBatchSize:                 1,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		SSTableCompressor:            &compressors.NoCompressionCompressor{},
	}

	engine, err := NewStorageEngine(opts)
	if err != nil || engine == nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err = engine.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer engine.Close()

	initialBytesWritten := testMetrics.WALBytesWrittenTotal.Value()
	initialEntriesWritten := testMetrics.WALEntriesWrittenTotal.Value()

	// --- Test 1: Put operation ---
	metric1 := "wal.metric.test"
	tags1 := map[string]string{"op": "put"}
	ts1 := time.Now().UnixNano()
	dp1 := HelperDataPoint(t, metric1, tags1, ts1, map[string]interface{}{"value": 1.0})
	if err := engine.Put(context.Background(), dp1); err != nil {
		t.Fatalf("Put(key1) failed: %v", err)
	}

	expectedEntriesAfterPut1 := initialEntriesWritten + 1
	if entries := testMetrics.WALEntriesWrittenTotal.Value(); entries != expectedEntriesAfterPut1 {
		t.Errorf("After Put(key1), WAL entries = %d, want %d", entries, expectedEntriesAfterPut1)
	}
	bytesAfterPut1 := testMetrics.WALBytesWrittenTotal.Value()
	if bytesAfterPut1 <= initialBytesWritten {
		t.Errorf("After Put(key1), WAL bytes should have increased, but got %d (initial was %d)", bytesAfterPut1, initialBytesWritten)
	}

	// --- Test 2: Delete operation ---
	tags2 := map[string]string{"op": "delete"}
	ts2 := time.Now().UnixNano()
	if err := engine.Delete(context.Background(), metric1, tags2, ts2); err != nil {
		t.Fatalf("Delete(key2) failed: %v", err)
	}

	expectedEntriesAfterDelete1 := expectedEntriesAfterPut1 + 1
	if entries := testMetrics.WALEntriesWrittenTotal.Value(); entries != expectedEntriesAfterDelete1 {
		t.Errorf("After Delete(key2), WAL entries = %d, want %d", entries, expectedEntriesAfterDelete1)
	}
	bytesAfterDelete1 := testMetrics.WALBytesWrittenTotal.Value()
	if bytesAfterDelete1 <= bytesAfterPut1 {
		t.Errorf("After Delete(key2), WAL bytes should have increased, but got %d (previous was %d)", bytesAfterDelete1, bytesAfterPut1)
	}
}

func TestStorageEngine_ActiveTimeSeriesMetric(t *testing.T) {
	testMetrics := NewEngineMetrics(false, "active_series_test_") // Create test-specific metrics with a unique prefix, no global publish

	tempDir := t.TempDir()
	opts := StorageEngineOptions{
		Metrics:                      testMetrics, // Inject metrics
		DataDir:                      tempDir,
		MemtableThreshold:            1024 * 1024, // Large enough to avoid flushes during test
		CompactionIntervalSeconds:    3600,        // Disable compaction
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		SSTableCompressor:            &compressors.NoCompressionCompressor{},
	}

	// --- Test 1: Basic Puts ---
	engine1, err := NewStorageEngine(opts) // This will publish the actual activeSeries count func
	if err != nil || engine1 == nil {
		t.Fatalf("NewStorageEngine (engine1) failed: %v", err)
	}
	if err = engine1.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	concreteEngine1, ok := engine1.(*storageEngine)
	if !ok {
		t.Fatalf("Failed to assert StorageEngineInterface to *storageEngine")
	}

	getActiveSeriesCount := func(metrics *EngineMetrics) int {
		if metrics == nil {
			t.Fatalf("EngineMetrics instance is nil")
		}
		if metrics.activeSeriesCountFunc == nil {
			// This can happen if NewStorageEngine didn't populate it when metrics were injected.
			// Or if the test expects a global expvar.Get which is no longer the primary way with injection.
			t.Fatalf("activeSeriesCountFunc not initialized in metrics for the current engine instance")
		}
		val := metrics.activeSeriesCountFunc()
		count, ok := val.(int)
		if !ok {
			if count64, ok64 := val.(int64); ok64 { // Handle if it returns int64
				return int(count64)
			}
			t.Fatalf("activeSeriesCountFunc did not return int or int64, got %T", val)
		}
		return count
	}

	metricA := "metric.series.a"
	tagsA := map[string]string{"host": "server1"}
	tsA1 := int64(1000)
	valA1 := 1.0

	metricB := "metric.series.b"
	tagsB := map[string]string{"host": "server2"}
	tsB1 := int64(2000)
	valB1 := 2.0

	if err := engine1.Put(context.Background(), HelperDataPoint(t, metricA, tagsA, tsA1, map[string]interface{}{"value": valA1})); err != nil {
		t.Fatalf("Put(metricA, tagsA, tsA1) failed: %v", err)
	}
	if count := getActiveSeriesCount(concreteEngine1.metrics); count != 1 {
		t.Errorf("After Put(metricA), active series count = %d, want 1", count)
	}

	// Put another point for the same series A
	tsA2 := int64(1500)
	if err := engine1.Put(context.Background(), HelperDataPoint(t, metricA, tagsA, tsA2, map[string]interface{}{"value": valA1 + 1})); err != nil {
		t.Fatalf("Put(metricA, tagsA, tsA2) failed: %v", err)
	}
	if count := getActiveSeriesCount(concreteEngine1.metrics); count != 1 { // Still seriesA
		t.Errorf("After Put(metricA again), active series count = %d, want 1", count)
	}

	if err := engine1.Put(context.Background(), HelperDataPoint(t, metricB, tagsB, tsB1, map[string]interface{}{"value": valB1})); err != nil {
		t.Fatalf("Put(metricB) failed: %v", err)
	}
	if count := getActiveSeriesCount(concreteEngine1.metrics); count != 2 { // seriesA and seriesB
		t.Errorf("After Put(metricB), active series count = %d, want 2", count)
	}

	// To simulate a crash, we don't call engine1.Close() which would do a graceful shutdown.
	// Instead, we just close the WAL file handle to release the lock, which is the
	// main cause of errors on some OSes (like Windows) when another process
	// (the next NewStorageEngine call) tries to access the same file.
	concreteEngine1.wal.Close()

	// --- Test 2: WAL Recovery ---
	// NewStorageEngine will re-publish its metrics, including the Func for active series count.
	// Create new opts for engine2 with its own metrics instance for proper isolation using the same dataDir.
	optsEngine2 := opts                                                   // Copy base opts
	optsEngine2.Metrics = NewEngineMetrics(false, "active_series_test2_") // Give engine2 its own, fresh metrics instance, no global publish
	engine2, err := NewStorageEngine(optsEngine2)                         // Should recover from WAL using the same dataDir
	if err != nil || engine2 == nil {
		t.Fatalf("NewStorageEngine (engine2) for WAL recovery failed: %v", err)
	}
	if err = engine2.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer engine2.Close()
	concreteEngine2, ok := engine2.(*storageEngine)
	if !ok {
		t.Fatal("Failed to assert StorageEngineInterface to *storageEngine")
	}

	// After WAL recovery, the active series count should reflect what was in the WAL
	if count := getActiveSeriesCount(concreteEngine2.metrics); count != 2 {
		t.Errorf("After WAL recovery, active series count = %d, want 2 (seriesA, seriesB from WAL)", count)
	}
}

func TestStorageEngine_CacheMetrics(t *testing.T) {
	testMetrics := NewEngineMetrics(false, "cache_test_") // Create test-specific metrics with a unique prefix, no global publish

	tempDir := t.TempDir()
	opts := StorageEngineOptions{
		Metrics:                      testMetrics, // Inject metrics
		DataDir:                      tempDir,
		MemtableThreshold:            64,   // Small to force flushes
		BlockCacheCapacity:           2,    // Small cache capacity (2 blocks)
		MaxL0Files:                   1,    // Force L0 compactions if many flushes
		TargetSSTableSize:            128,  // Small SSTables
		CompactionIntervalSeconds:    3600, // Disable compaction for predictable cache metrics
		SSTableDefaultBlockSize:      32,   // Small blocks to have more distinct blocks
		BloomFilterFalsePositiveRate: 0.01,
		MaxLevels:                    3,
		SSTableCompressor:            &compressors.NoCompressionCompressor{},
	}

	// --- Phase 1: Create engine1, put data, close to flush to SSTables ---
	opts1 := opts
	opts1.Metrics = NewEngineMetrics(false, "cache_test_engine1_")
	engine1, err := NewStorageEngine(opts1)
	if err != nil || engine1 == nil {
		t.Fatalf("NewStorageEngine (engine1) failed: %v", err)
	}
	if err = engine1.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	const metricCache = "cache.test.metric"
	tagsCache1 := map[string]string{"id": "1"}
	tagsCache2 := map[string]string{"id": "2"}
	tagsCache3 := map[string]string{"id": "3"}

	tsCache1 := int64(1000)
	tsCache2 := int64(2000)
	tsCache3 := int64(3000)

	engine1.Put(context.Background(), HelperDataPoint(t, metricCache, tagsCache1, tsCache1, map[string]interface{}{"value": 1.0}))
	engine1.Put(context.Background(), HelperDataPoint(t, metricCache, tagsCache2, tsCache2, map[string]interface{}{"value": 2.0}))
	engine1.Put(context.Background(), HelperDataPoint(t, metricCache, tagsCache3, tsCache3, map[string]interface{}{"value": 3.0}))

	if err := engine1.Close(); err != nil {
		t.Fatalf("Failed to close engine1: %v", err)
	}

	// --- Phase 2: Reopen engine2. Data is now in SSTables (loaded from disk). ---
	// WAL recovery will put data into memtable, then Close will flush it.
	opts2 := opts
	opts2.Metrics = NewEngineMetrics(false, "cache_test_engine2_")
	engine2, err := NewStorageEngine(opts2)
	if err != nil || engine2 == nil {
		t.Fatalf("NewStorageEngine (engine2) failed: %v", err)
	}
	if err = engine2.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if err := engine2.Close(); err != nil { // Close to flush data recovered from WAL
		t.Fatalf("Failed to close engine2 after WAL recovery: %v", err)
	}

	// --- Phase 3: Reopen engine3. Now data is definitely in SSTables from disk. ---
	// This is the engine instance we will test cache on.
	engine3, err := NewStorageEngine(opts)
	if err != nil || engine3 == nil {
		t.Fatalf("NewStorageEngine (engine3) failed: %v", err)
	}
	if err = engine3.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer engine3.Close() // This is the engine instance we will test cache on.

	concreteEngine3, ok := engine3.(*storageEngine)
	if !ok {
		t.Fatal("Failed to assert StorageEngineInterface to *storageEngine")
	}
	getMetricInt := func(name string) int64 {
		// Read from injected metrics
		if name == "engine_cache_hits" {
			return testMetrics.CacheHits.Value()
		}
		if name == "engine_cache_misses" {
			return testMetrics.CacheMisses.Value()
		}
		t.Fatalf("Unknown int metric name: %s", name)
		return 0
	}

	getMetricFloat := func(name string) float64 {
		// Calculate manually for the test from hits/misses.
		// The hit rate func is published globally, but for isolated tests,
		// calculating from the injected metrics is more reliable.
		hits := float64(testMetrics.CacheHits.Value())
		misses := float64(testMetrics.CacheMisses.Value())
		if hits+misses == 0 {
			return 0.0
		}
		return hits / (hits + misses)
	}

	// Clear the cache and metrics after startup to have a predictable state for testing Get operations.
	// The startup process (populateActiveSeriesFromSSTables) warms the cache, which makes the test complex.
	// By clearing it, we test the Get behavior on a cold cache.
	concreteEngine3.blockCache.Clear()

	// --- Test Sequence ---

	// 1. Get point 1 (should be a miss, populate cache)
	engine3.Get(context.Background(), metricCache, tagsCache1, tsCache1)
	if hits, misses := getMetricInt("engine_cache_hits"), getMetricInt("engine_cache_misses"); hits != 0 || misses != 1 {
		t.Errorf("After Get(point1): hits=%d, misses=%d; want hits=0, misses=1", hits, misses)
	}
	if rate := getMetricFloat("cache_hit_rate"); rate != 0.0 {
		t.Errorf("After Get(point1): hit_rate=%.2f; want 0.0", rate)
	}

	// 2. Get point 1 again (should be a hit)
	engine3.Get(context.Background(), metricCache, tagsCache1, tsCache1)
	if hits, misses := getMetricInt("engine_cache_hits"), getMetricInt("engine_cache_misses"); hits != 1 || misses != 1 {
		t.Errorf("After Get(point1) again: hits=%d, misses=%d; want hits=1, misses=1", hits, misses)
	}
	if rate := getMetricFloat("cache_hit_rate"); rate != 0.5 { // 1 hit / 2 total
		t.Errorf("After Get(point1) again: hit_rate=%.2f; want 0.5", rate)
	}

	// 3. Get point 2 (should be a miss, populate cache)
	// Cache capacity is 2, so point1 and point2 blocks should now be in cache.
	engine3.Get(context.Background(), metricCache, tagsCache2, tsCache2)
	if hits, misses := getMetricInt("engine_cache_hits"), getMetricInt("engine_cache_misses"); hits != 1 || misses != 2 {
		t.Errorf("After Get(point2): hits=%d, misses=%d; want hits=1, misses=2", hits, misses)
	}

	// 4. Get point 3 (miss, populate cache, evict one of point1 or point2's block)
	engine3.Get(context.Background(), metricCache, tagsCache3, tsCache3)
	if hits, misses := getMetricInt("engine_cache_hits"), getMetricInt("engine_cache_misses"); hits != 1 || misses != 3 {
		t.Errorf("After Get(point3): hits=%d, misses=%d; want hits=1, misses=3", hits, misses)
	}

	// 5. Get point 1 again (should be a miss now, as it was likely evicted)
	engine3.Get(context.Background(), metricCache, tagsCache1, tsCache1)
	if hits, misses := getMetricInt("engine_cache_hits"), getMetricInt("engine_cache_misses"); hits != 1 || misses != 4 {
		t.Errorf("After Get(point1) third time (expect miss): hits=%d, misses=%d; want hits=1, misses=4", hits, misses)
	}
}

func TestStorageEngine_BloomFilterMetrics(t *testing.T) {
	testMetrics := NewEngineMetrics(false, "bloom_filter_test_")
	tempDir := t.TempDir()
	opts := StorageEngineOptions{
		Metrics:                      testMetrics,
		DataDir:                      tempDir,
		MemtableThreshold:            64,   // Small to force flushes
		BloomFilterFalsePositiveRate: 0.01, // Enable bloom filters
		SSTableDefaultBlockSize:      32,
		SSTableCompressor:            &compressors.NoCompressionCompressor{},
		CompactionIntervalSeconds:    3600, // Disable auto-compaction
		MaxLevels:                    3,    // Add MaxLevels to initialize level manager correctly
	}

	// --- Phase 1: Create engine, put data, close to flush to SSTables ---
	engine1, err := NewStorageEngine(opts)
	require.NoError(t, err)
	require.NoError(t, engine1.Start())

	// Put a key that will exist in the SSTable
	metricExists := "metric.exists"
	tagsExists := map[string]string{"id": "1"}
	tsExists := int64(1000)
	engine1.Put(context.Background(), HelperDataPoint(t, metricExists, tagsExists, tsExists, map[string]interface{}{"value": 1.0}))

	// Put another key to ensure the SSTable is created
	engine1.Put(context.Background(), HelperDataPoint(t, "metric.other", map[string]string{"id": "2"}, 2000, map[string]interface{}{"value": 2.0}))

	require.NoError(t, engine1.Close(), "Failed to close engine1")

	// --- Phase 2: Reopen engine. Data is now in SSTables. ---
	engine2, err := NewStorageEngine(opts)
	require.NoError(t, err)
	require.NoError(t, engine2.Start())
	defer engine2.Close()

	initialChecks := testMetrics.BloomFilterChecksTotal.Value()
	initialFalsePositives := testMetrics.BloomFilterFalsePositivesTotal.Value()

	// --- Test Sequence ---

	// 1. Get a key that exists.
	// This should check the bloom filter (check count +1) and find the key (no false positive).
	_, err = engine2.Get(context.Background(), metricExists, tagsExists, tsExists)
	require.NoError(t, err, "Get for existing key should succeed")

	assert.Equal(t, initialChecks+1, testMetrics.BloomFilterChecksTotal.Value(), "After Get(existing): BloomFilterChecks count is wrong")
	assert.Equal(t, initialFalsePositives, testMetrics.BloomFilterFalsePositivesTotal.Value(), "After Get(existing): BloomFilterFalsePositives should not change")

	// 2. Get a key that does NOT exist.
	// This should check the bloom filter (check count +1) and not find it.
	// We use the same series as an existing key but with a different timestamp.
	// This ensures the query proceeds past the dictionary checks to the SSTable level.
	// The bloom filter might return true (false positive) or false. In either case, the check counter
	// should be incremented.
	_, err = engine2.Get(context.Background(), metricExists, tagsExists, tsExists+1)
	require.ErrorIs(t, err, sstable.ErrNotFound, "Get for non-existing key should return ErrNotFound")

	assert.Equal(t, initialChecks+2, testMetrics.BloomFilterChecksTotal.Value(), "After Get(non-existing): BloomFilterChecks count is wrong")
	assert.Equal(t, initialFalsePositives, testMetrics.BloomFilterFalsePositivesTotal.Value(), "After Get(non-existing): BloomFilterFalsePositives should not change")
}

// activeQueryTestListener is a simplified listener for the ActiveQueriesMetric test.
type activeQueryTestListener struct {
	t          *testing.T
	metrics    *EngineMetrics
	signalChan chan struct{}
}

var _ hooks.HookListener = (*activeQueryTestListener)(nil)

func (l *activeQueryTestListener) Priority() int {
	return 100
}

func (l *activeQueryTestListener) IsAsync() bool {
	return false
}

func (l *activeQueryTestListener) OnEvent(ctx context.Context, event hooks.HookEvent) error {
	assert.Equal(l.t, int64(1), l.metrics.ActiveQueries.Value(), "ActiveQueries should be 1 during query execution (inside hook)")
	close(l.signalChan)
	return nil
}

func TestStorageEngine_ActiveQueriesMetric(t *testing.T) {
	testMetrics := NewEngineMetrics(false, "active_queries_test_")
	tempDir := t.TempDir()
	opts := StorageEngineOptions{
		Metrics:                      testMetrics,
		DataDir:                      tempDir,
		MemtableThreshold:            1024,
		CompactionIntervalSeconds:    3600,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		SSTableCompressor:            &compressors.NoCompressionCompressor{},
	}

	engine, err := NewStorageEngine(opts)
	require.NoError(t, err)
	require.NoError(t, engine.Start())
	defer engine.Close()

	// Initial state check
	assert.Equal(t, int64(0), testMetrics.ActiveQueries.Value(), "Initial ActiveQueries should be 0")

	hookManager := engine.GetHookManager()
	require.NotNil(t, hookManager)

	hookExecuted := make(chan struct{})
	var wg sync.WaitGroup

	// Create and register a listener for the PreQuery event.
	listener := &activeQueryTestListener{
		t:          t,
		metrics:    testMetrics,
		signalChan: hookExecuted,
	}
	hookManager.Register(hooks.EventPreQuery, listener)

	// Run the query in a separate goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		// This query will be empty, but it's enough to trigger the metric and the hook
		iter, qErr := engine.Query(context.Background(), core.QueryParams{Metric: "non.existent.metric"})
		if qErr == nil && iter != nil {
			iter.Close()
		}
	}()

	// Wait for the hook to execute or timeout
	<-hookExecuted

	wg.Wait()

	// After the query is finished, the gauge should be back to 0
	assert.Equal(t, int64(0), testMetrics.ActiveQueries.Value(), "ActiveQueries should be 0 after query has finished")
}
