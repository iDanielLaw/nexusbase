package engine

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexusbase/utils"
	"github.com/INLOpen/nexusbase/wal"
	"github.com/stretchr/testify/require"
)

// initialPointKey is a helper struct to store the key components of pre-populated data points.
type initialPointKey struct {
	metric    string
	tags      map[string]string
	timestamp int64
}

func BenchmarkStorageEngine_Put(b *testing.B) {
	tempDir := b.TempDir()
	opts := StorageEngineOptions{ // Use StorageEngineOptions
		DataDir:                      tempDir,
		MemtableThreshold:            64 * 1024 * 1024, // 64MB Memtable
		IndexMemtableThreshold:       64 * 1024 * 1024,
		BlockCacheCapacity:           1000, // Example capacity
		MaxL0Files:                   4,
		TargetSSTableSize:            128 * 1024 * 1024,
		LevelsTargetSizeMultiplier:   10,
		MaxLevels:                    7,
		BloomFilterFalsePositiveRate: 0.01,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize, // Use default from sstable package
		// Metrics:                      NewEngineMetrics(false, "bench_put_"), // Not strictly needed for benchmark
		CompactionIntervalSeconds: 5, // Allow some background activity
		SSTableCompressor:         &compressors.LZ4Compressor{},
		Logger:                    slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{})),
	}
	opts.WALSyncMode = wal.SyncAlways
	opts.WALBatchSize = 1
	opts.WALFlushIntervalMs = 0
	eng, err := NewStorageEngine(opts) // Use NewStorageEngine
	if err != nil {
		b.Fatalf("NewStorageEngine failed: %v", err)
	}

	if err = eng.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	defer eng.Close()

	b.ResetTimer() // Start timing after setup
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		metric := "bench.put"
		tags := map[string]string{"host": "host1", "iter": fmt.Sprintf("%09d", i)}
		ts := time.Now().UnixNano() + int64(i)
		val := float64(i)
		dp, err := core.NewSimpleDataPoint(metric, tags, ts, map[string]interface{}{"value": val})
		if err != nil {
			b.Fatalf("NewSimpleDataPoint failed: %v", err)
		}
		if err := eng.Put(context.Background(), *dp); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}
	b.StopTimer() // Stop timing before cleanup
}

func BenchmarkStorageEngine_PutLargeDataPoint(b *testing.B) {
	tempDir := b.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            64 * 1024 * 1024, // 64MB Memtable
		IndexMemtableThreshold:       64 * 1024 * 1024,
		BlockCacheCapacity:           1000, // Example capacity
		MaxL0Files:                   4,
		TargetSSTableSize:            128 * 1024 * 1024,
		LevelsTargetSizeMultiplier:   10,
		MaxLevels:                    7,
		BloomFilterFalsePositiveRate: 0.01,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		CompactionIntervalSeconds:    5, // Allow some background activity
		SSTableCompressor:            &compressors.LZ4Compressor{},
		WALSyncMode:                  wal.SyncAlways, // Ensure WAL sync is enabled for realistic Put performance
		WALBatchSize:                 1,
		WALFlushIntervalMs:           0,
		Logger:                       slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{})),
	}

	eng, err := NewStorageEngine(opts)
	if err != nil {
		b.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err = eng.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	defer eng.Close()

	// To simulate "large data point" for TSDB, we can make the tags very long,
	// or if the underlying Put method supported arbitrary byte values, we'd pass a large byte slice.
	// For float64, the value itself is always 8 bytes.
	// Let's make the metric and tags longer to simulate larger key sizes.
	longValue := 123.456789           // A regular float64 value
	longTagValue := make([]byte, 512) // 512 bytes for a tag value
	for i := range longTagValue {
		longTagValue[i] = byte('a' + (i % 26))
	}

	b.ResetTimer() // Start timing after setup
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		metric := "large_data_metric_" + fmt.Sprintf("%05d", i) // Unique metric name to create many series
		tags := map[string]string{
			"host": fmt.Sprintf("host_%d", i%100), // Some host variation
			"data": string(longTagValue),          // A long tag value
		}
		ts := time.Now().UnixNano() + int64(i) // Unique timestamp
		dp, err := core.NewSimpleDataPoint(metric, tags, ts, map[string]interface{}{"value": longValue})
		if err != nil {
			b.Fatalf("NewSimpleDataPoint failed: %v", err)
		}
		if err := eng.Put(context.Background(), *dp); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}
	b.StopTimer() // Stop timing before cleanup
}

func BenchmarkStorageEngine_Get_Sequential(b *testing.B) {
	tempDir := b.TempDir()
	opts := StorageEngineOptions{ // Use StorageEngineOptions
		DataDir:                      tempDir,
		MemtableThreshold:            64 * 1024 * 1024,
		IndexMemtableThreshold:       64 * 1024 * 1024,
		BlockCacheCapacity:           1000,
		MaxL0Files:                   4,
		TargetSSTableSize:            128 * 1024 * 1024,
		LevelsTargetSizeMultiplier:   10,
		MaxLevels:                    7,
		BloomFilterFalsePositiveRate: 0.01,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		// Metrics:                      NewEngineMetrics(false, "bench_get_seq_"),
		CompactionIntervalSeconds: 3600, // Minimize compaction during Get benchmark
		SSTableCompressor:         &compressors.LZ4Compressor{},
		WALSyncMode:               wal.SyncDisabled, // Disable WAL Sync for faster pre-population in benchmark
		WALBatchSize:              1,
		WALFlushIntervalMs:        0,
		Logger:                    slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{})),
	}

	eng, err := NewStorageEngine(opts) // Use NewStorageEngine
	if err != nil {
		b.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err = eng.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	// defer eng.Close() // Will be closed and reopened

	// Pre-populate data
	type benchPoint struct {
		metric string
		tags   map[string]string
		ts     int64
	}
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	numPrePopulate := 20000 // Number of items to pre-populate
	prePopulatedPoints := make([]benchPoint, numPrePopulate)
	for i := 0; i < numPrePopulate; i++ {
		p := benchPoint{metric: "bench.get.seq", tags: map[string]string{"id": fmt.Sprintf("%09d", i)}, ts: baseTime.UnixNano() + int64(i)}
		prePopulatedPoints[i] = p
		val := float64(i)
		dp, err := core.NewSimpleDataPoint(p.metric, p.tags, p.ts, map[string]interface{}{"value": val})
		if err != nil {
			b.Fatalf("NewSimpleDataPoint for pre-population failed: %v", err)
		}
		if err := eng.Put(context.Background(), *dp); err != nil {
			b.Fatalf("Pre-populate Put failed: %v", err)
		}
	}
	// Force flush any remaining memtables
	if err := eng.Close(); err != nil { // Close and reopen to ensure data is on disk
		b.Fatalf("Failed to close engine after pre-population: %v", err)
	}
	eng, err = NewStorageEngine(opts) // Reopen
	if err != nil {
		b.Fatalf("NewStorageEngine (reopen) failed: %v", err)
	}
	if err = eng.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	defer eng.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Cycle through the pre-populated keys
		p := prePopulatedPoints[i%numPrePopulate]
		_, errGet := eng.Get(context.Background(), p.metric, p.tags, p.ts)
		if errGet != nil && errGet != sstable.ErrNotFound { // Allow ErrNotFound if b.N > numPrePopulate
			// If key should exist, this is a failure
			if i < numPrePopulate {
				b.Fatalf("Get failed for point %v: %v", p, errGet)
			}
		}
	}
	b.StopTimer()
}

func BenchmarkStorageEngine_Get_Random(b *testing.B) {
	tempDir := b.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            64 * 1024 * 1024,
		IndexMemtableThreshold:       64 * 1024 * 1024,
		BlockCacheCapacity:           1000,
		MaxL0Files:                   4,
		TargetSSTableSize:            128 * 1024 * 1024,
		LevelsTargetSizeMultiplier:   10,
		MaxLevels:                    7,
		BloomFilterFalsePositiveRate: 0.01,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		// Metrics:                      NewEngineMetrics(false, "bench_get_rand_"),
		CompactionIntervalSeconds: 3600, // Minimize compaction
		SSTableCompressor:         &compressors.LZ4Compressor{},
		WALSyncMode:               wal.SyncDisabled, // Disable WAL Sync for faster pre-population in benchmark
		WALBatchSize:              1,
		WALFlushIntervalMs:        0,
		Logger:                    slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{})),
	}

	eng, err := NewStorageEngine(opts)
	if err != nil {
		b.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err = eng.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}

	type benchPoint struct {
		metric string
		tags   map[string]string
		ts     int64
	}
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	numPrePopulate := 20000
	prePopulatedPoints := make([]benchPoint, numPrePopulate)
	for i := 0; i < numPrePopulate; i++ {
		p := benchPoint{metric: "bench.get.rand", tags: map[string]string{"id": fmt.Sprintf("%09d", i)}, ts: baseTime.UnixNano() + int64(i)}
		prePopulatedPoints[i] = p
		val := float64(i)
		dp, err := core.NewSimpleDataPoint(p.metric, p.tags, p.ts, map[string]interface{}{"value": val})
		if err != nil {
			b.Fatalf("NewSimpleDataPoint for pre-population failed: %v", err)
		}
		if err := eng.Put(context.Background(), *dp); err != nil {
			b.Fatalf("Pre-populate Put failed: %v", err)
		}
	}
	if err := eng.Close(); err != nil {
		b.Fatalf("Failed to close engine after pre-population: %v", err)
	}
	eng, err = NewStorageEngine(opts) // Reopen
	if err != nil {
		b.Fatalf("NewStorageEngine (reopen) failed: %v", err)
	}
	if err = eng.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	defer eng.Close()

	// Generate random keys to fetch
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		p := prePopulatedPoints[rng.Intn(numPrePopulate)] // Get a random existing point
		_, errGet := eng.Get(context.Background(), p.metric, p.tags, p.ts)
		if errGet != nil && errGet != sstable.ErrNotFound {
			b.Fatalf("Get failed for point %v: %v", p, errGet)
		}
	}
	b.StopTimer()
}

func BenchmarkStorageEngine_Delete(b *testing.B) {
	tempDir := b.TempDir()
	opts := StorageEngineOptions{
		DataDir:                    tempDir,
		MemtableThreshold:          64 * 1024 * 1024,
		IndexMemtableThreshold:     64 * 1024 * 1024,
		BlockCacheCapacity:         100, // Smaller cache for delete test
		CompactionIntervalSeconds:  5,   // Allow some background activity
		SSTableDefaultBlockSize:    sstable.DefaultBlockSize,
		MaxL0Files:                 4,
		TargetSSTableSize:          128 * 1024 * 1024,
		LevelsTargetSizeMultiplier: 10,
		MaxLevels:                  7,
		SSTableCompressor:          &compressors.LZ4Compressor{},
		WALSyncMode:                wal.SyncAlways,
		WALBatchSize:               1,
		WALFlushIntervalMs:         0,
		Logger:                     slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{})),
	}
	eng, err := NewStorageEngine(opts)
	if err != nil {
		b.Fatalf("NewStorageEngine failed: %v", err)
	}
	err = eng.Start()
	if err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	defer eng.Close()

	// Pre-populate some data that will be deleted
	type benchPoint struct {
		metric string
		tags   map[string]string
		ts     int64
	}
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	numToDelete := b.N // Delete as many items as b.N iterations
	if numToDelete == 0 {
		numToDelete = 1000
	} // Default if b.N is 0 (e.g. -benchtime=1x)
	pointsToDelete := make([]benchPoint, numToDelete)
	for i := 0; i < numToDelete; i++ {
		p := benchPoint{metric: "bench.delete", tags: map[string]string{"id": fmt.Sprintf("%09d", i)}, ts: baseTime.UnixNano() + int64(i)}
		pointsToDelete[i] = p
		val := float64(i)
		dp, err := core.NewSimpleDataPoint(p.metric, p.tags, p.ts, map[string]interface{}{"value": val})
		if err != nil {
			b.Fatalf("NewSimpleDataPoint for pre-population failed: %v", err)
		}
		if err := eng.Put(context.Background(), *dp); err != nil {
			b.Fatalf("Pre-populate Put for delete failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		p := pointsToDelete[i]
		if err := eng.Delete(context.Background(), p.metric, p.tags, p.ts); err != nil {
			b.Fatalf("Delete failed: %v", err)
		}
	}
	b.StopTimer()
}

// BenchmarkStorageEngine_RangeScan measures the performance of scanning a range of keys.
func BenchmarkStorageEngine_RangeScan(b *testing.B) {
	tempDir := b.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            64 * 1024 * 1024,
		IndexMemtableThreshold:       64 * 1024 * 1024,
		BlockCacheCapacity:           1000,
		MaxL0Files:                   4,
		TargetSSTableSize:            128 * 1024 * 1024,
		LevelsTargetSizeMultiplier:   10,
		MaxLevels:                    7,
		BloomFilterFalsePositiveRate: 0.01,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		// Metrics:                      NewEngineMetrics(false, "bench_scan_"),
		CompactionIntervalSeconds: 3600, // Minimize compaction during benchmark
		SSTableCompressor:         &compressors.LZ4Compressor{},
		WALSyncMode:               wal.SyncDisabled, // Disable WAL Sync for faster pre-population in benchmark
		WALBatchSize:              1,
		WALFlushIntervalMs:        0,
		Logger:                    slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{})),
	}

	eng, err := NewStorageEngine(opts)
	if err != nil {
		b.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err = eng.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	numPrePopulate := 20000 // Number of items to pre-populate
	for i := 0; i < numPrePopulate; i++ {
		metric := "bench.rangescan"
		tags := map[string]string{"id": fmt.Sprintf("%09d", i)}
		ts := baseTime.UnixNano() + int64(i)
		val := float64(i)
		dp, err := core.NewSimpleDataPoint(metric, tags, ts, map[string]interface{}{"value": val})
		if err != nil {
			b.Fatalf("NewSimpleDataPoint for pre-population failed: %v", err)
		}
		if err := eng.Put(context.Background(), *dp); err != nil {
			b.Fatalf("Pre-populate Put failed: %v", err)
		}
	}

	if err := eng.Close(); err != nil {
		b.Fatalf("Failed to close engine after pre-population: %v", err)
	}
	eng, err = NewStorageEngine(opts) // Reopen
	if err != nil {
		b.Fatalf("NewStorageEngine (reopen) failed: %v", err)
	}
	if err = eng.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	defer eng.Close()

	// Define a range that covers a portion of the data
	// For example, scan 10% of the keys in the middle
	scanStartTime := baseTime.UnixNano() + int64(numPrePopulate/4)
	scanEndTime := baseTime.UnixNano() + int64((numPrePopulate/4)+(numPrePopulate/10))

	scanOptions := core.QueryParams{
		Metric:    "bench.rangescan",
		Tags:      nil, // Scan all series for this metric
		StartTime: scanStartTime,
		EndTime:   scanEndTime,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		iter, errScan := eng.Query(context.Background(), scanOptions)
		if errScan != nil {
			b.Fatalf("RangeScan failed: %v", errScan)
		}
		for iter.Next() {
			// Access the item to simulate real usage
			item, err := iter.At()
			if err != nil {
				b.Fatalf("iter.At() failed: %v", err)
			}
			// Return the item to the pool
			iter.Put(item)
		}
		iter.Close() // Important to close the iterator
	}
	b.StopTimer()
}

// BenchmarkStorageEngine_Query_DownsampleAndAggregate measures the performance of a complex query
// involving downsampling and final aggregation over a large number of data points.
func BenchmarkStorageEngine_Query_DownsampleAndAggregate(b *testing.B) {
	tempDir := b.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            64 * 1024 * 1024, // 64MB, large enough for setup
		IndexMemtableThreshold:       64 * 1024 * 1024,
		BlockCacheCapacity:           10000, // A decent cache size for reads
		MaxL0Files:                   4,
		TargetSSTableSize:            128 * 1024 * 1024,
		LevelsTargetSizeMultiplier:   10,
		MaxLevels:                    7,
		BloomFilterFalsePositiveRate: 0.01,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		CompactionIntervalSeconds:    3600, // Disable compaction during benchmark
		SSTableCompressor:            &compressors.LZ4Compressor{},
		WALSyncMode:                  wal.SyncDisabled, // Disable WAL for faster setup
		Logger:                       slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{})),
	}

	eng, err := NewStorageEngine(opts)
	if err != nil {
		b.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err = eng.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}

	// Pre-populate data
	numSeries := 10
	pointsPerSeries := 10000 // 10k points per series
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	metricName := "bench.query.downsample"

	b.Logf("Pre-populating %d total data points (%d series x %d points)...", numSeries*pointsPerSeries, numSeries, pointsPerSeries)
	for i := 0; i < numSeries; i++ {
		tags := map[string]string{"series_id": fmt.Sprintf("series-%d", i)}
		for j := 0; j < pointsPerSeries; j++ {
			// Spread points 1 second apart
			ts := baseTime.Add(time.Duration(j) * time.Second).UnixNano()
			val := rand.Float64() * 100.0
			dp, err := core.NewSimpleDataPoint(metricName, tags, ts, map[string]interface{}{"value": val})
			if err != nil {
				b.Fatalf("NewSimpleDataPoint for pre-population failed: %v", err)
			}
			if err := eng.Put(context.Background(), *dp); err != nil {
				b.Fatalf("Pre-populate Put failed: %v", err)
			}
		}
	}
	b.Log("Pre-population complete. Closing and reopening engine to flush data to SSTables...")

	// Force flush all data to SSTables
	if err := eng.Close(); err != nil {
		b.Fatalf("Failed to close engine after pre-population: %v", err)
	}
	eng, err = NewStorageEngine(opts) // Reopen
	if err != nil {
		b.Fatalf("NewStorageEngine (reopen) failed: %v", err)
	}
	if err = eng.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	defer eng.Close()
	b.Log("Engine reopened. Starting benchmark.")

	// Define the query that will be run in the benchmark loop
	queryParams := core.QueryParams{
		Metric:             metricName,
		Tags:               map[string]string{"series_id": "series-3"}, // Query a single series
		StartTime:          baseTime.UnixNano(),
		EndTime:            baseTime.Add(time.Duration(pointsPerSeries) * time.Second).UnixNano(),
		DownsampleInterval: "1m",
		AggregationSpecs: []core.AggregationSpec{
			{Function: core.AggCount, Field: "count"},
			{Function: core.AggSum, Field: "sum"},
			{Function: core.AggAvg, Field: "avg"},
			{Function: core.AggMin, Field: "min"},
			{Function: core.AggMax, Field: "max"},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		iter, errQuery := eng.Query(context.Background(), queryParams)
		if errQuery != nil {
			b.Fatalf("Query failed: %v", errQuery)
		}

		// Drain the iterator to ensure all work is done
		for iter.Next() {
			item, err := iter.At()
			if err != nil {
				b.Fatalf("iter.At() failed: %v", err)
			}
			// Return the item to the pool
			iter.Put(item)
		}
		if err := iter.Error(); err != nil {
			b.Fatalf("Iterator error: %v", err)
		}
		iter.Close()
	}

	b.StopTimer()
}

// BenchmarkStorageEngine_Query_MultiSeries_DownsampleAndAggregate measures performance of
// downsampling and aggregating across multiple series.
func BenchmarkStorageEngine_Query_MultiSeries_DownsampleAndAggregate(b *testing.B) {
	tempDir := b.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            64 * 1024 * 1024,
		IndexMemtableThreshold:       64 * 1024 * 1024,
		BlockCacheCapacity:           10000,
		MaxL0Files:                   4,
		TargetSSTableSize:            128 * 1024 * 1024,
		LevelsTargetSizeMultiplier:   10,
		MaxLevels:                    7,
		BloomFilterFalsePositiveRate: 0.01,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		CompactionIntervalSeconds:    3600,
		SSTableCompressor:            &compressors.LZ4Compressor{},
		WALSyncMode:                  wal.SyncDisabled,
		Logger:                       slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{})),
	}

	eng, err := NewStorageEngine(opts)
	if err != nil {
		b.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err = eng.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}

	// Pre-populate data
	numSeries := 100
	pointsPerSeries := 1000
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	metricName := "bench.query.multiseries.downsample"

	b.Logf("Pre-populating %d total data points (%d series x %d points)...", numSeries*pointsPerSeries, numSeries, pointsPerSeries)
	for i := 0; i < numSeries; i++ {
		tags := map[string]string{"series_id": fmt.Sprintf("series-%d", i)}
		for j := 0; j < pointsPerSeries; j++ {
			ts := baseTime.Add(time.Duration(j) * time.Second).UnixNano()
			val := rand.Float64() * 100.0
			dp, err := core.NewSimpleDataPoint(metricName, tags, ts, map[string]interface{}{"value": val})
			if err != nil {
				b.Fatalf("NewSimpleDataPoint for pre-population failed: %v", err)
			}
			if err := eng.Put(context.Background(), *dp); err != nil {
				b.Fatalf("Pre-populate Put failed: %v", err)
			}
		}
	}
	b.Log("Pre-population complete. Closing and reopening engine to flush data to SSTables...")

	if err := eng.Close(); err != nil {
		b.Fatalf("Failed to close engine after pre-population: %v", err)
	}
	eng, err = NewStorageEngine(opts)
	if err != nil {
		b.Fatalf("NewStorageEngine (reopen) failed: %v", err)
	}
	if err = eng.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	defer eng.Close()
	b.Log("Engine reopened. Starting benchmark.")

	// Query across all series for the metric
	queryParams := core.QueryParams{
		Metric:             metricName,
		Tags:               nil, // No tag filter, so it queries all series for the metric
		StartTime:          baseTime.UnixNano(),
		EndTime:            baseTime.Add(time.Duration(pointsPerSeries) * time.Second).UnixNano(),
		DownsampleInterval: "5m",
		AggregationSpecs: []core.AggregationSpec{
			{Function: core.AggSum, Field: "sum"},
			{Function: core.AggAvg, Field: "avg"},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		iter, errQuery := eng.Query(context.Background(), queryParams)
		if errQuery != nil {
			b.Fatalf("Query failed: %v", errQuery)
		}
		// Drain the iterator
		for iter.Next() {
			item, err := iter.At()
			if err != nil {
				b.Fatalf("iter.At() failed: %v", err)
			}
			iter.Put(item)
		}
		iter.Close()
	}
	b.StopTimer()
}

// BenchmarkStorageEngine_ConcurrentWrites_SameDataPoint simulates multiple goroutines
// concurrently writing (updating) the exact same data point (metric, tags, timestamp).
// This tests the system's ability to handle concurrent updates and ensure the latest version wins.
func BenchmarkStorageEngine_ConcurrentWrites_SameDataPoint(b *testing.B) {
	tempDir := b.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            1 * 1024 * 1024, // 1MB Memtable to force flushes
		IndexMemtableThreshold:       1 * 1024 * 1024,
		BlockCacheCapacity:           100,
		MaxL0Files:                   2, // Aggressive L0 compaction
		TargetSSTableSize:            4 * 1024 * 1024,
		LevelsTargetSizeMultiplier:   2,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		CompactionIntervalSeconds:    1, // Frequent compaction
		SSTableCompressor:            &compressors.LZ4Compressor{},
		WALSyncMode:                  wal.SyncAlways, // Ensure durability
		WALBatchSize:                 1,
		WALFlushIntervalMs:           0,
		Logger:                       slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{})),
	}
	eng, err := NewStorageEngine(opts)
	if err != nil {
		b.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err = eng.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	defer eng.Close()

	metric := "concurrent.update.metric"
	tags := map[string]string{"host": "test_server", "region": "us-east"}
	fixedTimestamp := time.Date(2024, 7, 1, 12, 0, 0, 0, time.UTC).UnixNano() // Fixed timestamp

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		localRand := rand.New(rand.NewSource(time.Now().UnixNano() + int64(rand.Int63())))
		for pb.Next() {
			// Create a unique value for each operation to ensure updates are distinct.
			val := localRand.Float64() * 1000.0
			dp, err := core.NewSimpleDataPoint(metric, tags, fixedTimestamp, map[string]interface{}{"value": val})
			if err != nil {
				b.Errorf("NewSimpleDataPoint failed: %v", err)
				continue
			}
			if err := eng.Put(context.Background(), *dp); err != nil {
				b.Errorf("Put failed: %v", err)
			}
		}
	})

	b.StopTimer()

	// Verification: Get the data point and ensure it's found.
	// Since we put b.N times, the final value should be the one with the highest sequence number.
	if _, err := eng.Get(context.Background(), metric, tags, fixedTimestamp); err != nil {
		b.Errorf("Get after concurrent updates failed: %v", err)
	}
}

// BenchmarkStorageEngine_StressTest_MixedWorkload simulates a mixed read/write workload
// with high concurrency over a sustained period.
func BenchmarkStorageEngine_StressTest_MixedWorkload(b *testing.B) {
	tempDir := b.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            64 * 1024 * 1024, // 64MB Memtable
		IndexMemtableThreshold:       64 * 1024 * 1024,
		BlockCacheCapacity:           1000, // Example capacity
		MaxL0Files:                   4,
		TargetSSTableSize:            128 * 1024 * 1024,
		LevelsTargetSizeMultiplier:   10,
		MaxLevels:                    7,
		BloomFilterFalsePositiveRate: 0.01,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		CompactionIntervalSeconds:    1, // Allow frequent compaction
		SSTableCompressor:            &compressors.LZ4Compressor{},
		WALSyncMode:                  wal.SyncDisabled, // Disable WAL Sync for faster writes during stress test
		WALBatchSize:                 1,
		WALFlushIntervalMs:           0,
		Logger:                       slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{})),
	}

	eng, err := NewStorageEngine(opts)
	if err != nil {
		b.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err = eng.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	defer eng.Close()

	// Data structures to store initial points for later verification and random access
	numPrePopulate := 10000
	initialKeys := make([]initialPointKey, numPrePopulate)
	initialData := make(map[string]map[int64]float64)

	// Pre-populate some base data for reads
	b.Logf("Pre-populating %d initial data points...", numPrePopulate)
	for i := 0; i < numPrePopulate; i++ {
		metric := fmt.Sprintf("stress_metric_%d", i%100)                // 100 unique metrics
		tags := map[string]string{"host": fmt.Sprintf("host_%d", i%10)} // 10 unique hosts
		ts := time.Now().UnixNano() - int64(i)*1000                     // Spread timestamps, ensuring uniqueness
		val := float64(i)
		dp, err := core.NewSimpleDataPoint(metric, tags, ts, map[string]interface{}{"value": val})
		if err != nil {
			b.Fatalf("NewSimpleDataPoint for pre-population failed: %v", err)
		}
		if err := eng.Put(context.Background(), *dp); err != nil {
			b.Fatalf("Pre-population Put failed: %v", err)
		}
		initialKeys[i] = initialPointKey{metric: metric, tags: tags, timestamp: ts}
		if _, ok := initialData[metric]; !ok {
			initialData[metric] = make(map[int64]float64)
		}
		initialData[metric][ts] = val
	}
	b.Log("Pre-population complete.")

	// Define workload mix (e.g., 70% writes, 30% reads)
	writeRatio := 0.7 // Read ratio is implicitly 1 - writeRatio

	b.ResetTimer()
	b.ReportAllocs()

	var goroutineIDCounter atomic.Uint64
	var totalOps atomic.Uint64
	var totalErrors atomic.Uint64

	b.RunParallel(func(pb *testing.PB) {
		goroutineID := goroutineIDCounter.Add(1)                                  // Unique ID for each goroutine's random source
		r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(goroutineID))) // Local random source

		// Helper to pick a random initial point for read/delete operations
		pickRandomInitialPoint := func() initialPointKey {
			idx := r.Intn(numPrePopulate)
			return initialKeys[idx]
		}

		for pb.Next() {
			totalOps.Add(1) // Increment total operations for each iteration

			if r.Float64() < writeRatio {
				// Perform a write operation
				metric := fmt.Sprintf("stress_metric_%d", r.Intn(200)) // Mix of existing and new metrics
				tags := map[string]string{"host": fmt.Sprintf("host_%d", r.Intn(20))}
				ts := time.Now().UnixNano()
				val := r.Float64() * 100.0 // Random float value

				dp, err := core.NewSimpleDataPoint(metric, tags, ts, map[string]interface{}{"value": val})
				if err != nil {
					b.Errorf("NewSimpleDataPoint failed: %v", err)
					continue
				}
				if err := eng.Put(context.Background(), *dp); err != nil {
					b.Errorf("Put failed: %v", err)
					totalErrors.Add(1) // Increment error counter
				}
			} else if r.Float64() < 0.1 { // Perform a delete operation (10% of non-write ops)
				pointToDelete := pickRandomInitialPoint()
				if err := eng.Delete(context.Background(), pointToDelete.metric, pointToDelete.tags, pointToDelete.timestamp); err != nil {
					b.Errorf("Delete failed for %s (ts %d): %v", pointToDelete.metric, pointToDelete.timestamp, err)
					totalErrors.Add(1) // Increment error counter
				}
			} else { // Perform a read (query) operation
				pointToQuery := pickRandomInitialPoint() // Pick a random point from the pre-populated data
				queryParams := core.QueryParams{
					Metric:    pointToQuery.metric,
					StartTime: pointToQuery.timestamp - int64(time.Minute), // Query around the point's timestamp
					EndTime:   pointToQuery.timestamp + int64(time.Minute),
					Tags:      pointToQuery.tags, // Use the tags from the pre-populated point
				}
				iter, err := eng.Query(context.Background(), queryParams)
				if err != nil {
					b.Errorf("Query failed: %v", err)
					totalErrors.Add(1) // Increment error counter
					continue
				}
				// Iterate through results to simulate actual data consumption
				for iter.Next() {
					item, err := iter.At()
					if err != nil {
						b.Errorf("iter.At() failed: %v", err)
						totalErrors.Add(1)
						break // Stop iterating this one on error
					}
					// Return the item to the pool
					iter.Put(item)
				}
				if err := iter.Error(); err != nil {
					totalErrors.Add(1)
					b.Errorf("Query iterator error: %v", err)
				}
				iter.Close()
			}
		}
	})
	b.StopTimer()

	b.Log("Starting post-benchmark data verification...")

	// Allow compactions to settle after the stress test
	b.Log("Waiting for compactions to settle...")
	// This is a heuristic wait. A more robust way would be to poll compaction metrics.
	time.Sleep(5 * time.Second)

	// Verify initial data points
	verifiedCount := 0
	for i := 0; i < numPrePopulate; i += (numPrePopulate / 10) + 1 { // Check a sample of initial points (every ~10th point)
		key := initialKeys[i]
		expectedVal := initialData[key.metric][key.timestamp]

		retrievedVal, err := eng.Get(context.Background(), key.metric, key.tags, key.timestamp)
		if err != nil {
			if err == sstable.ErrNotFound {
				// This point might have been deleted during the test.
				// We can't easily track which points were deleted by the benchmark.
				// For now, we just log it.
				b.Logf("Initial point %s (ts %d) not found, possibly deleted.", key.metric, key.timestamp)
			} else {
				b.Errorf("Get for initial point %s (ts %d) failed: %v", key.metric, key.timestamp, err)
			}
		} else {
			if v, ok := retrievedVal["value"].ValueFloat64(); ok && math.Abs(v-expectedVal) > 1e-6 {
				b.Errorf("Value mismatch for initial point %s (ts %d): got %f, want %f", key.metric, key.timestamp, v, expectedVal)
			}
			verifiedCount++
		}
	}
	b.Logf("Verified %d initial data points (excluding those potentially deleted).", verifiedCount)
	b.Logf("Stress test completed. Total operations: %d, Total errors: %d", totalOps.Load(), totalErrors.Load())

	// You could add more specific checks here if you track which points were deleted.
	// For example, if you maintain a list of "known deleted" points, you can assert they are ErrNotFound.
}

// BenchmarkStorageEngine_ReadHeavy_MixedWorkload simulates a read-heavy workload,
// where the vast majority of operations are random reads from a large, pre-existing dataset,
// with a small percentage of concurrent writes. This is a common scenario for dashboards
// and monitoring systems querying historical data.
func BenchmarkStorageEngine_ReadHeavy_MixedWorkload(b *testing.B) {
	tempDir := b.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            64 * 1024 * 1024, // 64MB Memtable
		IndexMemtableThreshold:       64 * 1024 * 1024,
		BlockCacheCapacity:           10000, // A larger cache is beneficial for read-heavy workloads
		MaxL0Files:                   4,
		TargetSSTableSize:            128 * 1024 * 1024,
		LevelsTargetSizeMultiplier:   10,
		MaxLevels:                    7,
		BloomFilterFalsePositiveRate: 0.001,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		CompactionIntervalSeconds:    5, // Allow compactions to run in the background
		SSTableCompressor:            &compressors.LZ4Compressor{},
		WALSyncMode:                  wal.SyncDisabled, // Disable WAL Sync for faster writes during stress test
		Logger:                       slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{})),
	}

	eng, err := NewStorageEngine(opts)
	if err != nil {
		b.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err = eng.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}

	// Pre-populate a large dataset
	numPrePopulate := 100000 // 100k initial points
	initialKeys := make([]initialPointKey, numPrePopulate)
	b.Logf("Pre-populating %d initial data points...", numPrePopulate)
	for i := 0; i < numPrePopulate; i++ {
		metric := fmt.Sprintf("read_heavy_metric_%d", i%200)            // 200 unique metrics
		tags := map[string]string{"host": fmt.Sprintf("host_%d", i%50)} // 50 unique hosts
		ts := time.Now().UnixNano() - int64(i)*1000
		val := float64(i)
		initialKeys[i] = initialPointKey{metric: metric, tags: tags, timestamp: ts}
		dp, err := core.NewSimpleDataPoint(metric, tags, ts, map[string]interface{}{"value": val})
		if err != nil {
			b.Fatalf("NewSimpleDataPoint for pre-population failed: %v", err)
		}
		if err := eng.Put(context.Background(), *dp); err != nil {
			b.Fatalf("Pre-population Put failed: %v", err)
		}
	}

	// Close and reopen to ensure all data is flushed to SSTables, simulating a mature database state.
	b.Log("Flushing all data to disk...")
	if err := eng.Close(); err != nil {
		b.Fatalf("Failed to close engine after pre-population: %v", err)
	}
	eng, err = NewStorageEngine(opts)
	if err != nil {
		b.Fatalf("NewStorageEngine (reopen) failed: %v", err)
	}
	if err = eng.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	defer eng.Close()
	b.Log("Pre-population complete. Starting benchmark.")

	// Define workload mix: 95% reads, 5% writes
	readRatio := 0.95

	b.ResetTimer()
	b.ReportAllocs()

	var goroutineIDCounter atomic.Uint64

	b.RunParallel(func(pb *testing.PB) {
		goroutineID := goroutineIDCounter.Add(1)
		r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(goroutineID)))

		for pb.Next() {
			if r.Float64() < readRatio {
				// Perform a read operation
				pointToRead := initialKeys[r.Intn(numPrePopulate)]
				_, err := eng.Get(context.Background(), pointToRead.metric, pointToRead.tags, pointToRead.timestamp)
				if err != nil && err != sstable.ErrNotFound {
					b.Errorf("Get failed: %v", err)
				}
			} else {
				// Perform a write operation
				metric := fmt.Sprintf("read_heavy_new_metric_%d", r.Intn(10)) // Write to a smaller set of new metrics
				tags := map[string]string{"host": fmt.Sprintf("new_host_%d", r.Intn(5))}
				ts := time.Now().UnixNano()
				val := r.Float64() * 1000.0

				dp, err := core.NewSimpleDataPoint(metric, tags, ts, map[string]interface{}{"value": val})
				if err != nil {
					b.Errorf("NewSimpleDataPoint failed: %v", err)
					continue
				}
				if err := eng.Put(context.Background(), *dp); err != nil {
					b.Errorf("Put failed: %v", err)
				}
			}
		}
	})

	b.StopTimer()
}

// setupBenchmarkEngineWithData สร้าง test engine และใส่ข้อมูลอนุกรมเวลาจำนวนมากลงไป
// เพื่อจำลองภาระงาน (load) ที่ใกล้เคียงกับความเป็นจริง
func setupBenchmarkEngineWithData(b *testing.B, numSeries int, dataDuration time.Duration, interval time.Duration) (*storageEngine, func()) {
	b.Helper()

	// สร้าง temporary directory สำหรับฐานข้อมูลที่ใช้ทดสอบ
	dataDir, err := os.MkdirTemp("", "benchmark-engine-*")
	require.NoError(b, err)

	// ใช้ mock clock เพื่อควบคุมเวลาตอนสร้างข้อมูล
	mockClock := utils.NewMockClock(time.Date(2024, 7, 15, 12, 0, 0, 0, time.UTC))

	opts := StorageEngineOptions{
		DataDir:                      dataDir,
		MemtableThreshold:            64 * 1024 * 1024, // 64MB Memtable
		IndexMemtableThreshold:       64 * 1024 * 1024,
		BlockCacheCapacity:           10000, // A larger cache is beneficial for read-heavy workloads
		MaxL0Files:                   4,
		TargetSSTableSize:            128 * 1024 * 1024,
		LevelsTargetSizeMultiplier:   10,
		MaxLevels:                    7,
		BloomFilterFalsePositiveRate: 0.001,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		CompactionIntervalSeconds:    3600, // Disable background compaction to prevent race conditions during read benchmarks
		SSTableCompressor:            &compressors.LZ4Compressor{},
		WALSyncMode:                  wal.SyncDisabled, // Disable WAL Sync for faster writes during stress test
		Logger:                       slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{})),
		Clock:                        mockClock,
	}

	engine, err := initializeStorageEngine(opts)
	require.NoError(b, err)
	err = engine.Start()
	require.NoError(b, err)

	b.Logf("กำลังใส่ข้อมูลลง engine: %d series, ย้อนหลัง %v, ทุกๆ %v...", numSeries, dataDuration, interval)
	startTime := mockClock.Now().Add(-dataDuration)
	points := make([]core.DataPoint, 0, 1000) // ขนาดของ Batch
	totalPoints := 0

	for i := 0; i < numSeries; i++ {
		metric := "cpu.usage"
		tags := map[string]string{"host": fmt.Sprintf("server-%d", i)}
		for ts := startTime; ts.Before(mockClock.Now()); ts = ts.Add(interval) {
			fields, _ := core.NewFieldValuesFromMap(map[string]interface{}{"value": float64(i) + (float64(ts.UnixNano()%1000) / 100.0)})
			points = append(points, core.DataPoint{
				Metric:    metric,
				Tags:      tags,
				Timestamp: ts.UnixNano(),
				Fields:    fields,
			})

			if len(points) == 1000 {
				err := engine.PutBatch(context.Background(), points)
				require.NoError(b, err)
				totalPoints += len(points)
				points = points[:0]
			}
		}
	}
	// ใส่ข้อมูลที่เหลือ
	if len(points) > 0 {
		err := engine.PutBatch(context.Background(), points)
		require.NoError(b, err)
		totalPoints += len(points)
	}

	b.Logf("ใส่ข้อมูลเสร็จสิ้น รวมทั้งหมด: %d จุด", totalPoints)

	// Wait for any background flushes (triggered by full memtables) to complete
	// before forcing the final synchronous flush. This prevents a race condition
	// where ForceFlush returns ErrFlushInProgress because the flush loop is busy.
	b.Log("Waiting for background flushes to complete...")
	for engine.hasImmutableMemtables() {
		time.Sleep(100 * time.Millisecond)
	}
	b.Log("Background flushes completed.")

	// Flush ข้อมูลทั้งหมดลง disk เพื่อให้แน่ใจว่า Query จะวิ่งไปอ่าน SSTables
	b.Log("กำลัง Flush ข้อมูลทั้งหมดลง disk...")
	err = engine.ForceFlush(context.Background(), true)
	require.NoError(b, err)
	b.Log("Flush เสร็จสิ้น")

	cleanup := func() {
		engine.Close()
		os.RemoveAll(dataDir)
	}

	return engine, cleanup
}

// BenchmarkRelativeQuery_LoadTest วัดประสิทธิภาพของ Relative Query
// บนชุดข้อมูลขนาดใหญ่ เพื่อจำลองการทดสอบแบบ Load Test
func BenchmarkRelativeQuery_LoadTest(b *testing.B) {
	// --- Setup: สร้างชุดข้อมูลขนาดใหญ่ ---
	// ทำเพียงครั้งเดียวก่อนเริ่ม Benchmark
	// 100 series, ข้อมูลย้อนหลัง 7 วัน, 1 จุดข้อมูลทุกๆ 1 นาที
	// รวมทั้งหมด = 100 * 7 * 24 * 60 = 1,008,000 จุด
	engine, cleanup := setupBenchmarkEngineWithData(b, 100, 7*24*time.Hour, 1*time.Minute)
	defer cleanup()

	// --- Scenarios: กำหนดช่วงเวลาของ Relative Query ที่จะทดสอบ ---
	scenarios := []struct {
		name             string
		relativeDuration string
	}{
		{"Query_1_Hour_Relative", "1h"},
		{"Query_24_Hours_Relative", "24h"},
		{"Query_7_Days_Relative", "7d"},
	}

	for _, s := range scenarios {
		b.Run(s.name, func(b *testing.B) {
			b.ReportAllocs()
			var totalCount int64
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				params := core.QueryParams{
					Metric:           "cpu.usage", // Query ทุก series ของ metric นี้
					IsRelative:       true,
					RelativeDuration: s.relativeDuration,
				}

				iter, err := engine.Query(context.Background(), params)
				require.NoError(b, err)

				// ต้องวนอ่านข้อมูลจาก iterator ให้ครบทุกตัว เพื่อให้การวัดผลสะท้อนการทำงานจริง
				// เพราะ iterator ทำงานแบบ lazy (ดึงข้อมูลเมื่อต้องการ)
				count := 0
				for iter.Next() {
					_, err := iter.At()
					require.NoError(b, err)
					count++
				}
				totalCount += int64(count)

				require.NoError(b, iter.Error())
				iter.Close()
			}
			b.ReportMetric(float64(totalCount)/float64(b.N), "items/op")
		})
	}
}
