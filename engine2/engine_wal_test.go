package engine2

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexusbase/sys"
	"github.com/stretchr/testify/require"
)

func init() {
	sys.SetDebugMode(false)
}

// testDataPoint is a helper struct for defining test data points in this file.
type testDataPoint struct {
	metric    string
	tags      map[string]string
	timestamp int64
	value     float64
}

func TestStorageEngine_WALRecovery_CrashSimulation(t *testing.T) {
	tempDir := t.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            1024 * 1024,
		BlockCacheCapacity:           10,
		MaxL0Files:                   4,
		TargetSSTableSize:            2048,
		LevelsTargetSizeMultiplier:   2,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		Metrics:                      NewEngineMetrics(false, "wal_crash_"),
		WALSyncMode:                  core.WALSyncAlways,
		WALBatchSize:                 1,
		CompactionIntervalSeconds:    3600,
	}

	crashEngine(t, opts, func(e StorageEngineInterface) {
		require.NoError(t, e.Put(context.Background(), HelperDataPoint(t, "wal.metric", map[string]string{"id": "1"}, 1000, map[string]interface{}{"value": 1.0})))
		require.NoError(t, e.Put(context.Background(), HelperDataPoint(t, "wal.metric", map[string]string{"id": "2"}, 2000, map[string]interface{}{"value": 2.0})))
		require.NoError(t, e.Put(context.Background(), HelperDataPoint(t, "wal.metric", map[string]string{"id": "3"}, 3000, map[string]interface{}{"value": 3.0})))
	})

	engine2, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine (engine2) for recovery failed: %v", err)
	}
	if err = engine2.Start(); err != nil {
		t.Fatalf("Failed to start setup engine: %v", err)
	}
	defer engine2.Close()

	for i := 1; i <= 3; i++ {
		retrievedValue, err := engine2.Get(context.Background(), "wal.metric", map[string]string{"id": fmt.Sprintf("%d", i)}, int64(i*1000))
		if err != nil {
			t.Errorf("engine2.Get failed for id %d after WAL recovery: %v", i, err)
		}
		if val, ok := retrievedValue["value"].ValueFloat64(); !ok || val != float64(i) {
			t.Errorf("engine2.Get retrieved value mismatch for id %d: got %f, want %f", i, val, float64(i))
		}
	}

	// Note: do not inspect concrete internals here; rely on public API checks above.
}

func TestStorageEngine_WALRecovery_WithDeletes(t *testing.T) {
	tempDir := t.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            1024 * 1024,
		CompactionIntervalSeconds:    3600,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		MaxLevels:                    3,
		Metrics:                      NewEngineMetrics(false, "wal_delete_"),
		WALSyncMode:                  core.WALSyncAlways,
		WALBatchSize:                 1,
		BloomFilterFalsePositiveRate: 0.01,
		SSTableCompressor:            &compressors.NoCompressionCompressor{},
	}

	crashEngine(t, opts, func(e StorageEngineInterface) {
		require.NoError(t, e.Put(context.Background(), HelperDataPoint(t, "metric.keep", map[string]string{"id": "keep"}, 1000, map[string]interface{}{"value": 1.0})))
		require.NoError(t, e.Put(context.Background(), HelperDataPoint(t, "metric.point.delete", map[string]string{"id": "point_del"}, 2000, map[string]interface{}{"value": 2.0})))
		require.NoError(t, e.Delete(context.Background(), "metric.point.delete", map[string]string{"id": "point_del"}, 2000))
		require.NoError(t, e.Put(context.Background(), HelperDataPoint(t, "metric.series.delete", map[string]string{"id": "series_del"}, 3000, map[string]interface{}{"value": 3.0})))
		require.NoError(t, e.DeleteSeries(context.Background(), "metric.series.delete", map[string]string{"id": "series_del"}))
		require.NoError(t, e.Put(context.Background(), HelperDataPoint(t, "metric.range.delete", map[string]string{"id": "range_del"}, 4000, map[string]interface{}{"value": 4.0})))
		require.NoError(t, e.Put(context.Background(), HelperDataPoint(t, "metric.range.delete", map[string]string{"id": "range_del"}, 5000, map[string]interface{}{"value": 5.0})))
		require.NoError(t, e.Put(context.Background(), HelperDataPoint(t, "metric.range.delete", map[string]string{"id": "range_del"}, 6000, map[string]interface{}{"value": 6.0})))
		require.NoError(t, e.DeletesByTimeRange(context.Background(), "metric.range.delete", map[string]string{"id": "range_del"}, 4500, 5500))
	})

	engine2, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine (engine2) for recovery failed: %v", err)
	}
	if err = engine2.Start(); err != nil {
		t.Fatalf("Failed to start setup engine: %v", err)
	}
	defer engine2.Close()

	if _, err := engine2.Get(context.Background(), "metric.keep", map[string]string{"id": "keep"}, 1000); err != nil {
		t.Errorf("Expected to find kept data point, but got error: %v", err)
	}

	if _, err := engine2.Get(context.Background(), "metric.point.delete", map[string]string{"id": "point_del"}, 2000); err == nil {
		t.Errorf("Expected point-deleted data to be NotFound, but got no error")
	}

	if _, err := engine2.Get(context.Background(), "metric.series.delete", map[string]string{"id": "series_del"}, 3000); !errors.Is(err, sstable.ErrNotFound) {
		t.Errorf("Expected series-deleted data to be NotFound, but got error: %v", err)
	}

	if _, err := engine2.Get(context.Background(), "metric.range.delete", map[string]string{"id": "range_del"}, 4000); err != nil {
		t.Errorf("Expected to find data before range delete, but got error: %v", err)
	}
	if _, err := engine2.Get(context.Background(), "metric.range.delete", map[string]string{"id": "range_del"}, 5000); !errors.Is(err, sstable.ErrNotFound) {
		t.Errorf("Expected range-deleted data to be NotFound, but got error: %v", err)
	}
	if _, err := engine2.Get(context.Background(), "metric.range.delete", map[string]string{"id": "range_del"}, 6000); err != nil {
		t.Errorf("Expected to find data after range delete, but got error: %v", err)
	}
}

func TestStorageEngine_WALRecovery_AdvancedCorruption(t *testing.T) {
	setupWALWithData := func(t *testing.T, dir string, entries []testDataPoint) (StorageEngineOptions, string) {
		t.Helper()
		opts := GetBaseOptsForTest(t, "test")
		opts.DataDir = dir
		opts.WALSyncMode = core.WALSyncAlways

		crashEngine(t, opts, func(e StorageEngineInterface) {
			for _, entry := range entries {
				dp := HelperDataPoint(t, entry.metric, entry.tags, entry.timestamp, map[string]interface{}{"value": entry.value})
				require.NoError(t, e.Put(context.Background(), dp))
			}
		})

		walPath := filepath.Join(dir, "wal")
		return opts, walPath
	}

	testEntries := []testDataPoint{
		{"metric.1", map[string]string{"id": "a"}, 1000, 1.0},
		{"metric.2", map[string]string{"id": "b"}, 2000, 2.0},
		{"metric.3", map[string]string{"id": "c"}, 3000, 3.0},
	}

	t.Run("CorruptedHeader", func(t *testing.T) {
		tempDir := t.TempDir()
		opts, walPath := setupWALWithData(t, tempDir, testEntries)

		segmentPath := filepath.Join(walPath, "00000001.wal")
		data, err := os.ReadFile(segmentPath)

		if err != nil {
			t.Fatalf("Failed to read WAL segment file for corruption: %v", err)
		}
		binary.LittleEndian.PutUint32(data[0:4], 0xDEADBEEF)
		if err := os.WriteFile(segmentPath, data, 0644); err != nil {
			t.Fatalf("Failed to write corrupted WAL segment file: %v", err)
		}

		eng2, err := NewStorageEngine(opts)
		if err != nil {
			t.Fatal("NewStorageEngine should not have failed on corrupted WAL header, but it did")
		}

		if err = eng2.Start(); err == nil {
			t.Fatal("NewStorageEngine should have failed on corrupted WAL header, but it succeeded")
		}
		if !strings.Contains(err.Error(), "invalid magic number") {
			t.Errorf("Expected error message to contain 'invalid magic number', but got: %v", err)
		}
	})

	t.Run("TruncatedRecord", func(t *testing.T) {
		tempDir := t.TempDir()
		opts, walPath := setupWALWithData(t, tempDir, testEntries)
		segmentPath := filepath.Join(walPath, "00000001.wal")

		stat, err := os.Stat(segmentPath)
		if err != nil {
			t.Fatalf("Failed to stat WAL segment file: %v", err)
		}

		originalSize := stat.Size()

		walData, err := os.ReadFile(segmentPath)
		if err != nil {
			t.Fatalf("Failed to read WAL segment file to determine truncate size: %v", err)
		}
		fileHeaderSize := binary.Size(core.FileHeader{})
		firstRecordLen := binary.LittleEndian.Uint32(walData[int(fileHeaderSize) : int(fileHeaderSize)+4])
		firstRecordTotalSizeOnDisk := 4 + firstRecordLen + 4
		truncateOffset := int64(fileHeaderSize) + int64(firstRecordTotalSizeOnDisk) + 5
		if int64(truncateOffset) >= originalSize {
			t.Fatalf("Cannot test truncation; file size (%d) is not large enough for calculated offset (%d)", originalSize, truncateOffset)
		}
		if err := os.Truncate(segmentPath, int64(truncateOffset)); err != nil {
			t.Fatalf("Failed to truncate WAL segment file: %v", err)
		}

		eng, err := NewStorageEngine(opts)
		if err != nil {
			t.Fatal("NewStorageEngine should not have failed on corrupted WAL header, but it did")
		}

		if err = eng.Start(); err == nil {
			t.Fatal("NewStorageEngine should have failed on corrupted WAL header, but it succeeded")
		}

		if !errors.Is(err, io.ErrUnexpectedEOF) && !strings.Contains(err.Error(), "unexpected EOF") && !strings.Contains(err.Error(), "failed to read") {
			t.Errorf("Expected an unexpected EOF or read error, but got: %v", err)
		}

	})

	t.Run("InvalidRecordLength", func(t *testing.T) {
		tempDir := t.TempDir()
		opts, walPath := setupWALWithData(t, tempDir, testEntries)

		segmentPath := filepath.Join(walPath, "00000001.wal")

		data, err := os.ReadFile(segmentPath)
		if err != nil {
			t.Fatalf("Failed to read WAL segment file for corruption: %v", err)
		}

		fileHeaderSize := binary.Size(core.FileHeader{})
		firstRecordLen := binary.LittleEndian.Uint32(data[int(fileHeaderSize) : int(fileHeaderSize)+4])
		firstRecordTotalSizeOnDisk := 4 + firstRecordLen + 4

		secondRecordLengthOffset := int(fileHeaderSize) + int(firstRecordTotalSizeOnDisk)

		if len(data) <= secondRecordLengthOffset+4 {
			t.Fatalf("WAL file too small to corrupt second record's length.")
		}

		binary.LittleEndian.PutUint32(data[secondRecordLengthOffset:], 0xFFFFFFFF)
		if err := os.WriteFile(segmentPath, data, 0644); err != nil {
			t.Fatalf("Failed to write corrupted WAL file: %v", err)
		}

		eng, err := NewStorageEngine(opts)
		if err != nil {
			t.Fatal("NewStorageEngine should not have failed on corrupted WAL header, but it did")
		}

		if err = eng.Start(); err == nil {
			t.Fatal("NewStorageEngine should have failed on corrupted WAL header, but it succeeded")
		}
		if !strings.Contains(err.Error(), "exceeds sanity limit") {
			t.Errorf("Expected error to contain 'exceeds sanity limit', but got: %v", err)
		}
	})
}

func TestStorageEngine_Recovery_CorruptedWALWithValidManifest(t *testing.T) {
	tempDir := t.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            1024 * 1024,
		IndexMemtableThreshold:       1024 * 1024,
		CompactionIntervalSeconds:    3600,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		Metrics:                      NewEngineMetrics(false, "wal_manifest_inconsistent_"),
		WALSyncMode:                  core.WALSyncAlways,
		SSTableCompressor:            &compressors.NoCompressionCompressor{},
	}

	engine1, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("Phase 1: NewStorageEngine failed: %v", err)
	}
	if err = engine1.Start(); err != nil {
		t.Fatalf("Phase 1: Failed to start setup engine: %v", err)
	}

	pointInManifest := testDataPoint{"metric.stable", map[string]string{"state": "manifest"}, 1000, 100.0}
	if err := engine1.Put(context.Background(), HelperDataPoint(t, pointInManifest.metric, pointInManifest.tags, pointInManifest.timestamp, map[string]interface{}{"value": pointInManifest.value})); err != nil {
		t.Fatalf("Phase 1: Put failed: %v", err)
	}

	if err := engine1.Close(); err != nil {
		t.Fatalf("Phase 1: Clean close failed: %v", err)
	}

	opts.WALSyncMode = core.WALSyncAlways

	var walDir string
	crashEngine(t, opts, func(e StorageEngineInterface) {
		pointInWAL := testDataPoint{"metric.new", map[string]string{"state": "wal"}, 2000, 200.0}
		if err := e.Put(context.Background(), HelperDataPoint(t, pointInWAL.metric, pointInWAL.tags, pointInWAL.timestamp, map[string]interface{}{"value": pointInWAL.value})); err != nil {
			require.NoError(t, err, "Phase 2: Put failed")
		}
		walDir = e.GetWALPath()
	})

	files, err := os.ReadDir(walDir)
	require.NoError(t, err)
	require.NotEmpty(t, files, "WAL directory should not be empty after phase 2")

	var latestSegmentName string
	var latestIndex uint64
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".wal") {
			idxStr := strings.TrimSuffix(f.Name(), ".wal")
			idx, _ := strconv.ParseUint(idxStr, 10, 64)
			if idx > latestIndex {
				latestIndex = idx
				latestSegmentName = f.Name()
			}
		}
	}
	require.NotEmpty(t, latestSegmentName, "Could not find latest WAL segment to corrupt")

	segmentToCorruptPath := filepath.Join(walDir, latestSegmentName)

	data, err := os.ReadFile(segmentToCorruptPath)
	if err != nil {
		t.Fatalf("Phase 3: Failed to read WAL segment file for corruption: %v", err)
	}
	binary.LittleEndian.PutUint32(data[0:4], 0xDEADBEEF)
	if err := os.WriteFile(segmentToCorruptPath, data, 0644); err != nil {
		t.Fatalf("Phase 3: Failed to write corrupted WAL segment file: %v", err)
	}

	eng3, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("Phase 4:  NewStorageEngine failed: %v", err)
	}
	if err = eng3.Start(); err == nil {
		t.Fatal("Phase 4: Start should have failed due to corrupted WAL, but it succeeded")
	}
	if !strings.Contains(err.Error(), "invalid magic number") {
		t.Errorf("Expected error to contain 'invalid magic number', but got: %v", err)
	}
}

func TestStorageEngine_WALRecovery_TagIndex(t *testing.T) {
	tempDir := t.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            1024 * 1024,
		IndexMemtableThreshold:       1024 * 1024,
		CompactionIntervalSeconds:    3600,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		Metrics:                      NewEngineMetrics(false, "wal_tag_index_"),
		WALSyncMode:                  core.WALSyncAlways,
		WALBatchSize:                 1,
		SSTableCompressor:            &compressors.NoCompressionCompressor{},
	}

	crashEngine(t, opts, func(e StorageEngineInterface) {
		metric1 := "cpu.usage"
		tags1 := map[string]string{"host": "serverA", "region": "us-east"}
		require.NoError(t, e.Put(context.Background(), HelperDataPoint(t, metric1, tags1, 1000, map[string]interface{}{"value": 50.0})))

		metric2 := "memory.free"
		tags2 := map[string]string{"host": "serverB", "region": "us-west"}
		require.NoError(t, e.Put(context.Background(), HelperDataPoint(t, metric2, tags2, 2000, map[string]interface{}{"value": 1024.0})))

		require.NoError(t, e.DeleteSeries(context.Background(), metric1, tags1))
	})

	opts.Metrics = NewEngineMetrics(false, "wal_tag_index_recovery_")
	engine2, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine (engine2) for recovery failed: %v", err)
	}
	if err = engine2.Start(); err != nil {
		t.Fatalf("Failed to start setup engine: %v", err)
	}
	defer engine2.Close()
	metric1 := "cpu.usage"
	tags1 := map[string]string{"host": "serverA", "region": "us-east"}

	retrievedKeys1, err := engine2.GetSeriesByTags(metric1, tags1)
	if err != nil || len(retrievedKeys1) != 0 {
		t.Errorf("Expected GetSeriesByTags to find 0 series for a deleted series after WAL recovery, but got %d keys: %v", len(retrievedKeys1), retrievedKeys1)
	}

	iter, err := engine2.Query(context.Background(), core.QueryParams{Metric: metric1, Tags: tags1, StartTime: 0, EndTime: 2000})
	if err != nil {
		t.Fatalf("Query for deleted series failed after WAL recovery: %v", err)
	}
	defer iter.Close()

	if iter.Next() {
		t.Errorf("Expected Query to return no data for a deleted series after WAL recovery, but it did. Iterator error: %v", iter.Error())
	}

	seriesKey1Str := string(core.EncodeSeriesKeyWithString("cpu.usage", map[string]string{"host": "serverA", "region": "us-east"}))
	retrievedKeys1, err = engine2.GetSeriesByTags(metric1, tags1)
	if err != nil || len(retrievedKeys1) != 0 {
		t.Errorf("Expected series %x to NOT be found in tag index after WAL recovery, got %x", seriesKey1Str, retrievedKeys1)
	}

	metric2 := "memory.free"
	tags2 := map[string]string{"host": "serverB", "region": "us-west"}
	seriesKey2Str := string(core.EncodeSeriesKeyWithString(metric2, tags2))
	retrievedKeys2, err := engine2.GetSeriesByTags(metric2, tags2)
	if err != nil || len(retrievedKeys2) != 1 || retrievedKeys2[0] != seriesKey2Str {
		t.Errorf("Expected series %x to be found in tag index after WAL recovery, got %x", seriesKey2Str, retrievedKeys2)
	}
}

func TestStorageEngine_WALRecovery_RangeTombstones(t *testing.T) {
	tempDir := t.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            1024 * 1024,
		CompactionIntervalSeconds:    3600,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		Metrics:                      NewEngineMetrics(false, "wal_range_tombstone_"),
		WALSyncMode:                  core.WALSyncAlways,
		WALBatchSize:                 1,
		SSTableCompressor:            &compressors.NoCompressionCompressor{},
	}

	crashEngine(t, opts, func(e StorageEngineInterface) {
		metric := "sensor.temp"
		tags := map[string]string{"location": "room1"}
		ts1 := int64(1000)
		ts2 := int64(2000)
		ts3 := int64(3000)

		require.NoError(t, e.Put(context.Background(), HelperDataPoint(t, metric, tags, ts1, map[string]interface{}{"value": 10.0})))
		require.NoError(t, e.Put(context.Background(), HelperDataPoint(t, metric, tags, ts2, map[string]interface{}{"value": 2.0})))
		require.NoError(t, e.Put(context.Background(), HelperDataPoint(t, metric, tags, ts3, map[string]interface{}{"value": 3.0})))
		require.NoError(t, e.DeletesByTimeRange(context.Background(), metric, tags, ts2, ts2))
	})

	opts.Metrics = NewEngineMetrics(false, "wal_range_tombstone_recovery_")
	engine2, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine (engine2) for recovery failed: %v", err)
	}
	if err = engine2.Start(); err != nil {
		t.Fatalf("Failed to start setup engine: %v", err)
	}
	defer engine2.Close()
	metric := "sensor.temp"
	tags := map[string]string{"location": "room1"}

	_, err = engine2.Get(context.Background(), metric, tags, 2000)
	if err != sstable.ErrNotFound {
		t.Errorf("Expected range-deleted point at %d to be ErrNotFound after WAL recovery, got %v", 2000, err)
	}

	retrievedKeys, err := engine2.GetSeriesByTags(metric, tags)
	if err != nil || len(retrievedKeys) != 1 || retrievedKeys[0] != string(core.EncodeSeriesKeyWithString(metric, tags)) {
		t.Errorf("Expected series %s to be found in tag index after WAL recovery (despite range delete), got %v", string(core.EncodeSeriesKeyWithString(metric, tags)), retrievedKeys)
	}

	if _, err = engine2.Get(context.Background(), metric, tags, 1000); err != nil {
		t.Errorf("Expected point at %d to be found, got %v", 1000, err)
	}
	if _, err = engine2.Get(context.Background(), metric, tags, 3000); err != nil {
		t.Errorf("Expected point at %d to be found, got %v", 3000, err)
	}
}
