package engine

import (
	"context"
	"runtime"
	"strconv"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/memtable"
)

// Start begins the monitoring loop. It's a non-blocking call.
// The loop will stop when the provided context is cancelled.
func (eng *storageEngine) startMetrics(ctx context.Context) {
	if !eng.opts.SelfMonitoringEnabled {
		return
	}
	interval := time.Duration(eng.opts.SelfMonitoringIntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = 15 * time.Second // Default interval
	}
	ticker := time.NewTicker(interval)
	eng.logger.Info("Starting self-monitoring", "interval", interval)

	eng.wg.Add(1)
	go func() {
		defer eng.wg.Done()
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				eng.collectAndStoreMetrics(ctx)
			case <-eng.shutdownChan:
				eng.logger.Info("Self-monitoring loop shutting down.")
				return
			}
		}
	}()
}

// collectAndStoreMetrics gathers various runtime and engine metrics and stores them.
func (eng *storageEngine) collectAndStoreMetrics(ctx context.Context) {

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	ts := time.Now().UnixNano()
	prefix := eng.opts.SelfMonitoringPrefix
	if prefix == "" {
		prefix = "__" // Default prefix
	}

	// Pre-allocate with a reasonable capacity to avoid reallocations.
	// 8 runtime + 4 buffer pool + 6 memtable pool + ~7 LSM levels = ~25. 30 is a safe capacity.
	points := make([]core.DataPoint, 0, 30)

	// Helper to create a data point
	newPoint := func(metricName string, value float64, tags map[string]string) core.DataPoint {
		fv, _ := core.NewFieldValuesFromMap(map[string]interface{}{"value": value})
		return core.DataPoint{
			Metric:    prefix + metricName,
			Timestamp: ts,
			Fields:    fv,
			Tags:      tags,
		}
	}

	// Runtime Memory Metrics & Goroutines
	points = append(points, newPoint("runtime.memory.alloc_bytes", float64(memStats.Alloc), nil))
	points = append(points, newPoint("runtime.memory.total_alloc_bytes", float64(memStats.TotalAlloc), nil))
	points = append(points, newPoint("runtime.memory.sys_bytes", float64(memStats.Sys), nil))
	points = append(points, newPoint("runtime.memory.heap_alloc_bytes", float64(memStats.HeapAlloc), nil))
	points = append(points, newPoint("runtime.memory.heap_in_use_bytes", float64(memStats.HeapInuse), nil))
	points = append(points, newPoint("runtime.gc.pause_total_ns", float64(memStats.PauseTotalNs), nil))
	points = append(points, newPoint("runtime.gc.num_gc_total", float64(memStats.NumGC), nil))
	points = append(points, newPoint("runtime.goroutines.count", float64(runtime.NumGoroutine()), nil))

	// Buffer Pool Metrics
	bpHits, bpMisses, bpCreated, bpSize := core.BufferPool.GetMetrics()
	points = append(points, newPoint("buffer_pool.hits_total", float64(bpHits), nil))
	points = append(points, newPoint("buffer_pool.misses_total", float64(bpMisses), nil))
	points = append(points, newPoint("buffer_pool.created_total", float64(bpCreated), nil))
	points = append(points, newPoint("buffer_pool.size_bytes", float64(bpSize), nil))

	// Memtable Key Pool Metrics
	mkpHits, mkpMisses, mkpSize := memtable.KeyPool.GetMetrics()
	points = append(points, newPoint("memtable_key_pool.hits_total", float64(mkpHits), nil))
	points = append(points, newPoint("memtable_key_pool.misses_total", float64(mkpMisses), nil))
	points = append(points, newPoint("memtable_key_pool.size", float64(mkpSize), nil))

	// Memtable Entry Pool Metrics
	mepHits, mepMisses, mepSize := memtable.EntryPool.GetMetrics()
	points = append(points, newPoint("memtable_entry_pool.hits_total", float64(mepHits), nil))
	points = append(points, newPoint("memtable_entry_pool.misses_total", float64(mepMisses), nil))
	points = append(points, newPoint("memtable_entry_pool.size", float64(mepSize), nil))

	// Engine LSM-Tree Metrics
	// This requires the StorageEngineInterface to have a GetLevelTableCounts() method.
	if lsmStats, err := eng.levelsManager.GetLevelTableCounts(); err == nil {
		for level, count := range lsmStats {
			tags := map[string]string{
				"level": strconv.Itoa(level),
			}
			points = append(points, newPoint("lsm.level.sstable.count", float64(count), tags))
		}
	} else {
		eng.logger.Warn("Could not retrieve LSM level stats from engine", "error", err)
	}
	if len(points) > 0 {
		writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		if err := eng.PutBatch(writeCtx, points); err != nil {
			eng.logger.Error("Failed to store self-monitoring metrics", "error", err)
		} else {
			eng.logger.Debug("Successfully stored self-monitoring metrics", "count", len(points))
		}
	}
}
