package engine

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/INLOpen/nexusbase/api/tsdb"
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

	ts := eng.clock.Now().UnixNano()
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
		// If follower, send metrics to leader instead of writing locally
		if eng.replicationMode == "follower" {
			if err := eng.sendMetricsToLeader(points); err != nil {
				eng.logger.Error("Failed to send self-monitoring metrics to leader", "error", err)
			} else {
				eng.logger.Debug("Successfully sent self-monitoring metrics to leader", "count", len(points))
			}
		} else {
			writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := eng.PutBatch(writeCtx, points); err != nil {
				eng.logger.Error("Failed to store self-monitoring metrics", "error", err)
			} else {
				eng.logger.Debug("Successfully stored self-monitoring metrics", "count", len(points))
			}
		}
	}
}

// sendMetricsToLeader is a mock function. Replace with actual implementation (e.g. HTTP/gRPC to leader node)
func (eng *storageEngine) sendMetricsToLeader(points []core.DataPoint) error {
	// ตัวอย่าง: ใช้ gRPC client ส่ง metrics ไป leader
	// ต้อง import tsdb proto client และ google.golang.org/grpc
	// สมมติ leader address อยู่ใน eng.opts.LeaderAddress (เช่น "localhost:51000")
	leaderAddr := eng.opts.LeaderAddress
	if leaderAddr == "" {
		return fmt.Errorf("Leader address not configured")
	}

	// สร้าง gRPC connection
	conn, err := grpc.Dial(leaderAddr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(3*time.Second))
	if err != nil {
		return fmt.Errorf("failed to connect to leader: %w", err)
	}
	defer conn.Close()

	client := tsdb.NewTSDBServiceClient(conn)

	// แปลง core.DataPoint เป็น tsdb.PutRequest
	batch := &tsdb.PutBatchRequest{}
	for _, p := range points {
		fieldsStruct, err := structpb.NewStruct(p.Fields.ToMap())
		if err != nil {
			return fmt.Errorf("failed to convert fields to structpb: %w", err)
		}
		req := &tsdb.PutRequest{
			Metric:    p.Metric,
			Tags:      p.Tags,
			Timestamp: p.Timestamp,
			Fields:    fieldsStruct,
		}
		batch.Points = append(batch.Points, req)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.PutBatch(ctx, batch)
	if err != nil {
		return fmt.Errorf("failed to send metrics to leader via gRPC: %w", err)
	}
	eng.logger.Info("Sent self-monitoring metrics to leader via gRPC", "count", len(points))
	return nil
}
