package engine2

import (
	"context"
	"fmt"
	"path/filepath"

	// strconv and strings were used in previous implementation; keep none now

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/indexer"
	pb "github.com/INLOpen/nexusbase/replication/proto"
	"github.com/INLOpen/nexusbase/snapshot"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexusbase/wal"
	"github.com/INLOpen/nexuscore/utils/clock"
)

// Ensure Engine2 implements the StorageEngineInterface at compile time.
var _ engine.StorageEngineInterface = (*Engine2Adapter)(nil)

// Engine2Adapter adapts the lightweight engine2 implementation to the
// repository's StorageEngineInterface. Methods are minimal stubs for now
// and will be incrementally implemented as core features are added.
type Engine2Adapter struct {
	*Engine2
}

// NewEngine2Adapter wraps an existing Engine2.
func NewEngine2Adapter(e *Engine2) *Engine2Adapter {
	return &Engine2Adapter{Engine2: e}
}

func (a *Engine2Adapter) GetNextSSTableID() uint64 { return 0 }

// Data Manipulation
func (a *Engine2Adapter) Put(ctx context.Context, point core.DataPoint) error {
	if a.wal == nil || a.mem == nil {
		return fmt.Errorf("engine2 not initialized")
	}
	if err := a.wal.Append(&point); err != nil {
		return fmt.Errorf("wal append failed: %w", err)
	}
	a.mem.Put(&point)
	return nil
}
func (a *Engine2Adapter) PutBatch(ctx context.Context, points []core.DataPoint) error {
	if a.wal == nil || a.mem == nil {
		return fmt.Errorf("engine2 not initialized")
	}
	for i := range points {
		p := points[i]
		if err := a.wal.Append(&p); err != nil {
			return fmt.Errorf("wal append failed: %w", err)
		}
		a.mem.Put(&p)
	}
	return nil
}
func (a *Engine2Adapter) Get(ctx context.Context, metric string, tags map[string]string, timestamp int64) (core.FieldValues, error) {
	if a.mem == nil {
		return nil, fmt.Errorf("engine2 not initialized")
	}
	if fv, ok := a.mem.Get(metric, tags, timestamp); ok {
		if fv == nil || len(fv) == 0 {
			return nil, sstable.ErrNotFound
		}
		return fv, nil
	}
	return nil, sstable.ErrNotFound
}
func (a *Engine2Adapter) Delete(ctx context.Context, metric string, tags map[string]string, timestamp int64) error {
	if a.wal == nil || a.mem == nil {
		return fmt.Errorf("engine2 not initialized")
	}
	// append tombstone entry
	dp := core.DataPoint{Metric: metric, Tags: tags, Timestamp: timestamp, Fields: nil}
	if err := a.wal.Append(&dp); err != nil {
		return fmt.Errorf("wal append failed: %w", err)
	}
	a.mem.Delete(metric, tags, timestamp)
	return nil
}
func (a *Engine2Adapter) DeleteSeries(ctx context.Context, metric string, tags map[string]string) error {
	if a.wal == nil || a.mem == nil {
		return fmt.Errorf("engine2 not initialized")
	}
	// represent series tombstone with timestamp -1
	dp := core.DataPoint{Metric: metric, Tags: tags, Timestamp: -1, Fields: nil}
	if err := a.wal.Append(&dp); err != nil {
		return fmt.Errorf("wal append failed: %w", err)
	}
	a.mem.DeleteSeries(metric, tags)
	return nil
}
func (a *Engine2Adapter) DeletesByTimeRange(ctx context.Context, metric string, tags map[string]string, startTime, endTime int64) error {
	if a.wal == nil || a.mem == nil {
		return fmt.Errorf("engine2 not initialized")
	}
	for ts := startTime; ts <= endTime; ts++ {
		// append tombstone for each timestamp in range (simple implementation)
		dp := core.DataPoint{Metric: metric, Tags: tags, Timestamp: ts, Fields: nil}
		if err := a.wal.Append(&dp); err != nil {
			return fmt.Errorf("wal append failed: %w", err)
		}
		a.mem.Delete(metric, tags, ts)
	}
	return nil
}

// Querying
func (a *Engine2Adapter) Query(ctx context.Context, params core.QueryParams) (core.QueryResultIteratorInterface, error) {
	if a.mem == nil {
		return nil, fmt.Errorf("engine2 not initialized")
	}
	return getPooledIterator(a.mem, params), nil
}
func (a *Engine2Adapter) GetSeriesByTags(metric string, tags map[string]string) ([]string, error) {
	return nil, fmt.Errorf("GetSeriesByTags not implemented in engine2 adapter")
}

// Introspection
func (a *Engine2Adapter) GetMetrics() ([]string, error) { return nil, nil }
func (a *Engine2Adapter) GetTagsForMetric(metric string) ([]string, error) {
	return nil, nil
}
func (a *Engine2Adapter) GetTagValues(metric, tagKey string) ([]string, error) {
	return nil, nil
}

// Administration & Maintenance
func (a *Engine2Adapter) ForceFlush(ctx context.Context, wait bool) error {
	if a.mem == nil {
		return fmt.Errorf("engine2 not initialized")
	}
	a.mem.Flush()
	return nil
}
func (a *Engine2Adapter) TriggerCompaction() {}
func (a *Engine2Adapter) CreateIncrementalSnapshot(snapshotsBaseDir string) error {
	return fmt.Errorf("CreateIncrementalSnapshot not implemented")
}
func (a *Engine2Adapter) VerifyDataConsistency() []error { return nil }

func (a *Engine2Adapter) CreateSnapshot(ctx context.Context) (string, error) {
	// For now, create a placeholder snapshot path under data root.
	snap := filepath.Join(a.dataRoot, "snapshot-placeholder")
	return snap, nil
}
func (a *Engine2Adapter) RestoreFromSnapshot(ctx context.Context, path string, overwrite bool) error {
	return fmt.Errorf("RestoreFromSnapshot not implemented")
}

// Replication
func (a *Engine2Adapter) ApplyReplicatedEntry(ctx context.Context, entry *pb.WALEntry) error {
	return fmt.Errorf("ApplyReplicatedEntry not implemented")
}
func (a *Engine2Adapter) GetLatestAppliedSeqNum() uint64 { return 0 }
func (a *Engine2Adapter) ReplaceWithSnapshot(snapshotDir string) error {
	return fmt.Errorf("ReplaceWithSnapshot not implemented")
}

func (a *Engine2Adapter) CleanupEngine() {}
func (a *Engine2Adapter) Start() error   { return nil }
func (a *Engine2Adapter) Close() error {
	if a.wal != nil {
		if err := a.wal.Close(); err != nil {
			return err
		}
	}
	// drop memtable reference
	a.mem = nil
	return nil
}

// Introspection & Utilities
func (a *Engine2Adapter) GetPubSub() (engine.PubSubInterface, error) {
	return nil, fmt.Errorf("GetPubSub not implemented")
}
func (a *Engine2Adapter) GetSnapshotsBaseDir() string                   { return filepath.Join(a.dataRoot, "snapshots") }
func (a *Engine2Adapter) Metrics() (*engine.EngineMetrics, error)       { return nil, nil }
func (a *Engine2Adapter) GetHookManager() hooks.HookManager             { return nil }
func (a *Engine2Adapter) GetDLQDir() string                             { return filepath.Join(a.dataRoot, "dlq") }
func (a *Engine2Adapter) GetDataDir() string                            { return a.dataRoot }
func (a *Engine2Adapter) GetWALPath() string                            { return filepath.Join(a.dataRoot, "wal") }
func (a *Engine2Adapter) GetClock() clock.Clock                         { return clock.SystemClockDefault }
func (a *Engine2Adapter) GetWAL() wal.WALInterface                      { return nil }
func (a *Engine2Adapter) GetStringStore() indexer.StringStoreInterface  { return nil }
func (a *Engine2Adapter) GetSnapshotManager() snapshot.ManagerInterface { return nil }

func (a *Engine2Adapter) GetSequenceNumber() uint64 { return 0 }
