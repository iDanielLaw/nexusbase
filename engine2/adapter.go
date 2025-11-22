package engine2

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	// strconv and strings were used in previous implementation; keep none now

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/indexer"
	pb "github.com/INLOpen/nexusbase/replication/proto"
	"github.com/INLOpen/nexusbase/snapshot"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexusbase/sys"
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
	// simple monotonic sstable id allocator
	nextSSTableID uint64
	// string store for ID mapping (persisted)
	stringStore indexer.StringStoreInterface
	// manifest manager for discovered SSTables
	manifestMgr *ManifestManager
	// optional hook manager passed from hosting engine/server
	hookManager hooks.HookManager
}

// NewEngine2Adapter wraps an existing Engine2.
func NewEngine2Adapter(e *Engine2) *Engine2Adapter {
	return NewEngine2AdapterWithHooks(e, nil)
}

// NewEngine2AdapterWithHooks constructs an adapter and allows injecting a
// real hooks.HookManager to be passed into the persisted StringStore.
func NewEngine2AdapterWithHooks(e *Engine2, hm hooks.HookManager) *Engine2Adapter {
	a := &Engine2Adapter{
		Engine2:       e,
		nextSSTableID: uint64(time.Now().UnixNano()),
		stringStore:   indexer.NewStringStore(slog.Default(), hm),
		hookManager:   hm,
	}
	// initialize manifest manager (best-effort)
	if e != nil {
		manifestPath := filepath.Join(e.GetDataRoot(), "sstables", "manifest.json")
		if mgr, err := NewManifestManager(manifestPath); err == nil {
			a.manifestMgr = mgr
		}
		// try to load string store from data root
		if ss := a.stringStore; ss != nil {
			_ = ss.LoadFromFile(e.GetDataRoot())
		}
	}
	return a
}

func (a *Engine2Adapter) GetNextSSTableID() uint64 {
	return atomic.AddUint64(&a.nextSSTableID, 1)
}

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

	// Create sstable directory
	sstDir := filepath.Join(a.dataRoot, "sstables")

	// Gather entries from memtable into an in-memory slice (so we can estimate keys)
	type encEntry struct {
		key       []byte
		val       []byte
		entryType core.EntryType
	}
	var entries []encEntry
	a.mem.mu.RLock()
	for metric, tagMap := range a.mem.data {
		for tagKey, tsMap := range tagMap {
			tags := parseTagsFromKey(tagKey)
			// convert metric and tags to IDs using the string store
			metricID, _ := a.stringStore.GetOrCreateID(metric)
			// build encoded tag pairs
			var pairs []core.EncodedSeriesTagPair
			for k, v := range tags {
				kid, _ := a.stringStore.GetOrCreateID(k)
				vid, _ := a.stringStore.GetOrCreateID(v)
				pairs = append(pairs, core.EncodedSeriesTagPair{KeyID: kid, ValueID: vid})
			}
			// sort by KeyID for canonical ordering
			sort.Slice(pairs, func(i, j int) bool { return pairs[i].KeyID < pairs[j].KeyID })

			for ts, fv := range tsMap {
				var e encEntry
				e.key = core.EncodeTSDBKey(metricID, pairs, ts)
				if fv == nil || len(fv) == 0 {
					e.entryType = core.EntryTypeDelete
					e.val = nil
				} else {
					vb, encErr := fv.Encode()
					if encErr != nil {
						a.mem.mu.RUnlock()
						return fmt.Errorf("failed to encode FieldValues during flush preparation: %w", encErr)
					}
					e.entryType = core.EntryTypePutEvent
					e.val = vb
				}
				entries = append(entries, e)
			}
		}
	}
	a.mem.mu.RUnlock()

	if len(entries) == 0 {
		// Nothing to flush
		return nil
	}

	id := a.GetNextSSTableID()
	estimatedKeys := uint64(len(entries))

	writerOpts := core.SSTableWriterOptions{
		DataDir:                      sstDir,
		ID:                           id,
		EstimatedKeys:                estimatedKeys,
		BloomFilterFalsePositiveRate: 0.01,
		BlockSize:                    32 * 1024,
		Compressor:                   &compressors.NoCompressionCompressor{},
		Logger:                       nil,
	}

	writer, err := sstable.NewSSTableWriter(writerOpts)
	if err != nil {
		return fmt.Errorf("failed to create sstable writer: %w", err)
	}

	// Write all entries
	for _, e := range entries {
		if addErr := writer.Add(e.key, e.val, e.entryType, 0); addErr != nil {
			writer.Abort()
			return fmt.Errorf("failed to add entry to sstable: %w", addErr)
		}
	}

	if err := writer.Finish(); err != nil {
		writer.Abort()
		return fmt.Errorf("failed to finish sstable writer: %w", err)
	}

	// Validate by loading the SSTable
	loadOpts := sstable.LoadSSTableOptions{FilePath: writer.FilePath(), ID: id}
	_, loadErr := sstable.LoadSSTable(loadOpts)
	if loadErr != nil {
		// remove corrupted file
		_ = sys.Remove(writer.FilePath())
		return fmt.Errorf("failed to load newly created sstable %s: %w", writer.FilePath(), loadErr)
	}

	// Append manifest entry
	manifestPath := filepath.Join(sstDir, "manifest.json")
	entry := SSTableManifestEntry{
		ID:        id,
		FilePath:  writer.FilePath(),
		KeyCount:  uint64(len(entries)),
		CreatedAt: time.Now().UTC(),
	}
	// Persist manifest entry and update in-memory manager if available.
	if a.manifestMgr != nil {
		if err := a.manifestMgr.AddEntry(entry); err != nil {
			return fmt.Errorf("failed to persist manifest entry: %w", err)
		}
	} else {
		if err := AppendManifestEntry(manifestPath, entry); err != nil {
			return fmt.Errorf("failed to append sstable manifest: %w", err)
		}
	}

	// Success — clear memtable now
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
func (a *Engine2Adapter) Start() error {
	// Ensure manifest manager is present and loaded.
	if a.manifestMgr == nil && a.Engine2 != nil {
		manifestPath := filepath.Join(a.Engine2.GetDataRoot(), "sstables", "manifest.json")
		if mgr, err := NewManifestManager(manifestPath); err == nil {
			a.manifestMgr = mgr
		} else {
			return err
		}
	}
	if a.manifestMgr != nil {
		if err := a.manifestMgr.Reload(); err != nil {
			return err
		}
	}
	return nil
}
func (a *Engine2Adapter) Close() error {
	if a.wal != nil {
		if err := a.wal.Close(); err != nil {
			return err
		}
	}
	// close string store if present
	if a.stringStore != nil {
		_ = a.stringStore.Sync()
		_ = a.stringStore.Close()
	}
	// close manifest manager
	if a.manifestMgr != nil {
		a.manifestMgr.Close()
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
func (a *Engine2Adapter) GetHookManager() hooks.HookManager             { return a.hookManager }
func (a *Engine2Adapter) GetDLQDir() string                             { return filepath.Join(a.dataRoot, "dlq") }
func (a *Engine2Adapter) GetDataDir() string                            { return a.dataRoot }
func (a *Engine2Adapter) GetWALPath() string                            { return filepath.Join(a.dataRoot, "wal") }
func (a *Engine2Adapter) GetClock() clock.Clock                         { return clock.SystemClockDefault }
func (a *Engine2Adapter) GetWAL() wal.WALInterface                      { return nil }
func (a *Engine2Adapter) GetStringStore() indexer.StringStoreInterface  { return a.stringStore }
func (a *Engine2Adapter) GetSnapshotManager() snapshot.ManagerInterface { return nil }

func (a *Engine2Adapter) GetSequenceNumber() uint64 { return 0 }

// GetKnownSSTables returns the manifest entries currently known by the engine2 adapter.
func (a *Engine2Adapter) GetKnownSSTables() ([]SSTableManifestEntry, error) {
	if a.manifestMgr == nil {
		return nil, nil
	}
	return a.manifestMgr.ListEntries(), nil
}
