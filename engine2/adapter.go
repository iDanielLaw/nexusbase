package engine2

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/indexer"
	"github.com/INLOpen/nexusbase/internal"
	"github.com/INLOpen/nexusbase/levels"
	"github.com/INLOpen/nexusbase/memtable"
	pb "github.com/INLOpen/nexusbase/replication/proto"
	"github.com/INLOpen/nexusbase/snapshot"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexusbase/sys"
	"github.com/INLOpen/nexusbase/wal"
	"github.com/INLOpen/nexuscore/utils/clock"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
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
	// leader WAL (from wal package) exposed to replication/snapshot subsystems
	leaderWal wal.WALInterface
	// sequence number tracking for applied replicated entries
	sequenceNumber atomic.Uint64
	// series ID store and tag index manager for secondary index support
	seriesIDStore     indexer.SeriesIDStoreInterface
	tagIndexManager   indexer.TagIndexManagerInterface
	tagIndexManagerMu sync.RWMutex
	// levels manager used by snapshot manager (best-effort)
	levelsMgr levels.Manager
	// simple startup flag
	started atomic.Bool
	// cached snapshot manager
	snapshotMgr snapshot.ManagerInterface
	// adapter lock used by snapshot provider methods
	providerLock sync.Mutex
	// memtable snapshot/flush configuration
	memtableThreshold int64
	clk               clock.Clock
	// basic active series tracking (in-memory)
	activeSeries   map[string]struct{}
	activeSeriesMu sync.RWMutex
	// deleted series tracking used by tag index manager
	deletedSeries   map[string]uint64
	deletedSeriesMu sync.RWMutex
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
		activeSeries:  make(map[string]struct{}),
		deletedSeries: make(map[string]uint64),
	}
	// defaults for memtable snapshot conversion
	a.memtableThreshold = 1 << 30
	a.clk = clock.SystemClockDefault
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
		// initialize series id store
		a.seriesIDStore = indexer.NewSeriesIDStore(slog.Default(), hm)
		if a.seriesIDStore != nil {
			_ = a.seriesIDStore.LoadFromFile(e.GetDataRoot())
		}
		// initialize tag index manager (best-effort)
		deps := &indexer.TagIndexDependencies{
			StringStore:     a.stringStore,
			SeriesIDStore:   a.seriesIDStore,
			DeletedSeries:   a.deletedSeries,
			DeletedSeriesMu: &a.deletedSeriesMu,
			SSTNextID:       a.GetNextSSTableID,
		}
		timOpts := indexer.TagIndexManagerOptions{DataDir: e.GetDataRoot()}
		if tim, err := indexer.NewTagIndexManager(timOpts, deps, slog.Default(), nil); err == nil {
			a.tagIndexManager = tim
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
	// Also append a core.WALEntry to leader WAL so replication can stream it.
	if a.leaderWal != nil {
		if entry, encErr := a.encodeDataPointToWALEntry(&point); encErr == nil {
			_ = a.leaderWal.Append(*entry)
		}
	}
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
		if a.leaderWal != nil {
			if entry, encErr := a.encodeDataPointToWALEntry(&p); encErr == nil {
				_ = a.leaderWal.Append(*entry)
			}
		}
	}
	return nil
}
func (a *Engine2Adapter) Get(ctx context.Context, metric string, tags map[string]string, timestamp int64) (core.FieldValues, error) {
	if a.mem == nil {
		return nil, fmt.Errorf("engine2 not initialized")
	}
	if fv, ok := a.mem.Get(metric, tags, timestamp); ok {
		if len(fv) == 0 {
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
	if a.leaderWal != nil {
		// build delete WALEntry
		if entry, encErr := a.encodeDataPointToWALEntry(&dp); encErr == nil {
			// mark as delete event
			entry.EntryType = core.EntryTypeDelete
			_ = a.leaderWal.Append(*entry)
		}
	}
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
	if a.leaderWal != nil {
		if entry, encErr := a.encodeDataPointToWALEntry(&dp); encErr == nil {
			entry.EntryType = core.EntryTypeDelete
			_ = a.leaderWal.Append(*entry)
		}
	}
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
		if a.leaderWal != nil {
			if entry, encErr := a.encodeDataPointToWALEntry(&dp); encErr == nil {
				entry.EntryType = core.EntryTypeDelete
				_ = a.leaderWal.Append(*entry)
			}
		}
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
	if a.mem == nil {
		return fmt.Errorf("engine2 not initialized")
	}

	switch entry.GetEntryType() {
	case pb.WALEntry_PUT_EVENT:
		// convert fields
		var fieldsMap map[string]interface{}
		if entry.GetFields() != nil {
			fieldsMap = entry.GetFields().AsMap()
		}
		fv, err := core.NewFieldValuesFromMap(fieldsMap)
		if err != nil {
			return fmt.Errorf("failed to decode replicated fields: %w", err)
		}

		// Ensure dictionary IDs exist for metric and tags so indexers can use them.
		metric := entry.GetMetric()
		if a.stringStore != nil {
			_, _ = a.stringStore.GetOrCreateID(metric)
		}

		// build encoded tag pairs
		var pairs []core.EncodedSeriesTagPair
		for k, v := range entry.GetTags() {
			if a.stringStore != nil {
				kid, _ := a.stringStore.GetOrCreateID(k)
				vid, _ := a.stringStore.GetOrCreateID(v)
				pairs = append(pairs, core.EncodedSeriesTagPair{KeyID: kid, ValueID: vid})
			}
		}
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].KeyID < pairs[j].KeyID })

		// register series in active map and seriesID store
		if a.stringStore != nil {
			metricID, _ := a.stringStore.GetOrCreateID(metric)
			seriesKey := core.EncodeSeriesKey(metricID, pairs)
			seriesKeyStr := string(seriesKey)
			a.addActiveSeries(seriesKeyStr)
			if a.seriesIDStore != nil {
				sid, err := a.seriesIDStore.GetOrCreateID(seriesKeyStr)
				if err != nil {
					return fmt.Errorf("failed to persist series id for replicated entry: %w", err)
				}
				if a.tagIndexManager != nil {
					_ = a.tagIndexManager.AddEncoded(sid, pairs)
				}
			}
		}

		dp := core.DataPoint{Metric: entry.GetMetric(), Tags: entry.GetTags(), Timestamp: entry.GetTimestamp(), Fields: fv}
		a.mem.Put(&dp)

	case pb.WALEntry_DELETE_SERIES:
		// For replicated series delete, update memtable and index state without writing to local WAL.
		if a.stringStore != nil {
			_, _ = a.stringStore.GetOrCreateID(entry.GetMetric())
		}
		var pairs []core.EncodedSeriesTagPair
		for k, v := range entry.GetTags() {
			if a.stringStore != nil {
				kid, _ := a.stringStore.GetOrCreateID(k)
				vid, _ := a.stringStore.GetOrCreateID(v)
				pairs = append(pairs, core.EncodedSeriesTagPair{KeyID: kid, ValueID: vid})
			}
		}
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].KeyID < pairs[j].KeyID })
		if a.stringStore != nil && a.seriesIDStore != nil {
			metricID, _ := a.stringStore.GetOrCreateID(entry.GetMetric())
			seriesKey := core.EncodeSeriesKey(metricID, pairs)
			seriesKeyStr := string(seriesKey)
			a.deletedSeriesMu.Lock()
			a.deletedSeries[seriesKeyStr] = entry.GetSequenceNumber()
			a.deletedSeriesMu.Unlock()
			if sid, ok := a.seriesIDStore.GetID(seriesKeyStr); ok {
				if a.tagIndexManager != nil {
					a.tagIndexManager.RemoveSeries(sid)
				}
			}
		}
		a.mem.DeleteSeries(entry.GetMetric(), entry.GetTags())

	case pb.WALEntry_DELETE_RANGE:
		// Apply range delete directly to memtable without writing to WAL.
		for ts := entry.GetStartTime(); ts <= entry.GetEndTime(); ts++ {
			a.mem.Delete(entry.GetMetric(), entry.GetTags(), ts)
		}

	default:
		return fmt.Errorf("unknown replicated entry type: %v", entry.GetEntryType())
	}

	// update applied sequence number
	a.sequenceNumber.Store(entry.GetSequenceNumber())
	return nil
}
func (a *Engine2Adapter) GetLatestAppliedSeqNum() uint64 { return a.sequenceNumber.Load() }

// Snapshot provider helpers
func (a *Engine2Adapter) CheckStarted() error {
	if a.started.Load() {
		return nil
	}
	return fmt.Errorf("engine2 not started")
}

func (a *Engine2Adapter) GetLogger() *slog.Logger { return slog.Default() }
func (a *Engine2Adapter) GetTracer() trace.Tracer { return noop.NewTracerProvider().Tracer("engine2") }

func (a *Engine2Adapter) GetLevelsManager() levels.Manager {
	if a.levelsMgr == nil {
		if lm, err := levels.NewLevelsManager(7, 4, 1024, noop.NewTracerProvider().Tracer("levels"), levels.PickOldest, 1.5, 1.0); err == nil && lm != nil {
			a.levelsMgr = lm
		} else {
			// Fallback: provide a minimal no-op implementation so snapshot manager
			// can proceed without a fully-featured levels manager.
			a.levelsMgr = &noopLevels{}
		}
	}
	return a.levelsMgr
}

// noopLevels is a minimal no-op implementation of levels.Manager used as a
// fallback when creating a real LevelsManager fails. It returns empty state
// and no-ops for mutations.
type noopLevels struct{}

func (n *noopLevels) GetSSTablesForRead() ([]*levels.LevelState, func()) {
	return []*levels.LevelState{}, func() {}
}
func (n *noopLevels) AddL0Table(table *sstable.SSTable) error                     { return nil }
func (n *noopLevels) AddTablesToLevel(level int, tables []*sstable.SSTable) error { return nil }
func (n *noopLevels) AddTableToLevel(level int, table *sstable.SSTable) error     { return nil }
func (n *noopLevels) Close() error                                                { return nil }
func (n *noopLevels) VerifyConsistency() []error                                  { return nil }
func (n *noopLevels) GetTablesForLevel(level int) []*sstable.SSTable              { return nil }
func (n *noopLevels) GetTotalSizeForLevel(level int) int64                        { return 0 }
func (n *noopLevels) MaxLevels() int                                              { return 0 }
func (n *noopLevels) NeedsL0Compaction(maxL0Files int, l0CompactionTriggerSize int64) bool {
	return false
}
func (n *noopLevels) NeedsIntraL0Compaction(triggerFileCount int, maxFileSizeBytes int64) bool {
	return false
}
func (n *noopLevels) PickIntraL0CompactionCandidates(triggerFileCount int, maxFileSizeBytes int64) []*sstable.SSTable {
	return nil
}
func (n *noopLevels) NeedsLevelNCompaction(levelN int, multiplier int) bool        { return false }
func (n *noopLevels) PickCompactionCandidateForLevelN(levelN int) *sstable.SSTable { return nil }
func (n *noopLevels) GetOverlappingTables(level int, minKey, maxKey []byte) []*sstable.SSTable {
	return nil
}
func (n *noopLevels) ApplyCompactionResults(sourceLevel, targetLevel int, newTables, oldTables []*sstable.SSTable) error {
	return nil
}
func (n *noopLevels) RemoveTables(level int, tablesToRemove []uint64) error { return nil }
func (n *noopLevels) GetLevels() []*levels.LevelState                       { return nil }
func (n *noopLevels) GetTotalTableCount() int                               { return 0 }
func (n *noopLevels) GetLevelForTable(tableID uint64) (int, bool)           { return -1, false }
func (n *noopLevels) GetLevelTableCounts() (map[int]int, error)             { return nil, nil }

func (a *Engine2Adapter) GetTagIndexManager() indexer.TagIndexManagerInterface {
	return a.tagIndexManager
}

// noopWAL is a minimal no-op WAL implementation returned when no leader WAL
// exists. It ensures callers can safely call Path() and ActiveSegmentIndex()
// without nil pointer dereferences.
type noopWAL struct{}

func (n *noopWAL) AppendBatch(entries []core.WALEntry) error { return nil }
func (n *noopWAL) Append(entry core.WALEntry) error          { return nil }
func (n *noopWAL) Sync() error                               { return nil }
func (n *noopWAL) Purge(upToIndex uint64) error              { return nil }
func (n *noopWAL) Close() error                              { return nil }
func (n *noopWAL) Path() string                              { return "" }
func (n *noopWAL) SetTestingOnlyInjectCloseError(err error)  {}
func (n *noopWAL) ActiveSegmentIndex() uint64                { return 0 }
func (n *noopWAL) Rotate() error                             { return nil }
func (n *noopWAL) NewStreamReader(fromSeqNum uint64) (wal.StreamReader, error) {
	return nil, fmt.Errorf("noop wal has no stream reader")
}

// copyDir is a simple, best-effort directory copy helper used as a fallback
// when os.Rename fails across devices. It copies files and creates directories
// preserving file permissions where possible.
func copyDir(src string, dst string) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		// copy file
		r, err := os.Open(path)
		if err != nil {
			return err
		}
		defer r.Close()
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		w, err := os.Create(target)
		if err != nil {
			return err
		}
		if _, err := io.Copy(w, r); err != nil {
			w.Close()
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
		return os.Chmod(target, info.Mode())
	})
}

// privateManagerAdapter adapts an object that exposes GetLogFilePath() to the
// internal.PrivateManagerStore interface expected by the snapshot manager.
type privateManagerAdapter struct {
	pathFunc func() string
}

func (p *privateManagerAdapter) GetLogFilePath() string {
	if p == nil || p.pathFunc == nil {
		return ""
	}
	return p.pathFunc()
}

func (a *Engine2Adapter) GetPrivateStringStore() internal.PrivateManagerStore {
	if a.stringStore == nil {
		return nil
	}
	if v, ok := a.stringStore.(interface{ GetLogFilePath() string }); ok {
		return &privateManagerAdapter{pathFunc: v.GetLogFilePath}
	}
	return nil
}

func (a *Engine2Adapter) GetPrivateSeriesIDStore() internal.PrivateManagerStore {
	if a.seriesIDStore == nil {
		return nil
	}
	if v, ok := a.seriesIDStore.(interface{ GetLogFilePath() string }); ok {
		return &privateManagerAdapter{pathFunc: v.GetLogFilePath}
	}
	return nil
}
func (a *Engine2Adapter) GetSSTableCompressionType() string { return "none" }

func (a *Engine2Adapter) Lock()   { a.providerLock.Lock() }
func (a *Engine2Adapter) Unlock() { a.providerLock.Unlock() }

func (a *Engine2Adapter) GetMemtablesForFlush() (memtables []*memtable.Memtable, newMemtable *memtable.Memtable) {
	// Called while provider lock is held. Swap engine2's active memtable with
	// a fresh one and return a top-level memtable snapshot suitable for the
	// snapshot manager.
	if a.Engine2 == nil {
		return nil, nil
	}

	// Swap engine2's memtable under its own lock to keep invariants.
	a.Engine2.mu.Lock()
	old := a.Engine2.mem
	a.Engine2.mem = NewMemtable()
	a.Engine2.mu.Unlock()

	if old == nil {
		return nil, nil
	}

	// Convert engine2.Memtable -> top-level memtable.Memtable
	// Use configured threshold and clock so the resulting memtable doesn't
	// immediately trigger IsFull during snapshot operations.
	top := memtable.NewMemtable(a.memtableThreshold, a.clk)

	old.mu.RLock()
	for metric, tagMap := range old.data {
		for tagKey, tsMap := range tagMap {
			tags := parseTagsFromKey(tagKey)
			metricID, _ := a.stringStore.GetOrCreateID(metric)
			var pairs []core.EncodedSeriesTagPair
			for k, v := range tags {
				kid, _ := a.stringStore.GetOrCreateID(k)
				vid, _ := a.stringStore.GetOrCreateID(v)
				pairs = append(pairs, core.EncodedSeriesTagPair{KeyID: kid, ValueID: vid})
			}
			sort.Slice(pairs, func(i, j int) bool { return pairs[i].KeyID < pairs[j].KeyID })

			for ts, fv := range tsMap {
				key := core.EncodeTSDBKey(metricID, pairs, ts)
				if len(fv) == 0 {
					// tombstone
					_ = top.Put(key, nil, core.EntryTypeDelete, 0)
				} else {
					vb, encErr := fv.Encode()
					if encErr != nil {
						old.mu.RUnlock()
						return nil, nil
					}
					_ = top.Put(key, vb, core.EntryTypePutEvent, 0)
				}
			}
		}
	}
	old.mu.RUnlock()

	return []*memtable.Memtable{top}, nil
}

func (a *Engine2Adapter) FlushMemtableToL0(mem *memtable.Memtable, parentCtx context.Context) error {
	if mem == nil {
		return nil
	}

	// Use memtable's FlushToSSTable if available: create a writer and delegate.
	id := a.GetNextSSTableID()
	sstDir := filepath.Join(a.dataRoot, "sstables")
	writerOpts := core.SSTableWriterOptions{
		DataDir:                      sstDir,
		ID:                           id,
		EstimatedKeys:                0, // best-effort, memtable will write whatever it has
		BloomFilterFalsePositiveRate: 0.01,
		BlockSize:                    32 * 1024,
		Compressor:                   &compressors.NoCompressionCompressor{},
		Logger:                       nil,
	}
	writer, err := sstable.NewSSTableWriter(writerOpts)
	if err != nil {
		return fmt.Errorf("failed to create sstable writer: %w", err)
	}

	// Use mem.FlushToSSTable when available on top-level memtable implementation.
	if err := mem.FlushToSSTable(writer); err != nil {
		writer.Abort()
		return fmt.Errorf("failed to flush memtable to sstable: %w", err)
	}
	if err := writer.Finish(); err != nil {
		writer.Abort()
		return fmt.Errorf("failed to finish sstable writer: %w", err)
	}

	// Load SSTable to get *sstable.SSTable for levels manager
	loadOpts := sstable.LoadSSTableOptions{FilePath: writer.FilePath(), ID: id}
	sst, loadErr := sstable.LoadSSTable(loadOpts)
	if loadErr != nil {
		_ = sys.Remove(writer.FilePath())
		return fmt.Errorf("failed to load newly created sstable %s: %w", writer.FilePath(), loadErr)
	}

	// Persist manifest entry
	entry := SSTableManifestEntry{ID: id, FilePath: writer.FilePath(), KeyCount: sst.KeyCount(), CreatedAt: time.Now().UTC()}
	manifestPath := filepath.Join(sstDir, "manifest.json")
	if a.manifestMgr != nil {
		if err := a.manifestMgr.AddEntry(entry); err != nil {
			return fmt.Errorf("failed to persist manifest entry: %w", err)
		}
	} else {
		if err := AppendManifestEntry(manifestPath, entry); err != nil {
			return fmt.Errorf("failed to append sstable manifest: %w", err)
		}
	}

	if a.GetLevelsManager() != nil && sst != nil {
		if err := a.GetLevelsManager().AddL0Table(sst); err != nil {
			slog.Default().Warn("levels manager AddL0Table failed", "err", err)
		}
		// Log successful registration for debugging snapshot inclusion.
		slog.Default().Info("registered SSTable with levels manager", "id", sst.ID(), "path", sst.FilePath())
	}

	return nil
}

func (a *Engine2Adapter) GetDeletedSeries() map[string]uint64 {
	a.deletedSeriesMu.RLock()
	defer a.deletedSeriesMu.RUnlock()
	out := make(map[string]uint64, len(a.deletedSeries))
	for k, v := range a.deletedSeries {
		out[k] = v
	}
	return out
}

func (a *Engine2Adapter) GetRangeTombstones() map[string][]core.RangeTombstone {
	return map[string][]core.RangeTombstone{}
}

func (a *Engine2Adapter) GetSnapshotManager() snapshot.ManagerInterface {
	if a.snapshotMgr == nil {
		a.snapshotMgr = snapshot.NewManager(a)
	}
	return a.snapshotMgr
}

func (a *Engine2Adapter) ReplaceWithSnapshot(snapshotDir string) error {
	// Wipe current data directory then restore snapshot into it.
	dataDir := a.dataRoot
	if dataDir == "" {
		return fmt.Errorf("data dir not configured")
	}
	// If the snapshot directory is inside the data directory, preserve it by
	// moving it to a temporary location before wiping the data directory.
	restoreFrom := snapshotDir
	movedTempDir := ""
	absDataDir, _ := filepath.Abs(dataDir)
	absSnapDir, _ := filepath.Abs(snapshotDir)
	// Normalize with path separator to avoid partial prefix matches.
	if absDataDir != "" && strings.HasPrefix(absSnapDir, absDataDir+string(os.PathSeparator)) {
		tmp, err := os.MkdirTemp("", "nexus-snapshot-restore-*")
		if err != nil {
			return fmt.Errorf("failed to create temp dir to preserve snapshot: %w", err)
		}
		moved := filepath.Join(tmp, filepath.Base(absSnapDir))
		if err := os.Rename(absSnapDir, moved); err != nil {
			// If rename fails (cross-device), attempt copy fallback by copying files.
			if cpErr := copyDir(absSnapDir, moved); cpErr != nil {
				return fmt.Errorf("failed to preserve snapshot before restore: rename err=%v copy err=%v", err, cpErr)
			}
			// Remove original after copy
			_ = os.RemoveAll(absSnapDir)
		}
		restoreFrom = moved
		movedTempDir = tmp
	}

	if err := os.RemoveAll(dataDir); err != nil {
		// Attempt to clean up temp snapshot if we created one.
		if movedTempDir != "" {
			_ = os.RemoveAll(movedTempDir)
		}
		return fmt.Errorf("failed to wipe data directory before restore: %w", err)
	}

	mgr := a.GetSnapshotManager()
	if mgr == nil {
		if movedTempDir != "" {
			_ = os.RemoveAll(movedTempDir)
		}
		return fmt.Errorf("snapshot manager not available")
	}
	if err := mgr.RestoreFrom(context.Background(), restoreFrom); err != nil {
		if movedTempDir != "" {
			_ = os.RemoveAll(movedTempDir)
		}
		return fmt.Errorf("snapshot restore failed: %w", err)
	}

	// After a successful restore, some providers (like engine2) expect
	// SSTables to live under `sstables/` while snapshots use `sst/`.
	// Move any restored `sst/` files into `sstables/` so engine2 finds them
	// in its expected location. This is a best-effort, non-fatal step.
	srcSst := filepath.Join(dataDir, "sst")
	dstSstables := filepath.Join(dataDir, "sstables")
	if fi, err := os.Stat(srcSst); err == nil && fi.IsDir() {
		// ensure destination exists
		if err := os.MkdirAll(dstSstables, 0o755); err == nil {
			entries, rerr := os.ReadDir(srcSst)
			if rerr == nil {
				for _, e := range entries {
					src := filepath.Join(srcSst, e.Name())
					dst := filepath.Join(dstSstables, e.Name())
					// try rename first, fall back to copyDir for robustness
					if err := os.Rename(src, dst); err != nil {
						_ = copyDir(src, dst)
						_ = os.RemoveAll(src)
					}
				}
				// remove the leftover src dir if empty
				_ = os.RemoveAll(srcSst)
			}
		}
	}

	// Cleanup temp preserved snapshot if any.
	if movedTempDir != "" {
		_ = os.RemoveAll(movedTempDir)
	}
	return nil
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
	// Create/open leader WAL (wal package) for replication/snapshot features
	if a.leaderWal == nil && a.Engine2 != nil {
		lw, err := openLeaderWAL(a.Engine2.GetDataRoot())
		if err != nil {
			return err
		}
		a.leaderWal = lw
	}
	// start tag index manager background loops if present
	if a.tagIndexManager != nil {
		a.tagIndexManager.Start()
	}

	// mark started for snapshot provider
	a.started.Store(true)
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
	// close leader WAL if present
	if a.leaderWal != nil {
		_ = a.leaderWal.Close()
	}
	// drop memtable reference
	a.mem = nil

	// mark stopped
	a.started.Store(false)
	return nil
}

// Introspection & Utilities
func (a *Engine2Adapter) GetPubSub() (engine.PubSubInterface, error) {
	return nil, fmt.Errorf("GetPubSub not implemented")
}
func (a *Engine2Adapter) GetSnapshotsBaseDir() string             { return filepath.Join(a.dataRoot, "snapshots") }
func (a *Engine2Adapter) Metrics() (*engine.EngineMetrics, error) { return nil, nil }
func (a *Engine2Adapter) GetHookManager() hooks.HookManager {
	if a.hookManager != nil {
		return a.hookManager
	}
	return hooks.NewHookManager(nil)
}
func (a *Engine2Adapter) GetDLQDir() string     { return filepath.Join(a.dataRoot, "dlq") }
func (a *Engine2Adapter) GetDataDir() string    { return a.dataRoot }
func (a *Engine2Adapter) GetWALPath() string    { return filepath.Join(a.dataRoot, "wal") }
func (a *Engine2Adapter) GetClock() clock.Clock { return clock.SystemClockDefault }
func (a *Engine2Adapter) GetWAL() wal.WALInterface {
	// Prefer the leader WAL if present (tests and replication expect a WAL
	// implementation that supports Rotate/ActiveSegmentIndex). Fall back to
	// the engine-native WAL wrapper so snapshots can still include WAL files.
	if a.leaderWal != nil {
		return a.leaderWal
	}
	if a.wal != nil {
		return &engine2WALWrapper{w: a.wal}
	}
	return &noopWAL{}
}

// engine2WALWrapper adapts the engine2.WAL (file-based append-only wal)
// to the wal.WALInterface expected by the snapshot manager. Only a small
// subset of methods are meaningfully implemented: Path and ActiveSegmentIndex
// are provided; other methods are no-ops to satisfy the interface for
// snapshotting purposes.
type engine2WALWrapper struct {
	w *WAL
}

func (e *engine2WALWrapper) AppendBatch(entries []core.WALEntry) error { return nil }
func (e *engine2WALWrapper) Append(entry core.WALEntry) error          { return nil }
func (e *engine2WALWrapper) Sync() error                               { return nil }
func (e *engine2WALWrapper) Purge(upToIndex uint64) error              { return nil }
func (e *engine2WALWrapper) Close() error {
	if e.w == nil {
		return nil
	}
	return e.w.Close()
}
func (e *engine2WALWrapper) Path() string {
	if e.w == nil {
		return ""
	}
	return filepath.Dir(e.w.path)
}
func (e *engine2WALWrapper) SetTestingOnlyInjectCloseError(err error) {}
func (e *engine2WALWrapper) ActiveSegmentIndex() uint64               { return 0 }
func (e *engine2WALWrapper) Rotate() error                            { return nil }
func (e *engine2WALWrapper) NewStreamReader(fromSeqNum uint64) (wal.StreamReader, error) {
	return nil, fmt.Errorf("engine2 WAL wrapper does not support streaming")
}
func (a *Engine2Adapter) GetStringStore() indexer.StringStoreInterface { return a.stringStore }

// (GetSnapshotManager implemented earlier)

func (a *Engine2Adapter) GetSequenceNumber() uint64 { return a.sequenceNumber.Load() }

// encodeDataPointToWALEntry converts an engine2 DataPoint into a core.WALEntry
// suitable for appending to the repo WAL (used by replication). It uses the
// adapter's `stringStore` to map metric and tag strings to numeric IDs.
func (a *Engine2Adapter) encodeDataPointToWALEntry(dp *core.DataPoint) (*core.WALEntry, error) {
	if a.stringStore == nil {
		return nil, fmt.Errorf("string store not initialized")
	}
	// metric id
	metricID, _ := a.stringStore.GetOrCreateID(dp.Metric)
	// build encoded tag pairs
	var pairs []core.EncodedSeriesTagPair
	for k, v := range dp.Tags {
		kid, _ := a.stringStore.GetOrCreateID(k)
		vid, _ := a.stringStore.GetOrCreateID(v)
		pairs = append(pairs, core.EncodedSeriesTagPair{KeyID: kid, ValueID: vid})
	}
	// sort by KeyID for canonical ordering
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].KeyID < pairs[j].KeyID })

	key := core.EncodeTSDBKey(metricID, pairs, dp.Timestamp)

	if len(dp.Fields) == 0 {
		return &core.WALEntry{EntryType: core.EntryTypeDelete, Key: key, Value: nil}, nil
	}
	vb, err := dp.Fields.Encode()
	if err != nil {
		return nil, err
	}
	return &core.WALEntry{EntryType: core.EntryTypePutEvent, Key: key, Value: vb}, nil
}

// GetKnownSSTables returns the manifest entries currently known by the engine2 adapter.
func (a *Engine2Adapter) GetKnownSSTables() ([]SSTableManifestEntry, error) {
	if a.manifestMgr == nil {
		return nil, nil
	}
	return a.manifestMgr.ListEntries(), nil
}

// addActiveSeries tracks a new active time series in-memory.
func (a *Engine2Adapter) addActiveSeries(seriesKey string) {
	if seriesKey == "" {
		return
	}
	a.activeSeriesMu.Lock()
	if _, exists := a.activeSeries[seriesKey]; !exists {
		a.activeSeries[seriesKey] = struct{}{}
	}
	a.activeSeriesMu.Unlock()
}
