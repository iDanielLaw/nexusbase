package engine2

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/indexer"
	"github.com/INLOpen/nexusbase/levels"
	"github.com/INLOpen/nexusbase/replication/proto"
	"github.com/INLOpen/nexusbase/snapshot"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexusbase/wal"
	"github.com/INLOpen/nexuscore/utils/clock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

// Lightweight test types reused by the compaction tests.
type testEntry struct {
	metric string
	tags   map[string]string
	ts     int64
	value  string
	seqNum uint64
}

type testEntryWithTombstone struct {
	metric    string
	tags      map[string]string
	ts        int64
	value     string
	seqNum    uint64
	entryType core.EntryType
}

// createTestSSTableForCleanup creates a simple SSTable for cleanup tests.
func createTestSSTableForCleanup(t *testing.T, dir string, id uint64) *sstable.SSTable {
	t.Helper()
	writerOpts := core.SSTableWriterOptions{
		BloomFilterFalsePositiveRate: 0.01,
		DataDir:                      dir,
		ID:                           id,
		Compressor:                   &compressors.NoCompressionCompressor{},
		Logger:                       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	writer, err := sstable.NewSSTableWriter(writerOpts)
	require.NoError(t, err)
	require.NoError(t, writer.Add([]byte(fmt.Sprintf("key-%d", id)), []byte("value"), core.EntryTypePutEvent, id))
	require.NoError(t, writer.Finish())

	loadOpts := sstable.LoadSSTableOptions{FilePath: writer.FilePath(), ID: id, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	tbl, err := sstable.LoadSSTable(loadOpts)
	require.NoError(t, err)
	return tbl
}

// getTableIDs returns IDs for a slice of SSTables.
func getTableIDs(tables []*sstable.SSTable) []uint64 {
	ids := make([]uint64, len(tables))
	for i, tbl := range tables {
		ids[i] = tbl.ID()
	}
	return ids
}

// makeTestEventValue helper to encode a FieldValues payload.
func makeTestEventValue(t *testing.T, val string) []byte {
	t.Helper()
	fields, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": val})
	require.NoError(t, err)
	encoded, err := fields.Encode()
	require.NoError(t, err)
	return encoded
}

// helper to create a minimal LevelsManager used by compaction tests.
func newTestLevelsManager(t *testing.T) levels.Manager {
	lm, err := levels.NewLevelsManager(3, 2, 1024, trace.NewNoopTracerProvider().Tracer("test"), levels.PickOldest, 1.5, 1.0)
	require.NoError(t, err)
	t.Cleanup(func() { lm.Close() })
	return lm
}

// createDummySSTable creates an SSTable from provided entries without relying
// on a concrete storageEngine string store. It assigns deterministic numeric
// IDs for metric and tag keys for testing purposes.
func createDummySSTable(t *testing.T, dir string, id uint64, entries []testEntry) *sstable.SSTable {
	t.Helper()
	writerOpts := core.SSTableWriterOptions{
		DataDir:                      dir,
		ID:                           id,
		EstimatedKeys:                uint64(len(entries)),
		BloomFilterFalsePositiveRate: 0.01,
		BlockSize:                    sstable.DefaultBlockSize,
		Tracer:                       trace.NewNoopTracerProvider().Tracer("test"),
		Compressor:                   &compressors.NoCompressionCompressor{},
	}
	writer, err := sstable.NewSSTableWriter(writerOpts)
	require.NoError(t, err)

	// Simple deterministic id assignment
	nextMetricID := uint64(1000 + id*100)
	for i, e := range entries {
		metricID := nextMetricID + uint64(i)
		var pairs []core.EncodedSeriesTagPair
		j := uint64(1)
		for range e.tags {
			pairs = append(pairs, core.EncodedSeriesTagPair{KeyID: metricID + j, ValueID: metricID + j + 1})
			j += 2
		}
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].KeyID < pairs[j].KeyID })
		ts := e.ts
		if ts == 0 {
			ts = int64(e.seqNum)
		}
		tsdbKey := core.EncodeTSDBKey(metricID, pairs, ts)
		fields, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": e.value})
		require.NoError(t, err)
		encodedFields, err := fields.Encode()
		require.NoError(t, err)
		require.NoError(t, writer.Add(tsdbKey, encodedFields, core.EntryTypePutEvent, e.seqNum))
	}
	require.NoError(t, writer.Finish())
	loadOpts := sstable.LoadSSTableOptions{FilePath: writer.FilePath(), ID: id}
	sst, err := sstable.LoadSSTable(loadOpts)
	require.NoError(t, err)
	return sst
}

// createDummySSTableWithTombstones behaves like createDummySSTable but allows tombstone entries.
func createDummySSTableWithTombstones(t *testing.T, dir string, id uint64, entries []testEntryWithTombstone) *sstable.SSTable {
	t.Helper()
	writerOpts := core.SSTableWriterOptions{
		DataDir:                      dir,
		ID:                           id,
		EstimatedKeys:                uint64(len(entries)),
		BloomFilterFalsePositiveRate: 0.01,
		BlockSize:                    sstable.DefaultBlockSize,
		Tracer:                       trace.NewNoopTracerProvider().Tracer("test"),
		Compressor:                   &compressors.NoCompressionCompressor{},
	}
	writer, err := sstable.NewSSTableWriter(writerOpts)
	require.NoError(t, err)

	nextMetricID := uint64(2000 + id*100)
	for i, e := range entries {
		metricID := nextMetricID + uint64(i)
		var pairs []core.EncodedSeriesTagPair
		j := uint64(1)
		for range e.tags {
			pairs = append(pairs, core.EncodedSeriesTagPair{KeyID: metricID + j, ValueID: metricID + j + 1})
			j += 2
		}
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].KeyID < pairs[j].KeyID })
		ts := e.ts
		tsdbKey := core.EncodeTSDBKey(metricID, pairs, ts)

		var valueBytes []byte
		if e.entryType == core.EntryTypePutEvent {
			fields, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": e.value})
			require.NoError(t, err)
			valueBytes, err = fields.Encode()
			require.NoError(t, err)
		} else {
			valueBytes = []byte(e.value)
		}
		require.NoError(t, writer.Add(tsdbKey, valueBytes, e.entryType, e.seqNum))
	}
	require.NoError(t, writer.Finish())
	loadOpts := sstable.LoadSSTableOptions{FilePath: writer.FilePath(), ID: id}
	sst, err := sstable.LoadSSTable(loadOpts)
	require.NoError(t, err)
	return sst
}

// encodeTags is a lightweight encoder compatible with the helpers above.
func encodeTags(_ StorageEngineInterface, tags map[string]string) []core.EncodedSeriesTagPair {
	if tags == nil {
		return nil
	}
	var pairs []core.EncodedSeriesTagPair
	i := uint64(1)
	for range tags {
		pairs = append(pairs, core.EncodedSeriesTagPair{KeyID: i, ValueID: i + 1})
		i += 2
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].KeyID < pairs[j].KeyID })
	return pairs
}

// testEngine implements StorageEngineInterface with minimal behavior
// required by compaction tests. Methods not used by tests are no-ops.
type testEngine struct {
	dataDir string
	nextID  atomic.Uint64
	clk     clock.Clock
}

func (t *testEngine) GetNextSSTableID() uint64                                    { return t.nextID.Add(1) }
func (t *testEngine) Put(ctx context.Context, point core.DataPoint) error         { return nil }
func (t *testEngine) PutBatch(ctx context.Context, points []core.DataPoint) error { return nil }
func (t *testEngine) Get(ctx context.Context, metric string, tags map[string]string, timestamp int64) (core.FieldValues, error) {
	return nil, fmt.Errorf("not implemented")
}
func (t *testEngine) Delete(ctx context.Context, metric string, tags map[string]string, timestamp int64) error {
	return nil
}
func (t *testEngine) DeleteSeries(ctx context.Context, metric string, tags map[string]string) error {
	return nil
}
func (t *testEngine) DeletesByTimeRange(ctx context.Context, metric string, tags map[string]string, startTime, endTime int64) error {
	return nil
}
func (t *testEngine) Query(ctx context.Context, params core.QueryParams) (core.QueryResultIteratorInterface, error) {
	return nil, fmt.Errorf("not implemented")
}
func (t *testEngine) GetSeriesByTags(metric string, tags map[string]string) ([]string, error) {
	return nil, nil
}
func (t *testEngine) GetMetrics() ([]string, error)                           { return nil, nil }
func (t *testEngine) GetTagsForMetric(metric string) ([]string, error)        { return nil, nil }
func (t *testEngine) GetTagValues(metric, tagKey string) ([]string, error)    { return nil, nil }
func (t *testEngine) ForceFlush(ctx context.Context, wait bool) error         { return nil }
func (t *testEngine) TriggerCompaction()                                      {}
func (t *testEngine) CreateIncrementalSnapshot(snapshotsBaseDir string) error { return nil }
func (t *testEngine) VerifyDataConsistency() []error                          { return nil }
func (t *testEngine) CreateSnapshot(ctx context.Context) (string, error)      { return "", nil }
func (t *testEngine) RestoreFromSnapshot(ctx context.Context, path string, overwrite bool) error {
	return nil
}
func (t *testEngine) ApplyReplicatedEntry(ctx context.Context, entry *proto.WALEntry) error {
	return nil
}
func (t *testEngine) GetLatestAppliedSeqNum() uint64                { return 0 }
func (t *testEngine) ReplaceWithSnapshot(snapshotDir string) error  { return nil }
func (t *testEngine) CleanupEngine()                                {}
func (t *testEngine) Start() error                                  { return nil }
func (t *testEngine) Close() error                                  { return nil }
func (t *testEngine) GetPubSub() (PubSubInterface, error)           { return nil, nil }
func (t *testEngine) GetSnapshotsBaseDir() string                   { return filepath.Join(t.dataDir, "snapshots") }
func (t *testEngine) Metrics() (*EngineMetrics, error)              { return nil, nil }
func (t *testEngine) GetHookManager() hooks.HookManager             { return nil }
func (t *testEngine) GetDLQDir() string                             { return filepath.Join(t.dataDir, "dlq") }
func (t *testEngine) GetDataDir() string                            { return t.dataDir }
func (t *testEngine) GetWALPath() string                            { return filepath.Join(t.dataDir, "wal") }
func (t *testEngine) GetClock() clock.Clock                         { return t.clk }
func (t *testEngine) GetWAL() wal.WALInterface                      { return nil }
func (t *testEngine) GetStringStore() indexer.StringStoreInterface  { return nil }
func (t *testEngine) GetSnapshotManager() snapshot.ManagerInterface { return nil }
func (t *testEngine) GetSequenceNumber() uint64                     { return 0 }
