package snapshot

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
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/indexer"
	"github.com/INLOpen/nexusbase/internal"
	"github.com/INLOpen/nexusbase/levels"
	"github.com/INLOpen/nexusbase/memtable"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexusbase/utils"
	"github.com/INLOpen/nexusbase/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// testE2EProvider implements the EngineProvider interface for E2E testing,
// using real components where possible instead of mocks.
type testE2EProvider struct {
	lockMu             sync.Mutex
	dataDir            string
	walDir             string
	sstDir             string
	logger             *slog.Logger
	clock              utils.Clock
	tracer             trace.Tracer
	hooks              hooks.HookManager
	levelsManager      levels.Manager
	tagIndexManager    *indexer.TagIndexManager // Use concrete type to access internal methods if needed for testing
	stringStore        *indexer.StringStore
	seriesIDStore      *indexer.SeriesIDStore
	sstableCompression string
	sequenceNumber     uint64
	wal                wal.WALInterface
}

func newTestE2EProvider(t *testing.T, dataDir string) *testE2EProvider {
	t.Helper()
	walDir := filepath.Join(dataDir, "wal")
	sstDir := filepath.Join(dataDir, "sst")
	require.NoError(t, os.MkdirAll(walDir, 0755))
	require.NoError(t, os.MkdirAll(sstDir, 0755))

	lm, err := levels.NewLevelsManager(7, 4, 1024, noop.NewTracerProvider().Tracer(""))
	require.NoError(t, err)

	// --- Load state from manifest if it exists ---
	// This mimics the behavior of the main engine's startup process.
	currentFilePath := filepath.Join(dataDir, "CURRENT")
	if _, statErr := os.Stat(currentFilePath); statErr == nil {
		manifestFileNameBytes, readErr := os.ReadFile(currentFilePath)
		require.NoError(t, readErr, "Failed to read CURRENT file in test provider setup")

		manifestFileName := strings.TrimSpace(string(manifestFileNameBytes))
		manifestFilePath := filepath.Join(dataDir, manifestFileName)

		manifestFile, openErr := os.Open(manifestFilePath)
		require.NoError(t, openErr, "Failed to open manifest file in test provider setup")

		manifest, readManifestErr := ReadManifestBinary(manifestFile)
		manifestFile.Close() // Close the file immediately after reading
		require.NoError(t, readManifestErr, "Failed to read manifest binary in test provider setup")

		// Load the tables into the levels manager
		for _, levelManifest := range manifest.Levels {
			for _, tableMeta := range levelManifest.Tables {
				// The FileName in the manifest is relative, e.g., "sst/1.sst"
				sstFilePath := filepath.Join(dataDir, tableMeta.FileName)
				tbl, loadErr := sstable.LoadSSTable(sstable.LoadSSTableOptions{FilePath: sstFilePath, ID: tableMeta.ID})
				require.NoError(t, loadErr, "Failed to load SSTable %d from manifest", tableMeta.ID)
				addErr := lm.AddTableToLevel(levelManifest.LevelNumber, tbl)
				require.NoError(t, addErr, "Failed to add SSTable %d to level %d", tableMeta.ID, levelManifest.LevelNumber)
			}
		}
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tracer := noop.NewTracerProvider().Tracer("test")

	walOpts := wal.Options{
		Dir:      walDir,
		SyncMode: wal.SyncDisabled,
		Logger:   logger,
	}
	w, _, err := wal.Open(walOpts)
	require.NoError(t, err)

	// Setup dependencies for TagIndexManager
	stringStore := indexer.NewStringStore(logger, nil)
	require.NoError(t, stringStore.LoadFromFile(dataDir))

	seriesIDStore := indexer.NewSeriesIDStore(logger, nil)
	require.NoError(t, seriesIDStore.LoadFromFile(dataDir))

	var nextSSTID atomic.Uint64
	deps := &indexer.TagIndexDependencies{
		StringStore:     stringStore,
		SeriesIDStore:   seriesIDStore,
		DeletedSeries:   make(map[string]uint64),
		DeletedSeriesMu: &sync.RWMutex{},
		SSTNextID:       func() uint64 { return nextSSTID.Add(1) },
	}
	opts := indexer.TagIndexManagerOptions{
		DataDir: dataDir,
	}
	tim, err := indexer.NewTagIndexManager(opts, deps, logger, tracer)
	require.NoError(t, err)
	tim.Start()

	return &testE2EProvider{
		dataDir:            dataDir,
		walDir:             walDir,
		sstDir:             sstDir,
		logger:             logger,
		clock:              utils.NewMockClock(time.Now()),
		tracer:             tracer,
		hooks:              hooks.NewHookManager(nil),
		levelsManager:      lm,
		tagIndexManager:    tim,
		stringStore:        stringStore,
		seriesIDStore:      seriesIDStore,
		sstableCompression: "none",
		wal:                w,
	}
}

func (m *testE2EProvider) CheckStarted() error               { return nil }
func (m *testE2EProvider) GetWAL() wal.WALInterface          { return m.wal }
func (m *testE2EProvider) GetClock() utils.Clock             { return m.clock }
func (m *testE2EProvider) GetLogger() *slog.Logger           { return m.logger }
func (m *testE2EProvider) GetTracer() trace.Tracer           { return m.tracer }
func (m *testE2EProvider) GetHookManager() hooks.HookManager { return m.hooks }
func (m *testE2EProvider) GetLevelsManager() levels.Manager  { return m.levelsManager }
func (m *testE2EProvider) GetTagIndexManager() indexer.TagIndexManagerInterface {
	return m.tagIndexManager
}
func (m *testE2EProvider) GetPrivateStringStore() internal.PrivateManagerStore {
	return m.stringStore
}
func (m *testE2EProvider) GetPrivateSeriesIDStore() internal.PrivateManagerStore {
	return m.seriesIDStore
}
func (m *testE2EProvider) GetSSTableCompressionType() string { return m.sstableCompression }
func (m *testE2EProvider) GetSequenceNumber() uint64         { return m.sequenceNumber }
func (m *testE2EProvider) Lock()                             { m.lockMu.Lock() }
func (m *testE2EProvider) Unlock()                           { m.lockMu.Unlock() }
func (m *testE2EProvider) GetMemtablesForFlush() ([]*memtable.Memtable, *memtable.Memtable) {
	// For this E2E test, we assume memtables are already flushed and we are snapshotting a stable state.
	return nil, nil
}
func (m *testE2EProvider) FlushMemtableToL0(mem *memtable.Memtable, parentCtx context.Context) error {
	// No-op for this test, as we manually create SSTables.
	return nil
}
func (m *testE2EProvider) GetDeletedSeries() map[string]uint64 {
	return map[string]uint64{"deleted_series_1": 100}
}
func (m *testE2EProvider) GetRangeTombstones() map[string][]core.RangeTombstone {
	return map[string][]core.RangeTombstone{
		"rt_series_1": {{MinTimestamp: 100, MaxTimestamp: 200, SeqNum: 101}},
	}
}
func (m *testE2EProvider) Close(t *testing.T) {
	require.NoError(t, m.wal.Close())
	m.tagIndexManager.Stop()
	require.NoError(t, m.stringStore.Close())
	require.NoError(t, m.seriesIDStore.Close())
}

// createDummySSTableForE2E is a helper to create a real SSTable file.
func createDummySSTableForE2E(t *testing.T, dir string, id uint64, seqNumStart uint64) *sstable.SSTable {
	t.Helper()
	filePath := filepath.Join(dir, fmt.Sprintf("%d.sst", id))
	writerOpts := core.SSTableWriterOptions{
		DataDir:                      dir,
		ID:                           id,
		Compressor:                   &compressors.NoCompressionCompressor{},
		Logger:                       slog.New(slog.NewTextHandler(io.Discard, nil)),
		BloomFilterFalsePositiveRate: 0.01,
	}
	writer, err := sstable.NewSSTableWriter(writerOpts)
	require.NoError(t, err)

	key1 := []byte(fmt.Sprintf("key-%03d-a", id))
	key2 := []byte(fmt.Sprintf("key-%03d-z", id))
	require.NoError(t, writer.Add(key1, []byte("val1"), core.EntryTypePutEvent, seqNumStart))
	require.NoError(t, writer.Add(key2, []byte("val2"), core.EntryTypePutEvent, seqNumStart+1))
	require.NoError(t, writer.Finish())

	tbl, err := sstable.LoadSSTable(sstable.LoadSSTableOptions{FilePath: filePath, ID: id})
	require.NoError(t, err)
	return tbl
}

func TestSnapshot_E2E_CreateAndRestore(t *testing.T) {
	// --- 1. Setup Phase ---
	baseDir := t.TempDir()
	originalDataDir := filepath.Join(baseDir, "data_orig")
	snapshotDir := filepath.Join(baseDir, "snapshot")
	restoredDataDir := filepath.Join(baseDir, "data_restored")

	provider := newTestE2EProvider(t, originalDataDir)
	defer provider.Close(t)

	// --- 2. Populate Original State ---
	provider.sequenceNumber = 200

	// Add entries to WAL
	require.NoError(t, provider.wal.Append(core.WALEntry{Key: []byte("wal_key"), SeqNum: 150}))
	require.NoError(t, provider.wal.Rotate()) // Create a second WAL file
	require.NoError(t, provider.wal.Append(core.WALEntry{Key: []byte("wal_key_2"), SeqNum: 151}))

	// Create SSTables and add to levels
	sst1 := createDummySSTableForE2E(t, provider.sstDir, 1, 10)
	sst2 := createDummySSTableForE2E(t, provider.sstDir, 2, 20)
	provider.levelsManager.AddTableToLevel(0, sst1)
	provider.levelsManager.AddTableToLevel(1, sst2)

	// Add to tag index and populate string/series stores. This simulates the engine's
	// behavior of creating string/series mappings before adding to the index.
	tags := map[string]string{"tag1": "val1"}
	for k, v := range tags {
		_, err := provider.stringStore.GetOrCreateID(k)
		require.NoError(t, err)
		_, err = provider.stringStore.GetOrCreateID(v)
		require.NoError(t, err)
	}
	require.NoError(t, provider.tagIndexManager.Add(1, tags))

	// Sync the WAL to ensure all appended entries are on disk before snapshotting.
	// In a real engine, this would be handled by the flush process.
	require.NoError(t, provider.wal.Sync())

	// --- 3. Create Snapshot ---
	manager := NewManager(provider)
	err := manager.CreateFull(context.Background(), snapshotDir)
	require.NoError(t, err, "Snapshot creation should succeed")

	// Verify snapshot directory contents
	assert.FileExists(t, filepath.Join(snapshotDir, "CURRENT"))
	assert.FileExists(t, filepath.Join(snapshotDir, "sst", "1.sst"))
	assert.FileExists(t, filepath.Join(snapshotDir, "sst", "2.sst"))
	assert.FileExists(t, filepath.Join(snapshotDir, "wal", "00000001.wal"))
	assert.FileExists(t, filepath.Join(snapshotDir, "wal", "00000002.wal"))
	assert.FileExists(t, filepath.Join(snapshotDir, "index", indexer.IndexManifestFileName))
	assert.FileExists(t, filepath.Join(snapshotDir, "deleted_series.json"))
	assert.FileExists(t, filepath.Join(snapshotDir, "range_tombstones.json"))
	assert.FileExists(t, filepath.Join(snapshotDir, "string_mapping.log"))
	assert.FileExists(t, filepath.Join(snapshotDir, "series_mapping.log"))

	// --- 4. Restore Snapshot ---
	restoreOpts := RestoreOptions{
		DataDir: restoredDataDir,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	err = RestoreFromFull(restoreOpts, snapshotDir)
	require.NoError(t, err, "RestoreFromFull should succeed")

	// Verify restored directory contents
	assert.FileExists(t, filepath.Join(restoredDataDir, "CURRENT"))
	assert.FileExists(t, filepath.Join(restoredDataDir, "sst", "1.sst"))
	assert.FileExists(t, filepath.Join(restoredDataDir, "sst", "2.sst"))
	assert.FileExists(t, filepath.Join(restoredDataDir, "wal", "00000001.wal"))
	assert.FileExists(t, filepath.Join(restoredDataDir, "wal", "00000002.wal"))
	assert.FileExists(t, filepath.Join(restoredDataDir, indexer.IndexSSTDirName, indexer.IndexManifestFileName))
	assert.FileExists(t, filepath.Join(restoredDataDir, "deleted_series.json"))
	assert.FileExists(t, filepath.Join(restoredDataDir, "range_tombstones.json"))
	assert.FileExists(t, filepath.Join(restoredDataDir, "string_mapping.log"))
	assert.FileExists(t, filepath.Join(restoredDataDir, "series_mapping.log"))

	// --- 5. Verify Restored State ---
	// Open a new provider on the restored directory to check its state
	restoredProvider := newTestE2EProvider(t, restoredDataDir)
	defer restoredProvider.Close(t)

	// Verify levels manager state
	counts, err := restoredProvider.levelsManager.GetLevelTableCounts()
	require.NoError(t, err, "GetLevelTableCounts should not fail")

	var nonEmptyLevels int
	for _, count := range counts {
		if count > 0 {
			nonEmptyLevels++
		}
	}
	assert.Equal(t, 2, nonEmptyLevels, "Restored levels manager should have tables in exactly 2 levels")
	assert.Equal(t, 1, counts[0], "Restored L0 should have 1 table")
	assert.Equal(t, 1, counts[1], "Restored L1 should have 1 table")

	l0Tables := restoredProvider.levelsManager.GetTablesForLevel(0)
	require.Len(t, l0Tables, 1)
	assert.Equal(t, sst1.ID(), l0Tables[0].ID())

	l1Tables := restoredProvider.levelsManager.GetTablesForLevel(1)
	require.Len(t, l1Tables, 1)
	assert.Equal(t, sst2.ID(), l1Tables[0].ID())

	// Verify WAL state by recovering from it
	walOpts := wal.Options{Dir: restoredProvider.walDir}
	_, recoveredEntries, err := wal.Open(walOpts)
	require.NoError(t, err)
	require.Len(t, recoveredEntries, 2, "Should recover 2 entries from the restored WAL")
	assert.Equal(t, []byte("wal_key"), recoveredEntries[0].Key)
	assert.Equal(t, []byte("wal_key_2"), recoveredEntries[1].Key)

	// Verify tag index state
	bitmap, err := restoredProvider.tagIndexManager.Query(map[string]string{"tag1": "val1"})
	require.NoError(t, err)
	assert.True(t, bitmap.Contains(1), "Restored tag index should contain the original series ID")
}

// TestSnapshot_E2E_RestoreFailure_MissingManifest tests the restore process when the manifest file
// pointed to by CURRENT is missing from the snapshot.
func TestSnapshot_E2E_RestoreFailure_MissingManifest(t *testing.T) {
	// --- 1. Setup: Create a valid snapshot first ---
	baseDir := t.TempDir()
	originalDataDir := filepath.Join(baseDir, "data_orig")
	snapshotDir := filepath.Join(baseDir, "snapshot")
	restoredDataDir := filepath.Join(baseDir, "data_restored_fail")

	provider := newTestE2EProvider(t, originalDataDir)
	defer provider.Close(t)

	// Populate with some minimal data to make the snapshot non-trivial
	sst1 := createDummySSTableForE2E(t, provider.sstDir, 1, 10)
	provider.levelsManager.AddTableToLevel(0, sst1)

	// Create the snapshot
	manager := NewManager(provider)
	err := manager.CreateFull(context.Background(), snapshotDir)
	require.NoError(t, err, "Initial snapshot creation should succeed")
	require.FileExists(t, filepath.Join(snapshotDir, "CURRENT"))

	// --- 2. Corrupt the snapshot ---
	// Overwrite the CURRENT file to point to a non-existent manifest.
	// This simulates a form of corruption or an incomplete snapshot.
	err = os.WriteFile(filepath.Join(snapshotDir, "CURRENT"), []byte("MANIFEST_nonexistent.bin"), 0644)
	require.NoError(t, err, "Should be able to overwrite CURRENT file to simulate corruption")

	// --- 3. Attempt to restore from the corrupted snapshot ---
	restoreOpts := RestoreOptions{DataDir: restoredDataDir, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	err = RestoreFromFull(restoreOpts, snapshotDir)

	// --- 4. Verify the failure ---
	require.Error(t, err, "RestoreFromFull should fail when the manifest file is missing")
	assert.Contains(t, err.Error(), "could not read snapshot manifest", "Error message should indicate the manifest is missing")
	assert.ErrorIs(t, err, os.ErrNotExist, "Underlying error should be os.ErrNotExist")

	// Verify that the target directory was not created or was cleaned up.
	_, statErr := os.Stat(restoredDataDir)
	assert.True(t, os.IsNotExist(statErr), "Target data directory should not exist after a failed restore")
}
