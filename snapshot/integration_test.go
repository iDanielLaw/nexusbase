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
	"github.com/INLOpen/nexusbase/wal"
	"github.com/INLOpen/nexuscore/utils/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// mockProviderForTest is a minimal implementation of snapshot.EngineProvider
// needed for test helpers that instantiate the snapshot manager without a full engine.
type mockProviderForTest struct {
	EngineProvider
	logger *slog.Logger
}

func (m *mockProviderForTest) GetLogger() *slog.Logger { return m.logger }

// testE2EProvider implements the EngineProvider interface for E2E testing,
// using real components where possible instead of mocks.
type testE2EProvider struct {
	lockMu             sync.Mutex
	dataDir            string
	walDir             string
	sstDir             string
	logger             *slog.Logger
	clock              clock.Clock
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

	lm, err := levels.NewLevelsManager(7, 4, 1024, noop.NewTracerProvider().Tracer(""), levels.PickOldest, 1.5, 1.0)
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

	// This simulates the engine's startup logic after a restore. The restore process
	// places the tag index snapshot into `dataDir/index`. The TagIndexManager, however,
	// expects its data to be in `dataDir/index_sst`. We must move the data to the
	// correct location before the TagIndexManager is initialized and tries to load it.
	indexSnapshotPath := filepath.Join(dataDir, core.IndexDirName)
	indexDataPath := filepath.Join(dataDir, core.IndexSSTDirName)
	if _, statErr := os.Stat(indexSnapshotPath); statErr == nil {
		t.Logf("Moving restored tag index data from %s to %s", indexSnapshotPath, indexDataPath)
		// Clean up any old index data dir first, just in case.
		require.NoError(t, os.RemoveAll(indexDataPath))
		require.NoError(t, os.Rename(indexSnapshotPath, indexDataPath))
	}

	walOpts := wal.Options{
		Dir:      walDir,
		SyncMode: core.WALSyncDisabled,
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

	// After moving the files, we need to explicitly load them into the manager.
	err = tim.LoadFromFile(dataDir)
	require.NoError(t, err, "Failed to load tag index from its data directory")
	tim.Start()

	return &testE2EProvider{
		dataDir:            dataDir,
		walDir:             walDir,
		sstDir:             sstDir,
		logger:             logger,
		clock:              clock.NewMockClock(time.Now()),
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
func (m *testE2EProvider) GetClock() clock.Clock             { return m.clock }
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
func (m *testE2EProvider) GetDataDir() string {
	return m.dataDir
}
func (m *testE2EProvider) GetSequenceNumber() uint64 { return m.sequenceNumber }
func (m *testE2EProvider) Lock()                     { m.lockMu.Lock() }
func (m *testE2EProvider) Unlock()                   { m.lockMu.Unlock() }
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
	assert.FileExists(t, filepath.Join(snapshotDir, "index", core.IndexManifestFileName))
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
	assert.FileExists(t, filepath.Join(restoredDataDir, core.IndexDirName, core.IndexManifestFileName))
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
	assert.Contains(t, err.Error(), "failed to read manifest from", "Error message should indicate the manifest is missing")
	assert.ErrorIs(t, err, os.ErrNotExist, "Underlying error should be os.ErrNotExist")

	// Verify that the target directory was not created or was cleaned up.
	_, statErr := os.Stat(restoredDataDir)
	assert.True(t, os.IsNotExist(statErr), "Target data directory should not exist after a failed restore")
}

// TestSnapshot_E2E_CreateIncrementalAndRestore tests the full lifecycle of creating
// a full snapshot, followed by an incremental one, and then verifying that a
// restore of the incremental snapshot correctly reconstructs the state.
func TestSnapshot_E2E_CreateIncrementalAndRestore(t *testing.T) {
	// --- 1. Setup ---
	baseDir := t.TempDir()
	originalDataDir := filepath.Join(baseDir, "data_orig")
	snapshotsBaseDir := filepath.Join(baseDir, "snapshots")
	restoredDataDir := filepath.Join(baseDir, "data_restored_chain")
	require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))

	provider := newTestE2EProvider(t, originalDataDir)
	defer provider.Close(t)
	manager := NewManager(provider)
	mockClock, ok := provider.clock.(*clock.MockClock)
	require.True(t, ok)

	// --- 2. Create Full Parent Snapshot ---
	provider.sequenceNumber = 100
	sst1 := createDummySSTableForE2E(t, provider.sstDir, 1, 10)
	provider.levelsManager.AddTableToLevel(0, sst1)
	require.NoError(t, provider.wal.Append(core.WALEntry{Key: []byte("wal_key_1"), SeqNum: 90}))
	require.NoError(t, provider.wal.Sync())

	// The snapshot directory name is based on the clock, so we control it here.
	parentTime := mockClock.Now()
	parentSnapshotID := fmt.Sprintf("%d_full", parentTime.UnixNano())
	parentSnapshotDir := filepath.Join(snapshotsBaseDir, parentSnapshotID)

	err := manager.CreateFull(context.Background(), parentSnapshotDir)
	require.NoError(t, err)

	// --- 3. Add More Data for Incremental ---
	mockClock.Advance(time.Second) // Ensure new snapshot has a different name
	provider.sequenceNumber = 200
	sst2 := createDummySSTableForE2E(t, provider.sstDir, 2, 110)
	provider.levelsManager.AddTableToLevel(1, sst2) // Add to a different level
	require.NoError(t, provider.wal.Append(core.WALEntry{Key: []byte("wal_key_2"), SeqNum: 190}))
	require.NoError(t, provider.wal.Sync())

	// --- 4. Create Incremental Snapshot ---
	err = manager.CreateIncremental(context.Background(), snapshotsBaseDir)
	require.NoError(t, err)

	// --- 5. Verify Incremental Snapshot Contents ---
	latestID, latestPath, err := findLatestSnapshot(snapshotsBaseDir, newHelperSnapshot())
	require.NoError(t, err)
	require.NotEqual(t, parentSnapshotID, latestID, "A new snapshot should have been created")

	// Check manifest
	incrManifest, _, err := readManifestFromDir(latestPath, newHelperSnapshot())
	require.NoError(t, err)
	assert.Equal(t, core.SnapshotTypeIncremental, incrManifest.Type)
	assert.Equal(t, parentSnapshotID, incrManifest.ParentID)
	assert.Equal(t, uint64(200), incrManifest.SequenceNumber)

	// Check files: should contain ONLY the new SSTable, not the old one.
	assert.NoFileExists(t, filepath.Join(latestPath, "sst", "1.sst"))
	assert.FileExists(t, filepath.Join(latestPath, "sst", "2.sst"))
	// It should contain the complete, current WAL state.
	assert.FileExists(t, filepath.Join(latestPath, "wal", "00000001.wal"))

	// --- 6. Restore from the chain ---
	restoreOpts := RestoreOptions{
		DataDir: restoredDataDir,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	err = RestoreFromFull(restoreOpts, latestPath)
	require.NoError(t, err, "RestoreFromFull should succeed on an incremental snapshot")

	// --- 7. Verify Restored State ---
	restoredProvider := newTestE2EProvider(t, restoredDataDir)
	defer restoredProvider.Close(t)

	// Check levels
	counts, err := restoredProvider.levelsManager.GetLevelTableCounts()
	require.NoError(t, err)
	assert.Equal(t, 1, counts[0], "Restored L0 should have 1 table (sst1)")
	assert.Equal(t, 1, counts[1], "Restored L1 should have 1 table (sst2)")

	// Check WAL
	walOpts := wal.Options{Dir: restoredProvider.walDir}
	_, recoveredEntries, err := wal.Open(walOpts)
	require.NoError(t, err)
	// The WAL is copied entirely, so it contains all entries.
	require.Len(t, recoveredEntries, 2, "Should recover 2 entries from the restored WAL")
}

// TestSnapshot_E2E_RestoreFromIncrementalChain tests creating a full snapshot,
// followed by two incremental snapshots, and then restoring the full chain.
func TestSnapshot_E2E_RestoreFromIncrementalChain(t *testing.T) {
	// --- 1. Setup ---
	baseDir := t.TempDir()
	originalDataDir := filepath.Join(baseDir, "data_orig")
	snapshotsBaseDir := filepath.Join(baseDir, "snapshots")
	restoredDataDir := filepath.Join(baseDir, "data_restored_multi_chain")
	require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))

	provider := newTestE2EProvider(t, originalDataDir)
	defer provider.Close(t)
	manager := NewManager(provider)
	mockClock, ok := provider.clock.(*clock.MockClock)
	require.True(t, ok)

	// --- 2. Create Full Parent Snapshot (State 1) ---
	provider.sequenceNumber = 100
	sst1 := createDummySSTableForE2E(t, provider.sstDir, 1, 10)
	provider.levelsManager.AddTableToLevel(0, sst1)
	require.NoError(t, provider.wal.Append(core.WALEntry{Key: []byte("wal_key_1"), SeqNum: 90}))
	require.NoError(t, provider.wal.Sync())

	parentTime := mockClock.Now()
	parentSnapshotID := fmt.Sprintf("%d_full", parentTime.UnixNano())
	parentSnapshotDir := filepath.Join(snapshotsBaseDir, parentSnapshotID)

	err := manager.CreateFull(context.Background(), parentSnapshotDir)
	require.NoError(t, err)

	// --- 3. Create First Incremental Snapshot (State 2) ---
	mockClock.Advance(time.Second)
	provider.sequenceNumber = 200
	sst2 := createDummySSTableForE2E(t, provider.sstDir, 2, 110)
	provider.levelsManager.AddTableToLevel(1, sst2)
	require.NoError(t, provider.wal.Append(core.WALEntry{Key: []byte("wal_key_2"), SeqNum: 190}))
	require.NoError(t, provider.wal.Sync())

	err = manager.CreateIncremental(context.Background(), snapshotsBaseDir)
	require.NoError(t, err)

	// --- 4. Create Second Incremental Snapshot (State 3) ---
	mockClock.Advance(time.Second)
	provider.sequenceNumber = 300
	sst3 := createDummySSTableForE2E(t, provider.sstDir, 3, 210)
	provider.levelsManager.AddTableToLevel(0, sst3) // Add to L0, simulating compaction changes
	require.NoError(t, provider.wal.Append(core.WALEntry{Key: []byte("wal_key_3"), SeqNum: 290}))
	require.NoError(t, provider.wal.Sync())

	err = manager.CreateIncremental(context.Background(), snapshotsBaseDir)
	require.NoError(t, err)

	// --- 5. Verify Final Incremental Snapshot ---
	latestID, latestPath, err := findLatestSnapshot(snapshotsBaseDir, newHelperSnapshot())
	require.NoError(t, err)
	require.NotEqual(t, parentSnapshotID, latestID)

	// Check manifest
	finalIncrManifest, _, err := readManifestFromDir(latestPath, newHelperSnapshot())
	require.NoError(t, err)
	assert.Equal(t, core.SnapshotTypeIncremental, finalIncrManifest.Type)
	assert.NotEqual(t, parentSnapshotID, finalIncrManifest.ParentID) // Parent should be the first incremental
	assert.Equal(t, uint64(300), finalIncrManifest.SequenceNumber)

	// Check files: should contain ONLY the newest SSTable (sst3)
	assert.NoFileExists(t, filepath.Join(latestPath, "sst", "1.sst"))
	assert.NoFileExists(t, filepath.Join(latestPath, "sst", "2.sst"))
	assert.FileExists(t, filepath.Join(latestPath, "sst", "3.sst"))
	// It should contain the complete, current WAL state.
	assert.FileExists(t, filepath.Join(latestPath, "wal", "00000001.wal"))

	// --- 6. Restore from the full chain ---
	restoreOpts := RestoreOptions{
		DataDir: restoredDataDir,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	err = RestoreFromFull(restoreOpts, latestPath)
	require.NoError(t, err, "RestoreFromFull should succeed on a multi-level incremental chain")

	// --- 7. Verify Restored State ---
	restoredProvider := newTestE2EProvider(t, restoredDataDir)
	defer restoredProvider.Close(t)

	// Check levels: sst1 and sst3 should be in L0, sst2 in L1
	counts, err := restoredProvider.levelsManager.GetLevelTableCounts()
	require.NoError(t, err)
	assert.Equal(t, 2, counts[0], "Restored L0 should have 2 tables (sst1, sst3)")
	assert.Equal(t, 1, counts[1], "Restored L1 should have 1 table (sst2)")

	// Check WAL
	walOpts := wal.Options{Dir: restoredProvider.walDir}
	_, recoveredEntries, err := wal.Open(walOpts)
	require.NoError(t, err)
	// The WAL is copied entirely from the last snapshot, so it contains all entries.
	require.Len(t, recoveredEntries, 3, "Should recover 3 entries from the restored WAL")
	assert.Equal(t, []byte("wal_key_1"), recoveredEntries[0].Key)
	assert.Equal(t, []byte("wal_key_2"), recoveredEntries[1].Key)
	assert.Equal(t, []byte("wal_key_3"), recoveredEntries[2].Key)
}

func TestSnapshot_E2E_RestoreFromLatest_WithChain(t *testing.T) {
	// --- 1. Setup: Create a snapshot chain (Full -> Incremental) ---
	baseDir := t.TempDir()
	originalDataDir := filepath.Join(baseDir, "data_orig")
	snapshotsBaseDir := filepath.Join(baseDir, "snapshots")
	restoredDataDir := filepath.Join(baseDir, "data_restored_latest")
	require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))

	provider := newTestE2EProvider(t, originalDataDir)
	defer provider.Close(t)
	manager := NewManager(provider)
	mockClock, ok := provider.clock.(*clock.MockClock)
	require.True(t, ok)

	// Create Full Snapshot
	provider.sequenceNumber = 100
	sst1 := createDummySSTableForE2E(t, provider.sstDir, 1, 10)
	provider.levelsManager.AddTableToLevel(0, sst1)
	require.NoError(t, provider.wal.Append(core.WALEntry{Key: []byte("wal_key_1"), SeqNum: 90}))
	require.NoError(t, provider.wal.Sync())
	parentSnapshotDir := filepath.Join(snapshotsBaseDir, fmt.Sprintf("%d_full", mockClock.Now().UnixNano()))
	err := manager.CreateFull(context.Background(), parentSnapshotDir)
	require.NoError(t, err)

	// Create Incremental Snapshot
	mockClock.Advance(time.Second)
	provider.sequenceNumber = 200
	sst2 := createDummySSTableForE2E(t, provider.sstDir, 2, 110)
	provider.levelsManager.AddTableToLevel(1, sst2)
	require.NoError(t, provider.wal.Append(core.WALEntry{Key: []byte("wal_key_2"), SeqNum: 190}))
	require.NoError(t, provider.wal.Sync())
	err = manager.CreateIncremental(context.Background(), snapshotsBaseDir)
	require.NoError(t, err)

	// --- 2. Restore from the latest snapshot in the base directory ---
	restoreOpts := RestoreOptions{
		DataDir: restoredDataDir,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	// This is the key part of the test: call RestoreFromLatest on the base directory
	err = RestoreFromLatest(restoreOpts, snapshotsBaseDir)
	require.NoError(t, err, "RestoreFromLatest should succeed with a snapshot chain")

	// --- 3. Verify Restored State ---
	restoredProvider := newTestE2EProvider(t, restoredDataDir)
	defer restoredProvider.Close(t)

	// Check levels: sst1 should be in L0, sst2 in L1
	counts, err := restoredProvider.levelsManager.GetLevelTableCounts()
	require.NoError(t, err)
	assert.Equal(t, 1, counts[0], "Restored L0 should have 1 table (sst1)")
	assert.Equal(t, 1, counts[1], "Restored L1 should have 1 table (sst2)")

	// Check WAL: should contain entries from both snapshots
	_, recoveredEntries, err := wal.Open(wal.Options{Dir: restoredProvider.walDir})
	require.NoError(t, err)
	require.Len(t, recoveredEntries, 2, "Should recover 2 entries from the restored WAL")
}

func TestSnapshot_E2E_RestoreFromBrokenChain(t *testing.T) {
	// --- 1. Setup: Create a valid snapshot chain (Full -> Incr1 -> Incr2) ---
	baseDir := t.TempDir()
	originalDataDir := filepath.Join(baseDir, "data_orig")
	snapshotsBaseDir := filepath.Join(baseDir, "snapshots")
	restoredDataDir := filepath.Join(baseDir, "data_restored_broken")
	require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))

	provider := newTestE2EProvider(t, originalDataDir)
	defer provider.Close(t)
	manager := NewManager(provider)
	mockClock, ok := provider.clock.(*clock.MockClock)
	require.True(t, ok)

	// Create Full Snapshot
	provider.sequenceNumber = 100
	sst1 := createDummySSTableForE2E(t, provider.sstDir, 1, 10)
	provider.levelsManager.AddTableToLevel(0, sst1)
	parentSnapshotDir := filepath.Join(snapshotsBaseDir, fmt.Sprintf("%d_full", mockClock.Now().UnixNano()))
	err := manager.CreateFull(context.Background(), parentSnapshotDir)
	require.NoError(t, err)

	// Create First Incremental Snapshot
	mockClock.Advance(time.Second)
	provider.sequenceNumber = 200
	sst2 := createDummySSTableForE2E(t, provider.sstDir, 2, 110)
	provider.levelsManager.AddTableToLevel(1, sst2)
	err = manager.CreateIncremental(context.Background(), snapshotsBaseDir)
	require.NoError(t, err)
	middleSnapshotID, middleSnapshotPath, err := findLatestSnapshot(snapshotsBaseDir, newHelperSnapshot())
	require.NoError(t, err)

	// Create Second Incremental Snapshot
	mockClock.Advance(time.Second)
	provider.sequenceNumber = 300
	sst3 := createDummySSTableForE2E(t, provider.sstDir, 3, 210)
	provider.levelsManager.AddTableToLevel(0, sst3)
	err = manager.CreateIncremental(context.Background(), snapshotsBaseDir)
	require.NoError(t, err)
	latestID, latestPath, err := findLatestSnapshot(snapshotsBaseDir, newHelperSnapshot())
	require.NoError(t, err)

	// --- 2. Break the chain by deleting the middle snapshot ---
	t.Logf("Simulating broken chain by deleting middle snapshot: %s", middleSnapshotPath)
	require.NoError(t, os.RemoveAll(middleSnapshotPath))

	// --- 3. Attempt to restore from the end of the broken chain ---
	restoreOpts := RestoreOptions{
		DataDir: restoredDataDir,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	err = RestoreFromFull(restoreOpts, latestPath)

	// --- 4. Verify the failure ---
	require.Error(t, err, "RestoreFromFull should fail when the chain is broken")
	expectedErrStr := fmt.Sprintf("parent snapshot %s not found for %s", middleSnapshotID, latestID)
	assert.Contains(t, err.Error(), expectedErrStr, "Error message should indicate the missing parent")

	// Verify that the target directory was not created or was cleaned up.
	_, statErr := os.Stat(restoredDataDir)
	assert.True(t, os.IsNotExist(statErr), "Target data directory should not exist after a failed restore")
}

func TestSnapshot_E2E_ValidateChain(t *testing.T) {
	// --- 1. Setup: Create a valid snapshot chain (Full -> Incr1 -> Incr2) ---
	baseDir := t.TempDir()
	originalDataDir := filepath.Join(baseDir, "data_orig")
	snapshotsBaseDir := filepath.Join(baseDir, "snapshots")
	require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))

	provider := newTestE2EProvider(t, originalDataDir)
	defer provider.Close(t)
	manager := NewManager(provider)
	mockClock, ok := provider.clock.(*clock.MockClock)
	require.True(t, ok)

	// Create Full Snapshot
	provider.sequenceNumber = 100
	sst1 := createDummySSTableForE2E(t, provider.sstDir, 1, 10)
	provider.levelsManager.AddTableToLevel(0, sst1)
	parentSnapshotDir := filepath.Join(snapshotsBaseDir, fmt.Sprintf("%d_full", mockClock.Now().UnixNano()))
	err := manager.CreateFull(context.Background(), parentSnapshotDir)
	require.NoError(t, err)

	// Create First Incremental Snapshot
	mockClock.Advance(time.Second)
	provider.sequenceNumber = 200
	sst2 := createDummySSTableForE2E(t, provider.sstDir, 2, 110)
	provider.levelsManager.AddTableToLevel(1, sst2)
	err = manager.CreateIncremental(context.Background(), snapshotsBaseDir)
	require.NoError(t, err)
	middleSnapshotID, middleSnapshotPath, err := findLatestSnapshot(snapshotsBaseDir, newHelperSnapshot())
	require.NoError(t, err)

	// Create Second Incremental Snapshot
	mockClock.Advance(time.Second)
	provider.sequenceNumber = 300
	sst3 := createDummySSTableForE2E(t, provider.sstDir, 3, 210)
	provider.levelsManager.AddTableToLevel(0, sst3)
	err = manager.CreateIncremental(context.Background(), snapshotsBaseDir)
	require.NoError(t, err)
	latestID, latestPath, err := findLatestSnapshot(snapshotsBaseDir, newHelperSnapshot())
	require.NoError(t, err)

	// --- 2. Validate the complete chain ---
	t.Log("Validating complete chain...")
	err = manager.Validate(latestPath)
	require.NoError(t, err, "Validation of a complete chain should succeed")

	// --- 3. Break the chain ---
	t.Logf("Breaking chain by deleting middle snapshot: %s", middleSnapshotPath)
	require.NoError(t, os.RemoveAll(middleSnapshotPath))

	// --- 4. Validate the broken chain ---
	t.Log("Validating broken chain...")
	err = manager.Validate(latestPath)
	require.Error(t, err, "Validation of a broken chain should fail")
	expectedErrStr := fmt.Sprintf("parent snapshot %s for %s not found", middleSnapshotID, latestID)
	assert.Contains(t, err.Error(), expectedErrStr, "Error message should indicate the missing parent")

	// --- 5. Validate a non-existent snapshot ---
	err = manager.Validate(filepath.Join(snapshotsBaseDir, "non_existent_snapshot"))
	require.Error(t, err, "Validation of a non-existent snapshot should fail")
	assert.Contains(t, err.Error(), "does not exist")
}

func TestSnapshot_E2E_Prune(t *testing.T) {
	// --- 1. Setup: Create multiple snapshot chains ---
	baseDir := t.TempDir()
	originalDataDir := filepath.Join(baseDir, "data_orig")
	snapshotsBaseDir := filepath.Join(baseDir, "snapshots")
	require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))

	provider := newTestE2EProvider(t, originalDataDir)
	defer provider.Close(t)
	manager := NewManager(provider)
	mockClock, ok := provider.clock.(*clock.MockClock)
	require.True(t, ok)

	// --- Chain 1 (Oldest, to be pruned) ---
	provider.sequenceNumber = 100
	sst1 := createDummySSTableForE2E(t, provider.sstDir, 1, 10)
	provider.levelsManager.AddTableToLevel(0, sst1)
	chain1_full_dir := filepath.Join(snapshotsBaseDir, fmt.Sprintf("%d_full", mockClock.Now().UnixNano()))
	require.NoError(t, manager.CreateFull(context.Background(), chain1_full_dir))

	mockClock.Advance(time.Second)
	provider.sequenceNumber = 200
	sst2 := createDummySSTableForE2E(t, provider.sstDir, 2, 110)
	provider.levelsManager.AddTableToLevel(1, sst2)
	require.NoError(t, manager.CreateIncremental(context.Background(), snapshotsBaseDir))

	// --- Chain 2 (Newest, to be kept) ---
	mockClock.Advance(time.Hour)
	provider.sequenceNumber = 300
	sst3 := createDummySSTableForE2E(t, provider.sstDir, 3, 210)
	provider.levelsManager.AddTableToLevel(0, sst3)
	chain2_full_dir := filepath.Join(snapshotsBaseDir, fmt.Sprintf("%d_full", mockClock.Now().UnixNano()))
	require.NoError(t, manager.CreateFull(context.Background(), chain2_full_dir))

	mockClock.Advance(time.Second)
	provider.sequenceNumber = 400
	sst4 := createDummySSTableForE2E(t, provider.sstDir, 4, 310)
	provider.levelsManager.AddTableToLevel(1, sst4)
	require.NoError(t, manager.CreateIncremental(context.Background(), snapshotsBaseDir))

	// --- 2. List before pruning ---
	infosBefore, err := manager.ListSnapshots(snapshotsBaseDir)
	require.NoError(t, err)
	require.Len(t, infosBefore, 4, "Should have 4 snapshots before pruning")

	// --- 3. Prune, keeping 1 latest full chain ---
	deletedIDs, err := manager.Prune(context.Background(), snapshotsBaseDir, PruneOptions{KeepN: 1})
	require.NoError(t, err, "Prune operation should succeed")

	// --- 4. Verify results ---
	assert.Len(t, deletedIDs, 2, "Should have deleted 2 snapshots from the oldest chain")

	infosAfter, err := manager.ListSnapshots(snapshotsBaseDir)
	require.NoError(t, err)
	require.Len(t, infosAfter, 2, "Should have 2 snapshots remaining after pruning")

	// Verify that the remaining snapshots are from the newest chain
	var remainingIDs []string
	for _, info := range infosAfter {
		remainingIDs = append(remainingIDs, info.ID)
	}
	assert.Contains(t, remainingIDs, filepath.Base(chain2_full_dir))
	assert.NotContains(t, remainingIDs, filepath.Base(chain1_full_dir))
}

func TestSnapshot_E2E_Prune_ByAge(t *testing.T) {
	// --- 1. Setup: Create multiple snapshot chains with different ages ---
	baseDir := t.TempDir()
	snapshotsBaseDir := filepath.Join(baseDir, "snapshots")
	require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))

	provider := newTestE2EProvider(t, baseDir) // Use baseDir for dummy data
	defer provider.Close(t)
	manager := NewManager(provider)
	mockClock, ok := provider.clock.(*clock.MockClock)
	require.True(t, ok)

	// --- Chain 1 (Old, to be pruned by age) ---
	// Set clock to 3 days ago
	mockClock.SetTime(time.Now().Add(-72 * time.Hour))
	provider.sequenceNumber = 100
	sst1 := createDummySSTableForE2E(t, provider.sstDir, 1, 10)
	provider.levelsManager.AddTableToLevel(0, sst1)
	chain1_full_dir := filepath.Join(snapshotsBaseDir, fmt.Sprintf("%d_full", mockClock.Now().UnixNano()))
	require.NoError(t, manager.CreateFull(context.Background(), chain1_full_dir))

	// --- Chain 2 (New, to be kept) ---
	// Set clock to now
	mockClock.SetTime(time.Now())
	provider.sequenceNumber = 200
	sst2 := createDummySSTableForE2E(t, provider.sstDir, 2, 110)
	provider.levelsManager.AddTableToLevel(1, sst2)
	chain2_full_dir := filepath.Join(snapshotsBaseDir, fmt.Sprintf("%d_full", mockClock.Now().UnixNano()))
	require.NoError(t, manager.CreateFull(context.Background(), chain2_full_dir))

	// --- 2. List before pruning ---
	infosBefore, err := manager.ListSnapshots(snapshotsBaseDir)
	require.NoError(t, err)
	require.Len(t, infosBefore, 2, "Should have 2 snapshots before pruning")

	// --- 3. Prune, keeping chains newer than 48 hours ---
	// This should delete chain 1 but keep chain 2.
	deletedIDs, err := manager.Prune(context.Background(), snapshotsBaseDir, PruneOptions{PruneOlderThan: 48 * time.Hour})
	require.NoError(t, err, "Prune operation should succeed")

	// --- 4. Verify results ---
	assert.Len(t, deletedIDs, 1, "Should have deleted 1 snapshot from the oldest chain")
	assert.Equal(t, filepath.Base(chain1_full_dir), deletedIDs[0])

	infosAfter, err := manager.ListSnapshots(snapshotsBaseDir)
	require.NoError(t, err)
	require.Len(t, infosAfter, 1, "Should have 1 snapshot remaining after pruning")
	assert.Equal(t, filepath.Base(chain2_full_dir), infosAfter[0].ID)
}
