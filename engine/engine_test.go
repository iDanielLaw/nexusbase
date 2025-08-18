package engine

import (
	"bytes"
	"context" // Import context
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockListener is a mock implementation of HookListener for testing, shared across engine_*.go test files.
type mockListener struct {
	priority    int
	callSignal  chan hooks.HookEvent
	returnErr   error
	isAsync     bool
	onEventFunc func(event hooks.HookEvent)
}

func (m *mockListener) OnEvent(ctx context.Context, event hooks.HookEvent) error {
	if m.onEventFunc != nil {
		m.onEventFunc(event)
	}
	if m.callSignal != nil {
		// Non-blocking send to avoid hanging if the channel is not read
		select {
		case m.callSignal <- event:
		default:
		}
	}
	return m.returnErr
}

func (m *mockListener) Priority() int {
	return m.priority
}

func (m *mockListener) IsAsync() bool {
	return m.isAsync
}

// newEngineForTombstoneTests is a helper function to create a StorageEngine for testing tombstone-related functionality.
func newEngineForTombstoneTests(t *testing.T) StorageEngineInterface {
	t.Helper()
	tempDir := t.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            1024 * 1024, // Large enough to prevent accidental flushes
		CompactionIntervalSeconds:    3600,        // Disable auto-compaction
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		Metrics:                      NewEngineMetrics(false, "tombstone_test_"),
		Logger:                       slog.New(slog.NewTextHandler(io.Discard, nil)), // Use a discard logger for tests
	}
	eng, err := NewStorageEngine(opts)
	require.NoError(t, err, "NewStorageEngine failed")

	err = eng.Start()
	require.NoError(t, err, "Failed to start setup engine")

	// Ensure the engine is closed after the test runs.
	t.Cleanup(func() {
		require.NoError(t, eng.Close())
	})
	return eng
}

func createTestSSTableFile(t *testing.T, eng StorageEngineInterface, id uint64, entries []testEntry) string {
	t.Helper()
	concreteEngine, ok := eng.(*storageEngine)
	if !ok {
		t.Fatalf("Failed to assert StorageEngineInterface to *storageEngine")
		return ""
	}
	writerOpts := core.SSTableWriterOptions{
		DataDir:                      concreteEngine.sstDir,
		ID:                           id,
		EstimatedKeys:                uint64(len(entries)),
		BloomFilterFalsePositiveRate: 0.01,
		BlockSize:                    sstable.DefaultBlockSize,
		Tracer:                       nil,
		Compressor:                   &compressors.NoCompressionCompressor{},
	}
	writer, err := sstable.NewSSTableWriter(writerOpts)
	if err != nil {
		t.Fatalf("Failed to create SSTable writer for test: %v", err)
	}
	for _, entry := range entries {
		metricID, _ := concreteEngine.stringStore.GetOrCreateID(entry.metric)
		// For simplicity, this helper doesn't handle tags.
		tsdbKey := core.EncodeTSDBKey(metricID, nil, entry.ts)
		// Encode the value as a FieldValues map to use EntryTypePutEvent
		fields, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": entry.value})
		if err != nil {
			t.Fatalf("failed to create FieldValues from test entry: %v", err)
		}
		encodedFields, err := fields.Encode()
		if err != nil {
			t.Fatalf("failed to encode FieldValues: %v", err)
		}
		writer.Add(tsdbKey, encodedFields, core.EntryTypePutEvent, entry.seqNum)
	}
	if err := writer.Finish(); err != nil {
		t.Fatalf("Failed to finish test SSTable %d: %v", id, err)
	}
	return writer.FilePath()
}

func TestStorageEngine_VerifyDataConsistency(t *testing.T) {
	tempDir := t.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            1024,
		Metrics:                      NewEngineMetrics(false, "verify_consistent_"), // Inject metrics, no global publish
		MaxL0Files:                   2,
		TargetSSTableSize:            2048,
		LevelsTargetSizeMultiplier:   2,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		SSTableCompressor:            &compressors.NoCompressionCompressor{}, // เพิ่ม NoCompressionCompressor
		WALSyncMode:                  core.WALSyncAlways,
		WALBatchSize:                 1,
		WALFlushIntervalMs:           0,
		CompactionIntervalSeconds:    3600, // Disable compaction for manual setup
	}

	t.Run("consistent_data", func(t *testing.T) {
		localOpts := opts
		engineDir := filepath.Join(tempDir, "consistent")
		localOpts.DataDir = engineDir
		if err := os.MkdirAll(engineDir, 0755); err != nil {
			t.Fatalf("Failed to create engineDir %s: %v", engineDir, err)
		} // Use a unique prefix for this sub-test's metrics
		localOpts.Metrics = NewEngineMetrics(false, "verify_consistent_data_")
		engine, err := NewStorageEngine(localOpts)
		if err != nil {
			t.Fatalf("NewStorageEngine failed: %v", err)
		}
		if err = engine.Start(); err != nil {
			t.Fatalf("Failed to start setup engine: %v", err)
		}

		// Put some data using the correct TSDB API to ensure WAL entries are well-formed
		engine.Put(context.Background(), HelperDataPoint(t, "consistent.metric.a", map[string]string{"tag": "a"}, 1, map[string]interface{}{"value": 1.0}))
		engine.Put(context.Background(), HelperDataPoint(t, "consistent.metric.b", map[string]string{"tag": "b"}, 2, map[string]interface{}{"value": 2.0}))
		engine.Close() // Flushes memtable

		// Reopen to load tables
		engine, err = NewStorageEngine(localOpts)
		if err != nil {
			t.Fatalf("NewStorageEngine (reopen) failed: %v", err)
		}
		if err = engine.Start(); err != nil {
			t.Fatalf("Failed to start setup engine: %v", err)
		}
		defer engine.Close()

		errors := engine.VerifyDataConsistency()
		if len(errors) > 0 {
			t.Errorf("Expected no consistency errors, but got: %v", errors)
		}
	})

	t.Run("corrupted_sstable_checksum_detected_on_verify", func(t *testing.T) {
		localOpts := opts
		engineDir := filepath.Join(tempDir, "corrupted_checksum")
		localOpts.DataDir = engineDir
		localOpts.Metrics = NewEngineMetrics(false, "verify_corrupted_checksum_")
		// Set ErrorOnSSTableLoadFailure to false so engine starts even with corrupted files
		localOpts.ErrorOnSSTableLoadFailure = false
		if err := os.MkdirAll(engineDir, 0755); err != nil {
			t.Fatalf("Failed to create engineDir %s: %v", engineDir, err)
		}

		// 1. Create a valid SSTable by creating an engine, putting data, and closing it.
		// This ensures the SSTable and its keys are in the correct TSDB format.
		setupOpts := localOpts
		// Give the setup engine its own metrics instance to avoid sharing state
		setupOpts.Metrics = NewEngineMetrics(false, "verify_corrupted_checksum_setup_") // New metrics instance
		setupOpts.CompactionIntervalSeconds = 3600                                      // Disable compaction during setup
		engine1, err := NewStorageEngine(setupOpts)
		if err != nil {
			t.Fatalf("Failed to create engine1 for setup: %v", err)
		}
		if err = engine1.Start(); err != nil {
			t.Fatalf("Failed to start setup engine: %v", err)
		}
		engine1.Put(context.Background(), HelperDataPoint(t, "corrupt.metric", map[string]string{"id": "1"}, 100, map[string]interface{}{"value": 1.0}))
		engine1.Put(context.Background(), HelperDataPoint(t, "corrupt.metric", map[string]string{"id": "2"}, 200, map[string]interface{}{"value": 2.0}))
		engine1.Close()

		// Find the created SSTable file
		sstDir := filepath.Join(engineDir, "sst")
		files, err := os.ReadDir(sstDir)
		if err != nil || len(files) == 0 {
			t.Fatalf("SSTable file not found in setup directory")
		}
		var validSSTPath string
		for _, f := range files {
			if strings.HasSuffix(f.Name(), ".sst") {
				validSSTPath = filepath.Join(sstDir, f.Name())
				break
			}
		}
		if validSSTPath == "" {
			t.Fatal("SSTable file not found in setup directory")
		}

		// 2. Manually corrupt the SSTable file (e.g., modify a byte in a data block)
		// To do this, we need to load the SSTable to find a block offset, then corrupt the file.
		// This is a bit involved for a unit test, but necessary to simulate corruption.
		sst, err := sstable.LoadSSTable(sstable.LoadSSTableOptions{
			FilePath:   validSSTPath,
			ID:         1, // ID doesn't matter much for this part, just need to load it
			BlockCache: nil,
			Tracer:     nil,
		})
		if err != nil {
			t.Fatalf("Failed to load valid SSTable for corruption: %v", err)
		}
		if len(sst.GetIndex().GetEntries()) == 0 {
			sst.Close()
			t.Fatal("SSTable has no index entries, cannot corrupt a block.")
		}
		firstBlockMeta := sst.GetIndex().GetEntries()[0]
		sst.Close() // Close the SSTable file handle before modifying it

		fileData, err := os.ReadFile(validSSTPath)
		if err != nil {
			t.Fatalf("Failed to read SSTable file for corruption: %v", err)
		}

		// Corrupt a byte in the first block's checksum (assuming checksum is 4 bytes after 1-byte compression flag)
		checksumByteToCorruptOffset := int(firstBlockMeta.BlockOffset) + 1 // Offset of the first byte of checksum
		if checksumByteToCorruptOffset < len(fileData) {
			fileData[checksumByteToCorruptOffset]++ // Flip a bit to corrupt checksum
		} else {
			t.Fatalf("Calculated corruption offset %d is out of bounds for file size %d", checksumByteToCorruptOffset, len(fileData))
		}

		if err := os.WriteFile(validSSTPath, fileData, 0644); err != nil {
			t.Fatalf("Failed to write corrupted SSTable file: %v", err)
		}

		// 3. Initialize engine with the corrupted file.
		// The engine should START successfully because the corruption is in a data block,
		// which is not read during startup (only the index/footer is).
		engine, err := NewStorageEngine(localOpts)
		require.NoError(t, err, "NewStorageEngine should not fail on this corruption type")
		err = engine.Start()
		require.NoError(t, err, "Start should not fail, as corruption is in a data block")
		defer engine.Close()

		// 4. Verify consistency - THIS is where the error should be found.
		errors := engine.VerifyDataConsistency()
		require.NotEmpty(t, errors, "Expected consistency errors for corrupted SSTable, but got none.")

		foundChecksumError := false
		for _, err := range errors {
			if strings.Contains(err.Error(), "checksum mismatch") {
				foundChecksumError = true
				break
			}
		}
		if !foundChecksumError {
			t.Errorf("Expected 'checksum mismatch' error, but not found in errors: %v", errors)
		}
	})

	t.Run("inconsistent_l1_overlap_detected_by_levels_manager", func(t *testing.T) {
		localOpts := opts

		engineDir := filepath.Join(tempDir, "inconsistent_l1_order")
		localOpts.DataDir = engineDir
		localOpts.Metrics = NewEngineMetrics(false, "verify_inconsistent_l1_") // New metrics instance
		localOpts.ErrorOnSSTableLoadFailure = false                            // Ensure engine starts even if we corrupt files
		if err := os.MkdirAll(engineDir, 0755); err != nil {
			t.Fatalf("Failed to create engineDir %s: %v", engineDir, err)
		}
		engine, err := NewStorageEngine(localOpts)
		if err != nil || engine == nil {
			t.Fatalf("NewStorageEngine failed: %v", err)
		}

		if err = engine.Start(); err != nil {
			t.Fatalf("Failed to start setup engine: %v", err)
		}
		concreteEngine, ok := engine.(*storageEngine)
		if !ok {
			t.Fatal("Failed to assert StorageEngineInterface to *storageEngine")
		}

		// Create SSTables that will cause an overlap in L1
		// SSTable A: keys "apple" to "banana"
		sstAPath := createTestSSTableFile(t, engine, 201, []testEntry{
			{metric: "series.a", value: "valA1"}, {metric: "series.b", value: "valA2"},
		})
		// SSTable B: keys "banana" to "cherry" (overlaps with A at "banana")
		sstBPath := createTestSSTableFile(t, engine, 202, []testEntry{
			{metric: "series.b", value: "valB1"}, {metric: "series.c", value: "valB2"},
		})
		// SSTable C: keys "date" to "elderberry" (no overlap, for a clean table)
		sstCPath := createTestSSTableFile(t, engine, 203, []testEntry{
			{metric: "series.d", value: "valC1"}, {metric: "series.e", value: "valC2"},
		})

		sstA, _ := sstable.LoadSSTable(sstable.LoadSSTableOptions{FilePath: sstAPath, ID: 201})
		sstB, _ := sstable.LoadSSTable(sstable.LoadSSTableOptions{FilePath: sstBPath, ID: 202})
		sstC, _ := sstable.LoadSSTable(sstable.LoadSSTableOptions{FilePath: sstCPath, ID: 203})

		// Manually add SSTables to L1 to create an overlap.
		// LevelsManager.ApplyCompactionResults sorts L1+, so we need to bypass it
		// or directly manipulate the internal levels state for this test.
		// For this test, we'll directly set the tables in L1.
		// Ensure they are sorted by MinKey, but still overlap.
		tablesForL1 := []*sstable.SSTable{sstA, sstB, sstC}
		// Sort them by MinKey to satisfy the initial sort check, but the overlap check should still fail due to content.
		sort.Slice(tablesForL1, func(i, j int) bool {
			return bytes.Compare(tablesForL1[i].MinKey(), tablesForL1[j].MinKey()) < 0
		})

		// Set the tables for Level 1 using the setter method
		concreteEngine.levelsManager.GetLevels()[1].SetTables(tablesForL1)

		defer engine.Close()

		errors := engine.VerifyDataConsistency()
		if len(errors) == 0 {
			t.Errorf("Expected consistency errors for L1 overlap, but got none.")
		}
		foundOverlapError := false
		for _, err := range errors {
			if strings.Contains(err.Error(), "overlaps with SSTable ID") {
				foundOverlapError = true
				break
			}
		}
		if !foundOverlapError {
			t.Errorf("Expected 'overlaps with SSTable ID' error, but not found in errors: %v", errors)
		}
	})

	t.Run("corrupted_sstable_index_firstkey_mismatch_in_memory", func(t *testing.T) {

		engineDir := filepath.Join(tempDir, "corrupted_index")
		if err := os.MkdirAll(engineDir, 0755); err != nil {
			t.Fatalf("Failed to create engineDir %s: %v", engineDir, err)
		}

		// Create the SSTable in a separate temporary directory to prevent the engine from auto-loading it.
		sstCreationDir := t.TempDir()
		if err := os.MkdirAll(sstCreationDir, 0755); err != nil {
			t.Fatalf("Failed to create engineDir %s: %v", engineDir, err)
		}

		// 1. Create a valid SSTable with multiple blocks using a separate, temporary engine instance.
		// Configure setupOpts for the temporary engine that creates the SSTable.
		// Start from the original, clean 'opts' to ensure no state is shared with localOpts.
		localOpts := opts
		localOpts.DataDir = engineDir
		localOpts.Metrics = NewEngineMetrics(false, "verify_corrupted_index_")
		localOpts.ErrorOnSSTableLoadFailure = false
		localOpts.DataDir = sstCreationDir
		// Give the setup engine its own metrics instance to avoid sharing state via the metrics object.
		localOpts.Metrics = NewEngineMetrics(false, "verify_corrupted_index_setup_")
		localOpts.CompactionIntervalSeconds = 3600 // Disable compaction during setup
		localOpts.SSTableDefaultBlockSize = 32     // Use small block size to force multiple blocks
		engine1, err := NewStorageEngine(localOpts)
		if err != nil || engine1 == nil {
			t.Fatalf("Failed to create engine1 for setup: %v", err)
		}
		if err = engine1.Start(); err != nil {
			t.Fatalf("Failed to start setup engine: %v", err)
		}
		// Add points that will likely end up in different blocks
		engine1.Put(context.Background(), HelperDataPoint(t, "index.corrupt.metric", map[string]string{"id": "A"}, 100, map[string]interface{}{"value": 1.0}))
		engine1.Put(context.Background(), HelperDataPoint(t, "index.corrupt.metric", map[string]string{"id": "B"}, 200, map[string]interface{}{"value": 2.0}))
		engine1.Put(context.Background(), HelperDataPoint(t, "index.corrupt.metric", map[string]string{"id": "C"}, 300, map[string]interface{}{"value": 3.0}))
		engine1.Put(context.Background(), HelperDataPoint(t, "index.corrupt.metric", map[string]string{"id": "X"}, 400, map[string]interface{}{"value": 4.0}))
		engine1.Put(context.Background(), HelperDataPoint(t, "index.corrupt.metric", map[string]string{"id": "Y"}, 500, map[string]interface{}{"value": 5.0}))
		engine1.Put(context.Background(), HelperDataPoint(t, "index.corrupt.metric", map[string]string{"id": "Z"}, 600, map[string]interface{}{"value": 6.0}))
		engine1.Close()

		// Find the created SSTable file
		sstCreationDirSST := filepath.Join(sstCreationDir, "sst")
		files, err := os.ReadDir(sstCreationDirSST)
		if err != nil || len(files) == 0 {
			t.Fatalf("SSTable file not found in setup directory")
		}
		var validSSTPath string
		for _, f := range files {
			if strings.HasSuffix(f.Name(), ".sst") {
				validSSTPath = filepath.Join(sstCreationDirSST, f.Name())
				break
			}
		}
		if validSSTPath == "" {
			t.Fatal("SSTable file not found in setup directory")
		}

		// 2. Load the valid SSTable into memory
		sst, err := sstable.LoadSSTable(sstable.LoadSSTableOptions{
			FilePath:   validSSTPath,
			ID:         1, // ID from the first flush
			BlockCache: nil,
			Tracer:     nil,
		})
		if err != nil {
			t.Fatalf("Failed to load valid SSTable: %v", err)
		}
		defer sst.Close() // Ensure the file handle is closed

		// 3. Manually corrupt the in-memory index entry's FirstKey
		if len(sst.GetIndex().GetEntries()) < 2 {
			t.Fatalf("SSTable does not have enough index entries to corrupt the second one. Need at least 2 blocks.")
		}
		// Corrupt the FirstKey of the second index entry (index 1)
		// We'll change 'k' in "key101" to 'x'
		corruptedKey := []byte("corrupted_first_key")
		// Directly modify the FirstKey of the second index entry in the loaded SSTable's index
		// Note: This modifies the in-memory object, not the file on disk.
		sst.GetIndex().GetEntries()[1].FirstKey = corruptedKey

		// 4. Create a new StorageEngine instance and manually add the corrupted SSTable to its LevelsManager
		// The engine's data directory is empty, so it won't load any tables on its own.
		engine, err := NewStorageEngine(localOpts)
		if err != nil {
			t.Fatalf("NewStorageEngine failed: %v", err)
		}

		if err = engine.Start(); err != nil {
			t.Fatalf("Failed to start setup engine: %v", err)
		}
		// Add the corrupted SSTable to L1 (or L0, doesn't matter for VerifyIntegrity)
		// This will make LevelsManager.VerifyConsistency iterate over it.
		concreteEngine, ok := engine.(*storageEngine)
		if !ok {
			t.Fatal("Failed to assert StorageEngineInterface to *storageEngine")
		}
		concreteEngine.levelsManager.AddTableToLevel(1, sst)

		// 4. Verify consistency
		errors := engine.VerifyDataConsistency()
		if len(errors) == 0 {
			t.Errorf("Expected consistency errors for corrupted SSTable index, but got none.")
		}
		foundIndexError := false
		for _, err := range errors {
			if strings.Contains(err.Error(), "mismatch with actual block first key") {
				foundIndexError = true
				break
			}
		}
		if !foundIndexError {
			t.Errorf("Expected 'mismatch with actual block first key' error, but not found in errors: %v", errors)
		}

		engine.Close() // Ensure this new engine is closed
	})

	t.Run("sstable_minkey_greater_than_maxkey", func(t *testing.T) {
		localOpts := opts
		engineDir := filepath.Join(tempDir, "sstable_min_max")
		localOpts.DataDir = engineDir
		localOpts.Metrics = NewEngineMetrics(false, "verify_min_max_") // New metrics instance

		if err := os.MkdirAll(engineDir, 0755); err != nil {
			t.Fatalf("Failed to create engineDir %s: %v", engineDir, err)
		}

		// --- Create a valid SSTable using a temporary engine to ensure correct key format ---
		// We write it directly into the target directory, then clean up auxiliary files
		// so the main test engine only sees the SSTable on startup.
		{
			setupOpts := opts // Start from clean opts
			setupOpts.DataDir = engineDir
			setupOpts.Metrics = NewEngineMetrics(false, "verify_min_max_setup_")
			setupOpts.CompactionIntervalSeconds = 3600 // Disable compaction during setup

			setupEngine, err := NewStorageEngine(setupOpts)
			if err != nil || setupEngine == nil {
				t.Fatalf("Failed to create setup engine: %v", err)
			}

			if err = setupEngine.Start(); err != nil {
				t.Fatalf("Failed to start setup engine: %v", err)
			}
			// Put some data
			setupEngine.Put(context.Background(), HelperDataPoint(t, "minmax.test", map[string]string{"id": "a"}, 1, map[string]interface{}{"value": 1.0}))
			setupEngine.Put(context.Background(), HelperDataPoint(t, "minmax.test", map[string]string{"id": "b"}, 2, map[string]interface{}{"value": 2.0}))
			// Close the setup engine to flush the data to an SSTable
			if err := setupEngine.Close(); err != nil {
				t.Fatalf("Failed to close setup engine: %v", err)
			}
		}

		// --- Clean up auxiliary files, leaving only the .sst file(s) ---
		files, err := os.ReadDir(engineDir)
		if err != nil {
			t.Fatalf("Failed to read engineDir to clean up auxiliary files: %v", err)
		}
		for _, file := range files {
			if !file.IsDir() {
				if err := os.Remove(filepath.Join(engineDir, file.Name())); err != nil {
					t.Fatalf("Failed to remove auxiliary file %s: %v", file.Name(), err)
				}
			}
		}

		engine, err := NewStorageEngine(localOpts)
		if err != nil || engine == nil {
			t.Fatalf("NewStorageEngine failed: %v", err)
		}
		if err = engine.Start(); err != nil {
			t.Fatalf("Failed to start setup engine: %v", err)
		}
		defer engine.Close()

		errors := engine.VerifyDataConsistency() // Should be clean for a normally generated table
		if len(errors) > 0 {
			t.Errorf("Expected no errors for a valid table, got: %v", errors)
		}
		t.Log("Note: Testing SSTable MinKey > MaxKey requires manual file corruption or direct struct manipulation not done in this test.")
	})
}

func TestStorageEngine_Recovery_FallbackScan(t *testing.T) {
	dataDir := t.TempDir()
	// --- Phase 1: Create a valid engine state with some SSTables ---
	{
		setupOpts := getBaseOptsForFlushTest(t)
		setupOpts.DataDir = dataDir
		setupEngine, err := NewStorageEngine(setupOpts)
		require.NoError(t, err)
		err = setupEngine.Start()
		require.NoError(t, err)

		// Put data that will end up in SSTables after close
		setupEngine.Put(context.Background(), HelperDataPoint(t, "metric.load", map[string]string{"id": "101"}, 101, map[string]interface{}{"value": 101.0}))
		setupEngine.Put(context.Background(), HelperDataPoint(t, "metric.load", map[string]string{"id": "102"}, 102, map[string]interface{}{"value": 102.0}))
		setupEngine.Put(context.Background(), HelperDataPoint(t, "metric.load", map[string]string{"id": "103"}, 103, map[string]interface{}{"value": 103.0}))
		require.NoError(t, setupEngine.Close(), "Failed to close setup engine")

		// To properly test the fallback recovery path (`scanDataDirAndLoadToL0`), we must
		// remove the CURRENT file. This makes the primary recovery path (from MANIFEST)
		// fail with os.ErrNotExist, which correctly triggers the fallback logic.
		require.NoError(t, os.Remove(filepath.Join(dataDir, core.CurrentFileName)))
	}

	// --- Phase 2: Create a new engine, which should load the state via fallback scan ---
	opts := getBaseOptsForFlushTest(t)
	opts.DataDir = dataDir // Use the same directory
	engine, err := NewStorageEngine(opts)
	require.NoError(t, err)
	err = engine.Start()
	require.NoError(t, err)
	defer engine.Close()

	concreteEngine, ok := engine.(*storageEngine)
	require.True(t, ok)

	// Check LevelsManager L0
	l0Tables := concreteEngine.levelsManager.GetTablesForLevel(0)
	require.NotEmpty(t, l0Tables, "Expected at least 1 table in L0 after fallback scan, but got 0")
	t.Logf("Loaded %d tables into L0.", len(l0Tables))

	// Verify all data points
	allPoints := []struct {
		metric    string
		tags      map[string]string
		timestamp int64
		value     float64
	}{
		{"metric.load", map[string]string{"id": "101"}, 101, 101.0},
		{"metric.load", map[string]string{"id": "102"}, 102, 102.0},
		{"metric.load", map[string]string{"id": "103"}, 103, 103.0},
	}

	for _, p := range allPoints {
		t.Run(fmt.Sprintf("VerifyPoint_id_%s", p.tags["id"]), func(t *testing.T) {
			val, err := engine.Get(context.Background(), p.metric, p.tags, p.timestamp)
			require.NoError(t, err, "Get for point %v failed after fallback recovery", p)

			retrievedVal := HelperFieldValueValidateFloat64(t, val, "value")
			assert.InDelta(t, p.value, retrievedVal, 1e-9, "Value mismatch for point %v", p)
		})
	}
}
