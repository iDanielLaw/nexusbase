package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/iterator"
	"github.com/INLOpen/nexusbase/levels"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexusbase/sys"
	"github.com/INLOpen/nexuscore/types"
	"github.com/INLOpen/nexuscore/utils/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

var errMockRemove = fmt.Errorf("simulated os.Remove error")

func init() {
	sys.SetDebugMode(false)
}

// MockSSTableWriter for testing CompactionManager error handling
type MockSSTableWriter struct {
	failAdd         bool
	addErr          error
	failFinish      bool
	finishErr       error
	currentSizeFunc func() int64 // Function to return current size, for mocking dynamic size
	failLoad        bool         // Simulate LoadSSTable failure after Finish

	// --- Fields for parallel execution testing ---
	entries []struct { // Track entries added to this mock writer
		key       []byte
		value     []byte
		entryType core.EntryType
		seqNum    uint64
	}
	filePath string
	id       uint64
}

// controllableMockSSTableWriter is a mock writer that signals when it starts
// and waits for a signal to finish, allowing tests to control compaction duration.
type controllableMockSSTableWriter struct {
	// Embed a real writer to handle file creation and basic functionality.
	core.SSTableWriterInterface
	startSignal  chan bool // Signals that a writer has started
	finishSignal chan bool // Waits for a signal to finish
	signaled     atomic.Bool
}

func (m *controllableMockSSTableWriter) Add(key, value []byte, entryType core.EntryType, seqNum uint64) error {
	// On the first Add, signal that we've started and then block.
	if !m.signaled.Load() {
		if m.signaled.CompareAndSwap(false, true) {
			if m.startSignal != nil {
				m.startSignal <- true
			}
			if m.finishSignal != nil {
				<-m.finishSignal // Block until told to continue
			}
		}
	}
	return m.SSTableWriterInterface.Add(key, value, entryType, seqNum)
}

func (m *MockSSTableWriter) Add(key, value []byte, entryType core.EntryType, seqNum uint64) error {
	if m.failAdd {
		return m.addErr
	}
	m.entries = append(m.entries, struct {
		key       []byte
		value     []byte
		entryType core.EntryType
		seqNum    uint64
	}{key, value, entryType, seqNum})
	return nil
}
func (m *MockSSTableWriter) Finish() error {
	if m.failFinish {
		return m.finishErr
	}
	return nil
}
func (m *MockSSTableWriter) Abort() error     { return nil }
func (m *MockSSTableWriter) FilePath() string { return m.filePath }
func (m *MockSSTableWriter) CurrentSize() int64 {
	if m.currentSizeFunc != nil {
		return m.currentSizeFunc()
	}
	// Default behavior if no specific function is provided
	return 1000000 // Large enough to trigger rollover
}

// mockFileRemover for testing os.Remove errors
type mockFileRemover struct {
	mu sync.Mutex // Add a mutex to protect concurrent access to removedFiles
	// A map where keys are file paths that should fail removal, and values are the errors to return.
	// If a file path is not in this map, Remove will succeed.
	failPaths    map[string]error
	removedFiles []string // Tracks files that were successfully "removed" by the mock
}

func (m *mockFileRemover) Remove(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err, ok := m.failPaths[name]; ok {
		return err // Fail for this specific path
	}
	// If not configured to fail, simulate successful removal by actually removing the file.
	// This makes the test environment more realistic.
	if err := os.Remove(name); err != nil {
		return err
	}
	m.removedFiles = append(m.removedFiles, name)
	return nil
}

// Helper to create a CompactionManager with a mock SSTableWriterFactory
func setupCompactionManagerWithMockWriter(t *testing.T, mockWriter *MockSSTableWriter, clock clock.Clock) CompactionManagerInterface {
	t.Helper()
	logger := slog.Default()

	lm, _ := levels.NewLevelsManager(3, 2, 1024, trace.NewNoopTracerProvider().Tracer("test"), levels.PickOldest)
	t.Cleanup(func() { lm.Close() })

	// Create a dummy engine just to get a stringStore
	dummyEngine, _ := NewStorageEngine(StorageEngineOptions{
		DataDir:           t.TempDir(),
		MaxLevels:         3,
		MaxL0Files:        2,
		TargetSSTableSize: 1024,
		Clock:             clock,
	})

	concreteEngine := dummyEngine.(*storageEngine)

	concreteEngine.setCompactorFactory = func(seo StorageEngineOptions, se *storageEngine) (CompactionManagerInterface, error) {
		mockRemover := &mockFileRemover{
			failPaths: map[string]error{filepath.Join(se.sstDir, "101.sst"): errMockRemove},
		}

		cmParams := CompactionManagerParams{
			LevelsManager: se.levelsManager,
			DataDir:       se.sstDir,
			Opts: CompactionOptions{
				TargetSSTableSize:          100,
				LevelsTargetSizeMultiplier: 2,    // Add this back
				CompactionIntervalSeconds:  3600, // Disable auto compaction
				SSTableCompressor:          &compressors.NoCompressionCompressor{},
			},
			Logger:               logger,
			Tracer:               trace.NewNoopTracerProvider().Tracer("test"),
			IsSeriesDeleted:      func(b []byte, u uint64) bool { return false },
			IsRangeDeleted:       func(b []byte, i int64, u uint64) bool { return false },
			ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
			FileRemover:          mockRemover,
			SSTableWriterFactory: func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
				// For testing LoadSSTable error, we need a real writer to create a file,
				// then simulate LoadSSTable failure.
				if mockWriter.failLoad {
					// Create a dummy file that LoadSSTable will fail on
					dummyPath := filepath.Join(se.sstDir, fmt.Sprintf("%d.corrupted", opts.ID))
					os.WriteFile(dummyPath, []byte("corrupted data"), 0644)
					mockWriter.filePath = dummyPath // Set the path for the mock
					mockWriter.id = opts.ID
					return mockWriter, nil // Return mock, but it will cause LoadSSTable to fail
				}
				// For other errors (Add/Finish), just return the mock writer directly
				mockWriter.filePath = filepath.Join(se.sstDir, fmt.Sprintf("%d.sst", opts.ID))
				mockWriter.id = opts.ID
				return mockWriter, nil
			},
			Engine: se,
		}

		return NewCompactionManager(cmParams)
	}

	if err := dummyEngine.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
		return nil
	}
	t.Cleanup(func() { dummyEngine.Close() })
	// Configure mock remover to fail removing sstL1
	return concreteEngine.compactor
}

// createDummySSTable is a helper to create a dummy SSTable for input to mergeMultipleSSTables.
func createDummySSTable(t *testing.T, eng StorageEngineInterface, id uint64, entries []testEntry) *sstable.SSTable {
	t.Helper()
	// dir := eng.GetDataDir()
	concreteEngine := eng.(*storageEngine)
	dir := concreteEngine.sstDir

	filePath := filepath.Join(dir, fmt.Sprintf("%d.sst", id))
	writerOpts := core.SSTableWriterOptions{
		DataDir:                      dir,
		ID:                           id,
		EstimatedKeys:                uint64(len(entries)),
		BloomFilterFalsePositiveRate: 0.01,
		BlockSize:                    sstable.DefaultBlockSize, // Use default block size
		Tracer:                       trace.NewNoopTracerProvider().Tracer("test"),
		Compressor:                   &compressors.NoCompressionCompressor{},
	}
	writer, err := sstable.NewSSTableWriter(writerOpts)
	if err != nil {
		t.Fatalf("Failed to create dummy SSTable writer: %v", err)
	}
	concreteEngine, ok := eng.(*storageEngine)
	if !ok {
		t.Fatalf("Failed to assert StorageEngineInterface to *storageEngine for test setup")
	}
	for _, entry := range entries {
		metricID, _ := concreteEngine.stringStore.GetOrCreateID(entry.metric)
		var encodedTags []core.EncodedSeriesTagPair
		if entry.tags != nil {
			for k, v := range entry.tags {
				keyID, _ := concreteEngine.stringStore.GetOrCreateID(k)
				valID, _ := concreteEngine.stringStore.GetOrCreateID(v)
				encodedTags = append(encodedTags, core.EncodedSeriesTagPair{KeyID: keyID, ValueID: valID})
			}
		}
		sort.Slice(encodedTags, func(i, j int) bool { return encodedTags[i].KeyID < encodedTags[j].KeyID })

		ts := entry.ts
		if ts == 0 {
			ts = int64(entry.seqNum)
		}

		tsdbKey := core.EncodeTSDBKey(metricID, encodedTags, ts)
		// Encode value as FieldValues for EntryTypePutEvent
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
		t.Fatalf("Failed to finish dummy SSTable: %v", err)
	}
	loadOpts := sstable.LoadSSTableOptions{FilePath: filePath, ID: id}
	sst, err := sstable.LoadSSTable(loadOpts)
	if err != nil {
		t.Fatalf("Failed to load dummy SSTable: %v", err)
	}
	return sst
}

func createDummySSTableWithTombstones(t *testing.T, eng StorageEngineInterface, id uint64, entries []testEntryWithTombstone) *sstable.SSTable {
	t.Helper()
	// dir := eng.GetDataDir()
	concreteEng := eng.(*storageEngine)
	dir := concreteEng.sstDir

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
	if err != nil {
		t.Fatalf("Failed to create dummy SSTable writer: %v", err)
	}

	concreteEng, ok := eng.(*storageEngine)
	if !ok {
		t.Fatal("Failed to assert StorageEngineInterface to *storageEngine")
	}
	for _, entry := range entries {
		metricID, _ := concreteEng.stringStore.GetOrCreateID(entry.metric)
		encodedTags := encodeTags(eng, entry.tags)
		tsdbKey := core.EncodeTSDBKey(metricID, encodedTags, entry.ts)

		var valueBytes []byte
		if entry.entryType == core.EntryTypePutEvent {
			fields, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": entry.value})
			if err != nil {
				t.Fatalf("failed to create fields for tombstone test: %v", err)
			}
			valueBytes, err = fields.Encode()
			if err != nil {
				t.Fatalf("failed to encode fields for tombstone test: %v", err)
			}
		} else {
			valueBytes = []byte(entry.value) // For tombstones, value is often empty
		}
		writer.Add(tsdbKey, valueBytes, entry.entryType, entry.seqNum)
	}
	if err := writer.Finish(); err != nil {
		t.Fatalf("Failed to finish dummy SSTable: %v", err)
	}
	loadOpts := sstable.LoadSSTableOptions{FilePath: writer.FilePath(), ID: id}
	sst, err := sstable.LoadSSTable(loadOpts)
	if err != nil {
		t.Fatalf("Failed to load dummy SSTable: %v", err)
	}
	return sst
}

// verifySSTableContent is a helper to verify the content of a set of SSTables.
// It creates a MergingIterator over the provided tables and compares the iterated
// entries with the expected data.
func verifySSTableContent(t *testing.T, tables []*sstable.SSTable, expectedData map[string]string, eng StorageEngineInterface) {
	t.Helper()
	var iters []core.IteratorInterface[*core.IteratorNode]
	for _, tbl := range tables { // Pass nil for semaphore in test verification
		iter, err := tbl.NewIterator(nil, nil, nil, types.Ascending)
		if err != nil {
			t.Fatalf("Failed to create iterator for table %d: %v", tbl.ID(), err)
		}
		iters = append(iters, iter)
	}

	// Use a MergingIterator to get the combined, sorted, latest view of data
	mergeParams := iterator.MergingIteratorParams{
		Iters:                iters,
		IsSeriesDeleted:      func(b []byte, u uint64) bool { return false },
		IsRangeDeleted:       func(b []byte, i int64, u uint64) bool { return false },
		ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
		DecodeTsFunc:         core.DecodeTimestamp,
	}
	mergedIter, err := iterator.NewMergingIteratorWithTombstones(mergeParams)
	if err != nil {
		t.Fatalf("Failed to create merging iterator: %v", err)
	}
	defer mergedIter.Close()

	actualData := make(map[string]string)
	for mergedIter.Next() {
		// keyBytes, valueBytes, entryType, _ := mergedIter.At()
		cur, err := mergedIter.At()
		require.NoError(t, err)
		keyBytes, valueBytes, entryType, _ := cur.Key, cur.Value, cur.EntryType, cur.SeqNum

		if entryType == core.EntryTypePutEvent {
			decodedFields, err := core.DecodeFieldsFromBytes(valueBytes)
			if err != nil {
				t.Fatalf("failed to decode fields during verification: %v", err)
			}
			// Assuming we only care about the "value" field for these tests
			if val, ok := decodedFields["value"]; ok {
				if strVal, okStr := val.ValueString(); okStr {
					actualData[string(keyBytes)] = strVal
				} else {
					t.Fatalf("field 'value' is not a string for key %x", keyBytes)
				}
			} else {
				t.Fatalf("field 'value' not found for key %x", keyBytes)
			}
		}
	}
	if err := mergedIter.Error(); err != nil {
		t.Fatalf("Merging iterator error: %v", err)
	}

	// Improved map comparison for better test failure output.
	if len(actualData) != len(expectedData) {
		t.Errorf("SSTable content count mismatch: got %d, want %d", len(actualData), len(expectedData))
	}

	for k, expectedV := range expectedData {
		actualV, ok := actualData[k]
		if !ok {
			t.Errorf("Expected key not found in actual data: %x", []byte(k))
			continue // Avoid panic on next check
		}
		if actualV != expectedV {
			t.Errorf("Value mismatch for key %x: got %q, want %q", []byte(k), actualV, expectedV)
		}
	}
	for k := range actualData {
		if _, ok := expectedData[k]; !ok {
			t.Errorf("Unexpected key found in actual data: %x", []byte(k))
		}
	}
}

func TestCompactionManager_Merge_WriterAddError(t *testing.T) {
	mockNow := time.Date(2024, 7, 15, 12, 0, 0, 0, time.UTC)
	mockWriter := &MockSSTableWriter{
		failAdd:         true,
		addErr:          fmt.Errorf("simulated add error"),
		currentSizeFunc: func() int64 { return 0 }, // Prevent rollover logic from triggering
	}
	cmIface := setupCompactionManagerWithMockWriter(t, mockWriter, clock.NewMockClock(mockNow))
	cm := cmIface.(*CompactionManager)

	inputSST := createDummySSTable(t, cm.Engine, 101, []testEntry{
		{metric: "merge.test.metric", tags: map[string]string{"id": "1"}, value: "val1"},
	})
	t.Cleanup(func() { inputSST.Close(); os.Remove(inputSST.FilePath()) })

	_, err := cm.mergeMultipleSSTables(context.Background(), []*sstable.SSTable{inputSST}, 1)
	if err == nil {
		t.Fatal("Expected an error from mergeMultipleSSTables due to writer.Add failure, got nil")
	}
	if !assert.ErrorContains(t, err, mockWriter.addErr.Error()) {
		t.Errorf("Expected error to contain '%s', got '%s'", mockWriter.addErr.Error(), err.Error())
	}
}

func TestCompactionManager_Merge_WriterFinishError(t *testing.T) {
	mockWriter := &MockSSTableWriter{failFinish: true, finishErr: fmt.Errorf("simulated finish error")}
	mockNow := time.Date(2024, 7, 15, 12, 0, 0, 0, time.UTC)
	cmIface := setupCompactionManagerWithMockWriter(t, mockWriter, clock.NewMockClock(mockNow))
	cm := cmIface.(*CompactionManager)

	inputSST := createDummySSTable(t, cm.Engine, 101, []testEntry{
		{metric: "merge.test.metric", tags: map[string]string{"id": "1"}, value: "val1"},
		{metric: "merge.test.metric", tags: map[string]string{"id": "2"}, value: "val2"},
	})
	t.Cleanup(func() { inputSST.Close(); os.Remove(inputSST.FilePath()) })

	// To trigger Finish, we need enough data to cause a rollover or the final flush.
	// Mock CurrentSize() to be large enough to trigger rollover after first Add.
	mockWriter.currentSizeFunc = func() int64 { return cm.opts.TargetSSTableSize + 1 }

	_, err := cm.mergeMultipleSSTables(context.Background(), []*sstable.SSTable{inputSST}, 1)
	if err == nil {
		t.Fatal("Expected an error from mergeMultipleSSTables due to writer.Finish failure, got nil")
	}
	if !assert.ErrorContains(t, err, mockWriter.finishErr.Error()) {
		t.Errorf("Expected error to contain '%s', got '%s'", mockWriter.finishErr.Error(), err.Error())
	}
}

func TestCompactionManager_Merge_LoadSSTableError(t *testing.T) {
	mockWriter := &MockSSTableWriter{failLoad: true} // This mock will cause LoadSSTable to fail
	mockNow := time.Date(2024, 7, 15, 12, 0, 0, 0, time.UTC)
	cmIface := setupCompactionManagerWithMockWriter(t, mockWriter, clock.NewMockClock(mockNow))
	cm := cmIface.(*CompactionManager)

	inputSST := createDummySSTable(t, cm.Engine, 101, []testEntry{
		{metric: "merge.test.metric", tags: map[string]string{"id": "1"}, value: "val1"},
		{metric: "merge.test.metric", tags: map[string]string{"id": "2"}, value: "val2"},
	})
	t.Cleanup(func() { inputSST.Close(); os.Remove(inputSST.FilePath()) })

	// To trigger LoadSSTable, we need enough data to cause a rollover.
	mockWriter.currentSizeFunc = func() int64 { return cm.opts.TargetSSTableSize + 1 }

	_, err := cm.mergeMultipleSSTables(context.Background(), []*sstable.SSTable{inputSST}, 1)
	if err == nil {
		t.Fatal("Expected an error from mergeMultipleSSTables due to LoadSSTable failure, got nil")
	}
	if !assert.ErrorContains(t, err, "failed to load newly created sstable") {
		t.Errorf("Expected error to indicate LoadSSTable failure, got '%s'", err.Error())
	}
	// Clean up the dummy corrupted file created by the mock factory
	if mockWriter.filePath != "" {
		os.Remove(mockWriter.filePath)
	}
}

func TestCompactionManager_CompactL0ToL1_RemoveOldSSTableError(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	dummyEngine, err := NewStorageEngine(StorageEngineOptions{
		DataDir:           tempDir,
		Logger:            logger,
		MaxLevels:         3,
		MaxL0Files:        2,
		TargetSSTableSize: 1024,
	})

	require.NoError(t, err)

	concreteEngine := dummyEngine.(*storageEngine)

	// 2. Set up the compactor factory BEFORE starting the engine.
	concreteEngine.setCompactorFactory = func(seo StorageEngineOptions, se *storageEngine) (CompactionManagerInterface, error) {
		cmParams := CompactionManagerParams{
			LevelsManager: se.levelsManager,
			DataDir:       se.sstDir,
			Opts: CompactionOptions{
				TargetSSTableSize:          100,
				LevelsTargetSizeMultiplier: 2,    // Add this back
				CompactionIntervalSeconds:  3600, // Disable auto compaction
				SSTableCompressor:          &compressors.NoCompressionCompressor{},
			},
			Logger:               se.logger,
			Tracer:               trace.NewNoopTracerProvider().Tracer("test"),
			IsSeriesDeleted:      func(b []byte, u uint64) bool { return false },
			IsRangeDeleted:       func(b []byte, i int64, u uint64) bool { return false },
			ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
			FileRemover: &mockFileRemover{
				failPaths: make(map[string]error),
			},
			SSTableWriterFactory: func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
				return sstable.NewSSTableWriter(opts) // Use real writer for this test
			},
			Engine: se,
		}
		return NewCompactionManager(cmParams)
	}

	// 3. Start the engine. This initializes the compactor and resets ID counters.
	require.NoError(t, dummyEngine.Start())
	t.Cleanup(func() { dummyEngine.Close() })

	lm := concreteEngine.levelsManager
	cmIface := concreteEngine.compactor
	cm := cmIface.(*CompactionManager)

	// 4. Create L0 files. Now GetNextSSTableID will start from 1.
	sst1ID := dummyEngine.GetNextSSTableID() // Will be 1
	sst2ID := dummyEngine.GetNextSSTableID() // Will be 2
	sst1 := createDummySSTable(t, cm.Engine, sst1ID, []testEntry{
		{metric: "compaction.test.metric", tags: map[string]string{"host": "a"}, value: "1"},
	})
	sst2 := createDummySSTable(t, cm.Engine, sst2ID, []testEntry{
		{metric: "compaction.test.metric", tags: map[string]string{"host": "b"}, value: "2"},
	})
	// Cleanup is now managed by the test's tempDir cleanup.
	// We still need to close the sstables.
	t.Cleanup(func() {
		sst1.Close()
		sst2.Close()
	})
	// 5. Now that we have the final path for sst1, configure the mock remover to fail.
	mockRemover := cm.fileRemover.(*mockFileRemover)
	mockRemover.failPaths[sst1.FilePath()] = errMockRemove

	lm.AddL0Table(sst1) // This one will fail to be removed by the mock
	lm.AddL0Table(sst2) // This one will be removed successfully

	// 6. Trigger compaction and verify the error
	// The compaction should succeed overall, even if cleanup fails. The error is logged.
	err = cm.compactL0ToL1(context.Background())
	// The error from removeAndCleanupSSTables is now logged but not returned, so the overall task succeeds.
	require.NoError(t, err, "Expected compactL0ToL1 to succeed even if cleanup of an old file fails")

	// 7. Verify the final state
	// Verify that the failing file is still present on disk
	_, statErr := os.Stat(sst1.FilePath())
	require.NoError(t, statErr, "Expected file that failed to be removed (%s) to still exist, but it was removed.", sst1.FilePath())

	// Verify that the successfully removed file is gone from disk
	_, statErr = os.Stat(sst2.FilePath())
	require.ErrorIs(t, statErr, os.ErrNotExist, "Expected file %s to be removed, but it still exists.", sst2.FilePath())

	// Verify the new state of the levels manager (in-memory state)
	l0Tables := lm.GetTablesForLevel(0)
	require.Empty(t, l0Tables, "Expected L0 to be empty after compaction")

	l1Tables := lm.GetTablesForLevel(1)
	require.Len(t, l1Tables, 1, "Expected 1 new SSTable in L1")

	expectedNewID := sst2ID + 1
	require.Equal(t, expectedNewID, l1Tables[0].ID(), "New SSTable ID in L1 is incorrect")
}

func TestCompactionManager_CompactLNToLNPlus1_RemoveOldSSTableError(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.Default()

	dummyEngine, err := NewStorageEngine(StorageEngineOptions{
		DataDir:           tempDir,
		MaxLevels:         3,
		MaxL0Files:        2,
		TargetSSTableSize: 1024,
	})
	if err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}

	concreteEngine := dummyEngine.(*storageEngine)
	// pre setting SSTable ID
	concreteEngine.nextSSTableID.Store(100)

	concreteEngine.setCompactorFactory = func(seo StorageEngineOptions, se *storageEngine) (CompactionManagerInterface, error) {
		cmParams := CompactionManagerParams{
			LevelsManager: se.levelsManager,
			DataDir:       se.sstDir,
			Opts: CompactionOptions{
				MaxL0Files:                 2,
				TargetSSTableSize:          100,
				LevelsTargetSizeMultiplier: 2,    // Add this back
				CompactionIntervalSeconds:  3600, // Disable auto compaction
				SSTableCompressor:          &compressors.NoCompressionCompressor{},
			},
			Logger:               logger,
			Tracer:               trace.NewNoopTracerProvider().Tracer("test"),
			IsSeriesDeleted:      func(b []byte, u uint64) bool { return false },
			IsRangeDeleted:       func(b []byte, i int64, u uint64) bool { return false },
			ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
			FileRemover: &mockFileRemover{
				failPaths: make(map[string]error),
			},
			SSTableWriterFactory: func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
				return sstable.NewSSTableWriter(opts) // Use real writer for this test
			},
			Engine: se,
		}
		return NewCompactionManager(cmParams)

	}

	if err := dummyEngine.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	t.Cleanup(func() { dummyEngine.Close() })

	lm := concreteEngine.levelsManager
	cmIface := concreteEngine.compactor
	cm := cmIface.(*CompactionManager)

	// Create L1 and L2 files

	sstL1ID := dummyEngine.GetNextSSTableID()
	sstL2ID := dummyEngine.GetNextSSTableID()

	sstL1 := createDummySSTable(t, dummyEngine, sstL1ID, []testEntry{
		{metric: "metric.ln", tags: map[string]string{"id": "c"}, value: "3"},
	})
	// This table does NOT overlap with sstL1 and should remain untouched in L2.
	sstL2_no_overlap := createDummySSTable(t, dummyEngine, sstL2ID, []testEntry{
		{metric: "metric.ln", tags: map[string]string{"id": "z"}, value: "4"},
	})
	t.Cleanup(func() { sstL1.Close() })
	t.Cleanup(func() { sstL2_no_overlap.Close() })

	mockRemover := cm.fileRemover.(*mockFileRemover)
	mockRemover.failPaths[sstL1.FilePath()] = errMockRemove

	lm.GetLevels()[1].SetTables([]*sstable.SSTable{sstL1})
	lm.GetLevels()[2].SetTables([]*sstable.SSTable{sstL2_no_overlap})

	// The compaction should succeed overall, even if cleanup fails. The error is logged.
	err = cm.compactLevelNToLevelNPlus1(context.Background(), 1)
	require.NoError(t, err, "Expected compactLevelNToLevelNPlus1 to succeed even if cleanup of an old file fails")

	// Verify that the failing file (sstL1) is still present on disk
	if _, err := os.Stat(sstL1.FilePath()); os.IsNotExist(err) {
		t.Errorf("Expected file %s to still exist, but it was removed.", sstL1.FilePath())
	}

	// Verify the non-overlapping L2 table still exists on disk
	if _, err := os.Stat(sstL2_no_overlap.FilePath()); os.IsNotExist(err) {
		t.Errorf("Expected non-overlapping file %s to still exist, but it was removed.", sstL2_no_overlap.FilePath())
	}

	// Verify the new state of the levels manager
	l1Tables := lm.GetTablesForLevel(1)
	if len(l1Tables) != 0 {
		t.Errorf("Expected L1 to be empty after compaction, but it has %d tables", len(l1Tables))
	}
	l2Tables := lm.GetTablesForLevel(2)
	if len(l2Tables) != 2 {
		t.Fatalf("Expected 2 SSTables in L2 (original + new), got %d", len(l2Tables))
	}
}

func TestCompactionManager_CompactLNToLNPlus1_Success(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.Default()

	// Setup a dummy engine to get a StringStore, ensure it uses the same data directory
	dummyEngine, err := NewStorageEngine(StorageEngineOptions{
		DataDir:           tempDir,
		MaxLevels:         3,
		MaxL0Files:        2,
		TargetSSTableSize: 1024,
	})
	if err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}
	concreteEngine, ok := dummyEngine.(*storageEngine)
	if !ok {
		t.Fatal("Failed to assert StorageEngineInterface to *storageEngine")
	}

	concreteEngine.setCompactorFactory = func(seo StorageEngineOptions, se *storageEngine) (CompactionManagerInterface, error) {
		cmParams := CompactionManagerParams{
			LevelsManager: se.levelsManager,
			DataDir:       se.sstDir,
			Opts: CompactionOptions{
				MaxL0Files:                 2,
				TargetSSTableSize:          100,
				LevelsTargetSizeMultiplier: 2,    // Add this back
				CompactionIntervalSeconds:  3600, // Disable auto compaction
				SSTableCompressor:          &compressors.NoCompressionCompressor{},
			},
			Logger:               logger,
			Tracer:               trace.NewNoopTracerProvider().Tracer("test"),
			IsSeriesDeleted:      func(b []byte, u uint64) bool { return false },
			IsRangeDeleted:       func(b []byte, i int64, u uint64) bool { return false },
			ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
			FileRemover: &mockFileRemover{
				failPaths: map[string]error{},
			},
			SSTableWriterFactory: func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
				return sstable.NewSSTableWriter(opts) // Use real writer for this test
			},
			Engine: se,
		}
		return NewCompactionManager(cmParams)

	}

	if err := dummyEngine.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	t.Cleanup(func() { dummyEngine.Close() })

	lm := concreteEngine.levelsManager
	cm := concreteEngine.compactor.(*CompactionManager)

	// --- Prepare test data ---
	// Create an SSTable in L1 (source for compaction)
	// This table will be picked by PickCompactionCandidateForLevelN
	sstL1_source := createDummySSTable(t, cm.Engine, dummyEngine.GetNextSSTableID(), []testEntry{
		{metric: "metricA", tags: map[string]string{"id": "1"}, ts: 100, value: "valA1", seqNum: 100},
		{metric: "metricA", tags: map[string]string{"id": "2"}, ts: 101, value: "valA2_old", seqNum: 101}, // Old version
		{metric: "metricA", tags: map[string]string{"id": "3"}, ts: 102, value: "valA3", seqNum: 102},
		{metric: "metricA", tags: map[string]string{"id": "4"}, ts: 103, value: "valA4_old", seqNum: 103}, // Old version
		{metric: "metricA", tags: map[string]string{"id": "5"}, ts: 104, value: "valA5_old", seqNum: 104}, // Old version
		{metric: "metricA", tags: map[string]string{"id": "6"}, ts: 105, value: "valA6_old", seqNum: 105}, // Old version
	})
	defer sstL1_source.Close()

	// Create SSTables in L2 that overlap with sstL1_source
	sstL2_overlap1 := createDummySSTable(t, cm.Engine, dummyEngine.GetNextSSTableID(), []testEntry{
		{metric: "metricA", tags: map[string]string{"id": "2"}, ts: 101, value: "valA2_new", seqNum: 201}, // Same timestamp as valA2_old, but higher seqNum
		{metric: "metricA", tags: map[string]string{"id": "4"}, ts: 103, value: "valA4_new", seqNum: 202}, // Same timestamp as valA4_old, but higher seqNum
	})
	defer sstL2_overlap1.Close()

	sstL2_overlap2 := createDummySSTable(t, cm.Engine, dummyEngine.GetNextSSTableID(), []testEntry{
		{metric: "metricA", tags: map[string]string{"id": "5"}, ts: 104, value: "valA5_new", seqNum: 203}, // Same timestamp as valA5_old, but higher seqNum
		{metric: "metricA", tags: map[string]string{"id": "6"}, ts: 105, value: "valA6_new", seqNum: 204}, // Same timestamp as valA6_old, but higher seqNum
	})
	defer sstL2_overlap2.Close()

	// Add tables to LevelsManager
	lm.GetLevels()[1].SetTables([]*sstable.SSTable{sstL1_source})
	lm.GetLevels()[2].SetTables([]*sstable.SSTable{sstL2_overlap1, sstL2_overlap2})

	// --- Perform Compaction ---
	t.Log("Starting L1->L2 compaction...")
	err = cm.compactLevelNToLevelNPlus1(context.Background(), 1)
	if err != nil {
		t.Fatalf("compactLevelNToLevelNPlus1 failed: %v", err)
	}
	t.Log("L1->L2 compaction completed.")

	// --- Verify results ---

	// 1. Verify source level (L1) is empty and target level (L2) contains new tables
	l1Tables := lm.GetTablesForLevel(1)
	if len(l1Tables) != 0 {
		t.Errorf("Expected L1 to be empty after compaction, got %d tables: %v", len(l1Tables), getTableIDs(l1Tables))
	}

	// 2. Verify old tables are removed from disk
	l2Tables := lm.GetTablesForLevel(2)
	if len(l2Tables) == 0 {
		t.Errorf("Expected L2 to contain new tables after compaction, got 0")
	}
	// Check that old tables are removed from disk
	if _, err := os.Stat(sstL1_source.FilePath()); !os.IsNotExist(err) {
		t.Errorf("Old L1 source table %s was not removed from disk. Error: %v", sstL1_source.FilePath(), err)
	}
	if _, err := os.Stat(sstL2_overlap1.FilePath()); !os.IsNotExist(err) {
		t.Errorf("Old L2 overlapping table %s was not removed from disk", sstL2_overlap1.FilePath())
	}
	if _, err := os.Stat(sstL2_overlap2.FilePath()); !os.IsNotExist(err) {
		t.Errorf("Old L2 overlapping table %s was not removed from disk", sstL2_overlap2.FilePath())
	}

	// 3. Verify content of the new L2 tables.
	// The verification helper needs the engine to decode keys, but for creating the expected map,
	// we need to encode keys using the same string store as the test.
	expectedData := make(map[string]string)

	metricID, _ := concreteEngine.GetStringStore().GetOrCreateID("metricA")
	idTagKeyID, _ := concreteEngine.GetStringStore().GetOrCreateID("id")
	valIDs := make(map[string]uint64)
	for i := 1; i <= 6; i++ {
		valStr := fmt.Sprintf("%d", i)
		valIDs[valStr], _ = concreteEngine.GetStringStore().GetOrCreateID(valStr)
	}

	expectedData[string(core.EncodeTSDBKey(metricID, []core.EncodedSeriesTagPair{{KeyID: idTagKeyID, ValueID: valIDs["1"]}}, 100))] = "valA1"
	expectedData[string(core.EncodeTSDBKey(metricID, []core.EncodedSeriesTagPair{{KeyID: idTagKeyID, ValueID: valIDs["2"]}}, 101))] = "valA2_new"
	expectedData[string(core.EncodeTSDBKey(metricID, []core.EncodedSeriesTagPair{{KeyID: idTagKeyID, ValueID: valIDs["3"]}}, 102))] = "valA3"
	expectedData[string(core.EncodeTSDBKey(metricID, []core.EncodedSeriesTagPair{{KeyID: idTagKeyID, ValueID: valIDs["4"]}}, 103))] = "valA4_new"
	expectedData[string(core.EncodeTSDBKey(metricID, []core.EncodedSeriesTagPair{{KeyID: idTagKeyID, ValueID: valIDs["5"]}}, 104))] = "valA5_new"
	expectedData[string(core.EncodeTSDBKey(metricID, []core.EncodedSeriesTagPair{{KeyID: idTagKeyID, ValueID: valIDs["6"]}}, 105))] = "valA6_new"

	// Use a MergingIterator over the new L2 tables to verify content
	verifySSTableContent(t, l2Tables, expectedData, cm.Engine)
}

func TestEngine_Compaction_WithRetentionPolicy(t *testing.T) {
	tempDir := t.TempDir()

	// Define a fixed "now" for the test and a retention period
	mockNow := time.Now() //time.Date(2024, 7, 15, 12, 0, 0, 0, time.UTC)
	clk := clock.NewMockClock(mockNow)
	retentionPeriod := "30m"
	retentionDuration, _ := time.ParseDuration(retentionPeriod)
	cutoffTime := mockNow.Add(-retentionDuration)

	// Define data points inside and outside the retention window
	tsOld := cutoffTime.Add(-1 * time.Hour).UnixNano()  // Should be deleted
	tsNew := cutoffTime.Add(1 * time.Minute).UnixNano() // Should be kept

	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            1, // Small threshold to force flushes
		MaxLevels:                    3,
		MaxL0Files:                   2,
		TargetSSTableSize:            1024,
		CompactionIntervalSeconds:    3600, // Disable auto-compaction
		RetentionPeriod:              retentionPeriod,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		BloomFilterFalsePositiveRate: 0.01,
		Metrics:                      NewEngineMetrics(false, "retention_e2e_"),
		Clock:                        clk,
		Logger:                       slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})).With("component", "StorageEngine"),
	}

	// --- Phase 1: Create first L0 file with old data ---
	engine1, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("Failed to create engine1: %v", err)
	}

	if err := engine1.Start(); err != nil {
		t.Fatalf("Failed to start engine1: %v", err)
	}

	point, err := core.NewSimpleDataPoint(
		"metric.retention",
		map[string]string{"age": "old"},
		tsOld, // Should be deleted
		map[string]any{"value": 91.0},
	)

	if err != nil {
		t.Fatalf("Failed to create data point: %v", err)
	}

	if err := engine1.Put(context.Background(), *point); err != nil {
		t.Fatalf("Failed to put old data point: %v", err)
	}

	if err := engine1.Close(); err != nil {
		t.Fatalf("Failed to close engine1: %v", err)
	}

	// fmt.Println("_------------------------------------------------")

	// Advance the clock to ensure the next SSTable gets a unique ID
	opts.Clock = clock.NewMockClock(mockNow.Add(1 * time.Second))

	// --- Phase 2: Create second L0 file with new data ---
	engine2, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("Failed to create engine2: %v", err)
	}
	if err := engine2.Start(); err != nil {
		t.Fatalf("Failed to start engine2: %v", err)
	}

	point2, err := core.NewSimpleDataPoint(
		"metric.retention",
		map[string]string{"age": "new"},
		tsNew, // Should be kept
		map[string]any{"value": 2.0},
	)

	if err != nil {
		t.Fatalf("Failed to create data point: %v", err)
	}

	if err := engine2.Put(context.Background(), *point2); err != nil {
		t.Fatalf("Failed to put new data point: %v", err)
	}

	if err := engine2.Close(); err != nil {
		t.Fatalf("Failed to close engine2: %v", err)
	}

	// fmt.Println("_------------------------------------------------")
	// --- Phase 3: Reopen engine and trigger compaction ---
	engine3, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("Failed to create engine3: %v", err)
	}

	if err := engine3.Start(); err != nil {
		t.Fatalf("Failed to start engine3: %v", err)
	}
	defer engine3.Close()

	// Inject the mock time into the compactor
	concreteEngine3, ok := engine3.(*storageEngine)
	if !ok {
		t.Fatal("Failed to assert StorageEngineInterface to *storageEngine")
	}

	// In Phase 3, before calling compactL0ToL1
	// t.Logf("--- LevelsManager State before compaction ---")
	_, unlockFunc := concreteEngine3.levelsManager.GetSSTablesForRead()
	//for levelNum, level := range levelsState {
	//	t.Logf("Level %d: %d tables", levelNum, len(level.GetTables()))
	//	for i, tbl := range level.GetTables() {
	//		t.Logf("  Table %d (ID %d): MinKey='%x', MaxKey='%x', Size=%d bytes, Path='%s'", i, tbl.ID(), string(tbl.MinKey()), string(tbl.MaxKey()), tbl.Size(), tbl.FilePath())
	//		// ถ้าอยากเห็น Timestamp จาก Min/Max Key
	//		minTs, _ := core.DecodeTimestamp(tbl.MinKey()[len(tbl.MinKey())-8:])
	//		maxTs, _ := core.DecodeTimestamp(tbl.MaxKey()[len(tbl.MaxKey())-8:])
	//		t.Logf("    MinTs: %x (%d), MaxTs: %x (%d)", time.Unix(0, minTs).Format(time.RFC3339Nano), minTs, time.Unix(0, maxTs).Format(time.RFC3339Nano), maxTs)
	//	}
	//}
	unlockFunc()

	// Manually trigger a synchronous compaction
	clk.SetTime(mockNow.Add(1 * time.Minute))
	if err := concreteEngine3.compactor.(*CompactionManager).compactL0ToL1(context.Background()); err != nil {
		t.Fatalf("Manual compaction failed: %v", err)
	}

	//t.Logf("--- LevelsManager State after compaction ---")
	_, unlockFuncAfter := concreteEngine3.levelsManager.GetSSTablesForRead()
	// for levelNum, level := range levelsStateAfter {
	// 	t.Logf("Level %d: %d tables", levelNum, len(level.GetTables()))
	// 	for i, tbl := range level.GetTables() {
	// 		t.Logf("  Table %d (ID %d): MinKey='%x', MaxKey='%x', Size=%d bytes, Path='%s'", i, tbl.ID(), string(tbl.MinKey()), string(tbl.MaxKey()), tbl.Size(), tbl.FilePath())
	// 	}
	// }
	unlockFuncAfter()
	//t.Logf("------------------------------------------")
	// --- Phase 4: Verification ---
	// Verify old data is gone
	_, err = engine3.Get(context.Background(), "metric.retention", map[string]string{"age": "old"}, tsOld)

	if !errors.Is(err, sstable.ErrNotFound) {
		t.Errorf("Expected old data point to be deleted by retention policy, but got err: %v", err)
	}

	// Verify new data is still present
	dp, err := engine3.Get(context.Background(), "metric.retention", map[string]string{"age": "new"}, tsNew)
	if err != nil {
		t.Errorf("Expected new data point to exist, but got err: %v", err)
	}
	if val := HelperFieldValueValidateFloat64(t, dp, "value"); math.Abs(val-2.0) > 1e-9 {

		t.Errorf("Value mismatch for new data point: got %v, want 2.0", val)
	}

	// Verify L0 is now empty and L1 has one table
	if len(concreteEngine3.levelsManager.GetTablesForLevel(0)) != 0 {
		t.Errorf("Expected L0 to be empty after compaction, but found %d tables: %v", len(concreteEngine3.levelsManager.GetTablesForLevel(0)), concreteEngine3.levelsManager.GetTablesForLevel(0))
	}
	if len(concreteEngine3.levelsManager.GetTablesForLevel(1)) != 1 {
		t.Errorf("Expected L1 to have 1 table after compaction, but found %d", len(concreteEngine3.levelsManager.GetTablesForLevel(1)))
	}
}

func TestCompactionManager_Merge_WithTombstones(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.Default()

	// Setup LevelsManager
	/*lm, _ := levels.NewLevelsManager(3, 4, 1024, trace.NewNoopTracerProvider().Tracer("test"))
	defer lm.Close()*/

	// Setup a dummy engine to get a StringStore, ensure it uses the same data directory
	dummyEngine, err := NewStorageEngine(StorageEngineOptions{
		DataDir:           tempDir,
		MaxLevels:         3,
		MaxL0Files:        2,
		TargetSSTableSize: 1024,
	})
	if err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}

	concreteEng, ok := dummyEngine.(*storageEngine)
	if !ok {
		t.Fatal("Failed to assert StorageEngineInterface to *storageEngine")
	}

	concreteEng.setCompactorFactory = func(seo StorageEngineOptions, se *storageEngine) (CompactionManagerInterface, error) {
		// Setup CompactionManager
		cmParams := CompactionManagerParams{
			LevelsManager: se.levelsManager,
			DataDir:       se.sstDir,
			Opts: CompactionOptions{
				TargetSSTableSize: 1024 * 1024, // Large enough to produce one output table
				SSTableCompressor: &compressors.NoCompressionCompressor{},
			},
			Logger:               logger,
			Tracer:               trace.NewNoopTracerProvider().Tracer("test"),
			IsSeriesDeleted:      func(b []byte, u uint64) bool { return false },
			IsRangeDeleted:       func(b []byte, i int64, u uint64) bool { return false },
			ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
			// FileRemover:          &mockFileRemover{failPaths: map[string]error{}},
			SSTableWriterFactory: func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
				return sstable.NewSSTableWriter(opts)
			},
			Engine: se,
		}
		return NewCompactionManager(cmParams)
	}

	if err = dummyEngine.Start(); err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}
	lm := concreteEng.levelsManager
	cm := concreteEng.compactor.(*CompactionManager)

	t.Cleanup(func() { dummyEngine.Close() })

	// --- Prepare test data with tombstones ---
	// L0 Table 1
	sstL0_1 := createDummySSTableWithTombstones(t, concreteEng, concreteEng.GetNextSSTableID(), []testEntryWithTombstone{
		{metric: "series.a", ts: 100, value: "v1_old", seqNum: 10, entryType: core.EntryTypePutEvent},
		{metric: "series.b", ts: 200, value: "v1_old", seqNum: 20, entryType: core.EntryTypePutEvent},
		{metric: "series.c", ts: 300, value: "v1", seqNum: 30, entryType: core.EntryTypePutEvent},
	})
	defer sstL0_1.Close()

	// L0 Table 2
	sstL0_2 := createDummySSTableWithTombstones(t, concreteEng, concreteEng.GetNextSSTableID(), []testEntryWithTombstone{
		{metric: "series.a", ts: 100, value: "", seqNum: 15, entryType: core.EntryTypeDelete},         // Hides v1_old
		{metric: "series.b", ts: 200, value: "v2_new", seqNum: 25, entryType: core.EntryTypePutEvent}, // Newer than v1_old
		{metric: "series.d", ts: 400, value: "v1", seqNum: 40, entryType: core.EntryTypePutEvent},
	})
	defer sstL0_2.Close()

	// L1 Overlapping Table
	sstL1_overlap := createDummySSTableWithTombstones(t, concreteEng, concreteEng.GetNextSSTableID(), []testEntryWithTombstone{
		{metric: "series.c", ts: 300, value: "", seqNum: 35, entryType: core.EntryTypeDelete},                  // Hides v1
		{metric: "series.d", ts: 400, value: "", seqNum: 38, entryType: core.EntryTypeDelete},                  // Older than v1, so v1 should survive
		{metric: "series.e", ts: 500, value: "", seqNum: 50, entryType: core.EntryTypeDelete},                  // Standalone tombstone
		{metric: "series.a", ts: 100, value: "v3_reincarnated", seqNum: 18, entryType: core.EntryTypePutEvent}, // Newer than tombstone
	})
	defer sstL1_overlap.Close()

	// Add tables to LevelsManager
	lm.AddL0Table(sstL0_1)
	lm.AddL0Table(sstL0_2)
	lm.AddTableToLevel(1, sstL1_overlap)

	// --- Perform Compaction ---
	t.Log("Starting L0->L1 compaction with tombstones...")
	err = cm.compactL0ToL1(context.Background())
	if err != nil {
		t.Fatalf("compactL0ToL1 failed: %v", err)
	}
	t.Log("L0->L1 compaction completed.")

	// --- Verify results ---
	l1Tables := lm.GetTablesForLevel(1)
	if len(l1Tables) == 0 {
		t.Fatal("Expected L1 to contain new tables after compaction, got 0")
	}

	expectedData := make(map[string]string)
	{
		metricA_ID, _ := concreteEng.stringStore.GetOrCreateID("series.a")
		metricB_ID, _ := concreteEng.stringStore.GetOrCreateID("series.b")
		metricD_ID, _ := concreteEng.stringStore.GetOrCreateID("series.d")

		expectedData[string(core.EncodeTSDBKey(metricA_ID, nil, 100))] = "v3_reincarnated"
		expectedData[string(core.EncodeTSDBKey(metricB_ID, nil, 200))] = "v2_new"
		expectedData[string(core.EncodeTSDBKey(metricD_ID, nil, 400))] = "v1"
	}

	verifySSTableContent(t, l1Tables, expectedData, cm.Engine)
}

// TestCompactionManager_QuarantineCorruptedSSTable simulates a corrupted SSTable
// being encountered during compaction and verifies that it is quarantined.
func TestCompactionManager_QuarantineCorruptedSSTable(t *testing.T) {
	logger := slog.Default()

	dummyEngine, _ := NewStorageEngine(StorageEngineOptions{
		DataDir:           t.TempDir(),
		MaxLevels:         3,
		MaxL0Files:        2,
		TargetSSTableSize: 256,
	})

	concreteEngine := dummyEngine.(*storageEngine)
	concreteEngine.setCompactorFactory = func(seo StorageEngineOptions, se *storageEngine) (CompactionManagerInterface, error) {
		// Configure CompactionManager
		cmParams := CompactionManagerParams{
			LevelsManager: se.levelsManager,
			DataDir:       se.sstDir,
			Opts: CompactionOptions{
				MaxL0Files:                 seo.MaxL0Files, // Trigger L0->L1 compaction
				TargetSSTableSize:          seo.TargetSSTableSize,
				LevelsTargetSizeMultiplier: 2,
				CompactionIntervalSeconds:  3600, // Disable auto-compaction
				SSTableCompressor:          &compressors.NoCompressionCompressor{},
			},
			Logger:               logger,
			Tracer:               trace.NewNoopTracerProvider().Tracer("test"),
			IsSeriesDeleted:      func(b []byte, u uint64) bool { return false },
			IsRangeDeleted:       func(b []byte, i int64, u uint64) bool { return false },
			ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
			// FileRemover:          &mockFileRemover{failPaths: map[string]error{}}, // Real remover behavior
			SSTableWriterFactory: func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
				return sstable.NewSSTableWriter(opts) // Use real writer
			},
			Engine: se,
		}

		return NewCompactionManager(cmParams)
	}
	if err := dummyEngine.Start(); err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}
	t.Cleanup(func() { dummyEngine.Close() })

	cmIface := concreteEngine.compactor
	cm := cmIface.(*CompactionManager)
	lm := concreteEngine.levelsManager

	// --- Phase 1: Create two valid SSTables and add them to L0 ---
	sstID1 := concreteEngine.GetNextSSTableID()
	sstID2 := concreteEngine.GetNextSSTableID()

	validSST := createDummySSTable(t, dummyEngine, sstID1, []testEntry{
		{metric: "metric.valid", value: "v1"},
	})
	// This table will be corrupted after being added to the levels manager
	sstToCorrupt := createDummySSTable(t, dummyEngine, sstID2, []testEntry{
		{metric: "metric.corrupt", value: "v2"},
	})

	lm.AddL0Table(validSST)
	lm.AddL0Table(sstToCorrupt)

	// --- Phase 2: Corrupt one of the SSTable files on disk ---
	// Corrupt the file by modifying its checksum. This will cause an error during block read.
	fileData, err := os.ReadFile(sstToCorrupt.FilePath())
	if err != nil {
		t.Fatalf("Failed to read SSTable file for corruption: %v", err)
	}
	// The first block starts at offset 0. The checksum is after the 1-byte compression flag.
	if len(fileData) > 5 {
		fileData[1]++ // Corrupt the first byte of the checksum
	} else {
		t.Fatal("SSTable file is too small to corrupt.")
	}
	if err := os.WriteFile(sstToCorrupt.FilePath(), fileData, 0644); err != nil {
		t.Fatalf("Failed to write corrupted SSTable file: %v", err)
	}
	t.Logf("Corrupted file %s on disk.", sstToCorrupt.FilePath())

	// --- Phase 3: Trigger compaction and expect quarantine ---
	// The error should be logged, but the compaction function should handle it and not return it,
	// as the quarantine is the recovery mechanism.
	if _, err := os.Stat(validSST.FilePath()); errors.Is(err, os.ErrNotExist) {
		t.Fatalf("file %s does not exist", validSST.FilePath())
	}

	if _, err := os.Stat(sstToCorrupt.FilePath()); errors.Is(err, os.ErrNotExist) {
		t.Fatalf("file %s does not exist", sstToCorrupt.FilePath())
	}

	err = cm.compactL0ToL1(context.Background())
	if err != nil {
		t.Fatalf("compactL0ToL1 returned an unexpected error: %v. Expected nil due to quarantine handling.", err)
	}

	// --- Phase 4: Verify quarantine ---
	// Both input tables should be gone from levels and disk.
	if len(lm.GetTablesForLevel(0)) != 0 {
		t.Errorf("Expected L0 to be empty after quarantine, but found %d tables.", len(lm.GetTablesForLevel(0)))
	}
	if _, err := os.Stat(validSST.FilePath()); !os.IsNotExist(err) {
		t.Errorf("Expected valid input SSTable %s to be removed after quarantine, but it still exists.", validSST.FilePath())
	}
	if _, err := os.Stat(sstToCorrupt.FilePath()); !os.IsNotExist(err) {
		t.Errorf("Expected corrupted SSTable %s to be removed after quarantine, but it still exists.", sstToCorrupt.FilePath())
	}
}

func TestCompactionManager_RetentionPolicy(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.Default()

	// Use a fixed time for the test to be deterministic
	mockNow := time.Now() //time.Date(2024, 7, 15, 12, 0, 0, 0, time.UTC)
	// Set a retention period of 2 hours for this test
	retentionPeriod := "2h"
	retentionDuration, _ := time.ParseDuration(retentionPeriod)
	cutoffTime := mockNow.Add(-retentionDuration).UnixNano()

	// Setup LevelsManager
	lm, _ := levels.NewLevelsManager(3, 2, 1024, trace.NewNoopTracerProvider().Tracer("test"), levels.PickOldest)
	defer lm.Close()

	// Setup a dummy engine to get a StringStore, ensure it uses the same data directory
	dummyEngine, err := NewStorageEngine(StorageEngineOptions{
		DataDir:           tempDir,
		Clock:             clock.NewMockClock(mockNow),
		MaxLevels:         3,
		MaxL0Files:        2,
		TargetSSTableSize: 1024,
	})
	if err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err := dummyEngine.Start(); err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}
	t.Cleanup(func() { dummyEngine.Close() })

	// Setup CompactionManager with the retention policy
	cmParams := CompactionManagerParams{
		LevelsManager: lm,
		DataDir:       tempDir,
		Opts: CompactionOptions{
			MaxL0Files:        2,
			TargetSSTableSize: 1024,
			RetentionPeriod:   retentionPeriod, // Set retention period
			SSTableCompressor: &compressors.NoCompressionCompressor{},
		},
		Logger:               logger,
		Tracer:               trace.NewNoopTracerProvider().Tracer("test"),
		IsSeriesDeleted:      func(b []byte, u uint64) bool { return false },
		IsRangeDeleted:       func(b []byte, i int64, u uint64) bool { return false },
		ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
		FileRemover:          &mockFileRemover{failPaths: map[string]error{}},
		SSTableWriterFactory: func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
			return sstable.NewSSTableWriter(opts)
		},
		Engine: dummyEngine,
	}
	cmIface, err := NewCompactionManager(cmParams)
	if err != nil {
		t.Fatalf("NewCompactionManager failed: %v", err)
	}

	cm := cmIface.(*CompactionManager)
	concreteEngine, ok := dummyEngine.(*storageEngine)

	if !ok {
		t.Fatal("Failed to assert StorageEngineInterface to *storageEngine")
	}
	// Create an input SSTable with data both inside and outside the retention period
	inputEntries := []testEntry{
		{metric: "retention.test", tags: map[string]string{"id": "old"}, ts: cutoffTime - 1000, value: "old_data"},    // Should be deleted
		{metric: "retention.test", tags: map[string]string{"id": "new"}, ts: cutoffTime + 1000, value: "new_data"},    // Should be kept
		{metric: "retention.test", tags: map[string]string{"id": "border"}, ts: cutoffTime, value: "borderline_data"}, // Should be kept
	}
	inputSST := createDummySSTable(t, dummyEngine, 1, inputEntries)
	defer inputSST.Close()

	// Perform the merge/compaction
	newTables, err := cm.mergeMultipleSSTables(context.Background(), []*sstable.SSTable{inputSST}, 1)
	if err != nil {
		t.Fatalf("mergeMultipleSSTables failed: %v", err)
	}
	if len(newTables) != 1 {
		t.Fatalf("Expected 1 new table, got %d", len(newTables))
	}
	defer newTables[0].Close()

	// Verify the content of the new table
	iter, err := newTables[0].NewIterator(nil, nil, nil, types.Ascending)
	if err != nil {
		t.Fatalf("Failed to create iterator for new table: %v", err)
	}
	defer iter.Close()

	var foundKeys []string
	for iter.Next() {
		// key, _, _, _ := iter.At()
		cur, _ := iter.At()
		key := cur.Key

		metricID, _, _ := core.DecodeSeriesKey(key[:len(key)-8])
		metric, _ := concreteEngine.stringStore.GetString(metricID)
		foundKeys = append(foundKeys, metric)
	}

	if len(foundKeys) != 2 {
		t.Errorf("Expected 2 entries to be kept, but found %d. Found keys: %v", len(foundKeys), foundKeys)
	}
}
func TestCompactionManager_RemoveAndCleanupSSTables(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	baseDir := t.TempDir()

	t.Run("Success_AllFilesRemoved", func(t *testing.T) {
		mockRemover := &mockFileRemover{failPaths: make(map[string]error)}
		cm := &CompactionManager{
			logger:      logger,
			fileRemover: mockRemover,
			tracer:      trace.NewNoopTracerProvider().Tracer("test"),
		}

		sst1 := createTestSSTableForCleanup(t, baseDir, 1)
		sst2 := createTestSSTableForCleanup(t, baseDir, 2)

		err := cm.removeAndCleanupSSTables(context.Background(), []*sstable.SSTable{sst1, sst2})
		require.NoError(t, err)

		// Verify both files were "removed"
		require.Len(t, mockRemover.removedFiles, 2)
		sort.Strings(mockRemover.removedFiles)
		assert.True(t, sst1.FilePath() == mockRemover.removedFiles[0], "sst1 should be removed")
		assert.True(t, sst2.FilePath() == mockRemover.removedFiles[1], "sst2 should be removed")
	})

	t.Run("Failure_OneFileFailsToRemove", func(t *testing.T) {
		sst1 := createTestSSTableForCleanup(t, baseDir, 10)
		sst2 := createTestSSTableForCleanup(t, baseDir, 11)
		errRemove := errors.New("permission denied")

		mockRemover := &mockFileRemover{
			failPaths:    map[string]error{sst1.FilePath(): errRemove},
			removedFiles: []string{},
		}
		cm := &CompactionManager{
			logger:      logger,
			fileRemover: mockRemover,
			tracer:      trace.NewNoopTracerProvider().Tracer("test"),
		}

		err := cm.removeAndCleanupSSTables(context.Background(), []*sstable.SSTable{sst1, sst2})
		require.Error(t, err)
		assert.ErrorIs(t, err, errRemove, "The returned error should wrap the original remove error")
		assert.Contains(t, err.Error(), sst1.FilePath(), "Error message should contain the path of the failed file")

		// Verify only the second file was removed
		require.Len(t, mockRemover.removedFiles, 1)
		assert.Equal(t, sst2.FilePath(), mockRemover.removedFiles[0])
	})

	t.Run("Failure_OneFileFailsToClose", func(t *testing.T) {
		sst1 := createTestSSTableForCleanup(t, baseDir, 20)
		sst2 := createTestSSTableForCleanup(t, baseDir, 21)

		// Simulate a close error by closing the file handle beforehand.
		// With the new SSTable.Close(), the second call inside removeAndCleanupSSTables will return an error.
		require.NoError(t, sst1.Close())

		mockRemover := &mockFileRemover{failPaths: make(map[string]error)}
		cm := &CompactionManager{
			logger:      logger,
			fileRemover: mockRemover,
			tracer:      trace.NewNoopTracerProvider().Tracer("test"),
		}

		err := cm.removeAndCleanupSSTables(context.Background(), []*sstable.SSTable{sst1, sst2})
		require.Error(t, err)
		assert.ErrorIs(t, err, sstable.ErrClosed, "The returned error should be sstable.ErrClosed")
		assert.Contains(t, err.Error(), sst1.FilePath(), "Error message should contain the path of the failed file")

		// Verify that removal is still attempted for both files, even if one fails to close.
		require.Len(t, mockRemover.removedFiles, 2, "Both files should have been attempted to be removed")
	})

	t.Run("Failure_MultipleErrorsAreJoined", func(t *testing.T) {
		sst1 := createTestSSTableForCleanup(t, baseDir, 30) // Will fail to close
		sst2 := createTestSSTableForCleanup(t, baseDir, 31) // Will fail to remove
		sst3 := createTestSSTableForCleanup(t, baseDir, 32) // Will succeed
		errRemove := errors.New("disk full")

		// Pre-close sst1 to simulate a close error on the second call inside removeAndCleanupSSTables
		require.NoError(t, sst1.Close())

		mockRemover := &mockFileRemover{
			failPaths: map[string]error{
				sst2.FilePath(): errRemove,
			},
		}
		cm := &CompactionManager{
			logger:      logger,
			fileRemover: mockRemover,
			tracer:      trace.NewNoopTracerProvider().Tracer("test"),
		}

		err := cm.removeAndCleanupSSTables(context.Background(), []*sstable.SSTable{sst1, sst2, sst3})
		require.Error(t, err)

		// Check that both errors are present in the joined error string
		assert.ErrorIs(t, err, sstable.ErrClosed, "The joined error should contain the close error")
		assert.ErrorIs(t, err, errRemove, "The joined error should contain the remove error")
		assert.Contains(t, err.Error(), sst1.FilePath(), "Error message should contain the path of the file that failed to close")
		assert.Contains(t, err.Error(), sst2.FilePath(), "Error message should contain the path of the file that failed to remove")

		// sst1 (failed close but remove is still attempted) and sst3 should be "removed".
		// sst2 fails to remove.
		require.Len(t, mockRemover.removedFiles, 2)
		sort.Strings(mockRemover.removedFiles)
		assert.Equal(t, sst1.FilePath(), mockRemover.removedFiles[0])
		assert.Equal(t, sst3.FilePath(), mockRemover.removedFiles[1])
	})
}

func TestCompactionManager_PerformCompactionCycle_ParallelExecution(t *testing.T) {
	// 1. Setup
	tempDir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil)) // Discard logs for cleaner test output

	// Channels for coordinating the test
	compactionStartedChan := make(chan bool, 5) // Buffered channel to receive signals from started compactions
	finishCompactionChan := make(chan bool, 5)  // Channel to signal compactions to finish

	// Create a dummy engine. The compactor is created via a factory inside the engine.
	dummyEngine, err := NewStorageEngine(StorageEngineOptions{
		DataDir:                    tempDir,
		Logger:                     logger,
		MaxLevels:                  5,
		MaxL0Files:                 2,  // Trigger L0 compaction with 2 files
		TargetSSTableSize:          10, // Small size to trigger LN compactions
		LevelsTargetSizeMultiplier: 1,  // Any size > 0 will trigger
		MaxConcurrentLNCompactions: 2,  // Allow 2 LN compactions in parallel
		SSTableCompressor:          &compressors.NoCompressionCompressor{},
		CompactionIntervalSeconds:  3600, // Disable auto compaction
	})
	require.NoError(t, err)

	concreteEngine := dummyEngine.(*storageEngine)

	// 2. Override the compactor factory to inject our mocks
	concreteEngine.setCompactorFactory = func(seo StorageEngineOptions, se *storageEngine) (CompactionManagerInterface, error) {
		cmParams := CompactionManagerParams{
			LevelsManager: se.levelsManager,
			DataDir:       se.sstDir,
			Opts: CompactionOptions{
				MaxL0Files:                 2,
				L0CompactionTriggerSize:    1000,            // Don't trigger by size
				TargetSSTableSize:          1 * 1024 * 1024, // 1MB, large enough to prevent rollovers during this test
				LevelsTargetSizeMultiplier: 1,
				CompactionIntervalSeconds:  3600, // Disable auto compaction
				MaxConcurrentLNCompactions: 2,    // Allow 2 LN compactions in parallel
				SSTableCompressor:          &compressors.NoCompressionCompressor{},
			},
			Logger:               se.logger,
			Tracer:               trace.NewNoopTracerProvider().Tracer("test"),
			IsSeriesDeleted:      func(b []byte, u uint64) bool { return false },
			IsRangeDeleted:       func(b []byte, i int64, u uint64) bool { return false },
			ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
			FileRemover:          &realFileRemover{},
			SSTableWriterFactory: func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
				// This is the key part: create a real writer but wrap it in our controllable mock.
				realWriter, err := sstable.NewSSTableWriter(opts)
				if err != nil {
					return nil, err
				}
				return &controllableMockSSTableWriter{
					SSTableWriterInterface: realWriter,
					startSignal:            compactionStartedChan,
					finishSignal:           finishCompactionChan,
				}, nil
			},
			Engine: se,
		}
		return NewCompactionManager(cmParams)
	}

	// 3. Start the engine, which initializes the compactor.
	require.NoError(t, dummyEngine.Start())
	t.Cleanup(func() { dummyEngine.Close() })

	lm := concreteEngine.levelsManager
	cm := concreteEngine.compactor.(*CompactionManager)

	// 4. Setup the levels manager to trigger compactions
	// L0 needs compaction (2 files, max is 2)
	lm.AddL0Table(createDummySSTable(t, dummyEngine, dummyEngine.GetNextSSTableID(), []testEntry{{metric: "l0.a"}}))
	lm.AddL0Table(createDummySSTable(t, dummyEngine, dummyEngine.GetNextSSTableID(), []testEntry{{metric: "l0.b"}}))

	// L1 needs compaction (size > 10)
	l1Table := createDummySSTable(t, dummyEngine, dummyEngine.GetNextSSTableID(), []testEntry{{metric: "l1.a", value: "a_long_value_to_exceed_size"}})
	lm.GetLevels()[1].SetTables([]*sstable.SSTable{l1Table})

	// L2 needs compaction (size > 10)
	l2Table := createDummySSTable(t, dummyEngine, dummyEngine.GetNextSSTableID(), []testEntry{{metric: "l2.a", value: "a_long_value_to_exceed_size"}})
	lm.GetLevels()[2].SetTables([]*sstable.SSTable{l2Table})

	// L3 needs compaction, but should be skipped due to semaphore limit
	l3Table := createDummySSTable(t, dummyEngine, dummyEngine.GetNextSSTableID(), []testEntry{{metric: "l3.a", value: "a_long_value_to_exceed_size"}})
	lm.GetLevels()[3].SetTables([]*sstable.SSTable{l3Table})

	// 5. Execute the compaction cycle
	t.Log("Performing compaction cycle...")
	cm.performCompactionCycle()

	// 6. Verification
	startedCompactions := 0
	timeout := time.After(2 * time.Second)

	// Expect 3 compactions to start: L0->L1, L1->L2, L2->L3
	for i := 0; i < 3; i++ {
		select {
		case <-compactionStartedChan:
			startedCompactions++
			t.Logf("Detected start of compaction #%d", startedCompactions)
		case <-timeout:
			t.Fatalf("Timed out waiting for compaction %d to start. Only %d started.", i+1, startedCompactions)
		}
	}
	assert.Equal(t, 3, startedCompactions, "Expected 3 compactions to start in parallel (1 L0 + 2 LN)")
	assert.True(t, cm.l0CompactionActive.Load(), "l0CompactionActive should be true while L0 compaction is running")

	// Now, unblock the compactions
	t.Log("Unblocking compactions...")
	for i := 0; i < 3; i++ {
		finishCompactionChan <- true
	}

	// Wait for all compaction goroutines to finish.
	require.Eventually(t, func() bool {
		// We can't access the waitgroup directly, but we can check the final state.
		// The most reliable indicator is that l0CompactionActive is false and the levels are updated.
		return !cm.l0CompactionActive.Load() && len(lm.GetTablesForLevel(0)) == 0
	}, 2*time.Second, 20*time.Millisecond, "Timed out waiting for compactions to finish")

	// Final state verification
	assert.False(t, cm.l0CompactionActive.Load(), "l0CompactionActive should be false after compaction finishes")
	assert.Empty(t, lm.GetTablesForLevel(0), "L0 should be empty after compaction")
	assert.Len(t, lm.GetTablesForLevel(1), 1, "L1 should contain the newly compacted table from L0")
	assert.Len(t, lm.GetTablesForLevel(2), 1, "L2 should contain the newly compacted table from L1")
	// L3->L4 compaction was skipped due to semaphore limit.
	// However, L2->L3 compaction *did* run, adding a new table to L3.
	// So, L3 should now contain its original table plus the new one.
	l3Tables := lm.GetTablesForLevel(3)
	assert.Len(t, l3Tables, 2, "L3 should contain its original table plus the new one from L2 compaction")
	assert.Contains(t, getTableIDs(l3Tables), l3Table.ID(), "The original l3Table should still be present in L3")
}

func TestCompactionManager_PerformCompactionCycle_LN_Failure(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// 1. Setup engine with a factory to inject a failing compactor
	dummyEngine, err := NewStorageEngine(StorageEngineOptions{
		DataDir:                    tempDir,
		Logger:                     logger,
		MaxLevels:                  5,
		TargetSSTableSize:          10, // Small size to trigger LN compactions
		LevelsTargetSizeMultiplier: 1,  // Any size > 0 will trigger
		MaxConcurrentLNCompactions: 1,
		SSTableCompressor:          &compressors.NoCompressionCompressor{},
		CompactionIntervalSeconds:  3600, // Disable auto compaction
	})
	require.NoError(t, err)

	concreteEngine := dummyEngine.(*storageEngine)

	// This is the error we expect to see from the mock writer
	simulatedError := errors.New("simulated writer finish error")

	concreteEngine.setCompactorFactory = func(seo StorageEngineOptions, se *storageEngine) (CompactionManagerInterface, error) {
		cmParams := CompactionManagerParams{
			LevelsManager: se.levelsManager,
			DataDir:       se.sstDir,
			Opts: CompactionOptions{
				TargetSSTableSize:          10,
				LevelsTargetSizeMultiplier: 1,
				CompactionIntervalSeconds:  3600,
				MaxConcurrentLNCompactions: 1,
				SSTableCompressor:          &compressors.NoCompressionCompressor{},
			},
			Logger:               se.logger,
			Tracer:               trace.NewNoopTracerProvider().Tracer("test"),
			IsSeriesDeleted:      func(b []byte, u uint64) bool { return false },
			IsRangeDeleted:       func(b []byte, i int64, u uint64) bool { return false },
			ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
			FileRemover:          &realFileRemover{},
			SSTableWriterFactory: func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
				// This factory will produce a writer that fails on Finish()
				return &MockSSTableWriter{
					failFinish: true,
					finishErr:  simulatedError,
					filePath:   filepath.Join(se.sstDir, fmt.Sprintf("%d.sst.tmp", opts.ID)), // Provide a temp path
				}, nil
			},
			Engine: se,
		}
		return NewCompactionManager(cmParams)
	}

	// 2. Start the engine, which initializes the compactor.
	require.NoError(t, dummyEngine.Start())
	t.Cleanup(func() { dummyEngine.Close() })

	lm := concreteEngine.levelsManager
	cm := concreteEngine.compactor.(*CompactionManager)
	metrics := concreteEngine.metrics

	// 3. Arrange state to trigger L1->L2 compaction
	l1Table := createDummySSTable(t, dummyEngine, dummyEngine.GetNextSSTableID(), []testEntry{{metric: "l1.fail", value: "a_long_value_to_exceed_size"}})
	lm.GetLevels()[1].SetTables([]*sstable.SSTable{l1Table})
	require.Empty(t, lm.GetTablesForLevel(2), "L2 should be empty initially")
	initialErrorCount := metrics.CompactionErrorsTotal.Value()

	// 4. Act: Perform the compaction cycle
	t.Log("Performing compaction cycle that is expected to fail...")
	cm.performCompactionCycle()

	// 5. Assert
	// The compaction runs in a goroutine, so we need to wait for it to finish (and fail).
	// The best way to check is to see if the error metric has been incremented.
	require.Eventually(t, func() bool {
		return metrics.CompactionErrorsTotal.Value() > initialErrorCount
	}, 2*time.Second, 20*time.Millisecond, "Timed out waiting for compaction error metric to be incremented")

	// Verify final state
	assert.Equal(t, initialErrorCount+1, metrics.CompactionErrorsTotal.Value(), "CompactionErrorsTotal should be incremented by 1")

	// The original table should still be in L1 because the compaction failed.
	l1TablesAfter := lm.GetTablesForLevel(1)
	require.Len(t, l1TablesAfter, 1, "L1 should still contain its original table after failed compaction")
	assert.Equal(t, l1Table.ID(), l1TablesAfter[0].ID(), "The table in L1 should be the original one")

	// L2 should remain empty.
	assert.Empty(t, lm.GetTablesForLevel(2), "L2 should remain empty after failed compaction")

	// The semaphore should have been released.
	assert.Len(t, cm.lnCompactionSemaphore, 0, "LN compaction semaphore should be released after failure")
}

func TestCompactionManager_PerformCompactionCycle_L0_Failure(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// 1. Setup engine with a factory to inject a failing compactor
	dummyEngine, err := NewStorageEngine(StorageEngineOptions{
		DataDir:                   tempDir,
		Logger:                    logger,
		MaxLevels:                 5,
		MaxL0Files:                2, // Trigger L0 compaction with 2 files
		SSTableCompressor:         &compressors.NoCompressionCompressor{},
		CompactionIntervalSeconds: 3600, // Disable auto compaction
	})
	require.NoError(t, err)

	concreteEngine := dummyEngine.(*storageEngine)

	simulatedError := errors.New("simulated L0 writer finish error")

	concreteEngine.setCompactorFactory = func(seo StorageEngineOptions, se *storageEngine) (CompactionManagerInterface, error) {
		cmParams := CompactionManagerParams{
			LevelsManager: se.levelsManager,
			DataDir:       se.sstDir,
			Opts: CompactionOptions{
				MaxL0Files:        2,
				SSTableCompressor: &compressors.NoCompressionCompressor{},
			},
			Logger:               se.logger,
			Tracer:               trace.NewNoopTracerProvider().Tracer("test"),
			IsSeriesDeleted:      func(b []byte, u uint64) bool { return false },
			IsRangeDeleted:       func(b []byte, i int64, u uint64) bool { return false },
			ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
			FileRemover:          &realFileRemover{},
			SSTableWriterFactory: func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
				return &MockSSTableWriter{
					failFinish: true,
					finishErr:  simulatedError,
					filePath:   filepath.Join(se.sstDir, fmt.Sprintf("%d.sst.tmp", opts.ID)),
				}, nil
			},
			Engine: se,
		}
		return NewCompactionManager(cmParams)
	}

	// 2. Start the engine
	require.NoError(t, dummyEngine.Start())
	t.Cleanup(func() { dummyEngine.Close() })

	lm := concreteEngine.levelsManager
	cm := concreteEngine.compactor.(*CompactionManager)
	metrics := concreteEngine.metrics

	// 3. Arrange state to trigger L0 compaction
	l0Table1 := createDummySSTable(t, dummyEngine, dummyEngine.GetNextSSTableID(), []testEntry{{metric: "l0.fail.1"}})
	l0Table2 := createDummySSTable(t, dummyEngine, dummyEngine.GetNextSSTableID(), []testEntry{{metric: "l0.fail.2"}})
	lm.AddL0Table(l0Table1)
	lm.AddL0Table(l0Table2)
	require.Len(t, lm.GetTablesForLevel(0), 2, "L0 should have 2 tables initially")
	require.Empty(t, lm.GetTablesForLevel(1), "L1 should be empty initially")
	initialErrorCount := metrics.CompactionErrorsTotal.Value()

	// 4. Act
	cm.performCompactionCycle()

	// 5. Assert
	require.Eventually(t, func() bool {
		return metrics.CompactionErrorsTotal.Value() > initialErrorCount
	}, 2*time.Second, 20*time.Millisecond, "Timed out waiting for L0 compaction error metric to be incremented")

	assert.Equal(t, initialErrorCount+1, metrics.CompactionErrorsTotal.Value(), "CompactionErrorsTotal should be incremented by 1")
	assert.False(t, cm.l0CompactionActive.Load(), "l0CompactionActive should be false after failure")
	assert.Len(t, lm.GetTablesForLevel(0), 2, "L0 should still contain its original tables after failed compaction")
	assert.Empty(t, lm.GetTablesForLevel(1), "L1 should remain empty after failed compaction")
}
