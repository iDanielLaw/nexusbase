package engine2

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine"
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
	currentSizeFunc func() int64
	failLoad        bool
	entries         []struct {
		key       []byte
		value     []byte
		entryType core.EntryType
		seqNum    uint64
	}
	filePath string
	id       uint64
}

type controllableMockSSTableWriter struct {
	core.SSTableWriterInterface
	startSignal  chan bool
	finishSignal chan bool
	signaled     atomic.Bool
}

func (m *controllableMockSSTableWriter) Add(key, value []byte, entryType core.EntryType, seqNum uint64) error {
	if !m.signaled.Load() {
		if m.signaled.CompareAndSwap(false, true) {
			if m.startSignal != nil {
				m.startSignal <- true
			}
			if m.finishSignal != nil {
				<-m.finishSignal
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
	return 1000000
}

type mockFileRemover struct {
	mu           sync.Mutex
	failPaths    map[string]error
	removedFiles []string
}

func (m *mockFileRemover) Remove(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err, ok := m.failPaths[name]; ok {
		return err
	}
	if err := sys.Remove(name); err != nil {
		return err
	}
	m.removedFiles = append(m.removedFiles, name)
	return nil
}

func setupCompactionManagerWithMockWriter(t *testing.T, mockWriter *MockSSTableWriter, clock clock.Clock) CompactionManagerInterface {
	t.Helper()
	logger := slog.Default()

	lm, _ := levels.NewLevelsManager(3, 2, 1024, trace.NewNoopTracerProvider().Tracer("test"), levels.PickOldest, 1.5, 1.0)
	t.Cleanup(func() { lm.Close() })

	// Create a test engine that satisfies the minimal StorageEngineInterface
	dataDir := t.TempDir()
	sstDir := filepath.Join(dataDir, "sst")
	_ = os.MkdirAll(sstDir, 0o755)
	te := &testEngine{dataDir: dataDir, clk: clock}

	mockRemover := &mockFileRemover{failPaths: map[string]error{filepath.Join(sstDir, "101.sst"): errMockRemove}}

	cmParams := CompactionManagerParams{
		LevelsManager: lm,
		DataDir:       sstDir,
		Opts: CompactionOptions{
			TargetSSTableSize:          100,
			LevelsTargetSizeMultiplier: 2,
			CompactionIntervalSeconds:  3600,
			SSTableCompressor:          &compressors.NoCompressionCompressor{},
		},
		Logger:               logger,
		Tracer:               trace.NewNoopTracerProvider().Tracer("test"),
		IsSeriesDeleted:      func(b []byte, u uint64) bool { return false },
		IsRangeDeleted:       func(b []byte, i int64, u uint64) bool { return false },
		ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
		FileRemover:          mockRemover,
		SSTableWriterFactory: func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
			if mockWriter.failLoad {
				dummyPath := filepath.Join(sstDir, fmt.Sprintf("%d.corrupted", opts.ID))
				os.WriteFile(dummyPath, []byte("corrupted data"), 0644)
				mockWriter.filePath = dummyPath
				mockWriter.id = opts.ID
				return mockWriter, nil
			}
			mockWriter.filePath = filepath.Join(sstDir, fmt.Sprintf("%d.sst", opts.ID))
			mockWriter.id = opts.ID
			return mockWriter, nil
		},
		Engine: te,
	}

	cmIface, err := NewCompactionManager(cmParams)
	if err != nil {
		t.Fatalf("failed to create CompactionManager: %v", err)
	}
	return cmIface
}

func verifySSTableContent(t *testing.T, tables []*sstable.SSTable, expectedData map[string]string, eng StorageEngineInterface) {
	t.Helper()
	var iters []core.IteratorInterface[*core.IteratorNode]
	for _, tbl := range tables {
		iter, err := tbl.NewIterator(nil, nil, nil, types.Ascending)
		if err != nil {
			t.Fatalf("Failed to create iterator for table %d: %v", tbl.ID(), err)
		}
		iters = append(iters, iter)
	}

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
		cur, err := mergedIter.At()
		require.NoError(t, err)
		keyBytes, valueBytes, entryType, _ := cur.Key, cur.Value, cur.EntryType, cur.SeqNum

		if entryType == core.EntryTypePutEvent {
			decodedFields, err := core.DecodeFieldsFromBytes(valueBytes)
			if err != nil {
				t.Fatalf("failed to decode fields during verification: %v", err)
			}
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

	if len(actualData) != len(expectedData) {
		t.Errorf("SSTable content count mismatch: got %d, want %d", len(actualData), len(expectedData))
	}

	for k, expectedV := range expectedData {
		actualV, ok := actualData[k]
		if !ok {
			t.Errorf("Expected key not found in actual data: %x", []byte(k))
			continue
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

	inputSST := createDummySSTable(t, filepath.Join(cm.Engine.GetDataDir(), "sst"), 101, []testEntry{
		{metric: "merge.test.metric", tags: map[string]string{"id": "1"}, value: "val1"},
	})
	t.Cleanup(func() { inputSST.Close(); sys.Remove(inputSST.FilePath()) })

	_, err := engine.ExportMergeMultipleSSTables(cm, context.Background(), []*sstable.SSTable{inputSST}, 1)
	if err == nil {
		t.Fatal("Expected an error from mergeMultipleSSTables due to writer.Add failure, got nil")
	}
	if !assert.ErrorContains(t, err, mockWriter.addErr.Error()) {
		t.Errorf("Expected error to contain '%s', got '%s'", mockWriter.addErr.Error(), err.Error())
	}
}

// Remaining compaction tests copied from original engine file follow (omitted here for brevity).
