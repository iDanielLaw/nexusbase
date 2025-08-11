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
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// --- Mocks ---

// mockEngineProvider เป็น mock implementation ของ EngineProvider interface
type mockEngineProvider struct {
	mock.Mock
	dataDir string
	sstDir  string
	walDir  string
	logger  *slog.Logger
	clock   utils.Clock
	tracer  trace.Tracer
	hooks   hooks.HookManager

	// สถานะภายในสำหรับ mock
	lockMu             sync.Mutex
	memtablesToFlush   []*memtable.Memtable
	sequenceNumber     uint64
	deletedSeries      map[string]uint64
	rangeTombstones    map[string][]core.RangeTombstone
	levelsManager      levels.Manager
	tagIndexManager    *mockTagIndexManager
	stringStore        *mockPrivateManagerStore
	seriesIDStore      *mockPrivateManagerStore
	sstableCompression string
	flushedMemtables   []*memtable.Memtable
	isStarted          bool
}

func newMockEngineProvider(t *testing.T, dataDir string) *mockEngineProvider {
	t.Helper()
	lm, err := levels.NewLevelsManager(7, 4, 1024, noop.NewTracerProvider().Tracer(""))
	require.NoError(t, err)

	return &mockEngineProvider{
		dataDir:            dataDir,
		sstDir:             filepath.Join(dataDir, "sst"),
		walDir:             filepath.Join(dataDir, "wal"),
		logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		clock:              utils.NewMockClock(time.Now()),
		tracer:             noop.NewTracerProvider().Tracer("test"),
		hooks:              hooks.NewHookManager(nil),
		levelsManager:      lm,
		tagIndexManager:    new(mockTagIndexManager),
		stringStore:        newMockPrivateManagerStore(filepath.Join(dataDir, indexer.StringMappingLogName)),
		seriesIDStore:      newMockPrivateManagerStore(filepath.Join(dataDir, indexer.SeriesMappingLogName)),
		sstableCompression: "none",
		isStarted:          true,
	}
}

func (m *mockEngineProvider) CheckStarted() error {
	if m.isStarted {
		return nil
	}
	return fmt.Errorf("engine not started")
}
func (m *mockEngineProvider) GetWALPath() string                { return m.walDir }
func (m *mockEngineProvider) GetClock() utils.Clock             { return m.clock }
func (m *mockEngineProvider) GetLogger() *slog.Logger           { return m.logger }
func (m *mockEngineProvider) GetTracer() trace.Tracer           { return m.tracer }
func (m *mockEngineProvider) GetHookManager() hooks.HookManager { return m.hooks }
func (m *mockEngineProvider) GetLevelsManager() levels.Manager  { return m.levelsManager }
func (m *mockEngineProvider) GetTagIndexManager() indexer.TagIndexManagerInterface {
	return m.tagIndexManager
}
func (m *mockEngineProvider) GetPrivateStringStore() internal.PrivateManagerStore {
	return m.stringStore
}
func (m *mockEngineProvider) GetPrivateSeriesIDStore() internal.PrivateManagerStore {
	return m.seriesIDStore
}
func (m *mockEngineProvider) GetSSTableCompressionType() string { return m.sstableCompression }
func (m *mockEngineProvider) GetSequenceNumber() uint64         { return m.sequenceNumber }
func (m *mockEngineProvider) Lock()                             { m.lockMu.Lock() }
func (m *mockEngineProvider) Unlock()                           { m.lockMu.Unlock() }

func (m *mockEngineProvider) GetMemtablesForFlush() ([]*memtable.Memtable, *memtable.Memtable) {
	m.Called()
	newMem := memtable.NewMemtable(1024, m.clock)
	return m.memtablesToFlush, newMem
}

func (m *mockEngineProvider) FlushMemtableToL0(mem *memtable.Memtable, parentCtx context.Context) error {
	args := m.Called(mem, parentCtx)
	m.flushedMemtables = append(m.flushedMemtables, mem)
	return args.Error(0)
}

func (m *mockEngineProvider) GetDeletedSeries() map[string]uint64 {
	m.Called()
	return m.deletedSeries
}

func (m *mockEngineProvider) GetRangeTombstones() map[string][]core.RangeTombstone {
	m.Called()
	return m.rangeTombstones
}

// mockTagIndexManager เป็น mock สำหรับ indexer.TagIndexManagerInterface
type mockTagIndexManager struct {
	mock.Mock
}

func (m *mockTagIndexManager) Add(seriesID uint64, tags map[string]string) error {
	args := m.Called(seriesID, tags)
	return args.Error(0)
}
func (m *mockTagIndexManager) AddEncoded(seriesID uint64, encodedTags []core.EncodedSeriesTagPair) error {
	args := m.Called(seriesID, encodedTags)
	return args.Error(0)
}
func (m *mockTagIndexManager) RemoveSeries(seriesID uint64) {
	m.Called(seriesID)
}
func (m *mockTagIndexManager) Query(tags map[string]string) (*roaring64.Bitmap, error) {
	args := m.Called(tags)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*roaring64.Bitmap), args.Error(1)
}
func (m *mockTagIndexManager) Start() {
	m.Called()
}
func (m *mockTagIndexManager) Stop() {
	m.Called()
}
func (m *mockTagIndexManager) CreateSnapshot(snapshotDir string) error {
	args := m.Called(snapshotDir)
	err := args.Error(0)
	if err != nil {
		return err
	}

	// If no error is returned from the mock setup, simulate success for happy path tests.
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(snapshotDir, indexer.IndexManifestFileName), []byte(`{"levels":[]}`), 0644)
}
func (m *mockTagIndexManager) RestoreFromSnapshot(snapshotDir string) error {
	args := m.Called(snapshotDir)
	return args.Error(0)
}
func (m *mockTagIndexManager) LoadFromFile(dataDir string) error {
	args := m.Called(dataDir)
	return args.Error(0)
}

// mockPrivateManagerStore เป็น mock สำหรับ internal.PrivateManagerStore
type mockPrivateManagerStore struct {
	mock.Mock
	path string
}

func newMockPrivateManagerStore(path string) *mockPrivateManagerStore {
	// สร้างไฟล์จำลองเพื่อให้มีอยู่จริง
	os.MkdirAll(filepath.Dir(path), 0755)
	os.WriteFile(path, []byte("dummy data"), 0644)
	return &mockPrivateManagerStore{path: path}
}

func (m *mockPrivateManagerStore) GetLogFilePath() string {
	args := m.Called()
	return args.String(0)
}

// --- mock Helper

type mockSnapshotHelper struct {
	*helperSnapshot

	InterceptSaveJSON                    func(v interface{}, path string) error
	InterceptCopyAuxiliaryFile           func(srcPath, destFileName, snapshotDir string, manifestField *string, logger *slog.Logger) error
	InterceptCopyDirectoryContents       func(src, dst string) error
	InterceptLinkOrCopyDirectoryContents func(src, dst string) error
	InterceptRemoveAll                   func(path string) error
	InterceptWriteFile                   func(filename string, data []byte, perm os.FileMode) error
	InterceptReadFile                    func(filename string) ([]byte, error)
	InterceptMkdirTemp                   func(dir, pattern string) (string, error)
	InterceptMkdirAll                    func(path string, perm os.FileMode) error
	InterceptOpen                        func(name string) (*os.File, error)
	InterceptCopyFile                    func(src, dst string) error
	InterceptReadManifestBinary          func(r io.Reader) (*core.SnapshotManifest, error)
	InterceptRename                      func(oldPath, newPath string) error
}

var _ internal.PrivateSnapshotHelper = (*mockSnapshotHelper)(nil)

func (ms *mockSnapshotHelper) SaveJSON(v interface{}, path string) error {
	if ms.InterceptSaveJSON != nil {
		return ms.InterceptSaveJSON(v, path)
	}
	return ms.helperSnapshot.SaveJSON(v, path)
}

func (ms *mockSnapshotHelper) CopyAuxiliaryFile(srcPath, destFileName, snapshotDir string, manifestField *string, logger *slog.Logger) error {
	if ms.InterceptCopyAuxiliaryFile != nil {
		return ms.InterceptCopyAuxiliaryFile(srcPath, destFileName, snapshotDir, manifestField, logger)
	}
	return ms.helperSnapshot.CopyAuxiliaryFile(srcPath, destFileName, snapshotDir, manifestField, logger)
}

func (ms *mockSnapshotHelper) CopyDirectoryContents(src, dst string) error {
	if ms.InterceptCopyDirectoryContents != nil {
		return ms.InterceptCopyDirectoryContents(src, dst)
	}
	return ms.helperSnapshot.CopyDirectoryContents(src, dst)
}

func (ms *mockSnapshotHelper) LinkOrCopyDirectoryContents(src, dst string) error {
	if ms.InterceptLinkOrCopyDirectoryContents != nil {
		return ms.InterceptLinkOrCopyDirectoryContents(src, dst)
	}
	return ms.helperSnapshot.LinkOrCopyDirectoryContents(src, dst)
}

func (ms *mockSnapshotHelper) RemoveAll(path string) error {
	if ms.InterceptRemoveAll != nil {
		return ms.InterceptRemoveAll(path)
	}
	return ms.helperSnapshot.RemoveAll(path)
}

func (ms *mockSnapshotHelper) WriteFile(filename string, data []byte, perm os.FileMode) error {
	if ms.InterceptWriteFile != nil {
		return ms.InterceptWriteFile(filename, data, perm)
	}
	return ms.helperSnapshot.WriteFile(filename, data, perm)
}

func (ms *mockSnapshotHelper) ReadFile(filename string) ([]byte, error) {
	if ms.InterceptReadFile != nil {
		return ms.InterceptReadFile(filename)
	}
	return ms.helperSnapshot.ReadFile(filename)
}

func (ms *mockSnapshotHelper) MkdirAll(path string, perm os.FileMode) error {
	if ms.InterceptMkdirAll != nil {
		return ms.InterceptMkdirAll(path, perm)
	}
	return ms.helperSnapshot.MkdirAll(path, perm)
}

func (ms *mockSnapshotHelper) MkdirTemp(dir, pattern string) (string, error) {
	if ms.InterceptMkdirTemp != nil {
		return ms.InterceptMkdirTemp(dir, pattern)
	}
	return ms.helperSnapshot.MkdirTemp(dir, pattern)
}

func (ms *mockSnapshotHelper) Open(name string) (*os.File, error) {
	if ms.InterceptOpen != nil {
		return ms.InterceptOpen(name)
	}
	return ms.helperSnapshot.Open(name)
}

func (ms *mockSnapshotHelper) ReadManifestBinary(r io.Reader) (*core.SnapshotManifest, error) {
	if ms.InterceptReadManifestBinary != nil {
		return ms.InterceptReadManifestBinary(r)
	}
	return ms.helperSnapshot.ReadManifestBinary(r)
}

func (ms *mockSnapshotHelper) CopyFile(src, dst string) error {
	if ms.InterceptCopyFile != nil {
		return ms.InterceptCopyFile(src, dst)
	}
	return ms.helperSnapshot.CopyFile(src, dst)
}

func (ms *mockSnapshotHelper) Rename(oldPath, newPath string) error {
	if ms.InterceptRename != nil {
		return ms.InterceptRename(oldPath, newPath)
	}
	return ms.helperSnapshot.Rename(oldPath, newPath)
}

//

// --- Helper Functions ---

// createDummySSTable สร้างไฟล์ SSTable จำลองสำหรับการทดสอบ
func createDummySSTable(t *testing.T, dir string, id uint64) *sstable.SSTable {
	t.Helper()
	filePath := filepath.Join(dir, fmt.Sprintf("%d.sst", id))
	writerOpts := core.SSTableWriterOptions{
		DataDir:                      dir,
		ID:                           id,
		Compressor:                   &compressors.NoCompressionCompressor{},
		Logger:                       slog.New(slog.NewTextHandler(io.Discard, nil)),
		BloomFilterFalsePositiveRate: 0.01, // Add this to prevent bloom filter creation error
	}
	writer, err := sstable.NewSSTableWriter(writerOpts)
	require.NoError(t, err)

	// เพิ่มข้อมูลบางส่วนเพื่อให้มี Min/Max keys
	key1 := []byte(fmt.Sprintf("key-%03d", id))
	key2 := []byte(fmt.Sprintf("key-%03d-z", id))
	require.NoError(t, writer.Add(key1, []byte("val1"), core.EntryTypePutEvent, 1))
	require.NoError(t, writer.Add(key2, []byte("val2"), core.EntryTypePutEvent, 2))
	require.NoError(t, writer.Finish())

	tbl, err := sstable.LoadSSTable(sstable.LoadSSTableOptions{FilePath: filePath, ID: id})
	require.NoError(t, err)
	return tbl
}

// --- Tests ---

func TestManager_CreateFull(t *testing.T) {
	// 1. Setup
	tempDir := t.TempDir()
	snapshotDir := filepath.Join(tempDir, "snapshot")
	dataDir := filepath.Join(tempDir, "data")
	require.NoError(t, os.MkdirAll(dataDir, 0755))

	provider := newMockEngineProvider(t, dataDir)
	require.NoError(t, os.MkdirAll(provider.sstDir, 0755))

	// กำหนดค่าสถานะของ mock provider
	provider.sequenceNumber = 123
	provider.deletedSeries = map[string]uint64{"deleted_series_1": 100}
	provider.rangeTombstones = map[string][]core.RangeTombstone{
		"range_tombstone_series_1": {{MinTimestamp: 100, MaxTimestamp: 200, SeqNum: 101}},
	}
	mem1 := memtable.NewMemtable(1024, provider.clock)
	mem1.Put([]byte("mem_key_1"), []byte("mem_val_1"), core.EntryTypePutEvent, 120)
	provider.memtablesToFlush = []*memtable.Memtable{mem1}

	// สร้าง SSTables จำลองและเพิ่มเข้าไปใน mock levels manager
	sst1 := createDummySSTable(t, provider.sstDir, 1)
	sst2 := createDummySSTable(t, provider.sstDir, 2)
	provider.levelsManager.AddTableToLevel(0, sst1)
	provider.levelsManager.AddTableToLevel(1, sst2)

	// สร้างไฟล์ WAL จำลอง
	require.NoError(t, os.MkdirAll(provider.walDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(provider.walDir, "000001.wal"), []byte("wal data 1"), 0644))

	// ตั้งค่า mock expectations
	provider.On("GetMemtablesForFlush").Return()
	provider.On("FlushMemtableToL0", mock.Anything, mock.Anything).Return(nil)
	provider.On("GetDeletedSeries").Return()
	provider.On("GetRangeTombstones").Return()
	provider.tagIndexManager.On("CreateSnapshot", filepath.Join(snapshotDir, "index")).Return(nil)
	provider.stringStore.On("GetLogFilePath").Return(provider.stringStore.path)
	provider.seriesIDStore.On("GetLogFilePath").Return(provider.seriesIDStore.path)

	// 2. Execution
	manager := NewManager(provider)
	err := manager.CreateFull(context.Background(), snapshotDir)
	require.NoError(t, err)

	// 3. Verification
	// ตรวจสอบ snapshot directory และไฟล์ CURRENT
	currentBytes, err := os.ReadFile(filepath.Join(snapshotDir, "CURRENT"))
	require.NoError(t, err)
	manifestFileName := string(currentBytes)
	assert.True(t, strings.HasPrefix(manifestFileName, "MANIFEST_"))

	// ตรวจสอบไฟล์ manifest
	manifestPath := filepath.Join(snapshotDir, manifestFileName)
	require.FileExists(t, manifestPath)

	// อ่านและตรวจสอบเนื้อหาของ manifest
	f, err := os.Open(manifestPath)
	require.NoError(t, err)
	defer f.Close()
	manifest, err := ReadManifestBinary(f)
	require.NoError(t, err)

	assert.Equal(t, provider.sequenceNumber, manifest.SequenceNumber)
	assert.Equal(t, provider.sstableCompression, manifest.SSTableCompression)
	require.Len(t, manifest.Levels, 2)
	assert.Equal(t, 0, manifest.Levels[0].LevelNumber)
	require.Len(t, manifest.Levels[0].Tables, 1)
	assert.Equal(t, sst1.ID(), manifest.Levels[0].Tables[0].ID)
	assert.Equal(t, filepath.Join("sst", "1.sst"), manifest.Levels[0].Tables[0].FileName)
	assert.Equal(t, 1, manifest.Levels[1].LevelNumber)
	require.Len(t, manifest.Levels[1].Tables, 1)
	assert.Equal(t, sst2.ID(), manifest.Levels[1].Tables[0].ID)

	// ตรวจสอบไฟล์เสริม
	assert.Equal(t, "deleted_series.json", manifest.DeletedSeriesFile)
	require.FileExists(t, filepath.Join(snapshotDir, "deleted_series.json"))
	assert.Equal(t, "range_tombstones.json", manifest.RangeTombstonesFile)
	require.FileExists(t, filepath.Join(snapshotDir, "range_tombstones.json"))
	assert.Equal(t, "string_mapping.log", manifest.StringMappingFile)
	require.FileExists(t, filepath.Join(snapshotDir, "string_mapping.log"))
	assert.Equal(t, "series_mapping.log", manifest.SeriesMappingFile)
	require.FileExists(t, filepath.Join(snapshotDir, "series_mapping.log"))
	assert.Equal(t, "wal", manifest.WALFile)
	require.FileExists(t, filepath.Join(snapshotDir, "wal", "000001.wal"))

	// ตรวจสอบว่า SSTables ถูกคัดลอก
	require.FileExists(t, filepath.Join(snapshotDir, "sst", "1.sst"))
	require.FileExists(t, filepath.Join(snapshotDir, "sst", "2.sst"))

	// ตรวจสอบว่า tag index snapshot ถูกสร้างขึ้น
	provider.tagIndexManager.AssertCalled(t, "CreateSnapshot", filepath.Join(snapshotDir, "index"))
	require.FileExists(t, filepath.Join(snapshotDir, "index", indexer.IndexManifestFileName))

	// ตรวจสอบว่า memtables ถูก flush
	provider.AssertCalled(t, "FlushMemtableToL0", mem1, mock.Anything)
}

func TestManager_CreateFull_FlushError(t *testing.T) {
	// 1. Setup
	tempDir := t.TempDir()
	snapshotDir := filepath.Join(tempDir, "snapshot_flush_error")
	dataDir := filepath.Join(tempDir, "data_flush_error")
	require.NoError(t, os.MkdirAll(dataDir, 0755))

	provider := newMockEngineProvider(t, dataDir)

	// Create a memtable that needs flushing
	mem1 := memtable.NewMemtable(1024, provider.clock)
	mem1.Put([]byte("mem_key_1"), []byte("mem_val_1"), core.EntryTypePutEvent, 120)
	provider.memtablesToFlush = []*memtable.Memtable{mem1}

	// Define the expected error
	expectedFlushError := fmt.Errorf("simulated flush error")

	// Set up mock expectations
	// GetMemtablesForFlush will be called to get the memtable.
	provider.On("GetMemtablesForFlush").Return()
	provider.On("GetDeletedSeries").Return(nil)
	provider.On("GetRangeTombstones").Return(nil)
	// FlushMemtableToL0 will be called and should return our simulated error.
	provider.On("FlushMemtableToL0", mem1, mock.Anything).Return(expectedFlushError)

	// 2. Execution
	manager := NewManager(provider)
	err := manager.CreateFull(context.Background(), snapshotDir)

	// 3. Verification
	// The call should fail.
	require.Error(t, err)
	assert.ErrorIs(t, err, expectedFlushError, "The returned error should wrap the original flush error")
	assert.Contains(t, err.Error(), "failed to flush memtable")

	// The snapshot directory should have been cleaned up due to the defer block in CreateFull.
	_, statErr := os.Stat(snapshotDir)
	assert.True(t, os.IsNotExist(statErr), "Snapshot directory should be cleaned up on failure")
}

func TestManager_CreateFull_TagIndexSnapshotError(t *testing.T) {
	// 1. Setup
	tempDir := t.TempDir()
	snapshotDir := filepath.Join(tempDir, "snapshot_tag_index_error")
	dataDir := filepath.Join(tempDir, "data_tag_index_error")
	require.NoError(t, os.MkdirAll(dataDir, 0755))

	provider := newMockEngineProvider(t, dataDir)

	// Define the expected error
	expectedIndexError := fmt.Errorf("simulated tag index snapshot error")

	// Set up mock expectations.
	// The flow will get past flushing memtables.
	provider.On("GetMemtablesForFlush").Return()
	provider.On("FlushMemtableToL0", mock.Anything, mock.Anything).Return(nil) // Assume flush succeeds
	provider.On("GetDeletedSeries").Return(nil)
	provider.On("GetRangeTombstones").Return(nil)

	// The call to CreateSnapshot on the tag index manager should fail.
	provider.tagIndexManager.On("CreateSnapshot", mock.Anything).Return(expectedIndexError)

	// 2. Execution
	manager := NewManager(provider)
	err := manager.CreateFull(context.Background(), snapshotDir)

	// 3. Verification
	// The call should fail.
	require.Error(t, err)
	assert.ErrorIs(t, err, expectedIndexError, "The returned error should wrap the original tag index snapshot error")
	assert.Contains(t, err.Error(), "failed to create tag index snapshot")

	// The snapshot directory should have been cleaned up due to the defer block in CreateFull.
	_, statErr := os.Stat(snapshotDir)
	assert.True(t, os.IsNotExist(statErr), "Snapshot directory should be cleaned up on failure")
}

func TestManager_CreateFull_SSTableCopyError(t *testing.T) {
	// 1. Setup
	tempDir := t.TempDir()
	snapshotDir := filepath.Join(tempDir, "snapshot_copy_error")
	dataDir := filepath.Join(tempDir, "data_copy_error")
	require.NoError(t, os.MkdirAll(dataDir, 0755))

	provider := newMockEngineProvider(t, dataDir)
	require.NoError(t, os.MkdirAll(provider.sstDir, 0755)) // Ensure sstDir exists

	// Create a dummy SSTable and add it to the levels manager
	sst1 := createDummySSTable(t, provider.sstDir, 1)
	provider.levelsManager.AddTableToLevel(0, sst1)

	// Set up mock expectations for the parts that will be called before the error
	provider.On("GetMemtablesForFlush").Return()
	provider.On("FlushMemtableToL0", mock.Anything, mock.Anything).Return(nil)
	provider.On("GetDeletedSeries").Return(nil)
	provider.On("GetRangeTombstones").Return(nil)

	// 2. Simulate the error condition
	// Remove the source SSTable file *after* it's been registered with the
	// levels manager but *before* the snapshot process tries to copy it.
	require.NoError(t, os.Remove(sst1.FilePath()))

	// 3. Execution
	manager := NewManager(provider)
	err := manager.CreateFull(context.Background(), snapshotDir)

	// 4. Verification
	require.Error(t, err, "CreateFull should fail when an SSTable cannot be copied")
	assert.Contains(t, err.Error(), "failed to link or copy SSTable", "Error message should indicate a copy failure")
	assert.ErrorIs(t, err, os.ErrNotExist, "The underlying error should be os.ErrNotExist")

	_, statErr := os.Stat(snapshotDir)
	assert.True(t, os.IsNotExist(statErr), "Snapshot directory should be cleaned up on failure")
}

func TestManager_CreateFull_SaveJSONError(t *testing.T) {
	// This test replaces the package-level saveJSON function to simulate errors.
	// We must restore it after the test.
	helper := &mockSnapshotHelper{
		helperSnapshot: newHelperSnapshot(),
	}

	t.Run("OnDeletedSeries", func(t *testing.T) {
		defer func() {
			helper.InterceptSaveJSON = nil
		}()
		// 1. Setup
		tempDir := t.TempDir()
		snapshotDir := filepath.Join(tempDir, "snapshot_json_err_deleted")
		dataDir := filepath.Join(tempDir, "data_json_err_deleted")
		require.NoError(t, os.MkdirAll(dataDir, 0755))

		provider := newMockEngineProvider(t, dataDir)
		provider.deletedSeries = map[string]uint64{"deleted_series_1": 100} // Ensure saveJSON is called

		provider.On("GetMemtablesForFlush").Return()
		provider.On("FlushMemtableToL0", mock.Anything, mock.Anything).Return(nil)
		provider.On("GetDeletedSeries").Return()
		provider.On("GetRangeTombstones").Return(nil)                            // Add expectation for this call
		provider.tagIndexManager.On("CreateSnapshot", mock.Anything).Return(nil) // Add expectation for this call

		// 2. Simulate the error condition
		expectedErr := fmt.Errorf("simulated saveJSON error for deleted_series")
		helper.InterceptSaveJSON = func(v interface{}, path string) error {
			return expectedErr
		}

		// 3. Execution
		manager := NewManagerWithTesting(provider, helper)
		err := manager.CreateFull(context.Background(), snapshotDir)

		// 4. Verification
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Contains(t, err.Error(), "failed to save deleted_series for snapshot")
		_, statErr := os.Stat(snapshotDir)
		assert.True(t, os.IsNotExist(statErr), "Snapshot directory should be cleaned up on failure")
	})

	t.Run("OnRangeTombstones", func(t *testing.T) {
		// 1. Setup
		tempDir := t.TempDir()
		snapshotDir := filepath.Join(tempDir, "snapshot_json_err_tombstone")
		dataDir := filepath.Join(tempDir, "data_json_err_tombstone")
		require.NoError(t, os.MkdirAll(dataDir, 0755))

		provider := newMockEngineProvider(t, dataDir)
		provider.rangeTombstones = map[string][]core.RangeTombstone{"rt1": {{MinTimestamp: 1, MaxTimestamp: 2}}} // Ensure saveJSON is called

		provider.On("GetMemtablesForFlush").Return()
		provider.On("FlushMemtableToL0", mock.Anything, mock.Anything).Return(nil)
		provider.On("GetDeletedSeries").Return()                                 // Assume this succeeds
		provider.On("GetRangeTombstones").Return()                               // This will return the map above
		provider.tagIndexManager.On("CreateSnapshot", mock.Anything).Return(nil) // Add expectation for this call

		// 2. Simulate the error condition
		expectedErr := fmt.Errorf("simulated saveJSON error for range_tombstones")
		helper.InterceptSaveJSON = func(v interface{}, path string) error {
			if strings.Contains(path, "range_tombstones") {
				return expectedErr
			}
			return helper.helperSnapshot.SaveJSON(v, path) // Let other calls succeed
		}

		// 3. Execution & Verification
		manager := NewManagerWithTesting(provider, helper)
		err := manager.CreateFull(context.Background(), snapshotDir)
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Contains(t, err.Error(), "failed to save range_tombstones for snapshot")
	})
}

func TestManager_CreateFull_CopyAuxiliaryFileError(t *testing.T) {
	// 1. Setup
	helper := &mockSnapshotHelper{
		helperSnapshot: newHelperSnapshot(),
	}

	t.Run("OnStringMappingFileCopy", func(t *testing.T) {
		// 1. Setup
		defer func() {
			helper.InterceptCopyAuxiliaryFile = nil
		}()

		tempDir := t.TempDir()
		snapshotDir := filepath.Join(tempDir, "snapshot_aux_copy_error")
		dataDir := filepath.Join(tempDir, "data_aux_copy_error")
		require.NoError(t, os.MkdirAll(dataDir, 0755))

		provider := newMockEngineProvider(t, dataDir)

		// Set up mock expectations for calls that happen before the error
		provider.On("GetMemtablesForFlush").Return()
		provider.On("FlushMemtableToL0", mock.Anything, mock.Anything).Return(nil)
		provider.On("GetDeletedSeries").Return()
		provider.On("GetRangeTombstones").Return()
		provider.tagIndexManager.On("CreateSnapshot", mock.Anything).Return(nil)
		provider.stringStore.On("GetLogFilePath").Return(provider.stringStore.path)

		// 2. Simulate the error condition
		expectedErr := fmt.Errorf("simulated copy auxiliary file error")
		helper.InterceptCopyAuxiliaryFile = func(srcPath, destFileName, snapshotDir string, manifestField *string, logger *slog.Logger) error {
			return expectedErr
		}

		// 3. Execution
		manager := NewManagerWithTesting(provider, helper)
		err := manager.CreateFull(context.Background(), snapshotDir)

		// 4. Verification
		require.Error(t, err, "CreateFull should fail when copyAuxiliaryFile fails")
		assert.ErrorIs(t, err, expectedErr, "The returned error should be the one from our mock")
		_, statErr := os.Stat(snapshotDir)
		assert.True(t, os.IsNotExist(statErr), "Snapshot directory should be cleaned up on failure")
	})
}

func TestManager_CreateFull_WALCopyError(t *testing.T) {
	// 1. Setup
	helper := &mockSnapshotHelper{
		helperSnapshot: newHelperSnapshot(),
	}

	t.Run("OnWALDirectoryCopy", func(t *testing.T) {
		// 1. Setup
		defer func() { helper.InterceptLinkOrCopyDirectoryContents = nil }()
		tempDir := t.TempDir()
		snapshotDir := filepath.Join(tempDir, "snapshot_wal_copy_error")
		dataDir := filepath.Join(tempDir, "data_wal_copy_error")
		require.NoError(t, os.MkdirAll(dataDir, 0755))

		provider := newMockEngineProvider(t, dataDir)

		// Create a dummy WAL directory and file so the copy is attempted
		require.NoError(t, os.MkdirAll(provider.walDir, 0755))
		require.NoError(t, os.WriteFile(filepath.Join(provider.walDir, "000001.wal"), []byte("wal data"), 0644))

		// Set up mock expectations for calls that happen before the error
		provider.On("GetMemtablesForFlush").Return()
		provider.On("FlushMemtableToL0", mock.Anything, mock.Anything).Return(nil)
		provider.On("GetDeletedSeries").Return()
		provider.On("GetRangeTombstones").Return()
		provider.tagIndexManager.On("CreateSnapshot", mock.Anything).Return(nil)
		provider.stringStore.On("GetLogFilePath").Return("")   // Return empty string to simulate no file
		provider.seriesIDStore.On("GetLogFilePath").Return("") // Return empty string to simulate no file

		// 2. Simulate the error condition
		expectedErr := fmt.Errorf("simulated WAL copy error")
		helper.InterceptLinkOrCopyDirectoryContents = func(src, dst string) error {
			return expectedErr
		}

		// 3. Execution & Verification
		manager := NewManagerWithTesting(provider, helper)
		err := manager.CreateFull(context.Background(), snapshotDir)
		require.Error(t, err, "CreateFull should fail when linkOrCopyDirectoryContents for WAL fails")
		assert.ErrorIs(t, err, expectedErr, "The returned error should be the one from our mock")
		assert.Contains(t, err.Error(), "failed to copy WAL directory to snapshot")
		_, statErr := os.Stat(snapshotDir)
		assert.True(t, os.IsNotExist(statErr), "Snapshot directory should be cleaned up on failure")
	})
}

func TestManager_CreateFull_RemoveAllError(t *testing.T) {
	// 1. Setup
	helper := &mockSnapshotHelper{
		helperSnapshot: newHelperSnapshot(),
	}

	tempDir := t.TempDir()
	snapshotDir := filepath.Join(tempDir, "snapshot_remove_error")
	// The snapshot directory must exist for os.RemoveAll to be called.
	require.NoError(t, os.MkdirAll(snapshotDir, 0755))
	expectedErr := fmt.Errorf("simulated os.RemoveAll error")
	helper.InterceptRemoveAll = func(path string) error {
		return expectedErr
	}

	// We only need a minimal provider for this test.
	provider := newMockEngineProvider(t, tempDir)

	// 2. Simulate the error condition

	// 3. Execution
	manager := NewManagerWithTesting(provider, helper)
	err := manager.CreateFull(context.Background(), snapshotDir)

	// 4. Verification
	require.Error(t, err, "CreateFull should fail when os.RemoveAll fails")
	assert.ErrorIs(t, err, expectedErr, "The returned error should wrap the one from os.RemoveAll")
	assert.Contains(t, err.Error(), "failed to clean existing snapshot directory")
}

func TestManager_CreateFull_WriteManifestError(t *testing.T) {
	// 1. Setup
	tempDir := t.TempDir()
	snapshotDir := filepath.Join(tempDir, "snapshot_manifest_error")
	dataDir := filepath.Join(tempDir, "data_manifest_error")
	require.NoError(t, os.MkdirAll(dataDir, 0755))

	provider := newMockEngineProvider(t, dataDir)
	// We need to create the WAL directory so linkOrCopyDirectoryContents doesn't fail
	require.NoError(t, os.MkdirAll(provider.walDir, 0755))

	// Set up mock expectations for all calls that happen before writing the manifest.
	provider.On("GetMemtablesForFlush").Return()
	provider.On("FlushMemtableToL0", mock.Anything, mock.Anything).Return(nil)
	provider.On("GetDeletedSeries").Return()
	provider.On("GetRangeTombstones").Return()
	provider.tagIndexManager.On("CreateSnapshot", mock.Anything).Return(nil)
	provider.stringStore.On("GetLogFilePath").Return("")   // Return empty string to simulate no file
	provider.seriesIDStore.On("GetLogFilePath").Return("") // Return empty string to simulate no file

	// 2. Simulate the error condition
	mgr := NewManager(provider)
	// Cast to the concrete type to modify the function field for the test.
	concreteManager, ok := mgr.(*manager)
	require.True(t, ok)

	expectedErr := fmt.Errorf("simulated write manifest error")
	concreteManager.writeManifestFunc = func(w io.Writer, manifest *core.SnapshotManifest) error {
		return expectedErr
	}

	// 3. Execution & Verification
	err := concreteManager.CreateFull(context.Background(), snapshotDir)
	require.Error(t, err, "CreateFull should fail when writeManifestBinary fails")
	assert.ErrorIs(t, err, expectedErr, "The returned error should wrap the one from our mock")
	assert.Contains(t, err.Error(), "failed to write binary snapshot manifest")
}

func TestManager_CreateFull_EngineNotStarted(t *testing.T) {
	// 1. Setup
	tempDir := t.TempDir()
	snapshotDir := filepath.Join(tempDir, "snapshot_not_started")
	dataDir := filepath.Join(tempDir, "data_not_started")
	provider := newMockEngineProvider(t, dataDir)
	provider.isStarted = false // Simulate engine not started

	// 2. Execution
	manager := NewManager(provider)
	err := manager.CreateFull(context.Background(), snapshotDir)

	// 3. Verification
	require.Error(t, err)
	assert.Contains(t, err.Error(), "engine not started")
}

func TestManager_CreateFull_EmptyEngineState(t *testing.T) {
	// 1. Setup
	tempDir := t.TempDir()
	snapshotDir := filepath.Join(tempDir, "snapshot_empty")
	dataDir := filepath.Join(tempDir, "data_empty")
	require.NoError(t, os.MkdirAll(dataDir, 0755))

	provider := newMockEngineProvider(t, dataDir)

	// Configure empty state
	provider.sequenceNumber = 1
	provider.memtablesToFlush = []*memtable.Memtable{} // No memtables
	provider.deletedSeries = nil
	provider.rangeTombstones = nil
	// Levels manager is already empty by default

	// Set up mock expectations for an empty run
	provider.On("GetMemtablesForFlush").Return()
	// FlushMemtableToL0 should not be called
	provider.On("GetDeletedSeries").Return()
	provider.On("GetRangeTombstones").Return()
	provider.tagIndexManager.On("CreateSnapshot", mock.Anything).Return(nil)
	provider.stringStore.On("GetLogFilePath").Return("")
	provider.seriesIDStore.On("GetLogFilePath").Return("")

	// 2. Execution
	manager := NewManager(provider)
	err := manager.CreateFull(context.Background(), snapshotDir)
	require.NoError(t, err)

	// 3. Verification
	currentBytes, err := os.ReadFile(filepath.Join(snapshotDir, "CURRENT"))
	require.NoError(t, err)
	manifestFileName := string(currentBytes)
	manifestPath := filepath.Join(snapshotDir, manifestFileName)
	require.FileExists(t, manifestPath)

	f, err := os.Open(manifestPath)
	require.NoError(t, err)
	defer f.Close()
	manifest, err := ReadManifestBinary(f)
	require.NoError(t, err)

	// Verify manifest for an empty state
	assert.Equal(t, uint64(1), manifest.SequenceNumber)
	assert.Empty(t, manifest.Levels, "Manifest should have no levels")
	assert.Empty(t, manifest.DeletedSeriesFile)
	assert.Empty(t, manifest.RangeTombstonesFile)
	// WAL might still be copied if the directory exists, which is fine.
}

// mockListener is a mock implementation of HookListener for testing.
type mockThrowErrorListener struct {
	err error
}

func (m *mockThrowErrorListener) OnEvent(ctx context.Context, event hooks.HookEvent) error {
	return m.err
}

func (m *mockThrowErrorListener) Priority() int {
	return 10
}

func (m *mockThrowErrorListener) IsAsync() bool {
	return false
}

func TestManager_CreateFull_HookCancellation(t *testing.T) {
	// 1. Setup
	tempDir := t.TempDir()
	snapshotDir := filepath.Join(tempDir, "snapshot_hook_cancel")
	dataDir := filepath.Join(tempDir, "data_hook_cancel")

	provider := newMockEngineProvider(t, dataDir)
	expectedHookError := fmt.Errorf("snapshot creation cancelled by hook")

	// Register a hook that returns an error
	lis := &mockThrowErrorListener{err: expectedHookError}
	provider.GetHookManager().Register(hooks.EventPreCreateSnapshot, lis)

	// 2. Execution
	manager := NewManager(provider)
	err := manager.CreateFull(context.Background(), snapshotDir)

	// 3. Verification
	require.Error(t, err, "CreateFull should fail when a pre-hook returns an error")
	assert.ErrorIs(t, err, expectedHookError)
	assert.Contains(t, err.Error(), "operation cancelled by pre-hook")

	// Snapshot directory should not exist
	_, statErr := os.Stat(snapshotDir)
	assert.True(t, os.IsNotExist(statErr), "Snapshot directory should not be created if hook cancels operation")
}

func TestRestoreFromFull_ManifestWithMissingFile(t *testing.T) {
	// 1. Setup: Create a snapshot where the manifest lists a file that doesn't exist.
	tempDir := t.TempDir()
	snapshotDir := filepath.Join(tempDir, "snapshot_missing_file")
	targetDataDir := filepath.Join(tempDir, "restored_data_missing_file")
	require.NoError(t, os.MkdirAll(snapshotDir, 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(snapshotDir, "sst"), 0755))

	// Create one valid file that should be copied
	require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "sst", "1.sst"), []byte("sst1"), 0644))

	// Create a manifest that lists the valid file AND a missing file
	manifest := &core.SnapshotManifest{
		SequenceNumber: 1,
		Levels: []core.SnapshotLevelManifest{
			{
				LevelNumber: 0,
				Tables: []core.SSTableMetadata{
					{ID: 1, FileName: filepath.Join("sst", "1.sst")},
					{ID: 2, FileName: filepath.Join("sst", "2.sst")}, // This file does not exist
				},
			},
		},
	}
	manifestFileName := "MANIFEST_missing.bin"
	manifestPath := filepath.Join(snapshotDir, manifestFileName)
	f, err := os.Create(manifestPath)
	require.NoError(t, err) //nolint:staticcheck
	require.NoError(t, WriteManifestBinary(f, manifest))
	f.Close()
	require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "CURRENT"), []byte(manifestFileName), 0644))

	// 2. Execution
	restoreOpts := RestoreOptions{DataDir: targetDataDir}
	err = RestoreFromFull(restoreOpts, snapshotDir)
	require.NoError(t, err, "Restore should succeed even with a missing file, logging a warning")

	// 3. Verification
	// The valid file should be restored
	assert.FileExists(t, filepath.Join(targetDataDir, "sst", "1.sst"))
	// The missing file should not exist
	_, err = os.Stat(filepath.Join(targetDataDir, "sst", "2.sst"))
	assert.True(t, os.IsNotExist(err), "The missing file should not have been created in the target directory")
}

func TestManager_CreateFull_WriteCurrentFileError(t *testing.T) {
	// This test replaces the package-level osWriteFile function to simulate errors.
	helper := &mockSnapshotHelper{
		helperSnapshot: newHelperSnapshot(),
	}

	// 1. Setup
	tempDir := t.TempDir()
	snapshotDir := filepath.Join(tempDir, "snapshot_current_error")
	dataDir := filepath.Join(tempDir, "data_current_error")
	require.NoError(t, os.MkdirAll(dataDir, 0755))

	provider := newMockEngineProvider(t, dataDir)
	// We need to create the WAL and SST directories so dependent functions don't fail
	require.NoError(t, os.MkdirAll(provider.walDir, 0755))
	require.NoError(t, os.MkdirAll(provider.sstDir, 0755))

	// Create a dummy SSTable to have something in the manifest
	sst1 := createDummySSTable(t, provider.sstDir, 1)
	provider.levelsManager.AddTableToLevel(0, sst1)

	// Set up mock expectations for all calls that happen before writing the CURRENT file.
	provider.On("GetMemtablesForFlush").Return()
	provider.On("FlushMemtableToL0", mock.Anything, mock.Anything).Return(nil)
	provider.On("GetDeletedSeries").Return()
	provider.On("GetRangeTombstones").Return()
	provider.tagIndexManager.On("CreateSnapshot", mock.Anything).Return(nil)
	provider.stringStore.On("GetLogFilePath").Return("")
	provider.seriesIDStore.On("GetLogFilePath").Return("")

	// 2. Simulate the error condition
	expectedErr := fmt.Errorf("simulated write CURRENT error")
	helper.InterceptWriteFile = func(name string, data []byte, perm os.FileMode) error {
		if strings.HasSuffix(name, CURRENT_FILE_NAME) {
			return expectedErr
		}
		return helper.helperSnapshot.WriteFile(name, data, perm)
	}

	// 3. Execution & Verification
	manager := NewManagerWithTesting(provider, helper)
	err := manager.CreateFull(context.Background(), snapshotDir)
	require.Error(t, err, "CreateFull should fail when os.WriteFile for CURRENT fails")
	assert.ErrorIs(t, err, expectedErr, "The returned error should wrap the one from our mock")
	assert.Contains(t, err.Error(), "failed to write CURRENT file")
	_, statErr := os.Stat(snapshotDir)
	assert.True(t, os.IsNotExist(statErr), "Snapshot directory should be cleaned up on failure")
}

func TestRestoreFromFull(t *testing.T) {
	// 1. Setup: สร้างโครงสร้างไดเรกทอรี snapshot ที่ถูกต้อง
	tempDir := t.TempDir()
	snapshotDir := filepath.Join(tempDir, "snapshot")
	targetDataDir := filepath.Join(tempDir, "restored_data")
	require.NoError(t, os.MkdirAll(snapshotDir, 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(snapshotDir, "sst"), 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(snapshotDir, "wal"), 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(snapshotDir, "index"), 0755))

	// สร้างไฟล์จำลองใน snapshot
	require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "sst", "1.sst"), []byte("sst1"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "wal", "000001.wal"), []byte("wal1"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "index", "index1.sst"), []byte("index1"), 0644)) // A file inside the index dir
	require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "index", indexer.IndexManifestFileName), []byte("{}"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "deleted_series.json"), []byte("{}"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "string_mapping.log"), []byte("str"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "series_mapping.log"), []byte("ser"), 0644))

	// สร้างเนื้อหา manifest
	manifest := &core.SnapshotManifest{
		SequenceNumber: 42,
		Levels: []core.SnapshotLevelManifest{
			{
				LevelNumber: 0,
				Tables: []core.SSTableMetadata{
					{ID: 1, FileName: filepath.Join("sst", "1.sst")},
				},
			},
		},
		WALFile:           "wal",
		DeletedSeriesFile: "deleted_series.json",
		StringMappingFile: "string_mapping.log",
		SeriesMappingFile: "series_mapping.log",
	}
	manifestFileName := "MANIFEST_12345.bin"
	manifestPath := filepath.Join(snapshotDir, manifestFileName)
	f, err := os.Create(manifestPath)
	require.NoError(t, err)
	require.NoError(t, WriteManifestBinary(f, manifest))
	f.Close()

	// สร้างไฟล์ CURRENT
	require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "CURRENT"), []byte(manifestFileName), 0644))

	// 2. Execution
	restoreOpts := RestoreOptions{
		DataDir: targetDataDir,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	err = RestoreFromFull(restoreOpts, snapshotDir)
	require.NoError(t, err)

	// 3. Verification
	// ตรวจสอบว่าไฟล์ทั้งหมดถูกคัดลอกไปยังไดเรกทอรีข้อมูลเป้าหมาย
	assert.FileExists(t, filepath.Join(targetDataDir, "sst", "1.sst"))
	assert.FileExists(t, filepath.Join(targetDataDir, "wal", "000001.wal"))
	assert.FileExists(t, filepath.Join(targetDataDir, indexer.IndexSSTDirName, "index1.sst"))
	assert.FileExists(t, filepath.Join(targetDataDir, indexer.IndexSSTDirName, indexer.IndexManifestFileName))
	assert.FileExists(t, filepath.Join(targetDataDir, "deleted_series.json"))
	assert.FileExists(t, filepath.Join(targetDataDir, "string_mapping.log"))
	assert.FileExists(t, filepath.Join(targetDataDir, "series_mapping.log"))
	assert.FileExists(t, filepath.Join(targetDataDir, "CURRENT"))
	assert.FileExists(t, filepath.Join(targetDataDir, manifestFileName))

	// ตรวจสอบเนื้อหาของไฟล์ที่คัดลอก
	content, err := os.ReadFile(filepath.Join(targetDataDir, "sst", "1.sst"))
	require.NoError(t, err)
	assert.Equal(t, "sst1", string(content))
}

func TestRestoreFromFull_TargetExists(t *testing.T) {
	// Setup: สร้าง snapshot และไดเรกทอรีเป้าหมายที่มีอยู่แล้ว
	tempDir := t.TempDir()
	snapshotDir := filepath.Join(tempDir, "snapshot_exists")
	targetDataDir := filepath.Join(tempDir, "target_exists")
	require.NoError(t, os.MkdirAll(snapshotDir, 0755))
	require.NoError(t, os.MkdirAll(targetDataDir, 0755))

	// สร้างไฟล์จำลองในเป้าหมายเพื่อให้แน่ใจว่ามันจะถูกลบ
	require.NoError(t, os.WriteFile(filepath.Join(targetDataDir, "old_file.txt"), []byte("old"), 0644))

	// สร้าง snapshot ที่ถูกต้องน้อยที่สุด
	manifestFileName := "MANIFEST_1.bin"
	manifestPath := filepath.Join(snapshotDir, manifestFileName)
	f, err := os.Create(manifestPath)
	require.NoError(t, err) //nolint:staticcheck
	require.NoError(t, WriteManifestBinary(f, &core.SnapshotManifest{}))
	f.Close()
	require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "CURRENT"), []byte(manifestFileName), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "new_file.txt"), []byte("new"), 0644))

	// Execution
	restoreOpts := RestoreOptions{DataDir: targetDataDir}
	err = RestoreFromFull(restoreOpts, snapshotDir)
	require.NoError(t, err)

	// Verification
	// ไฟล์เก่าควรจะหายไป
	_, err = os.Stat(filepath.Join(targetDataDir, "old_file.txt"))
	assert.True(t, os.IsNotExist(err), "ไฟล์เก่าในไดเรกทอรีเป้าหมายควรถูกลบ")

	// ไฟล์ใหม่จาก snapshot ควรจะอยู่
	assert.FileExists(t, filepath.Join(targetDataDir, "CURRENT"))
	assert.FileExists(t, filepath.Join(targetDataDir, manifestFileName))
}

func TestRestoreFromFull_ErrorHandling(t *testing.T) {
	tempDir := t.TempDir()

	helper := &mockSnapshotHelper{
		helperSnapshot: newHelperSnapshot(),
	}

	snapshotDir := filepath.Join(tempDir, "snapshot_err")
	targetDataDir := filepath.Join(tempDir, "target_err")
	require.NoError(t, os.MkdirAll(snapshotDir, 0755))

	restoreOpts := RestoreOptions{DataDir: targetDataDir, wrapper: helper}

	t.Run("SnapshotDirNotExist", func(t *testing.T) {
		err := RestoreFromFull(restoreOpts, "/path/to/nonexistent/snapshot")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")
	})

	t.Run("SnapshotMissingCurrentFile", func(t *testing.T) {
		err := RestoreFromFull(restoreOpts, snapshotDir)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read CURRENT file")
	})

	t.Run("CurrentFileReadError", func(t *testing.T) {
		// This test replaces the package-level osReadFile function to simulate errors.
		defer func() {
			helper.InterceptReadFile = nil
		}()
		// Setup: Create a CURRENT file so the os.Stat check passes, but reading it will fail.
		currentFilePath := filepath.Join(snapshotDir, "CURRENT")
		require.NoError(t, os.WriteFile(currentFilePath, []byte("some_manifest.bin"), 0644))
		defer os.Remove(currentFilePath) // Ensure cleanup for the next sub-test

		// Simulate the error condition
		expectedErr := fmt.Errorf("simulated read file error")
		helper.InterceptReadFile = func(name string) ([]byte, error) {
			// Only fail if it's trying to read our specific CURRENT file
			if name == currentFilePath {
				return nil, expectedErr
			}
			return helper.helperSnapshot.ReadFile(name)
		}

		// Execution
		err := RestoreFromFull(restoreOpts, snapshotDir)

		// Verification
		require.Error(t, err, "RestoreFromFull should fail when os.ReadFile fails")
		assert.ErrorIs(t, err, expectedErr, "The returned error should wrap the one from our mock")
		assert.Contains(t, err.Error(), "failed to read CURRENT file")

	})

	t.Run("SnapshotMissingManifestFile", func(t *testing.T) {
		require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "CURRENT"), []byte("MANIFEST_missing.bin"), 0644))
		err := RestoreFromFull(restoreOpts, snapshotDir)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "snapshot manifest file")
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("MkdirTempError", func(t *testing.T) {
		// This test replaces the package-level osMkdirTemp function to simulate errors.
		defer func() { helper.InterceptMkdirTemp = nil }()

		// Setup: Need a valid CURRENT and MANIFEST file to get past the initial checks.
		currentFilePath := filepath.Join(snapshotDir, "CURRENT")
		manifestFileName := "MANIFEST_dummy.bin"
		require.NoError(t, os.WriteFile(currentFilePath, []byte(manifestFileName), 0644))
		defer os.Remove(currentFilePath) // Cleanup for the next sub-test

		// Create the manifest file itself so the os.Stat check passes.
		manifestPath := filepath.Join(snapshotDir, manifestFileName)
		require.NoError(t, os.WriteFile(manifestPath, []byte("dummy content"), 0644))
		defer os.Remove(manifestPath)

		// Simulate the error condition
		expectedErr := fmt.Errorf("simulated mkdir temp error")
		helper.InterceptMkdirTemp = func(dir, pattern string) (string, error) { //nolint:unparam
			return "", expectedErr
		}

		// Execution & Verification
		err := RestoreFromFull(restoreOpts, snapshotDir)
		require.Error(t, err, "RestoreFromFull should fail when os.MkdirTemp fails")
		assert.ErrorIs(t, err, expectedErr, "The returned error should wrap the one from our mock")
		assert.Contains(t, err.Error(), "failed to create temporary restore directory")
	})
}

func TestRestoreFromFull_ErrorHandling_Continued(t *testing.T) {
	tempDir := t.TempDir()
	helper := &mockSnapshotHelper{
		helperSnapshot: newHelperSnapshot(),
	}

	snapshotDir := filepath.Join(tempDir, "snapshot_err_cont")
	targetDataDir := filepath.Join(tempDir, "target_err_cont")
	require.NoError(t, os.MkdirAll(snapshotDir, 0755))

	restoreOpts := RestoreOptions{DataDir: targetDataDir, wrapper: helper}

	t.Run("ManifestFileOpenError", func(t *testing.T) {
		defer func() { helper.InterceptOpen = nil }()

		manifestFileName := "MANIFEST_dummy.bin"
		currentFilePath := filepath.Join(snapshotDir, "CURRENT")
		require.NoError(t, os.WriteFile(currentFilePath, []byte(manifestFileName), 0644))
		defer os.Remove(currentFilePath)

		// Create the manifest file so that os.Stat passes and os.Open is called.
		manifestPath := filepath.Join(snapshotDir, manifestFileName)
		require.NoError(t, os.WriteFile(manifestPath, []byte("dummy content"), 0644))
		defer os.Remove(manifestPath)

		expectedErr := fmt.Errorf("simulated open error")
		helper.InterceptOpen = func(name string) (*os.File, error) {
			if name == manifestPath {
				return nil, expectedErr
			}
			return helper.helperSnapshot.Open(name)
		}

		err := RestoreFromFull(restoreOpts, snapshotDir)
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Contains(t, err.Error(), "failed to read snapshot manifest file")
	})

	t.Run("ReadManifestError", func(t *testing.T) {
		defer func() { helper.InterceptReadManifestBinary = nil }()
		expectedErr := fmt.Errorf("simulated read manifest error")

		helper.InterceptReadManifestBinary = func(r io.Reader) (*core.SnapshotManifest, error) {
			return nil, expectedErr
		}

		currentFilePath := filepath.Join(snapshotDir, "CURRENT")
		manifestFileName := "MANIFEST_read_err.bin"
		require.NoError(t, os.WriteFile(currentFilePath, []byte(manifestFileName), 0644))
		defer os.Remove(currentFilePath)
		manifestPath := filepath.Join(snapshotDir, manifestFileName)
		require.NoError(t, os.WriteFile(manifestPath, []byte("dummy content"), 0644))
		defer os.Remove(manifestPath)

		err := RestoreFromFull(restoreOpts, snapshotDir)
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Contains(t, err.Error(), "failed to read binary snapshot manifest")
	})

	t.Run("PreCreateSubdirError", func(t *testing.T) {
		defer func() { helper.InterceptMkdirAll = nil }()

		currentFilePath := filepath.Join(snapshotDir, "CURRENT")
		manifestFileName := "MANIFEST_mkdir_err.bin"
		require.NoError(t, os.WriteFile(currentFilePath, []byte(manifestFileName), 0644))
		defer os.Remove(currentFilePath)
		manifestPath := filepath.Join(snapshotDir, manifestFileName)
		f, err := os.Create(manifestPath)
		require.NoError(t, err)
		require.NoError(t, WriteManifestBinary(f, &core.SnapshotManifest{}))
		f.Close()
		defer os.Remove(manifestPath)

		expectedErr := fmt.Errorf("simulated mkdirall error")
		helper.InterceptMkdirAll = func(path string, perm os.FileMode) error {
			if strings.Contains(path, ".restore-tmp-") {
				return expectedErr
			}
			return helper.helperSnapshot.MkdirAll(path, perm)
		}

		err = RestoreFromFull(restoreOpts, snapshotDir)
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Contains(t, err.Error(), "failed to pre-create required subdirectory")
	})

	t.Run("CopyFileError", func(t *testing.T) {
		defer func() { helper.InterceptCopyFile = nil }()
		expectedErr := fmt.Errorf("simulated copy file error")
		helper.InterceptCopyFile = func(src, dst string) error { return expectedErr }
		currentFilePath := filepath.Join(snapshotDir, "CURRENT")
		manifestFileName := "MANIFEST_copy_file_err.bin"
		require.NoError(t, os.WriteFile(currentFilePath, []byte(manifestFileName), 0644))
		defer os.Remove(currentFilePath)
		manifestPath := filepath.Join(snapshotDir, manifestFileName)
		f, err := os.Create(manifestPath)
		require.NoError(t, err)
		manifestWithFile := &core.SnapshotManifest{DeletedSeriesFile: "deleted_series.json"} //nolint:govet
		require.NoError(t, WriteManifestBinary(f, manifestWithFile))
		f.Close()
		defer os.Remove(manifestPath)
		srcFilePath := filepath.Join(snapshotDir, "deleted_series.json")
		require.NoError(t, os.WriteFile(srcFilePath, []byte("{}"), 0644))
		defer os.Remove(srcFilePath)

		err = RestoreFromFull(restoreOpts, snapshotDir)
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Contains(t, err.Error(), "failed to copy file")
	})

	t.Run("RemoveOriginalDirError", func(t *testing.T) {
		defer func() { helper.InterceptRemoveAll = nil }()

		manifestFileName := "MANIFEST_remove_err.bin"
		require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "CURRENT"), []byte(manifestFileName), 0644))
		manifestPath := filepath.Join(snapshotDir, manifestFileName)
		f, err := os.Create(manifestPath)                                    //nolint:staticcheck
		require.NoError(t, err)                                              //nolint:staticcheck
		require.NoError(t, WriteManifestBinary(f, &core.SnapshotManifest{})) // Create a valid, empty manifest
		f.Close()

		require.NoError(t, os.MkdirAll(targetDataDir, 0755))

		expectedErr := fmt.Errorf("simulated remove all error")
		helper.InterceptRemoveAll = func(path string) error {
			if path == targetDataDir {
				return expectedErr
			}
			return helper.helperSnapshot.RemoveAll(path)
		}

		err = RestoreFromFull(restoreOpts, snapshotDir)
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Contains(t, err.Error(), "failed to remove original data directory")
	})
}

func TestRestoreFromFull_CopyDirectoryError(t *testing.T) {
	// This test replaces the package-level copyDirectoryContents function to simulate errors.
	helper := &mockSnapshotHelper{
		helperSnapshot: newHelperSnapshot(),
	}

	// 1. Setup: Create a minimal valid snapshot structure
	tempDir := t.TempDir()
	snapshotDir := filepath.Join(tempDir, "snapshot_copy_dir_err")
	targetDataDir := filepath.Join(tempDir, "target_copy_dir_err")
	require.NoError(t, os.MkdirAll(snapshotDir, 0755))
	// Create the index directory that the function will attempt to copy
	require.NoError(t, os.MkdirAll(filepath.Join(snapshotDir, indexer.IndexDirName), 0755))

	// Create manifest and CURRENT file
	manifestFileName := "MANIFEST_copy_err.bin"
	manifestPath := filepath.Join(snapshotDir, manifestFileName)
	f, err := os.Create(manifestPath)
	require.NoError(t, err)
	// An empty manifest is fine for this test, as we fail before copying its contents
	require.NoError(t, WriteManifestBinary(f, &core.SnapshotManifest{}))
	f.Close()
	require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "CURRENT"), []byte(manifestFileName), 0644))

	// 2. Simulate the error condition
	expectedErr := fmt.Errorf("simulated copy directory error")
	helper.InterceptCopyDirectoryContents = func(src, dst string) error {
		// We only want to fail on the first call, which is for the index directory
		if strings.HasSuffix(src, indexer.IndexDirName) {
			return expectedErr
		}
		return helper.helperSnapshot.CopyDirectoryContents(src, dst)
	}

	// 3. Execution
	restoreOpts := RestoreOptions{DataDir: targetDataDir, wrapper: helper}
	err = RestoreFromFull(restoreOpts, snapshotDir)

	// 4. Verification
	require.Error(t, err, "RestoreFromFull should fail when copyDirectoryContents fails")
	assert.ErrorIs(t, err, expectedErr, "The returned error should be the one from our mock")
	assert.Contains(t, err.Error(), "failed to copy tag index files from snapshot")
}

func TestRestoreFromFull_RenameError(t *testing.T) {
	// This test replaces the package-level osRename function to simulate errors.
	helper := &mockSnapshotHelper{
		helperSnapshot: newHelperSnapshot(),
	}

	// 1. Setup: Create a minimal valid snapshot structure
	tempDir := t.TempDir()
	snapshotDir := filepath.Join(tempDir, "snapshot_rename_err")
	targetDataDir := filepath.Join(tempDir, "target_rename_err")
	require.NoError(t, os.MkdirAll(snapshotDir, 0755))

	// Create manifest and CURRENT file
	manifestFileName := "MANIFEST_rename_err.bin"
	manifestPath := filepath.Join(snapshotDir, manifestFileName)
	f, err := os.Create(manifestPath)
	require.NoError(t, err) //nolint:staticcheck
	require.NoError(t, WriteManifestBinary(f, &core.SnapshotManifest{}))
	f.Close()
	require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "CURRENT"), []byte(manifestFileName), 0644))

	// 2. Simulate the error condition
	expectedErr := fmt.Errorf("simulated rename error")
	var tempRestoreDir string // Variable to capture the temp dir path
	helper.InterceptRename = func(oldpath, newpath string) error {
		tempRestoreDir = oldpath // Capture the path for verification
		return expectedErr
	}

	// 3. Execution
	restoreOpts := RestoreOptions{DataDir: targetDataDir, wrapper: helper}
	err = RestoreFromFull(restoreOpts, snapshotDir)

	// 4. Verification
	require.Error(t, err, "RestoreFromFull should fail when os.Rename fails")
	assert.ErrorIs(t, err, expectedErr, "The returned error should be the one from our mock")
	assert.Contains(t, err.Error(), "failed to rename temporary restore directory")

	// Verify the temporary directory was created and then cleaned up by the defer block.
	require.NotEmpty(t, tempRestoreDir, "Temporary restore directory path should have been captured")
	_, statErr := os.Stat(tempRestoreDir)
	assert.True(t, os.IsNotExist(statErr), "Temporary restore directory should be cleaned up on failure")
}

func TestRestoreFromFull_TargetIsAFile(t *testing.T) {
	// 1. Setup
	tempDir := t.TempDir()
	snapshotDir := filepath.Join(tempDir, "snapshot_target_is_file")
	targetDataDir := filepath.Join(tempDir, "target_is_a_file")

	// Create a minimal valid snapshot
	require.NoError(t, os.MkdirAll(snapshotDir, 0755))
	manifestFileName := "MANIFEST_1.bin"
	manifestPath := filepath.Join(snapshotDir, manifestFileName)
	f, err := os.Create(manifestPath)
	require.NoError(t, err) //nolint:staticcheck
	require.NoError(t, WriteManifestBinary(f, &core.SnapshotManifest{}))
	f.Close()
	require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "CURRENT"), []byte(manifestFileName), 0644))

	// Create the target as a file instead of a directory
	require.NoError(t, os.WriteFile(targetDataDir, []byte("i am a file"), 0644))

	// 2. Execution
	restoreOpts := RestoreOptions{DataDir: targetDataDir}
	err = RestoreFromFull(restoreOpts, snapshotDir)

	// 3. Verification
	// The operation should succeed. os.RemoveAll works on files, so the old file
	// is removed, and then the temporary directory is renamed to the target path.
	require.NoError(t, err, "Restore should succeed even if the target data directory is a file")

	// Verify that the target is now a directory.
	info, statErr := os.Stat(targetDataDir)
	require.NoError(t, statErr, "Target data path should exist after restore")
	assert.True(t, info.IsDir(), "Target data path should be a directory after restore")
}

func TestCreateFull_AuxiliaryFileNotExist(t *testing.T) {
	// This test ensures that if an auxiliary file (e.g., string mapping log)
	// does not exist, the snapshot creation proceeds without error, and the
	// manifest field is simply left empty.

	// 1. Setup
	tempDir := t.TempDir()
	snapshotDir := filepath.Join(tempDir, "snapshot_aux_missing")
	dataDir := filepath.Join(tempDir, "data_aux_missing")
	require.NoError(t, os.MkdirAll(dataDir, 0755))

	provider := newMockEngineProvider(t, dataDir)

	// Simulate non-existent auxiliary files by having GetLogFilePath return an empty string
	provider.stringStore.On("GetLogFilePath").Return("")
	provider.seriesIDStore.On("GetLogFilePath").Return("")

	// Set up other mock expectations for a successful run
	provider.On("GetMemtablesForFlush").Return()
	provider.On("GetDeletedSeries").Return()
	provider.On("GetRangeTombstones").Return()
	provider.tagIndexManager.On("CreateSnapshot", mock.Anything).Return(nil)

	// 2. Execution
	manager := NewManager(provider)
	err := manager.CreateFull(context.Background(), snapshotDir)
	require.NoError(t, err, "Snapshot creation should succeed even if auxiliary files are missing")

	// 3. Verification
	currentBytes, err := os.ReadFile(filepath.Join(snapshotDir, "CURRENT"))
	require.NoError(t, err)
	manifestFileName := string(currentBytes)
	f, err := os.Open(filepath.Join(snapshotDir, manifestFileName))
	require.NoError(t, err)
	defer f.Close()
	manifest, err := ReadManifestBinary(f)
	require.NoError(t, err)

	assert.Empty(t, manifest.StringMappingFile, "StringMappingFile should be empty in manifest")
	assert.Empty(t, manifest.SeriesMappingFile, "SeriesMappingFile should be empty in manifest")
}
