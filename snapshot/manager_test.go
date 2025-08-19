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
	"github.com/INLOpen/nexusbase/sys"
	"github.com/INLOpen/nexusbase/wal"
	"github.com/INLOpen/nexuscore/utils/clock"
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
	clock   clock.Clock
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
	wal                *mockWAL
	flushedMemtables   []*memtable.Memtable
	isStarted          bool
}

func newMockEngineProvider(t *testing.T, dataDir string) *mockEngineProvider {
	t.Helper()
	lm, err := levels.NewLevelsManager(7, 4, 1024, noop.NewTracerProvider().Tracer(""), levels.PickOldest, 1.5, 1.0)
	require.NoError(t, err)

	return &mockEngineProvider{
		dataDir:            dataDir,
		sstDir:             filepath.Join(dataDir, "sst"),
		walDir:             filepath.Join(dataDir, "wal"),
		logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		clock:              clock.NewMockClock(time.Now()),
		tracer:             noop.NewTracerProvider().Tracer("test"),
		hooks:              hooks.NewHookManager(nil),
		levelsManager:      lm,
		tagIndexManager:    new(mockTagIndexManager),
		stringStore:        newMockPrivateManagerStore(filepath.Join(dataDir, core.StringMappingLogName)),
		seriesIDStore:      newMockPrivateManagerStore(filepath.Join(dataDir, core.SeriesMappingLogName)),
		sstableCompression: "none",
		wal:                new(mockWAL),
		isStarted:          true,
	}
}

func (m *mockEngineProvider) CheckStarted() error {
	// Lenient mock: just return the state, don't require an expectation.
	if m.isStarted {
		return nil
	}
	return fmt.Errorf("engine not started")
}
func (m *mockEngineProvider) GetWAL() wal.WALInterface { return m.wal }
func (m *mockEngineProvider) GetClock() clock.Clock    { return m.clock }
func (m *mockEngineProvider) GetLogger() *slog.Logger {
	// Lenient mock: just return the logger.
	return m.logger
}
func (m *mockEngineProvider) GetTracer() trace.Tracer {
	// Lenient mock: just return the tracer.
	return m.tracer
}
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
func (m *mockEngineProvider) GetSequenceNumber() uint64 {
	args := m.Called()
	return args.Get(0).(uint64)
}
func (m *mockEngineProvider) Lock()   { m.lockMu.Lock() }
func (m *mockEngineProvider) Unlock() { m.lockMu.Unlock() }

func (m *mockEngineProvider) GetMemtablesForFlush() ([]*memtable.Memtable, *memtable.Memtable) {
	args := m.Called()
	var memsToFlush []*memtable.Memtable
	if len(args) > 0 && args.Get(0) != nil {
		memsToFlush = args.Get(0).([]*memtable.Memtable)
	}

	var newMem *memtable.Memtable
	if len(args) > 1 && args.Get(1) != nil {
		newMem, _ = args.Get(1).(*memtable.Memtable)
	}
	return memsToFlush, newMem
}

func (m *mockEngineProvider) FlushMemtableToL0(mem *memtable.Memtable, parentCtx context.Context) error {
	args := m.Called(mem, parentCtx)
	m.flushedMemtables = append(m.flushedMemtables, mem)
	return args.Error(0)
}

func (m *mockEngineProvider) GetDeletedSeries() map[string]uint64 {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(map[string]uint64)
}

func (m *mockEngineProvider) GetRangeTombstones() map[string][]core.RangeTombstone {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(map[string][]core.RangeTombstone)
}

func (m *mockEngineProvider) GetDataDir() string {
	args := m.Called()
	return args.String(0)
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
	return os.WriteFile(filepath.Join(snapshotDir, core.IndexManifestFileName), []byte(`{"levels":[]}`), 0644)
}
func (m *mockTagIndexManager) RestoreFromSnapshot(snapshotDir string) error {
	args := m.Called(snapshotDir)
	return args.Error(0)
}
func (m *mockTagIndexManager) LoadFromFile(dataDir string) error {
	args := m.Called(dataDir)
	return args.Error(0)
}

// mockWALReader is a mock for wal.WALReader
type mockWALReader struct {
	mock.Mock
}

func (m *mockWALReader) Next(ctx context.Context) (*core.WALEntry, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*core.WALEntry), args.Error(1)
}

func (m *mockWALReader) Close() error {
	args := m.Called()
	if len(args) == 0 {
		return nil
	}
	return args.Error(0)
}

// mockWAL is a mock for wal.WALInterface
type mockWAL struct {
	mock.Mock
}

func (m *mockWAL) AppendBatch(entries []core.WALEntry) error { return m.Called(entries).Error(0) }
func (m *mockWAL) Append(entry core.WALEntry) error          { return m.Called(entry).Error(0) }
func (m *mockWAL) Sync() error                               { return m.Called().Error(0) }
func (m *mockWAL) Purge(upToIndex uint64) error              { return m.Called(upToIndex).Error(0) }
func (m *mockWAL) Close() error                              { return m.Called().Error(0) }
func (m *mockWAL) Path() string                              { return m.Called().String(0) }
func (m *mockWAL) SetTestingOnlyInjectCloseError(err error)  { m.Called(err) }
func (m *mockWAL) ActiveSegmentIndex() uint64 {
	args := m.Called()
	return args.Get(0).(uint64)
}
func (m *mockWAL) Rotate() error { return m.Called().Error(0) }

func (m *mockWAL) OpenReader(fromSeqNum uint64) (wal.WALReader, error) {
	args := m.Called(fromSeqNum)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(wal.WALReader), args.Error(1)
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
	InterceptLinkOrCopyFile              func(src, dst string) error
	InterceptRemoveAll                   func(path string) error
	InterceptCreate                      func(name string) (*os.File, error)
	InterceptWriteFile                   func(filename string, data []byte, perm os.FileMode) error
	InterceptReadFile                    func(filename string) ([]byte, error)
	InterceptMkdirTemp                   func(dir, pattern string) (string, error)
	InterceptMkdirAll                    func(path string, perm os.FileMode) error
	InterceptOpen                        func(name string) (sys.FileInterface, error)
	InterceptStat                        func(name string) (os.FileInfo, error)
	InterceptCopyFile                    func(src, dst string) error
	InterceptReadManifestBinary          func(r io.Reader) (*core.SnapshotManifest, error)
	InterceptReadDir                     func(name string) ([]os.DirEntry, error)
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

func (ms *mockSnapshotHelper) Create(name string) (sys.FileInterface, error) {
	if ms.InterceptCreate != nil {
		return ms.InterceptCreate(name)
	}
	return ms.helperSnapshot.Create(name)
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

func (ms *mockSnapshotHelper) Open(name string) (sys.FileInterface, error) {
	if ms.InterceptOpen != nil {
		return ms.InterceptOpen(name)
	}
	return ms.helperSnapshot.Open(name)
}

func (ms *mockSnapshotHelper) Stat(name string) (os.FileInfo, error) {
	if ms.InterceptStat != nil {
		return ms.InterceptStat(name)
	}
	return ms.helperSnapshot.Stat(name)
}

func (ms *mockSnapshotHelper) ReadManifestBinary(r io.Reader) (*core.SnapshotManifest, error) {
	if ms.InterceptReadManifestBinary != nil {
		return ms.InterceptReadManifestBinary(r)
	}
	return ms.helperSnapshot.ReadManifestBinary(r)
}

func (ms *mockSnapshotHelper) ReadDir(name string) ([]os.DirEntry, error) {
	if ms.InterceptReadDir != nil {
		return ms.InterceptReadDir(name)
	}
	return ms.helperSnapshot.ReadDir(name)
}

func (ms *mockSnapshotHelper) CopyFile(src, dst string) error {
	if ms.InterceptCopyFile != nil {
		return ms.InterceptCopyFile(src, dst)
	}
	// Re-implement the logic from helperSnapshot.CopyFile but using `ms` as the receiver for other calls
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file %s: %w", src, err)
	}
	defer in.Close()

	if err := ms.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return fmt.Errorf("failed to create destination directory for %s: %w", dst, err)
	}

	out, err := ms.Create(dst)
	if err != nil {
		return fmt.Errorf("failed to create destination file %s: %w", dst, err)
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return fmt.Errorf("failed to copy data from %s to %s: %w", src, dst, err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("failed to close destination file %s: %w", dst, err)
	}
	return nil
}

func (ms *mockSnapshotHelper) Rename(oldPath, newPath string) error {
	if ms.InterceptRename != nil {
		return ms.InterceptRename(oldPath, newPath)
	}
	return ms.helperSnapshot.Rename(oldPath, newPath)
}

func (ms *mockSnapshotHelper) LinkOrCopyFile(src, dst string) error {
	if ms.InterceptLinkOrCopyFile != nil {
		return ms.InterceptLinkOrCopyFile(src, dst)
	}
	// Re-implement to ensure calls go through the mock receiver
	if err := ms.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return fmt.Errorf("failed to create destination directory for link %s: %w", dst, err)
	}
	err := os.Link(src, dst)
	if err == nil {
		return nil
	}
	return ms.CopyFile(src, dst) // Call the (now fixed) mock CopyFile
}

//

// --- Helper Functions ---

// setupRestorerTest creates a minimal valid snapshot directory and a restorer instance for testing.
func setupRestorerTest(t *testing.T) (r *restorer, snapshotDir, targetDataDir string, helper *mockSnapshotHelper) {
	t.Helper()
	tempDir := t.TempDir()
	snapshotDir = filepath.Join(tempDir, "snapshot")
	targetDataDir = filepath.Join(tempDir, "target")
	require.NoError(t, os.MkdirAll(snapshotDir, 0755))

	// Create a minimal manifest
	manifest := &core.SnapshotManifest{
		SequenceNumber: 1,
		// Add files that will be copied to trigger different code paths
		DeletedSeriesFile: "deleted.json",
		WALFile:           "wal",
		Levels: []core.SnapshotLevelManifest{
			{LevelNumber: 0, Tables: []core.SSTableMetadata{{ID: 1, FileName: "sst/1.sst"}}},
		},
	}
	// Create the files mentioned in the manifest inside the snapshot dir
	require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "deleted.json"), []byte("{}"), 0644))
	require.NoError(t, os.MkdirAll(filepath.Join(snapshotDir, "sst"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "sst", "1.sst"), []byte("data"), 0644))
	require.NoError(t, os.MkdirAll(filepath.Join(snapshotDir, "wal"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "wal", "000001.wal"), []byte("wal"), 0644))
	require.NoError(t, os.MkdirAll(filepath.Join(snapshotDir, core.IndexDirName), 0755)) // index dir
	require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, core.IndexDirName, "index.sst"), []byte("index"), 0644))

	manifestFileName, err := writeTestManifest(snapshotDir, manifest)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "CURRENT"), []byte(manifestFileName), 0644))

	helper = &mockSnapshotHelper{helperSnapshot: newHelperSnapshot()}
	opts := RestoreOptions{
		DataDir: targetDataDir,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		wrapper: helper,
	}

	// Create the restorer instance. We call validateAndPrepare here so sub-tests can focus on later stages.
	r = &restorer{
		opts:        opts,
		snapshotDir: snapshotDir,
		logger:      opts.Logger.With("component", "RestoreFromSnapshot"),
		wrapper:     opts.wrapper,
	}

	return r, snapshotDir, targetDataDir, helper
}

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
	provider.On("GetSequenceNumber").Return(uint64(123)).Once()
	provider.deletedSeries = map[string]uint64{"deleted_series_1": 100}
	provider.rangeTombstones = map[string][]core.RangeTombstone{
		"range_tombstone_series_1": {{MinTimestamp: 100, MaxTimestamp: 200, SeqNum: 101}},
	}
	mem1 := memtable.NewMemtable(1024, provider.clock)
	mem1.Put([]byte("mem_key_1"), []byte("mem_val_1"), core.EntryTypePutEvent, 120)
	provider.memtablesToFlush = []*memtable.Memtable{mem1}

	// สร้าง SSTables จำลองและเพิ่มเข้าไปใน mock levels manager
	require.NoError(t, os.MkdirAll(provider.sstDir, 0755))
	sst1 := createDummySSTable(t, provider.sstDir, 1)
	sst2 := createDummySSTable(t, provider.sstDir, 2)
	provider.levelsManager.AddTableToLevel(0, sst1)
	provider.levelsManager.AddTableToLevel(1, sst2)

	// สร้างไฟล์ WAL จำลอง
	require.NoError(t, os.MkdirAll(provider.walDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(provider.walDir, "000001.wal"), []byte("wal data 1"), 0644))

	// ตั้งค่า mock expectations
	provider.On("GetDeletedSeries").Return(provider.deletedSeries)
	provider.On("GetRangeTombstones").Return(provider.rangeTombstones)
	provider.tagIndexManager.On("CreateSnapshot", filepath.Join(snapshotDir, "index")).Return(nil)
	provider.wal.On("ActiveSegmentIndex").Return(uint64(1))
	provider.wal.On("Path").Return(provider.walDir)
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
	defer f.Close() //nolint:staticcheck
	manifest, err := ReadManifestBinary(f)
	require.NoError(t, err)

	assert.Equal(t, uint64(123), manifest.SequenceNumber)
	assert.Equal(t, provider.sstableCompression, manifest.SSTableCompression)
	require.Len(t, manifest.Levels, 2)
	assert.Equal(t, 0, manifest.Levels[0].LevelNumber)
	require.Len(t, manifest.Levels[0].Tables, 1)
	assert.Equal(t, sst1.ID(), manifest.Levels[0].Tables[0].ID)
	assert.Equal(t, filepath.Join("sst", "1.sst"), manifest.Levels[0].Tables[0].FileName)
	assert.Equal(t, 1, manifest.Levels[1].LevelNumber)
	require.Len(t, manifest.Levels[1].Tables, 1)
	assert.Equal(t, sst2.ID(), manifest.Levels[1].Tables[0].ID)
	assert.Equal(t, string(core.SnapshotTypeFull), string(manifest.Type))
	assert.Empty(t, manifest.ParentID)
	assert.False(t, manifest.CreatedAt.IsZero())
	assert.Equal(t, uint64(1), manifest.LastWALSegmentIndex)

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
	require.FileExists(t, filepath.Join(snapshotDir, "index", core.IndexManifestFileName))

	// The assertion for flushing memtables is removed because this responsibility
	// has been moved to the engine layer, which calls the snapshot manager.
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
	provider.On("GetDeletedSeries").Return(nil)
	provider.On("GetRangeTombstones").Return(nil)
	provider.On("GetSequenceNumber").Return(uint64(0)).Once()

	provider.wal.On("ActiveSegmentIndex").Return(uint64(0))
	provider.stringStore.On("GetLogFilePath").Return(provider.stringStore.path)
	provider.seriesIDStore.On("GetLogFilePath").Return(provider.seriesIDStore.path)
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
	provider.On("GetDeletedSeries").Return(nil)
	provider.On("GetRangeTombstones").Return(nil)
	provider.On("GetSequenceNumber").Return(uint64(0)).Once()
	provider.wal.On("ActiveSegmentIndex").Return(uint64(0))
	provider.stringStore.On("GetLogFilePath").Return(provider.stringStore.path)
	provider.seriesIDStore.On("GetLogFilePath").Return(provider.seriesIDStore.path)
	provider.tagIndexManager.On("CreateSnapshot", mock.Anything).Return(nil)

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

		provider.On("GetDeletedSeries").Return(provider.deletedSeries)
		provider.On("GetRangeTombstones").Return(nil)
		provider.On("GetSequenceNumber").Return(uint64(0)).Once()
		provider.tagIndexManager.On("CreateSnapshot", mock.Anything).Return(nil) // Add expectation for this call
		provider.wal.On("ActiveSegmentIndex").Return(uint64(0))
		provider.stringStore.On("GetLogFilePath").Return(provider.stringStore.path)
		provider.seriesIDStore.On("GetLogFilePath").Return(provider.seriesIDStore.path)

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

		provider.On("GetDeletedSeries").Return(nil)
		provider.On("GetRangeTombstones").Return(provider.rangeTombstones)
		provider.On("GetSequenceNumber").Return(uint64(0)).Once()
		provider.tagIndexManager.On("CreateSnapshot", mock.Anything).Return(nil) // Add expectation for this call
		provider.wal.On("ActiveSegmentIndex").Return(uint64(0))
		provider.stringStore.On("GetLogFilePath").Return(provider.stringStore.path)
		provider.seriesIDStore.On("GetLogFilePath").Return(provider.seriesIDStore.path)

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
		provider.On("GetDeletedSeries").Return(nil)
		provider.On("GetRangeTombstones").Return(nil)
		provider.On("GetSequenceNumber").Return(uint64(0)).Once()
		provider.tagIndexManager.On("CreateSnapshot", mock.Anything).Return(nil)
		provider.wal.On("ActiveSegmentIndex").Return(uint64(0))
		provider.seriesIDStore.On("GetLogFilePath").Return(provider.seriesIDStore.path)
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
		provider.On("GetDeletedSeries").Return(nil)
		provider.On("GetRangeTombstones").Return(nil)
		provider.On("GetSequenceNumber").Return(uint64(0)).Once()
		provider.tagIndexManager.On("CreateSnapshot", mock.Anything).Return(nil)
		provider.wal.On("ActiveSegmentIndex").Return(uint64(1))
		provider.stringStore.On("GetLogFilePath").Return("") // Return empty string to simulate no file
		provider.wal.On("Path").Return(provider.walDir)
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
	helper := &mockSnapshotHelper{
		helperSnapshot: newHelperSnapshot(),
	}
	snapshotDir := filepath.Join(tempDir, "snapshot_manifest_error")
	dataDir := filepath.Join(tempDir, "data_manifest_error")
	require.NoError(t, os.MkdirAll(dataDir, 0755))

	provider := newMockEngineProvider(t, dataDir)
	// We need to create the WAL directory so linkOrCopyDirectoryContents doesn't fail
	require.NoError(t, os.MkdirAll(provider.walDir, 0755))

	// Set up mock expectations for all calls that happen before writing the manifest.
	provider.On("GetDeletedSeries").Return(nil)
	provider.On("GetRangeTombstones").Return(nil)
	provider.On("GetSequenceNumber").Return(uint64(0)).Once()
	provider.tagIndexManager.On("CreateSnapshot", mock.Anything).Return(nil)
	provider.wal.On("ActiveSegmentIndex").Return(uint64(0))
	provider.stringStore.On("GetLogFilePath").Return("") // Return empty string to simulate no file
	provider.wal.On("Path").Return(provider.walDir)
	provider.seriesIDStore.On("GetLogFilePath").Return("") // Return empty string to simulate no file

	// 2. Simulate the error condition
	mgr := NewManager(provider)
	// Cast to the concrete type to modify the function field for the test.
	concreteManager, ok := mgr.(*manager)
	require.True(t, ok)
	concreteManager.wrapper = helper
	expectedErr := fmt.Errorf("simulated write manifest error")
	concreteManager.writeManifestAndCurrentFunc = func(snapshotDir string, manifest *core.SnapshotManifest) (string, error) {
		return "", expectedErr
	}

	// 3. Execution & Verification
	err := concreteManager.CreateFull(context.Background(), snapshotDir)
	require.Error(t, err, "CreateFull should fail when writeManifestBinary fails")
	assert.ErrorIs(t, err, expectedErr, "The returned error should wrap the one from our mock")
	// The error message will be exactly the expected error, as we replaced the whole function.
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
	provider.On("GetSequenceNumber").Return(uint64(1)).Once()
	// Levels manager is already empty by default

	// Set up mock expectations for an empty run
	provider.On("GetDeletedSeries").Return(nil)
	provider.On("GetRangeTombstones").Return(nil)
	provider.tagIndexManager.On("CreateSnapshot", mock.Anything).Return(nil)
	provider.wal.On("ActiveSegmentIndex").Return(uint64(0))
	provider.wal.On("Path").Return(provider.walDir)
	provider.wal.On("Path").Return(provider.walDir)
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

	f, err := os.Open(manifestPath) //nolint:staticcheck
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

	// Add the required mock expectation for the initial check

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

func TestManager_CreateIncremental(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		// 1. Setup Phase
		tempDir := t.TempDir()
		snapshotsBaseDir := filepath.Join(tempDir, "snapshots")
		dataDir := filepath.Join(tempDir, "data")
		require.NoError(t, os.MkdirAll(dataDir, 0755))
		require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))

		provider := newMockEngineProvider(t, dataDir)
		mockClock, ok := provider.clock.(*clock.MockClock)
		require.True(t, ok, "provider clock should be a mock clock")

		require.NoError(t, os.MkdirAll(provider.sstDir, 0755))
		require.NoError(t, os.MkdirAll(provider.walDir, 0755))

		manager := NewManager(provider)

		// --- Define state for parent snapshot ---
		parentTime := mockClock.Now()
		parentSnapshotDir := filepath.Join(snapshotsBaseDir, fmt.Sprintf("%d", parentTime.UnixNano()))
		parentSnapshotID := filepath.Base(parentSnapshotDir)
		parentSST := createDummySSTable(t, provider.sstDir, 1)

		// --- Define state for incremental snapshot ---
		mockClock.Advance(time.Second) // Advance time for new snapshot ID
		newSST := createDummySSTable(t, provider.sstDir, 2)

		// --- Set ALL mock expectations upfront ---

		// Mocks for CreateFull call
		provider.On("GetSequenceNumber").Return(uint64(100)).Once() // For CreateFull
		provider.On("GetDeletedSeries").Return(nil).Once()
		provider.On("GetRangeTombstones").Return(nil).Once()
		provider.tagIndexManager.On("CreateSnapshot", filepath.Join(parentSnapshotDir, "index")).Return(nil).Once()
		provider.wal.On("ActiveSegmentIndex").Return(uint64(1)).Once()
		provider.stringStore.On("GetLogFilePath").Return(provider.stringStore.path).Once()
		provider.seriesIDStore.On("GetLogFilePath").Return(provider.seriesIDStore.path).Once()
		provider.wal.On("Path").Return(provider.walDir).Once()

		// Mocks for CreateIncremental call
		provider.On("GetSequenceNumber").Return(uint64(150)).Once() // For findAndValidateParent check
		provider.On("GetMemtablesForFlush").Return(nil, nil).Once()
		provider.On("GetSequenceNumber").Return(uint64(150)).Once() // For new manifest
		provider.On("GetDeletedSeries").Return(map[string]uint64{"new_deleted": 140}).Once()
		provider.On("GetRangeTombstones").Return(nil).Once()
		provider.tagIndexManager.On("CreateSnapshot", mock.MatchedBy(func(path string) bool {
			return strings.HasSuffix(path, "index") && strings.Contains(path, "_incr")
		})).Return(nil).Once()
		provider.wal.On("ActiveSegmentIndex").Return(uint64(2)).Once() // New WAL segment
		provider.stringStore.On("GetLogFilePath").Return(provider.stringStore.path).Once()
		provider.seriesIDStore.On("GetLogFilePath").Return(provider.seriesIDStore.path).Once()
		provider.wal.On("Path").Return(provider.walDir).Once()

		// --- Execution Phase ---
		// 1. Create the Full Parent Snapshot
		provider.levelsManager.AddTableToLevel(0, parentSST) // State for CreateFull
		err := manager.CreateFull(context.Background(), parentSnapshotDir)
		require.NoError(t, err)

		// 2. Prepare and Create the Incremental Snapshot
		provider.levelsManager.AddTableToLevel(0, newSST) // State for CreateIncremental
		err = manager.CreateIncremental(context.Background(), snapshotsBaseDir)
		require.NoError(t, err)

		// --- Verification Phase ---
		// Find the new incremental snapshot directory
		latestID, latestPath, err := findLatestSnapshot(snapshotsBaseDir, newHelperSnapshot())
		require.NoError(t, err)
		require.NotEqual(t, parentSnapshotID, latestID, "A new snapshot directory should have been created")
		assert.True(t, strings.HasSuffix(latestID, "_incr"), "New snapshot should be marked as incremental")

		// Verify its contents
		manifest, _, err := readManifestFromDir(latestPath, newHelperSnapshot())
		require.NoError(t, err)

		assert.Equal(t, core.SnapshotTypeIncremental, manifest.Type)
		assert.Equal(t, parentSnapshotID, manifest.ParentID)
		assert.Equal(t, uint64(150), manifest.SequenceNumber)
		assert.Equal(t, uint64(2), manifest.LastWALSegmentIndex)

		// Check that only the NEW sstable is in the manifest
		require.Len(t, manifest.Levels, 1)
		require.Len(t, manifest.Levels[0].Tables, 1)
		assert.Equal(t, newSST.ID(), manifest.Levels[0].Tables[0].ID)

		// Check that the new SSTable file was physically copied
		assert.FileExists(t, filepath.Join(latestPath, "sst", "2.sst"))
		// Check that the OLD SSTable file was NOT copied
		assert.NoFileExists(t, filepath.Join(latestPath, "sst", "1.sst"))

		// Check that auxiliary files were copied
		assert.FileExists(t, filepath.Join(latestPath, "deleted_series.json"))

		provider.AssertExpectations(t)
		provider.tagIndexManager.AssertExpectations(t)
	})

	t.Run("NoParent", func(t *testing.T) {
		// 1. Setup
		tempDir := t.TempDir()
		snapshotsBaseDir := filepath.Join(tempDir, "snapshots_no_parent")
		dataDir := filepath.Join(tempDir, "data_no_parent")
		require.NoError(t, os.MkdirAll(dataDir, 0755))
		require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))

		provider := newMockEngineProvider(t, dataDir)

		// 2. Execution
		manager := NewManager(provider)
		err := manager.CreateIncremental(context.Background(), snapshotsBaseDir)

		// 3. Verification
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no parent snapshot found")
	})

	t.Run("NoChanges", func(t *testing.T) {
		// 1. Setup
		tempDir := t.TempDir()
		snapshotsBaseDir := filepath.Join(tempDir, "snapshots_no_changes")
		dataDir := filepath.Join(tempDir, "data_no_changes")
		require.NoError(t, os.MkdirAll(dataDir, 0755))
		require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))

		provider := newMockEngineProvider(t, dataDir)
		manager := NewManager(provider)

		// Create a full parent snapshot
		parentSnapshotDir := filepath.Join(snapshotsBaseDir, "parent")
		provider.On("GetSequenceNumber").Return(uint64(100)).Once() // For CreateFull
		provider.On("GetSequenceNumber").Return(uint64(100)).Once() // For CreateIncremental check
		provider.On("GetDeletedSeries").Return(nil)
		provider.On("GetRangeTombstones").Return(nil)
		provider.tagIndexManager.On("CreateSnapshot", mock.Anything).Return(nil)
		provider.wal.On("ActiveSegmentIndex").Return(uint64(1))
		provider.stringStore.On("GetLogFilePath").Return("")
		provider.seriesIDStore.On("GetLogFilePath").Return("")
		provider.wal.On("Path").Return(provider.walDir)
		err := manager.CreateFull(context.Background(), parentSnapshotDir)
		require.NoError(t, err)

		// Do not make any changes to the provider state. The sequence number is still 100.

		// 2. Execution
		err = manager.CreateIncremental(context.Background(), snapshotsBaseDir)

		// 3. Verification
		require.NoError(t, err, "CreateIncremental should not return an error when there are no changes")

		// Check that no new snapshot directory was created
		entries, err := os.ReadDir(snapshotsBaseDir)
		require.NoError(t, err)
		assert.Len(t, entries, 1, "No new snapshot directory should be created")
	})

	t.Run("ParentManifestReadError", func(t *testing.T) {
		// 1. Setup
		tempDir := t.TempDir()
		snapshotsBaseDir := filepath.Join(tempDir, "snapshots_parent_err")
		dataDir := filepath.Join(tempDir, "data_parent_err")
		require.NoError(t, os.MkdirAll(dataDir, 0755))
		require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))

		// Create a "valid" parent snapshot directory structure
		parentSnapshotDir := filepath.Join(snapshotsBaseDir, "parent")
		require.NoError(t, os.MkdirAll(parentSnapshotDir, 0755))

		// Create a corrupted MANIFEST file
		manifestPath := filepath.Join(parentSnapshotDir, "MANIFEST_corrupt.bin")
		require.NoError(t, os.WriteFile(manifestPath, []byte("this is not a valid manifest"), 0644))

		// Create the CURRENT file pointing to the corrupted manifest
		require.NoError(t, os.WriteFile(filepath.Join(parentSnapshotDir, "CURRENT"), []byte("MANIFEST_corrupt.bin"), 0644))

		// 2. Execution
		provider := newMockEngineProvider(t, dataDir)
		provider.On("GetSequenceNumber").Return(uint64(100))
		manager := NewManager(provider)
		err := manager.CreateIncremental(context.Background(), snapshotsBaseDir)

		// 3. Verification
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read parent snapshot manifest")
	})
}

func TestManager_CreateIncremental_ErrorPaths(t *testing.T) {
	// createTestParentSnapshot creates a valid parent snapshot directory structure for testing.
	createTestParentSnapshot := func(t *testing.T, snapshotsBaseDir string, seqNum uint64, sstMetas []core.SSTableMetadata) string {
		t.Helper()
		parentID := fmt.Sprintf("%d", time.Now().UnixNano())
		parentSnapshotDir := filepath.Join(snapshotsBaseDir, parentID)
		require.NoError(t, os.MkdirAll(parentSnapshotDir, 0755))
		require.NoError(t, os.MkdirAll(filepath.Join(parentSnapshotDir, "sst"), 0755))

		manifest := &core.SnapshotManifest{
			Type:           core.SnapshotTypeFull,
			SequenceNumber: seqNum,
			Levels: []core.SnapshotLevelManifest{
				{LevelNumber: 0, Tables: sstMetas},
			},
		}

		manifestFileName, err := writeTestManifest(parentSnapshotDir, manifest)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(parentSnapshotDir, "CURRENT"), []byte(manifestFileName), 0644))

		return parentID
	}

	t.Run("FlushError", func(t *testing.T) {
		// 1. Setup
		tempDir := t.TempDir()
		snapshotsBaseDir := filepath.Join(tempDir, "snapshots")
		dataDir := filepath.Join(tempDir, "data")
		require.NoError(t, os.MkdirAll(dataDir, 0755))
		require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))

		provider := newMockEngineProvider(t, dataDir)
		createTestParentSnapshot(t, snapshotsBaseDir, 100, nil)

		// Prepare for incremental
		memToFlush := memtable.NewMemtable(1024, provider.clock)
		expectedErr := fmt.Errorf("simulated flush error")

		// Mock calls for the incremental snapshot
		provider.On("GetSequenceNumber").Return(uint64(150)).Once() // For findAndValidateParent
		provider.On("GetMemtablesForFlush").Return([]*memtable.Memtable{memToFlush}, nil).Once()
		provider.On("FlushMemtableToL0", memToFlush, mock.Anything).Return(expectedErr).Once()

		// 2. Execution
		manager := NewManager(provider)
		err := manager.CreateIncremental(context.Background(), snapshotsBaseDir)

		// 3. Verification
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Contains(t, err.Error(), "failed to flush memtable during incremental snapshot")

		// Check that no new incremental snapshot directory was left behind
		entries, readErr := os.ReadDir(snapshotsBaseDir)
		require.NoError(t, readErr)
		assert.Len(t, entries, 1, "No new snapshot directory should be created on failure")
	})

	t.Run("NewSSTableCopyError", func(t *testing.T) {
		// 1. Setup
		tempDir := t.TempDir()
		snapshotsBaseDir := filepath.Join(tempDir, "snapshots")
		dataDir := filepath.Join(tempDir, "data")
		require.NoError(t, os.MkdirAll(dataDir, 0755))
		require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))

		provider := newMockEngineProvider(t, dataDir)
		require.NoError(t, os.MkdirAll(provider.sstDir, 0755))
		createTestParentSnapshot(t, snapshotsBaseDir, 100, nil)
		helper := &mockSnapshotHelper{helperSnapshot: newHelperSnapshot()}

		// Prepare for incremental
		newSST := createDummySSTable(t, provider.sstDir, 2)
		provider.levelsManager.AddTableToLevel(0, newSST) // Add a new table
		expectedErr := fmt.Errorf("simulated copy error")

		// Mock calls for the incremental snapshot
		provider.On("GetSequenceNumber").Return(uint64(150)).Once() // For findAndValidateParent
		provider.On("GetMemtablesForFlush").Return(nil, nil).Once()
		provider.wal.On("ActiveSegmentIndex").Return(uint64(1)).Once()
		provider.On("GetSequenceNumber").Return(uint64(150)).Once() // For new manifest
		helper.InterceptLinkOrCopyFile = func(src, dst string) error {
			if strings.Contains(src, "2.sst") {
				return expectedErr
			}
			return helper.helperSnapshot.LinkOrCopyFile(src, dst)
		}

		// 2. Execution
		manager := NewManagerWithTesting(provider, helper)
		err := manager.CreateIncremental(context.Background(), snapshotsBaseDir)

		// 3. Verification
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Contains(t, err.Error(), "failed to copy new SSTable")

		entries, readErr := os.ReadDir(snapshotsBaseDir)
		require.NoError(t, readErr)
		assert.Len(t, entries, 1)
	})

	t.Run("TagIndexSnapshotError", func(t *testing.T) {
		// 1. Setup
		tempDir := t.TempDir()
		snapshotsBaseDir := filepath.Join(tempDir, "snapshots")
		dataDir := filepath.Join(tempDir, "data")
		require.NoError(t, os.MkdirAll(dataDir, 0755))
		require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))

		provider := newMockEngineProvider(t, dataDir)
		createTestParentSnapshot(t, snapshotsBaseDir, 100, nil)

		// Prepare for incremental
		expectedErr := fmt.Errorf("simulated tag index error")

		// Mock calls for the incremental snapshot
		provider.On("GetSequenceNumber").Return(uint64(150)).Once() // For findAndValidateParent
		provider.On("GetSequenceNumber").Return(uint64(150)).Once() // For new manifest
		provider.On("GetMemtablesForFlush").Return(nil, nil).Once()
		provider.On("GetDeletedSeries").Return(nil).Once()
		provider.On("GetRangeTombstones").Return(nil).Once()
		provider.tagIndexManager.On("CreateSnapshot", mock.Anything).Return(expectedErr).Once()
		provider.wal.On("ActiveSegmentIndex").Return(uint64(1)).Once()
		provider.stringStore.On("GetLogFilePath").Return("").Once()
		provider.seriesIDStore.On("GetLogFilePath").Return("").Once()
		provider.wal.On("Path").Return(provider.walDir).Once()

		// 2. Execution
		manager := NewManager(provider)
		err := manager.CreateIncremental(context.Background(), snapshotsBaseDir)

		// 3. Verification
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Contains(t, err.Error(), "failed to create tag index snapshot")

		entries, readErr := os.ReadDir(snapshotsBaseDir)
		require.NoError(t, readErr)
		assert.Len(t, entries, 1)
	})

	t.Run("WriteManifestError", func(t *testing.T) {
		// 1. Setup
		tempDir := t.TempDir()
		snapshotsBaseDir := filepath.Join(tempDir, "snapshots")
		dataDir := filepath.Join(tempDir, "data")
		require.NoError(t, os.MkdirAll(dataDir, 0755))
		require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))

		provider := newMockEngineProvider(t, dataDir)
		createTestParentSnapshot(t, snapshotsBaseDir, 100, nil)

		// Prepare for incremental
		expectedErr := fmt.Errorf("simulated write manifest error")

		// Mock calls for the incremental snapshot
		provider.On("GetSequenceNumber").Return(uint64(150)).Once() // For findAndValidateParent
		provider.On("GetSequenceNumber").Return(uint64(150)).Once() // For new manifest
		provider.On("GetMemtablesForFlush").Return(nil, nil).Once()
		provider.On("GetDeletedSeries").Return(nil).Once()
		provider.On("GetRangeTombstones").Return(nil).Once()
		provider.tagIndexManager.On("CreateSnapshot", mock.Anything).Return(nil).Once()
		provider.wal.On("ActiveSegmentIndex").Return(uint64(1)).Once()
		provider.stringStore.On("GetLogFilePath").Return("").Once()
		provider.seriesIDStore.On("GetLogFilePath").Return("").Once()
		provider.wal.On("Path").Return(provider.walDir).Once()

		// 2. Execution
		mgr := NewManager(provider)
		// Replace the write function to inject the error
		concreteManager, ok := mgr.(*manager)
		require.True(t, ok)
		concreteManager.writeManifestAndCurrentFunc = func(snapshotDir string, manifest *core.SnapshotManifest) (string, error) {
			return "", expectedErr
		}

		err := mgr.CreateIncremental(context.Background(), snapshotsBaseDir)

		// 3. Verification
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)

		entries, readErr := os.ReadDir(snapshotsBaseDir)
		require.NoError(t, readErr)
		assert.Len(t, entries, 1)
	})

	t.Run("CreateDirError", func(t *testing.T) {
		// 1. Setup
		tempDir := t.TempDir()
		snapshotsBaseDir := filepath.Join(tempDir, "snapshots")
		dataDir := filepath.Join(tempDir, "data")
		require.NoError(t, os.MkdirAll(dataDir, 0755))
		require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))

		provider := newMockEngineProvider(t, dataDir)
		createTestParentSnapshot(t, snapshotsBaseDir, 100, nil)
		helper := &mockSnapshotHelper{helperSnapshot: newHelperSnapshot()}

		// Prepare for incremental
		expectedErr := fmt.Errorf("simulated mkdir error")

		provider.On("GetSequenceNumber").Return(uint64(150)).Once() // For findAndValidateParent
		// Mock the directory creation to fail
		helper.InterceptMkdirAll = func(path string, perm os.FileMode) error {
			// Fail only when creating the new snapshot directory
			if strings.Contains(path, "_incr") {
				return expectedErr
			}
			return os.MkdirAll(path, perm)
		}

		// 2. Execution
		manager := NewManagerWithTesting(provider, helper)
		err := manager.CreateIncremental(context.Background(), snapshotsBaseDir)

		// 3. Verification
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Contains(t, err.Error(), "failed to create incremental snapshot directory")
	})
}

// writeTestManifest is a helper to reduce boilerplate in tests.
func writeTestManifest(snapshotDir string, manifest *core.SnapshotManifest) (string, error) {
	manifestFileName := fmt.Sprintf("MANIFEST_%d.bin", time.Now().UnixNano())
	manifestPath := filepath.Join(snapshotDir, manifestFileName)
	f, err := os.Create(manifestPath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if err := WriteManifestBinary(f, manifest); err != nil {
		return "", err
	}
	return manifestFileName, nil
}

func TestRestoreFromFull_MissingFiles(t *testing.T) {
	// This test verifies that the restore process can handle cases where files
	// listed in the manifest, or expected directories, are missing from the
	// snapshot source. The restore should log a warning and continue, rather
	// than failing.

	t.Run("MissingSSTable", func(t *testing.T) {
		// 1. Setup: Create a snapshot where the manifest lists an SSTable that doesn't exist.
		tempDir := t.TempDir()
		snapshotDir := filepath.Join(tempDir, "snapshot_missing_sst")
		targetDataDir := filepath.Join(tempDir, "restored_data_missing_sst")
		require.NoError(t, os.MkdirAll(snapshotDir, 0755))
		require.NoError(t, os.MkdirAll(filepath.Join(snapshotDir, "sst"), 0755))

		// Create one valid file that should be copied
		require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "sst", "1.sst"), []byte("sst1"), 0644))

		// Create a manifest that lists the valid file AND a missing file
		manifest := &core.SnapshotManifest{
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
		manifestFileName, err := writeTestManifest(snapshotDir, manifest)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "CURRENT"), []byte(manifestFileName), 0644))

		// 2. Execution
		restoreOpts := RestoreOptions{DataDir: targetDataDir}
		err = RestoreFromFull(restoreOpts, snapshotDir)
		require.NoError(t, err, "Restore should succeed even with a missing SSTable, logging a warning")

		// 3. Verification
		assert.FileExists(t, filepath.Join(targetDataDir, "sst", "1.sst"))
		assert.NoFileExists(t, filepath.Join(targetDataDir, "sst", "2.sst"))
	})

	t.Run("MissingAuxiliaryFile", func(t *testing.T) {
		// 1. Setup: Create a snapshot where the manifest lists an auxiliary file that doesn't exist.
		tempDir := t.TempDir()
		snapshotDir := filepath.Join(tempDir, "snapshot_missing_aux")
		targetDataDir := filepath.Join(tempDir, "restored_data_missing_aux")
		require.NoError(t, os.MkdirAll(snapshotDir, 0755))

		// Create one valid file that should be copied
		require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "string_mapping.log"), []byte("str"), 0644))

		// Create a manifest that lists the valid file AND a missing file
		manifest := &core.SnapshotManifest{
			StringMappingFile: "string_mapping.log",  // This file exists
			DeletedSeriesFile: "deleted_series.json", // This file does not exist
		}
		manifestFileName, err := writeTestManifest(snapshotDir, manifest)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "CURRENT"), []byte(manifestFileName), 0644))

		// 2. Execution
		restoreOpts := RestoreOptions{DataDir: targetDataDir}
		err = RestoreFromFull(restoreOpts, snapshotDir)
		require.NoError(t, err, "Restore should succeed even with a missing auxiliary file")

		// 3. Verification
		assert.FileExists(t, filepath.Join(targetDataDir, "string_mapping.log"))
		assert.NoFileExists(t, filepath.Join(targetDataDir, "deleted_series.json"))
	})

	t.Run("MissingWALDirectory", func(t *testing.T) {
		// 1. Setup: Create a snapshot where the manifest lists a WAL directory, but it doesn't exist.
		tempDir := t.TempDir()
		snapshotDir := filepath.Join(tempDir, "snapshot_missing_wal")
		targetDataDir := filepath.Join(tempDir, "restored_data_missing_wal")
		require.NoError(t, os.MkdirAll(snapshotDir, 0755))

		// Create a manifest that lists the WAL directory
		manifest := &core.SnapshotManifest{
			WALFile: "wal", // This directory does not exist in the snapshot source
		}
		manifestFileName, err := writeTestManifest(snapshotDir, manifest)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "CURRENT"), []byte(manifestFileName), 0644))

		// 2. Execution
		restoreOpts := RestoreOptions{DataDir: targetDataDir}
		err = RestoreFromFull(restoreOpts, snapshotDir)
		require.NoError(t, err, "Restore should succeed even when the WAL directory is missing")

		// 3. Verification
		assert.NoFileExists(t, filepath.Join(targetDataDir, "wal"))
	})

	t.Run("MissingIndexDirectory", func(t *testing.T) {
		// 1. Setup: Create a snapshot that is missing the 'index' directory entirely.
		tempDir := t.TempDir()
		snapshotDir := filepath.Join(tempDir, "snapshot_missing_index")
		targetDataDir := filepath.Join(tempDir, "restored_data_missing_index")
		require.NoError(t, os.MkdirAll(snapshotDir, 0755))

		// Create a minimal valid manifest and CURRENT file. The 'index' directory is
		// copied by convention if it exists, it's not listed in the manifest.
		manifest := &core.SnapshotManifest{}
		manifestFileName, err := writeTestManifest(snapshotDir, manifest)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "CURRENT"), []byte(manifestFileName), 0644))

		// 2. Execution
		restoreOpts := RestoreOptions{DataDir: targetDataDir}
		err = RestoreFromFull(restoreOpts, snapshotDir)
		require.NoError(t, err, "Restore should succeed even when the index directory is missing")

		// 3. Verification
		assert.NoFileExists(t, filepath.Join(targetDataDir, core.IndexSSTDirName))
	})
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
	provider.On("GetMemtablesForFlush").Return(nil, nil)
	provider.On("GetDeletedSeries").Return(nil)
	provider.On("GetRangeTombstones").Return(nil)
	provider.On("GetSequenceNumber").Return(uint64(0)).Once()
	provider.tagIndexManager.On("CreateSnapshot", mock.Anything).Return(nil)
	provider.stringStore.On("GetLogFilePath").Return("")
	provider.seriesIDStore.On("GetLogFilePath").Return("")
	provider.wal.On("ActiveSegmentIndex").Return(uint64(0))
	provider.wal.On("Path").Return(provider.walDir)

	// 2. Simulate the error condition
	expectedErr := fmt.Errorf("simulated write CURRENT error")
	helper.InterceptWriteFile = func(name string, data []byte, perm os.FileMode) error {
		if strings.HasSuffix(name, core.CurrentFileName) {
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
	require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "index", core.IndexManifestFileName), []byte("{}"), 0644))
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
	assert.FileExists(t, filepath.Join(targetDataDir, "index", "index1.sst"))
	assert.FileExists(t, filepath.Join(targetDataDir, "index", core.IndexManifestFileName))
	assert.FileExists(t, filepath.Join(targetDataDir, "deleted_series.json"))
	assert.FileExists(t, filepath.Join(targetDataDir, "string_mapping.log"))
	assert.FileExists(t, filepath.Join(targetDataDir, "series_mapping.log"))
	assert.FileExists(t, filepath.Join(targetDataDir, "CURRENT"))

	currentBytes, err := os.ReadFile(filepath.Join(targetDataDir, "CURRENT"))
	require.NoError(t, err)
	assert.FileExists(t, filepath.Join(targetDataDir, string(currentBytes)), "Consolidated manifest file pointed to by CURRENT should exist")
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
	f, err := os.Create(manifestPath) //nolint:staticcheck
	require.NoError(t, err)           //nolint:staticcheck
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
	info, statErr := os.Stat(targetDataDir)
	require.NoError(t, statErr)
	assert.True(t, info.IsDir())
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
		assert.Contains(t, err.Error(), "failed to read manifest from")
		assert.ErrorIs(t, err, os.ErrNotExist)
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
		f, err := os.Create(manifestPath)
		require.NoError(t, err)
		require.NoError(t, WriteManifestBinary(f, &core.SnapshotManifest{}))
		f.Close()
		defer os.Remove(manifestPath)

		// Simulate the error condition
		expectedErr := fmt.Errorf("simulated mkdir temp error")
		helper.InterceptMkdirTemp = func(dir, pattern string) (string, error) { //nolint:unparam
			return "", expectedErr
		}

		// Execution & Verification
		err2 := RestoreFromFull(restoreOpts, snapshotDir)
		require.Error(t, err2, "RestoreFromFull should fail when os.MkdirTemp fails")
		assert.ErrorIs(t, err2, expectedErr, "The returned error should wrap the one from our mock")
		assert.Contains(t, err2.Error(), "failed to create temporary restore directory")
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
		helper.InterceptOpen = func(name string) (sys.FileInterface, error) {
			if name == manifestPath {
				return nil, expectedErr
			}
			return helper.helperSnapshot.Open(name)
		}

		err := RestoreFromFull(restoreOpts, snapshotDir)
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Contains(t, err.Error(), "failed to open manifest file")
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
		assert.Contains(t, err.Error(), "failed to read manifest from")
	})

	// This test simulates an error when creating the directory structure inside the
	// temporary restore directory, which happens just before a file is copied.
	// It was previously named "PreCreateSubdirError" but the logic has changed
	// from pre-creating all directories to creating them just-in-time.
	t.Run("DirectoryCreationForRestoreError", func(t *testing.T) {
		defer func() { helper.InterceptMkdirAll = nil }()

		currentFilePath := filepath.Join(snapshotDir, "CURRENT")
		manifestFileName := "MANIFEST_mkdir_err.bin"
		require.NoError(t, os.WriteFile(currentFilePath, []byte(manifestFileName), 0644))
		defer os.Remove(currentFilePath)
		manifestPath := filepath.Join(snapshotDir, manifestFileName)
		f, err := os.Create(manifestPath)
		require.NoError(t, err)
		// Add a file in a subdirectory to the manifest to trigger MkdirAll inside CopyFile
		manifestWithFile := &core.SnapshotManifest{
			Levels: []core.SnapshotLevelManifest{
				{LevelNumber: 0, Tables: []core.SSTableMetadata{{ID: 1, FileName: "sst/1.sst"}}},
			},
		}
		require.NoError(t, WriteManifestBinary(f, manifestWithFile))
		f.Close()
		defer os.Remove(manifestPath)

		// Create the source file that the manifest points to
		require.NoError(t, os.MkdirAll(filepath.Join(snapshotDir, "sst"), 0755))
		require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "sst", "1.sst"), []byte("data"), 0644))

		expectedErr := fmt.Errorf("simulated mkdirall error")
		helper.InterceptMkdirAll = func(path string, perm os.FileMode) error {
			// Fail when creating the 'sst' directory inside the temp restore dir
			if strings.Contains(path, ".restore-tmp-") && strings.HasSuffix(path, "sst") {
				return expectedErr
			}
			return helper.helperSnapshot.MkdirAll(path, perm)
		}

		err = RestoreFromFull(restoreOpts, snapshotDir)
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Contains(t, err.Error(), "failed to create destination directory")
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
		assert.Contains(t, err.Error(), "failed to copy chained file")
	})

	t.Run("RemoveOriginalDirError", func(t *testing.T) {
		defer func() { helper.InterceptRemoveAll = nil }()

		manifestFileName := "MANIFEST_remove_err.bin"
		require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "CURRENT"), []byte(manifestFileName), 0644))
		manifestPath := filepath.Join(snapshotDir, manifestFileName)
		f, err := os.Create(manifestPath)
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
	f, err := os.Create(manifestPath) //nolint:staticcheck
	require.NoError(t, err)           //nolint:staticcheck
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
	require.NotEmpty(t, tempRestoreDir, "Temporary restore directory path should have been captured") //nolint:staticcheck
	_, statErr := os.Stat(tempRestoreDir)
	assert.True(t, os.IsNotExist(statErr), "Temporary restore directory should be cleaned up on failure")
}

func TestRestorer_ProcessErrors(t *testing.T) {
	// This test focuses on error paths within the restorer's methods,
	// assuming initial validation and temp dir creation succeed.

	t.Run("CopyFileError", func(t *testing.T) {
		// 1. Setup
		r, _, _, helper := setupRestorerTest(t)
		expectedErr := fmt.Errorf("simulated copy file error")

		// 2. Inject error
		helper.InterceptCopyFile = func(src, dst string) error {
			// Fail on any file copy attempt.
			return expectedErr
		}

		// 3. Execute and Verify
		err := r.run()
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Contains(t, err.Error(), "failed to copy chained file")
		assert.NoFileExists(t, r.opts.DataDir, "Target data directory should be cleaned up on failure")
	})

	t.Run("ParentSnapshotNotFoundInChain", func(t *testing.T) {
		// 1. Setup
		r, _, _, helper := setupRestorerTest(t)
		// Create a corrupted manifest that is incremental and points to a non-existent parent.
		corruptedManifest := &core.SnapshotManifest{
			Type:     core.SnapshotTypeIncremental,
			ParentID: "non_existent_parent_123",
		}

		// Rewrite the manifest in the snapshot dir
		manifestFileName, err := writeTestManifest(r.snapshotDir, corruptedManifest)
		require.NoError(t, err)
		require.NoError(t, helper.WriteFile(filepath.Join(r.snapshotDir, "CURRENT"), []byte(manifestFileName), 0644))

		// 3. Execute and Verify
		err = r.run()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parent snapshot non_existent_parent_123 not found")
		assert.NoFileExists(t, r.opts.DataDir, "Target data directory should be cleaned up on failure")
	})

	t.Run("Swap_RemoveOriginalError", func(t *testing.T) {
		// 1. Setup
		r, _, targetDataDir, helper := setupRestorerTest(t)
		expectedErr := fmt.Errorf("simulated remove original error")

		// Create an "original" data directory that the process will try to remove.
		require.NoError(t, os.MkdirAll(targetDataDir, 0755))
		require.NoError(t, os.WriteFile(filepath.Join(targetDataDir, "existing.file"), []byte("data"), 0644))

		// 2. Inject error
		helper.InterceptRemoveAll = func(path string) error {
			if path == targetDataDir {
				return expectedErr
			}
			return helper.helperSnapshot.RemoveAll(path)
		}

		// 3. Execute and Verify
		err := r.run()
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Contains(t, err.Error(), "failed to remove original data directory")

		// In this specific failure case, the temp directory IS cleaned up by the defer in run(),
		// but the original directory is left untouched. This is the expected behavior.
		// The cleanup of the temp dir happens because the error occurs before the successful rename.
		assert.NoFileExists(t, r.tempRestoreDir, "Temp directory should be cleaned up on swap failure")
		assert.DirExists(t, targetDataDir, "Original directory should still exist on swap failure")
	})

	t.Run("Swap_StatOriginalError", func(t *testing.T) {
		// 1. Setup
		r, _, targetDataDir, helper := setupRestorerTest(t)
		expectedErr := fmt.Errorf("simulated stat error")

		// 2. Inject error
		helper.InterceptStat = func(name string) (os.FileInfo, error) {
			if name == targetDataDir {
				return nil, expectedErr
			}
			return helper.helperSnapshot.Stat(name)
		}

		// 3. Execute and Verify
		err := r.run()
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Contains(t, err.Error(), "failed to stat original data directory")
		assert.NoFileExists(t, r.opts.DataDir, "Target data directory should be cleaned up on failure")
	})
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
	provider.On("GetDeletedSeries").Return(nil)
	provider.On("GetRangeTombstones").Return(nil)
	provider.On("GetSequenceNumber").Return(uint64(0)).Once()
	provider.tagIndexManager.On("CreateSnapshot", mock.Anything).Return(nil)
	provider.wal.On("ActiveSegmentIndex").Return(uint64(0))
	provider.wal.On("Path").Return(provider.walDir)

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

func TestRestoreFromLatest(t *testing.T) {
	// Mock restoreFromFullFunc for testing purposes
	originalRestoreFromFull := restoreFromFullFunc
	defer func() { restoreFromFullFunc = originalRestoreFromFull }()

	t.Run("Success", func(t *testing.T) {
		// 1. Setup
		tempDir := t.TempDir()
		snapshotsBaseDir := filepath.Join(tempDir, "snapshots")
		require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))

		// Create multiple snapshot directories to ensure the latest is chosen
		snapshotDir1 := filepath.Join(snapshotsBaseDir, "1000")
		snapshotDir2 := filepath.Join(snapshotsBaseDir, "2000") // This one is later
		require.NoError(t, os.MkdirAll(snapshotDir1, 0755))
		require.NoError(t, os.MkdirAll(snapshotDir2, 0755))

		var calledSnapshotDir string
		var calledOpts RestoreOptions
		restoreFromFullFunc = func(opts RestoreOptions, snapshotDir string) error {
			calledOpts = opts
			calledSnapshotDir = snapshotDir
			return nil
		}

		// 2. Execution
		opts := RestoreOptions{DataDir: filepath.Join(tempDir, "target")}
		err := RestoreFromLatest(opts, snapshotsBaseDir)

		// 3. Verification
		require.NoError(t, err)
		assert.Equal(t, snapshotDir2, calledSnapshotDir, "Should have called RestoreFromFull with the latest snapshot directory")
		assert.Equal(t, opts.DataDir, calledOpts.DataDir, "Options should be passed through correctly")
	})

	t.Run("NoSnapshotsFound", func(t *testing.T) {
		// 1. Setup
		tempDir := t.TempDir()
		snapshotsBaseDir := filepath.Join(tempDir, "snapshots")
		require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755)) // Directory exists but is empty

		// 2. Execution
		opts := RestoreOptions{DataDir: filepath.Join(tempDir, "target")}
		err := RestoreFromLatest(opts, snapshotsBaseDir)

		// 3. Verification
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no snapshots found")
	})

	t.Run("FindLatestFails", func(t *testing.T) {
		// 1. Setup
		tempDir := t.TempDir()
		snapshotsBaseDir := filepath.Join(tempDir, "snapshots")

		helper := &mockSnapshotHelper{helperSnapshot: newHelperSnapshot()}
		expectedErr := fmt.Errorf("simulated readdir error")
		helper.InterceptReadDir = func(name string) ([]os.DirEntry, error) {
			return nil, expectedErr
		}

		// 2. Execution
		opts := RestoreOptions{DataDir: filepath.Join(tempDir, "target"), wrapper: helper}
		err := RestoreFromLatest(opts, snapshotsBaseDir)

		// 3. Verification
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Contains(t, err.Error(), "failed to find the latest snapshot")
	})

	t.Run("RestoreFromFullFails", func(t *testing.T) {
		// 1. Setup
		tempDir := t.TempDir()
		snapshotsBaseDir := filepath.Join(tempDir, "snapshots")
		require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))
		require.NoError(t, os.MkdirAll(filepath.Join(snapshotsBaseDir, "1000"), 0755))

		expectedErr := fmt.Errorf("simulated restore error")
		restoreFromFullFunc = func(opts RestoreOptions, snapshotDir string) error {
			return expectedErr
		}

		// 2. Execution
		opts := RestoreOptions{DataDir: filepath.Join(tempDir, "target")}
		err := RestoreFromLatest(opts, snapshotsBaseDir)

		// 3. Verification
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr, "Error from RestoreFromFull should be propagated")
	})
}

// calculateDirSize is a test helper to get the deterministic size of a directory.
func calculateDirSize(dir string) (int64, error) {
	var totalSize int64
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			info, statErr := d.Info()
			if statErr != nil {
				return statErr
			}
			totalSize += info.Size()
		}
		return nil
	})
	return totalSize, err
}

func TestManager_ListSnapshots(t *testing.T) {
	t.Run("SuccessWithFullAndIncremental", func(t *testing.T) {
		// 1. Setup
		tempDir := t.TempDir()
		snapshotsBaseDir := filepath.Join(tempDir, "snapshots")
		require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))

		provider := newMockEngineProvider(t, tempDir)
		manager := NewManager(provider)

		// Create a full snapshot
		time1 := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
		fullSnapshotID := fmt.Sprintf("%d", time1.UnixNano())
		fullSnapshotDir := filepath.Join(snapshotsBaseDir, fullSnapshotID)
		require.NoError(t, os.MkdirAll(fullSnapshotDir, 0755))
		fullManifest := &core.SnapshotManifest{Type: core.SnapshotTypeFull, CreatedAt: time1}
		manifestFileName1, err := writeTestManifest(fullSnapshotDir, fullManifest)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(fullSnapshotDir, "CURRENT"), []byte(manifestFileName1), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(fullSnapshotDir, "dummy.dat"), make([]byte, 1024), 0644)) // Add size
		fullSize, err := calculateDirSize(fullSnapshotDir)
		require.NoError(t, err)

		// Create an incremental snapshot
		time2 := time.Now().Add(-1 * time.Hour).Truncate(time.Second)
		incrSnapshotID := fmt.Sprintf("%d_incr", time2.UnixNano())
		incrSnapshotDir := filepath.Join(snapshotsBaseDir, incrSnapshotID)
		require.NoError(t, os.MkdirAll(incrSnapshotDir, 0755))
		incrManifest := &core.SnapshotManifest{Type: core.SnapshotTypeIncremental, ParentID: fullSnapshotID, CreatedAt: time2}
		manifestFileName2, err := writeTestManifest(incrSnapshotDir, incrManifest)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(incrSnapshotDir, "CURRENT"), []byte(manifestFileName2), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(incrSnapshotDir, "dummy2.dat"), make([]byte, 512), 0644)) // Add size
		incrSize, err := calculateDirSize(incrSnapshotDir)
		require.NoError(t, err)

		// 2. Execution
		infos, err := manager.ListSnapshots(snapshotsBaseDir)
		require.NoError(t, err)

		// 3. Verification
		require.Len(t, infos, 2)

		// Results should be sorted by time
		assert.Equal(t, fullSnapshotID, infos[0].ID)
		assert.Equal(t, core.SnapshotTypeFull, infos[0].Type)
		assert.True(t, time1.Equal(infos[0].CreatedAt))
		assert.Equal(t, fullSize, infos[0].Size)
		assert.Equal(t, fullSize, infos[0].TotalChainSize, "Chain size for a full snapshot should be its own size")
		assert.Empty(t, infos[0].ParentID)

		assert.Equal(t, incrSnapshotID, infos[1].ID)
		assert.Equal(t, core.SnapshotTypeIncremental, infos[1].Type)
		assert.True(t, time2.Equal(infos[1].CreatedAt))
		assert.Equal(t, fullSnapshotID, infos[1].ParentID)
		assert.Equal(t, incrSize, infos[1].Size)
		assert.Equal(t, fullSize+incrSize, infos[1].TotalChainSize, "Chain size for an incremental snapshot should be its size plus its parent's")
	})

	t.Run("EmptyAndNonExistentDirectory", func(t *testing.T) {
		tempDir := t.TempDir()
		manager := NewManager(newMockEngineProvider(t, tempDir))

		// Non-existent
		infos, err := manager.ListSnapshots(filepath.Join(tempDir, "nonexistent"))
		require.NoError(t, err)
		assert.Empty(t, infos)

		// Empty
		emptyDir := filepath.Join(tempDir, "empty")
		require.NoError(t, os.Mkdir(emptyDir, 0755))
		infos, err = manager.ListSnapshots(emptyDir)
		require.NoError(t, err)
		assert.Empty(t, infos)
	})

	t.Run("DirectoryWithInvalidEntries", func(t *testing.T) {
		tempDir := t.TempDir()
		snapshotsBaseDir := filepath.Join(tempDir, "snapshots")
		require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))
		manager := NewManager(newMockEngineProvider(t, tempDir))

		// A valid snapshot
		validSnapshotDir := filepath.Join(snapshotsBaseDir, "valid_snapshot")
		require.NoError(t, os.MkdirAll(validSnapshotDir, 0755))
		manifestFileName, err := writeTestManifest(validSnapshotDir, &core.SnapshotManifest{Type: core.SnapshotTypeFull, CreatedAt: time.Now()})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(validSnapshotDir, "CURRENT"), []byte(manifestFileName), 0644))

		// An invalid entry (a file)
		require.NoError(t, os.WriteFile(filepath.Join(snapshotsBaseDir, "not_a_snapshot.txt"), []byte("ignore me"), 0644))
		// An invalid entry (a directory without a manifest)
		require.NoError(t, os.MkdirAll(filepath.Join(snapshotsBaseDir, "empty_dir"), 0755))

		infos, err := manager.ListSnapshots(snapshotsBaseDir)
		require.NoError(t, err)
		require.Len(t, infos, 1, "Should only list the one valid snapshot")
		assert.Equal(t, "valid_snapshot", infos[0].ID)
	})

	t.Run("WithBrokenChain", func(t *testing.T) {
		// 1. Setup
		tempDir := t.TempDir()
		snapshotsBaseDir := filepath.Join(tempDir, "snapshots")
		require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))
		manager := NewManager(newMockEngineProvider(t, tempDir))

		// Create full
		time1 := time.Now().Add(-3 * time.Hour)
		fullID := fmt.Sprintf("%d_full", time1.UnixNano())
		fullDir := filepath.Join(snapshotsBaseDir, fullID)
		require.NoError(t, os.MkdirAll(fullDir, 0755))
		manifestFileName1, err := writeTestManifest(fullDir, &core.SnapshotManifest{Type: core.SnapshotTypeFull, CreatedAt: time1})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(fullDir, "CURRENT"), []byte(manifestFileName1), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(fullDir, "dummy.dat"), make([]byte, 100), 0644))
		fullSize, _ := calculateDirSize(fullDir)

		// Create incr1
		time2 := time.Now().Add(-2 * time.Hour)
		incr1ID := fmt.Sprintf("%d_incr1", time2.UnixNano())
		incr1Dir := filepath.Join(snapshotsBaseDir, incr1ID)
		require.NoError(t, os.MkdirAll(incr1Dir, 0755))
		manifestFileName2, err := writeTestManifest(incr1Dir, &core.SnapshotManifest{Type: core.SnapshotTypeIncremental, ParentID: fullID, CreatedAt: time2})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(incr1Dir, "CURRENT"), []byte(manifestFileName2), 0644))

		// Create incr2
		time3 := time.Now().Add(-1 * time.Hour)
		incr2ID := fmt.Sprintf("%d_incr2", time3.UnixNano())
		incr2Dir := filepath.Join(snapshotsBaseDir, incr2ID)
		require.NoError(t, os.MkdirAll(incr2Dir, 0755))
		manifestFileName3, err := writeTestManifest(incr2Dir, &core.SnapshotManifest{Type: core.SnapshotTypeIncremental, ParentID: incr1ID, CreatedAt: time3})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(incr2Dir, "CURRENT"), []byte(manifestFileName3), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(incr2Dir, "dummy.dat"), make([]byte, 200), 0644))
		incr2Size, _ := calculateDirSize(incr2Dir)

		// Break the chain
		require.NoError(t, os.RemoveAll(incr1Dir))

		// 2. Execution
		infos, err := manager.ListSnapshots(snapshotsBaseDir)
		require.NoError(t, err)

		// 3. Verification
		require.Len(t, infos, 2) // full and incr2 remain
		// The order is not guaranteed after filtering, so we find them.
		var fullInfo, incr2Info Info
		for _, info := range infos {
			if info.ID == fullID {
				fullInfo = info
			} else if info.ID == incr2ID {
				incr2Info = info
			}
		}
		assert.Equal(t, fullSize, fullInfo.TotalChainSize)
		assert.Equal(t, incr2Size, incr2Info.TotalChainSize, "Chain size for a snapshot with a missing parent should be its own size")
	})

	t.Run("ReadDirError", func(t *testing.T) {
		tempDir := t.TempDir()
		snapshotsBaseDir := filepath.Join(tempDir, "snapshots")
		provider := newMockEngineProvider(t, tempDir)
		helper := &mockSnapshotHelper{helperSnapshot: newHelperSnapshot()}
		manager := NewManagerWithTesting(provider, helper)

		expectedErr := fmt.Errorf("simulated readdir error")
		helper.InterceptReadDir = func(name string) ([]os.DirEntry, error) {
			return nil, expectedErr
		}

		_, err := manager.ListSnapshots(snapshotsBaseDir)
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Contains(t, err.Error(), "failed to read snapshots base directory")
	})
}

func TestManager_RestoreFrom(t *testing.T) {
	// 1. Setup
	tempDir := t.TempDir()
	dataDir := filepath.Join(tempDir, "data")
	snapshotPath := filepath.Join(tempDir, "snapshot")
	provider := newMockEngineProvider(t, dataDir)
	manager := NewManager(provider).(*manager)

	// 2. Mock the restoreFromFullFunc
	var calledOpts RestoreOptions
	var calledPath string
	expectedErr := fmt.Errorf("simulated restore error")

	originalRestoreFunc := restoreFromFullFunc
	defer func() { restoreFromFullFunc = originalRestoreFunc }() // Restore original function after test

	restoreFromFullFunc = func(opts RestoreOptions, path string) error {
		calledOpts = opts
		calledPath = path
		return expectedErr // Return an error to test error propagation
	}

	// 3. Mock provider methods that RestoreFrom will call
	provider.On("GetDataDir").Return(dataDir)

	// 4. Execution
	err := manager.RestoreFrom(context.Background(), snapshotPath)

	// 5. Verification
	require.Error(t, err, "Expected RestoreFrom to return the error from the underlying function")
	assert.ErrorIs(t, err, expectedErr)
	provider.AssertExpectations(t)
	assert.Equal(t, dataDir, calledOpts.DataDir, "DataDir in RestoreOptions is incorrect")
	assert.Equal(t, snapshotPath, calledPath, "Snapshot path passed to restore function is incorrect")
}

func TestFindLatestSnapshot(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		// 1. Setup
		tempDir := t.TempDir()
		snapshotsBaseDir := filepath.Join(tempDir, "snapshots")
		require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))

		// Create snapshot directories. The names should be sortable.
		require.NoError(t, os.Mkdir(filepath.Join(snapshotsBaseDir, "1000"), 0755))
		require.NoError(t, os.Mkdir(filepath.Join(snapshotsBaseDir, "3000"), 0755)) // This is the latest
		require.NoError(t, os.Mkdir(filepath.Join(snapshotsBaseDir, "2000"), 0755))

		// 2. Execution
		helper := newHelperSnapshot()
		latestID, latestPath, err := findLatestSnapshot(snapshotsBaseDir, helper)

		// 3. Verification
		require.NoError(t, err)
		assert.Equal(t, "3000", latestID)
		assert.Equal(t, filepath.Join(snapshotsBaseDir, "3000"), latestPath)
	})

	t.Run("NoSnapshotsInDirectory", func(t *testing.T) {
		// 1. Setup
		tempDir := t.TempDir()
		snapshotsBaseDir := filepath.Join(tempDir, "snapshots")
		require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))

		// 2. Execution
		helper := newHelperSnapshot()
		latestID, latestPath, err := findLatestSnapshot(snapshotsBaseDir, helper)

		// 3. Verification
		require.NoError(t, err)
		assert.Empty(t, latestID)
		assert.Empty(t, latestPath)
	})

	t.Run("DirectoryDoesNotExist", func(t *testing.T) {
		// 1. Setup
		tempDir := t.TempDir()
		snapshotsBaseDir := filepath.Join(tempDir, "nonexistent")

		// 2. Execution
		helper := newHelperSnapshot()
		latestID, latestPath, err := findLatestSnapshot(snapshotsBaseDir, helper)

		// 3. Verification
		require.NoError(t, err)
		assert.Empty(t, latestID)
		assert.Empty(t, latestPath)
	})

	t.Run("ReadDirError", func(t *testing.T) {
		// 1. Setup
		tempDir := t.TempDir()
		snapshotsBaseDir := filepath.Join(tempDir, "snapshots")
		helper := &mockSnapshotHelper{helperSnapshot: newHelperSnapshot()}
		expectedErr := fmt.Errorf("simulated readdir error")
		helper.InterceptReadDir = func(name string) ([]os.DirEntry, error) {
			return nil, expectedErr
		}

		// 2. Execution
		_, _, err := findLatestSnapshot(snapshotsBaseDir, helper)

		// 3. Verification
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Contains(t, err.Error(), "failed to read snapshots directory")
	})

	t.Run("IgnoresFiles", func(t *testing.T) {
		// 1. Setup
		tempDir := t.TempDir()
		snapshotsBaseDir := filepath.Join(tempDir, "snapshots")
		require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))

		// Create snapshot directories and a file
		require.NoError(t, os.Mkdir(filepath.Join(snapshotsBaseDir, "1000"), 0755))
		require.NoError(t, os.Mkdir(filepath.Join(snapshotsBaseDir, "2000"), 0755)) // This is the latest directory
		require.NoError(t, os.WriteFile(filepath.Join(snapshotsBaseDir, "3000.txt"), []byte("i am a file"), 0644))

		// 2. Execution
		helper := newHelperSnapshot()
		latestID, latestPath, err := findLatestSnapshot(snapshotsBaseDir, helper)

		// 3. Verification
		require.NoError(t, err)
		assert.Equal(t, "2000", latestID)
		assert.Equal(t, filepath.Join(snapshotsBaseDir, "2000"), latestPath)
	})
}

func TestManager_collectAllSSTablesInChain(t *testing.T) {
	// Helper to create a snapshot directory with a manifest
	createTestSnapshotDir := func(t *testing.T, baseDir, id string, manifest *core.SnapshotManifest) {
		t.Helper()
		snapshotDir := filepath.Join(baseDir, id)
		require.NoError(t, os.MkdirAll(snapshotDir, 0755))
		manifestFileName, err := writeTestManifest(snapshotDir, manifest)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "CURRENT"), []byte(manifestFileName), 0644))
	}

	t.Run("Success_Full_Plus_Two_Incrementals", func(t *testing.T) {
		// 1. Setup
		tempDir := t.TempDir()
		snapshotsBaseDir := filepath.Join(tempDir, "snapshots")
		require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))

		provider := newMockEngineProvider(t, tempDir)
		manager := NewManager(provider).(*manager)

		// Create a chain: full -> incr1 -> incr2
		fullID := "1_full"
		incr1ID := "2_incr"
		incr2ID := "3_incr"

		fullManifest := &core.SnapshotManifest{
			Type:   core.SnapshotTypeFull,
			Levels: []core.SnapshotLevelManifest{{Tables: []core.SSTableMetadata{{FileName: "sst/1.sst"}}}},
		}
		createTestSnapshotDir(t, snapshotsBaseDir, fullID, fullManifest)

		incr1Manifest := &core.SnapshotManifest{
			Type:     core.SnapshotTypeIncremental,
			ParentID: fullID,
			Levels:   []core.SnapshotLevelManifest{{Tables: []core.SSTableMetadata{{FileName: "sst/2.sst"}}}},
		}
		createTestSnapshotDir(t, snapshotsBaseDir, incr1ID, incr1Manifest)

		incr2Manifest := &core.SnapshotManifest{
			Type:     core.SnapshotTypeIncremental,
			ParentID: incr1ID,
			Levels:   []core.SnapshotLevelManifest{{Tables: []core.SSTableMetadata{{FileName: "sst/3.sst"}}}},
		}
		createTestSnapshotDir(t, snapshotsBaseDir, incr2ID, incr2Manifest)

		// 2. Execution
		startPath := filepath.Join(snapshotsBaseDir, incr2ID)
		collectedTables, err := manager.collectAllSSTablesInChain(snapshotsBaseDir, startPath)

		// 3. Verification
		require.NoError(t, err)
		expected := map[string]struct{}{
			"sst/1.sst": {},
			"sst/2.sst": {},
			"sst/3.sst": {},
		}
		assert.Equal(t, expected, collectedTables)
	})

	t.Run("Error_ParentNotFound", func(t *testing.T) {
		// 1. Setup
		tempDir := t.TempDir()
		snapshotsBaseDir := filepath.Join(tempDir, "snapshots")
		require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))

		provider := newMockEngineProvider(t, tempDir)
		manager := NewManager(provider).(*manager)

		// Create an incremental snapshot with a non-existent parent
		incrID := "1_incr"
		incrManifest := &core.SnapshotManifest{
			Type:     core.SnapshotTypeIncremental,
			ParentID: "non_existent_parent",
			Levels:   []core.SnapshotLevelManifest{{Tables: []core.SSTableMetadata{{FileName: "sst/1.sst"}}}},
		}
		createTestSnapshotDir(t, snapshotsBaseDir, incrID, incrManifest)

		// 2. Execution
		startPath := filepath.Join(snapshotsBaseDir, incrID)
		_, err := manager.collectAllSSTablesInChain(snapshotsBaseDir, startPath)

		// 3. Verification
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parent snapshot non_existent_parent not found")
	})
}

func TestManager_Prune(t *testing.T) {
	// Helper to create a snapshot directory for pruning tests
	createPruneTestSnapshot := func(t *testing.T, baseDir, id string, snapType core.SnapshotType, parentID string, createdAt time.Time) {
		t.Helper()
		snapshotDir := filepath.Join(baseDir, id)
		require.NoError(t, os.MkdirAll(snapshotDir, 0755))
		manifest := &core.SnapshotManifest{
			Type:      snapType,
			ParentID:  parentID,
			CreatedAt: createdAt,
		}
		manifestFileName, err := writeTestManifest(snapshotDir, manifest)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "CURRENT"), []byte(manifestFileName), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "dummy.dat"), make([]byte, 10), 0644))
	}

	setup := func(t *testing.T) (context.Context, *manager, string) {
		t.Helper()
		ctx := context.Background()
		tempDir := t.TempDir()
		snapshotsBaseDir := filepath.Join(tempDir, "snapshots")
		require.NoError(t, os.MkdirAll(snapshotsBaseDir, 0755))
		provider := newMockEngineProvider(t, tempDir)
		manager := NewManager(provider).(*manager)
		return ctx, manager, snapshotsBaseDir
	}

	t.Run("Success_KeepN_Only", func(t *testing.T) {
		// 1. Setup
		ctx, manager, snapshotsBaseDir := setup(t)

		// Chain 1 (oldest)
		createPruneTestSnapshot(t, snapshotsBaseDir, "full_1", core.SnapshotTypeFull, "", time.Now().Add(-3*time.Hour))
		createPruneTestSnapshot(t, snapshotsBaseDir, "incr_1.1", core.SnapshotTypeIncremental, "full_1", time.Now().Add(-2*time.Hour))
		// Chain 2 (middle)
		createPruneTestSnapshot(t, snapshotsBaseDir, "full_2", core.SnapshotTypeFull, "", time.Now().Add(-1*time.Hour))
		// Chain 3 (newest)
		createPruneTestSnapshot(t, snapshotsBaseDir, "full_3", core.SnapshotTypeFull, "", time.Now())
		createPruneTestSnapshot(t, snapshotsBaseDir, "incr_3.1", core.SnapshotTypeIncremental, "full_3", time.Now().Add(1*time.Minute))
		createPruneTestSnapshot(t, snapshotsBaseDir, "incr_3.2", core.SnapshotTypeIncremental, "incr_3.1", time.Now().Add(2*time.Minute))

		// 2. Execution
		// Keep the 2 newest full chains (full_2 and full_3)
		deletedIDs, err := manager.Prune(ctx, snapshotsBaseDir, PruneOptions{KeepN: 2})

		// 3. Verification
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"full_1", "incr_1.1"}, deletedIDs)

		// Check remaining files
		assert.NoDirExists(t, filepath.Join(snapshotsBaseDir, "full_1"))
		assert.NoDirExists(t, filepath.Join(snapshotsBaseDir, "incr_1.1"))
		assert.DirExists(t, filepath.Join(snapshotsBaseDir, "full_2"))
		assert.DirExists(t, filepath.Join(snapshotsBaseDir, "full_3"))
		assert.DirExists(t, filepath.Join(snapshotsBaseDir, "incr_3.1"))
		assert.DirExists(t, filepath.Join(snapshotsBaseDir, "incr_3.2"))
	})

	t.Run("KeepN_MoreThanAvailable", func(t *testing.T) {
		// 1. Setup
		ctx, manager, snapshotsBaseDir := setup(t)
		createPruneTestSnapshot(t, snapshotsBaseDir, "full_1", core.SnapshotTypeFull, "", time.Now())

		// 2. Execution
		deletedIDs, err := manager.Prune(ctx, snapshotsBaseDir, PruneOptions{KeepN: 1})

		// 3. Verification
		require.NoError(t, err)
		assert.Empty(t, deletedIDs)
		assert.DirExists(t, filepath.Join(snapshotsBaseDir, "full_1"))
	})

	t.Run("PruneOlderThan_Only", func(t *testing.T) {
		// 1. Setup
		ctx, manager, snapshotsBaseDir := setup(t)
		// Chain 1 (old, should be pruned)
		createPruneTestSnapshot(t, snapshotsBaseDir, "full_old", core.SnapshotTypeFull, "", time.Now().Add(-48*time.Hour))
		// Chain 2 (new, should be kept)
		createPruneTestSnapshot(t, snapshotsBaseDir, "full_new", core.SnapshotTypeFull, "", time.Now().Add(-1*time.Hour))
		createPruneTestSnapshot(t, snapshotsBaseDir, "incr_new", core.SnapshotTypeIncremental, "full_new", time.Now())

		// 2. Execution
		// Prune chains whose newest snapshot is older than 24 hours.
		deletedIDs, err := manager.Prune(ctx, snapshotsBaseDir, PruneOptions{PruneOlderThan: 24 * time.Hour})

		// 3. Verification
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"full_old"}, deletedIDs)
		assert.NoDirExists(t, filepath.Join(snapshotsBaseDir, "full_old"))
		assert.DirExists(t, filepath.Join(snapshotsBaseDir, "full_new"))
		assert.DirExists(t, filepath.Join(snapshotsBaseDir, "incr_new"))
	})

	t.Run("KeepN_And_PruneOlderThan_Combined", func(t *testing.T) {
		// 1. Setup
		ctx, manager, snapshotsBaseDir := setup(t)
		// Chain 1 (very old, should be pruned)
		createPruneTestSnapshot(t, snapshotsBaseDir, "full_1", core.SnapshotTypeFull, "", time.Now().Add(-72*time.Hour))
		// Chain 2 (old, but should be kept by KeepN)
		createPruneTestSnapshot(t, snapshotsBaseDir, "full_2", core.SnapshotTypeFull, "", time.Now().Add(-48*time.Hour))
		// Chain 3 (new, should be kept)
		createPruneTestSnapshot(t, snapshotsBaseDir, "full_3", core.SnapshotTypeFull, "", time.Now().Add(-1*time.Hour))

		// 2. Execution
		// Prune chains older than 24h, but always keep the 2 newest.
		deletedIDs, err := manager.Prune(ctx, snapshotsBaseDir, PruneOptions{KeepN: 2, PruneOlderThan: 24 * time.Hour})

		// 3. Verification
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"full_1"}, deletedIDs) // Only full_1 is old enough AND not protected by KeepN
		assert.NoDirExists(t, filepath.Join(snapshotsBaseDir, "full_1"))
		assert.DirExists(t, filepath.Join(snapshotsBaseDir, "full_2")) // Kept by KeepN
		assert.DirExists(t, filepath.Join(snapshotsBaseDir, "full_3")) // Kept by KeepN and age
	})

	t.Run("No_Options_Set", func(t *testing.T) {
		// 1. Setup
		ctx, manager, snapshotsBaseDir := setup(t)
		createPruneTestSnapshot(t, snapshotsBaseDir, "full_1", core.SnapshotTypeFull, "", time.Now())

		// 2. Execution
		deletedIDs, err := manager.Prune(ctx, snapshotsBaseDir, PruneOptions{})

		// 3. Verification
		require.NoError(t, err)
		assert.Empty(t, deletedIDs, "Should not prune anything if no policies are set")
		assert.DirExists(t, filepath.Join(snapshotsBaseDir, "full_1"))
	})

	t.Run("PruneBroken_Only", func(t *testing.T) {
		// 1. Setup
		ctx, manager, snapshotsBaseDir := setup(t)

		// A valid chain
		createPruneTestSnapshot(t, snapshotsBaseDir, "full_1", core.SnapshotTypeFull, "", time.Now().Add(-2*time.Hour))
		createPruneTestSnapshot(t, snapshotsBaseDir, "incr_1.1", core.SnapshotTypeIncremental, "full_1", time.Now().Add(-1*time.Hour))

		// A broken chain (parent does not exist)
		createPruneTestSnapshot(t, snapshotsBaseDir, "orphan_1", core.SnapshotTypeIncremental, "non_existent_parent", time.Now())

		// Another broken chain (part of a chain whose root is missing)
		createPruneTestSnapshot(t, snapshotsBaseDir, "orphan_2", core.SnapshotTypeIncremental, "missing_full", time.Now())
		createPruneTestSnapshot(t, snapshotsBaseDir, "orphan_3", core.SnapshotTypeIncremental, "orphan_2", time.Now())

		// 2. Execution
		deletedIDs, err := manager.Prune(ctx, snapshotsBaseDir, PruneOptions{PruneBroken: true})

		// 3. Verification
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"orphan_1", "orphan_2", "orphan_3"}, deletedIDs)

		// Check remaining files
		assert.DirExists(t, filepath.Join(snapshotsBaseDir, "full_1"))
		assert.DirExists(t, filepath.Join(snapshotsBaseDir, "incr_1.1"))
		assert.NoDirExists(t, filepath.Join(snapshotsBaseDir, "orphan_1"))
		assert.NoDirExists(t, filepath.Join(snapshotsBaseDir, "orphan_2"))
		assert.NoDirExists(t, filepath.Join(snapshotsBaseDir, "orphan_3"))
	})

	t.Run("RemoveAll_Error", func(t *testing.T) {
		// 1. Setup
		ctx, manager, snapshotsBaseDir := setup(t)
		createPruneTestSnapshot(t, snapshotsBaseDir, "full_1_to_delete", core.SnapshotTypeFull, "", time.Now().Add(-2*time.Hour))
		createPruneTestSnapshot(t, snapshotsBaseDir, "full_2_to_delete", core.SnapshotTypeFull, "", time.Now().Add(-1*time.Hour))
		createPruneTestSnapshot(t, snapshotsBaseDir, "full_3_to_keep", core.SnapshotTypeFull, "", time.Now())

		// Inject error
		expectedErr := fmt.Errorf("simulated remove error")
		helper := &mockSnapshotHelper{helperSnapshot: newHelperSnapshot()}
		helper.InterceptRemoveAll = func(path string) error {
			if strings.HasSuffix(path, "full_1_to_delete") {
				return expectedErr
			}
			return os.RemoveAll(path)
		}
		manager.wrapper = helper

		// 2. Execution
		deletedIDs, err := manager.Prune(ctx, snapshotsBaseDir, PruneOptions{KeepN: 1}) // Prunes full_1 and full_2

		// 3. Verification
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		// It should have successfully deleted the second one.
		assert.ElementsMatch(t, []string{"full_2_to_delete"}, deletedIDs)

		assert.DirExists(t, filepath.Join(snapshotsBaseDir, "full_1_to_delete"))   // The one that failed
		assert.NoDirExists(t, filepath.Join(snapshotsBaseDir, "full_2_to_delete")) // The one that succeeded
		assert.DirExists(t, filepath.Join(snapshotsBaseDir, "full_3_to_keep"))     // The one that should be kept
	})

	t.Run("Complex_KeepN_Age_And_Broken", func(t *testing.T) {
		// 1. Setup
		ctx, manager, snapshotsBaseDir := setup(t)
		now := time.Now()

		// Chain 1 (Newest, Kept by KeepN=2)
		createPruneTestSnapshot(t, snapshotsBaseDir, "full_1", core.SnapshotTypeFull, "", now)
		createPruneTestSnapshot(t, snapshotsBaseDir, "incr_1.1", core.SnapshotTypeIncremental, "full_1", now.Add(1*time.Minute))

		// Chain 2 (Second newest, Kept by KeepN=2)
		createPruneTestSnapshot(t, snapshotsBaseDir, "full_2", core.SnapshotTypeFull, "", now.Add(-1*time.Hour))

		// Chain 3 (Old, but kept by PruneOlderThan=25h because its newest part is newer than 25h ago)
		createPruneTestSnapshot(t, snapshotsBaseDir, "full_3", core.SnapshotTypeFull, "", now.Add(-26*time.Hour))
		createPruneTestSnapshot(t, snapshotsBaseDir, "incr_3.1", core.SnapshotTypeIncremental, "full_3", now.Add(-24*time.Hour))

		// Chain 4 (Old, pruned by PruneOlderThan=25h)
		createPruneTestSnapshot(t, snapshotsBaseDir, "full_4", core.SnapshotTypeFull, "", now.Add(-30*time.Hour))

		// Chain 5 (Old, also pruned by PruneOlderThan=25h)
		createPruneTestSnapshot(t, snapshotsBaseDir, "full_5", core.SnapshotTypeFull, "", now.Add(-48*time.Hour))
		createPruneTestSnapshot(t, snapshotsBaseDir, "incr_5.1", core.SnapshotTypeIncremental, "full_5", now.Add(-47*time.Hour))

		// Broken Chain (Pruned by PruneBroken=true)
		createPruneTestSnapshot(t, snapshotsBaseDir, "orphan_1", core.SnapshotTypeIncremental, "non_existent_parent", now)

		// 2. Execution
		opts := PruneOptions{
			KeepN:          2,
			PruneOlderThan: 25 * time.Hour,
			PruneBroken:    true,
		}
		deletedIDs, err := manager.Prune(ctx, snapshotsBaseDir, opts)

		// 3. Verification
		require.NoError(t, err)
		expectedDeleted := []string{"full_4", "full_5", "incr_5.1", "orphan_1"}
		assert.ElementsMatch(t, expectedDeleted, deletedIDs)

		// Check remaining/pruned files
		assert.DirExists(t, filepath.Join(snapshotsBaseDir, "full_1"))
		assert.DirExists(t, filepath.Join(snapshotsBaseDir, "full_2"))
		assert.DirExists(t, filepath.Join(snapshotsBaseDir, "full_3"))
		assert.NoDirExists(t, filepath.Join(snapshotsBaseDir, "full_4"))
		assert.NoDirExists(t, filepath.Join(snapshotsBaseDir, "full_5"))
		assert.NoDirExists(t, filepath.Join(snapshotsBaseDir, "orphan_1"))
	})
}
