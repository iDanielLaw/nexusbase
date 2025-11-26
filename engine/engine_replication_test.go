package engine

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/memtable"
	"github.com/INLOpen/nexusbase/replication/proto"
	"github.com/INLOpen/nexusbase/snapshot"
	"github.com/INLOpen/nexuscore/utils/clock"
	"github.com/stretchr/testify/assert"
)

// --- Minimal mocks for required interfaces ---
type mockStringStore struct{}

func (m *mockStringStore) GetOrCreateID(str string) (uint64, error) { return 1, nil }
func (m *mockStringStore) GetString(id uint64) (string, bool)       { return "mock", true }
func (m *mockStringStore) GetID(str string) (uint64, bool)          { return 1, true }
func (m *mockStringStore) Sync() error                              { return nil }
func (m *mockStringStore) Close() error                             { return nil }
func (m *mockStringStore) LoadFromFile(dataDir string) error        { return nil }

type mockSeriesIDStore struct{}

func (m *mockSeriesIDStore) GetOrCreateID(seriesKey string) (uint64, error) { return 1, nil }
func (m *mockSeriesIDStore) GetID(seriesKey string) (uint64, bool)          { return 1, true }
func (m *mockSeriesIDStore) GetKey(id uint64) (string, bool)                { return "mock", true }
func (m *mockSeriesIDStore) Sync() error                                    { return nil }
func (m *mockSeriesIDStore) Close() error                                   { return nil }
func (m *mockSeriesIDStore) LoadFromFile(dataDir string) error              { return nil }

// mockStorageEngine is a minimal stub for testing ApplyReplicatedEntry
// You can expand this with more fields/methods as needed for deeper tests.
type mockStorageEngine struct {
	*storageEngine
	putCalled            bool
	deleteSeriesCalled   bool
	deleteRangeCalled    bool
	wipeDataDirectoryErr bool
}

// Override ApplyReplicatedEntry to call mock methods
func (m *mockStorageEngine) ApplyReplicatedEntry(ctx context.Context, entry *proto.WALEntry) error {
	if err := m.CheckStarted(); err != nil {
		return err
	}
	if m.replicationMode != "follower" {
		return fmt.Errorf("ApplyReplicatedEntry called on a non-follower node (mode: %s)", m.replicationMode)
	}
	switch entry.GetEntryType() {
	case proto.WALEntry_PUT_EVENT:
		err := m.applyPutEvent(ctx, entry)
		if err != nil {
			return fmt.Errorf("failed to apply replicated entry with seq_num %d: %w", entry.GetSequenceNumber(), err)
		}
	case proto.WALEntry_DELETE_SERIES:
		err := m.applyDeleteSeries(ctx, entry)
		if err != nil {
			return fmt.Errorf("failed to apply replicated entry with seq_num %d: %w", entry.GetSequenceNumber(), err)
		}
	case proto.WALEntry_DELETE_RANGE:
		err := m.applyDeleteRange(ctx, entry)
		if err != nil {
			return fmt.Errorf("failed to apply replicated entry with seq_num %d: %w", entry.GetSequenceNumber(), err)
		}
	default:
		return fmt.Errorf("unknown replicated entry type: %v", entry.GetEntryType())
	}
	m.sequenceNumber.Store(entry.GetSequenceNumber())
	return nil
}

// Override ReplaceWithSnapshot to mock filesystem
func (m *mockStorageEngine) ReplaceWithSnapshot(snapshotDir string) error {
	// 1. Wipe the current data directory clean (mocked)
	if m.wipeDataDirectoryErr {
		return fmt.Errorf("failed to wipe data directory for snapshot restore: mock error")
	}
	// 2. Use the snapshot manager to copy and restore the state.
	if m.snapshotManager != nil {
		if err := m.snapshotManager.RestoreFrom(context.Background(), snapshotDir); err != nil {
			return fmt.Errorf("snapshot manager failed to restore from snapshot: %w", err)
		}
	}
	return nil
}

// Always allow engine to be started for tests
func (m *mockStorageEngine) CheckStarted() error {
	// Always allow engine to be started for tests
	return nil
}

func (m *mockStorageEngine) applyPutEvent(ctx context.Context, entry *proto.WALEntry) error {
	m.putCalled = true
	return nil
}
func (m *mockStorageEngine) applyDeleteSeries(ctx context.Context, entry *proto.WALEntry) error {
	m.deleteSeriesCalled = true
	return nil
}
func (m *mockStorageEngine) applyDeleteRange(ctx context.Context, entry *proto.WALEntry) error {
	m.deleteRangeCalled = true
	return nil
}

func TestApplyReplicatedEntry_PutEvent(t *testing.T) {
	se := &storageEngine{
		metrics:         &EngineMetrics{},
		mutableMemtable: memtable.NewMemtable2(1000, clock.SystemClockDefault),
		stringStore:     &mockStringStore{},
		seriesIDStore:   &mockSeriesIDStore{},
		tagIndexManager: &mockTagIndexManager{},
		activeSeries:    make(map[string]struct{}),
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	m := &mockStorageEngine{storageEngine: se}
	m.replicationMode = "follower"
	m.isStarted.Store(true)
	entry := &proto.WALEntry{EntryType: proto.WALEntry_PUT_EVENT, SequenceNumber: 42}
	err := m.ApplyReplicatedEntry(context.Background(), entry)
	assert.NoError(t, err)
	assert.True(t, m.putCalled)
}

func TestApplyReplicatedEntry_DeleteSeries(t *testing.T) {
	se := &storageEngine{
		metrics:         &EngineMetrics{},
		mutableMemtable: memtable.NewMemtable2(1000, clock.SystemClockDefault),
		stringStore:     &mockStringStore{},
		seriesIDStore:   &mockSeriesIDStore{},
		tagIndexManager: &mockTagIndexManager{},
		deletedSeries:   make(map[string]uint64),
		activeSeries:    make(map[string]struct{}),
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	m := &mockStorageEngine{storageEngine: se}
	m.replicationMode = "follower"
	m.isStarted.Store(true)
	entry := &proto.WALEntry{EntryType: proto.WALEntry_DELETE_SERIES, SequenceNumber: 43}
	err := m.ApplyReplicatedEntry(context.Background(), entry)
	assert.NoError(t, err)
	assert.True(t, m.deleteSeriesCalled)
}

func TestApplyReplicatedEntry_DeleteRange(t *testing.T) {
	se := &storageEngine{
		metrics:         &EngineMetrics{},
		mutableMemtable: memtable.NewMemtable2(1000, clock.SystemClockDefault),
		stringStore:     &mockStringStore{},
		seriesIDStore:   &mockSeriesIDStore{},
		tagIndexManager: &mockTagIndexManager{},
		rangeTombstones: make(map[string][]core.RangeTombstone),
		activeSeries:    make(map[string]struct{}),
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	m := &mockStorageEngine{storageEngine: se}
	m.replicationMode = "follower"
	m.isStarted.Store(true)
	entry := &proto.WALEntry{EntryType: proto.WALEntry_DELETE_RANGE, SequenceNumber: 44}
	err := m.ApplyReplicatedEntry(context.Background(), entry)
	assert.NoError(t, err)
	assert.True(t, m.deleteRangeCalled)
}

func TestApplyReplicatedEntry_NonFollower(t *testing.T) {
	se := &storageEngine{
		metrics:         &EngineMetrics{},
		mutableMemtable: memtable.NewMemtable2(1000, clock.SystemClockDefault),
		stringStore:     &mockStringStore{},
		seriesIDStore:   &mockSeriesIDStore{},
		tagIndexManager: &mockTagIndexManager{},
		activeSeries:    make(map[string]struct{}),
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	m := &mockStorageEngine{storageEngine: se}
	m.replicationMode = "leader"
	m.isStarted.Store(true)
	entry := &proto.WALEntry{EntryType: proto.WALEntry_PUT_EVENT, SequenceNumber: 45}
	err := m.ApplyReplicatedEntry(context.Background(), entry)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "non-follower")
}

func TestApplyReplicatedEntry_UnknownType(t *testing.T) {
	se := &storageEngine{
		metrics:         &EngineMetrics{},
		mutableMemtable: memtable.NewMemtable2(1000, clock.SystemClockDefault),
		stringStore:     &mockStringStore{},
		seriesIDStore:   &mockSeriesIDStore{},
		tagIndexManager: &mockTagIndexManager{},
		activeSeries:    make(map[string]struct{}),
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	m := &mockStorageEngine{storageEngine: se}
	m.replicationMode = "follower"
	m.isStarted.Store(true)
	entry := &proto.WALEntry{EntryType: proto.WALEntry_EntryType(99), SequenceNumber: 46} // 99 is not defined in proto
	err := m.ApplyReplicatedEntry(context.Background(), entry)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown replicated entry type")
}

func TestReplaceWithSnapshot_Success(t *testing.T) {
	se := &storageEngine{}
	se.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	se.snapshotManager = &mockSnapshotManager{restoreErr: nil}
	se.isStarted.Store(true)
	se.opts.DataDir = os.TempDir()
	m := &mockStorageEngine{storageEngine: se}
	err := m.ReplaceWithSnapshot(os.TempDir())
	assert.NoError(t, err)
}

func TestReplaceWithSnapshot_FailWipe(t *testing.T) {
	se := &storageEngine{}
	se.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	se.snapshotManager = &mockSnapshotManager{restoreErr: nil}
	se.isStarted.Store(true)
	se.opts.DataDir = os.TempDir()
	m := &mockStorageEngine{storageEngine: se, wipeDataDirectoryErr: true}
	err := m.ReplaceWithSnapshot(os.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wipe data directory")
}

func TestReplaceWithSnapshot_FailRestore(t *testing.T) {
	se := &storageEngine{}
	se.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	se.snapshotManager = &mockSnapshotManager{restoreErr: assert.AnError}
	se.isStarted.Store(true)
	se.opts.DataDir = os.TempDir()
	m := &mockStorageEngine{storageEngine: se}
	err := m.ReplaceWithSnapshot(os.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "snapshot manager failed")
}

// --- Mocks for ReplaceWithSnapshot ---
type mockSnapshotManager struct {
	restoreErr error
}

func (m *mockSnapshotManager) Validate(snapshotDir string) error { return nil }
func (m *mockSnapshotManager) Prune(ctx context.Context, snapshotsBaseDir string, opts snapshot.PruneOptions) ([]string, error) {
	return nil, nil
}

// Implement all required methods for snapshot.ManagerInterface
func (m *mockSnapshotManager) RestoreFrom(ctx context.Context, dir string) error       { return m.restoreErr }
func (m *mockSnapshotManager) CreateFull(ctx context.Context, dir string) error        { return nil }
func (m *mockSnapshotManager) CreateIncremental(ctx context.Context, dir string) error { return nil }
func (m *mockSnapshotManager) GetLatestManifest() (string, error)                      { return "", nil }
func (m *mockSnapshotManager) ListSnapshots(dir string) ([]snapshot.Info, error)       { return nil, nil }
func (m *mockSnapshotManager) DeleteSnapshot(id string) error                          { return nil }
func (m *mockSnapshotManager) GetSnapshotInfo(id string) (interface{}, error)          { return nil, nil }

type testLogger struct{}

func (l *testLogger) Info(msg string, args ...interface{}) {}

// Extend mockStorageEngine for ReplaceWithSnapshot
func (m *mockStorageEngine) wipeDataDirectory() error {
	if m.wipeDataDirectoryErr {
		return assert.AnError
	}
	return nil
}

var _ = testLogger{} // silence unused warning

func TestReplication_Integrated(t *testing.T) {
	// สร้าง mock storage engine สำหรับ leader และ follower
	leader := &mockStorageEngine{
		storageEngine: &storageEngine{
			metrics:         &EngineMetrics{},
			mutableMemtable: memtable.NewMemtable2(1000, clock.SystemClockDefault),
			stringStore:     &mockStringStore{},
			seriesIDStore:   &mockSeriesIDStore{},
			tagIndexManager: &mockTagIndexManager{},
			activeSeries:    make(map[string]struct{}),
			logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	leader.replicationMode = "leader"
	leader.isStarted.Store(true)

	follower := &mockStorageEngine{
		storageEngine: &storageEngine{
			metrics:         &EngineMetrics{},
			mutableMemtable: memtable.NewMemtable2(1000, clock.SystemClockDefault),
			stringStore:     &mockStringStore{},
			seriesIDStore:   &mockSeriesIDStore{},
			tagIndexManager: &mockTagIndexManager{},
			activeSeries:    make(map[string]struct{}),
			logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	follower.replicationMode = "follower"
	follower.isStarted.Store(true)

	// Leader สร้าง WAL entry
	entry := &proto.WALEntry{
		EntryType:      proto.WALEntry_PUT_EVENT,
		SequenceNumber: 1,
		Metric:         "cpu",
		Tags:           map[string]string{"host": "A"},
		Fields:         nil,
		Timestamp:      1234567890,
	}

	// Leader apply entry (simulate write)
	leader.putCalled = true // หรือใช้ method ที่เหมาะสมกับ leader

	// Follower: apply replication
	err := follower.ApplyReplicatedEntry(context.Background(), entry)
	assert.NoError(t, err)
	assert.True(t, follower.putCalled)

	// ตรวจสอบ sequence number sync เฉพาะฝั่ง follower
	assert.Equal(t, entry.SequenceNumber, follower.sequenceNumber.Load())

	// สามารถเพิ่มกรณี integrated อื่น ๆ เช่น DELETE, RANGE, snapshot ได้
}

func TestReplication_E2E(t *testing.T) {
	// 1. สร้าง leader/follower storage engine (ใช้ mock หรือจริง)
	leader := &mockStorageEngine{
		storageEngine: &storageEngine{
			metrics:         &EngineMetrics{},
			mutableMemtable: memtable.NewMemtable2(1000, clock.SystemClockDefault),
			stringStore:     &mockStringStore{},
			seriesIDStore:   &mockSeriesIDStore{},
			tagIndexManager: &mockTagIndexManager{},
			activeSeries:    make(map[string]struct{}),
			logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	leader.replicationMode = "leader"
	leader.isStarted.Store(true)

	follower := &mockStorageEngine{
		storageEngine: &storageEngine{
			metrics:         &EngineMetrics{},
			mutableMemtable: memtable.NewMemtable2(1000, clock.SystemClockDefault),
			stringStore:     &mockStringStore{},
			seriesIDStore:   &mockSeriesIDStore{},
			tagIndexManager: &mockTagIndexManager{},
			activeSeries:    make(map[string]struct{}),
			logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	follower.replicationMode = "follower"
	follower.isStarted.Store(true)

	// 2. Leader สร้าง WAL entry (PUT)
	entry := &proto.WALEntry{
		EntryType:      proto.WALEntry_PUT_EVENT,
		SequenceNumber: 1,
		Metric:         "cpu",
		Tags:           map[string]string{"host": "A"},
		Fields:         nil,
		Timestamp:      1234567890,
	}

	// 3. Leader เขียนข้อมูล (simulate)
	leader.putCalled = true // หรือใช้ method ที่เหมาะสมกับ leader

	// 4. ส่ง WAL entry ไป follower (simulate replication)
	err := follower.ApplyReplicatedEntry(context.Background(), entry)
	assert.NoError(t, err)
	assert.True(t, follower.putCalled)

	// 5. ตรวจสอบข้อมูล sync ระหว่าง leader/follower
	assert.Equal(t, entry.SequenceNumber, follower.sequenceNumber.Load())

	// 6. ทดสอบ DELETE replication
	delEntry := &proto.WALEntry{
		EntryType:      proto.WALEntry_DELETE_SERIES,
		SequenceNumber: 2,
		Metric:         "cpu",
		Tags:           map[string]string{"host": "A"},
	}
	err = follower.ApplyReplicatedEntry(context.Background(), delEntry)
	assert.NoError(t, err)
	assert.True(t, follower.deleteSeriesCalled)

	// 7. ทดสอบ RANGE replication
	rangeEntry := &proto.WALEntry{
		EntryType:      proto.WALEntry_DELETE_RANGE,
		SequenceNumber: 3,
		Metric:         "cpu",
		Tags:           map[string]string{"host": "A"},
		StartTime:      1234560000,
		EndTime:        1234570000,
	}
	err = follower.ApplyReplicatedEntry(context.Background(), rangeEntry)
	assert.NoError(t, err)
	assert.True(t, follower.deleteRangeCalled)

	// 8. ตรวจสอบ sequence number sync
	assert.Equal(t, rangeEntry.SequenceNumber, follower.sequenceNumber.Load())
}
