package nbql

import (
	"context"
	"errors"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine"
	"github.com/INLOpen/nexusbase/hooks"
	corenbql "github.com/INLOpen/nexuscore/nbql"
	"github.com/INLOpen/nexuscore/utils/clock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockStorageEngine is a mock implementation of the StorageEngineInterface.
type MockStorageEngine struct {
	mock.Mock
}

// Ensure MockStorageEngine implements the interface.
var _ engine.StorageEngineInterface = (*MockStorageEngine)(nil)

// CreateSnapshot is a mock method.
func (m *MockStorageEngine) CreateSnapshot(ctx context.Context) (string, error) {
	args := m.Called(ctx)
	return args.String(0), args.Error(1)
}

// RestoreFromSnapshot is a mock method.
func (m *MockStorageEngine) RestoreFromSnapshot(ctx context.Context, path string, overwrite bool) error {
	args := m.Called(ctx, path, overwrite)
	return args.Error(0)
}

// Implement other methods of the interface for compilation.
func (m *MockStorageEngine) Put(ctx context.Context, point core.DataPoint) error {
	args := m.Called(ctx, point)
	return args.Error(0)
}
func (m *MockStorageEngine) PutBatch(ctx context.Context, points []core.DataPoint) error {
	args := m.Called(ctx, points)
	return args.Error(0)
}
func (m *MockStorageEngine) Query(ctx context.Context, params core.QueryParams) (core.QueryResultIteratorInterface, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(core.QueryResultIteratorInterface), args.Error(1)
}
func (m *MockStorageEngine) DeleteSeries(ctx context.Context, metric string, tags map[string]string) error {
	args := m.Called(ctx, metric, tags)
	return args.Error(0)
}
func (m *MockStorageEngine) DeletesByTimeRange(ctx context.Context, metric string, tags map[string]string, start, end int64) error {
	args := m.Called(ctx, metric, tags, start, end)
	return args.Error(0)
}
func (m *MockStorageEngine) GetMetrics() ([]string, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}
func (m *MockStorageEngine) GetTagsForMetric(metric string) ([]string, error) {
	args := m.Called(metric)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}
func (m *MockStorageEngine) GetTagValues(metric, tagKey string) ([]string, error) {
	args := m.Called(metric, tagKey)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}
func (m *MockStorageEngine) ForceFlush(ctx context.Context, wait bool) error {
	args := m.Called(ctx, wait)
	return args.Error(0)
}
func (m *MockStorageEngine) TriggerCompaction() { m.Called() }
func (m *MockStorageEngine) Close() error       { return m.Called().Error(0) }

// --- Added missing methods ---
func (m *MockStorageEngine) GetNextSSTableID() uint64 {
	args := m.Called()
	return args.Get(0).(uint64)
}

func (m *MockStorageEngine) Get(ctx context.Context, metric string, tags map[string]string, timestamp int64) (core.FieldValues, error) {
	args := m.Called(ctx, metric, tags, timestamp)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(core.FieldValues), args.Error(1)
}

func (m *MockStorageEngine) Delete(ctx context.Context, metric string, tags map[string]string, timestamp int64) error {
	args := m.Called(ctx, metric, tags, timestamp)
	return args.Error(0)
}

func (m *MockStorageEngine) GetSeriesByTags(metric string, tags map[string]string) ([]string, error) {
	args := m.Called(metric, tags)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockStorageEngine) CreateIncrementalSnapshot(snapshotsBaseDir string) error {
	args := m.Called(snapshotsBaseDir)
	return args.Error(0)
}

func (m *MockStorageEngine) VerifyDataConsistency() []error {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).([]error)
}

func (m *MockStorageEngine) CleanupEngine() {
	m.Called()
}

func (m *MockStorageEngine) Start() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockStorageEngine) GetPubSub() (engine.PubSubInterface, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(engine.PubSubInterface), args.Error(1)
}

func (m *MockStorageEngine) GetSnapshotsBaseDir() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockStorageEngine) Metrics() (*engine.EngineMetrics, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*engine.EngineMetrics), args.Error(1)
}

func (m *MockStorageEngine) GetHookManager() hooks.HookManager {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(hooks.HookManager)
}

func (m *MockStorageEngine) GetDLQDir() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockStorageEngine) GetDataDir() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockStorageEngine) GetWALPath() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockStorageEngine) GetClock() clock.Clock {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(clock.Clock)
}

func (m *MockStorageEngine) ApplyReplicatedEntry(ctx context.Context, entry *core.WALEntry) error {
	args := m.Called(ctx, entry)
	return args.Error(0)
}

func (m *MockStorageEngine) GetSequenceNumber() uint64 {
	args := m.Called()
	return args.Get(0).(uint64)
}

func (m *MockStorageEngine) SetSequenceNumber(seqNum uint64) {
	m.Called(seqNum)
}

func TestExecutor_SnapshotRestore(t *testing.T) {
	ctx := context.Background()

	t.Run("executeSnapshot success", func(t *testing.T) {
		mockEngine := new(MockStorageEngine)
		executor := NewExecutor(mockEngine, clock.SystemClockDefault)
		cmd := &corenbql.SnapshotStatement{}
		expectedPath := "/var/data/nexus/snapshots/snap-123.nbb"

		mockEngine.On("CreateSnapshot", ctx).Return(expectedPath, nil).Once()

		result, err := executor.Execute(ctx, cmd)
		require.NoError(t, err)
		resMap, ok := result.(map[string]interface{})
		require.True(t, ok, "Result should be a map")
		require.Equal(t, "OK", resMap["status"])
		require.Equal(t, expectedPath, resMap["path"])
		mockEngine.AssertExpectations(t)
	})

	t.Run("executeSnapshot engine error", func(t *testing.T) {
		mockEngine := new(MockStorageEngine)
		executor := NewExecutor(mockEngine, clock.SystemClockDefault)
		cmd := &corenbql.SnapshotStatement{}
		expectedError := errors.New("disk is full")

		mockEngine.On("CreateSnapshot", ctx).Return("", expectedError).Once()

		_, err := executor.Execute(ctx, cmd)
		require.Error(t, err)
		require.ErrorContains(t, err, expectedError.Error())
		mockEngine.AssertExpectations(t)
	})

	t.Run("executeRestore success", func(t *testing.T) {
		mockEngine := new(MockStorageEngine)
		executor := NewExecutor(mockEngine, clock.SystemClockDefault)
		cmd := &corenbql.RestoreStatement{Path: "/path/to/restore.nbb", Overwrite: true}

		mockEngine.On("RestoreFromSnapshot", ctx, cmd.Path, cmd.Overwrite).Return(nil).Once()

		_, err := executor.Execute(ctx, cmd)
		require.NoError(t, err)
		mockEngine.AssertExpectations(t)
	})
}
