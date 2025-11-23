package server

import (
	"context"
	"sync/atomic"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine2"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/indexer"
	"github.com/INLOpen/nexusbase/replication/proto"
	"github.com/INLOpen/nexusbase/snapshot"
	"github.com/INLOpen/nexusbase/wal"
	"github.com/INLOpen/nexuscore/utils/clock"
	"github.com/stretchr/testify/mock"
)

// MockStorageEngine is a mock implementation of engine.StorageEngineInterface for testing.
// It uses testify/mock to allow setting expectations and asserting calls.
type MockStorageEngine struct {
	mock.Mock
	nextId  atomic.Uint64
	started bool
}

// Ensure MockStorageEngine implements the interface.
var _ engine2.StorageEngineInterface = (*MockStorageEngine)(nil)

// ReplaceWithSnapshot is a stub for testing purposes.
func (m *MockStorageEngine) ReplaceWithSnapshot(snapshotDir string) error {
	return nil
}

// GetWAL returns nil for testing purposes.
func (m *MockStorageEngine) GetWAL() wal.WALInterface {
	return nil
}

// GetStringStore returns a dummy implementation for testing purposes.
func (m *MockStorageEngine) GetStringStore() indexer.StringStoreInterface {
	return &indexer.StringStore{} // Always return non-nil dummy object
}

// GetSnapshotManager returns nil for testing purposes.
func (m *MockStorageEngine) GetSnapshotManager() snapshot.ManagerInterface {
	return nil
}

// GetLatestAppliedSeqNum returns a mock sequence number for replication tests.
func (m *MockStorageEngine) GetLatestAppliedSeqNum() uint64 {
	return 0
}

// ApplyReplicatedEntry is a mock implementation for replication tests.
func (m *MockStorageEngine) ApplyReplicatedEntry(ctx context.Context, entry *proto.WALEntry) error {
	args := m.Called(ctx, entry)
	return args.Error(0)
}

func (m *MockStorageEngine) GetNextSSTableID() uint64 {
	return m.nextId.Add(1)
}

func (m *MockStorageEngine) Start() error {
	args := m.Called()
	m.started = true
	return args.Error(0)
}

func (m *MockStorageEngine) Put(ctx context.Context, point core.DataPoint) error {
	args := m.Called(ctx, point)
	return args.Error(0)
}

func (m *MockStorageEngine) PutBatch(ctx context.Context, points []core.DataPoint) error {
	args := m.Called(ctx, points)
	return args.Error(0)
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

func (m *MockStorageEngine) DeleteSeries(ctx context.Context, metric string, tags map[string]string) error {
	args := m.Called(ctx, metric, tags)
	return args.Error(0)
}

func (m *MockStorageEngine) DeletesByTimeRange(ctx context.Context, metric string, tags map[string]string, startTime int64, endTime int64) error {
	args := m.Called(ctx, metric, tags, startTime, endTime)
	return args.Error(0)
}

func (m *MockStorageEngine) Query(ctx context.Context, params core.QueryParams) (core.QueryResultIteratorInterface, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(core.QueryResultIteratorInterface), args.Error(1)
}

func (m *MockStorageEngine) GetSeriesByTags(metric string, tags map[string]string) ([]string, error) {
	args := m.Called(metric, tags)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockStorageEngine) ForceFlush(ctx context.Context, wait bool) error {
	args := m.Called(ctx, wait)
	return args.Error(0)
}

func (m *MockStorageEngine) TriggerCompaction() {
	m.Called()
}

func (m *MockStorageEngine) CreateSnapshot(ctx context.Context) (string, error) {
	args := m.Called(ctx)
	return args.String(0), args.Error(1)
}

// RestoreFromSnapshot is a mock method.
func (m *MockStorageEngine) RestoreFromSnapshot(ctx context.Context, path string, overwrite bool) error {
	args := m.Called(ctx, path, overwrite)
	return args.Error(0)
}

func (m *MockStorageEngine) CreateIncrementalSnapshot(snapshotsBaseDir string) error {
	args := m.Called(snapshotsBaseDir)
	return args.Error(0)
}

func (m *MockStorageEngine) GetPubSub() (engine2.PubSubInterface, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	// Return the interface type directly.
	// The test will provide a mock that satisfies this interface.
	return args.Get(0).(engine2.PubSubInterface), nil
}

func (m *MockStorageEngine) GetHookManager() hooks.HookManager {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(hooks.HookManager)
}

func (m *MockStorageEngine) CleanupEngine() {
	m.Called()
}

func (m *MockStorageEngine) Close() error {
	args := m.Called()
	return args.Error(0)
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
func (m *MockStorageEngine) GetSnapshotsBaseDir() string {
	args := m.Called()
	return args.String(0)
}
func (m *MockStorageEngine) VerifyDataConsistency() []error {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).([]error)
}
func (m *MockStorageEngine) Metrics() (*engine2.EngineMetrics, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*engine2.EngineMetrics), nil
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
func (m *MockStorageEngine) GetClock() clock.Clock {
	args := m.Called()
	if args.Get(0) == nil {
		return &clock.SystemClock{} // Return a default clock if not mocked
	}
	return args.Get(0).(clock.Clock)
}

func (m *MockStorageEngine) GetSequenceNumber() uint64 {
	args := m.Called()
	return args.Get(0).(uint64)
}

// MockQueryResultIterator is a manual mock implementation of core.QueryResultIteratorInterface.
// It's designed to be stateful for testing iteration logic.
type MockQueryResultIterator struct {
	items    []*core.QueryResultItem
	index    int
	err      error
	closeErr error
}

// Ensure MockQueryResultIterator implements the interface.
var _ core.QueryResultIteratorInterface = (*MockQueryResultIterator)(nil)

// NewMockQueryResultIterator creates a new mock iterator with predefined items and a potential error.
func NewMockQueryResultIterator(items []*core.QueryResultItem, err error) *MockQueryResultIterator {
	return &MockQueryResultIterator{
		items: items,
		index: -1, // Start before the first item
		err:   err,
	}
}

func (m *MockQueryResultIterator) Next() bool {
	if m.index+1 < len(m.items) {
		m.index++
		return true
	}
	return false
}

func (m *MockQueryResultIterator) At() (*core.QueryResultItem, error) {
	if m.index < 0 || m.index >= len(m.items) {
		// This indicates a programming error in the test: At() was called
		// without a preceding successful call to Next(), or after Next() returned false.
		// Panicking makes the test failure immediate and obvious.
		panic("MockQueryResultIterator: At() called out of bounds")
	}
	return m.items[m.index], nil
}

// AtValue returns a value-copy of the current item. This mirrors the
// behavior of real iterators that offer AtValue() for callers who need
// stable copies independent of iterator internal buffers.
func (m *MockQueryResultIterator) AtValue() (core.QueryResultItem, error) {
	if m.index < 0 || m.index >= len(m.items) {
		panic("MockQueryResultIterator: AtValue() called out of bounds")
	}
	src := m.items[m.index]
	out := core.QueryResultItem{
		Metric:          src.Metric,
		Timestamp:       src.Timestamp,
		IsAggregated:    src.IsAggregated,
		WindowStartTime: src.WindowStartTime,
		WindowEndTime:   src.WindowEndTime,
		IsEvent:         src.IsEvent,
	}
	if src.Tags != nil {
		tags := make(map[string]string, len(src.Tags))
		for k, v := range src.Tags {
			tags[k] = v
		}
		out.Tags = tags
	}
	if src.Fields != nil {
		fv := make(core.FieldValues, len(src.Fields))
		for k, v := range src.Fields {
			fv[k] = v
		}
		out.Fields = fv
	}
	if src.AggregatedValues != nil {
		av := make(map[string]float64, len(src.AggregatedValues))
		for k, v := range src.AggregatedValues {
			av[k] = v
		}
		out.AggregatedValues = av
	}
	return out, nil
}

func (m *MockQueryResultIterator) Error() error {
	return m.err
}

func (m *MockQueryResultIterator) Close() error {
	return m.closeErr
}

func (m *MockQueryResultIterator) Put(item *core.QueryResultItem) {
	// No-op for the mock, as we don't need to manage a pool in tests.
}

func (m *MockQueryResultIterator) UnderlyingAt() (*core.IteratorNode, error) {
	// This is a mock implementation. Return nil or sensible defaults.
	// Tests that need specific underlying data can extend this mock.
	return nil, nil
}

// Dummy implementation for indexer.StringStoreInterface
// Ensure only one definition exists in the test package
// Add all required methods for the interface

type dummyStringStore struct{}

func (d *dummyStringStore) GetOrCreateID(s string) (uint64, error) {
	return 0, nil
}

func (d *dummyStringStore) GetID(s string) (uint64, bool) {
	return 0, false
}

func (d *dummyStringStore) Put(ctx context.Context, key string, value string) error {
	return nil
}

func (d *dummyStringStore) Get(ctx context.Context, key string) (string, error) {
	return "", nil
}

func (d *dummyStringStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (d *dummyStringStore) GetString(id uint64) (string, bool) {
	return "", false
}
func (d *dummyStringStore) Sync() error                       { return nil }
func (d *dummyStringStore) Close() error                      { return nil }
func (d *dummyStringStore) LoadFromFile(dataDir string) error { return nil }

// Ensure dummyStringStore implements indexer.StringStoreInterface
var _ indexer.StringStoreInterface = (*dummyStringStore)(nil)
