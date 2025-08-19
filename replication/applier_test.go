package replication

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	apiv1 "github.com/INLOpen/nexusbase/api/v1"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexuscore/utils/clock"
)

// MockStorageEngine is a mock implementation of engine.StorageEngineInterface for testing.
type MockStorageEngine struct {
	mock.Mock
}

// Ensure MockStorageEngine implements the interface.
var _ engine.StorageEngineInterface = (*MockStorageEngine)(nil)

// Implement all methods of the interface, but we only care about ApplyReplicatedEntry for this test.
func (m *MockStorageEngine) ApplyReplicatedEntry(ctx context.Context, entry *core.WALEntry) error {
	args := m.Called(ctx, entry)
	return args.Error(0)
}

// Add other methods with dummy implementations to satisfy the interface.
func (m *MockStorageEngine) Put(ctx context.Context, point core.DataPoint) error { return nil }
func (m *MockStorageEngine) PutBatch(ctx context.Context, points []core.DataPoint) error { return nil }
func (m *MockStorageEngine) Get(ctx context.Context, metric string, tags map[string]string, timestamp int64) (core.FieldValues, error) {
	return nil, nil
}
func (m *MockStorageEngine) Delete(ctx context.Context, metric string, tags map[string]string, timestamp int64) error {
	return nil
}
func (m *MockStorageEngine) DeleteSeries(ctx context.Context, metric string, tags map[string]string) error {
	return nil
}
func (m *MockStorageEngine) DeletesByTimeRange(ctx context.Context, metric string, tags map[string]string, startTime, endTime int64) error {
	return nil
}
func (m *MockStorageEngine) Query(ctx context.Context, params core.QueryParams) (core.QueryResultIteratorInterface, error) {
	return nil, nil
}
func (m *MockStorageEngine) GetSeriesByTags(metric string, tags map[string]string) ([]string, error) {
	return nil, nil
}
func (m *MockStorageEngine) GetSequenceNumber() uint64 { return 0 }
func (m *MockStorageEngine) SetSequenceNumber(seqNum uint64) {}
func (m *MockStorageEngine) GetReplicationTracker() *core.ReplicationTracker { return nil }
func (m *MockStorageEngine) GetMetrics() ([]string, error) { return nil, nil }
func (m *MockStorageEngine) GetTagsForMetric(metric string) ([]string, error) { return nil, nil }
func (m *MockStorageEngine) GetTagValues(metric, tagKey string) ([]string, error) { return nil, nil }
func (m *MockStorageEngine) ForceFlush(ctx context.Context, wait bool) error { return nil }
func (m *MockStorageEngine) TriggerCompaction() {}
func (m *MockStorageEngine) CreateIncrementalSnapshot(snapshotsBaseDir string) error { return nil }
func (m *MockStorageEngine) VerifyDataConsistency() []error { return nil }
func (m *MockStorageEngine) CreateSnapshot(ctx context.Context) (string, error) { return "", nil }
func (m *MockStorageEngine) RestoreFromSnapshot(ctx context.Context, path string, overwrite bool) error {
	return nil
}
func (m *MockStorageEngine) CleanupEngine() {}
func (m *MockStorageEngine) Start() error { return nil }
func (m *MockStorageEngine) Close() error { return nil }
func (m *MockStorageEngine) GetPubSub() (engine.PubSubInterface, error) { return nil, nil }
func (m *MockStorageEngine) GetSnapshotsBaseDir() string { return "" }
func (m *MockStorageEngine) Metrics() (*engine.EngineMetrics, error) { return nil, nil }
func (m *MockStorageEngine) GetHookManager() hooks.HookManager { return nil }
func (m *MockStorageEngine) GetDLQDir() string { return "" }
func (m *MockStorageEngine) GetDataDir() string { return "" }
func (m *MockStorageEngine) GetWALPath() string { return "" }
func (m *MockStorageEngine) GetClock() clock.Clock { return nil }
func (m *MockStorageEngine) GetNextSSTableID() uint64 { return 0 }

func TestApplier_ApplyEntry(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name          string
		apiEntry      *apiv1.WALEntry
		expectedCore  *core.WALEntry
		expectSuccess bool
	}{
		{
			name: "Apply PutEvent",
			apiEntry: &apiv1.WALEntry{
				SequenceNumber:  101,
				WalSegmentIndex: 5,
				Payload: &apiv1.WALEntry_PutEvent{
					PutEvent: &apiv1.PutEvent{Key: []byte("key1"), Value: []byte("val1")},
				},
			},
			expectedCore: &core.WALEntry{
				SeqNum:       101,
				SegmentIndex: 5,
				EntryType:    core.EntryTypePutEvent,
				Key:          []byte("key1"),
				Value:        []byte("val1"),
			},
			expectSuccess: true,
		},
		// ... (add other cases for Delete, DeleteSeries, etc.) ...
		{
			name: "Unknown Payload Type",
			apiEntry: &apiv1.WALEntry{
				SequenceNumber:  105,
				WalSegmentIndex: 5,
				Payload:         nil, // Invalid
			},
			expectedCore:  nil,
			expectSuccess: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockEngine := new(MockStorageEngine)
			applier := NewApplier(mockEngine, nil)

			if tc.expectSuccess {
				// Setup mock expectation for a successful call
				mockEngine.On("ApplyReplicatedEntry", ctx, mock.MatchedBy(func(entry *core.WALEntry) bool {
					assert.Equal(t, tc.expectedCore, entry)
					return true
				})).Return(nil).Once()
			}

			err := applier.ApplyEntry(ctx, tc.apiEntry)

			if tc.expectSuccess {
				require.NoError(t, err)
				mockEngine.AssertExpectations(t)
			} else {
				require.Error(t, err)
			}
		})
	}
}