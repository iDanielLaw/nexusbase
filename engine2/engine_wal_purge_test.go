package engine2

import (
	"fmt"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/wal"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// mockWAL is a mock implementing wal.WALInterface for tests.
type mockWAL struct{ mock.Mock }

func (m *mockWAL) AppendBatch(entries []core.WALEntry) error { return nil }
func (m *mockWAL) Append(entry core.WALEntry) error          { return nil }
func (m *mockWAL) Sync() error                               { return nil }
func (m *mockWAL) Purge(upToIndex uint64) error              { args := m.Called(upToIndex); return args.Error(0) }
func (m *mockWAL) Close() error                              { return nil }
func (m *mockWAL) Path() string                              { return "" }
func (m *mockWAL) SetTestingOnlyInjectCloseError(err error)  {}
func (m *mockWAL) ActiveSegmentIndex() uint64                { return 0 }
func (m *mockWAL) Rotate() error                             { return nil }
func (m *mockWAL) NewStreamReader(fromSeqNum uint64) (wal.StreamReader, error) {
	return nil, fmt.Errorf("not implemented")
}

func TestAdapter_PurgeWALSegments(t *testing.T) {
	cases := []struct {
		name               string
		keepSegments       int
		lastFlushed        uint64
		expectPurge        bool
		expectedPurgeIndex uint64
	}{
		{"Purge_Success", 2, 10, true, 8},
		{"Skip_NotEnoughSegments_Equal", 5, 5, false, 0},
		{"Skip_NotEnoughSegments_Less", 5, 4, false, 0},
		{"DefaultKeepCount_Zero", 0, 10, true, 9},
		{"DefaultKeepCount_Negative", -1, 10, true, 9},
		{"Skip_ZeroLastFlushed", 2, 0, false, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Prepare adapter with mock WAL
			a := &Engine2Adapter{}
			mw := &mockWAL{}
			a.leaderWal = mw

			if tc.expectPurge {
				mw.On("Purge", tc.expectedPurgeIndex).Return(nil).Once()
			}

			err := a.PurgeWALSegments(tc.lastFlushed, tc.keepSegments)
			require.NoError(t, err)

			if tc.expectPurge {
				mw.AssertExpectations(t)
			} else {
				mw.AssertNotCalled(t, "Purge", mock.Anything)
			}
		})
	}
}
