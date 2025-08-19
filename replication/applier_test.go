package replication

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	apiv1 "github.com/INLOpen/nexusbase/api/v1"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine"
)

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
			mockEngine := new(engine.MockStorageEngine)
			discardLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
			applier := NewApplier(mockEngine, discardLogger)

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