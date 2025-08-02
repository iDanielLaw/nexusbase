package iterator

import (
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSkippingDeletedSeriesIterator(t *testing.T) {
	// Setup the series key for the deleted series using the string-based helper
	deletedSeriesKey := core.EncodeSeriesKeyWithString("deleted_series", nil)

	// Use the standardized seriesDeletedChecker from mockup.go
	seriesDeleter := seriesDeletedChecker(map[string]uint64{
		string(deletedSeriesKey): 100, // Deletes if seqNum <= 100
	})

	// Use the standardized mockIterator and testPoint helpers
	inputPoints := []*testPoint{
		makeTestPoint(t, "active_series", nil, 1, map[string]interface{}{"value": "val1"}, core.EntryTypePutEvent, 1),
		makeTestPoint(t, "deleted_series", nil, 2, map[string]interface{}{"value": "val2"}, core.EntryTypePutEvent, 50), // older seq -> skipped
		makeTestPoint(t, "active_series", nil, 3, map[string]interface{}{"value": "val3"}, core.EntryTypePutEvent, 3),
		makeTestPoint(t, "deleted_series", nil, 4, map[string]interface{}{"value": "val4"}, core.EntryTypePutEvent, 100), // equal seq -> skipped
		makeTestPoint(t, "deleted_series", nil, 5, map[string]interface{}{"value": "val5"}, core.EntryTypePutEvent, 150), // newer seq -> NOT skipped
	}

	mockIter := &mockIterator{points: inputPoints}
	// Use the standardized extractor and the checker's method
	skippingIter := NewSkippingDeletedSeriesIterator(mockIter, seriesDeleter, extractSeriesKey)

	expectedPoints := []*testPoint{
		inputPoints[0], // active_series
		inputPoints[2], // active_series
		inputPoints[4], // deleted_series (newer seqNum)
	}

	var actualKeys [][]byte
	for skippingIter.Next() {
		key, _, _, _ := skippingIter.At()
		actualKeys = append(actualKeys, key)
	}
	require.NoError(t, skippingIter.Error(), "Iterator should not have an error")

	require.Len(t, actualKeys, len(expectedPoints), "Number of results mismatch")
	for i, expectedPoint := range expectedPoints {
		expectedKey := makeKey(expectedPoint)
		assert.Equal(t, expectedKey, actualKeys[i], "Key mismatch at index %d", i)
	}
}
