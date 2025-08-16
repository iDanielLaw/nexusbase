package iterator

import (
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSkippingRangeDeletedIterator(t *testing.T) {
	// Setup: Create a series key that will have range tombstones
	seriesKeyBytes := core.EncodeSeriesKeyWithString("range.metric", map[string]string{"zone": "north"})
	seriesKeyStr := string(seriesKeyBytes)

	// Setup range tombstones using the mock checker from mockup.go
	rangeDeleter := rangeDeletedChecker(map[string][]timeRange{
		seriesKeyStr: []timeRange{
			{start: 100, end: 200, maxSeq: 500}, // RT1
			{start: 300, end: 400, maxSeq: 600}, // RT2
		},
	})

	// Input data points for the underlying iterator
	inputPoints := []*testPoint{
		// Point before all ranges
		makeTestPoint(t, "range.metric", map[string]string{"zone": "north"}, 50, map[string]interface{}{"value": "v1"}, core.EntryTypePutEvent, 10),
		// Point inside RT1, but newer seqNum -> should be kept
		makeTestPoint(t, "range.metric", map[string]string{"zone": "north"}, 150, map[string]interface{}{"value": "v2_new"}, core.EntryTypePutEvent, 600),
		// Point inside RT1, older seqNum -> should be skipped
		makeTestPoint(t, "range.metric", map[string]string{"zone": "north"}, 160, map[string]interface{}{"value": "v3_old"}, core.EntryTypePutEvent, 400),
		// Point between ranges
		makeTestPoint(t, "range.metric", map[string]string{"zone": "north"}, 250, map[string]interface{}{"value": "v4"}, core.EntryTypePutEvent, 10),
		// Point inside RT2, same seqNum -> should be skipped
		makeTestPoint(t, "range.metric", map[string]string{"zone": "north"}, 350, map[string]interface{}{"value": "v5_same"}, core.EntryTypePutEvent, 600),
		// Point after all ranges
		makeTestPoint(t, "range.metric", map[string]string{"zone": "north"}, 450, map[string]interface{}{"value": "v6"}, core.EntryTypePutEvent, 10),
		// Point for a different series -> should always be kept
		makeTestPoint(t, "other.metric", nil, 170, map[string]interface{}{"value": "v_other"}, core.EntryTypePutEvent, 10),
	}

	// Expected points that should NOT be skipped
	expectedPoints := []*testPoint{
		inputPoints[0], // Before range
		inputPoints[1], // In range, but newer
		inputPoints[3], // Between ranges
		inputPoints[5], // After range
		inputPoints[6], // Different series
	}

	mockIter := &mockIterator{points: inputPoints}
	// Create the iterator, passing the mock checker's method and helpers from mockup.go
	skippingIter := NewSkippingRangeDeletedIterator(mockIter, rangeDeleter, extractSeriesKey, decodeTs)

	var actualKeys [][]byte
	for skippingIter.Next() {
		// key, _, _, _ := skippingIter.At()
		cur, _ := skippingIter.At()
		key := cur.Key

		actualKeys = append(actualKeys, key)
	}

	require.NoError(t, skippingIter.Error(), "SkippingRangeDeletedIterator reported an error")

	var expectedKeys [][]byte
	for _, p := range expectedPoints {
		expectedKeys = append(expectedKeys, makeKey(p))
	}

	assert.Equal(t, expectedKeys, actualKeys, "Iterator results mismatch")
}
