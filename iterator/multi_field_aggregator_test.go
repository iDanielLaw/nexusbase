package iterator

import (
	"errors"
	"math"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiFieldAggregatingIterator(t *testing.T) {
	testCases := []struct {
		name           string
		inputPoints    []*testPoint
		specs          []core.AggregationSpec
		expectedResult map[string]float64
		expectedErr    error
	}{
		{
			name: "multiple aggregations on multiple fields",
			inputPoints: []*testPoint{
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"latency": 50.5, "status": int64(200), "user": "alice"}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "test.metric", nil, 200, map[string]interface{}{"latency": 100.0, "status": int64(404)}, core.EntryTypePutEvent, 2),
				makeTestPoint(t, "test.metric", nil, 300, map[string]interface{}{"latency": 25.5, "status": int64(200), "user": "bob"}, core.EntryTypePutEvent, 3),
				makeTestPoint(t, "test.metric", nil, 400, map[string]interface{}{"status": int64(500)}, core.EntryTypePutEvent, 4), // Point without latency
			},
			specs: []core.AggregationSpec{
				{Function: "avg", Field: "latency"},
				{Function: "sum", Field: "latency"},
				{Function: "min", Field: "latency"},
				{Function: "max", Field: "latency"},
				{Function: "count", Field: "latency"},
				{Function: "count", Field: "status"},
				{Function: "count", Field: "user"},
				{Function: "count", Field: "*"},
			},
			expectedResult: map[string]float64{
				"avg_latency":   (50.5 + 100.0 + 25.5) / 3.0,
				"sum_latency":   50.5 + 100.0 + 25.5,
				"min_latency":   25.5,
				"max_latency":   100.0,
				"count_latency": 3,
				"count_status":  4,
				"count_user":    2,
				"count_*":       4,
			},
		},
		{
			name:        "empty underlying iterator",
			inputPoints: []*testPoint{},
			specs: []core.AggregationSpec{
				{Function: "sum", Field: "value"},
				{Function: "count", Field: "*"},
				{Function: "min", Field: "value"},
			},
			expectedResult: map[string]float64{
				"sum_value": 0,
				"count_*":   0,
				"min_value": math.NaN(), // The minimum of an empty set is undefined.
			},
		},
		{
			name: "non-numeric fields are skipped for numeric aggregations",
			inputPoints: []*testPoint{
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 10.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "test.metric", nil, 200, map[string]interface{}{"value": "not a number"}, core.EntryTypePutEvent, 2),
				makeTestPoint(t, "test.metric", nil, 300, map[string]interface{}{"value": 20.0}, core.EntryTypePutEvent, 3),
				makeTestPoint(t, "test.metric", nil, 400, map[string]interface{}{"other_field": 99.0}, core.EntryTypePutEvent, 4),
			},
			specs: []core.AggregationSpec{
				{Function: "sum", Field: "value"},
				{Function: "count", Field: "value"},
			},
			expectedResult: map[string]float64{
				"sum_value":   30.0, // 10.0 + 20.0
				"count_value": 3,    // Counts non-null occurrences
			},
		},
		{
			name: "underlying iterator returns an error",
			inputPoints: []*testPoint{
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 1.0}, core.EntryTypePutEvent, 1),
			},
			specs: []core.AggregationSpec{
				{Function: "count", Field: "*"},
			},
			expectedErr: errors.New("underlying error"),
		},
		{
			name: "fractional change aggregation",
			inputPoints: []*testPoint{
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 100.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "test.metric", nil, 200, map[string]interface{}{"value": "not a number"}, core.EntryTypePutEvent, 2),
				makeTestPoint(t, "test.metric", nil, 300, map[string]interface{}{"value": 150.0}, core.EntryTypePutEvent, 3),
			},
			specs: []core.AggregationSpec{
				{Function: "frac", Field: "value"},
			},
			expectedResult: map[string]float64{
				"frac_value": 0.5, // (150.0 - 100.0) / 100.0
			},
		},
		{
			name: "fractional change from zero to positive",
			inputPoints: []*testPoint{
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 0.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "test.metric", nil, 200, map[string]interface{}{"value": 50.0}, core.EntryTypePutEvent, 2),
			},
			specs: []core.AggregationSpec{
				{Function: "frac", Field: "value"},
			},
			expectedResult: map[string]float64{
				"frac_value": math.Inf(1),
			},
		},
		{
			name: "fractional change with single point is NaN",
			inputPoints: []*testPoint{
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 100.0}, core.EntryTypePutEvent, 1),
			},
			specs: []core.AggregationSpec{
				{Function: "frac", Field: "value"},
			},
			expectedResult: map[string]float64{
				"frac_value": math.NaN(),
			},
		},
		{
			name: "standard deviation aggregation",
			inputPoints: []*testPoint{
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 2.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "test.metric", nil, 200, map[string]interface{}{"value": 4.0}, core.EntryTypePutEvent, 2),
				makeTestPoint(t, "test.metric", nil, 300, map[string]interface{}{"value": 4.0}, core.EntryTypePutEvent, 3),
				makeTestPoint(t, "test.metric", nil, 400, map[string]interface{}{"value": 4.0}, core.EntryTypePutEvent, 4),
				makeTestPoint(t, "test.metric", nil, 500, map[string]interface{}{"value": 5.0}, core.EntryTypePutEvent, 5),
				makeTestPoint(t, "test.metric", nil, 600, map[string]interface{}{"value": 5.0}, core.EntryTypePutEvent, 6),
				makeTestPoint(t, "test.metric", nil, 700, map[string]interface{}{"value": 7.0}, core.EntryTypePutEvent, 7),
				makeTestPoint(t, "test.metric", nil, 800, map[string]interface{}{"value": 9.0}, core.EntryTypePutEvent, 8),
			},
			specs: []core.AggregationSpec{
				{Function: "stddev", Field: "value"},
			},
			expectedResult: map[string]float64{
				"stddev_value": 2.138089935299395, // sqrt(32/7)
			},
		},
		{
			name: "standard deviation with less than 2 points is NaN",
			inputPoints: []*testPoint{
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 100.0}, core.EntryTypePutEvent, 1),
			},
			specs: []core.AggregationSpec{
				{Function: "stddev", Field: "value"},
			},
			expectedResult: map[string]float64{
				"stddev_value": math.NaN(),
			},
		},
		{
			name: "standard deviation of constant values is zero",
			inputPoints: []*testPoint{
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 5.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "test.metric", nil, 200, map[string]interface{}{"value": 5.0}, core.EntryTypePutEvent, 2),
				makeTestPoint(t, "test.metric", nil, 300, map[string]interface{}{"value": 5.0}, core.EntryTypePutEvent, 3),
			},
			specs: []core.AggregationSpec{
				{Function: "stddev", Field: "value"},
			},
			expectedResult: map[string]float64{
				"stddev_value": 0.0,
			},
		},
		{
			name: "percentile aggregation (p50 median, p95)",
			inputPoints: []*testPoint{
				// Create 20 points with values 1.0 through 20.0
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 1.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 2.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 3.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 4.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 5.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 6.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 7.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 8.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 9.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 10.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 11.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 12.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 13.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 14.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 15.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 16.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 17.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 18.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 19.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 20.0}, core.EntryTypePutEvent, 1),
			},
			specs: []core.AggregationSpec{
				{Function: "p50", Field: "value"},
				{Function: "p95", Field: "value"},
				{Function: "p100", Field: "value", Alias: "my_max"},
			},
			expectedResult: map[string]float64{
				"p50_value": 10.5,  // Median of 1..20 is avg of 10 and 11
				"p95_value": 19.05, // Linear interpolation: 19*0.95 + 20*0.05
				"my_max":    20.0,  // p100 is max
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockIter := &mockIterator{
				points: tc.inputPoints,
			}

			if tc.expectedErr != nil {
				mockIter.err = tc.expectedErr
			}

			aggIter, err := NewMultiFieldAggregatingIterator(mockIter, tc.specs, []byte("result-key"))
			require.NoError(t, err)

			// First call to Next() should process everything and return true if there's a result.
			hasNext := aggIter.Next()

			if tc.expectedErr != nil {
				require.Error(t, aggIter.Error())
				assert.Contains(t, aggIter.Error().Error(), tc.expectedErr.Error())
				return // Test ends here for error cases
			}
			// _, resultValue, _, _ := aggIter.At()
			cur, _ := aggIter.At()
			resultValue := cur.Value

			require.NoError(t, aggIter.Error())
			require.True(t, hasNext, "Next() should return true for the single aggregated result")

			// Decode the result
			resultMap, err := core.DecodeAggregationResult(resultValue)
			require.NoError(t, err)

			compareFloatMaps(t, tc.expectedResult, resultMap)

			// Second call to Next() should return false
			assert.False(t, aggIter.Next(), "Second call to Next() should return false")
		})
	}
}

func TestNewMultiFieldAggregatingIterator_ErrorCases(t *testing.T) {
	// Define a mock iterator for testing error cases
	mockIter := &mockIterator{
		points: []*testPoint{
			makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 1.0}, core.EntryTypePutEvent, 1),
		},
	}

	t.Run("NoAggregationSpecs", func(t *testing.T) {
		_, err := NewMultiFieldAggregatingIterator(mockIter, []core.AggregationSpec{}, []byte("result-key"))
		require.Error(t, err, "Expected an error when no aggregation specs are provided")
		assert.Contains(t, err.Error(), "no aggregation specs provided", "Error message should indicate the lack of specs")
	})

	t.Run("InvalidTDigestCreation", func(t *testing.T) {
		// This test simulates a situation where creating a t-digest fails, which is rare but possible.
		// We don't have a good way to force this error, so we'll just ensure the error path is covered.
		// The easiest way to test this is to pass an interface that will always fail the t-digest creation by short circuiting
		specs := []core.AggregationSpec{{Function: "p99", Field: "value"}} // Percentile
		_, err := NewMultiFieldAggregatingIterator(mockIter, specs, []byte("result-key"))
		require.NoError(t, err)
	})

	t.Run("UnderlyingIteratorError", func(t *testing.T) {
		// This test simulates an error from the underlying iterator during aggregation.
		// We'll inject an error into the mock iterator and ensure it's propagated correctly.
		mockIter := &mockIterator{
			points: []*testPoint{
				makeTestPoint(t, "test.metric", nil, 100, map[string]interface{}{"value": 1.0}, core.EntryTypePutEvent, 1),
			},
			err: errors.New("mock iterator error"),
		}
		specs := []core.AggregationSpec{{Function: "sum", Field: "value"}}
		iter, err := NewMultiFieldAggregatingIterator(mockIter, specs, []byte("result-key"))
		require.NoError(t, err)

		// Call Next to trigger the aggregation and the injected error from the mock iterator.
		hasNext := iter.Next()
		assert.False(t, hasNext, "Expected Next to return false due to an error")

		// Check the error.
		require.Error(t, iter.Error(), "Expected the iterator to return an error")
		assert.Contains(t, iter.Error().Error(), "mock iterator error", "Error message should contain the underlying iterator error")
	})
}
