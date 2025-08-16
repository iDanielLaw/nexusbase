package iterator

import (
	"errors"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiFieldDownsamplingIterator(t *testing.T) {
	seriesA := "seriesA"
	seriesB := "seriesB"
	baseTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)

	testCases := []struct {
		name             string
		inputPoints      []*testPoint
		specs            []core.AggregationSpec
		interval         time.Duration
		startTime        int64
		endTime          int64
		emitEmptyWindows bool
		expected         []struct {
			windowStart int64
			seriesKey   []byte
			values      map[string]float64
		}
		expectCtorErr    bool
		expectRuntimeErr error
	}{
		{
			name: "single series with multiple windows (with alias)",
			inputPoints: []*testPoint{
				makeTestPoint(t, seriesA, nil, baseTime.Add(10*time.Second).UnixNano(), map[string]interface{}{"latency": 100.0, "bytes": 1024.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, seriesA, nil, baseTime.Add(30*time.Second).UnixNano(), map[string]interface{}{"latency": 150.0, "bytes": 2048.0}, core.EntryTypePutEvent, 2),
				//  Next Windows
				makeTestPoint(t, seriesA, nil, baseTime.Add(70*time.Second).UnixNano(), map[string]interface{}{"latency": 200.0, "bytes": 4096.0}, core.EntryTypePutEvent, 3),
				makeTestPoint(t, seriesA, nil, baseTime.Add(90*time.Second).UnixNano(), map[string]interface{}{"latency": 220.0}, core.EntryTypePutEvent, 4),
			},
			specs: []core.AggregationSpec{
				{Function: core.AggSum, Field: "latency", Alias: "total_latency"},
				{Function: core.AggCount, Field: "latency"},
				{Function: core.AggAvg, Field: "bytes", Alias: "avg_b"},
			},
			interval:         1 * time.Minute,
			startTime:        baseTime.UnixNano(),
			endTime:          baseTime.Add(2 * time.Minute).UnixNano(),
			emitEmptyWindows: false,
			expected: []struct {
				windowStart int64
				seriesKey   []byte
				values      map[string]float64
			}{
				{baseTime.UnixNano(), makeKeyFromString(seriesA, nil), map[string]float64{"total_latency": 250.0, "count_latency": 2.0, "avg_b": 1536.0}},
				{baseTime.Add(1 * time.Minute).UnixNano(), makeKeyFromString(seriesA, nil), map[string]float64{"total_latency": 420.0, "count_latency": 2.0, "avg_b": 4096.0}},
			},
		},
		{
			name: "downsampling with aliases",
			inputPoints: []*testPoint{
				makeTestPoint(t, seriesA, nil, baseTime.Add(10*time.Second).UnixNano(), map[string]interface{}{"latency": 100.0, "bytes": 1024.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, seriesA, nil, baseTime.Add(30*time.Second).UnixNano(), map[string]interface{}{"latency": 150.0, "bytes": 2048.0}, core.EntryTypePutEvent, 2),
			},
			specs: []core.AggregationSpec{
				{Function: core.AggSum, Field: "latency", Alias: "total_latency"},
				{Function: core.AggCount, Field: "*", Alias: "request_count"},
			},
			interval:         1 * time.Minute,
			startTime:        baseTime.UnixNano(),
			endTime:          baseTime.Add(1 * time.Minute).UnixNano(),
			emitEmptyWindows: false,
			expected: []struct {
				windowStart int64
				seriesKey   []byte
				values      map[string]float64
			}{{baseTime.UnixNano(), makeKeyFromString(seriesA, nil), map[string]float64{"total_latency": 250.0, "request_count": 2.0}}},
		},
		{
			name: "multiple series interleaved",
			inputPoints: []*testPoint{
				makeTestPoint(t, seriesA, nil, baseTime.Add(10*time.Second).UnixNano(), map[string]interface{}{"value": 10.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, seriesB, nil, baseTime.Add(20*time.Second).UnixNano(), map[string]interface{}{"value": 100.0}, core.EntryTypePutEvent, 2),
				makeTestPoint(t, seriesA, nil, baseTime.Add(30*time.Second).UnixNano(), map[string]interface{}{"value": 20.0}, core.EntryTypePutEvent, 3),
				makeTestPoint(t, seriesB, nil, baseTime.Add(70*time.Second).UnixNano(), map[string]interface{}{"value": 200.0}, core.EntryTypePutEvent, 4),
				makeTestPoint(t, seriesA, nil, baseTime.Add(80*time.Second).UnixNano(), map[string]interface{}{"value": 30.0}, core.EntryTypePutEvent, 5),
			},
			specs:            []core.AggregationSpec{{Function: core.AggSum, Field: "value"}},
			interval:         1 * time.Minute,
			startTime:        baseTime.UnixNano(),
			endTime:          baseTime.Add(2 * time.Minute).UnixNano(),
			emitEmptyWindows: false,
			expected: []struct {
				windowStart int64
				seriesKey   []byte
				values      map[string]float64
			}{
				{baseTime.UnixNano(), makeKeyFromString(seriesA, nil), map[string]float64{"sum_value": 30.0}},
				{baseTime.UnixNano(), makeKeyFromString(seriesB, nil), map[string]float64{"sum_value": 100.0}},
				{baseTime.Add(1 * time.Minute).UnixNano(), makeKeyFromString(seriesA, nil), map[string]float64{"sum_value": 30.0}},
				{baseTime.Add(1 * time.Minute).UnixNano(), makeKeyFromString(seriesB, nil), map[string]float64{"sum_value": 200.0}},
			},
		},

		{
			name: "all points in one window",
			inputPoints: []*testPoint{
				makeTestPoint(t, seriesA, nil, baseTime.Add(1*time.Second).UnixNano(), map[string]interface{}{"value": 1.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, seriesA, nil, baseTime.Add(2*time.Second).UnixNano(), map[string]interface{}{"value": 2.0}, core.EntryTypePutEvent, 2),
				makeTestPoint(t, seriesA, nil, baseTime.Add(3*time.Second).UnixNano(), map[string]interface{}{"value": 3.0}, core.EntryTypePutEvent, 3),
			},
			specs:            []core.AggregationSpec{{Function: core.AggSum, Field: "value"}},
			interval:         1 * time.Minute,
			startTime:        baseTime.UnixNano(),
			endTime:          baseTime.Add(1 * time.Minute).UnixNano(),
			emitEmptyWindows: false,
			expected: []struct {
				windowStart int64
				seriesKey   []byte
				values      map[string]float64
			}{
				{baseTime.UnixNano(), makeKeyFromString(seriesA, nil), map[string]float64{"sum_value": 6.0}},
			},
		},
		{
			name:             "no input points",
			inputPoints:      []*testPoint{},
			specs:            []core.AggregationSpec{{Function: core.AggSum, Field: "value"}},
			interval:         1 * time.Minute,
			startTime:        baseTime.UnixNano(),
			endTime:          baseTime.Add(1 * time.Minute).UnixNano(),
			emitEmptyWindows: false,
			expected: []struct {
				windowStart int64
				seriesKey   []byte
				values      map[string]float64
			}{},
		},
		{
			name: "emit empty windows",
			inputPoints: []*testPoint{
				makeTestPoint(t, seriesA, nil, baseTime.Add(10*time.Second).UnixNano(), map[string]interface{}{"value": 10.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, seriesA, nil, baseTime.Add(130*time.Second).UnixNano(), map[string]interface{}{"value": 30.0}, core.EntryTypePutEvent, 2),
			},
			specs:            []core.AggregationSpec{{Function: core.AggSum, Field: "value"}, {Function: core.AggAvg, Field: "value"}},
			interval:         1 * time.Minute,
			startTime:        baseTime.UnixNano(),
			endTime:          baseTime.Add(3 * time.Minute).UnixNano(), // Query up to, but not including, the start of the 4th window
			emitEmptyWindows: true,
			expected: []struct {
				windowStart int64
				seriesKey   []byte
				values      map[string]float64
			}{
				{baseTime.UnixNano(), makeKeyFromString(seriesA, nil), map[string]float64{"sum_value": 10.0, "avg_value": 10.0}},
				{baseTime.Add(1 * time.Minute).UnixNano(), makeKeyFromString(seriesA, nil), map[string]float64{"sum_value": 0.0, "avg_value": math.NaN()}},
				{baseTime.Add(2 * time.Minute).UnixNano(), makeKeyFromString(seriesA, nil), map[string]float64{"sum_value": 30.0, "avg_value": 30.0}},
			},
		},
		{
			name:             "invalid interval",
			inputPoints:      []*testPoint{},
			specs:            []core.AggregationSpec{},
			interval:         0,
			startTime:        baseTime.UnixNano(),
			endTime:          baseTime.Add(1 * time.Minute).UnixNano(),
			emitEmptyWindows: true,
			expected: []struct {
				windowStart int64
				seriesKey   []byte
				values      map[string]float64
			}{},
			expectCtorErr: true,
		},
		{
			name: "first and last aggregations",
			inputPoints: []*testPoint{
				// Window 1
				makeTestPoint(t, seriesA, nil, baseTime.Add(10*time.Second).UnixNano(), map[string]interface{}{"value": 10.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, seriesA, nil, baseTime.Add(20*time.Second).UnixNano(), map[string]interface{}{"value": 20.0}, core.EntryTypePutEvent, 2),
				makeTestPoint(t, seriesA, nil, baseTime.Add(30*time.Second).UnixNano(), map[string]interface{}{"value": 30.0}, core.EntryTypePutEvent, 3),
				// Window 2
				makeTestPoint(t, seriesA, nil, baseTime.Add(70*time.Second).UnixNano(), map[string]interface{}{"value": 40.0}, core.EntryTypePutEvent, 4),
			},
			specs: []core.AggregationSpec{
				{Function: "first", Field: "value"},
				{Function: "last", Field: "value"},
			},
			interval:         1 * time.Minute,
			startTime:        baseTime.UnixNano(),
			endTime:          baseTime.Add(2 * time.Minute).UnixNano(),
			emitEmptyWindows: false,
			expected: []struct {
				windowStart int64
				seriesKey   []byte
				values      map[string]float64
			}{
				{baseTime.UnixNano(), makeKeyFromString(seriesA, nil), map[string]float64{"first_value": 10.0, "last_value": 30.0}},
				{baseTime.Add(1 * time.Minute).UnixNano(), makeKeyFromString(seriesA, nil), map[string]float64{"first_value": 40.0, "last_value": 40.0}},
			},
		},
		{
			name: "fractional change downsampling",
			inputPoints: []*testPoint{
				// Window 1
				makeTestPoint(t, seriesA, nil, baseTime.Add(10*time.Second).UnixNano(), map[string]interface{}{"value": 100.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, seriesA, nil, baseTime.Add(20*time.Second).UnixNano(), map[string]interface{}{"value": "not-numeric"}, core.EntryTypePutEvent, 2),
				makeTestPoint(t, seriesA, nil, baseTime.Add(30*time.Second).UnixNano(), map[string]interface{}{"value": 150.0}, core.EntryTypePutEvent, 3),
				// Window 2 (not enough points)
				makeTestPoint(t, seriesA, nil, baseTime.Add(70*time.Second).UnixNano(), map[string]interface{}{"value": 200.0}, core.EntryTypePutEvent, 4),
				// Window 3 (division by zero)
				makeTestPoint(t, seriesA, nil, baseTime.Add(130*time.Second).UnixNano(), map[string]interface{}{"value": 0.0}, core.EntryTypePutEvent, 5),
				makeTestPoint(t, seriesA, nil, baseTime.Add(140*time.Second).UnixNano(), map[string]interface{}{"value": 50.0}, core.EntryTypePutEvent, 6),
			},
			specs:            []core.AggregationSpec{{Function: "frac", Field: "value"}},
			interval:         1 * time.Minute,
			startTime:        baseTime.UnixNano(),
			endTime:          baseTime.Add(3 * time.Minute).UnixNano(),
			emitEmptyWindows: false,
			expected: []struct {
				windowStart int64
				seriesKey   []byte
				values      map[string]float64
			}{
				{baseTime.UnixNano(), makeKeyFromString(seriesA, nil), map[string]float64{"frac_value": 0.5}},                              // (150-100)/100
				{baseTime.Add(1 * time.Minute).UnixNano(), makeKeyFromString(seriesA, nil), map[string]float64{"frac_value": math.NaN()}},  // only 1 point
				{baseTime.Add(2 * time.Minute).UnixNano(), makeKeyFromString(seriesA, nil), map[string]float64{"frac_value": math.Inf(1)}}, // (50-0)/0
			},
		},
		{
			name: "standard deviation downsampling",
			inputPoints: []*testPoint{
				// Window 1: values 10, 20, 30 -> stddev = 10
				makeTestPoint(t, seriesA, nil, baseTime.Add(10*time.Second).UnixNano(), map[string]interface{}{"value": 10.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, seriesA, nil, baseTime.Add(20*time.Second).UnixNano(), map[string]interface{}{"value": 20.0}, core.EntryTypePutEvent, 2),
				makeTestPoint(t, seriesA, nil, baseTime.Add(30*time.Second).UnixNano(), map[string]interface{}{"value": 30.0}, core.EntryTypePutEvent, 3),
				// Window 2: values 40, 40 -> stddev = 0
				makeTestPoint(t, seriesA, nil, baseTime.Add(70*time.Second).UnixNano(), map[string]interface{}{"value": 40.0}, core.EntryTypePutEvent, 4),
				makeTestPoint(t, seriesA, nil, baseTime.Add(80*time.Second).UnixNano(), map[string]interface{}{"value": 40.0}, core.EntryTypePutEvent, 5),
				// Window 3: value 50 -> stddev = NaN
				makeTestPoint(t, seriesA, nil, baseTime.Add(130*time.Second).UnixNano(), map[string]interface{}{"value": 50.0}, core.EntryTypePutEvent, 6),
			},
			specs:            []core.AggregationSpec{{Function: "stddev", Field: "value"}},
			interval:         1 * time.Minute,
			startTime:        baseTime.UnixNano(),
			endTime:          baseTime.Add(3 * time.Minute).UnixNano(),
			emitEmptyWindows: false,
			expected: []struct {
				windowStart int64
				seriesKey   []byte
				values      map[string]float64
			}{
				{baseTime.UnixNano(), makeKeyFromString(seriesA, nil), map[string]float64{"stddev_value": 10.0}},
				{baseTime.Add(1 * time.Minute).UnixNano(), makeKeyFromString(seriesA, nil), map[string]float64{"stddev_value": 0.0}},
				{baseTime.Add(2 * time.Minute).UnixNano(), makeKeyFromString(seriesA, nil), map[string]float64{"stddev_value": math.NaN()}},
			},
		},
		{
			name: "percentile downsampling (p50, p90)",
			inputPoints: []*testPoint{
				// Window 1: values 1..10
				makeTestPoint(t, seriesA, nil, baseTime.Add(1*time.Second).UnixNano(), map[string]interface{}{"value": 1.0}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, seriesA, nil, baseTime.Add(2*time.Second).UnixNano(), map[string]interface{}{"value": 2.0}, core.EntryTypePutEvent, 2),
				makeTestPoint(t, seriesA, nil, baseTime.Add(3*time.Second).UnixNano(), map[string]interface{}{"value": 3.0}, core.EntryTypePutEvent, 3),
				makeTestPoint(t, seriesA, nil, baseTime.Add(4*time.Second).UnixNano(), map[string]interface{}{"value": 4.0}, core.EntryTypePutEvent, 4),
				makeTestPoint(t, seriesA, nil, baseTime.Add(5*time.Second).UnixNano(), map[string]interface{}{"value": 5.0}, core.EntryTypePutEvent, 5),
				makeTestPoint(t, seriesA, nil, baseTime.Add(6*time.Second).UnixNano(), map[string]interface{}{"value": 6.0}, core.EntryTypePutEvent, 6),
				makeTestPoint(t, seriesA, nil, baseTime.Add(7*time.Second).UnixNano(), map[string]interface{}{"value": 7.0}, core.EntryTypePutEvent, 7),
				makeTestPoint(t, seriesA, nil, baseTime.Add(8*time.Second).UnixNano(), map[string]interface{}{"value": 8.0}, core.EntryTypePutEvent, 8),
				makeTestPoint(t, seriesA, nil, baseTime.Add(9*time.Second).UnixNano(), map[string]interface{}{"value": 9.0}, core.EntryTypePutEvent, 9),
				makeTestPoint(t, seriesA, nil, baseTime.Add(10*time.Second).UnixNano(), map[string]interface{}{"value": 10.0}, core.EntryTypePutEvent, 10),
				// Window 2: single value
				makeTestPoint(t, seriesA, nil, baseTime.Add(70*time.Second).UnixNano(), map[string]interface{}{"value": 100.0}, core.EntryTypePutEvent, 11),
			},
			specs:            []core.AggregationSpec{{Function: "p50", Field: "value"}, {Function: "p90", Field: "value"}},
			interval:         1 * time.Minute,
			startTime:        baseTime.UnixNano(),
			endTime:          baseTime.Add(2 * time.Minute).UnixNano(),
			emitEmptyWindows: false,
			expected: []struct {
				windowStart int64
				seriesKey   []byte
				values      map[string]float64
			}{
				{baseTime.UnixNano(), makeKeyFromString(seriesA, nil), map[string]float64{"p50_value": 5.5, "p90_value": 9.1}},
				{baseTime.Add(1 * time.Minute).UnixNano(), makeKeyFromString(seriesA, nil), map[string]float64{"p50_value": 100.0, "p90_value": 100.0}},
			},
		},
		{
			name: "error in underlying iterator mid-stream",
			inputPoints: []*testPoint{
				makeTestPoint(t, seriesA, nil, baseTime.Add(10*time.Second).UnixNano(), map[string]interface{}{"value": 10.0}, core.EntryTypePutEvent, 1),
			},
			specs:            []core.AggregationSpec{{Function: core.AggSum, Field: "value"}},
			interval:         1 * time.Minute,
			startTime:        baseTime.UnixNano(),
			endTime:          baseTime.Add(2 * time.Minute).UnixNano(),
			emitEmptyWindows: false,
			expected: []struct {
				windowStart int64
				seriesKey   []byte
				values      map[string]float64
			}{
				// We expect the first window to be processed successfully before the error is encountered.
				{baseTime.UnixNano(), makeKeyFromString(seriesA, nil), map[string]float64{"sum_value": 10.0}},
			},
			expectRuntimeErr: errors.New("mid-stream read error"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockIter := &mockIterator{points: tc.inputPoints}

			downsampler, err := NewMultiFieldDownsamplingIterator(mockIter, tc.specs, tc.interval, tc.startTime, tc.endTime, tc.emitEmptyWindows)

			if tc.expectCtorErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			if tc.expectRuntimeErr != nil {
				mockIter.err = tc.expectRuntimeErr
			}

			var actualResults []struct {
				windowStart int64
				seriesKey   []byte
				values      map[string]float64
			}

			for downsampler.Next() {
				// key, value, _, _ := downsampler.At()
				cur, _ := downsampler.At()
				key := cur.Key
				value := cur.Value

				ts, _ := core.DecodeTimestamp(key[len(key)-8:])
				seriesKey := key[:len(key)-8]

				decodedValues, _ := core.DecodeAggregationResult(value)
				actualResults = append(actualResults, struct {
					windowStart int64
					seriesKey   []byte
					values      map[string]float64
				}{ts, seriesKey, decodedValues})

				// t.Logf("downsampling timestamp: %d, series key: %s, values: %v\n", ts, string(seriesKey), decodedValues)
			}

			if tc.expectRuntimeErr != nil {
				require.Error(t, downsampler.Error())
				assert.Contains(t, downsampler.Error().Error(), tc.expectRuntimeErr.Error())
			} else {
				require.NoError(t, downsampler.Error())
			}

			// Sort both slices for stable comparison, as multi-series results for the same window are not ordered.
			sort.Slice(actualResults, func(i, j int) bool {
				if actualResults[i].windowStart != actualResults[j].windowStart {
					return actualResults[i].windowStart < actualResults[j].windowStart
				}
				return string(actualResults[i].seriesKey) < string(actualResults[j].seriesKey)
			})
			sort.Slice(tc.expected, func(i, j int) bool {
				if tc.expected[i].windowStart != tc.expected[j].windowStart {
					return tc.expected[i].windowStart < tc.expected[j].windowStart
				}
				return string(tc.expected[i].seriesKey) < string(tc.expected[j].seriesKey)
			})

			require.Len(t, actualResults, len(tc.expected), "number of windows mismatch")

			t.Log("Actual Results:", actualResults)
			t.Log("Expected Results:", tc.expected)
			for i, expectedWindow := range tc.expected {
				actualWindow := actualResults[i]
				assert.Equal(t, expectedWindow.windowStart, actualWindow.windowStart, "window start time mismatch for window %d", i)
				assert.Equal(t, expectedWindow.seriesKey, actualWindow.seriesKey, "series key mismatch for window %d", i)
				require.Equal(t, len(expectedWindow.values), len(actualWindow.values), "map length mismatch for window %d", i)
				for key, expectedVal := range expectedWindow.values {
					actualVal, ok := actualWindow.values[key]
					require.True(t, ok, "expected key %s not found in actual values for window %d", key, i)
					if math.IsNaN(expectedVal) {
						assert.True(t, math.IsNaN(actualVal), "for key %s in window %d, expected NaN, got %f", key, i, actualVal)
					} else {
						assert.InDelta(t, expectedVal, actualVal, 1e-9, "value mismatch for key %s in window %d", key, i)
					}
				}
			}
		})
	}
}
