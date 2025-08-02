package iterator

import (
	"bytes"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Mock Iterator for testing ---
// mockIterator is a simple implementation of the Interface for testing.
// testPoint is a helper struct for defining test data points.
// It embeds core.DataPoint and adds test-specific metadata.

// --- Main Test Function ---

func TestMergingIterator_WithTombstones(t *testing.T) {
	testCases := []struct {
		name           string
		iterators      []Interface
		seriesDeleter  SeriesDeletedChecker
		rangeDeleter   RangeDeletedChecker
		expectedResult []*testPoint
	}{
		{
			name: "Simple merge with no tombstones",
			iterators: []Interface{
				&mockIterator{
					points: []*testPoint{
						makeTestPoint(t, "seriesA", nil, 100, map[string]interface{}{"value": "v1"}, core.EntryTypePutEvent, 1),
						makeTestPoint(t, "seriesB", nil, 150, map[string]interface{}{"value": "v3"}, core.EntryTypePutEvent, 3),
					},
				},
				&mockIterator{
					points: []*testPoint{
						makeTestPoint(t, "seriesA", nil, 120, map[string]interface{}{"value": "v2"}, core.EntryTypePutEvent, 2),
					},
				},
			},
			seriesDeleter: seriesDeletedChecker(nil),
			rangeDeleter:  rangeDeletedChecker(nil),
			expectedResult: []*testPoint{
				makeTestPoint(t, "seriesA", nil, 100, map[string]interface{}{"value": "v1"}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "seriesA", nil, 120, map[string]interface{}{"value": "v2"}, core.EntryTypePutEvent, 2),
				makeTestPoint(t, "seriesB", nil, 150, map[string]interface{}{"value": "v3"}, core.EntryTypePutEvent, 3),
			},
		},
		{
			name: "Point tombstone hides older value",
			iterators: []Interface{
				&mockIterator{
					points: []*testPoint{
						makeTestPoint(t, "seriesA", nil, 100, map[string]interface{}{"value": "v1"}, core.EntryTypePutEvent, 1),
					},
				},
				&mockIterator{
					points: []*testPoint{
						makeTestPoint(t, "seriesA", nil, 100, nil, core.EntryTypeDelete, 2), // Tombstone
					},
				},
			},
			seriesDeleter:  seriesDeletedChecker(nil),
			rangeDeleter:   rangeDeletedChecker(nil),
			expectedResult: []*testPoint{}, // Should be empty
		},
		{
			name: "Point tombstone is hidden by newer value",
			iterators: []Interface{
				&mockIterator{
					points: []*testPoint{
						makeTestPoint(t, "seriesA", nil, 100, map[string]interface{}{"value": "v2"}, core.EntryTypePutEvent, 2),
					},
				},
				&mockIterator{
					points: []*testPoint{
						makeTestPoint(t, "seriesA", nil, 100, nil, core.EntryTypeDelete, 1), // Older tombstone
					},
				},
			},
			seriesDeleter: seriesDeletedChecker(nil),
			rangeDeleter:  rangeDeletedChecker(nil),
			expectedResult: []*testPoint{
				makeTestPoint(t, "seriesA", nil, 100, map[string]interface{}{"value": "v2"}, core.EntryTypePutEvent, 2),
			},
		},
		{
			name: "Series tombstone hides all points in series",
			iterators: []Interface{
				&mockIterator{
					points: []*testPoint{
						makeTestPoint(t, "seriesA", nil, 100, map[string]interface{}{"value": "v1"}, core.EntryTypePutEvent, 1),
						makeTestPoint(t, "seriesB", nil, 110, map[string]interface{}{"value": "v3"}, core.EntryTypePutEvent, 3),
						makeTestPoint(t, "seriesA", nil, 120, map[string]interface{}{"value": "v2"}, core.EntryTypePutEvent, 2),
					},
				},
			},
			seriesDeleter: seriesDeletedChecker(map[string]uint64{
				string(makeKeyFromString("seriesA", nil)): 2, // Deletes seriesA for any point with seqNum <= 2
			}),
			rangeDeleter: rangeDeletedChecker(nil),
			expectedResult: []*testPoint{
				makeTestPoint(t, "seriesB", nil, 110, map[string]interface{}{"value": "v3"}, core.EntryTypePutEvent, 3),
			},
		},
		{
			name: "Range tombstone hides points in range",
			iterators: []Interface{
				&mockIterator{
					points: []*testPoint{
						makeTestPoint(t, "seriesA", nil, 100, map[string]interface{}{"value": "v1"}, core.EntryTypePutEvent, 1),
						makeTestPoint(t, "seriesA", nil, 150, map[string]interface{}{"value": "v2"}, core.EntryTypePutEvent, 2), // Should be deleted
						makeTestPoint(t, "seriesA", nil, 200, map[string]interface{}{"value": "v3"}, core.EntryTypePutEvent, 3),
					},
				},
			},
			seriesDeleter: seriesDeletedChecker(nil),
			rangeDeleter: rangeDeletedChecker(map[string][]timeRange{
				string(makeKeyFromString("seriesA", nil)): {{start: 120, end: 180, maxSeq: 5}}, // Deletes points in [120, 180) with seqNum <= 5
			}),
			expectedResult: []*testPoint{
				makeTestPoint(t, "seriesA", nil, 100, map[string]interface{}{"value": "v1"}, core.EntryTypePutEvent, 1),
				makeTestPoint(t, "seriesA", nil, 200, map[string]interface{}{"value": "v3"}, core.EntryTypePutEvent, 3),
			},
		},
		{
			name: "Combined tombstones",
			iterators: []Interface{
				&mockIterator{
					points: []*testPoint{
						makeTestPoint(t, "seriesA", nil, 100, map[string]interface{}{"value": "vA"}, core.EntryTypePutEvent, 1), // Kept
						makeTestPoint(t, "seriesB", nil, 110, map[string]interface{}{"value": "vB"}, core.EntryTypePutEvent, 2), // Deleted by point tombstone
						makeTestPoint(t, "seriesC", nil, 120, map[string]interface{}{"value": "vC"}, core.EntryTypePutEvent, 3), // Deleted by series tombstone
						makeTestPoint(t, "seriesD", nil, 130, map[string]interface{}{"value": "vD"}, core.EntryTypePutEvent, 4), // Deleted by range tombstone
					},
				},
				&mockIterator{
					points: []*testPoint{
						makeTestPoint(t, "seriesB", nil, 110, nil, core.EntryTypeDelete, 5), // Point tombstone
					},
				},
			},
			seriesDeleter: seriesDeletedChecker(map[string]uint64{
				string(makeKeyFromString("seriesC", nil)): 10,
			}),
			rangeDeleter: rangeDeletedChecker(map[string][]timeRange{
				string(makeKeyFromString("seriesD", nil)): {{start: 100, end: 150, maxSeq: 10}},
			}),
			expectedResult: []*testPoint{
				makeTestPoint(t, "seriesA", nil, 100, map[string]interface{}{"value": "vA"}, core.EntryTypePutEvent, 1),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			params := MergingIteratorParams{
				Iters:                tc.iterators,
				IsSeriesDeleted:      tc.seriesDeleter,
				IsRangeDeleted:       tc.rangeDeleter,
				ExtractSeriesKeyFunc: extractSeriesKey,
				DecodeTsFunc:         decodeTs,
			}
			iter, err := NewMergingIteratorWithTombstones(params)
			require.NoError(t, err)
			defer iter.Close()

			type rawResult struct {
				key    []byte
				value  []byte
				eType  core.EntryType
				seqNum uint64
			}
			var results []rawResult

			for iter.Next() {
				key, value, entryType, seqNum := iter.At()
				// Create copies of key and value as the underlying buffer might be reused
				keyCopy := make([]byte, len(key))
				copy(keyCopy, key)
				var valCopy []byte
				if value != nil {
					valCopy = make([]byte, len(value))
					copy(valCopy, value)
				}

				results = append(results, rawResult{
					key:    keyCopy,
					value:  valCopy,
					eType:  entryType,
					seqNum: seqNum,
				})
			}

			require.NoError(t, iter.Error())

			// Compare results
			require.Equal(t, len(tc.expectedResult), len(results), "Number of results mismatch")
			for i, expected := range tc.expectedResult {
				actual := results[i]

				expectedKey := makeKey(expected)
				expectedValue, _ := expected.Fields.Encode()

				assert.True(t, bytes.Equal(expectedKey, actual.key), "Key mismatch at index %d", i)
				assert.True(t, bytes.Equal(expectedValue, actual.value), "Value mismatch at index %d for key %s", i, string(actual.key))
				assert.Equal(t, expected.eType, actual.eType, "EntryType mismatch at index %d", i)
				assert.Equal(t, expected.seqNum, actual.seqNum, "SequenceNumber mismatch at index %d", i)
			}
		})
	}
}

func TestMergingIterator_DescendingOrder(t *testing.T) {
	// For DESC order, the underlying iterators should also provide data from newest to oldest.
	// We simulate this by pre-sorting the mock data in descending order for each mock iterator.
	iter1 := &mockIterator{
		points: []*testPoint{
			makeTestPoint(t, "seriesA", nil, 300, map[string]interface{}{"value": "v3"}, core.EntryTypePutEvent, 30),
			makeTestPoint(t, "seriesA", nil, 100, map[string]interface{}{"value": "v1_old"}, core.EntryTypePutEvent, 10),
		},
	}
	iter2 := &mockIterator{
		points: []*testPoint{
			makeTestPoint(t, "seriesA", nil, 250, map[string]interface{}{"value": "v4"}, core.EntryTypePutEvent, 40),
			makeTestPoint(t, "seriesA", nil, 150, map[string]interface{}{"value": "v2"}, core.EntryTypePutEvent, 20),
		},
	}
	iter3 := &mockIterator{
		points: []*testPoint{
			// A newer version of the point at ts=100
			makeTestPoint(t, "seriesA", nil, 100, map[string]interface{}{"value": "v1_new"}, core.EntryTypePutEvent, 15),
		},
	}

	// Create the merging iterator with DESCENDING order
	params := MergingIteratorParams{
		Iters:                []Interface{iter1, iter2, iter3},
		Order:                core.Descending,
		IsSeriesDeleted:      seriesDeletedChecker(nil), // No deletions for this test
		IsRangeDeleted:       rangeDeletedChecker(nil),  // No deletions for this test
		ExtractSeriesKeyFunc: extractSeriesKey,
		DecodeTsFunc:         decodeTs,
	}
	iter, err := NewMergingIteratorWithTombstones(params)
	require.NoError(t, err)
	defer iter.Close()

	// Expected results should be sorted from newest to oldest timestamp.
	// For the same timestamp, the one with the higher sequence number comes first.
	// The older version of the point at ts=100 (seqNum=10) should be skipped.
	expectedResult := []*testPoint{
		makeTestPoint(t, "seriesA", nil, 300, map[string]interface{}{"value": "v3"}, core.EntryTypePutEvent, 30),
		makeTestPoint(t, "seriesA", nil, 250, map[string]interface{}{"value": "v4"}, core.EntryTypePutEvent, 40),
		makeTestPoint(t, "seriesA", nil, 150, map[string]interface{}{"value": "v2"}, core.EntryTypePutEvent, 20),
		makeTestPoint(t, "seriesA", nil, 100, map[string]interface{}{"value": "v1_new"}, core.EntryTypePutEvent, 15),
	}

	var actualResults []*testPoint
	for iter.Next() {
		key, value, entryType, seqNum := iter.At()
		// Decode the key/value back into a testPoint for comparison
		metricBytes, _ := extractSeriesKey(key)
		metric, tags, _ := core.ExtractMetricAndTagsFromSeriesKeyWithString(metricBytes)
		ts, _ := decodeTs(key[len(key)-8:])
		fields, _ := core.DecodeFieldsFromBytes(value)

		actualResults = append(actualResults, &testPoint{
			DataPoint: core.DataPoint{Metric: metric, Tags: tags, Timestamp: ts, Fields: fields},
			eType:     entryType,
			seqNum:    seqNum,
		})
	}

	require.NoError(t, iter.Error())
	require.Len(t, actualResults, len(expectedResult), "Number of results mismatch")

	for i, expected := range expectedResult {
		actual := actualResults[i]
		assert.Equal(t, expected.Timestamp, actual.Timestamp, "Timestamp mismatch at index %d", i)
		assert.Equal(t, expected.seqNum, actual.seqNum, "SequenceNumber mismatch at index %d", i)
	}
}
