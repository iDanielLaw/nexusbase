package iterator

import (
	"bytes"
	"reflect"
	"sort"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergingIterator_TombstoneHandling(t *testing.T) {
	// Iterator 1: Contains a point tombstone for "cherry"
	iter1 := &mockIterator{points: []*testPoint{
		makeTestPoint(t, "apple", nil, 10, map[string]interface{}{"value": "val_apple_old"}, core.EntryTypePutEvent, 10),
		makeTestPoint(t, "banana", nil, 20, map[string]interface{}{"value": "val_banana_1"}, core.EntryTypePutEvent, 20),
		makeTestPoint(t, "cherry", nil, 30, nil, core.EntryTypeDelete, 30), // Point tombstone for cherry
	}}

	// Iterator 2: Contains a newer version of "apple", and a series-deleted entry
	iter2 := &mockIterator{points: []*testPoint{
		makeTestPoint(t, "apple", nil, 10, map[string]interface{}{"value": "val_apple_new"}, core.EntryTypePutEvent, 100),              // Newer apple
		makeTestPoint(t, "banana", nil, 20, map[string]interface{}{"value": "val_banana_2"}, core.EntryTypePutEvent, 25),               // Newer banana
		makeTestPoint(t, "deleted.series", nil, 40, map[string]interface{}{"value": "val_deleted_series"}, core.EntryTypePutEvent, 50), // Should be skipped by series checker
	}}

	// Iterator 3: Contains a range-deleted entry
	iter3 := &mockIterator{points: []*testPoint{
		makeTestPoint(t, "range.deleted.series", nil, 150, map[string]interface{}{"value": "val_range_deleted"}, core.EntryTypePutEvent, 900), // Should be skipped by range checker
		makeTestPoint(t, "date", nil, 50, map[string]interface{}{"value": "val_date"}, core.EntryTypePutEvent, 60),
	}}

	// Define tombstone rules using the map-based checkers from mockup.go
	seriesDeleter := seriesDeletedChecker(map[string]uint64{
		string(core.EncodeSeriesKeyWithString("deleted.series", nil)): 100, // Deletes if seqNum <= 100
	})

	rangeDeleter := rangeDeletedChecker(map[string][]timeRange{
		string(core.EncodeSeriesKeyWithString("range.deleted.series", nil)): []timeRange{{start: 100, end: 201, maxSeq: 1000}}, // Deletes if ts in [100, 201) and seqNum <= 1000
	})
	mergeParams := MergingIteratorParams{
		Iters:                []Interface{iter1, iter2, iter3},
		StartKey:             nil,
		EndKey:               nil,
		IsSeriesDeleted:      seriesDeleter,
		IsRangeDeleted:       rangeDeleter,
		ExtractSeriesKeyFunc: extractSeriesKey, // Use helper from mockup.go
		DecodeTsFunc:         decodeTs,         // Use helper from mockup.go
	}

	mergingIter, err := NewMergingIteratorWithTombstones(mergeParams)
	require.NoError(t, err, "NewMergingIteratorWithTombstones failed")
	defer mergingIter.Close()

	// Expected results after merging and tombstone handling
	// MergingIterator now filters out point tombstones, series-deleted entries, and range-deleted entries.
	// Only live, non-deleted entries should remain.
	expectedResults := []*testPoint{
		makeTestPoint(t, "apple", nil, 10, map[string]interface{}{"value": "val_apple_new"}, core.EntryTypePutEvent, 100),
		makeTestPoint(t, "banana", nil, 20, map[string]interface{}{"value": "val_banana_2"}, core.EntryTypePutEvent, 25),
		makeTestPoint(t, "date", nil, 50, map[string]interface{}{"value": "val_date"}, core.EntryTypePutEvent, 60),
	}

	var actualResults []*testPoint

	for mergingIter.Next() {
		key, value, entryType, seqNo := mergingIter.At()
		// Decode the key/value back into a testPoint for comparison
		metricBytes, err := extractSeriesKey(key)
		require.NoError(t, err)
		metric, tags, err := core.ExtractMetricAndTagsFromSeriesKeyWithString(metricBytes)
		require.NoError(t, err)
		ts, err := decodeTs(key[len(key)-8:])
		require.NoError(t, err)
		fields, err := core.DecodeFieldsFromBytes(value)
		require.NoError(t, err)

		actualResults = append(actualResults, &testPoint{
			DataPoint: core.DataPoint{Metric: metric, Tags: tags, Timestamp: ts, Fields: fields},
			eType:     entryType,
			seqNum:    seqNo,
		})
	}

	require.NoError(t, mergingIter.Error(), "MergingIterator error")

	// Sort actual results by key for consistent comparison
	sort.Slice(actualResults, func(i, j int) bool {
		return bytes.Compare(makeKey(actualResults[i]), makeKey(actualResults[j])) < 0
	})

	require.Len(t, actualResults, len(expectedResults), "Number of results mismatch")

	for i, expected := range expectedResults {
		actual := actualResults[i]
		assert.Equal(t, expected.Metric, actual.Metric, "Metric mismatch at index %d", i)
		assert.Equal(t, expected.Tags, actual.Tags, "Tags mismatch at index %d", i)
		assert.Equal(t, expected.Timestamp, actual.Timestamp, "Timestamp mismatch at index %d", i)
		assert.Equal(t, expected.eType, actual.eType, "EntryType mismatch at index %d", i)
		assert.Equal(t, expected.seqNum, actual.seqNum, "SequenceNumber mismatch at index %d", i)
		assert.True(t, reflect.DeepEqual(expected.Fields, actual.Fields), "Fields mismatch at index %d.\nGot:    %#v\nWanted: %#v", i, actual.Fields, expected.Fields)
	}
}
