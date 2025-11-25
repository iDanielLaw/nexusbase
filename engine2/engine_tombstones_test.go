package engine2

import (
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsSeriesDeleted(t *testing.T) {
	s := newStorageEngineShim(t.TempDir())
	require.NoError(t, s.Start())

	metric := "test.metric"
	tags := map[string]string{"host": "server1"}
	seriesKey := core.EncodeSeriesKeyWithString(metric, tags)

	// Mark series as deleted at seqNum 100
	s.SetDeletedSeries(seriesKey, 100)

	testCases := []struct {
		name            string
		keyToCheck      []byte
		dataPointSeqNum uint64
		expectedDeleted bool
	}{
		{"SeriesNotMarkedAsDeleted", []byte("other.key"), 10, false},
		{"DataPointOlderThanDeletion", seriesKey, 50, true},
		{"DataPointSameAsDeletion", seriesKey, 100, true},
		{"DataPointNewerThanDeletion", seriesKey, 150, false},
		{"DataPointSeqNumIsZero (considered old)", seriesKey, 0, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectedDeleted, s.IsSeriesDeleted(tc.keyToCheck, tc.dataPointSeqNum))
		})
	}
}

func TestIsCoveredByRangeTombstone(t *testing.T) {
	s := newStorageEngineShim(t.TempDir())
	require.NoError(t, s.Start())

	metric := "range.metric"
	tags := map[string]string{"zone": "north"}
	seriesKey := core.EncodeSeriesKeyWithString(metric, tags)

	// Setup range tombstones
	tombstones := []core.RangeTombstone{
		{MinTimestamp: 100, MaxTimestamp: 200, SeqNum: 500},
		{MinTimestamp: 180, MaxTimestamp: 220, SeqNum: 550},
		{MinTimestamp: 300, MaxTimestamp: 400, SeqNum: 600},
	}
	s.SetRangeTombstones(seriesKey, tombstones)

	testCases := []struct {
		name            string
		keyToCheck      []byte
		timestamp       int64
		dataPointSeqNum uint64
		expectedCovered bool
	}{
		{"NoRangeTombstonesForThisSeries", []byte("other.key"), 150, 10, false},
		{"TimestampBeforeAllRanges", seriesKey, 50, 10, false},
		{"TimestampAfterAllRanges", seriesKey, 500, 10, false},
		{"TimestampInRT1_OlderData", seriesKey, 150, 400, true},
		{"TimestampInRT1_SameData", seriesKey, 150, 500, true},
		{"TimestampInRT1_NewerData", seriesKey, 150, 600, false},
		{"TimestampInRT1_ZeroSeqNum", seriesKey, 150, 0, true},
		{"TimestampInOverlappingRegion_CoveredByNewerRT2", seriesKey, 190, 520, true},
		{"TimestampInOverlappingRegion_NewerThanBoth", seriesKey, 190, 560, false},
		{"TimestampInRT3", seriesKey, 350, 580, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectedCovered, s.IsCoveredByRangeTombstone(tc.keyToCheck, tc.timestamp, tc.dataPointSeqNum))
		})
	}
}
