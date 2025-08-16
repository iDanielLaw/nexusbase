package iterator

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/require"
)

// --- Generic Iterator Mock ---

// testPoint is a helper struct for defining test data points.
// It embeds core.DataPoint and adds test-specific metadata.

type testPoint struct {
	core.DataPoint
	eType  core.EntryType
	seqNum uint64
}

type mockIterator struct {
	points []*testPoint
	idx    int
	err    error
}

var _ core.Interface = (*mockIterator)(nil)

func (m *mockIterator) Next() bool {
	// A mid-stream error is typically discovered upon trying to read the next item.
	// So, we only check for the error in the Error() method after Next() returns false.
	if m.idx >= len(m.points) {
		return false
	}
	m.idx++
	return true
}

func (m *mockIterator) At() ([]byte, []byte, core.EntryType, uint64) {
	if m.idx > 0 && m.idx <= len(m.points) {
		p := m.points[m.idx-1]
		key := makeKey(p)
		var value []byte
		if p.eType != core.EntryTypeDelete {
			value, _ = p.Fields.Encode()
		}
		return key, value, p.eType, p.seqNum
	}
	return nil, nil, 0, 0
}

func (m *mockIterator) Error() error {
	// The error is only returned if we have iterated past all available points.
	if m.idx >= len(m.points) {
		return m.err
	}
	return nil
}
func (m *mockIterator) Close() error { return nil }

// --- Test Helpers ---

func makeKey(point *testPoint) []byte {
	tsBytes := make([]byte, 8)
	core.EncodeTimestamp(tsBytes, point.Timestamp)

	seriesKey := core.EncodeSeriesKeyWithString(point.Metric, point.Tags)

	// The full key is series key + timestamp
	keyBuf := bytes.NewBuffer(seriesKey)
	keyBuf.Write(tsBytes)
	return keyBuf.Bytes()
}

func makeKeyFromString(metric string, tags map[string]string) []byte {
	seriesKey := core.EncodeSeriesKeyWithString(metric, tags)

	return seriesKey
}

func makeTestPoint(t *testing.T, metric string, tags map[string]string, ts int64, fields map[string]interface{}, eType core.EntryType, seqNum uint64) *testPoint {
	t.Helper()
	fv, err := core.NewFieldValuesFromMap(fields)
	require.NoError(t, err)
	return &testPoint{
		DataPoint: core.DataPoint{
			Metric:    metric,
			Tags:      tags,
			Timestamp: ts,
			Fields:    fv,
		},
		eType:  eType,
		seqNum: seqNum,
	}
}

func seriesDeletedChecker(deletedSeries map[string]uint64) SeriesDeletedChecker {
	return func(seriesKey []byte, seqNum uint64) bool {
		if maxSeq, ok := deletedSeries[string(seriesKey)]; ok {
			return seqNum <= maxSeq
		}
		return false
	}
}

func makeDataPointForExpect(ts int64, metric string, tag map[string]string, fields map[string]interface{}) core.DataPoint {
	dp := core.DataPoint{
		Timestamp: ts,
		Metric:    metric,
		Tags:      tag,
	}

	if len(fields) > 0 {
		for k, v := range fields {
			if v1, err := core.NewPointValue(v); err == nil {
				dp.AddField(k, v1)
			}
		}

	}
	return dp
}

type timeRange struct {
	start, end int64
	maxSeq     uint64
}

func rangeDeletedChecker(deletedRanges map[string][]timeRange) RangeDeletedChecker {
	return func(seriesKey []byte, timestamp int64, seqNum uint64) bool {
		if ranges, ok := deletedRanges[string(seriesKey)]; ok {
			for _, r := range ranges {
				if timestamp >= r.start && timestamp < r.end && seqNum <= r.maxSeq {
					return true
				}
			}
		}
		return false
	}
}

func extractSeriesKey(internalKey []byte) ([]byte, error) {
	if len(internalKey) < 8 {
		return nil, fmt.Errorf("key too short")
	}
	return internalKey[:len(internalKey)-8], nil
}

func decodeTs(tsBytes []byte) (int64, error) {
	if len(tsBytes) != 8 {
		return 0, fmt.Errorf("ts bytes wrong length")
	}
	return int64(binary.BigEndian.Uint64(tsBytes)), nil
}

// compareFloatMaps is a helper to compare two maps of string to float64, with tolerance for NaN.
func compareFloatMaps(t *testing.T, actual, expected map[string]float64) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Fatalf("Result map length mismatch:\nGot:    %v (%d entries)\nWanted: %v (%d entries)", actual, len(actual), expected, len(expected))
	}

	for key, expectedVal := range expected {
		actualVal, ok := actual[key]
		if !ok {
			t.Errorf("Expected key %q not found in result map", key)
			continue
		}
		// Check for NaN equality
		if math.IsNaN(expectedVal) {
			if !math.IsNaN(actualVal) {
				t.Errorf("For key %q, expected NaN, got %f", key, actualVal)
			}
		} else if math.Abs(actualVal-expectedVal) > 1e-9 { // Check for regular float equality with tolerance
			t.Errorf("For key %q, value mismatch: got %f, want %f", key, actualVal, expectedVal)
		}
	}
}
