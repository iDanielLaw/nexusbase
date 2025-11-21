package engine2

import (
	"testing"

	"github.com/INLOpen/nexusbase/core"
)

// Test that Query returns results in ascending timestamp order when timestamps
// were inserted out-of-order. This validates the memtable ordered-timestamp index.
func TestQueryReturnsOrderedTimestamps(t *testing.T) {
	m := NewMemtable()
	metric := "m1"
	tags := map[string]string{"host": "a"}

	// insert timestamps out of order
	// build FieldValues using helper to get proper PointValue types
	fv1, err := core.NewFieldValuesFromMap(map[string]interface{}{"v": 3})
	if err != nil {
		t.Fatalf("failed to create field values: %v", err)
	}
	fv2, err := core.NewFieldValuesFromMap(map[string]interface{}{"v": 1})
	if err != nil {
		t.Fatalf("failed to create field values: %v", err)
	}
	fv3, err := core.NewFieldValuesFromMap(map[string]interface{}{"v": 2})
	if err != nil {
		t.Fatalf("failed to create field values: %v", err)
	}
	fv4, err := core.NewFieldValuesFromMap(map[string]interface{}{"v": 4})
	if err != nil {
		t.Fatalf("failed to create field values: %v", err)
	}

	dps := []*core.DataPoint{
		{Metric: metric, Tags: tags, Timestamp: 300, Fields: fv1},
		{Metric: metric, Tags: tags, Timestamp: 100, Fields: fv2},
		{Metric: metric, Tags: tags, Timestamp: 200, Fields: fv3},
		{Metric: metric, Tags: tags, Timestamp: 400, Fields: fv4},
	}

	for _, dp := range dps {
		m.Put(dp)
	}

	params := core.QueryParams{Metric: metric, Tags: tags, StartTime: 0, EndTime: 1000}
	res := m.Query(params)

	if len(res) != 4 {
		t.Fatalf("expected 4 results, got %d", len(res))
	}

	// ensure ascending order
	last := int64(-1)
	for i, it := range res {
		if it.Timestamp <= last {
			t.Fatalf("result %d not in ascending order: %d <= %d", i, it.Timestamp, last)
		}
		last = it.Timestamp
	}
}
