package engine2

import (
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/memtable"
	"github.com/INLOpen/nexuscore/utils/clock"
)

// Test that Query returns results in ascending timestamp order when timestamps
// were inserted out-of-order. This validates the memtable ordered-timestamp index.
func TestQueryReturnsOrderedTimestamps(t *testing.T) {
	m := memtable.NewMemtable2(1<<30, clock.SystemClockDefault)
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
		if err := m.Put(dp); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	params := core.QueryParams{Metric: metric, Tags: tags, StartTime: 0, EndTime: 1000}
	it := getPooledIterator(m, params)
	defer it.Close()
	var res []core.QueryResultItem
	for it.Next() {
		v, err := it.AtValue()
		if err != nil {
			t.Fatalf("iterator AtValue error: %v", err)
		}
		res = append(res, v)
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}

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
