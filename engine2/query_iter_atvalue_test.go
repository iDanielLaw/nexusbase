package engine2

import (
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/memtable"
	"github.com/INLOpen/nexuscore/utils/clock"
)

// Test that AtValue returns a value copy that is stable across iterator advances
// while At() returns a pointer to an internal buffer that is reused.
func TestMemQueryIterator_AtValueIndependent(t *testing.T) {
	m := memtable.NewMemtable2(1<<30, clock.SystemClockDefault)

	// create two datapoints for same metric+tags with different timestamps
	dp1, _ := core.NewDataPoint("cpu.usage", map[string]string{"host": "a"}, 100)
	pv1, _ := core.NewPointValue(float64(1.23))
	dp1.AddField("value", pv1)
	if err := m.Put(dp1); err != nil {
		t.Fatalf("PutDataPoint failed: %v", err)
	}

	dp2, _ := core.NewDataPoint("cpu.usage", map[string]string{"host": "a"}, 200)
	pv2, _ := core.NewPointValue(float64(4.56))
	dp2.AddField("value", pv2)
	if err := m.Put(dp2); err != nil {
		t.Fatalf("PutDataPoint failed: %v", err)
	}

	params := core.QueryParams{Metric: "cpu.usage", StartTime: 0, EndTime: 1000, Tags: map[string]string{"host": "a"}}
	it := getPooledIterator(m, params)
	defer it.Close()

	if !it.Next() {
		t.Fatalf("expected first Next() to be true")
	}

	// pointer to internal buffer
	ptr, err := it.At()
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}

	// value copy
	val, err := it.AtValue()
	if err != nil {
		t.Fatalf("AtValue() error: %v", err)
	}

	// advance to next item (this will overwrite internal curr used by At())
	if !it.Next() {
		t.Fatalf("expected second Next() to be true")
	}

	// ptr should now reflect the second element (overwritten)
	if ptr.Timestamp == val.Timestamp {
		t.Fatalf("expected At() pointer to be overwritten after Next(); got same timestamp %d", ptr.Timestamp)
	}

	// val should remain the original first element
	if val.Timestamp != 100 {
		t.Fatalf("expected AtValue copy to keep original timestamp 100; got %d", val.Timestamp)
	}

	// ensure fields copied
	if fv, ok := val.Fields["value"]; !ok {
		t.Fatalf("expected copied Fields to contain 'value' field")
	} else {
		if v, ok := fv.ValueFloat64(); !ok || v != 1.23 {
			t.Fatalf("expected copied field value 1.23; got %v", fv)
		}
	}
}
