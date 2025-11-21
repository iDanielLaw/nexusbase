package engine2

import (
	"context"
	"testing"

	"github.com/INLOpen/nexusbase/core"
)

func TestQueryAndDeletes(t *testing.T) {
	dir := t.TempDir()
	eng, err := NewEngine2(dir)
	if err != nil {
		t.Fatalf("NewEngine2 failed: %v", err)
	}
	ad := NewEngine2Adapter(eng)
	defer ad.Close()

	// insert points
	for i := int64(0); i < 5; i++ {
		fv, _ := core.NewFieldValuesFromMap(map[string]interface{}{"v": i})
		dp := core.DataPoint{Metric: "m", Tags: map[string]string{"k": "v"}, Timestamp: 1000 + i, Fields: fv}
		if err := ad.Put(context.Background(), dp); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query time range
	qp := core.QueryParams{Metric: "m", StartTime: 1000, EndTime: 1004, Tags: map[string]string{"k": "v"}}
	it, err := ad.Query(context.Background(), qp)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	count := 0
	for it.Next() {
		itm, err := it.At()
		if err != nil {
			t.Fatalf("iterator At failed: %v", err)
		}
		if itm.Timestamp < 1000 || itm.Timestamp > 1004 {
			t.Fatalf("unexpected timestamp %d", itm.Timestamp)
		}
		count++
	}
	if count != 5 {
		t.Fatalf("expected 5 results, got %d", count)
	}

	// delete a point
	if err := ad.Delete(context.Background(), "m", map[string]string{"k": "v"}, 1002); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	// query again
	it2, _ := ad.Query(context.Background(), qp)
	cnt2 := 0
	for it2.Next() {
		itm, _ := it2.At()
		if itm.Timestamp == 1002 {
			t.Fatalf("deleted timestamp returned")
		}
		cnt2++
	}
	if cnt2 != 4 {
		t.Fatalf("expected 4 results after delete, got %d", cnt2)
	}

	// delete series
	if err := ad.DeleteSeries(context.Background(), "m", map[string]string{"k": "v"}); err != nil {
		t.Fatalf("DeleteSeries failed: %v", err)
	}
	it3, _ := ad.Query(context.Background(), qp)
	if it3.Next() {
		t.Fatalf("expected no results after DeleteSeries")
	}
}
