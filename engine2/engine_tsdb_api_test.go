package engine2

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageEngine_PutAndGet(t *testing.T) {
	shim := newStorageEngineShim(t.TempDir())
	require.NoError(t, shim.Start())
	defer shim.Close()

	metric := "cpu.temp"
	tags := map[string]string{"host": "serverA"}
	ts := time.Now().UnixNano()
	dp, err := core.NewSimpleDataPoint(metric, tags, ts, map[string]interface{}{"value": 3.14})
	require.NoError(t, err)

	require.NoError(t, shim.Put(context.Background(), *dp))

	fv, err := shim.Get(context.Background(), metric, tags, ts)
	require.NoError(t, err)
	v, ok := fv["value"].ValueFloat64()
	require.True(t, ok)
	assert.InDelta(t, 3.14, v, 1e-9)

	// Delete and verify not found
	require.NoError(t, shim.Delete(context.Background(), metric, tags, ts))
	_, err = shim.Get(context.Background(), metric, tags, ts)
	assert.ErrorIs(t, err, sstable.ErrNotFound)
}

func TestStorageEngine_GetSeriesByTags(t *testing.T) {
	shim := newStorageEngineShim(t.TempDir())
	require.NoError(t, shim.Start())
	defer shim.Close()

	type tp struct {
		metric string
		tags   map[string]string
		ts     int64
		val    float64
		key    string
	}

	points := []tp{
		{metric: "cpu.usage", tags: map[string]string{"host": "serverA", "region": "us-east"}, ts: 1000, val: 50.0, key: string(core.EncodeSeriesKeyWithString("cpu.usage", map[string]string{"host": "serverA", "region": "us-east"}))},
		{metric: "cpu.usage", tags: map[string]string{"host": "serverB", "region": "us-east"}, ts: 1001, val: 60.0, key: string(core.EncodeSeriesKeyWithString("cpu.usage", map[string]string{"host": "serverB", "region": "us-east"}))},
		{metric: "memory.free", tags: map[string]string{"host": "serverA", "region": "us-west"}, ts: 1002, val: 1024.0, key: string(core.EncodeSeriesKeyWithString("memory.free", map[string]string{"host": "serverA", "region": "us-west"}))},
		{metric: "network.in", tags: map[string]string{"host": "serverC", "env": "prod"}, ts: 1003, val: 100.0, key: string(core.EncodeSeriesKeyWithString("network.in", map[string]string{"host": "serverC", "env": "prod"}))},
	}

	for _, p := range points {
		dp, err := core.NewSimpleDataPoint(p.metric, p.tags, p.ts, map[string]interface{}{"value": p.val})
		require.NoError(t, err)
		require.NoError(t, shim.Put(context.Background(), *dp))
	}

	tests := []struct {
		name     string
		metric   string
		tags     map[string]string
		expected []string
	}{
		{name: "metric cpu.usage", metric: "cpu.usage", tags: nil, expected: []string{points[0].key, points[1].key}},
		{name: "metric memory.free", metric: "memory.free", tags: nil, expected: []string{points[2].key}},
		{name: "tag host=serverA", metric: "", tags: map[string]string{"host": "serverA"}, expected: []string{points[0].key, points[2].key}},
		{name: "tag region=us-east", metric: "", tags: map[string]string{"region": "us-east"}, expected: []string{points[0].key, points[1].key}},
		{name: "metric+tag cpu.usage region=us-east", metric: "cpu.usage", tags: map[string]string{"region": "us-east"}, expected: []string{points[0].key, points[1].key}},
		{name: "non-existent metric", metric: "no.such", tags: nil, expected: []string{}},
		{name: "non-existent tag", metric: "", tags: map[string]string{"no": "tag"}, expected: []string{}},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			keys, err := shim.GetSeriesByTags(tc.metric, tc.tags)
			require.NoError(t, err)
			if keys == nil {
				keys = []string{}
			}
			if tc.expected == nil {
				tc.expected = []string{}
			}
			sort.Strings(keys)
			sort.Strings(tc.expected)
			if !reflect.DeepEqual(keys, tc.expected) {
				t.Fatalf("mismatch for %s: got %v want %v", tc.name, keys, tc.expected)
			}
		})
	}

	// Delete one series and ensure it's removed
	require.NoError(t, shim.DeleteSeries(context.Background(), points[0].metric, points[0].tags))
	keys, err := shim.GetSeriesByTags(points[0].metric, points[0].tags)
	require.NoError(t, err)
	assert.Len(t, keys, 0)
}

func TestStorageEngine_Query_BasicAndMultiSeries(t *testing.T) {
	shim := newStorageEngineShim(t.TempDir())
	require.NoError(t, shim.Start())
	defer shim.Close()

	metric := "temp.cpu"
	tags := map[string]string{"dc": "nyc"}

	ts1 := time.Now().UnixNano()
	ts2 := ts1 + 1000
	ts3 := ts1 + 2000

	dp1, _ := core.NewSimpleDataPoint(metric, tags, ts1, map[string]interface{}{"value": 25.0})
	dp2, _ := core.NewSimpleDataPoint(metric, tags, ts2, map[string]interface{}{"value": 26.5})
	dp3, _ := core.NewSimpleDataPoint(metric, tags, ts3, map[string]interface{}{"value": 27.0})
	require.NoError(t, shim.Put(context.Background(), *dp1))
	require.NoError(t, shim.Put(context.Background(), *dp2))
	require.NoError(t, shim.Put(context.Background(), *dp3))

	// Query that should only return ts2
	qp := core.QueryParams{Metric: metric, Tags: tags, StartTime: ts1 + 500, EndTime: ts3 - 500}
	iter, err := shim.Query(context.Background(), qp)
	require.NoError(t, err)
	defer iter.Close()

	var items []*core.QueryResultItem
	for iter.Next() {
		it, err := iter.At()
		require.NoError(t, err)
		items = append(items, it)
	}
	require.NoError(t, iter.Error())
	require.Len(t, items, 1)
	v, ok := items[0].Fields["value"].ValueFloat64()
	require.True(t, ok)
	assert.InDelta(t, 26.5, v, 1e-9)

	// Multi-series query: metric with tag subset region=east
	metric2 := "multi.query"
	tagsA := map[string]string{"region": "east", "host": "A"}
	tagsB := map[string]string{"region": "east", "host": "B"}
	tagsC := map[string]string{"region": "west", "host": "C"}

	dpA1, err := core.NewSimpleDataPoint(metric2, tagsA, 100, map[string]interface{}{"value": 10.0})
	require.NoError(t, err)
	require.NoError(t, shim.Put(context.Background(), *dpA1))
	dpA2, err := core.NewSimpleDataPoint(metric2, tagsA, 300, map[string]interface{}{"value": 30.0})
	require.NoError(t, err)
	require.NoError(t, shim.Put(context.Background(), *dpA2))
	dpB1, err := core.NewSimpleDataPoint(metric2, tagsB, 150, map[string]interface{}{"value": 15.0})
	require.NoError(t, err)
	require.NoError(t, shim.Put(context.Background(), *dpB1))
	dpB2, err := core.NewSimpleDataPoint(metric2, tagsB, 250, map[string]interface{}{"value": 25.0})
	require.NoError(t, err)
	require.NoError(t, shim.Put(context.Background(), *dpB2))
	dpC1, err := core.NewSimpleDataPoint(metric2, tagsC, 200, map[string]interface{}{"value": 20.0})
	require.NoError(t, err)
	require.NoError(t, shim.Put(context.Background(), *dpC1))

	iter2, err := shim.Query(context.Background(), core.QueryParams{Metric: metric2, Tags: map[string]string{"region": "east"}, StartTime: 0, EndTime: 1000})
	require.NoError(t, err)
	defer iter2.Close()

	var actual []struct {
		ts  int64
		val float64
	}
	for iter2.Next() {
		it, err := iter2.At()
		require.NoError(t, err)
		fv, ok := it.Fields["value"]
		require.True(t, ok)
		v, ok := fv.ValueFloat64()
		require.True(t, ok)
		actual = append(actual, struct {
			ts  int64
			val float64
		}{ts: it.Timestamp, val: v})
	}
	require.NoError(t, iter2.Error())

	expected := []struct {
		ts  int64
		val float64
	}{
		{100, 10.0}, {150, 15.0}, {250, 25.0}, {300, 30.0},
	}
	require.Len(t, actual, len(expected))
	for i := range expected {
		assert.Equal(t, expected[i].ts, actual[i].ts)
		assert.InDelta(t, expected[i].val, actual[i].val, 1e-9)
	}
}

func TestStorageEngine_Query_FromMemtableAndSSTable(t *testing.T) {
	shim := newStorageEngineShim(t.TempDir())
	require.NoError(t, shim.Start())
	defer shim.Close()

	metric := "test.query.mixed.source"
	tags := map[string]string{"host": "mixed-host"}

	// Phase 1
	dp1a, err := core.NewSimpleDataPoint(metric, tags, 100, map[string]interface{}{"value": 10.0})
	require.NoError(t, err)
	require.NoError(t, shim.Put(context.Background(), *dp1a))
	dp1b, err := core.NewSimpleDataPoint(metric, tags, 300, map[string]interface{}{"value": 30.0})
	require.NoError(t, err)
	require.NoError(t, shim.Put(context.Background(), *dp1b))

	// Force flush is a no-op in shim, but call for compatibility
	require.NoError(t, shim.ForceFlush(context.Background(), true))

	// Phase 2 (remain in memtable)
	dp2a, err := core.NewSimpleDataPoint(metric, tags, 50, map[string]interface{}{"value": 5.0})
	require.NoError(t, err)
	require.NoError(t, shim.Put(context.Background(), *dp2a))
	dp2b, err := core.NewSimpleDataPoint(metric, tags, 200, map[string]interface{}{"value": 20.0})
	require.NoError(t, err)
	require.NoError(t, shim.Put(context.Background(), *dp2b))

	iter, err := shim.Query(context.Background(), core.QueryParams{Metric: metric, Tags: tags, StartTime: 0, EndTime: 1000})
	require.NoError(t, err)
	defer iter.Close()

	var res []*core.QueryResultItem
	for iter.Next() {
		it, err := iter.At()
		require.NoError(t, err)
		res = append(res, it)
	}
	require.NoError(t, iter.Error())

	expected := []struct {
		ts  int64
		val float64
	}{{50, 5.0}, {100, 10.0}, {200, 20.0}, {300, 30.0}}
	require.Len(t, res, len(expected))
	for i, exp := range expected {
		assert.Equal(t, exp.ts, res[i].Timestamp)
		v, ok := res[i].Fields["value"].ValueFloat64()
		require.True(t, ok)
		assert.InDelta(t, exp.val, v, 1e-9)
	}
}

func TestStorageEngine_PutBatch(t *testing.T) {
	shim := newStorageEngineShim(t.TempDir())
	require.NoError(t, shim.Start())
	defer shim.Close()

	points := []core.DataPoint{}
	for i := 0; i < 4; i++ {
		dp, err := core.NewSimpleDataPoint("batch.metric", map[string]string{"host": "h"}, int64(100+i), map[string]interface{}{"value": float64(10 + i)})
		require.NoError(t, err)
		points = append(points, *dp)
	}
	require.NoError(t, shim.PutBatch(context.Background(), points))

	for _, p := range points {
		fv, err := shim.Get(context.Background(), p.Metric, p.Tags, p.Timestamp)
		require.NoError(t, err)
		v, ok := fv["value"].ValueFloat64()
		require.True(t, ok)
		// ensure value roughly equals
		exp, ok2 := p.Fields["value"].ValueFloat64()
		require.True(t, ok2)
		assert.InDelta(t, exp, v, 1e-9)
	}
}

func TestStorageEngine_DeleteSeries(t *testing.T) {
	shim := newStorageEngineShim(t.TempDir())
	require.NoError(t, shim.Start())
	defer shim.Close()

	metric := "test.delete.series"
	tags := map[string]string{"env": "dev"}

	dp1, err := core.NewSimpleDataPoint(metric, tags, 1, map[string]interface{}{"value": 1.0})
	require.NoError(t, err)
	dp2, err := core.NewSimpleDataPoint(metric, tags, 2, map[string]interface{}{"value": 2.0})
	require.NoError(t, err)
	require.NoError(t, shim.Put(context.Background(), *dp1))
	require.NoError(t, shim.Put(context.Background(), *dp2))

	require.NoError(t, shim.DeleteSeries(context.Background(), metric, tags))

	_, err = shim.Get(context.Background(), metric, tags, 1)
	assert.ErrorIs(t, err, sstable.ErrNotFound)

	keys, err := shim.GetSeriesByTags(metric, tags)
	require.NoError(t, err)
	assert.Len(t, keys, 0)

	iter, err := shim.Query(context.Background(), core.QueryParams{Metric: metric, Tags: tags, StartTime: 0, EndTime: time.Now().UnixNano()})
	require.NoError(t, err)
	defer iter.Close()
	assert.False(t, iter.Next())
}

func TestStorageEngine_DeletesByTimeRange(t *testing.T) {
	shim := newStorageEngineShim(t.TempDir())
	require.NoError(t, shim.Start())
	defer shim.Close()

	metric := "sensor.temp"
	tags := map[string]string{"location": "room1"}

	timestamps := []int64{100, 200, 300, 400, 500}
	vals := []float64{20.0, 20.5, 21.0, 21.5, 22.0}
	for i, ts := range timestamps {
		dp, err := core.NewSimpleDataPoint(metric, tags, ts, map[string]interface{}{"value": vals[i]})
		require.NoError(t, err)
		require.NoError(t, shim.Put(context.Background(), *dp))
	}

	// Delete ts 200..400
	require.NoError(t, shim.DeletesByTimeRange(context.Background(), metric, tags, 200, 400))

	expected := map[int64]bool{100: true, 200: false, 300: false, 400: false, 500: true}
	for ts, should := range expected {
		_, err := shim.Get(context.Background(), metric, tags, ts)
		if should {
			require.NoError(t, err)
		} else {
			assert.ErrorIs(t, err, sstable.ErrNotFound)
		}
	}
}
