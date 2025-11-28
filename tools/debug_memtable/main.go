package main

import (
	"encoding/binary"
	"fmt"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/memtable"
	"github.com/INLOpen/nexuscore/utils/clock"
)

func main() {
	m := memtable.NewMemtable2(1<<30, clock.SystemClockDefault)
	metric := "m1"
	tags := map[string]string{"host": "a"}

	fv1, _ := core.NewFieldValuesFromMap(map[string]interface{}{"v": 3})
	fv2, _ := core.NewFieldValuesFromMap(map[string]interface{}{"v": 1})
	fv3, _ := core.NewFieldValuesFromMap(map[string]interface{}{"v": 2})
	fv4, _ := core.NewFieldValuesFromMap(map[string]interface{}{"v": 4})

	dps := []*core.DataPoint{
		{Metric: metric, Tags: tags, Timestamp: 300, Fields: fv1},
		{Metric: metric, Tags: tags, Timestamp: 100, Fields: fv2},
		{Metric: metric, Tags: tags, Timestamp: 200, Fields: fv3},
		{Metric: metric, Tags: tags, Timestamp: 400, Fields: fv4},
	}

	for _, dp := range dps {
		if err := m.Put(dp); err != nil {
			panic(err)
		}
	}

	iter := m.NewIterator(nil, nil, 0)
	defer iter.Close()

	for iter.Next() {
		node, _ := iter.At()
		k := node.Key
		if len(k) >= 8 {
			ts := int64(binary.BigEndian.Uint64(k[len(k)-8:]))
			seriesKey := k[:len(k)-8]
			metricName, tagsMap, derr := core.ExtractMetricAndTagsFromSeriesKeyWithString(seriesKey)
			fmt.Printf("Key len=%d ts=%d metric=%s tags=%v entryType=%v value=%v decodeErr=%v\n", len(k), ts, metricName, tagsMap, node.EntryType, node.Value, derr)
		}
	}
	if err := iter.Error(); err != nil {
		fmt.Printf("iter err: %v\n", err)
	}
}
