package engine

import (
	"bytes"
	"math"
	"reflect"
	"sort"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/sstable"
)

const epsilon = 1e-9

// Helper to sort string slices for consistent comparison
func sortAndCompare(a, b []string) bool {
	sort.Strings(a)
	sort.Strings(b)
	return reflect.DeepEqual(a, b)
}

func getOverallKeyRange(tables []*sstable.SSTable) (minKey, maxKey []byte) {
	if len(tables) == 0 {
		return nil, nil
	}
	minKey = tables[0].MinKey()
	maxKey = tables[0].MaxKey()
	for i := 1; i < len(tables); i++ {
		if bytes.Compare(tables[i].MinKey(), minKey) < 0 {
			minKey = tables[i].MinKey()
		}
		if bytes.Compare(tables[i].MaxKey(), maxKey) > 0 {
			maxKey = tables[i].MaxKey()
		}
	}
	return minKey, maxKey
}

// ApproximatelyEqual checks if two floats are close enough to be considered equal.
// It uses a combination of absolute and relative tolerance.
func ApproximatelyEqual(a, b float64) bool {
	// 1. ตรวจสอบค่าสัมบูรณ์สำหรับตัวเลขที่ใกล้ศูนย์มากๆ
	if math.Abs(a-b) < epsilon {
		return true
	}
	// 2. ตรวจสอบค่าสัมพัทธ์สำหรับตัวเลขขนาดอื่นๆ
	// เพื่อหลีกเลี่ยงการหารด้วยศูนย์เมื่อ a หรือ b เป็น 0
	if b == 0 {
		return math.Abs(a) < epsilon
	}
	return (math.Abs(a-b) / math.Abs(b)) < epsilon
}

// latencyObservingIterator wraps an iterator to observe latency upon Close.
type LatencyObservingIterator struct {
	core.Interface
	engine             *storageEngine
	startTime          time.Time
	aggregations       []string
	underlyingIterator core.Interface
}

func NewLatencyObservingIterator(iter core.Interface, engine *storageEngine, startTime time.Time, aggregations []string) core.Interface {
	return &LatencyObservingIterator{
		Interface:          iter,
		engine:             engine,
		startTime:          startTime,
		aggregations:       aggregations,
		underlyingIterator: iter,
	}
}

func (it *LatencyObservingIterator) Close() error {
	clock := it.engine.GetClock()
	if len(it.aggregations) > 0 && it.engine.metrics.AggregationQueryLatencyHist != nil {
		duration := clock.Now().Sub(it.startTime).Seconds()
		observeLatency(it.engine.metrics.AggregationQueryLatencyHist, duration)
	}
	return it.underlyingIterator.Close()
}
