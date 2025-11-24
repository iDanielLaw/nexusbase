package engine2

import (
	"bytes"
	"expvar"
	"fmt"

	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexusbase/sys"
)

// latencyBuckets defines the buckets for latency histograms (in seconds).
var latencyBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0}

// observeLatency records the duration in the provided histogram map.
func observeLatency(histMap *expvar.Map, durationSeconds float64) {
	if histMap == nil {
		return
	}
	if countVar := histMap.Get("count"); countVar != nil {
		if countInt, ok := countVar.(*expvar.Int); ok {
			countInt.Add(1)
		}
	}
	if sumVar := histMap.Get("sum"); sumVar != nil {
		if sumFloat, ok := sumVar.(*expvar.Float); ok {
			sumFloat.Add(durationSeconds)
		}
	}
	for _, b := range latencyBuckets {
		bucketName := fmt.Sprintf("le_%.3f", b)
		if durationSeconds <= b {
			if bucketVar := histMap.Get(bucketName); bucketVar != nil {
				if bucketInt, ok := bucketVar.(*expvar.Int); ok {
					bucketInt.Add(1)
				}
			}
		}
	}
	if infVar := histMap.Get("le_inf"); infVar != nil {
		if infInt, ok := infVar.(*expvar.Int); ok {
			infInt.Add(1)
		}
	}
}

// realFileRemover is the production implementation of core.FileRemover.
type realFileRemover struct{}

func (r *realFileRemover) Remove(name string) error { return sys.Remove(name) }

// getOverallKeyRange returns the min and max keys across a slice of SSTables.
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
