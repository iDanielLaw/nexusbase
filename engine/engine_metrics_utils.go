package engine

import (
	"expvar"
	"fmt"
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

	// For a cumulative histogram, a value that fits in a smaller bucket
	// must also be counted in all larger buckets.
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
	// All finite observations are less than +Inf.
	if infVar := histMap.Get("le_inf"); infVar != nil {
		if infInt, ok := infVar.(*expvar.Int); ok {
			infInt.Add(1)
		}
	}
}

// publishExpvarInt safely publishes an expvar.Int.
// If the name already exists and is an *expvar.Int, it resets it and returns it.
// If the name exists but is not an *expvar.Int, it panics.
// If the name does not exist, it creates and returns a new one.
func publishExpvarInt(name string) *expvar.Int {
	v := expvar.Get(name)
	if v == nil {
		return expvar.NewInt(name)
	}
	if iv, ok := v.(*expvar.Int); ok {
		iv.Set(0) // Reset existing
		return iv
	}
	panic(fmt.Sprintf("expvar: trying to publish Int %s but variable already exists with different type %T", name, v))
}

// publishExpvarFloat safely publishes an expvar.Float.
// Similar logic to publishExpvarInt.
func publishExpvarFloat(name string) *expvar.Float {
	v := expvar.Get(name)
	if v == nil {
		return expvar.NewFloat(name)
	}
	if fv, ok := v.(*expvar.Float); ok {
		fv.Set(0.0) // Reset existing
		return fv
	}
	panic(fmt.Sprintf("expvar: trying to publish Float %s but variable already exists with different type %T", name, v))
}

// publishExpvarFunc safely publishes an expvar.Func.
func publishExpvarFunc(name string, f func() interface{}) {
	// expvar.Publish panics on reuse. Only publish if it doesn't exist.
	if expvar.Get(name) != nil {
		return // Already published
	}
	expvar.Publish(name, expvar.Func(f))
}

// publishExpvarMap safely publishes an expvar.Map.
// If the name already exists and is an *expvar.Map, it returns it.
// The caller (NewEngineMetrics) is responsible for setting/resetting sub-metrics within the map.
// If the name exists but is not an *expvar.Map, it panics.
// If the name does not exist, it creates and returns a new one.
func publishExpvarMap(name string) *expvar.Map {
	v := expvar.Get(name)
	if v == nil {
		return expvar.NewMap(name) // Registers the new map
	}
	if mv, ok := v.(*expvar.Map); ok {
		// Return the existing map. NewEngineMetrics will call .Set on this map
		// for "count", "sum", etc., effectively resetting or ensuring they are correct.
		return mv
	}
	panic(fmt.Sprintf("expvar: trying to publish Map %s but variable already exists with different type %T", name, v))
}
