package memtable

import (
	"fmt"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexuscore/utils/clock"
)

// Benchmark that inserts many entries into a Memtable2, comparing fresh
// allocations (similar to current code) vs using the existing pools.

func makeKey(i int) []byte {
	// simple synthetic key
	return []byte{byte(i), byte(i >> 8), byte(i >> 16)}
}

func makeValue(i int) []byte {
	v := make([]byte, 16)
	for j := range v {
		v[j] = byte(i + j)
	}
	return v
}

func BenchmarkPut_Fresh(b *testing.B) {
	m := NewMemtable2(1<<30, clock.SystemClockDefault)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := makeKey(i)
		val := makeValue(i)
		// mimic PutRaw fresh-allocation path
		newKey := &MemtableKey{Key: key, PointID: uint64(i)}
		newEntry := &MemtableEntry{Key: key}
		vc := make([]byte, len(val))
		copy(vc, val)
		newEntry.Value = vc
		newEntry.EntryType = core.EntryTypePutEvent
		newEntry.PointID = uint64(i)
		m.data.Insert(newKey, newEntry)
	}
}

func BenchmarkPut_Pooled(b *testing.B) {
	m := NewMemtable2(1<<30, clock.SystemClockDefault)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := makeKey(i)
		val := makeValue(i)
		// mimic pooled path: get objects from pools and return them
		k := KeyPool.Get()
		k.Key = key
		k.PointID = uint64(i)
		e := EntryPool.Get()
		e.Key = key
		vc := make([]byte, len(val))
		copy(vc, val)
		e.Value = vc
		e.EntryType = core.EntryTypePutEvent
		e.PointID = uint64(i)
		m.data.Insert(k, e)
		// for benchmark fairness, do not return k/e to pool immediately
	}
}

func BenchmarkPut_DelayedPools(b *testing.B) {
	delays := []int{0, 10, 50, 250, 1000} // milliseconds
	for _, d := range delays {
		name := fmt.Sprintf("Delayed_%dms", d)
		b.Run(name, func(sb *testing.B) {
			// replace global EntryPool with a delayed pool using given delay
			old := EntryPool
			dp := newDelayedEntryPoolWithDelay(16384, time.Duration(d)*time.Millisecond)
			EntryPool = dp
			defer func() {
				// restore and stop the temp pool
				EntryPool = old
				dp.Stop()
			}()

			m := NewMemtable2(1<<30, clock.SystemClockDefault)
			sb.ResetTimer()
			for i := 0; i < sb.N; i++ {
				key := makeKey(i)
				val := makeValue(i)
				k := KeyPool.Get()
				k.Key = key
				k.PointID = uint64(i)
				e := EntryPool.Get()
				e.Key = key
				vc := make([]byte, len(val))
				copy(vc, val)
				e.Value = vc
				e.EntryType = core.EntryTypePutEvent
				e.PointID = uint64(i)
				m.data.Insert(k, e)
			}
		})
	}
}
