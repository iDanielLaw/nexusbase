package engine2

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/INLOpen/nexusbase/core"
)

// buildMemtable populates a memtable with `pointsPerTag` timestamps per tagKey,
// inserting timestamps in random order. Deterministic seed is used for repeatability.
func buildMemtable(pointsPerTag int, tagCount int) *Memtable {
	m := NewMemtable()
	metric := "m_bench"
	rand.Seed(42)
	for tk := 0; tk < tagCount; tk++ {
		tags := map[string]string{"host": fmt.Sprintf("h%d", tk)}
		// build sequential timestamps then shuffle
		ts := make([]int64, pointsPerTag)
		for i := 0; i < pointsPerTag; i++ {
			ts[i] = int64(i)
		}
		rand.Shuffle(len(ts), func(i, j int) { ts[i], ts[j] = ts[j], ts[i] })
		for _, t := range ts {
			fv, _ := core.NewFieldValuesFromMap(map[string]interface{}{"v": t})
			dp := &core.DataPoint{Metric: metric, Tags: tags, Timestamp: t, Fields: fv}
			m.Put(dp)
		}
	}
	return m
}

// runQuery executes a Query and ignores the result; used inside benchmarks.
func runQuery(m *Memtable, q core.QueryParams) {
	_ = m.Query(q)
}

func BenchmarkQueryScenarios(b *testing.B) {
	// table-driven: pointsPerTag x tagCount x rangeType
	pointsCases := []int{1000, 10000, 50000}
	tagCases := []int{1, 8}
	ranges := []struct {
		name  string
		start int64
		end   int64
	}{
		{"full", 0, -1},
		{"half", 0, -1},
	}

	for _, pts := range pointsCases {
		for _, tcount := range tagCases {
			// full range uses EndTime = pts-1, half uses half the range
			for _, rg := range ranges {
				rg := rg
				rangeName := rg.name
				endFull := int64(pts - 1)
				var endFullVal int64
				if rangeName == "full" {
					endFullVal = endFull
				} else {
					endFullVal = endFull / 2
				}

				benchName := fmt.Sprintf("Pts%d_Tags%d_Range%s/WithIndex", pts, tcount, rangeName)
				b.Run(benchName, func(b *testing.B) {
					m := buildMemtable(pts, tcount)
					q := core.QueryParams{Metric: "m_bench", Tags: map[string]string{"host": "h0"}, StartTime: 0, EndTime: endFullVal}
					b.ResetTimer()
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						runQuery(m, q)
					}
				})

				benchNameNoIdx := fmt.Sprintf("Pts%d_Tags%d_Range%s/NoIndex", pts, tcount, rangeName)
				b.Run(benchNameNoIdx, func(b *testing.B) {
					m := buildMemtable(pts, tcount)
					// remove index to force building & sorting timestamps per query
					m.index = nil
					q := core.QueryParams{Metric: "m_bench", Tags: map[string]string{"host": "h0"}, StartTime: 0, EndTime: endFullVal}
					b.ResetTimer()
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						runQuery(m, q)
					}
				})
			}
		}
	}
}
