package engine2

import (
	"sort"

	"github.com/INLOpen/nexusbase/core"
)

// memQueryIterator is a streaming iterator that walks a memtable snapshot and
// emits QueryResultItem values on demand without materializing the full result
// set. It reuses a single QueryResultItem for At() (valid until Next()).
type memQueryIterator struct {
	params      core.QueryParams
	metric      string
	metricIdx   map[string]map[int64]core.FieldValues
	metricIndex map[string][]int64

	tagKeys    []string
	tagPos     int
	tsList     []int64
	tsPos      int
	builtFromP bool

	curr core.QueryResultItem
	err  error
}

// newStreamingMemQueryIterator snapshots the memtable maps under RLock and
// returns an iterator that will produce results from that snapshot.
func newStreamingMemQueryIterator(m *Memtable, params core.QueryParams) *memQueryIterator {
	m.mu.RLock()
	metricIdx, _ := m.data[params.Metric]
	// copy metricIdx reference (shallow) and metricIndex slice map
	var metricIndex map[string][]int64
	if m.index != nil {
		if mi, ok := m.index[params.Metric]; ok {
			metricIndex = mi
		}
	}
	// collect tagKeys that match filters and are not series-tombstoned
	tagKeys := make([]string, 0, len(metricIdx))
	for tagKey, tagIdx := range metricIdx {
		if _, hasSeriesTomb := tagIdx[-1]; hasSeriesTomb {
			continue
		}
		// quick tag filter check
		tags := parseTagsFromKey(tagKey)
		match := true
		for qk, qv := range params.Tags {
			if tv, ok := tags[qk]; !ok || tv != qv {
				match = false
				break
			}
		}
		if !match {
			continue
		}
		tagKeys = append(tagKeys, tagKey)
	}
	m.mu.RUnlock()

	it := &memQueryIterator{
		params:      params,
		metric:      params.Metric,
		metricIdx:   metricIdx,
		metricIndex: metricIndex,
		tagKeys:     tagKeys,
		tagPos:      0,
		tsList:      nil,
		tsPos:       0,
		curr:        core.QueryResultItem{},
	}
	return it
}

func (it *memQueryIterator) Next() bool {
	// loop until we find a valid item or exhaust tagKeys
	for {
		// if we have a tsList and position, try to emit
		if it.tsList != nil && it.tsPos < len(it.tsList) {
			ts := it.tsList[it.tsPos]
			it.tsPos++
			if ts < it.params.StartTime || ts > it.params.EndTime {
				continue
			}
			if tagIdx, ok := it.metricIdx[it.tagKeys[it.tagPos-1]]; ok {
				if fv, ok := tagIdx[ts]; ok {
					if fv == nil || len(fv) == 0 {
						continue
					}
					// populate curr and return
					it.curr.Metric = it.params.Metric
					it.curr.Tags = parseTagsFromKey(it.tagKeys[it.tagPos-1])
					it.curr.Timestamp = ts
					it.curr.Fields = fv
					return true
				}
			}
			continue
		}

		// advance to next tagKey
		if it.tagPos >= len(it.tagKeys) {
			return false
		}
		tagKey := it.tagKeys[it.tagPos]
		it.tagPos++
		// prepare tsList for this tagKey
		if it.metricIndex != nil {
			it.tsList = it.metricIndex[tagKey]
			it.builtFromP = false
		} else {
			// build tsList from tagIdx (use pooled slice)
			tagIdx := it.metricIdx[tagKey]
			s, _ := getTsSlice(len(tagIdx))
			if cap(s) < len(tagIdx) {
				s = make([]int64, 0, len(tagIdx))
			}
			for ts := range tagIdx {
				if ts == -1 {
					continue
				}
				s = append(s, ts)
			}
			sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
			it.tsList = s
			it.builtFromP = true
		}
		it.tsPos = 0
		// loop continues to emit from tsList
	}
}

func (it *memQueryIterator) At() (*core.QueryResultItem, error) {
	return &it.curr, nil
}

func (it *memQueryIterator) Error() error { return it.err }

func (it *memQueryIterator) Close() error {
	// return pooled tsList if built
	if it.builtFromP && it.tsList != nil {
		putTsSlice(it.tsList)
		it.tsList = nil
	}
	return nil
}

func (it *memQueryIterator) Put(v *core.QueryResultItem) {
	// no-op: we don't pool QueryResultItem instances here
}

func (it *memQueryIterator) UnderlyingAt() (*core.IteratorNode, error) { return nil, nil }
