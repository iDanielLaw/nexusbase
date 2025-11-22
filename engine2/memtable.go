package engine2

import (
	"sort"
	"strings"
	"sync"

	"github.com/INLOpen/nexusbase/core"
)

// size-bucketed pools for temporary timestamp slices used when building tsList
var (
	// bucket sizes in increasing order
	tsBuckets = []int{16, 64, 256, 1024, 4096, 16384, 65536}
	tsPools   = make([]sync.Pool, len(tsBuckets))
)

func init() {
	for i, b := range tsBuckets {
		cap := b
		// return pointer to slice to reduce allocations (satisfy analyzer)
		tsPools[i] = sync.Pool{New: func() any {
			s := make([]int64, 0, cap)
			return &s
		}}
	}
}

// getTsSlice returns a slice with capacity at least need. The bool return
// indicates whether the slice came from a pool (and should be returned later).
func getTsSlice(need int) ([]int64, bool) {
	for i, b := range tsBuckets {
		if need <= b {
			v := tsPools[i].Get()
			if v == nil {
				s := make([]int64, 0, b)
				return s[:0], true
			}
			p := v.(*[]int64)
			s := *p
			return s[:0], true
		}
	}
	// need larger than largest bucket: try last pool but ensure capacity
	last := len(tsPools) - 1
	v := tsPools[last].Get()
	if v == nil {
		s := make([]int64, 0, need)
		return s[:0], true
	}
	p := v.(*[]int64)
	s := *p
	if cap(s) < need {
		s = make([]int64, 0, need)
	}
	return s[:0], true
}

func putTsSlice(s []int64) {
	capv := cap(s)
	s = s[:0]
	for i, b := range tsBuckets {
		if capv <= b {
			ss := s
			tsPools[i].Put(&ss)
			return
		}
	}
	// larger than last bucket: put into last pool
	ss := s
	tsPools[len(tsPools)-1].Put(&ss)
}

// Memtable is a simple in-memory store keyed by metric|sortedtags|timestamp
// Memtable is a simple in-memory store keyed by metric|sortedtags|timestamp
//
// Snapshot semantics and copy-on-write expectations:
//   - Consumers (iterators) snapshot the memtable by taking references to
//     the `data` and `index` maps under `RLock` and then release the lock.
//     After the lock is released readers may still hold references to inner
//     maps and to the timestamp `[]int64` slices stored in `index`.
//   - To avoid unsafe concurrent access and subtle races, writers must not
//     mutate the underlying arrays or slices in place. Instead, on any
//     modification (insert/remove) writers should allocate new slices or
//     maps and assign them into the parent map (copy-on-write). This keeps
//     previously taken snapshots valid for readers while allowing writers to
//     update state safely.
//
// The implementation relies on this pattern: readers see a consistent
// snapshot without holding locks during iteration; writers allocate fresh
// slices when changing per-tag timestamp lists.
type Memtable struct {
	mu sync.RWMutex
	// nested index: metric -> tagsKey -> timestamp -> FieldValues (nil means tombstone)
	data map[string]map[string]map[int64]core.FieldValues
	// per-metric per-tagKey ordered timestamps for fast range scans
	index map[string]map[string][]int64
}

func NewMemtable() *Memtable {
	return &Memtable{
		data:  make(map[string]map[string]map[int64]core.FieldValues),
		index: make(map[string]map[string][]int64),
	}
}

func tagsKey(tags map[string]string) string {
	if len(tags) == 0 {
		return ""
	}
	// simple deterministic ordering: concatenate key=val pairs sorted by key
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	// sort
	sort.Strings(keys)
	out := ""
	for _, k := range keys {
		out += k + "=" + tags[k] + ";"
	}
	return out
}

// keyFor was removed in favor of the nested index representation.

// parseTagsFromKey parses the tagsKey string back into a map.
func parseTagsFromKey(s string) map[string]string {
	if s == "" {
		return nil
	}
	out := make(map[string]string)
	parts := strings.Split(s, ";")
	for _, p := range parts {
		if p == "" {
			continue
		}
		kv := strings.SplitN(p, "=", 2)
		if len(kv) == 2 {
			out[kv[0]] = kv[1]
		}
	}
	return out
}

func (m *Memtable) Put(dp *core.DataPoint) {
	m.mu.Lock()
	defer m.mu.Unlock()
	metricIdx, ok := m.data[dp.Metric]
	if !ok {
		metricIdx = make(map[string]map[int64]core.FieldValues)
		m.data[dp.Metric] = metricIdx
	}
	tagIdx, ok := metricIdx[tagsKey(dp.Tags)]
	if !ok {
		tagIdx = make(map[int64]core.FieldValues)
		metricIdx[tagsKey(dp.Tags)] = tagIdx
	}
	tagIdx[dp.Timestamp] = dp.Fields

	// maintain index
	metricIndex, ok := m.index[dp.Metric]
	if !ok {
		metricIndex = make(map[string][]int64)
		m.index[dp.Metric] = metricIndex
	}
	tkey := tagsKey(dp.Tags)
	tsList := metricIndex[tkey]
	// insert ts in sorted order if not present
	i := sort.Search(len(tsList), func(i int) bool { return tsList[i] >= dp.Timestamp })
	if i < len(tsList) && tsList[i] == dp.Timestamp {
		// already present
	} else {
		// create a new slice and copy elements to avoid mutating any shared
		// underlying array that may be referenced by readers (snapshot).
		newList := make([]int64, len(tsList)+1)
		copy(newList, tsList[:i])
		newList[i] = dp.Timestamp
		copy(newList[i+1:], tsList[i:])
		metricIndex[tkey] = newList
	}
}

// Delete marks a single point as deleted by storing a nil FieldValues.
func (m *Memtable) Delete(metric string, tags map[string]string, ts int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	metricIdx, ok := m.data[metric]
	if !ok {
		metricIdx = make(map[string]map[int64]core.FieldValues)
		m.data[metric] = metricIdx
	}
	tkey := tagsKey(tags)
	tagIdx, ok := metricIdx[tkey]
	if !ok {
		tagIdx = make(map[int64]core.FieldValues)
		metricIdx[tkey] = tagIdx
	}
	tagIdx[ts] = nil

	// remove from index as well
	if metricIndex, ok := m.index[metric]; ok {
		if tsList, ok := metricIndex[tkey]; ok {
			i := sort.Search(len(tsList), func(i int) bool { return tsList[i] >= ts })
			if i < len(tsList) && tsList[i] == ts {
				// build a new slice without the removed element to avoid
				// mutating an underlying array that readers may hold.
				newList := make([]int64, 0, len(tsList)-1)
				newList = append(newList, tsList[:i]...)
				newList = append(newList, tsList[i+1:]...)
				metricIndex[tkey] = newList
			}
		}
	}
}

// DeleteSeries removes all entries for a metric+tags combination.
func (m *Memtable) DeleteSeries(metric string, tags map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	metricIdx, ok := m.data[metric]
	if !ok {
		metricIdx = make(map[string]map[int64]core.FieldValues)
		m.data[metric] = metricIdx
	}
	tkey := tagsKey(tags)
	tagIdx, ok := metricIdx[tkey]
	if !ok {
		tagIdx = make(map[int64]core.FieldValues)
		metricIdx[tkey] = tagIdx
	}
	// series tombstone marker
	tagIdx[-1] = nil

	// mark series tombstone in index
	if metricIndex, ok := m.index[metric]; ok {
		metricIndex[tkey] = []int64{-1}
	} else {
		metricIndex = make(map[string][]int64)
		metricIndex[tkey] = []int64{-1}
		m.index[metric] = metricIndex
	}
}

func (m *Memtable) Get(metric string, tags map[string]string, ts int64) (core.FieldValues, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if metricIdx, ok := m.data[metric]; ok {
		if tagIdx, ok := metricIdx[tagsKey(tags)]; ok {
			fv, ok := tagIdx[ts]
			return fv, ok
		}
	}
	return nil, false
}

func (m *Memtable) Flush() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string]map[string]map[int64]core.FieldValues)
	m.index = make(map[string]map[string][]int64)
}

// Query returns matching QueryResultItems for the provided params from the memtable.
func (m *Memtable) Query(params core.QueryParams) []*core.QueryResultItem {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*core.QueryResultItem
	metricIdx, ok := m.data[params.Metric]
	if !ok {
		return out
	}
	metricIndex := m.index[params.Metric]

	// Try to approximate total number of timestamps to preallocate output slice
	approxTotal := 0
	if metricIndex != nil {
		for tagKey := range metricIdx {
			approxTotal += len(metricIndex[tagKey])
		}
	} else {
		for _, tagIdx := range metricIdx {
			approxTotal += len(tagIdx)
		}
	}
	if approxTotal > 0 {
		out = make([]*core.QueryResultItem, 0, approxTotal)
	}

	for tagKey, tagIdx := range metricIdx {
		tags := parseTagsFromKey(tagKey)
		// verify tag filters
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
		// if a series tombstone exists, skip this tagKey entirely
		if _, hasSeriesTomb := tagIdx[-1]; hasSeriesTomb {
			continue
		}

		// get ordered timestamps for this tagKey
		var tsList []int64
		if metricIndex != nil {
			tsList = metricIndex[tagKey]
		}
		// if index is not present for this tagKey, build it from tagIdx keys
		builtFromPool := false
		if len(tsList) == 0 {
			// try to get a slice from size-bucketed pools
			s, _ := getTsSlice(len(tagIdx))
			if cap(s) < len(tagIdx) {
				s = make([]int64, 0, len(tagIdx))
			}
			tsList = s
			builtFromPool = true
			for ts := range tagIdx {
				if ts == -1 {
					continue
				}
				tsList = append(tsList, ts)
			}
			sort.Slice(tsList, func(i, j int) bool { return tsList[i] < tsList[j] })
		}

		// if tsList contains the series tombstone marker [-1], skip
		if len(tsList) == 1 && tsList[0] == -1 {
			continue
		}

		// reuse tagIdx local variable to avoid map lookup inside loop
		localTagIdx := tagIdx

		// iterate timestamps in order
		for _, ts := range tsList {
			if ts < params.StartTime || ts > params.EndTime {
				continue
			}
			if fv, ok := localTagIdx[ts]; ok {
				if fv == nil {
					continue
				}
				out = append(out, &core.QueryResultItem{Metric: params.Metric, Tags: tags, Timestamp: ts, Fields: fv})
			}
		}
		if builtFromPool {
			// return the slice to the appropriate pool for reuse
			putTsSlice(tsList)
		}
	}
	return out
}
