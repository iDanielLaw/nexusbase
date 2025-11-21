package engine2

import (
	"sort"
	"strings"
	"sync"

	"github.com/INLOpen/nexusbase/core"
)

// Memtable is a simple in-memory store keyed by metric|sortedtags|timestamp
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
		tsList = append(tsList, 0)
		copy(tsList[i+1:], tsList[i:])
		tsList[i] = dp.Timestamp
		metricIndex[tkey] = tsList
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
				tsList = append(tsList[:i], tsList[i+1:]...)
				metricIndex[tkey] = tsList
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
		if len(tsList) == 0 {
			tsList = make([]int64, 0, len(tagIdx))
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
	}
	return out
}
