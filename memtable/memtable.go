package memtable

// Deprecated: the legacy Memtable implementation has been replaced by
// Memtable2. This file provides a thin wrapper type, `Memtable`, that
// delegates to `Memtable2` to preserve the legacy API surface.

import (
	"encoding/binary"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexuscore/types"
	"github.com/INLOpen/nexuscore/utils/clock"
)

// Memtable is a compatibility wrapper around Memtable2. Methods on this
// wrapper delegate to the underlying Memtable2 instance. This avoids
// method name collisions while preserving the legacy surface.
type Memtable struct {
	inner *Memtable2
}

// NewMemtable constructs a legacy-style Memtable that delegates to Memtable2.
func NewMemtable(threshold int64, clk clock.Clock) *Memtable {
	return &Memtable{inner: NewMemtable2(threshold, clk)}
}

// Put delegates the legacy raw-put signature to the underlying Memtable2.PutRaw.
func (m *Memtable) Put(key, value []byte, entryType core.EntryType, pointID uint64) error {
	return m.inner.PutRaw(key, value, entryType, pointID)
}

// PutDataPoint delegates DataPoint-centric puts to Memtable2.Put.
func (m *Memtable) PutDataPoint(dp *core.DataPoint) error {
	return m.inner.Put(dp)
}

// DeleteSeries removes an entire series by writing a tombstone with timestamp -1.
func (m *Memtable) DeleteSeries(metric string, tags map[string]string) error {
	key := core.EncodeTSDBKeyWithString(metric, tags, -1)
	return m.inner.PutRaw(key, nil, core.EntryTypeDelete, 0)
}

// Delete writes a tombstone for a specific timestamp for the provided metric+tags.
func (m *Memtable) Delete(metric string, tags map[string]string, timestamp int64) error {
	key := core.EncodeTSDBKeyWithString(metric, tags, timestamp)
	return m.inner.PutRaw(key, nil, core.EntryTypeDelete, 0)
}

// Get retrieves the latest version of a key from the underlying memtable.
func (m *Memtable) Get(key []byte) (value []byte, entryType core.EntryType, found bool) {
	return m.inner.Get(key)
}

// Size returns the current memory usage of the memtable in bytes.
func (m *Memtable) Size() int64 { return m.inner.Size() }

// IsFull returns true if the memtable has reached its size threshold.
func (m *Memtable) IsFull() bool { return m.inner.IsFull() }

// Len returns the number of entries (including all versions) in the memtable.
func (m *Memtable) Len() int { return m.inner.Len() }

// NewIterator creates a new iterator for scanning the memtable.
func (m *Memtable) NewIterator(startKey, endKey []byte, order types.SortOrder) core.IteratorInterface[*core.IteratorNode] {
	return m.inner.NewIterator(startKey, endKey, order)
}

// FlushToSSTable forwards flush requests to the underlying Memtable2.
func (m *Memtable) FlushToSSTable(writer core.SSTableWriterInterface) error {
	return m.inner.FlushToSSTable(writer)
}

// Close releases all resources held by the memtable.
func (m *Memtable) Close() { m.inner.Close() }

// Query performs a simple in-memory scan over the memtable and returns
// results that match the provided QueryParams. This mirrors the legacy
// behavior by leveraging the iterator returned from the underlying memtable.
func (m *Memtable) Query(params core.QueryParams) []core.QueryResultItem {
	out := make([]core.QueryResultItem, 0)
	iter := m.NewIterator(nil, nil, types.Ascending)
	defer iter.Close()
	for iter.Next() {
		node, err := iter.At()
		if err != nil {
			continue
		}
		k := node.Key
		if len(k) < 8 {
			continue
		}
		seriesKey := k[:len(k)-8]
		ts := int64(binary.BigEndian.Uint64(k[len(k)-8:]))
		if ts < params.StartTime || ts > params.EndTime {
			continue
		}
		metric, tags, derr := core.ExtractMetricAndTagsFromSeriesKeyWithString(seriesKey)
		if derr != nil {
			continue
		}
		if metric != params.Metric {
			continue
		}
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
		if node.EntryType != core.EntryTypePutEvent || len(node.Value) == 0 {
			continue
		}
		fv, derr := core.DecodeFieldsFromBytes(node.Value)
		if derr != nil {
			continue
		}
		out = append(out, core.QueryResultItem{Metric: metric, Tags: tags, Timestamp: ts, Fields: fv})
	}
	return out
}
