package memtable

import (
	"bytes"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexuscore/types"
	"github.com/INLOpen/nexuscore/utils/clock"
	"github.com/INLOpen/skiplist"
)

// Memtable2 is an independent memtable implementation that accepts
// DataPoint-centric puts. It intentionally does not embed or wrap the
// existing `Memtable` type to provide a clean, explicit surface similar
// to the legacy engine2 expectations while reusing the same key/entry
// types and comparator logic.
type Memtable2 struct {
	mu        sync.RWMutex
	data      *skiplist.SkipList[*MemtableKey, *MemtableEntry]
	sizeBytes int64
	threshold int64

	// Retry/backoff metadata
	FlushRetries   int
	NextRetryDelay time.Duration

	CreationTime        time.Time
	LastWALSegmentIndex uint64

	CompletionChan chan error
	Err            error
}

// NewMemtable2 creates a new Memtable2 with the given threshold.
func NewMemtable2(threshold int64, clk clock.Clock) *Memtable2 {
	return &Memtable2{
		data:           skiplist.NewWithComparator[*MemtableKey, *MemtableEntry](comparator),
		threshold:      threshold,
		sizeBytes:      0,
		FlushRetries:   0,
		CreationTime:   clk.Now(),
		CompletionChan: nil,
		Err:            nil,
	}
}

// Put accepts a *core.DataPoint, encodes it to the internal TSDB key using
// string-based encoding, and inserts it into the memtable. If the DataPoint
// has no fields, a tombstone entry (EntryTypeDelete) is written.
func (m *Memtable2) Put(dp *core.DataPoint) error {
	if dp == nil {
		return fmt.Errorf("nil datapoint")
	}

	key := core.EncodeTSDBKeyWithString(dp.Metric, dp.Tags, dp.Timestamp)

	m.mu.Lock()
	defer m.mu.Unlock()

	// acquire from pools
	newKey := KeyPool.Get()
	newKey.Key = key
	newKey.PointID = 0

	newEntry := EntryPool.Get()
	newEntry.Key = key

	if len(dp.Fields) == 0 {
		newEntry.Value = nil
		newEntry.EntryType = core.EntryTypeDelete
	} else {
		vb, err := dp.Fields.Encode()
		if err != nil {
			// return pools before returning
			KeyPool.Put(newKey)
			EntryPool.Put(newEntry)
			return err
		}
		newEntry.Value = vb
		newEntry.EntryType = core.EntryTypePutEvent
	}
	newEntry.PointID = 0

	oldNode := m.data.Insert(newKey, newEntry)
	if oldNode != nil {
		KeyPool.Put(newKey)
		oldValue := oldNode.Value()
		EntryPool.Put(oldValue)
		m.sizeBytes -= oldValue.Size()
	}
	m.sizeBytes += newEntry.Size()
	return nil
}

// PutRaw inserts an entry using an already-encoded full TSDB key and raw value.
// This is a compatibility helper for code that constructs keys directly
// (e.g., tests or legacy engine code). The provided pointID is stored on
// the memtable entry and used when flushing to SSTable.
func (m *Memtable2) PutRaw(key []byte, value []byte, entryType core.EntryType, pointID uint64) error {
	if key == nil {
		return fmt.Errorf("nil key")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	newKey := KeyPool.Get()
	newKey.Key = key
	newKey.PointID = pointID

	newEntry := EntryPool.Get()
	newEntry.Key = key
	newEntry.Value = value
	newEntry.EntryType = entryType
	newEntry.PointID = pointID

	oldNode := m.data.Insert(newKey, newEntry)
	if oldNode != nil {
		KeyPool.Put(newKey)
		oldValue := oldNode.Value()
		EntryPool.Put(oldValue)
		m.sizeBytes -= oldValue.Size()
	}
	m.sizeBytes += newEntry.Size()
	return nil
}

// Get retrieves latest entry for a full encoded key (including timestamp).
func (m *Memtable2) Get(key []byte) (value []byte, entryType core.EntryType, found bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	searchKey := KeyPool.Get()
	searchKey.Key = key
	searchKey.PointID = ^uint64(0)
	defer KeyPool.Put(searchKey)

	node, ok := m.data.Seek(searchKey)
	if !ok {
		return nil, 0, false
	}
	foundKey := node.Key()
	if !bytes.Equal(foundKey.Key, key) {
		return nil, 0, false
	}
	entry := node.Value()
	if entry.EntryType == core.EntryTypeDelete {
		return nil, entry.EntryType, true
	}
	return entry.Value, entry.EntryType, true
}

// Size reports estimated memory usage.
func (m *Memtable2) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sizeBytes
}

// IsFull reports whether memtable reached threshold.
func (m *Memtable2) IsFull() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sizeBytes >= m.threshold
}

// Len returns number of entries (including versions)
func (m *Memtable2) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.data.Len()
}

// NewIterator returns an iterator over entries (holds read lock until Close).
func (m *Memtable2) NewIterator(startKey, endKey []byte, order types.SortOrder) core.IteratorInterface[*core.IteratorNode] {
	m.mu.RLock()

	opts := make([]skiplist.IteratorOption[*MemtableKey, *MemtableEntry], 0)
	if order == types.Descending {
		opts = append(opts, skiplist.WithReverse[*MemtableKey, *MemtableEntry]())
	}
	iter := m.data.NewIterator(opts...)

	return &MemtableIterator{
		mu:       &m.mu,
		iter:     iter,
		startKey: startKey,
		endKey:   endKey,
		order:    order,
		valid:    false,
	}
}

// FlushToSSTable writes all entries to the provided writer.
func (m *Memtable2) FlushToSSTable(writer core.SSTableWriterInterface) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	slog.Default().Debug("Memtable2.FlushToSSTable: start", "mem_ptr", fmt.Sprintf("%p", m), "len", m.Len(), "size", m.Size())

	iter := m.data.NewIterator()
	entriesWritten := 0
	for iter.Next() {
		memKey := iter.Key()
		memEntry := iter.Value()
		if err := writer.Add(memEntry.Key, memEntry.Value, memEntry.EntryType, memKey.PointID); err != nil {
			slog.Default().Warn("Memtable2.FlushToSSTable: writer.Add error", "key", string(memEntry.Key), "entries_written", entriesWritten, "err", err)
			return fmt.Errorf("failed to add memtable entry to sstable (key=%s, entries_written=%d): %w", string(memEntry.Key), entriesWritten, err)
		}
		entriesWritten++
	}
	slog.Default().Debug("Memtable2.FlushToSSTable: done", "entries_written", entriesWritten)
	return nil
}

// Close releases resources and returns pooled objects.
func (m *Memtable2) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.data == nil {
		return
	}
	m.data.Range(func(key *MemtableKey, value *MemtableEntry) bool {
		KeyPool.Put(key)
		EntryPool.Put(value)
		return true
	})
	m.data = nil
	m.sizeBytes = 0
}
