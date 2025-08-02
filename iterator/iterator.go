package iterator

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"

	"github.com/INLOpen/nexusbase/core" // For core.EntryType
)

// Interface defines a common interface for all iterators in the system.
type Interface interface {
	Next() bool
	// At returns the current key, value, entry type, and sequence number.
	// The returned slices are only valid until the next call to Next().
	At() ([]byte, []byte, core.EntryType, uint64)
	Error() error
	Close() error
}

var ErrSkipPoint = errors.New("skip point")

// mergingIteratorItem is an item in the min-heap for MergingIterator.
type mergingIteratorItem struct {
	iter      Interface // The underlying iterator
	key       []byte    // Full internal key, copied for safety
	value     []byte
	entryType core.EntryType // Changed to core.EntryType
	seqNum    uint64

	// Cached parsed components for performance.
	// These are parsed once when the item is created.
	seriesKey []byte
	timestamp int64
}

var _ Interface = (*MergingIterator)(nil)
var _ Interface = (*EmptyIterator)(nil)
var _ Interface = (*MultiFieldAggregatingIterator)(nil)
var _ Interface = (*MultiFieldDownsamplingIterator)(nil)

// mergingIteratorHeap implements heap.Interface for mergingIteratorItem.
type mergingIteratorHeap struct {
	items []*mergingIteratorItem
	order core.SortOrder
}

func (h *mergingIteratorHeap) Len() int { return len(h.items) }
func (h *mergingIteratorHeap) Less(i, j int) bool {
	itemI, itemJ := h.items[i], h.items[j]

	// Primary sort key is the timestamp.
	if itemI.timestamp != itemJ.timestamp {
		if h.order == core.Descending {
			return itemI.timestamp > itemJ.timestamp
		}
		return itemI.timestamp < itemJ.timestamp
	}

	// Secondary sort key is the series key.
	seriesCmp := bytes.Compare(itemI.seriesKey, itemJ.seriesKey)
	if seriesCmp != 0 {
		if h.order == core.Descending {
			return seriesCmp > 0
		}
		return seriesCmp < 0
	}

	// Tertiary sort key is the sequence number (descending) to get the newest version.
	return itemI.seqNum > itemJ.seqNum
}
func (h *mergingIteratorHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *mergingIteratorHeap) Push(x interface{}) {
	h.items = append(h.items, x.(*mergingIteratorItem))
}
func (h *mergingIteratorHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	h.items = old[0 : n-1]
	return item
}

// MergingIterator combines multiple Iterators into a single sorted view.
type MergingIterator struct {
	iters    []Interface
	heap     *mergingIteratorHeap
	startKey []byte
	endKey   []byte
	// Checkers for tombstones during merge
	isSeriesDeleted      SeriesDeletedChecker
	isRangeDeleted       RangeDeletedChecker
	extractSeriesKeyFunc SeriesKeyExtractorFunc
	decodeTsFunc         func([]byte) (int64, error) // Now returns error

	currentKey       []byte
	currentValue     []byte
	currentEntryType core.EntryType // Changed to core.EntryType
	currentSeqNum    uint64
	err              error
}

// NewMergingIterator creates a new MergingIterator.
// iters should be a list of iterators that are already positioned (or will be by their first Next()).
func NewMergingIterator(iters []Interface, startKey, endKey []byte) (Interface, error) {
	// This constructor is being updated to receive checker functions.
	// The actual constructor used in engine.go will need to pass these.
	// For now, this signature is a placeholder.
	// A better approach is to add a new constructor like NewMergingIteratorWithTombstones.
	// Let's add the new constructor.
	return nil, fmt.Errorf("use NewMergingIteratorWithTombstones instead")
}

// MergingIteratorParams holds all parameters for creating a NewMergingIteratorWithTombstones.
type MergingIteratorParams struct {
	Iters                []Interface
	StartKey             []byte
	EndKey               []byte
	Order                core.SortOrder
	IsSeriesDeleted      SeriesDeletedChecker
	IsRangeDeleted       RangeDeletedChecker
	ExtractSeriesKeyFunc SeriesKeyExtractorFunc
	DecodeTsFunc         func([]byte) (int64, error)
}

// NewMergingIteratorWithTombstones creates a new MergingIterator that is aware of tombstones.
func NewMergingIteratorWithTombstones(params MergingIteratorParams) (Interface, error) {
	mi := &MergingIterator{
		iters:                params.Iters,
		startKey:             params.StartKey,
		endKey:               params.EndKey,
		isSeriesDeleted:      params.IsSeriesDeleted,
		isRangeDeleted:       params.IsRangeDeleted,
		extractSeriesKeyFunc: params.ExtractSeriesKeyFunc,
		decodeTsFunc:         params.DecodeTsFunc,
		heap: &mergingIteratorHeap{
			items: make([]*mergingIteratorItem, 0, len(params.Iters)),
			order: params.Order, // Pass the order to the heap
		},
	}

	for _, iter := range mi.iters {
		if iter.Next() { // Prime the iterator
			item, err := newMergingIteratorItem(iter, mi.extractSeriesKeyFunc, mi.decodeTsFunc)
			if err != nil {
				mi.Close()
				return nil, err
			}
			mi.heap.Push(item)
		} else if err := iter.Error(); err != nil {
			// Handle or propagate error from underlying iterator initialization
			// For now, close all and return error
			mi.Close()
			return nil, err
		}
	}
	heap.Init(mi.heap)

	return mi, nil
}

func (mi *MergingIterator) Next() bool {
	// The main Next() loop now relies on getNextCandidateFromHeap
	if mi.err != nil {
		return false
	}

	for { // Loop to find the next valid, non-deleted, distinct key
		if mi.heap.Len() == 0 {
			mi.currentKey = nil
			mi.currentValue = nil   // Reset current value too
			mi.currentEntryType = 0 // Reset current entry type
			mi.currentSeqNum = 0    // Reset current sequence number
			return false
		}

		// Get the next candidate entry (smallest key, highest seqNum for that key)
		candidateItem, err := mi.getNextCandidateFromHeap()
		if err != nil {
			mi.err = err // Error occurred while getting candidate
			return false
		}
		// If candidateItem is nil, it means the heap became empty during processing (e.g., all remaining items were skipped internally)
		if candidateItem == nil {
			continue // Loop again to check if heap is truly empty now
		}

		// Check range boundaries
		if mi.startKey != nil && bytes.Compare(candidateItem.key, mi.startKey) < 0 {
			continue // Before start key, so loop to next candidate
		}
		if mi.endKey != nil && bytes.Compare(candidateItem.key, mi.endKey) >= 0 {
			// If the candidate key is at or beyond the endKey, we are done.
			// Reset current values and return false.
			mi.currentKey = nil
			mi.currentValue = nil
			mi.currentEntryType = 0
			mi.currentSeqNum = 0
			return false
		}

		// --- Filtering Logic ---
		if candidateItem.entryType == core.EntryTypeDelete {
			continue // Skip this entry, it's a point tombstone
		}

		if mi.isSeriesDeleted(candidateItem.seriesKey, candidateItem.seqNum) {
			continue // Skip this entry, its series is deleted
		}

		if mi.isRangeDeleted(candidateItem.seriesKey, candidateItem.timestamp, candidateItem.seqNum) {
			continue // Skip this entry, it's covered by a range tombstone
		}

		// If we reached here, it's the latest version of a distinct key within the range, and it's not deleted.
		mi.currentKey = candidateItem.key
		mi.currentValue = candidateItem.value
		mi.currentEntryType = candidateItem.entryType
		mi.currentSeqNum = candidateItem.seqNum
		return true
	}
}

// At returns the current key, value, entry type, and sequence number.
func (mi *MergingIterator) At() ([]byte, []byte, core.EntryType, uint64) {
	return mi.currentKey, mi.currentValue, mi.currentEntryType, mi.currentSeqNum
}

func (mi *MergingIterator) Error() error { return mi.err }
func (mi *MergingIterator) Close() error {
	var firstErr error
	for _, iter := range mi.iters {
		if err := iter.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	mi.iters = nil
	mi.heap = nil
	return firstErr
}

// getNextCandidateFromHeap pops the top item from the heap, advances its iterator,
// and then advances any other iterators that had the exact same key.
// It returns the item with the highest sequence number for the smallest key currently in the heap.
// Returns nil if the heap is empty or if an error occurs in an underlying iterator.
func (mi *MergingIterator) getNextCandidateFromHeap() (*mergingIteratorItem, error) {
	if mi.heap.Len() == 0 {
		return nil, nil // Heap is empty
	}

	// Pop the item with the smallest key (and highest seqNum for that key)
	item := heap.Pop(mi.heap).(*mergingIteratorItem)
	topKey := item.key // Store the key of the item we just popped

	// Advance the iterator that produced this item.
	// If it has more data, push the *next* item from this iterator back onto the heap.
	if item.iter.Next() {
		nextItem, err := newMergingIteratorItem(item.iter, mi.extractSeriesKeyFunc, mi.decodeTsFunc)
		if err != nil {
			return nil, err
		}
		heap.Push(mi.heap, nextItem)
	} else {
		// Iterator is exhausted or has an error.
		if err := item.iter.Error(); err != nil {
			return nil, err // Propagate error from underlying iterator
		}
		// Do not close the iterator here. The parent MergingIterator's Close() method
		// is responsible for closing all underlying iterators once.
	}

	// Now, consume and advance any other iterators whose current item has the exact same key.
	// Due to the heap's Less function, these items will be at the top of the heap,
	// immediately after the item we just popped, and will have sequence numbers <= item.seqNum.
	for mi.heap.Len() > 0 && bytes.Equal(mi.heap.items[0].key, topKey) {
		otherItemWithSameKey := heap.Pop(mi.heap).(*mergingIteratorItem)
		// Advance this other iterator. If it has more data, push it back.
		if otherItemWithSameKey.iter.Next() {
			nextItem, err := newMergingIteratorItem(otherItemWithSameKey.iter, mi.extractSeriesKeyFunc, mi.decodeTsFunc)
			if err != nil {
				return nil, err
			}
			heap.Push(mi.heap, nextItem)
		} else {
			// This iterator is also exhausted or has an error.
			if err := otherItemWithSameKey.iter.Error(); err != nil {
				return nil, err // Propagate error
			}
			// Do not close here either. Let the main Close() handle it to prevent double-closing.
		}
	}

	// `item` now holds the key-value pair with the highest sequence number for the key `topKey`.
	// This is the entry we will potentially yield (if not filtered by range or tombstones).
	return item, nil
}

// newMergingIteratorItem creates a new item for the heap.
// It copies the key and value from the underlying iterator to ensure they are safe
// from buffer reuse, and it pre-parses the key components for performance.
func newMergingIteratorItem(iter Interface, extractSeriesKeyFunc SeriesKeyExtractorFunc, decodeTsFunc func([]byte) (int64, error)) (*mergingIteratorItem, error) {
	key, value, entryType, seqNum := iter.At()

	// Key and Value must be copied because the underlying iterator's buffer is reused on the next call to Next().
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	seriesKey, err := extractSeriesKeyFunc(keyCopy)
	if err != nil {
		return nil, fmt.Errorf("newMergingIteratorItem: failed to extract series key: %w", err)
	}

	timestamp, err := decodeTsFunc(keyCopy[len(keyCopy)-8:])
	if err != nil {
		return nil, fmt.Errorf("newMergingIteratorItem: failed to decode timestamp: %w", err)
	}

	return &mergingIteratorItem{
		iter:      iter,
		key:       keyCopy,
		value:     valueCopy,
		entryType: entryType,
		seqNum:    seqNum,
		seriesKey: seriesKey,
		timestamp: timestamp,
	}, nil
}

// EmptyIterator is an iterator that is always exhausted.
type EmptyIterator struct{}

// NewEmptyIterator creates a new empty iterator.
func NewEmptyIterator() Interface {
	return &EmptyIterator{}
}

// Next always returns false.
func (it *EmptyIterator) Next() bool {
	return false
}

// At returns nil values.
func (it *EmptyIterator) At() ([]byte, []byte, core.EntryType, uint64) {
	return nil, nil, 0, 0
}

// Error always returns nil.
func (it *EmptyIterator) Error() error {
	return nil
}

// Close does nothing and returns nil.
func (it *EmptyIterator) Close() error {
	return nil
}
