package iterator

import (
	"bytes"
	"container/heap"
	"fmt"

	"github.com/INLOpen/nexusbase/core"
)

// minHeap implements heap.Interface for a slice of Iterators.
// It is used to efficiently find the iterator with the smallest current key.
type minHeap []core.IteratorInterface[*core.IteratorNode]

func (h minHeap) Len() int { return len(h) }

func (h minHeap) Less(i, j int) bool {
	// Compare keys first
	curA, errA := h[i].At()
	if errA != nil {
		panic(fmt.Sprintf("iterator in heap has error: %v", errA))
	}
	curB, errB := h[j].At()
	if errB != nil {
		panic(fmt.Sprintf("iterator in heap has error: %v", errB))
	}
	keyI, _, _, pointIDI := curA.Key, curA.Value, curA.EntryType, curA.SeqNum
	keyJ, _, _, pointIDJ := curB.Key, curB.Value, curB.EntryType, curB.SeqNum
	keyCmp := bytes.Compare(keyI, keyJ)
	if keyCmp != 0 {
		return keyCmp < 0
	}
	// If keys are equal, the one with the higher point id is "smaller"
	// because it's the newer version and should be processed first.
	return pointIDI > pointIDJ
}

func (h minHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *minHeap) Push(x interface{}) {
	*h = append(*h, x.(core.IteratorInterface[*core.IteratorNode]))
}

func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// NewMinHeap creates and initializes a new min-heap from a slice of iterators.
// It filters out any iterators that are already exhausted.
func NewMinHeap(iters []core.IteratorInterface[*core.IteratorNode]) *minHeap {
	// Filter out iterators that are already invalid
	validIters := make(minHeap, 0, len(iters))
	for _, iter := range iters {
		if iter.Next() { // Initial advance to the first element
			validIters = append(validIters, iter)
		} else {
			// If an iterator is exhausted from the start, close it.
			iter.Close()
		}
	}

	h := &validIters
	heap.Init(h)
	return h
}

// Key returns the key of the iterator at the top of the heap without removing it.
func (h *minHeap) Key() []byte {
	if h.Len() == 0 {
		return nil
	}
	cur, err := (*h)[0].At()
	if err != nil {
		panic(fmt.Sprintf("iterator at top of heap has error: %v", err))
	}
	return cur.Key
}

// Value returns the value of the iterator at the top of the heap.
func (h *minHeap) Value() []byte {
	if h.Len() == 0 {
		return nil
	}
	cur, err := (*h)[0].At()
	if err != nil {
		panic(fmt.Sprintf("iterator at top of heap has error: %v", err))
	}
	return cur.Value
}

// Next advances the iterator at the top of the heap to its next element.
// If the iterator is exhausted, it's removed from the heap.
func (h *minHeap) Next() {
	if h.Len() == 0 {
		return
	}
	topIter := (*h)[0]
	if topIter.Next() {
		heap.Fix(h, 0) // The key might have changed, so fix the heap.
	} else {
		heap.Pop(h) // The iterator is exhausted, remove it.
		topIter.Close()
	}
}
