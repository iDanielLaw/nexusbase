package engine2

import (
	"errors"

	"github.com/INLOpen/nexusbase/core"
)

// memQueryIterator is a simple in-memory iterator over QueryResultItem
type memQueryIterator struct {
	items []*core.QueryResultItem
	idx   int
}

func newMemQueryIterator(items []*core.QueryResultItem) *memQueryIterator {
	return &memQueryIterator{items: items, idx: -1}
}

func (it *memQueryIterator) Next() bool {
	it.idx++
	return it.idx < len(it.items)
}

func (it *memQueryIterator) At() (*core.QueryResultItem, error) {
	if it.idx < 0 || it.idx >= len(it.items) {
		return nil, errors.New("iterator out of range")
	}
	return it.items[it.idx], nil
}

func (it *memQueryIterator) Error() error { return nil }

func (it *memQueryIterator) Close() error { return nil }

func (it *memQueryIterator) Put(v *core.QueryResultItem) {}

// UnderlyingAt is part of QueryResultIteratorInterface; return not implemented.
func (it *memQueryIterator) UnderlyingAt() (*core.IteratorNode, error) { return nil, nil }
