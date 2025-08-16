package core

type IteratorNodeInterface interface {
	TypeNode() string
}

type IteratorInterface[V IteratorNodeInterface] interface {
	Next() bool
	// At returns the current key, value, entry type, and sequence number.
	// The returned slices are only valid until the next call to Next().
	At() (V, error)
	Error() error
	Close() error
}

type IteratorPoolInterface[V IteratorNodeInterface] interface {
	IteratorInterface[V]
	Put(V)
}

type IteratorNode struct {
	Key       []byte
	Value     []byte
	EntryType EntryType
	SeqNum    uint64
}

func (it *IteratorNode) TypeNode() string {
	return "NODEITERATOR"
}
