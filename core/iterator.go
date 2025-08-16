package core

type Interface interface {
	Next() bool
	// At returns the current key, value, entry type, and sequence number.
	// The returned slices are only valid until the next call to Next().
	At() ([]byte, []byte, EntryType, uint64)
	Error() error
	Close() error
}

type IteratorInterface[V any] interface {
	Next() bool
	// At returns the current key, value, entry type, and sequence number.
	// The returned slices are only valid until the next call to Next().
	At() (V, error)
	Error() error
	Close() error
}

type IteratorPoolInterface[V any] interface {
	IteratorInterface[V]
	Put(V)
}
