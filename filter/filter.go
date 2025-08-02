package filter

// Filter is an interface for probabilistic data structures like Bloom filters.
type Filter interface {
	// Contains checks if the data may be in the set.
	// A false return value means the data is definitely not in the set.
	// A true return value means the data is probably in the set.
	Contains(data []byte) bool

	// Bytes returns the filter data as a byte slice.
	Bytes() []byte
}
