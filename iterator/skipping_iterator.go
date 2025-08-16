package iterator

import (
	"bytes"

	"github.com/INLOpen/nexusbase/core"
)

// SkippingIterator wraps another iterator and skips the first element if its key matches a specific key.
// This is the core mechanism for cursor-based pagination, ensuring the last item of the previous
// page is not included in the current page.
type SkippingIterator struct {
	underlying core.Interface
	skipKey    []byte
	skipped    bool
}

// NewSkippingIterator creates a new iterator that wraps another and skips a specific key.
func NewSkippingIterator(iter core.Interface, keyToSkip []byte) core.Interface {
	return &SkippingIterator{
		underlying: iter,
		skipKey:    keyToSkip,
	}
}

// Next advances the iterator. On the first call, it checks if the current item
// matches the key to be skipped and, if so, advances one more time.
func (it *SkippingIterator) Next() bool {
	if !it.skipped {
		it.skipped = true
		if !it.underlying.Next() {
			return false // Underlying is empty or has an error.
		}
		// Check if the first item's key is the one we need to skip.
		key, _, _, _ := it.underlying.At()
		if bytes.Equal(key, it.skipKey) {
			// It matches, so advance the underlying iterator one more time to skip it.
			return it.underlying.Next()
		}
		// It doesn't match, so the current position is valid.
		return true
	}
	return it.underlying.Next()
}

func (it *SkippingIterator) At() ([]byte, []byte, core.EntryType, uint64) { return it.underlying.At() }
func (it *SkippingIterator) Error() error                                 { return it.underlying.Error() }
func (it *SkippingIterator) Close() error                                 { return it.underlying.Close() }
