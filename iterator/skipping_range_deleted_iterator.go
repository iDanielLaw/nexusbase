package iterator

import (
	"fmt"

	"github.com/INLOpen/nexusbase/core"
)

// For sstable.EntryType

// RangeDeletedChecker is a function type that checks if a data point is covered by a range tombstone.
// It takes the series key string, the data point's timestamp, and the data point's sequence number.
type RangeDeletedChecker func(seriesKey []byte, timestamp int64, dataPointSeqNum uint64) bool

// SkippingRangeDeletedIterator wraps an existing iterator and skips entries
// that are covered by a range tombstone.
type SkippingRangeDeletedIterator struct {
	underlying           Interface
	isRangeDeleted       RangeDeletedChecker
	extractSeriesKeyFunc SeriesKeyExtractorFunc      // To extract series part from data point key
	decodeTsFunc         func([]byte) (int64, error) // To decode timestamp from data point key, now returns error
	err                  error
}

// NewSkippingRangeDeletedIterator creates a new iterator that skips range-deleted entries.
func NewSkippingRangeDeletedIterator(
	underlying Interface,
	checker RangeDeletedChecker,
	extractor SeriesKeyExtractorFunc,
	decodeTs func([]byte) (int64, error),
) Interface {
	return &SkippingRangeDeletedIterator{
		underlying:           underlying,
		isRangeDeleted:       checker,
		extractSeriesKeyFunc: extractor,
		decodeTsFunc:         decodeTs,
	}
}

// Next advances the iterator to the next entry not covered by a range tombstone.
func (it *SkippingRangeDeletedIterator) Next() bool {
	if it.err != nil {
		return false
	}
	for it.underlying.Next() {
		dataPointKey, _, _, dataPointSeqNum := it.underlying.At()

		// Extract series identifier from the data point key

		seriesKeyBytes, extractErr := it.extractSeriesKeyFunc(dataPointKey)
		if extractErr != nil {
			it.err = fmt.Errorf("skipping_range_deleted_iterator: %w", extractErr)
			return false
		}

		// Timestamp is the last 8 bytes of the dataPointKey
		if len(dataPointKey) < 8 { // Should be caught by extractSeriesKeyFunc ideally
			it.err = fmt.Errorf("skipping_range_deleted_iterator: key too short to contain timestamp: %x", dataPointKey)
			return false
		}
		timestamp, decodeErr := it.decodeTsFunc(dataPointKey[len(dataPointKey)-8:])
		if decodeErr != nil {
			it.err = fmt.Errorf("skipping_range_deleted_iterator: failed to decode timestamp: %w", decodeErr)
			return false
		}

		if !it.isRangeDeleted(seriesKeyBytes, timestamp, dataPointSeqNum) {
			return true // Found an entry not covered by a range tombstone
		}
		// If covered by range tombstone, skip it and try next.
	}
	it.err = it.underlying.Error()
	return false // Underlying iterator is exhausted or an error occurred
}

func (it *SkippingRangeDeletedIterator) At() ([]byte, []byte, core.EntryType, uint64) {
	return it.underlying.At()
}

// Error returns any error from the underlying iterator.
func (it *SkippingRangeDeletedIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.underlying.Error()
}

// Close closes the underlying iterator.
func (it *SkippingRangeDeletedIterator) Close() error { return it.underlying.Close() }
