package iterator

import (
	"fmt"

	"github.com/INLOpen/nexusbase/core"
)

// SeriesDeletedChecker is a function type that checks if a series is deleted.
type SeriesDeletedChecker func(seriesKey []byte, dataPointSeqNum uint64) bool

// SeriesKeyExtractorFunc is a function type for extracting the series key from a data point key.
type SeriesKeyExtractorFunc func(dataPointKey []byte) ([]byte, error)

// SkippingDeletedSeriesIterator wraps an existing iterator and skips entries
// belonging to series that have been marked with a series tombstone,
// provided the data point's sequence number is older than or equal to
// the deletion sequence number.
type SkippingDeletedSeriesIterator struct {
	underlying           core.IteratorInterface[*core.IteratorNode]
	isSeriesDeleted      SeriesDeletedChecker
	extractSeriesKeyFunc SeriesKeyExtractorFunc // Function to extract series key
	err                  error
}

// NewSkippingDeletedSeriesIterator creates a new iterator that skips deleted series.
func NewSkippingDeletedSeriesIterator(underlying core.IteratorInterface[*core.IteratorNode], checker SeriesDeletedChecker, extractor SeriesKeyExtractorFunc) core.IteratorInterface[*core.IteratorNode] {
	return &SkippingDeletedSeriesIterator{
		underlying:           underlying,
		isSeriesDeleted:      checker,
		extractSeriesKeyFunc: extractor,
	}
}

// Next advances the iterator to the next non-deleted series entry.
func (it *SkippingDeletedSeriesIterator) Next() bool {
	if it.err != nil {
		return false
	}
	for it.underlying.Next() {
		cur, _ := it.underlying.At()

		// Extract series identifier from the data point key
		// This seriesKeyBytes is metric_name<NULL>tags_string
		seriesKeyBytes, extractErr := it.extractSeriesKeyFunc(cur.Key)
		if extractErr != nil {
			it.err = fmt.Errorf("skipping_deleted_series_iterator: %w", extractErr)
			return false
		}

		if !it.isSeriesDeleted(seriesKeyBytes, cur.SeqNum) {
			return true // Found a data point from a non-deleted series (or newer than deletion)
		}
		// If series is deleted and this data point is old enough, skip it.
	}
	it.err = it.underlying.Error() // Propagate error from underlying if it's exhausted
	return false                   // Underlying iterator is exhausted or an error occurred
}

func (it *SkippingDeletedSeriesIterator) At() (*core.IteratorNode, error) {
	return it.underlying.At()
}

// Error returns any error from the underlying iterator.
func (it *SkippingDeletedSeriesIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.underlying.Error()
}

// Close closes the underlying iterator.
func (it *SkippingDeletedSeriesIterator) Close() error {
	return it.underlying.Close()
}
