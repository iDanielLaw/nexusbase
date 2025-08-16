package iterator

import (
	"fmt"
	"math"
	"time"

	"github.com/INLOpen/nexusbase/core"
	tdigest "github.com/caio/go-tdigest/v4"
)

type finalizedResult struct {
	key   []byte
	value []byte
}

// downsampleAccumulator holds the state for all aggregations on a single field within a window.
type downsampleAccumulator struct {
	// For numeric aggregations (sum, avg, min, max, first, last, frac)
	sum          float64
	sumOfSquares float64
	numericCount uint64
	min          float64
	max          float64
	first        float64
	last         float64
	firstNumeric bool

	// For count(field)
	nonNullCount uint64

	// For percentile calculations
	collectValues bool
	td            *tdigest.TDigest
}

func newDownsampleAccumulator() *downsampleAccumulator {
	return &downsampleAccumulator{
		min: math.Inf(1),
		max: math.Inf(-1),
	}
}

func (a *downsampleAccumulator) Add(val core.PointValue) error {
	if val.IsNull() {
		return nil
	}
	a.nonNullCount++

	var numValue float64
	if f, ok := val.ValueFloat64(); ok {
		numValue = f
	} else if i, ok := val.ValueInt64(); ok {
		numValue = float64(i)
	} else {
		return nil // Not a numeric value
	}

	// From here on, we have a numeric value.
	a.numericCount++

	if !a.firstNumeric {
		a.first = numValue
		a.firstNumeric = true
	}
	a.last = numValue

	if numValue < a.min {
		a.min = numValue
	}
	if numValue > a.max {
		a.max = numValue
	}
	a.sum += numValue
	a.sumOfSquares += numValue * numValue

	if a.collectValues {
		if a.td == nil {
			var err error
			a.td, err = tdigest.New()
			if err != nil {
				return fmt.Errorf("tdigest.New failed: %w", err)
			}
		}
		if err := a.td.AddWeighted(numValue, 1); err != nil {
			return fmt.Errorf("tdigest AddWeighted failed: %w", err)
		}
	}
	return nil
}

// MultiFieldDownsamplingIterator aggregates multiple fields over time windows.
type MultiFieldDownsamplingIterator struct {
	underlying core.Interface
	specs      []core.AggregationSpec
	interval   time.Duration

	err error

	// Parameters for emitting empty windows
	queryStartTime   int64
	queryEndTime     int64
	emitEmptyWindows bool
	percentileFields map[string]bool

	// State for the current window being processed
	windowStartTime int64
	// A map from series key (as a string) to its map of aggregators.
	windowAggregators map[string]map[string]*downsampleAccumulator
	hasDataInWindow   bool // Does the current window have any data?
	lastSeenSeriesKey string

	resultsBuffer []finalizedResult
	// State for peeking at the next item from the underlying iterator
	peekedKey   []byte
	peekedValue []byte
	peeked      bool

	// State for the currently emitted item
	currentKey   []byte
	currentValue []byte
	hasNext      bool
}

// NewMultiFieldDownsamplingIterator creates a new downsampling iterator.
func NewMultiFieldDownsamplingIterator(iter core.Interface, specs []core.AggregationSpec, interval time.Duration, startTime, endTime int64, emitEmpty bool) (*MultiFieldDownsamplingIterator, error) {
	if interval <= 0 {
		return nil, fmt.Errorf("downsample interval must be positive")
	}

	percentileFields := make(map[string]bool)
	for _, spec := range specs {
		if _, isPercentile := parsePercentile(string(spec.Function)); isPercentile {
			percentileFields[spec.Field] = true
		}
	}

	return &MultiFieldDownsamplingIterator{
		underlying:        iter,
		specs:             specs,
		interval:          interval,
		queryStartTime:    startTime,
		queryEndTime:      endTime,
		emitEmptyWindows:  emitEmpty,
		windowStartTime:   -1, // Initialize to an invalid value
		windowAggregators: make(map[string]map[string]*downsampleAccumulator),
		percentileFields:  percentileFields,
		resultsBuffer:     make([]finalizedResult, 0, 8), // Pre-allocate a small buffer
	}, nil
}

// Next advances the iterator to the next downsampled window.
func (it *MultiFieldDownsamplingIterator) Next() bool {
	// This check is for the At()/Next() contract. If we already have a value,
	// the user must call At() first. This should not be hit in a normal for-loop.
	if it.hasNext {
		return true
	}

	// This loop ensures we keep processing windows until we have a result to emit
	// or we are completely done.
	for {
		// Priority 1: Emit any results that were already buffered from a previous window.
		if len(it.resultsBuffer) > 0 {
			result := it.resultsBuffer[0]
			it.resultsBuffer = it.resultsBuffer[1:]
			it.currentKey = result.key
			it.currentValue = result.value
			it.hasNext = true
			return true
		}

		// Priority 2: If the buffer is empty, check if we've hit a terminal error.
		// If so, we can't process any more windows, so we are done.
		if it.err != nil {
			return false
		}

		// Priority 3: If buffer is empty and no error, check if we are past the end time.
		if it.windowStartTime != -1 && it.windowStartTime >= it.queryEndTime {
			it.err = it.underlying.Error() // Capture any final error.
			return false
		}

		// If we are here, the buffer is empty, there's no error yet, and we are within the time range.
		// So, we need to process the next window.

		// Initialize window start time on the first run.
		if it.windowStartTime == -1 {
			it.windowStartTime = it.queryStartTime - (it.queryStartTime % it.interval.Nanoseconds())
		}

		// Reset state and calculate window boundaries.
		it.resetWindowState(it.windowStartTime)
		windowEndTime := it.windowStartTime + it.interval.Nanoseconds()

		// Process all points belonging to the current window.
		for {
			key, value, ok := it.peekNextPoint()
			if !ok { // End of data or error in underlying iterator.
				break
			}

			ts, _ := core.DecodeTimestamp(key[len(key)-8:])
			if ts >= windowEndTime { // Point belongs to the next window.
				break
			}

			// Process and consume the point.
			seriesKey, _ := core.ExtractSeriesKeyFromInternalKeyWithErr(key)
			it.processPoint(seriesKey, value)
			if it.err != nil { // Check for error from processPoint immediately.
				break
			}
			it.hasDataInWindow = true
			it.consumePoint()
		}

		// Finalize the window we just processed. This may populate the resultsBuffer.
		// It may also set it.err if encoding fails.
		it.finalizeCurrentWindow()

		// Advance to the next window time.
		it.windowStartTime += it.interval.Nanoseconds()

		// The outer `for` loop will now repeat. If the buffer was populated, it will be emitted.
		// If not, it will check for errors and then proceed to the next window.
	}
}

// At returns the current downsampled window's key and value.
func (it *MultiFieldDownsamplingIterator) At() ([]byte, []byte, core.EntryType, uint64) {
	it.hasNext = false
	return it.currentKey, it.currentValue, core.EntryTypePutEvent, 0
}

func (it *MultiFieldDownsamplingIterator) Error() error {
	return it.err
}

func (it *MultiFieldDownsamplingIterator) Close() error {
	return it.underlying.Close()
}

func (it *MultiFieldDownsamplingIterator) processPoint(seriesKey, valueBytes []byte) {
	fields, err := core.DecodeFieldsFromBytes(valueBytes)
	if err != nil {
		it.err = fmt.Errorf("failed to decode fields during downsampling: %w", err)
		return
	}

	seriesKeyStr := string(seriesKey) // Keep track of the last series we saw
	it.lastSeenSeriesKey = seriesKeyStr

	// Get or create the map of accumulators for this series
	seriesFieldAccs, ok := it.windowAggregators[seriesKeyStr]
	if !ok {
		seriesFieldAccs = make(map[string]*downsampleAccumulator)
		it.windowAggregators[seriesKeyStr] = seriesFieldAccs
	}

	// Handle count(*) - it's not in the fields map, but we need to increment its accumulator.
	// We use the nonNullCount field of the accumulator for the "*" field.
	starAcc, ok := seriesFieldAccs["*"]
	if !ok {
		starAcc = newDownsampleAccumulator()
		seriesFieldAccs["*"] = starAcc
	}
	starAcc.nonNullCount++

	// Process each field from the data point
	for fieldName, fieldVal := range fields {
		acc, ok := seriesFieldAccs[fieldName]
		if !ok {
			acc = newDownsampleAccumulator()
			if it.percentileFields[fieldName] {
				acc.collectValues = true
			}
			seriesFieldAccs[fieldName] = acc
		}
		if err := acc.Add(fieldVal); err != nil {
			it.err = err
			return
		}
	}
}

// finalizeCurrentWindow processes all series in the completed window and populates the resultsBuffer.
func (it *MultiFieldDownsamplingIterator) finalizeCurrentWindow() {
	// If the window was empty and we're not supposed to emit empty windows, just return.
	if !it.hasDataInWindow && !it.emitEmptyWindows {
		return
	}

	// Handle emitting an empty window if required.
	if !it.hasDataInWindow && it.emitEmptyWindows {
		// We need a series key to emit an empty result. If we've never seen a series, we can't.
		if it.lastSeenSeriesKey == "" {
			return
		}
		// Create a result map with default values for an empty window.
		resultMap := make(map[string]float64)
		for _, spec := range it.specs {
			resultKey := core.MakeKeyAggregator(spec)
			switch spec.Function {
			case "count", "sum":
				resultMap[resultKey] = 0
			default: // Catches avg, min, max, first, last, frac, stddev, and all percentiles
				resultMap[resultKey] = math.NaN()
			}
		}

		encodedResult, err := core.EncodeAggregationResult(resultMap)
		if err != nil {
			it.err = fmt.Errorf("failed to encode empty downsample result: %w", err)
			return
		}

		key := make([]byte, len(it.lastSeenSeriesKey)+8)
		copy(key, []byte(it.lastSeenSeriesKey))
		core.EncodeTimestamp(key[len(it.lastSeenSeriesKey):], it.windowStartTime)
		it.resultsBuffer = append(it.resultsBuffer, finalizedResult{key: key, value: encodedResult})
		return
	}

	// Process all series that had data in this window.
	for seriesKeyStr, seriesFieldAccs := range it.windowAggregators {
		resultMap := make(map[string]float64)
		for _, spec := range it.specs {
			acc, ok := seriesFieldAccs[spec.Field]
			if !ok {
				// This series didn't have this specific field in this window.
				// Use a new empty accumulator to get default values.
				acc = newDownsampleAccumulator()
			}

			resultKey := core.MakeKeyAggregator(spec)
			// Handle percentile functions first.
			if p, isPercentile := parsePercentile(string(spec.Function)); isPercentile {
				if acc.td != nil {
					resultMap[resultKey] = acc.td.Quantile(p / 100.0)
				} else {
					resultMap[resultKey] = math.NaN()
				}
				continue
			}

			switch spec.Function {
			case "count":
				resultMap[resultKey] = float64(acc.nonNullCount)
			case "sum":
				resultMap[resultKey] = acc.sum
			case "avg":
				if acc.numericCount > 0 {
					resultMap[resultKey] = acc.sum / float64(acc.numericCount)
				} else {
					resultMap[resultKey] = math.NaN()
				}
			case "min":
				if acc.numericCount > 0 {
					resultMap[resultKey] = acc.min
				} else {
					resultMap[resultKey] = math.NaN()
				}
			case "max":
				if acc.numericCount > 0 {
					resultMap[resultKey] = acc.max
				} else {
					resultMap[resultKey] = math.NaN()
				}
			case "first":
				if acc.numericCount > 0 {
					resultMap[resultKey] = acc.first
				} else {
					resultMap[resultKey] = math.NaN()
				}
			case "last":
				if acc.numericCount > 0 {
					resultMap[resultKey] = acc.last
				} else {
					resultMap[resultKey] = math.NaN()
				}
			case "frac":
				if acc.numericCount < 2 {
					resultMap[resultKey] = math.NaN()
				} else if acc.first == 0 {
					if acc.last == 0 {
						resultMap[resultKey] = 0.0
					} else if acc.last > 0 {
						resultMap[resultKey] = math.Inf(1)
					} else {
						resultMap[resultKey] = math.Inf(-1)
					}
				} else {
					resultMap[resultKey] = (acc.last - acc.first) / acc.first
				}
			case "stddev":
				if acc.numericCount < 2 {
					resultMap[resultKey] = math.NaN()
				} else {
					countF := float64(acc.numericCount)
					variance := (acc.sumOfSquares - (acc.sum*acc.sum)/countF) / (countF - 1)
					if variance < 0 {
						variance = 0
					}
					resultMap[resultKey] = math.Sqrt(variance)
				}
			}
		}

		encodedResult, err := core.EncodeAggregationResult(resultMap)
		if err != nil {
			it.err = fmt.Errorf("failed to encode downsample result for series %s: %w", seriesKeyStr, err)
			// Clear the buffer and stop, as we're in a bad state.
			it.resultsBuffer = it.resultsBuffer[:0]
			return
		}

		// Create the full key here, using the current window's start time.
		key := make([]byte, len(seriesKeyStr)+8)
		copy(key, []byte(seriesKeyStr))
		core.EncodeTimestamp(key[len(seriesKeyStr):], it.windowStartTime)

		it.resultsBuffer = append(it.resultsBuffer, finalizedResult{key: key, value: encodedResult})
	}
	it.hasDataInWindow = false
}

func (it *MultiFieldDownsamplingIterator) resetWindowState(startTime int64) {
	it.windowStartTime = startTime
	it.windowAggregators = make(map[string]map[string]*downsampleAccumulator)
	it.hasDataInWindow = false
	it.hasNext = false

}

// peekNextPoint looks at the next point from the underlying iterator without consuming it.
// It returns the point's components and a boolean indicating if a point was available.
func (it *MultiFieldDownsamplingIterator) peekNextPoint() ([]byte, []byte, bool) {
	if it.peeked {
		return it.peekedKey, it.peekedValue, true
	}
	if it.underlying.Next() {
		key, value, _, _ := it.underlying.At()
		it.peekedKey = key
		it.peekedValue = value
		it.peeked = true
		return key, value, true
	}
	// If underlying.Next() is false, check for an error immediately.
	if err := it.underlying.Error(); err != nil {
		it.err = err
	}
	return nil, nil, false
}

// consumePoint marks the peeked point as consumed, so the next call to peekNextPoint will advance the underlying iterator.
func (it *MultiFieldDownsamplingIterator) consumePoint() {
	it.peeked = false
	it.peekedKey = nil
	it.peekedValue = nil
}
