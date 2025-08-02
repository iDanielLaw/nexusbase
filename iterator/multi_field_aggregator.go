package iterator

import (
	"fmt"
	"math"

	"github.com/INLOpen/nexusbase/core"
	"github.com/caio/go-tdigest/v4"
)

// aggregateAccumulator holds the state for all aggregations on a single field.
type aggregateAccumulator struct {
	sum          float64
	sumOfSquares float64
	numericCount uint64
	min          float64
	max          float64
	first        float64
	last         float64
	firstNumeric bool
	nonNullCount uint64
	// For percentile calculations
	td *tdigest.TDigest
}

func newAggregateAccumulator(needsTDigest bool) (*aggregateAccumulator, error) {
	acc := &aggregateAccumulator{
		min: math.Inf(1),
		max: math.Inf(-1),
	}
	if needsTDigest {
		var err error
		acc.td, err = tdigest.New()
		if err != nil {
			return nil, fmt.Errorf("tdigest.New failed: %w", err)
		}
	}
	return acc, nil
}

func (a *aggregateAccumulator) Add(val core.PointValue) error {
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

	// Ignore non-finite numbers for all numeric aggregations.
	if math.IsNaN(numValue) || math.IsInf(numValue, 0) {
		return nil
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

	if a.td != nil {
		if err := a.td.AddWeighted(numValue, 1); err != nil {
			return fmt.Errorf("tdigest AddWeighted failed: %w", err)
		}
	}
	return nil
}

// MultiFieldAggregatingIterator aggregates multiple fields from an underlying iterator.
// It consumes the entire underlying iterator on the first call to Next() and produces a single result.
type MultiFieldAggregatingIterator struct {
	underlying        Interface
	specs             []core.AggregationSpec
	fieldAccumulators map[string]*aggregateAccumulator
	starAccumulator   *aggregateAccumulator // For count(*)
	done              bool
	err               error
	resultKey         []byte
	resultValue       []byte
}

// NewMultiFieldAggregatingIterator creates a new iterator that aggregates multiple fields.
func NewMultiFieldAggregatingIterator(underlying Interface, specs []core.AggregationSpec, resultKey []byte) (*MultiFieldAggregatingIterator, error) {
	if len(specs) == 0 {
		return nil, fmt.Errorf("no aggregation specs provided")
	}

	fieldAccumulators := make(map[string]*aggregateAccumulator)
	var starAccumulator *aggregateAccumulator
	var err error

	// Pre-process specs to determine which accumulators are needed.
	fieldsWithTDigest := make(map[string]bool)
	uniqueFields := make(map[string]bool)
	needsStarCount := false

	for _, spec := range specs {
		if spec.Field == "*" {
			if spec.Function == "count" {
				needsStarCount = true
			}
			continue
		}
		uniqueFields[spec.Field] = true
		if _, isPercentile := parsePercentile(string(spec.Function)); isPercentile {
			fieldsWithTDigest[spec.Field] = true
		}
	}

	// Create accumulators for each unique field.
	for field := range uniqueFields {
		fieldAccumulators[field], err = newAggregateAccumulator(fieldsWithTDigest[field])
		if err != nil {
			return nil, err
		}
	}

	// Create accumulator for count(*) if needed.
	if needsStarCount {
		starAccumulator, err = newAggregateAccumulator(false)
		if err != nil {
			return nil, err
		}
	}

	return &MultiFieldAggregatingIterator{
		underlying:        underlying,
		specs:             specs,
		fieldAccumulators: fieldAccumulators,
		starAccumulator:   starAccumulator,
		resultKey:         resultKey,
	}, nil
}

func (it *MultiFieldAggregatingIterator) Next() bool {
	if it.done || it.err != nil {
		return false
	}

	// This iterator consumes the entire underlying iterator in the first call to Next().
	it.done = true

	for it.underlying.Next() {
		_, valueBytes, _, _ := it.underlying.At()

		if len(valueBytes) == 0 {
			continue
		}

		fields, err := core.DecodeFieldsFromBytes(valueBytes)
		if err != nil {
			it.err = fmt.Errorf("failed to decode fields during aggregation: %w", err)
			return false
		}

		// Handle count(*) for every point seen.
		if it.starAccumulator != nil {
			it.starAccumulator.nonNullCount++
		}

		// Iterate over the required field accumulators and update them if the field exists in the point.
		// This is more efficient if the number of aggregated fields is smaller than the number of fields in the data point.
		for fieldName, acc := range it.fieldAccumulators {
			if fieldVal, ok := fields[fieldName]; ok {
				if err := acc.Add(fieldVal); err != nil {
					it.err = err
					return false // Stop processing on error
				}
			}
		}
	}

	if err := it.underlying.Error(); err != nil {
		it.err = err
		return false
	}

	// After consuming all data, prepare the single result.
	it.prepareResult()

	// Return true this one time to signal the result is ready.
	return true
}

func (it *MultiFieldAggregatingIterator) prepareResult() {
	resultMap := make(map[string]float64)
	for _, spec := range it.specs {
		// Use MakeKeyAggregator to generate the result key, which respects the alias.
		key := core.MakeKeyAggregator(spec)
		var acc *aggregateAccumulator

		if spec.Field == "*" {
			acc = it.starAccumulator
		} else {
			acc = it.fieldAccumulators[spec.Field]
		}

		// If the accumulator doesn't exist (e.g., a field was specified but never appeared in the data),
		// we create a temporary empty one to provide default values (0 for count/sum, NaN for others).
		if acc == nil {
			var err error
			acc, err = newAggregateAccumulator(false)
			if err != nil { // Should not happen
				it.err = err
				return
			}
		}

		// Handle percentile functions first.
		if p, isPercentile := parsePercentile(string(spec.Function)); isPercentile {
			if acc.td != nil && acc.td.Count() > 0 {
				resultMap[key] = acc.td.Quantile(p / 100.0)
			} else {
				resultMap[key] = math.NaN()
			}
			continue
		}

		switch spec.Function {
		case "count":
			resultMap[key] = float64(acc.nonNullCount)
		case "sum":
			resultMap[key] = acc.sum
		case "avg":
			if acc.numericCount > 0 {
				resultMap[key] = acc.sum / float64(acc.numericCount)
			} else {
				resultMap[key] = math.NaN()
			}
		case "min":
			if acc.numericCount > 0 {
				resultMap[key] = acc.min
			} else {
				resultMap[key] = math.NaN()
			}
		case "max":
			if acc.numericCount > 0 {
				resultMap[key] = acc.max
			} else {
				resultMap[key] = math.NaN()
			}
		case "first":
			if acc.numericCount > 0 {
				resultMap[key] = acc.first
			} else {
				resultMap[key] = math.NaN()
			}
		case "last":
			if acc.numericCount > 0 {
				resultMap[key] = acc.last
			} else {
				resultMap[key] = math.NaN()
			}
		case "frac":
			if acc.numericCount < 2 {
				resultMap[key] = math.NaN()
			} else if acc.first == 0 {
				if acc.last == 0 {
					resultMap[key] = 0.0
				} else if acc.last > 0 {
					resultMap[key] = math.Inf(1)
				} else {
					resultMap[key] = math.Inf(-1)
				}
			} else {
				resultMap[key] = (acc.last - acc.first) / acc.first
			}
		case "stddev":
			if acc.numericCount < 2 {
				resultMap[key] = math.NaN()
			} else {
				countF := float64(acc.numericCount)
				variance := (acc.sumOfSquares - (acc.sum*acc.sum)/countF) / (countF - 1)
				if variance < 0 {
					variance = 0
				}
				resultMap[key] = math.Sqrt(variance)
			}
		}
	}

	// Encode the final map into the resultValue
	var err error
	it.resultValue, err = core.EncodeAggregationResult(resultMap)
	if err != nil {
		it.err = fmt.Errorf("failed to encode final aggregation result: %w", err)
	}
}

func (it *MultiFieldAggregatingIterator) At() ([]byte, []byte, core.EntryType, uint64) {
	// The result of an aggregation is a new event-like structure.
	// Using EntryTypePutEvent is consistent with how other data is represented.
	return it.resultKey, it.resultValue, core.EntryTypePutEvent, 0
}

func (it *MultiFieldAggregatingIterator) Error() error {
	return it.err
}

func (it *MultiFieldAggregatingIterator) Close() error {
	return it.underlying.Close()
}
