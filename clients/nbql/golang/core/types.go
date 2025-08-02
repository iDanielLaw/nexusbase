package core

import (
	"fmt"

	"github.com/bits-and-blooms/bitset"
)

// PointValueType defines the type of data stored in a PointValue.
type PointValueType byte

const (
	PointValueFloat64 PointValueType = 1
	PointValueInt64   PointValueType = 2
	PointValueString  PointValueType = 3
	PointValueBool    PointValueType = 4
	PointValueBytes   PointValueType = 5
	PointValueBitSet  PointValueType = 6
)

// PointValue is a container for a single value in a data point's fields.
// It holds the type and the actual data.
type PointValue struct {
	valueType PointValueType
	data      interface{}
}

// NewPointValue creates a new PointValue from a generic interface{}.
// It automatically detects the type and stores it.
func NewPointValue(v interface{}) (PointValue, error) {
	switch val := v.(type) {
	case float64:
		return PointValue{valueType: PointValueFloat64, data: val}, nil
	case int64:
		return PointValue{valueType: PointValueInt64, data: val}, nil
	case int:
		return PointValue{valueType: PointValueInt64, data: int64(val)}, nil
	case string:
		return PointValue{valueType: PointValueString, data: val}, nil
	case bool:
		return PointValue{valueType: PointValueBool, data: val}, nil
	case []byte:
		return PointValue{valueType: PointValueBytes, data: val}, nil
	case *bitset.BitSet:
		return PointValue{valueType: PointValueBitSet, data: val}, nil
	default:
		return PointValue{}, fmt.Errorf("unsupported value type: %T", v)
	}
}

// Value returns the underlying data as an interface{}.
func (pv PointValue) Value() interface{} {
	return pv.data
}

// ValueFloat64 returns the float64 value and a boolean indicating if the type was correct.
func (pv PointValue) ValueFloat64() (float64, bool) {
	if pv.valueType != PointValueFloat64 {
		return 0, false
	}
	return pv.data.(float64), true
}

// ValueInt64 returns the int64 value and a boolean indicating if the type was correct.
func (pv PointValue) ValueInt64() (int64, bool) {
	if pv.valueType != PointValueInt64 {
		return 0, false
	}
	return pv.data.(int64), true
}

// ValueString returns the string value and a boolean indicating if the type was correct.
func (pv PointValue) ValueString() (string, bool) {
	if pv.valueType != PointValueString {
		return "", false
	}
	return pv.data.(string), true
}
