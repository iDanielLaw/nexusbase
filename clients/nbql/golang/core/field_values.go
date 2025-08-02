package core

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"

	"github.com/bits-and-blooms/bitset"
)

// FieldValues represents a collection of field key-value pairs for a single data point.
// It is optimized for efficient serialization and deserialization.
type FieldValues map[string]PointValue

// NewFieldValuesFromMap creates a FieldValues map from a standard map[string]interface{}.
// It converts the interface{} values into the appropriate PointValue type.
func NewFieldValuesFromMap(m map[string]interface{}) (FieldValues, error) {
	if m == nil {
		return nil, nil
	}
	fv := make(FieldValues, len(m))
	for k, v := range m {
		pv, err := NewPointValue(v)
		if err != nil {
			return nil, fmt.Errorf("failed to create point value for field '%s': %w", k, err)
		}
		fv[k] = pv
	}
	return fv, nil
}

// ToMap converts FieldValues back to a standard map[string]interface{}.
func (fv FieldValues) ToMap() map[string]interface{} {
	if fv == nil {
		return nil
	}
	m := make(map[string]interface{}, len(fv))
	for k, v := range fv {
		m[k] = v.Value()
	}
	return m
}

// MarshalBinary serializes the FieldValues map into a byte slice.
// The format is:
// 1. Number of fields (uint16)
// 2. For each field:
//    a. Length of key (uint16)
//    b. Key (string)
//    c. Value type (byte)
//    d. Value data (variable size)
func (fv FieldValues) MarshalBinary() ([]byte, error) {
	if fv == nil {
		return nil, nil
	}
	buf := new(bytes.Buffer)

	// Write number of fields
	if err := binary.Write(buf, binary.BigEndian, uint16(len(fv))); err != nil {
		return nil, err
	}

	// To ensure deterministic output, sort keys
	keys := make([]string, 0, len(fv))
	for k := range fv {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		v := fv[k]
		// Write key length and key
		keyBytes := []byte(k)
		if err := binary.Write(buf, binary.BigEndian, uint16(len(keyBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(keyBytes); err != nil {
			return nil, err
		}

		// Write value type
		if err := buf.WriteByte(byte(v.valueType)); err != nil {
			return nil, err
		}

		// Write value data
		switch v.valueType {
		case PointValueFloat64:
			if err := binary.Write(buf, binary.BigEndian, v.data.(float64)); err != nil {
				return nil, err
			}
		case PointValueInt64:
			if err := binary.Write(buf, binary.BigEndian, v.data.(int64)); err != nil {
				return nil, err
			}
		case PointValueString:
			strBytes := []byte(v.data.(string))
			if err := binary.Write(buf, binary.BigEndian, uint32(len(strBytes))); err != nil {
				return nil, err
			}
			if _, err := buf.Write(strBytes); err != nil {
				return nil, err
			}
		case PointValueBool:
			b := byte(0)
			if v.data.(bool) {
				b = 1
			}
			if err := buf.WriteByte(b); err != nil {
				return nil, err
			}
		case PointValueBytes:
			byteSlice := v.data.([]byte)
			if err := binary.Write(buf, binary.BigEndian, uint32(len(byteSlice))); err != nil {
				return nil, err
			}
			if _, err := buf.Write(byteSlice); err != nil {
				return nil, err
			}
		case PointValueBitSet:
			bsBytes, err := v.data.(*bitset.BitSet).MarshalBinary()
			if err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.BigEndian, uint32(len(bsBytes))); err != nil {
				return nil, err
			}
			if _, err := buf.Write(bsBytes); err != nil {
				return nil, err
			}
		}
	}
	return buf.Bytes(), nil
}

// DecodeFields deserializes a byte slice into a FieldValues map.
func DecodeFields(r io.Reader) (FieldValues, error) {
	var numFields uint16
	if err := binary.Read(r, binary.BigEndian, &numFields); err != nil {
		if err == io.EOF {
			return make(FieldValues), nil // Empty map if no data
		}
		return nil, err
	}

	fv := make(FieldValues, numFields)
	for i := 0; i < int(numFields); i++ {
		// Read key
		var keyLen uint16
		if err := binary.Read(r, binary.BigEndian, &keyLen); err != nil {
			return nil, err
		}
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyBytes); err != nil {
			return nil, err
		}
		key := string(keyBytes)

		// Read value type
		typeByte, err := readByte(r)
		if err != nil {
			return nil, err
		}
		valueType := PointValueType(typeByte)

		// Read value data
		var val interface{}
		switch valueType {
		case PointValueFloat64:
			var f float64
			if err := binary.Read(r, binary.BigEndian, &f); err != nil {
				return nil, err
			}
			val = f
		case PointValueInt64:
			var i int64
			if err := binary.Read(r, binary.BigEndian, &i); err != nil {
				return nil, err
			}
			val = i
		case PointValueString:
			var strLen uint32
			if err := binary.Read(r, binary.BigEndian, &strLen); err != nil {
				return nil, err
			}
			strBytes := make([]byte, strLen)
			if _, err := io.ReadFull(r, strBytes); err != nil {
				return nil, err
			}
			val = string(strBytes)
		case PointValueBool:
			b, err := readByte(r)
			if err != nil {
				return nil, err
			}
			val = b == 1
		case PointValueBytes:
			var bytesLen uint32
			if err := binary.Read(r, binary.BigEndian, &bytesLen); err != nil {
				return nil, err
			}
			bytesSlice := make([]byte, bytesLen)
			if _, err := io.ReadFull(r, bytesSlice); err != nil {
				return nil, err
			}
			val = bytesSlice
		case PointValueBitSet:
			var bsLen uint32
			if err := binary.Read(r, binary.BigEndian, &bsLen); err != nil {
				return nil, err
			}
			bsBytes := make([]byte, bsLen)
			if _, err := io.ReadFull(r, bsBytes); err != nil {
				return nil, err
			}
			bs := &bitset.BitSet{}
			if err := bs.UnmarshalBinary(bsBytes); err != nil {
				return nil, err
			}
			val = bs
		default:
			return nil, fmt.Errorf("unknown point value type: %d", valueType)
		}
		fv[key] = PointValue{valueType: valueType, data: val}
	}
	return fv, nil
}

func readByte(r io.Reader) (byte, error) {
	b := make([]byte, 1)
	_, err := io.ReadFull(r, b)
	return b[0], err
}
