package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFieldValues_EncodeDecode(t *testing.T) {
	// Helper to create a PointValue and panic on error, simplifying test setup.
	mustNewPointValue := func(data any) PointValue {
		pv, err := NewPointValue(data)
		if err != nil {
			panic(err)
		}
		return pv
	}

	testCases := []struct {
		name   string
		fields FieldValues
	}{
		{
			name: "full event with all types",
			fields: FieldValues{
				"status_code": mustNewPointValue(int64(200)),
				"latency_ms":  mustNewPointValue(123.45),
				"method":      mustNewPointValue("GET"),
				"cached":      mustNewPointValue(true),
				"user_id":     mustNewPointValue(nil),
				"message":     mustNewPointValue("สวัสดีชาวโลก"), // Unicode test
			},
		},
		{
			name: "event with a single field",
			fields: FieldValues{
				"temperature": mustNewPointValue(float32(25.5)), // Test float32 promotion
			},
		},
		{
			name:   "event with no fields",
			fields: FieldValues{},
		},
		{
			name: "event with empty string and false bool",
			fields: FieldValues{
				"notes":    mustNewPointValue(""),
				"is_admin": mustNewPointValue(false),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 1. Encode the fields map
			encodedBytes, err := tc.fields.Encode()
			require.NoError(t, err, "Encode() should not produce an error")

			// 2. Decode the byte slice back into a map
			decodedFields, err := DecodeFieldsFromBytes(encodedBytes)
			require.NoError(t, err, "DecodeFieldsFromBytes() should not produce an error")

			// 3. Compare the original and decoded maps
			require.Equal(t, len(tc.fields), len(decodedFields), "Number of fields should match")

			// Deep comparison of the maps
			// We can't use assert.Equal directly on the maps because the order of iteration is not guaranteed.
			// So we iterate through the original and check each key in the decoded map.
			for key, originalPV := range tc.fields {
				decodedPV, ok := decodedFields[key]
				require.True(t, ok, "Key '%s' should exist in the decoded map", key)

				assert.Equal(t, originalPV.valueType, decodedPV.valueType, "ValueType for key '%s' should match", key)
				assert.Equal(t, originalPV.data, decodedPV.data, "Data for key '%s' should match", key)
			}
		})
	}
}
