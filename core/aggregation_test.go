package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeAggregationResult(t *testing.T) {
	testCases := []struct {
		name    string
		results map[string]float64
	}{
		{
			name: "non-empty map",
			results: map[string]float64{
				"sum_value":   150.5,
				"count_value": 10,
				"avg_value":   15.05,
			},
		},
		{
			name:    "empty map",
			results: map[string]float64{},
		},
		{
			name:    "nil map",
			results: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := EncodeAggregationResult(tc.results)
			require.NoError(t, err)
			require.NotNil(t, encoded)

			decoded, err := DecodeAggregationResult(encoded)
			require.NoError(t, err)

			if tc.results == nil {
				require.Empty(t, decoded, "Decoding result of nil map should be an empty map")
			} else {
				assert.Equal(t, tc.results, decoded)
			}
		})
	}
}

func TestDecodeAggregationResult_ErrorCases(t *testing.T) {
	testCases := []struct {
		name       string
		input      []byte
		errContain string
	}{
		{
			name:       "nil slice",
			input:      nil,
			errContain: "EOF", // binary.Read on empty reader returns EOF
		},
		{
			name:       "empty slice",
			input:      []byte{},
			errContain: "EOF",
		},
		{
			name:       "slice too short for count",
			input:      []byte{0x00},
			errContain: "unexpected EOF",
		},
		{
			name:       "corrupt count (claims 1 item but no data)",
			input:      []byte{0x00, 0x01},
			errContain: "failed to read key length",
		},
		{
			name:       "corrupt key length",
			input:      []byte{0x00, 0x01, 0x00}, // count=1, but keylen is only 1 byte
			errContain: "unexpected EOF",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeAggregationResult(tc.input)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errContain)
		})
	}
}
