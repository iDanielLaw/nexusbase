package nbql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDuration(t *testing.T) {
	testCases := []struct {
		name        string
		input       string
		expected    time.Duration
		expectErr   bool
		errContains string
	}{
		// Standard units (handled by time.ParseDuration)
		{
			name:     "standard hours",
			input:    "2h",
			expected: 2 * time.Hour,
		},
		{
			name:     "standard minutes",
			input:    "30m",
			expected: 30 * time.Minute,
		},
		{
			name:     "standard seconds",
			input:    "15s",
			expected: 15 * time.Second,
		},
		{
			name:     "standard combined",
			input:    "1h10m5s",
			expected: 1*time.Hour + 10*time.Minute + 5*time.Second,
		},

		// Custom units
		{
			name:     "custom days",
			input:    "3d",
			expected: 3 * 24 * time.Hour,
		},
		{
			name:     "custom weeks",
			input:    "2w",
			expected: 2 * 7 * 24 * time.Hour,
		},
		{
			name:     "custom years",
			input:    "1y",
			expected: 1 * 365 * 24 * time.Hour,
		},
		{
			name:     "custom zero days",
			input:    "0d",
			expected: 0,
		},

		// Error cases
		{
			name:        "invalid unit",
			input:       "5x",
			expectErr:   true,
			errContains: `time: unknown unit "x" in duration "5x"`,
		},
		{
			name:        "invalid number part",
			input:       "a1d",
			expectErr:   true,
			errContains: `invalid duration value`,
		},
		{
			name:        "empty string",
			input:       "",
			expectErr:   true,
			errContains: `time: invalid duration ""`,
		},
		{
			name:        "number only",
			input:       "123",
			expectErr:   true,
			errContains: `time: missing unit in duration "123"`, // This error comes from time.ParseDuration
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			duration, err := parseDuration(tc.input)
			if tc.expectErr {
				require.Error(t, err)
				if tc.errContains != "" {
					assert.Contains(t, err.Error(), tc.errContains)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, duration)
			}
		})
	}
}