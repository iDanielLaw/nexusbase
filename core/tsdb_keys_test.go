package core

import (
	"bytes"
	"encoding/binary"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestEncodeTSDBKey(t *testing.T) {
	ts := time.Now().UnixNano()

	testCases := []struct {
		name       string
		metricID   uint64
		sortedTags []EncodedSeriesTagPair
		timestamp  int64
		expected   []byte
	}{
		{
			name:     "with multiple tags",
			metricID: 1,
			sortedTags: []EncodedSeriesTagPair{
				{KeyID: 10, ValueID: 100}, // dc=us-east-1
				{KeyID: 20, ValueID: 200}, // host=server1
			},
			timestamp: ts,
			expected: func() []byte {
				var buf bytes.Buffer
				binary.Write(&buf, binary.BigEndian, uint64(1))
				binary.Write(&buf, binary.BigEndian, uint16(2))
				binary.Write(&buf, binary.BigEndian, uint64(10))
				binary.Write(&buf, binary.BigEndian, uint64(100))
				binary.Write(&buf, binary.BigEndian, uint64(20))
				binary.Write(&buf, binary.BigEndian, uint64(200))
				binary.Write(&buf, binary.BigEndian, uint64(ts))
				return buf.Bytes()
			}(),
		},
		{
			name:       "with no tags",
			metricID:   2,
			sortedTags: []EncodedSeriesTagPair{},
			timestamp:  ts,
			expected: func() []byte {
				var buf bytes.Buffer
				binary.Write(&buf, binary.BigEndian, uint64(2))
				binary.Write(&buf, binary.BigEndian, uint16(0))
				binary.Write(&buf, binary.BigEndian, uint64(ts))
				return buf.Bytes()
			}(),
		},
		{
			name:       "with nil tags",
			metricID:   3,
			sortedTags: nil,
			timestamp:  ts,
			expected: func() []byte {
				var buf bytes.Buffer
				binary.Write(&buf, binary.BigEndian, uint64(3))
				binary.Write(&buf, binary.BigEndian, uint16(0))
				binary.Write(&buf, binary.BigEndian, uint64(ts))
				return buf.Bytes()
			}(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := EncodeTSDBKey(tc.metricID, tc.sortedTags, tc.timestamp)
			if !bytes.Equal(got, tc.expected) {
				t.Errorf("EncodeTSDBKey() = %x, want %x", got, tc.expected)
			}
		})
	}
}

func TestEncodeDecodeSeriesKey(t *testing.T) {
	testCases := []struct {
		name       string
		metricID   uint64
		sortedTags []EncodedSeriesTagPair
	}{
		{
			name:     "with multiple tags",
			metricID: 1,
			sortedTags: []EncodedSeriesTagPair{
				{KeyID: 10, ValueID: 100},
				{KeyID: 20, ValueID: 200},
			},
		},
		{
			name:       "with no tags",
			metricID:   2,
			sortedTags: []EncodedSeriesTagPair{},
		},
		{
			name:       "with nil tags",
			metricID:   3,
			sortedTags: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := EncodeSeriesKey(tc.metricID, tc.sortedTags)
			decodedMetricID, decodedTags, err := DecodeSeriesKey(encoded)
			if err != nil {
				t.Fatalf("DecodeSeriesKey() returned an unexpected error: %v", err)
			}
			if decodedMetricID != tc.metricID {
				t.Errorf("MetricID mismatch: got %d, want %d", decodedMetricID, tc.metricID)
			}

			if len(decodedTags) == 0 && len(tc.sortedTags) == 0 {
			} else if !reflect.DeepEqual(decodedTags, tc.sortedTags) {
				t.Errorf("Tags mismatch:\nGot:    %+v\nWanted: %+v", decodedTags, tc.sortedTags)
			}
		})
	}
}

func TestEncodeDecodeTSDBValue(t *testing.T) {
	testCases := []struct {
		name  string
		value float64
	}{
		{"positive float", 123.456},
		{"negative float", -987.654},
		{"zero", 0.0},
		{"large float", 1.23456789e30},
		{"small float", 1.23456789e-30},
		{"infinity", math.Inf(1)},
		{"negative infinity", math.Inf(-1)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := EncodeTSDBValue(tc.value)
			decoded, err := DecodeTSDBValue(encoded)
			if err != nil {
				t.Fatalf("DecodeTSDBValue() returned an unexpected error: %v", err)
			}
			if decoded != tc.value {
				t.Errorf("Round trip failed: got %f, want %f", decoded, tc.value)
			}
		})
	}

	t.Run("NaN", func(t *testing.T) {
		nan := math.NaN()
		encoded := EncodeTSDBValue(nan)
		decoded, err := DecodeTSDBValue(encoded)
		if err != nil {
			t.Fatalf("DecodeTSDBValue() for NaN returned an unexpected error: %v", err)
		}
		if !math.IsNaN(decoded) {
			t.Errorf("Round trip for NaN failed: got %f, want NaN", decoded)
		}
	})

	t.Run("decode error wrong length", func(t *testing.T) {
		_, err := DecodeTSDBValue([]byte{1, 2, 3})
		if err == nil {
			t.Error("DecodeTSDBValue() expected an error for wrong length, but got nil")
		}
	})
}

func TestEncodeDecodeTimestamp(t *testing.T) {
	ts := time.Date(2025, 1, 2, 3, 4, 5, 6, time.UTC).UnixNano()
	b := make([]byte, 8)

	err := EncodeTimestamp(b, ts)
	if err != nil {
		t.Fatalf("EncodeTimestamp() returned an unexpected error: %v", err)
	}

	decodedTs, err := DecodeTimestamp(b)
	if err != nil {
		t.Fatalf("DecodeTimestamp() returned an unexpected error: %v", err)
	}

	if decodedTs != ts {
		t.Errorf("Round trip failed: got %d, want %d", decodedTs, ts)
	}

	t.Run("decode error short buffer", func(t *testing.T) {
		shortBuf := []byte{1, 2, 3}
		_, err := DecodeTimestamp(shortBuf)
		if err == nil {
			t.Error("DecodeTimestamp() expected an error for short buffer, but got nil")
		}
	})

	t.Run("encode error short buffer", func(t *testing.T) {
		shortBuf := make([]byte, 7)
		err := EncodeTimestamp(shortBuf, ts)
		if err == nil {
			t.Error("EncodeTimestamp() expected an error for short buffer, but got nil")
		}
	})
}

func TestValidateMetricName(t *testing.T) {
	valid := NewValidator()
	testCases := []struct {
		name    string
		metric  string
		wantErr bool
	}{
		{"valid simple", "cpu.usage", false},
		{"valid with underscore", "disk_io_total", false},
		{"valid with colon", "node:cpu_seconds_total", false},
		{"valid starts with underscore", "_internal_metric", false},
		{"valid starts with unicode letter (Thai)", "อุณหภูมิ", false},
		{"valid starts with unicode letter (Cyrillic)", "метрика", false},
		{"valid starts with unicode letter (Japanese)", "測定値", false},
		{"valid starts with unicode letter (Arabic)", "مقياس", false},
		{"valid starts with colon", ":custom_metric", false},
		{"valid with numbers", "metric_2024", false},
		{"empty", "", true},
		{"invalid starts with hyphen", "-metric", true},
		{"invalid contains special char", "metric.with.dot!", true},
		{"invalid contains space", "metric with space", true},
		{"invalid contains slash", "metric/slash", true},                  // Still invalid as '/' is not allowed by pattern
		{"valid contains unicode combining mark (Thai)", "เมตริก", false}, // 'ิ' is \p{M}, allowed by pattern
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateMetricAndTags(valid, tc.metric, nil)
			if (err != nil) != tc.wantErr {
				t.Errorf("ValidateMetricName('%s') error = %v, wantErr %v", tc.metric, err, tc.wantErr)
			}
		})
	}
}

func TestValidateLabelName(t *testing.T) {
	valid := NewValidator()
	testCases := []struct {
		name    string
		label   string
		wantErr bool
	}{
		{"valid simple", "host", false},
		{"valid with underscore", "instance_id", false},
		{"valid with colon", "job:name", false},
		{"valid starts with underscore", "_private_label", false},
		{"valid starts with colon", ":label", false},
		{"valid with numbers", "version_1", false},
		{"valid starts with unicode letter (Thai)", "แท็ก", false},
		{"valid starts with unicode letter (Cyrillic)", "хост", false},
		{"valid starts with unicode letter (Japanese)", "ホスト", false},
		{"valid starts with unicode letter (Arabic)", "مضيف", false},
		{"empty", "", true},
		{"invalid starts with hyphen", "-label", true},
		{"invalid contains special char", "label.with.dot!", true},
		{"invalid contains space", "label with space", true},
		{"invalid contains slash", "label/slash", true},
		{"reserved name", "__internal_id", true},
		{"reserved name with colon", "__name__:test", true}, // Reserved names cannot start with __
		{"not reserved (single underscore)", "_label", false},
		{"not reserved (double underscore in middle)", "my__label", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// The label to test is `tc.label`. It should be the key in the map.
			// The value can be any valid string.
			err := ValidateMetricAndTags(valid, "metric", map[string]string{tc.label: "valid_value"})
			if (err != nil) != tc.wantErr {
				t.Errorf("ValidateLabelName('%s') error = %v, wantErr %v", tc.label, err, tc.wantErr)
			}
		})
	}
}

func TestValidateLabelValue(t *testing.T) {
	testCases := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{"valid simple", "value1", false},
		{"valid with spaces", "my value", false},
		{"valid with special chars", "value!@#$%^&*()", false},
		{"valid empty string", "", false}, // Current spec allows empty
		{"valid UTF-8", "ค่าภาษาไทย", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateLabelValue(tc.value)
			if (err != nil) != tc.wantErr {
				t.Errorf("ValidateLabelValue('%s') error = %v, wantErr %v", tc.value, err, tc.wantErr)
			}
		})
	}
}
