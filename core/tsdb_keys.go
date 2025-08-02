package core

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"
)

const (
	// NULL_BYTE is used as a separator between components of the TSDB key.
	NULL_BYTE = 0x00
	// TAG_SEPARATOR_BYTE is used to separate individual tag key-value pairs within the serialized tags string.
	TAG_SEPARATOR_BYTE = 0x01
)

// EncodedSeriesTagPair represents a tag key/value pair using their integer IDs.
type EncodedSeriesTagPair struct {
	KeyID   uint64
	ValueID uint64
}

// GetBuffer returns a buffer from the pool.
func GetBuffer() *bytes.Buffer {
	return BufferPool.Get()
}

// PutBuffer returns a buffer to the pool after resetting it.
func PutBuffer(buf *bytes.Buffer) {
	BufferPool.Put(buf)
}

// EncodeSeriesKeyToBuffer writes a canonical series key into the provided buffer.
// The buffer is NOT reset before writing.
func EncodeSeriesKeyToBuffer(buf *bytes.Buffer, metricID uint64, sortedTags []EncodedSeriesTagPair) {
	// 1. Metric ID
	binary.Write(buf, binary.BigEndian, metricID)

	// 2. Tag Count
	binary.Write(buf, binary.BigEndian, uint16(len(sortedTags)))

	// 3. Sorted Tag Pairs
	for _, pair := range sortedTags {
		binary.Write(buf, binary.BigEndian, pair.KeyID)
		binary.Write(buf, binary.BigEndian, pair.ValueID)
	}
}

// EncodeSeriesKey creates a canonical byte key for a series identifier from its component IDs.
// The format is: <metric_id><tag_count><tag_key_id_1><tag_value_id_1>...<tag_key_id_N><tag_value_id_N>
// Tags must be pre-sorted by KeyID.
func EncodeSeriesKey(metricID uint64, sortedTags []EncodedSeriesTagPair) []byte {
	buf := GetBuffer()
	defer PutBuffer(buf)
	EncodeSeriesKeyToBuffer(buf, metricID, sortedTags)
	// Return a copy of the bytes, as the buffer will be reused.
	keyCopy := make([]byte, buf.Len())
	copy(keyCopy, buf.Bytes())
	return keyCopy
}

// EncodeTSDBKeyToBuffer writes a full TSDB data point key into the provided buffer.
// The buffer is NOT reset before writing.
func EncodeTSDBKeyToBuffer(buf *bytes.Buffer, metricID uint64, sortedTags []EncodedSeriesTagPair, timestamp int64) {
	// Write series key part
	EncodeSeriesKeyToBuffer(buf, metricID, sortedTags)
	// Write timestamp part
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(timestamp))
	buf.Write(tsBytes)
}

// EncodeTSDBKey creates an internal byte key from component IDs and a timestamp.
// The format is: <series_key_from_ids><timestamp_bytes>
func EncodeTSDBKey(metricID uint64, sortedTags []EncodedSeriesTagPair, timestamp int64) []byte {
	buf := GetBuffer()
	defer PutBuffer(buf)
	EncodeTSDBKeyToBuffer(buf, metricID, sortedTags, timestamp)
	keyCopy := make([]byte, buf.Len())
	copy(keyCopy, buf.Bytes())
	return keyCopy
}

// DecodeSeriesKey parses a binary series key into its component IDs.
func DecodeSeriesKey(seriesKey []byte) (metricID uint64, tags []EncodedSeriesTagPair, err error) {
	if len(seriesKey) < 10 { // Min length: 8 (metricID) + 2 (tagCount)
		return 0, nil, fmt.Errorf("series key too short: %d bytes", len(seriesKey))
	}
	reader := bytes.NewReader(seriesKey)

	binary.Read(reader, binary.BigEndian, &metricID)

	var tagCount uint16
	binary.Read(reader, binary.BigEndian, &tagCount)
	if tagCount > 0 {
		tags = make([]EncodedSeriesTagPair, tagCount)
		for i := 0; i < int(tagCount); i++ {
			if err := binary.Read(reader, binary.BigEndian, &tags[i].KeyID); err != nil {
				return 0, nil, fmt.Errorf("failed to read tag key id %d: %w", i, err)
			}
			if err := binary.Read(reader, binary.BigEndian, &tags[i].ValueID); err != nil {
				return 0, nil, fmt.Errorf("failed to read tag value id %d: %w", i, err)
			}
		}
	}
	return metricID, tags, nil
}

// EncodeTSDBKeyWithString creates an internal byte key from metric name, tags, and timestamp.
// This is the legacy version using strings.
// Deprecated: Use ID-based encoding functions like EncodeTSDBKey for better performance and storage efficiency.
//   - timestamp_bytes: Timestamp (int64) is converted to an 8-byte big-endian uint64.
//     Using big-endian ensures that byte-wise sorting of keys also sorts timestamps correctly for positive values.
func EncodeTSDBKeyWithString(metric string, tags map[string]string, timestamp int64) []byte {
	var keyBuffer bytes.Buffer

	// 1. Metric Name
	keyBuffer.WriteString(metric)
	keyBuffer.WriteByte(NULL_BYTE)

	// 2. Serialized Sorted Tags
	if len(tags) > 0 {
		tagKeys := make([]string, 0, len(tags))
		for k := range tags {
			tagKeys = append(tagKeys, k)
		}
		sort.Strings(tagKeys) // Sort tag keys for consistent serialization

		var tagParts []string
		for _, k := range tagKeys {
			tagParts = append(tagParts, k+"="+tags[k])
		}
		serializedTags := strings.Join(tagParts, string([]byte{TAG_SEPARATOR_BYTE}))
		keyBuffer.WriteString(serializedTags)
	}
	keyBuffer.WriteByte(NULL_BYTE)

	// 3. Timestamp
	// Convert int64 timestamp to uint64 for big-endian byte representation.
	// For time series, timestamps are typically positive. If negative timestamps
	// were possible and needed to sort correctly, a different encoding (like two's complement
	// with sign bit flipped for positive, or a more complex scheme) might be needed.
	// For typical Unix timestamps (positive), direct big-endian uint64 works for sorting.
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(timestamp))
	keyBuffer.Write(tsBytes)

	return keyBuffer.Bytes()
}

// ExtractSeriesIdentifierFromTSDBKeyWithString extracts the series identifier part (metric + tags)
// from a fully encoded TSDB key.
// Deprecated: Use ID-based key manipulation functions.
// This function returns the "metric_name<NULL_BYTE>serialized_sorted_tags" part.
// It can also handle a pure series key (metric_name<NULL_BYTE>serialized_sorted_tags)
// by returning the key itself.
func ExtractSeriesIdentifierFromTSDBKeyWithString(key []byte) ([]byte, error) {
	// Find the first NULL_BYTE (separator after metric name)
	idx1 := bytes.IndexByte(key, NULL_BYTE)
	if idx1 == -1 {
		return nil, fmt.Errorf("malformed TSDB key: missing first NULL_BYTE separator")
	}

	// Check if there's a second NULL_BYTE (indicating a full TSDB key with timestamp)
	// Search from idx1 + 1 to avoid finding the same NULL_BYTE again.
	idx2 := bytes.IndexByte(key[idx1+1:], NULL_BYTE)

	if idx2 == -1 {
		// If no second NULL_BYTE is found, it means the key is likely a pure series key
		// (metric<NULL_BYTE>tags) without a timestamp.
		// In this case, the entire key is the series identifier.
		return key, nil
	} else {
		// If a second NULL_BYTE is found, it's a full TSDB key.
		// The series identifier ends at the second NULL_BYTE.
		// idx2 is relative to key[idx1+1:], so add (idx1+1) to get absolute index.
		absIdx2 := idx1 + 1 + idx2
		return key[:absIdx2], nil
	}
}

// EncodeSeriesKeyWithString creates a canonical byte key for a series identifier (metric + tags).
// Deprecated: Use ID-based encoding functions like EncodeSeriesKey.
// The format is: metric_name<NULL_BYTE>serialized_sorted_tags
// This key is used for series-level operations like marking a series as deleted.
func EncodeSeriesKeyWithString(metric string, tags map[string]string) []byte {
	var keyBuffer bytes.Buffer

	// 1. Metric Name
	keyBuffer.WriteString(metric)
	keyBuffer.WriteByte(NULL_BYTE)

	// 2. Serialized Sorted Tags
	if len(tags) > 0 {
		tagKeys := make([]string, 0, len(tags))
		for k := range tags {
			tagKeys = append(tagKeys, k)
		}
		sort.Strings(tagKeys) // Sort tag keys for consistent serialization

		var tagParts []string
		for _, k := range tagKeys {
			tagParts = append(tagParts, k+"="+tags[k])
		}
		serializedTags := strings.Join(tagParts, string([]byte{TAG_SEPARATOR_BYTE}))
		keyBuffer.WriteString(serializedTags)
	}
	// No trailing NULL_BYTE or timestamp for a series key.
	return keyBuffer.Bytes()
}

// ExtractMetricFromSeriesKeyWithString extracts the metric part from an encoded series key.
// The series key format is: metric_name<NULL_BYTE>serialized_sorted_tags.
// Deprecated: Use DecodeSeriesKey and the StringStore to get the metric name from an ID-based key.
func ExtractMetricFromSeriesKeyWithString(seriesKey []byte) ([]byte, error) {
	// Find the first NULL_BYTE (separator after metric name)
	idx := bytes.IndexByte(seriesKey, NULL_BYTE)
	if idx == -1 {
		// If no NULL_BYTE is found, it means the entire seriesKey is the metric.
		// This might happen for series keys that only contain a metric and no tags.
		// Or it could be a malformed key. For robustness, return the whole key.
		return seriesKey, nil // Or return an error if strict validation is needed
	}
	return seriesKey[:idx], nil
}

// ExtractTagsFromSeriesKeyWithString extracts the tags map from an encoded series key.
// The series key format is: metric_name<NULL_BYTE>serialized_sorted_tags.
func ExtractTagsFromSeriesKeyWithString(seriesKey []byte) (map[string]string, error) {
	idx := bytes.IndexByte(seriesKey, NULL_BYTE)
	if idx == -1 {
		return nil, fmt.Errorf("malformed series key: missing NULL_BYTE separator")
	}
	tagsPart := seriesKey[idx+1:]
	if len(tagsPart) == 0 {
		return nil, nil // No tags
	}

	tags := make(map[string]string)
	tagPairs := bytes.Split(tagsPart, []byte{TAG_SEPARATOR_BYTE})
	for _, pair := range tagPairs {
		kv := bytes.SplitN(pair, []byte("="), 2)
		if len(kv) == 2 {
			tags[string(kv[0])] = string(kv[1])
		} else {
			return nil, fmt.Errorf("malformed tag pair in series key: %s", string(pair))
		}
	}
	return tags, nil
}

// EncodeTSDBValue converts a float64 value into a byte slice.
// It uses the IEEE 754 binary representation of the float64.
func EncodeTSDBValue(value float64) []byte {
	valBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(valBytes, math.Float64bits(value))
	return valBytes
}

// DecodeTSDBValue converts a byte slice back into a float64 value.
func DecodeTSDBValue(valBytes []byte) (float64, error) {
	if len(valBytes) != 8 {
		return 0, fmt.Errorf("value bytes must be 8 bytes long, got %d", len(valBytes))
	}
	bits := binary.BigEndian.Uint64(valBytes)
	return math.Float64frombits(bits), nil
}

// DecodeTimestamp decodes timestamp from a byte slice (assuming 8 bytes, BigEndian).
func DecodeTimestamp(b []byte) (int64, error) {
	if len(b) < 8 {
		return 0, fmt.Errorf("cannot decode timestamp, buffer too short: got %d bytes, want 8", len(b))
	}
	return int64(binary.BigEndian.Uint64(b)), nil
}

// EncodeTimestamp encodes timestamp into a byte slice (8 bytes, BigEndian).
// Returns an error if the buffer is too short.
func EncodeTimestamp(b []byte, ts int64) error {
	if len(b) < 8 {
		return fmt.Errorf("cannot encode timestamp, buffer too short: got %d bytes, want 8", len(b))
	}
	binary.BigEndian.PutUint64(b, uint64(ts))
	return nil
}

// ExtractMetricAndTagsFromSeriesKeyWithString extracts both the metric and tags from an encoded series key.
// This is a convenience function for cases where both are needed.
// Deprecated: Use DecodeSeriesKey and the StringStore.
func ExtractMetricAndTagsFromSeriesKeyWithString(seriesKey []byte) (string, map[string]string, error) {
	metricBytes, err := ExtractMetricFromSeriesKeyWithString(seriesKey)
	if err != nil {
		return "", nil, err
	}
	tags, err := ExtractTagsFromSeriesKeyWithString(seriesKey)
	return string(metricBytes), tags, err
}

// ValidateLabelValue checks if the label value adheres to the defined naming conventions (FR3.1.6).
func ValidateLabelValue(value string) error {
	// Currently, label values can be any UTF-8 string. We might add more restrictions later.
	return nil
}
