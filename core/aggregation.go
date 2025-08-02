package core

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// MakeKeyAggregator creates a key for an aggregated result by combining
// the aggregation function and field name (e.g., "sum_value").
func MakeKeyAggregator(agg AggregationSpec) string {
	if agg.Alias != "" {
		return agg.Alias
	}
	return fmt.Sprintf("%s_%s", agg.Function.String(), agg.Field)
}

// EncodeAggregationResult serializes a map of aggregation results into a byte slice.
func EncodeAggregationResult(results map[string]float64) ([]byte, error) {
	buf := BufferPool.Get()
	buf.Reset() // Ensure the buffer is clean before use
	defer BufferPool.Put(buf)

	if err := binary.Write(buf, binary.BigEndian, uint16(len(results))); err != nil {
		return nil, err
	}
	for key, val := range results {
		keyBytes := []byte(key)
		if err := binary.Write(buf, binary.BigEndian, uint16(len(keyBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(keyBytes); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, val); err != nil {
			return nil, err
		}
	}
	// Create a copy of the data, as the buffer will be reused.
	dataCopy := make([]byte, buf.Len())
	copy(dataCopy, buf.Bytes())
	return dataCopy, nil
}

// DecodeAggregationResult deserializes a byte slice into a map of aggregation results.
func DecodeAggregationResult(data []byte) (map[string]float64, error) {
	// The check for nil is removed as per review.
	// bytes.NewReader(nil) is safe and will result in an io.EOF on the first read,
	// which is the desired error behavior for invalid/empty input, consistent with `[]byte{}`.
	buf := bytes.NewReader(data)
	var count uint16
	if err := binary.Read(buf, binary.BigEndian, &count); err != nil {
		return nil, fmt.Errorf("failed to read aggregation result count: %w", err)
	}
	results := make(map[string]float64, count)
	for i := 0; i < int(count); i++ {
		var keyLen uint16
		if err := binary.Read(buf, binary.BigEndian, &keyLen); err != nil {
			return nil, fmt.Errorf("failed to read key length for item %d: %w", i, err)
		}
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(buf, keyBytes); err != nil {
			return nil, fmt.Errorf("failed to read key for item %d: %w", i, err)
		}
		var val float64
		if err := binary.Read(buf, binary.BigEndian, &val); err != nil {
			return nil, fmt.Errorf("failed to read value for item %d (key: %s): %w", i, string(keyBytes), err)
		}
		results[string(keyBytes)] = val
	}
	return results, nil
}
