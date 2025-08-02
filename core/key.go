package core

import "fmt"

// ExtractSeriesKeyFromInternalKey is a helper to get the series part of a TSDB key.
func ExtractSeriesKeyFromInternalKey(internalKey []byte) []byte {
	// The series identifier is the entire key minus the last 8 bytes (timestamp).
	if len(internalKey) <= 8 {
		return nil // Invalid key
	}
	return internalKey[:len(internalKey)-8]
}

// ExtractSeriesKeyFromInternalKeyWithErr is a helper to get the series part of a TSDB key, returning an error for invalid keys.
func ExtractSeriesKeyFromInternalKeyWithErr(internalKey []byte) ([]byte, error) {
	// The series identifier is the entire key minus the last 8 bytes (timestamp).
	if len(internalKey) <= 8 {
		return nil, fmt.Errorf("invalid internal key: too short (len %d)", len(internalKey))
	}
	return internalKey[:len(internalKey)-8], nil
}
