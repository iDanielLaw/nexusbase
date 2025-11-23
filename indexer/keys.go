package indexer

import "encoding/binary"

// EncodeTagIndexKey encodes a tag key/value pair into the on-disk SSTable key
// format used by the TagIndexManager.
//
// The format is a fixed 16-byte big-endian key: keyID(8) | valueID(8).
// This layout is chosen to make lexicographic key ordering follow numeric
// ordering of (keyID, valueID) pairs and is referenced from
// `docs/index-disk-format.md`.
func EncodeTagIndexKey(keyID, valueID uint64) []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], keyID)
	binary.BigEndian.PutUint64(buf[8:16], valueID)
	return buf
}

// Backwards-compatible alias used by tests and older callers inside the package.
func encodeTagIndexKey(keyID, valueID uint64) []byte {
	return EncodeTagIndexKey(keyID, valueID)
}
