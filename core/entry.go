package core

import (
	"encoding/binary"
	"fmt"
)

// EntryType defines the type of an entry in the WAL or SSTable.
type EntryType byte

const (
	// EntryTypeDelete represents a tombstone for a single key (point deletion).
	EntryTypeDelete EntryType = 'D'
	// EntryTypeDeleteSeries represents a tombstone for an entire series.
	EntryTypeDeleteSeries EntryType = 'S'
	// EntryTypePutEvent represents a structured event with multiple fields.
	EntryTypePutEvent EntryType = 'E'
	// EntryTypePutBatch represents a batch of multiple Put/Delete entries written atomically to the WAL.
	EntryTypePutBatch EntryType = 'B'
	// EntryTypeDeleteRange represents a tombstone for a time range within a series.
	EntryTypeDeleteRange EntryType = 'R'
)

// EncodeRangeTombstoneValue encodes the start and end timestamps for a range tombstone.
func EncodeRangeTombstoneValue(startTime, endTime int64) []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], uint64(startTime))
	binary.BigEndian.PutUint64(buf[8:16], uint64(endTime))
	return buf
}

// DecodeRangeTombstoneValue decodes the start and end timestamps from a range tombstone value.
func DecodeRangeTombstoneValue(value []byte) (startTime, endTime int64, err error) {
	if len(value) != 16 {
		return 0, 0, fmt.Errorf("invalid range tombstone value length: got %d, want 16", len(value))
	}
	startTime = int64(binary.BigEndian.Uint64(value[0:8]))
	endTime = int64(binary.BigEndian.Uint64(value[8:16]))
	return startTime, endTime, nil
}
