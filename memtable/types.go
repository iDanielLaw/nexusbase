package memtable

import (
	"bytes"
	"encoding/binary"

	"github.com/INLOpen/nexusbase/core"
)

// MemtableKey represents a unique key in the memtable.
// Keys are sorted first by the user key (Key), then by PointID in descending order.
// This ensures that for any given key, the latest version (highest PointID) appears first.
type MemtableKey struct {
	Key     []byte // User-provided key
	PointID uint64 // Sequence number for MVCC (Multi-Version Concurrency Control)
}

// Bytes serializes the MemtableKey into a byte slice.
// Format: [Key bytes][PointID as BigEndian uint64]
func (mk *MemtableKey) Bytes() []byte {
	buf := make([]byte, len(mk.Key)+8) // 8 bytes for uint64
	copy(buf, mk.Key)
	binary.BigEndian.PutUint64(buf[len(mk.Key):], mk.PointID)
	return buf
}

// MemtableEntry represents a single key-value operation in the memtable.
// It can be either a Put operation (EntryTypePutEvent) or a Delete operation (EntryTypeDelete).
type MemtableEntry struct {
	Key       []byte         // User-provided key
	Value     []byte         // Encoded value (nil for tombstones)
	EntryType core.EntryType // Type of operation (Put or Delete)
	PointID   uint64         // Sequence number matching the key's PointID
}

// Size returns the estimated memory size of the entry in bytes.
// This is used for tracking memtable size and determining when to flush.
// Uses binary.MaxVarintLen64 (10 bytes) for PointID to match the original calculation.
func (e *MemtableEntry) Size() int64 {
	// Key + Value + PointID (MaxVarintLen64 = 10 bytes) + EntryType (1 byte)
	return int64(len(e.Key) + len(e.Value) + binary.MaxVarintLen64 + 1)
}

// comparator defines the sort order for MemtableKey objects in the skip list.
// Sorting rules:
// 1. Primary: Sort by Key (ascending, lexicographic)
// 2. Secondary: Sort by PointID (descending - newer versions first)
//
// This ensures that:
// - Keys are ordered lexicographically
// - For duplicate keys, the newest version (highest PointID) comes first
// - Get operations can quickly find the latest version
func comparator(a, b *MemtableKey) int {
	// Compare keys first
	cmp := bytes.Compare(a.Key, b.Key)
	if cmp != 0 {
		return cmp
	}

	// Keys are equal, compare PointIDs (descending)
	// Higher PointID should come first, so it's "less than" in our sort order
	if a.PointID > b.PointID {
		return -1 // a comes before b
	}
	if a.PointID < b.PointID {
		return 1 // b comes before a
	}

	return 0 // Identical keys and PointIDs
}
