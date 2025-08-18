package sstable

// format.go: Constants for SSTable file format, entry types, etc.
// Placeholder for SSTable format definitions.
// Details to be filled based on FR4.1.

import "errors"

// MagicString is a unique identifier placed at the end of an SSTable file.
// Used for basic file corruption detection (FR7.2).
const MagicString = "LSMT-SSTABLE-V1"

// MagicStringLen is the length of the MagicString.
const MagicStringLen = len(MagicString)

// Size constants for lengths in the file format.
const (
	KeyLenSize    = 4 // uint32 for key length
	ValueLenSize  = 4 // uint32 for value length
	EntryTypeSize = 1 // byte for entry type

	// Footer component sizes
	IndexOffsetSize       = 8 // uint64 for index offset
	IndexLenSize          = 4 // uint32 for index length
	BloomFilterOffsetSize = 8 // uint64 for bloom filter offset
	BloomFilterLenSize    = 4 // uint32 for bloom filter length
	MinKeyOffsetSize      = 8 // uint64 for min key offset
	MinKeyLenSize         = 4 // uint32 for min key length
	MaxKeyOffsetSize      = 8 // uint64 for max key offset
	MaxKeyLenSize         = 4 // uint32 for max key length
	KeyCountSize          = 8 // uint64 for the total number of entries
	TombstoneCountSize    = 8 // uint64 for the total number of tombstone entries
	BlockLengthSize       = 4 // uint32 for block length (used in block index)
	// MagicStringLen is already defined
)

// DefaultBlockSize specifies the target size for data blocks in bytes.
const DefaultBlockSize = 4 * 1024 // 4KB

// DefaultRestartPointInterval specifies how often a restart point is stored.
const DefaultRestartPointInterval = 16

// FooterSize is the total fixed size of the footer excluding the magic string.
// Index(12) + Bloom(12) + MinKey(12) + MaxKey(12) + KeyCount(8) + TombstoneCount(8)
const FooterFixedComponentSize = IndexOffsetSize + IndexLenSize + BloomFilterOffsetSize + BloomFilterLenSize + MinKeyOffsetSize + MinKeyLenSize + MaxKeyOffsetSize + MaxKeyLenSize + KeyCountSize + TombstoneCountSize
const FooterSize = FooterFixedComponentSize + MagicStringLen

// ErrNotFound is returned by Get when a key is not found.
var ErrNotFound = errors.New("key not found")
var ErrCorrupted = errors.New("sstable data is corrupted")
var ErrClosed = errors.New("sstable is closed")
