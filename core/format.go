package core

import (
	"fmt"
	"strconv"
	"strings"
)

// This file centralizes constants related to file formats, magic numbers,
// and other protocol-level identifiers used across the database engine.

// --- Magic Numbers ---
const (
	// WALMagicNumber identifies a Write-Ahead Log segment file.
	WALMagicNumber uint32 = 0xBAADF00D
	// StringStoreMagicNumber identifies the string-to-id mapping file.
	StringStoreMagicNumber uint32 = 0x57524E47 // "STRG"
	// ManifestMagicNumber identifies a binary manifest file.
	ManifestMagicNumber uint32 = 0x424E414D // "MANB" for MANifest Binary
	// SeriesStoreMagicNumber identifies the series-to-id mapping file.
	SeriesStoreMagicNumber uint32 = 0x53455249 // "SERI"
	// TagIndexMagicNumber identifies a tag index file.
	TagIndexMagicNumber uint32 = 0x54414758 // "TAGX"
	// SSTableMagicNumber identifies an SSTable file.
	SSTableMagicNumber uint32 = 0x53535442 // "SSTB"

	CheckpointMagicNumber uint32 = 0x54504B43
)

// --- Magic Strings ---
const (
	// SSTableMagicString is a unique identifier placed at the end of an SSTable file.
	SSTableMagicString    = "LSMT-SSTABLE-V1"
	SSTableMagicStringLen = len(SSTableMagicString)
)

// --- File Names & Prefixes ---
const (
	// CurrentFileName is the name of the file that points to the latest MANIFEST file.
	CurrentFileName = "CURRENT"
	// ManifestFilePrefix is the prefix for manifest files, e.g., MANIFEST_12345.bin
	ManifestFilePrefix = "MANIFEST"
	// NextIDFileName is the name of the file storing the next SSTable ID.
	NextIDFileName = "NEXTID"
	// WALFileSuffix is the suffix for WAL segment files.
	WALFileSuffix = ".wal"
	// CheckpointFileName is the name of the file storing checkpoint information.
	CheckpointFileName = "CHECKPOINT"

	SeriesMappingLogName = "series_mapping.log"

	StringMappingLogName = "string_mapping.log"
)

// --- Protocol & Format Versions ---
const (
	// FormatVersion is the current version for all persistent file formats.
	FormatVersion uint8 = 2
)

// --- Default Sizes & Limits ---
const (
	// WALMaxSegmentSize is the default maximum size for a WAL segment file.
	WALMaxSegmentSize = 128 * 1024 * 1024 // 128 MB
)

const (
	// IndexManifestFileName is the name of the manifest file for the tag index.
	IndexManifestFileName = "MANIFEST"
	IndexDirName          = "index"
	IndexSSTDirName       = "index_sst"
)

// Checkpoint stores the state of the last durable checkpoint.
type Checkpoint struct {
	LastSafeSegmentIndex uint64
}

func FormatTempFilename(prefix, postfix string) string {
	return fmt.Sprintf("%s.%s", prefix, postfix)
}

// FormatSegmentFileName creates a segment file name from its index.
func FormatSegmentFileName(index uint64) string {
	return fmt.Sprintf("%08d%s", index, WALFileSuffix)
}

// ParseSegmentFileName extracts the index from a segment file name.
func ParseSegmentFileName(name string) (uint64, error) {
	if !strings.HasSuffix(name, WALFileSuffix) {
		return 0, fmt.Errorf("file %s is not a WAL segment file", name)
	}
	name = strings.TrimSuffix(name, WALFileSuffix)
	return strconv.ParseUint(name, 10, 64)
}
