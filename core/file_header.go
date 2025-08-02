package core

import (
	"encoding/binary"
	"time"
)

const (
	// Magic numbers to identify file types
	WALMagic         uint32 = 0xBAADF00D
	StringStoreMagic uint32 = 0x57524E47 // "STRG"
	ManifestMagic    uint32 = 0x424E414D // "MANB" for MANifest Binary
	SeriesStoreMagic uint32 = 0x53455249 // "SERI"
	TagIndexMagic    uint32 = 0x54414758 // "TAGX"
	SSTableMagic     uint32 = 0x53535442 // "SSTB"

	// Current file format version
	CurrentVersion uint8 = 2
)

// FileHeader is a standard header for all persistent log/index files.
type FileHeader struct {
	Magic          uint32
	Version        uint8
	CreatedAt      int64 // UnixNano timestamp
	CompressorType CompressionType
}

func (h *FileHeader) Size() int {
	return binary.Size(h)
}

// NewFileHeader creates a new header with the current time and specified magic number.
func NewFileHeader(magic uint32, compressorType CompressionType) FileHeader {
	return FileHeader{
		Magic:          magic,
		Version:        CurrentVersion,
		CreatedAt:      time.Now().UnixNano(),
		CompressorType: compressorType,
	}
}
