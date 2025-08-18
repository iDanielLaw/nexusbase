package core

import (
	"encoding/binary"
	"time"
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
		Version:        FormatVersion,
		CreatedAt:      time.Now().UnixNano(),
		CompressorType: compressorType,
	}
}
