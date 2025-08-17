package core

import (
	"bytes"
	"io"
)

// CompressionType identifies the compression algorithm used.
// This will be stored on disk to know how to decompress.
type CompressionType byte

const (
	CompressionNone   CompressionType = 0
	CompressionSnappy CompressionType = 1
	CompressionLZ4    CompressionType = 2
	CompressionZSTD   CompressionType = 3
	// Add other types as needed
)

// Compressor defines the interface for compression and decompression algorithms.
type Compressor interface {
	// Compress compresses the input data.
	Compress(data []byte) ([]byte, error)
	CompressTo(dst *bytes.Buffer, src []byte) error
	// Decompress decompresses the input data.
	Decompress(data []byte) (io.ReadCloser, error)
	// Type returns the CompressionType identifier for this compressor.
	Type() CompressionType
}

// String returns the string representation of the CompressionType.
func (ct CompressionType) String() string {
	switch ct {
	case CompressionNone:
		return "none"
	case CompressionSnappy:
		return "snappy"
	case CompressionLZ4:
		return "lz4"
	case CompressionZSTD:
		return "zstd"
	default:
		return "unknown"
	}
}

const (
	SeqNumSize   = 8 // uint64 for sequence number (FR5.3)
	ChecksumSize = 4 // uint32 for CRC32 checksum (FR5.1)
)
