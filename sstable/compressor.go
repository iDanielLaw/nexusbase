package sstable

import (
	"fmt"

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
)

// GetCompressor returns a Compressor instance based on the CompressionType.
// This will be used during decompression.
func GetCompressor(compressionType core.CompressionType) (core.Compressor, error) {
	switch compressionType {
	case core.CompressionNone:
		return &compressors.NoCompressionCompressor{}, nil
	case core.CompressionSnappy:
		return &compressors.SnappyCompressor{}, nil
	case core.CompressionLZ4:
		return &compressors.LZ4Compressor{}, nil
	case core.CompressionZSTD:
		// NewZstdCompressor returns (*ZstdCompressor, error), so we return it directly.
		return compressors.NewZstdCompressor(), nil
	default:
		return nil, fmt.Errorf("unknown compression type: %d", compressionType)
	}
}
