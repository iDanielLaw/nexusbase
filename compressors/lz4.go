package compressors

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/INLOpen/nexusbase/core"
	lz4 "github.com/pierrec/lz4/v4"
)

// LZ4Compressor implements the Compressor interface using LZ4.
type LZ4Compressor struct{}

// lz4ReadCloser is a simple wrapper around bytes.Reader that implements io.ReadCloser.
type lz4ReadCloser struct {
	*bytes.Reader
}

// Close implement interface io.Closer สำหรับ lz4ReadCloser
// มันจะเรียกเมธอด Close ของ lz4.Reader ที่อยู่ข้างใต้
func (lrc *lz4ReadCloser) Close() error {
	return nil
}

var _ core.Compressor = (*LZ4Compressor)(nil)

func NewLz4Compressor() *LZ4Compressor {
	return &LZ4Compressor{}
}

func (c *LZ4Compressor) Compress(data []byte) ([]byte, error) {
	// Allocate a destination buffer with the maximum possible compressed size.
	dst := make([]byte, lz4.CompressBlockBound(len(data)))
	// Perform the compression.
	n, err := lz4.CompressBlock(data, dst, nil)
	if err != nil {
		return nil, fmt.Errorf("lz4 compress error: %w", err)
	}

	if n == 0 && len(data) > 0 {
		return nil, fmt.Errorf("lz4 compression resulted in zero bytes for non-empty input")
	}

	return dst[:n], nil // คืนค่า byte slice ที่บีบอัดจาก buffer
}

func (c *LZ4Compressor) Decompress(data []byte) (io.ReadCloser, error) {
	// The pierrec/lz4 block format does not store the original size.
	// We must allocate a buffer large enough. A common heuristic is to
	// assume a certain compression ratio and grow the buffer if needed.
	if len(data) == 0 {
		return &lz4ReadCloser{Reader: bytes.NewReader(nil)}, nil
	}
	// Heuristic: Start with a buffer 3 times the size of the compressed data.
	dstSize := len(data) * 3
	if dstSize < 1024 { // Have a minimum buffer size
		dstSize = 1024
	}
	dst := make([]byte, dstSize)

	for {
		n, err := lz4.UncompressBlock(data, dst)
		if err == nil {
			// Success! Wrap the result in a ReadCloser.
			return &lz4ReadCloser{Reader: bytes.NewReader(dst[:n])}, nil
		}

		// Check if the error is specifically about the destination buffer being too small.
		if errors.Is(err, lz4.ErrInvalidSourceShortBuffer) {
			// Double the buffer size and retry.
			if len(dst) > 16*1024*1024 { // Add a sanity limit to prevent infinite loops
				return nil, fmt.Errorf("lz4 decompression buffer grew too large (>16MB)")
			}
			dst = make([]byte, len(dst)*2)
			continue
		}

		// It's a different, unrecoverable error.
		return nil, fmt.Errorf("lz4 decompress error: %w", err)
	}
}

func (c *LZ4Compressor) Type() core.CompressionType {
	return core.CompressionLZ4
}

// CompressTo compresses src data into the dst buffer using LZ4.
func (c *LZ4Compressor) CompressTo(dst *bytes.Buffer, src []byte) error {
	dst.Reset()
	// The previous implementation used lz4.NewWriter, which creates a stream
	// format incompatible with lz4.UncompressBlock used in Decompress().
	// This version uses lz4.CompressBlock to produce the correct block format.
	// It allocates a temporary slice to hold the compressed data before writing to the buffer.
	maxDstSize := lz4.CompressBlockBound(len(src))
	tempBuf := make([]byte, maxDstSize)

	n, err := lz4.CompressBlock(src, tempBuf, nil)
	if err != nil {
		return fmt.Errorf("lz4 CompressTo block compress error: %w", err)
	}
	if n == 0 && len(src) > 0 {
		return fmt.Errorf("lz4 compression resulted in zero bytes for non-empty input")
	}

	dst.Write(tempBuf[:n])
	return nil
}
