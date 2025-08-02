package compressors

import (
	"bytes"
	"fmt"
	"io"

	"github.com/INLOpen/nexusbase/core"
	"github.com/golang/snappy"
)

// SnappyCompressor implements the Compressor interface using Snappy.
type SnappyCompressor struct{}

// snappyReadCloser is a simple wrapper around bytes.Reader that implements io.ReadCloser.
// It's used to return decompressed Snappy data as a stream.
type snappyReadCloser struct {
	*bytes.Reader // Embed bytes.Reader to inherit its Read, Seek, etc. methods
}

// Close implements the io.Closer interface for snappyReadCloser.
// For in-memory data from Snappy, there are no external resources to close,
// so this is a no-op method that always returns nil.
func (src *snappyReadCloser) Close() error {
	// No resources to release for in-memory data
	return nil
}

var _ core.Compressor = (*SnappyCompressor)(nil)
var _ io.ReadCloser = (*snappyReadCloser)(nil)

func NewSnappyCompressor() *SnappyCompressor {
	return &SnappyCompressor{}
}

func (c *SnappyCompressor) Compress(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}

func (c *SnappyCompressor) Decompress(data []byte) (io.ReadCloser, error) {
	// snappy.Decode returns the decompressed data as a new byte slice.
	// It also handles its own internal buffer management for the decompression process.
	decompressed, err := snappy.Decode(nil, data)
	if err != nil {
		return nil, fmt.Errorf("snappy decompress error: %w", err)
	}

	// Wrap the decompressed []byte into a bytes.Reader, then into our snappyReadCloser.
	// bytes.NewReader provides the io.Reader functionality.
	// snappyReadCloser adds the Close() method to satisfy io.ReadCloser.
	return &snappyReadCloser{Reader: bytes.NewReader(decompressed)}, nil
}

func (c *SnappyCompressor) Type() core.CompressionType {
	return core.CompressionSnappy
}

// CompressTo compresses src data into the dst buffer using Snappy, avoiding extra allocations.
func (c *SnappyCompressor) CompressTo(dst *bytes.Buffer, src []byte) error {
	dst.Reset()
	// The previous implementation used snappy.NewBufferedWriter, which creates a stream
	// format incompatible with snappy.Decode used in Decompress().
	// This version uses snappy.Encode to produce the correct block format.
	// It lets snappy.Encode allocate the destination slice and then writes it to the buffer.
	compressed := snappy.Encode(nil, src)
	dst.Write(compressed)
	return nil
}
