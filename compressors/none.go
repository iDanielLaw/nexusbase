package compressors

import (
	"bytes"
	"io"

	"github.com/INLOpen/nexusbase/core"
)

// NoCompressionCompressor implements the Compressor interface without performing compression.
type NoCompressionCompressor struct{}

type planTextDecoder struct {
	*bytes.Reader
}

func (p *planTextDecoder) Close() error {
	return nil
}

var _ core.Compressor = (*NoCompressionCompressor)(nil)

func (c *NoCompressionCompressor) Compress(data []byte) ([]byte, error) {
	return data, nil // Return data as is
}

func (c *NoCompressionCompressor) Decompress(data []byte) (io.ReadCloser, error) {
	return &planTextDecoder{Reader: bytes.NewReader(data)}, nil // Return data as is
}

func (c *NoCompressionCompressor) Type() core.CompressionType {
	return core.CompressionNone
}

// CompressTo "compresses" src data into the dst buffer by simply writing it.
// This avoids the allocation of a new slice that Compress() does.
func (c *NoCompressionCompressor) CompressTo(dst *bytes.Buffer, src []byte) error {
	dst.Reset()
	_, err := dst.Write(src)
	return err
}
