package compressors

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/INLOpen/nexusbase/core"
	"github.com/klauspost/compress/zstd"
)

// NoCompressionCompressor implements the Compressor interface without performing compression.
type ZstdCompressor struct {
	encoderPool sync.Pool
	decoderPool sync.Pool // NEW: Pool for decoders
}

type zstdReadCloser struct {
	*zstd.Decoder            // Embed the zstd.Decoder to inherit its Read method
	pool          *sync.Pool // NEW: Reference to the pool to return the decoder
}

func (zrc *zstdReadCloser) Close() error {
	// Do not call zrc.Decoder.Close() as it invalidates the decoder for reuse.
	zrc.pool.Put(zrc.Decoder) // Return the decoder to our custom pool.
	return nil
}

var _ core.Compressor = (*ZstdCompressor)(nil)
var _ io.ReadCloser = (*zstdReadCloser)(nil)

func NewZstdCompressor() *ZstdCompressor {
	return &ZstdCompressor{
		encoderPool: sync.Pool{
			New: func() interface{} {
				// Create a new zstd.Encoder.
				// We pass nil because the actual io.Writer will be set during Reset.
				// You can add options here, e.g., zstd.WithEncoderLevel(zstd.SpeedDefault)
				enc, err := zstd.NewWriter(nil)
				if err != nil {
					// Handle error during encoder creation. This should ideally not happen
					// with default options unless there's a serious underlying issue.
					log.Printf("Error creating new zstd encoder: %v", err)
					return nil // Return nil, the pool will try again or the caller will handle it.
				}
				return enc
			},
		},
		decoderPool: sync.Pool{ // NEW: Initialize decoder pool
			New: func() interface{} {
				dec, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(100*1024*1024))
				if err != nil {
					log.Printf("Error creating new zstd decoder: %v", err)
					return nil
				}
				return dec
			},
		},
	}
}

func (c *ZstdCompressor) Compress(data []byte) ([]byte, error) {
	enc := c.encoderPool.Get().(*zstd.Encoder)
	defer c.encoderPool.Put(enc)

	// Use the shared buffer pool to avoid allocations
	buf := core.BufferPool.Get()
	defer core.BufferPool.Put(buf)

	enc.Reset(buf)

	_, err := enc.Write(data)
	if err != nil {
		return nil, fmt.Errorf("zstd compress write error: %w", err)
	}

	// Close the encoder to flush any buffered data and finalize compression.
	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("zstd compress close error: %w", err)
	}

	// Create a copy of the data because the buffer from the pool will be reset and reused.
	compressedData := make([]byte, buf.Len())
	copy(compressedData, buf.Bytes())
	return compressedData, nil
}

func (c *ZstdCompressor) Decompress(data []byte) (io.ReadCloser, error) {
	// Get a decoder from the pool
	dec := c.decoderPool.Get().(*zstd.Decoder)

	// Reset it with the new data source
	err := dec.Reset(bytes.NewReader(data))
	if err != nil {
		// If reset fails, put it back and return error
		c.decoderPool.Put(dec)
		return nil, fmt.Errorf("zstd decoder reset error: %w", err)
	}

	// Return a custom ReadCloser that will return the decoder to the pool on Close()
	return &zstdReadCloser{Decoder: dec, pool: &c.decoderPool}, nil
}

func (c *ZstdCompressor) Type() core.CompressionType {
	return core.CompressionZSTD
}

// CompressTo compresses src data into the dst buffer using ZSTD.
func (c *ZstdCompressor) CompressTo(dst *bytes.Buffer, src []byte) error {
	enc := c.encoderPool.Get().(*zstd.Encoder)
	defer c.encoderPool.Put(enc)

	dst.Reset()
	enc.Reset(dst)

	if _, err := enc.Write(src); err != nil {
		// Even on error, Close must be called to not break the encoder state.
		_ = enc.Close()
		return fmt.Errorf("zstd compress (to) write error: %w", err)
	}

	// Close the encoder to flush any buffered data and finalize compression into dst.
	return enc.Close()
}
