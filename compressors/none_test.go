package compressors

import (
	"bytes"
	"io"
	"testing"

	"github.com/INLOpen/nexusbase/core"
)

func TestNoCompressionCompressor(t *testing.T) {
	compressor := &NoCompressionCompressor{}

	if compressor.Type() != core.CompressionNone {
		t.Errorf("NoCompressionCompressor.Type() got = %v, want %v", compressor.Type(), core.CompressionNone)
	}

	data := []byte("this is some test data")

	// Compress
	compressed, err := compressor.Compress(data)
	if err != nil {
		t.Fatalf("Compress() returned an unexpected error: %v", err)
	}
	if !bytes.Equal(data, compressed) {
		t.Errorf("Expected compressed data to be the same as original, but it was different")
	}

	// Decompress
	decompressedReader, err := compressor.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress() returned an unexpected error: %v", err)
	}
	defer decompressedReader.Close()

	decompressed, err := io.ReadAll(decompressedReader)
	if err != nil {
		t.Fatalf("Failed to read decompressed data: %v", err)
	}

	if !bytes.Equal(data, decompressed) {
		t.Errorf("Decompressed data does not match original data")
	}
}

func BenchmarkNoCompressionCompress(b *testing.B) {
	compressor := &NoCompressionCompressor{}
	data := bytes.Repeat([]byte("The quick brown fox jumps over the lazy dog."), 100)

	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = compressor.Compress(data)
	}
}

func BenchmarkNoCompressionDecompress(b *testing.B) {
	compressor := &NoCompressionCompressor{}
	data := bytes.Repeat([]byte("The quick brown fox jumps over the lazy dog."), 100)
	compressed, _ := compressor.Compress(data)

	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		decompressedReader, _ := compressor.Decompress(compressed)
		_, _ = io.Copy(io.Discard, decompressedReader)
		_ = decompressedReader.Close()
	}
}
