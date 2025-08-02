package compressors

import (
	"bytes"
	"io"
	"testing"

	"github.com/INLOpen/nexusbase/core"
)

func TestLZ4Compressor(t *testing.T) {
	compressor := &LZ4Compressor{}

	if compressor.Type() != core.CompressionLZ4 {
		t.Errorf("LZ4Compressor.Type() got = %v, want %v", compressor.Type(), core.CompressionLZ4)
	}

	testCases := []struct {
		name string
		data []byte
	}{
		{
			name: "simple string",
			data: []byte("hello world, this is a test of the lz4 compressor"),
		},
		{
			name: "repetitive data",
			data: bytes.Repeat([]byte("a"), 1024),
		},
		{
			name: "empty data",
			data: []byte{},
		},
		{
			name: "random data (less compressible)",
			data: []byte("82f7b5a3e1d9c0f4b8a6d2c1e0f3a9b8d7c6e5f4a3b2c1d0e9f8a7b6c5d4e3f2"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// --- Test Compress and Decompress ---
			compressed, err := compressor.Compress(tc.data)
			if err != nil {
				t.Fatalf("Compress() returned an unexpected error: %v", err)
			}

			// Decompress the data from Compress
			decompressedReader, err := compressor.Decompress(compressed)
			if err != nil {
				t.Fatalf("Decompress() returned an unexpected error: %v", err)
			}
			defer decompressedReader.Close()

			decompressedBytes, err := io.ReadAll(decompressedReader)
			if err != nil {
				t.Fatalf("เกิดข้อผิดพลาดในการอ่านข้อมูลที่ถอดรหัส: %v", err)
			}

			// Verify the decompressed data matches the original
			if !bytes.Equal(tc.data, decompressedBytes) {
				t.Errorf("Decompressed data from Compress does not match original data.\nOriginal: %q\nDecompressed: %q", string(tc.data), string(decompressedBytes))
			}

			// --- Test CompressTo ---
			var compressedBuf bytes.Buffer
			err = compressor.CompressTo(&compressedBuf, tc.data)
			if err != nil {
				t.Fatalf("CompressTo() returned an unexpected error: %v", err)
			}

			// Decompress the result from CompressTo to verify
			decompressedReaderFromTo, err := compressor.Decompress(compressedBuf.Bytes())
			if err != nil {
				t.Fatalf("Decompress() after CompressTo() returned an unexpected error: %v", err)
			}
			defer decompressedReaderFromTo.Close()

			decompressedBytesFromTo, err := io.ReadAll(decompressedReaderFromTo)
			if err != nil {
				t.Fatalf("Failed to read decompressed data after CompressTo: %v", err)
			}

			if !bytes.Equal(tc.data, decompressedBytesFromTo) {
				t.Errorf("Decompressed data from CompressTo does not match original data")
			}

			// Optional: Check if compression actually reduced size (for compressible data)
			if len(tc.data) > 0 && len(compressed) >= len(tc.data) && tc.name == "repetitive data" {
				t.Logf("Warning: LZ4 compression did not reduce data size for repetitive data. Original: %d, Compressed: %d", len(tc.data), len(compressed))
			}
		})
	}
}

func BenchmarkLZ4Compress(b *testing.B) {
	compressor := NewLz4Compressor()
	// Using a more realistic payload, like a JSON object repeated.
	data := []byte(`{"metric":"cpu.usage","tags":{"host":"server-a","region":"us-east-1"},"timestamp":1678886400000000000,"fields":{"value":99.8}}`)
	data = bytes.Repeat(data, 50) // Repeat to make it larger

	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := compressor.Compress(data)
		if err != nil {
			b.Fatalf("Compress() error: %v", err)
		}
	}
}

func BenchmarkLZ4CompressTo(b *testing.B) {
	compressor := NewLz4Compressor()
	data := []byte(`{"metric":"cpu.usage","tags":{"host":"server-a","region":"us-east-1"},"timestamp":1678886400000000000,"fields":{"value":99.8}}`)
	data = bytes.Repeat(data, 50)

	// Create a buffer outside the loop to reuse it, which is the point of CompressTo
	var buf bytes.Buffer

	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := compressor.CompressTo(&buf, data)
		if err != nil {
			b.Fatalf("CompressTo() error: %v", err)
		}
	}
}

func BenchmarkLZ4Decompress(b *testing.B) {
	compressor := NewLz4Compressor()
	data := []byte(`{"metric":"cpu.usage","tags":{"host":"server-a","region":"us-east-1"},"timestamp":1678886400000000000,"fields":{"value":99.8}}`)
	data = bytes.Repeat(data, 50)
	compressed, err := compressor.Compress(data)
	if err != nil {
		b.Fatalf("Setup: Compress() error: %v", err)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		decompressedReader, err := compressor.Decompress(compressed)
		if err != nil {
			b.Fatalf("Decompress() error: %v", err)
		}
		_, _ = io.Copy(io.Discard, decompressedReader)
		_ = decompressedReader.Close()
	}
}
