package engine2

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// BenchmarkWriteChunksFile measures performance of writeChunksFile with
// synthetic per-series sample data. It creates N series each containing
// M samples with a fixed payload size and writes them into a temp block
// directory using the provided maxChunkBytes split threshold.
func BenchmarkWriteChunksFile(b *testing.B) {
	// parameters tuned for a reasonably fast benchmark
	const (
		nSeries       = 100
		samplesPer    = 200
		payloadSize   = 128
		maxChunkBytes = 16 * 1024
	)

	// prepare synthetic samples
	samples := make(map[string]*bytes.Buffer, nSeries)
	for i := 0; i < nSeries; i++ {
		key := "metric|series" + strconv.Itoa(i)
		buf := &bytes.Buffer{}
		for s := 0; s < samplesPer; s++ {
			// timestamp (8 bytes big-endian)
			tsb := make([]byte, 8)
			binary.BigEndian.PutUint64(tsb, uint64(s))
			buf.Write(tsb)
			// length prefix (uvarint)
			tmp := make([]byte, binary.MaxVarintLen64)
			n := binary.PutUvarint(tmp, uint64(payloadSize))
			buf.Write(tmp[:n])
			// payload bytes
			payload := make([]byte, payloadSize)
			for j := range payload {
				payload[j] = byte(j & 0xff)
			}
			buf.Write(payload)
		}
		samples[key] = buf
	}

	dir := b.TempDir()
	blockDir := filepath.Join(dir, "blocks", "1")
	if err := os.MkdirAll(blockDir, 0o755); err != nil {
		b.Fatalf("mkdir: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// ensure a clean output path for each iteration
		// remove any existing chunks.dat
		_ = os.Remove(filepath.Join(blockDir, "chunks.dat"))
		refs, err := writeChunksFile(blockDir, samples, maxChunkBytes)
		if err != nil {
			b.Fatalf("writeChunksFile failed: %v", err)
		}
		// sanity-check: ensure some refs were returned and file exists
		if len(refs) == 0 {
			b.Fatalf("expected non-zero refs")
		}
		// read the created file to prevent it being optimized away
		f, err := os.Open(filepath.Join(blockDir, "chunks.dat"))
		if err != nil {
			b.Fatalf("open chunks.dat: %v", err)
		}
		_, _ = io.Copy(io.Discard, f)
		_ = f.Close()
	}
}
