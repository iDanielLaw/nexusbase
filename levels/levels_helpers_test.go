package levels

import (
	"io"
	"log/slog"
	"testing"

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/stretchr/testify/require"
)

// newTestSSTable is a shared helper function to create a dummy SSTable for testing.
func newTestSSTable(t *testing.T, id uint64, entries []struct{ key, value []byte }) *sstable.SSTable {
	t.Helper()
	testSpecificDir := t.TempDir()
	writerOpts := core.SSTableWriterOptions{
		DataDir:                      testSpecificDir,
		ID:                           id,
		EstimatedKeys:                uint64(len(entries)),
		BloomFilterFalsePositiveRate: 0.01,
		BlockSize:                    256,
		Tracer:                       nil,
		Compressor:                   &compressors.NoCompressionCompressor{},
		Logger:                       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	writer, err := sstable.NewSSTableWriter(writerOpts)
	require.NoError(t, err, "Failed to create SSTable writer for test")
	for i, entry := range entries {
		fields, _ := core.NewFieldValuesFromMap(map[string]interface{}{"value": string(entry.value)})
		encodedValue, _ := fields.Encode()
		writer.Add(entry.key, encodedValue, core.EntryTypePutEvent, uint64(i+1))
	}
	require.NoError(t, writer.Finish(), "Failed to finish test SSTable")
	loadOpts := sstable.LoadSSTableOptions{FilePath: writer.FilePath(), ID: id, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	tbl, err := sstable.LoadSSTable(loadOpts)
	require.NoError(t, err, "Failed to load test SSTable")
	return tbl
}

// makeValue is a shared helper to create a byte slice of a specific size for testing table sizes.
func makeValue(size int) []byte {
	return make([]byte, size)
}
