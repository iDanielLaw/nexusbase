package core

import (
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// SSTableWriterInterface defines the interface for an SSTable writer.
// This allows for mocking the writer in tests.
type SSTableWriterInterface interface {
	Add(key, value []byte, entryType EntryType, pointID uint64) error
	Finish() error
	Abort() error
	FilePath() string
	CurrentSize() int64
}

// SSTableWriterOptions holds configuration for creating a new SSTableWriter.
type SSTableWriterOptions struct {
	DataDir                      string
	ID                           uint64
	EstimatedKeys                uint64
	BloomFilterFalsePositiveRate float64
	BlockSize                    int
	Tracer                       trace.Tracer
	Compressor                   Compressor
	Logger                       *slog.Logger // Logger for debug/info messages
	// If true, attempt to preallocate the sstable file on disk (best-effort).
	Preallocate bool
	// RestartPointInterval controls how often restart points are emitted
	// inside blocks (number of entries). If zero, the writer's default is used.
	RestartPointInterval int
}

type SSTableWriterFactory func(opts SSTableWriterOptions) (SSTableWriterInterface, error)

type SSTableNextIDFactory func() uint64
