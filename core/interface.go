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
}

type SSTableWriterFactory func(opts SSTableWriterOptions) (SSTableWriterInterface, error)

type SSTableNextIDFactory func() uint64
