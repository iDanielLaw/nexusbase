package core

import (
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// DefaultSSTablePreallocMultiplier is the default number of bytes to
// preallocate per estimated key when SSTable preallocation is enabled.
// This is a conservative heuristic used when callers do not provide an
// explicit PreallocMultiplier in `SSTableWriterOptions`.
const DefaultSSTablePreallocMultiplier = 128

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
	// PreallocMultiplier is the multiplier (bytes per estimated key) used
	// when computing the preallocation size. If zero, a conservative
	// default (128 bytes per entry) is used.
	PreallocMultiplier int
	// RestartPointInterval controls how often restart points are emitted
	// inside blocks (number of entries). If zero, the writer's default is used.
	RestartPointInterval int
}

type SSTableWriterFactory func(opts SSTableWriterOptions) (SSTableWriterInterface, error)

type SSTableNextIDFactory func() uint64
