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

// SnapshotManifest defines the structure of the snapshot manifest file.
type SnapshotManifest struct {
	SequenceNumber      uint64                  `json:"sequence_number"`
	Levels              []SnapshotLevelManifest `json:"levels"`
	WALFile             string                  `json:"wal_file,omitempty"`
	DeletedSeriesFile   string                  `json:"deleted_series_file,omitempty"`
	RangeTombstonesFile string                  `json:"range_tombstones_file,omitempty"`
	StringMappingFile   string                  `json:"string_mapping_file,omitempty"`
	SeriesMappingFile   string                  `json:"series_mapping_file,omitempty"`
	SSTableCompression  string                  `json:"sstable_compression,omitempty"`
}

// SnapshotLevelManifest stores metadata for SSTables in a specific level.
type SnapshotLevelManifest struct {
	LevelNumber int               `json:"level_number"`
	Tables      []SSTableMetadata `json:"tables"`
}

// SSTableMetadata stores essential metadata for an SSTable in the snapshot.
type SSTableMetadata struct {
	ID       uint64 `json:"id"`
	FileName string `json:"file_name"`
	MinKey   []byte `json:"min_key"`
	MaxKey   []byte `json:"max_key"`
}
