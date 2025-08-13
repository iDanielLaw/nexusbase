package core

// SnapshotManifest defines the structure of the snapshot manifest file.
type SnapshotManifest struct {
	SequenceNumber      uint64                  `json:"sequence_number"`
	// ParentManifest stores the relative path to the parent manifest file,
	// forming a chain for incremental snapshots. It's empty for a full snapshot.
	ParentManifest      string                  `json:"parent_manifest,omitempty"`
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

