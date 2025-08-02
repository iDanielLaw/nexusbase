package indexer

import (
	"github.com/INLOpen/nexusbase/core"
	"github.com/RoaringBitmap/roaring/roaring64"
)

// StringStoreInterface defines the public API for the string-to-ID mapping store.
type StringStoreInterface interface {
	GetOrCreateID(str string) (uint64, error)
	GetString(id uint64) (string, bool)
	GetID(str string) (uint64, bool)
	Sync() error
	Close() error
	LoadFromFile(dataDir string) error
}

// SeriesIDStoreInterface defines the public API for the seriesKey-to-SeriesID mapping store.
type SeriesIDStoreInterface interface {
	GetOrCreateID(seriesKey string) (uint64, error)
	GetID(seriesKey string) (uint64, bool)
	GetKey(id uint64) (string, bool)
	Sync() error
	Close() error
	LoadFromFile(dataDir string) error
}

// TagIndexManagerInterface defines the public API for the tag index manager.
type TagIndexManagerInterface interface {
	Add(seriesID uint64, tags map[string]string) error
	AddEncoded(seriesID uint64, encodedTags []core.EncodedSeriesTagPair) error
	RemoveSeries(seriesID uint64)
	Query(tags map[string]string) (*roaring64.Bitmap, error)
	Start()
	Stop()
	CreateSnapshot(snapshotDir string) error
	RestoreFromSnapshot(snapshotDir string) error
	LoadFromFile(dataDir string) error
}
