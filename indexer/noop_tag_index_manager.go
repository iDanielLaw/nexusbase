package indexer

import (
	"github.com/INLOpen/nexusbase/core"
	"github.com/RoaringBitmap/roaring/roaring64"
)

// noopTagIndexManager is a minimal TagIndexManagerInterface implementation
// that performs no background work and returns empty results. It's intended
// for use in tests that only need to write WAL files or exercise storage
// paths without starting the full index background loops.
type noopTagIndexManager struct{}

// NewNoopTagIndexManager returns a TagIndexManagerInterface that does nothing.
func NewNoopTagIndexManager() TagIndexManagerInterface {
	return &noopTagIndexManager{}
}

func (n *noopTagIndexManager) Add(seriesID uint64, tags map[string]string) error {
	return nil
}
func (n *noopTagIndexManager) AddEncoded(seriesID uint64, encodedTags []core.EncodedSeriesTagPair) error {
	return nil
}
func (n *noopTagIndexManager) RemoveSeries(seriesID uint64) {}
func (n *noopTagIndexManager) Query(tags map[string]string) (*roaring64.Bitmap, error) {
	return roaring64.New(), nil
}
func (n *noopTagIndexManager) Start()                                       {}
func (n *noopTagIndexManager) Stop()                                        {}
func (n *noopTagIndexManager) CreateSnapshot(snapshotDir string) error      { return nil }
func (n *noopTagIndexManager) RestoreFromSnapshot(snapshotDir string) error { return nil }
func (n *noopTagIndexManager) LoadFromFile(dataDir string) error            { return nil }
