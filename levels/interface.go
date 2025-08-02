package levels

import "github.com/INLOpen/nexusbase/sstable"

// Manager defines the public interface for a levels manager.
type Manager interface {
	GetSSTablesForRead() ([]*LevelState, func())
	AddL0Table(table *sstable.SSTable) error
	AddTableToLevel(level int, table *sstable.SSTable) error
	Close() error
	VerifyConsistency() []error
	GetTablesForLevel(level int) []*sstable.SSTable
	GetTotalSizeForLevel(level int) int64
	MaxLevels() int
	NeedsL0Compaction(maxL0Files int, l0CompactionTriggerSize int64) bool
	NeedsLevelNCompaction(levelN int, multiplier int) bool
	PickCompactionCandidateForLevelN(levelN int) *sstable.SSTable
	GetOverlappingTables(level int, minKey, maxKey []byte) []*sstable.SSTable
	ApplyCompactionResults(sourceLevel, targetLevel int, newTables, oldTables []*sstable.SSTable) error
	RemoveTables(level int, tablesToRemove []uint64) error
	GetLevels() []*LevelState
	GetTotalTableCount() int
	GetLevelForTable(tableID uint64) (int, bool)
	GetLevelTableCounts() (map[int]int, error)
}
