package levels

import (
	"bytes"
	"sort"

	"github.com/INLOpen/nexusbase/sstable"
)

// getOverlappingTablesLocked is the unlocked version of GetOverlappingTables.
// It must be called with the LevelsManager's read lock held.
func (lm *LevelsManager) getOverlappingTablesLocked(levelNum int, minRangeKey, maxRangeKey []byte) []*sstable.SSTable {
	if levelNum < 0 || levelNum >= len(lm.levels) {
		return nil
	}

	if levelNum == 0 {
		return lm.getL0OverlappingTables(minRangeKey, maxRangeKey)
	}
	return lm.getLeveledOverlappingTables(levelNum, minRangeKey, maxRangeKey)
}

// getL0OverlappingTables finds overlapping tables in L0.
// L0 tables can overlap arbitrarily, so we check each table individually.
func (lm *LevelsManager) getL0OverlappingTables(minRangeKey, maxRangeKey []byte) []*sstable.SSTable {
	var overlappingTables []*sstable.SSTable
	srcSST := lm.levels[0].GetTables()
	for _, table := range srcSST {
		if tableOverlapsRange(table, minRangeKey, maxRangeKey) {
			overlappingTables = append(overlappingTables, table)
		}
	}
	return overlappingTables
}

// getLeveledOverlappingTables finds overlapping tables in L1+ levels.
// L1+ tables are sorted by MinKey and are non-overlapping, so we can use binary search.
func (lm *LevelsManager) getLeveledOverlappingTables(levelNum int, minRangeKey, maxRangeKey []byte) []*sstable.SSTable {
	tables := lm.levels[levelNum].GetTables()
	if len(tables) == 0 {
		return nil
	}

	// The first candidate is the first table whose MaxKey is >= minRangeKey.
	startIndex := sort.Search(len(tables), func(i int) bool {
		return bytes.Compare(tables[i].MaxKey(), minRangeKey) >= 0
	})

	var overlappingTables []*sstable.SSTable
	// Iterate from the first candidate.
	for i := startIndex; i < len(tables); i++ {
		table := tables[i]
		// If the table's MinKey is already beyond maxRangeKey, no further tables will overlap.
		if maxRangeKey != nil && bytes.Compare(table.MinKey(), maxRangeKey) > 0 {
			break
		}
		overlappingTables = append(overlappingTables, table)
	}
	return overlappingTables
}

// tableOverlapsRange checks if a table overlaps with the given key range [minRangeKey, maxRangeKey].
func tableOverlapsRange(table *sstable.SSTable, minRangeKey, maxRangeKey []byte) bool {
	// Table overlaps if:
	// - table.MaxKey >= minRangeKey (table doesn't end before range starts)
	// - table.MinKey <= maxRangeKey (table doesn't start after range ends)
	if maxRangeKey != nil && bytes.Compare(table.MinKey(), maxRangeKey) > 0 {
		return false // Table starts after range ends
	}
	if minRangeKey != nil && bytes.Compare(table.MaxKey(), minRangeKey) < 0 {
		return false // Table ends before range starts
	}
	return true
}

// GetOverlappingTables returns SSTables from a given level that overlap with the provided key range [minKey, maxKey].
// For L0, all tables are considered overlapping if the range itself is valid (as L0 tables can overlap arbitrarily).
// For L1+, tables are sorted by minKey and non-overlapping, so we can find relevant tables more efficiently.
func (lm *LevelsManager) GetOverlappingTables(levelNum int, minRangeKey, maxRangeKey []byte) []*sstable.SSTable {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	return lm.getOverlappingTablesLocked(levelNum, minRangeKey, maxRangeKey)
}
