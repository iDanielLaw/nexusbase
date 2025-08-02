package levels

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/INLOpen/nexusbase/sstable"
)

// LevelState represents the state of a single level in the LSM tree.
type LevelState struct {
	levelNumber int
	// tables is the source of truth for the order of tables.
	// L0: sorted by creation time (newest first for lookups)
	// L1+: sorted by minKey
	tables   []*sstable.SSTable
	tableMap map[uint64]*sstable.SSTable
}

// Add adds a table to the level, maintaining the correct order.
// For L0 (levelNumber 0), it prepends. For L1+, it appends and re-sorts.
func (ls *LevelState) Add(table *sstable.SSTable) error {
	if table == nil {
		return fmt.Errorf("cannot add nil table to LevelState")
	}
	if _, exists := ls.tableMap[table.ID()]; exists {
		return fmt.Errorf("table with ID %d already exists in level %d", table.ID(), ls.levelNumber)
	}

	// Add to map for quick lookup
	ls.tableMap[table.ID()] = table

	// Add to slice, maintaining order
	if ls.levelNumber == 0 {
		// For L0, prepend to keep newest first.
		ls.tables = append([]*sstable.SSTable{table}, ls.tables...)
	} else {
		// For L1+, append and then re-sort by MinKey.
		ls.tables = append(ls.tables, table)
		sort.Slice(ls.tables, func(i, j int) bool {
			// Handle nil keys just in case, though they shouldn't exist in L1+
			if ls.tables[i].MinKey() == nil {
				return true
			}
			if ls.tables[j].MinKey() == nil {
				return false
			}
			return bytes.Compare(ls.tables[i].MinKey(), ls.tables[j].MinKey()) < 0
		})
	}
	return nil
}

// AddBatch adds multiple tables to the level, maintaining the correct order.
// This is much more efficient for L1+ than calling Add in a loop, as it sorts only once.
func (ls *LevelState) AddBatch(tablesToAdd []*sstable.SSTable) error {
	if len(tablesToAdd) == 0 {
		return nil
	}

	// Add to map and slice
	for _, table := range tablesToAdd {
		if table == nil {
			return fmt.Errorf("cannot add nil table to LevelState")
		}
		if _, exists := ls.tableMap[table.ID()]; exists {
			return fmt.Errorf("table with ID %d already exists in level %d", table.ID(), ls.levelNumber)
		}
		ls.tableMap[table.ID()] = table
		ls.tables = append(ls.tables, table)
	}

	// Sort the entire slice once
	if ls.levelNumber == 0 {
		// For L0, sort by ID descending to approximate newest-first.
		// A real system might use creation timestamps stored in the manifest.
		sort.Slice(ls.tables, func(i, j int) bool {
			return ls.tables[i].ID() > ls.tables[j].ID()
		})
	} else {
		// For L1+, sort by MinKey.
		sort.SliceStable(ls.tables, func(i, j int) bool {
			return bytes.Compare(ls.tables[i].MinKey(), ls.tables[j].MinKey()) < 0
		})
	}
	return nil
}

// Remove removes a table from the level by its ID.
func (ls *LevelState) Remove(sstID uint64) error {
	if _, ok := ls.tableMap[sstID]; !ok {
		return fmt.Errorf("SSTable with ID %d not found in LevelState %d", sstID, ls.levelNumber)
	}

	// Remove from map
	delete(ls.tableMap, sstID)

	// Remove from slice without allocating a new underlying array if possible
	newTables := ls.tables[:0]
	for _, table := range ls.tables {
		if table.ID() != sstID {
			newTables = append(newTables, table)
		}
	}
	ls.tables = newTables
	return nil
}

// Size returns the number of tables in the level.
func (ls *LevelState) Size() int {
	// The length of the slice is the source of truth for the count.
	return len(ls.tables)
}

// GetTables returns a copy of the tables slice, preserving order.
func (ls *LevelState) GetTables() []*sstable.SSTable {
	// Return a copy to prevent external modification of the internal slice.
	tablesCopy := make([]*sstable.SSTable, len(ls.tables))
	copy(tablesCopy, ls.tables)
	return tablesCopy
}

// SetTables replaces all tables in the level with a new set.
// It ensures the map and slice are consistent with the new set.
// For L1+, it also sorts the provided tables.
func (ls *LevelState) SetTables(tables []*sstable.SSTable) {
	newTableMap := make(map[uint64]*sstable.SSTable, len(tables))
	for _, table := range tables {
		newTableMap[table.ID()] = table
	}

	ls.tables = tables // The provided slice becomes the new source of truth for order
	ls.tableMap = newTableMap

	// Ensure L1+ levels are sorted correctly after setting.
	if ls.levelNumber > 0 {
		sort.Slice(ls.tables, func(i, j int) bool {
			if ls.tables[i].MinKey() == nil {
				return true
			}
			if ls.tables[j].MinKey() == nil {
				return false
			}
			return bytes.Compare(ls.tables[i].MinKey(), ls.tables[j].MinKey()) < 0
		})
	}
}
