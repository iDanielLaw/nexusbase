package levels

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/INLOpen/nexusbase/sstable"
)

// LevelState represents the state of a single level in the LSM tree.
type LevelState struct {
	levelNumber int // หมายเลขของ level
	// tables is the source of truth for the order of tables.
	// L0: sorted by creation time (newest first for lookups)
	// L1+: sorted by minKey
	tables    []*sstable.SSTable
	tableMap  map[uint64]*sstable.SSTable
	totalSize int64 // ขนาดรวมของตารางทั้งหมดใน level นี้ (bytes)
}

// newLevelState creates a new, empty LevelState.
func newLevelState(levelNumber int) *LevelState {
	return &LevelState{
		levelNumber: levelNumber,
		tables:      make([]*sstable.SSTable, 0),
		tableMap:    make(map[uint64]*sstable.SSTable),
		totalSize:   0,
	}
}

// Add adds a table to the level, maintaining the correct order.
// For L0 (levelNumber 0), it prepends. For L1+, it appends and inserts in sorted order.
// Returns an error if the table is nil or already exists in the level.
func (ls *LevelState) Add(table *sstable.SSTable) error {
	if table == nil {
		return fmt.Errorf("cannot add nil table to LevelState")
	}
	if _, exists := ls.tableMap[table.ID()]; exists {
		return fmt.Errorf("table with ID %d already exists in level %d", table.ID(), ls.levelNumber)
	}

	// Add to map for quick lookup
	ls.tableMap[table.ID()] = table
	ls.totalSize += table.Size()

	// Add to slice, maintaining order
	if ls.levelNumber == 0 {
		// For L0, prepend to keep newest first.
		ls.tables = append([]*sstable.SSTable{table}, ls.tables...)
	} else {
		// For L1+, use binary search to find the correct position and insert.
		// This is more efficient than appending and re-sorting for single additions.
		ls.insertTableSorted(table)
	}
	return nil
}

// insertTableSorted inserts a table into the correct position in ls.tables
// to maintain sorted order by MinKey (for L1+ levels only).
func (ls *LevelState) insertTableSorted(table *sstable.SSTable) {
	idx := sort.Search(len(ls.tables), func(i int) bool {
		return bytes.Compare(ls.tables[i].MinKey(), table.MinKey()) >= 0
	})

	// Insert table at the found position
	ls.tables = append(ls.tables, nil) // Expand slice
	copy(ls.tables[idx+1:], ls.tables[idx:])
	ls.tables[idx] = table
}

// AddBatch adds multiple tables to the level, maintaining the correct order.
// This is much more efficient for L1+ than calling Add in a loop, as it sorts only once.
// Returns an error if any table is nil or already exists in the level.
func (ls *LevelState) AddBatch(tablesToAdd []*sstable.SSTable) error {
	if len(tablesToAdd) == 0 {
		return nil
	}

	// Validate and add to map first
	for _, table := range tablesToAdd {
		if table == nil {
			return fmt.Errorf("cannot add nil table to LevelState")
		}
		if _, exists := ls.tableMap[table.ID()]; exists {
			return fmt.Errorf("table with ID %d already exists in level %d", table.ID(), ls.levelNumber)
		}
	}

	// Add to map and update totalSize
	for _, table := range tablesToAdd {
		ls.totalSize += table.Size()
		ls.tableMap[table.ID()] = table
		ls.tables = append(ls.tables, table)
	}

	// Sort the entire slice once
	ls.sortTables()
	return nil
}

// sortTables sorts the tables slice based on the level type.
// L0: sorted by ID descending (newest first)
// L1+: sorted by MinKey ascending
func (ls *LevelState) sortTables() {
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
}

// Remove removes a table from the level by its ID.
// Returns an error if the table ID is not found in the level.
func (ls *LevelState) Remove(sstID uint64) error {
	tableToRemove, ok := ls.tableMap[sstID]
	if !ok {
		return fmt.Errorf("SSTable with ID %d not found in LevelState %d", sstID, ls.levelNumber)
	}

	// Remove from map and update totalSize
	delete(ls.tableMap, sstID)
	ls.totalSize -= tableToRemove.Size()

	// Remove from slice without allocating a new underlying array if possible
	ls.removeTableFromSlice(sstID)
	return nil
}

// removeTableFromSlice removes a table from the tables slice by its ID.
// It reuses the existing slice capacity to avoid unnecessary allocations.
func (ls *LevelState) removeTableFromSlice(sstID uint64) {
	newTables := ls.tables[:0]
	for _, table := range ls.tables {
		if table.ID() != sstID {
			newTables = append(newTables, table)
		}
	}
	ls.tables = newTables
}

// RemoveBatch removes multiple tables from the level by their IDs in a single pass.
// This is more efficient than calling Remove in a loop. It does not return an
// error for IDs that are not found - only updates the tables that exist.
func (ls *LevelState) RemoveBatch(sstIDs []uint64) {
	if len(sstIDs) == 0 {
		return
	}

	toRemove := make(map[uint64]struct{}, len(sstIDs))
	for _, id := range sstIDs {
		toRemove[id] = struct{}{}
	}

	// Single pass to remove from both map and slice
	newTables := ls.tables[:0] // Re-slice to avoid new allocation
	for _, table := range ls.tables {
		if _, found := toRemove[table.ID()]; found {
			// This table is marked for removal.
			delete(ls.tableMap, table.ID())
			ls.totalSize -= table.Size()
		} else {
			// Keep this table.
			newTables = append(newTables, table)
		}
	}
	ls.tables = newTables
}

// Size returns the number of tables in the level.
func (ls *LevelState) Size() int {
	return len(ls.tables)
}

// LevelNumber returns the number of the level.
func (ls *LevelState) LevelNumber() int {
	return ls.levelNumber
}

// TotalSize returns the total size of all tables in the level in bytes.
func (ls *LevelState) TotalSize() int64 {
	return ls.totalSize
}

// GetTables returns a copy of the tables slice, preserving order.
// This prevents external modification of the internal slice.
func (ls *LevelState) GetTables() []*sstable.SSTable {
	tablesCopy := make([]*sstable.SSTable, len(ls.tables))
	copy(tablesCopy, ls.tables)
	return tablesCopy
}

// SetTables replaces all tables in the level with a new set.
// It ensures the map and slice are consistent with the new set.
// For L1+, it also sorts the provided tables.
func (ls *LevelState) SetTables(tables []*sstable.SSTable) {
	// Rebuild map and calculate total size
	var newTotalSize int64
	newTableMap := make(map[uint64]*sstable.SSTable, len(tables))
	for _, table := range tables {
		newTableMap[table.ID()] = table
		newTotalSize += table.Size()
	}

	// Update state
	ls.tables = tables
	ls.tableMap = newTableMap
	ls.totalSize = newTotalSize

	// Ensure L1+ levels are sorted correctly after setting.
	if ls.levelNumber > 0 {
		ls.sortTables()
	}
}
