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
		// สำหรับ L1+ ค้นหาตำแหน่งที่ถูกต้องเพื่อแทรกและรักษลำดับการเรียงตาม MinKey
		// ซึ่งมีประสิทธิภาพมากกว่าการเพิ่มแล้วเรียงใหม่ทั้งหมดสำหรับการเพิ่มทีละรายการ
		idx := sort.Search(len(ls.tables), func(i int) bool {
			return bytes.Compare(ls.tables[i].MinKey(), table.MinKey()) >= 0
		})

		// แทรกตารางในตำแหน่งที่ค้นพบ
		ls.tables = append(ls.tables, nil) // ขยาย slice
		copy(ls.tables[idx+1:], ls.tables[idx:])
		ls.tables[idx] = table
	}
	ls.totalSize += table.Size()
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
		ls.totalSize += table.Size()
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
	tableToRemove, ok := ls.tableMap[sstID]
	if !ok {
		return fmt.Errorf("SSTable with ID %d not found in LevelState %d", sstID, ls.levelNumber)
	}

	// Remove from map
	delete(ls.tableMap, sstID)
	ls.totalSize -= tableToRemove.Size()

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

// LevelNumber returns the number of the level.
func (ls *LevelState) LevelNumber() int {
	return ls.levelNumber
}

// TotalSize returns the total size of all tables in the level in bytes.
func (ls *LevelState) TotalSize() int64 {
	return ls.totalSize
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
	var newTotalSize int64
	newTableMap := make(map[uint64]*sstable.SSTable, len(tables))
	for _, table := range tables {
		newTableMap[table.ID()] = table
		newTotalSize += table.Size()
	}

	ls.tables = tables // The provided slice becomes the new source of truth for order
	ls.tableMap = newTableMap
	ls.totalSize = newTotalSize

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
