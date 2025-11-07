package levels

import "github.com/INLOpen/nexusbase/sstable"

// GetTableIDs extracts IDs from a slice of SSTables.
// This is a helper function used for batch operations and logging.
func GetTableIDs(tables []*sstable.SSTable) []uint64 {
	ids := make([]uint64, len(tables))
	for i, tbl := range tables {
		ids[i] = tbl.ID()
	}
	return ids
}
