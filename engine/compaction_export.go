package engine

import (
	"context"

	"github.com/INLOpen/nexusbase/sstable"
)

// ExportMergeMultipleSSTables is a small exported wrapper to allow external
// packages (tests in engine2 during migration) to call the internal
// mergeMultipleSSTables implementation without duplicating logic.
func ExportMergeMultipleSSTables(cm *CompactionManager, ctx context.Context, tables []*sstable.SSTable, targetLevel int) ([]*sstable.SSTable, error) {
	return cm.mergeMultipleSSTables(ctx, tables, targetLevel)
}
