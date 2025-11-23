package engine2

import (
	"github.com/INLOpen/nexusbase/engine"
)

// Re-export compaction manager types from the legacy engine package to
// avoid duplicating complex compaction logic during the migration.
type CompactionManagerInterface = engine.CompactionManagerInterface
type CompactionManager = engine.CompactionManager
type CompactionManagerParams = engine.CompactionManagerParams
type CompactionOptions = engine.CompactionOptions

func NewCompactionManager(params CompactionManagerParams) (CompactionManagerInterface, error) {
	// Delegate to legacy implementation
	return engine.NewCompactionManager(engine.CompactionManagerParams(params))
}
