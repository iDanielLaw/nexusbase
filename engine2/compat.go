package engine2

import "github.com/INLOpen/nexusbase/engine"

// Compatibility aliases to ease migrating callers from `engine` -> `engine2`.
// These aliases point to the original `engine` package types so caller code
// can switch imports to `engine2` with minimal changes. This is a temporary
// migration aid and will be removed when `engine` is deleted.
type StorageEngineInterface = engine.StorageEngineInterface
type PubSubInterface = engine.PubSubInterface
type EngineMetrics = engine.EngineMetrics

// Re-export common option types and helper constructors by delegating to the
// legacy `engine` package. This lets callers change imports to `engine2`
// without immediately requiring larger refactors. These are thin wrappers and
// will be removed once the migration completes.
type StorageEngineOptions = engine.StorageEngineOptions

func NewStorageEngine(opts StorageEngineOptions) (StorageEngineInterface, error) {
	return engine.NewStorageEngine(engine.StorageEngineOptions(opts))
}

func NewEngineMetrics(publishGlobally bool, prefix string) *EngineMetrics {
	return engine.NewEngineMetrics(publishGlobally, prefix)
}
