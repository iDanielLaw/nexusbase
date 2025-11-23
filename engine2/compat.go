package engine2

import (
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine"
)

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

// Subscription types are defined in the legacy `engine` package. Provide
// aliases here so callers that migrate imports to `engine2` can still
// construct and use subscription filters without importing `engine`.
type SubscriptionFilter = engine.SubscriptionFilter
type Subscription = engine.Subscription

func NewStorageEngine(opts StorageEngineOptions) (StorageEngineInterface, error) {
	return engine.NewStorageEngine(engine.StorageEngineOptions(opts))
}

func NewEngineMetrics(publishGlobally bool, prefix string) *EngineMetrics {
	return engine.NewEngineMetrics(publishGlobally, prefix)
}

// GetActiveSeriesSnapshot delegates to the legacy engine helper so callers that
// switch imports to `engine2` can still obtain an active series snapshot
// without importing the legacy package directly. This is a temporary shim
// during migration and will be removed when helper functions are ported.
func GetActiveSeriesSnapshot(e StorageEngineInterface) ([]string, error) {
	return engine.GetActiveSeriesSnapshot(e)
}

// GetBaseOptsForTest delegates the engine test helper to provide standard
// StorageEngineOptions for tests. Returning the alias type lets callers
// import `engine2` only during migration.
func GetBaseOptsForTest(t *testing.T, prefix string) StorageEngineOptions {
	return engine.GetBaseOptsForTest(t, prefix)
}

// HelperDataPoint delegates engine's test helper for creating DataPoints.
func HelperDataPoint(t *testing.T, metric string, tags map[string]string, ts int64, fields map[string]any) core.DataPoint {
	return engine.HelperDataPoint(t, metric, tags, ts, fields)
}
