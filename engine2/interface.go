package engine2

import (
	"context"

	"github.com/INLOpen/nexusbase/core"
	internalpkg "github.com/INLOpen/nexusbase/engine2/internal"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/sys"
)

// StorageEngineInterface defines the public API for the storage engine used
// within engine2. It mirrors the legacy engine.StorageEngineInterface but
// returns engine2's EngineMetrics type so engine2 can own its metrics type.
type internalFileManage struct {
	Create   sys.CreateHandler
	Open     sys.OpenHandler
	OpenFile sys.OpenFileHandler
}

// StorageEngineExternal defines the surface intended for external callers
// (e.g. server handlers, CLI, etc.). These are the public operations a
// consumer of the storage engine will typically invoke.
type StorageEngineExternal interface {
	// Data Manipulation
	Put(ctx context.Context, point core.DataPoint) error
	PutBatch(ctx context.Context, points []core.DataPoint) error
	Get(ctx context.Context, metric string, tags map[string]string, timestamp int64) (core.FieldValues, error)
	Delete(ctx context.Context, metric string, tags map[string]string, timestamp int64) error
	DeleteSeries(ctx context.Context, metric string, tags map[string]string) error
	DeletesByTimeRange(ctx context.Context, metric string, tags map[string]string, startTime, endTime int64) error

	// Querying
	Query(ctx context.Context, params core.QueryParams) (core.QueryResultIteratorInterface, error)
	GetSeriesByTags(metric string, tags map[string]string) ([]string, error)

	// Introspection
	GetMetrics() ([]string, error)
	GetTagsForMetric(metric string) ([]string, error)
	GetTagValues(metric, tagKey string) ([]string, error)

	// Administration & Maintenance
	ForceFlush(ctx context.Context, wait bool) error
	TriggerCompaction()
	CreateIncrementalSnapshot(snapshotsBaseDir string) error
	VerifyDataConsistency() []error

	CreateSnapshot(ctx context.Context) (snapshotPath string, err error)
	RestoreFromSnapshot(ctx context.Context, path string, overwrite bool) error

	Start() error
	Close() error

	// Introspection & Utilities
	GetPubSub() (PubSubInterface, error)
	GetSnapshotsBaseDir() string
	Metrics() (*EngineMetrics, error)
	GetHookManager() hooks.HookManager
	GetDLQDir() string
	GetDataDir() string
	GetWALPath() string
}

// StorageEngineInternal defines methods intended for internal consumers
// (replication, snapshot machinery, tests) that require access to lower-
// level resources and identifiers. This is kept separate to make the
// external surface clearer while preserving the full combined interface
// for backward compatibility.
// StorageEngineInternal is provided from the internal package below.

// StorageEngineInterface is the full interface (external + internal).
// It preserves backward compatibility for existing callers that expect
// a single aggregated interface type.
type StorageEngineInterface interface {
	StorageEngineExternal
	internalpkg.StorageEngineInternal
}
