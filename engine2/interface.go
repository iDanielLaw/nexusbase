package engine2

import (
	"context"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/indexer"
	pb "github.com/INLOpen/nexusbase/replication/proto"
	"github.com/INLOpen/nexusbase/snapshot"
	"github.com/INLOpen/nexusbase/sys"
	"github.com/INLOpen/nexusbase/wal"
	"github.com/INLOpen/nexuscore/utils/clock"
)

// StorageEngineInterface defines the public API for the storage engine used
// within engine2. It mirrors the legacy engine.StorageEngineInterface but
// returns engine2's EngineMetrics type so engine2 can own its metrics type.
type internalFileManage struct {
	Create   sys.CreateHandler
	Open     sys.OpenHandler
	OpenFile sys.OpenFileHandler
}

type StorageEngineInterface interface {
	GetNextSSTableID() uint64
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

	// Replication
	ApplyReplicatedEntry(ctx context.Context, entry *pb.WALEntry) error
	GetLatestAppliedSeqNum() uint64
	ReplaceWithSnapshot(snapshotDir string) error

	// CleanupEngine is intended for internal use by the constructor to clean up
	// resources if initialization fails.
	CleanupEngine()
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
	GetClock() clock.Clock
	GetWAL() wal.WALInterface
	GetStringStore() indexer.StringStoreInterface
	GetSnapshotManager() snapshot.ManagerInterface

	GetSequenceNumber() uint64
}
