package internal

import (
	"context"

	"github.com/INLOpen/nexusbase/indexer"
	pb "github.com/INLOpen/nexusbase/replication/proto"
	"github.com/INLOpen/nexusbase/snapshot"
	"github.com/INLOpen/nexusbase/wal"
	"github.com/INLOpen/nexuscore/utils/clock"
)

// StorageEngineInternal defines methods intended for internal consumers
// (replication, snapshot machinery, tests) that require access to lower-
// level resources and identifiers.
type StorageEngineInternal interface {
	GetNextSSTableID() uint64
	CreateIncrementalSnapshot(snapshotsBaseDir string) error

	// Replication
	ApplyReplicatedEntry(ctx context.Context, entry *pb.WALEntry) error
	GetLatestAppliedSeqNum() uint64
	ReplaceWithSnapshot(snapshotDir string) error

	// Low-level accessors used by internal subsystems and tests
	GetClock() clock.Clock
	GetWAL() wal.WALInterface
	GetStringStore() indexer.StringStoreInterface
	GetSnapshotManager() snapshot.ManagerInterface
	GetSequenceNumber() uint64
}
