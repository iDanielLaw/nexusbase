package snapshot

import (
	"context"
	"log/slog"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/internal"
)

// Info holds metadata about a single snapshot, useful for listing.
type Info struct {
	ID             string // e.g., the timestamp-based directory name
	Type           core.SnapshotType
	CreatedAt      time.Time
	Size           int64  // Approximate size on disk of this individual snapshot.
	TotalChainSize int64  // Approximate cumulative size on disk of this snapshot and all its parents.
	ParentID       string // The ID of the immediate parent snapshot.
}

// PruneOptions defines the policies for pruning old snapshots.
type PruneOptions struct {
	// KeepN specifies the number of the most recent full snapshot chains to keep.
	// A chain consists of a full snapshot and all its subsequent incremental snapshots.
	// If KeepN is 0, all snapshots will be pruned.
	// A negative value will result in an error.
	KeepN int
}

// RestoreOptions contains the necessary parameters for a restore operation.
// It's a subset of engine.StorageEngineOptions to avoid circular dependencies.
type RestoreOptions struct {
	DataDir string
	Logger  *slog.Logger

	wrapper internal.PrivateSnapshotHelper
}

// ManagerInterface defines a high-level API for managing database snapshots.
type ManagerInterface interface {
	// CreateFull creates a complete, self-contained snapshot in the specified directory.
	CreateFull(ctx context.Context, snapshotDir string) error

	// CreateIncremental creates a new snapshot that only contains changes since the last one.
	CreateIncremental(ctx context.Context, snapshotsBaseDir string) error

	// ListSnapshots scans a base directory and returns information about all snapshots in the chain.
	ListSnapshots(snapshotsBaseDir string) ([]Info, error)

	// Validate checks the integrity of a snapshot and its entire parent chain.
	// It verifies that all parent snapshots exist and their manifests are readable.
	Validate(snapshotDir string) error

	// Prune deletes old snapshots based on the provided policy.
	// It returns a list of the snapshot IDs that were deleted.
	Prune(ctx context.Context, snapshotsBaseDir string, opts PruneOptions) (deletedIDs []string, err error)
}
