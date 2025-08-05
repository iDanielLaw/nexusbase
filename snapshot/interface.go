package snapshot

import (
	"context"
	"log/slog"
	"time"
)

// SnapshotType defines the type of a snapshot.
type SnapshotType string

const (
	SnapshotTypeFull        SnapshotType = "FULL"
	SnapshotTypeIncremental SnapshotType = "INCR"
)

// Info holds metadata about a single snapshot, useful for listing.
type Info struct {
	ID        string       // e.g., the timestamp-based directory name
	Type      SnapshotType
	CreatedAt time.Time
	Size      int64 // Approximate size on disk
	ParentID  string
}

// RestoreOptions contains the necessary parameters for a restore operation.
// It's a subset of engine.StorageEngineOptions to avoid circular dependencies.
type RestoreOptions struct {
	DataDir string
	Logger  *slog.Logger
}

// ManagerInterface defines a high-level API for managing database snapshots.
type ManagerInterface interface {
	// CreateFull creates a complete, self-contained snapshot in the specified directory.
	CreateFull(ctx context.Context, snapshotDir string) error

	// CreateIncremental creates a new snapshot that only contains changes since the last one.
	CreateIncremental(ctx context.Context, snapshotsBaseDir string) error

	// ListSnapshots scans a base directory and returns information about all snapshots in the chain.
	ListSnapshots(snapshotsBaseDir string) ([]Info, error)
}

