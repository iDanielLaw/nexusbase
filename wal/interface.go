package wal

import (
	"context"

	"github.com/INLOpen/nexusbase/core"
)

// WALReader defines an interface for reading entries sequentially from the WAL.
type WALReader interface {
	Next(ctx context.Context) (*core.WALEntry, error)
	Close() error
}

// WALInterface defines the public API for the Write-Ahead Log.
type WALInterface interface {
	// AppendBatch writes a slice of WAL entries as a single, atomic record.
	AppendBatch(entries []core.WALEntry) error
	// Append writes a single WALEntry to the log.
	Append(entry core.WALEntry) error
	// Sync flushes the WAL to stable storage.
	Sync() error
	// Purge deletes segment files with an index less than or equal to the given index.
	Purge(upToIndex uint64) error
	Close() error
	Path() string
	SetTestingOnlyInjectCloseError(err error)
	// ActiveSegmentIndex returns the index of the current active segment file.
	ActiveSegmentIndex() uint64
	// Rotate manually triggers a segment rotation.
	Rotate() error
	// OpenReader opens a reader that can stream WAL entries starting from a specific sequence number.
	OpenReader(fromSeqNum uint64) (WALReader, error)
}
