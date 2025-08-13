package wal

import "github.com/INLOpen/nexusbase/core"

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
}
