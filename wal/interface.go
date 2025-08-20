package wal

import (
	"github.com/INLOpen/nexusbase/core"
)

// StreamReader defines the interface for reading WAL entries as a continuous stream.
// This is designed for replication, where a follower needs to "tail" the leader's WAL.
type StreamReader interface {
	// Next returns the next available WAL entry. If no new entry is available,
	// it should block or return a specific error (like ErrNoNewEntries) to indicate
	// that the caller should retry.
	Next() (*core.WALEntry, error)
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
	// NewStreamReader creates a new reader for streaming WAL entries, starting
	// from the entry immediately after the given sequence number.
	NewStreamReader(fromSeqNum uint64) (StreamReader, error)
}
