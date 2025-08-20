package core

// WALEntry represents a single operation recorded in the WAL.
type WALEntry struct {
	EntryType    EntryType
	Key          []byte
	Value        []byte
	SeqNum       uint64
	SegmentIndex uint64
	// DataPoint holds the original, unencoded data. This is crucial for followers
	// to reconstruct their own metadata (string/series stores) without needing
	// to query the leader, ensuring self-contained replication entries.
	DataPoint DataPoint `json:"-"` // Exclude from default JSON marshaling if any
}

type WALSyncMode string

const (
	WALSyncAlways   WALSyncMode = "always"   // Sync after every append (highest durability, lowest performance)
	WALSyncInterval WALSyncMode = "interval" // Sync periodically (not handled by WAL anymore, but by engine)
	WALSyncDisabled WALSyncMode = "disabled" // No sync (for testing/benchmarking, high risk of data loss)
)
