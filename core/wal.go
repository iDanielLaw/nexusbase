package core

// WALEntry represents a single operation recorded in the WAL.
type WALEntry struct {
	EntryType EntryType
	Key       []byte
	Value     []byte
	SeqNum    uint64
}
