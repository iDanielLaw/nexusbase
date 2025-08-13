package core

// RangeTombstone represents a time range within a series that is marked for deletion.
type RangeTombstone struct {
	MinTimestamp int64 `json:"min_ts"`
	MaxTimestamp int64 `json:"max_ts"`
	SeqNum       uint64
}
