package nbql

import "github.com/INLOpen/nexusbase/core"

// Command is the interface for all executable commands.
type Command interface {
	isCommand() // Marker method
}

// UnsupportedStatement is a placeholder for commands that are parsed but not yet implemented.
type UnsupportedStatement struct{}

func (s *UnsupportedStatement) isCommand() {}

// PushStatement represents a PUSH command in NBQL.
type PushStatement struct {
	Metric    string
	Tags      map[string]string
	Timestamp int64
	Fields    map[string]interface{}
}

func (s *PushStatement) isCommand() {}

// PushsStatement represents a PUSHS command in NBQL.
type PushsStatement struct {
	Items []PushStatement
}

func (s *PushsStatement) isCommand() {}

// QueryStatement represents a QUERY command in NBQL.
type QueryStatement struct {
	Metric             string
	Tags               map[string]string
	StartTime          int64
	EndTime            int64
	AggregationSpecs   []AggregationSpec
	DownsampleInterval string
	IsRelative         bool   // New: Flag to indicate a relative time query
	RelativeDuration   string // New: Stores the duration string, e.g., "1h"
	Limit              int64
	EmitEmptyWindows   bool   // For downsampling queries
	AfterCursor        string // For cursor-based pagination
	SortOrder          core.SortOrder
}

func (s *QueryStatement) isCommand() {}

// RemoveStatementType defines the type of REMOVE command.
type RemoveStatementType string

const (
	RemoveTypeSeries RemoveStatementType = "SERIES"
	RemoveTypePoint  RemoveStatementType = "POINT"
	RemoveTypeRange  RemoveStatementType = "RANGE"
)

// RemoveStatement represents a REMOVE command in NBQL.
type RemoveStatement struct {
	Type      RemoveStatementType
	Metric    string
	Tags      map[string]string
	StartTime int64
	EndTime   int64
}

func (s *RemoveStatement) isCommand() {}

// ShowStatementType defines the type of SHOW command.
type ShowStatementType string

const (
	ShowMetrics   ShowStatementType = "METRICS"
	ShowTagKeys   ShowStatementType = "TAG_KEYS"
	ShowTagValues ShowStatementType = "TAG_VALUES"
)

// ShowStatement represents a SHOW command in NBQL.
type ShowStatement struct {
	Type   ShowStatementType
	Metric string
	TagKey string
}

func (s *ShowStatement) isCommand() {}

// FlushType defines the type of FLUSH command.
type FlushType string

const (
	FlushAll      FlushType = "ALL"
	FlushMemtable FlushType = "MEMTABLE"
	FlushDisk     FlushType = "DISK"
)

// FlushStatement represents a FLUSH command in NBQL.
type FlushStatement struct {
	Type FlushType
}

func (s *FlushStatement) isCommand() {}

// SnapshotStatement represents a SNAPSHOT command in NBQL.
type SnapshotStatement struct{}

func (s *SnapshotStatement) isCommand() {}

// RestoreStatement represents a RESTORE command in NBQL.
type RestoreStatement struct {
	Path      string
	Overwrite bool
}

func (s *RestoreStatement) isCommand() {}

// AggregationSpec is the AST representation of an aggregation function call.
type AggregationSpec struct {
	Function string
	Field    string
	Alias    string // Optional alias for the result column
}
