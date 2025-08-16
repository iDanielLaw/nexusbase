package core

type AggregationFunc string

const (
	AggAvg   AggregationFunc = "avg"
	AggSum   AggregationFunc = "sum"
	AggMin   AggregationFunc = "min"
	AggMax   AggregationFunc = "max"
	AggCount AggregationFunc = "count"
	AggFirst AggregationFunc = "first"
	AggLast  AggregationFunc = "last"
)

func (agg AggregationFunc) String() string {
	return string(agg)
}

// AggregationSpec defines a single aggregation operation, like avg(latency).
type AggregationSpec struct {
	Function AggregationFunc
	Field    string // Field can be "*" for functions like count(*).
	Alias    string // Optional alias for the result column
}

// QueryParams defines the parameters for a query.
type QueryParams struct {
	Metric             string
	StartTime          int64
	EndTime            int64
	Tags               map[string]string
	AggregationSpecs   []AggregationSpec
	DownsampleInterval string
	IsRelative         bool   // New: Flag to indicate a relative time query
	RelativeDuration   string // New: Stores the duration string, e.g., "1h"
	Order              SortOrder
	Limit              int64
	EmitEmptyWindows   bool
	AfterKey           []byte // For cursor-based pagination
}

// QueryResultItem is a struct that holds a single result from a query.
type QueryResultItem struct {
	Metric           string
	Tags             map[string]string
	Timestamp        int64
	Fields           FieldValues
	IsAggregated     bool
	WindowStartTime  int64
	WindowEndTime    int64
	IsEvent          bool
	AggregatedValues map[string]float64
}

// QueryResultIteratorInterface defines the iterator for query results.
type QueryResultIteratorInterface interface {
	IteratorPoolInterface[*QueryResultItem]
	UnderlyingAt() (*IteratorNode, error) // Expose raw data for cursor creation
}

func (it *QueryResultItem) TypeNode() string {
	return "QUERYITERATOR"
}
