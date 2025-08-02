package goclient

import (
	pb "github.com/INLOpen/nexusbase/clients/goclient/proto"
)

// DataPoint represents a single data point to be written to the database.
type DataPoint struct {
	Metric    string
	Tags      map[string]string
	Timestamp int64
	Fields    map[string]interface{}
}

// QueryParams holds all parameters for a query.
type QueryParams struct {
	Metric             string
	Tags               map[string]string
	StartTime          int64
	EndTime            int64
	AggregationSpecs   []AggregationSpec
	DownsampleInterval string
	Limit              int64
}

// AggregationSpec defines an aggregation function to be applied.
type AggregationSpec struct {
	Function string // e.g., "sum", "avg", "count", "min", "max"
	Field    string
}

// QueryResultItem represents a single item returned from a query.
type QueryResultItem struct {
	Metric           string
	Tags             map[string]string
	Timestamp        int64
	Fields           map[string]interface{}
	IsAggregated     bool
	WindowStartTime  int64
	WindowEndTime    int64
	AggregatedValues map[string]float64
}

// QueryResultIterator is an iterator for query results.
type QueryResultIterator struct {
	stream pb.TSDBService_QueryClient
}
