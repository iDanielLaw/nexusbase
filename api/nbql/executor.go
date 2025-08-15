package nbql

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine"
	"github.com/INLOpen/nexusbase/utils"
)

// Executor processes parsed AST commands and interacts with the storage engine.
type Executor struct {
	engine engine.StorageEngineInterface
	clock  utils.Clock
}

// NewExecutor creates a new command executor.
func NewExecutor(eng engine.StorageEngineInterface, clock utils.Clock) *Executor {
	return &Executor{engine: eng, clock: clock}
}

// Execute takes a command from the AST and runs it against the engine.
// It returns a formatted string response.
func (e *Executor) Execute(ctx context.Context, cmd Command) (interface{}, error) {
	switch c := cmd.(type) {
	case *PushStatement:
		return e.executePush(ctx, c)
	case *PushsStatement:
		return e.executePushs(ctx, c)
	case *QueryStatement:
		return e.executeQuery(ctx, c)
	case *RemoveStatement:
		return e.executeRemove(ctx, c)
	case *ShowStatement:
		return e.executeShow(ctx, c)
	case *FlushStatement:
		return e.executeFlush(ctx, c)
	case *SnapshotStatement:
		return e.executeSnapshot(ctx, c)
	case *RestoreStatement:
		return e.executeRestore(ctx, c)
	default:
		return "", fmt.Errorf("unknown or unsupported command type: %T", c)
	}
}

// executeSnapshot handles the SNAPSHOT command.
func (e *Executor) executeSnapshot(ctx context.Context, cmd *SnapshotStatement) (interface{}, error) {
	// Note: The StorageEngineInterface needs to be updated with a CreateSnapshot method.
	// e.g., CreateSnapshot(ctx context.Context) (snapshotPath string, err error)
	snapshotPath, err := e.engine.CreateSnapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot: %w", err)
	}

	// Return a structured response with the path to the created snapshot.
	return map[string]interface{}{
		"status":  "OK",
		"message": fmt.Sprintf("Snapshot created successfully at: %s", snapshotPath),
		"path":    snapshotPath,
	}, nil
}

// executeRestore handles the RESTORE command.
func (e *Executor) executeRestore(ctx context.Context, cmd *RestoreStatement) (interface{}, error) {
	// Note: The StorageEngineInterface needs to be updated with a RestoreFromSnapshot method.
	// e.g., RestoreFromSnapshot(ctx context.Context, path string, overwrite bool) error
	err := e.engine.RestoreFromSnapshot(ctx, cmd.Path, cmd.Overwrite)
	if err != nil {
		// For restore, we return a standard ManipulateResponse even on error,
		// but the error field will be populated.
		return nil, fmt.Errorf("failed to restore from snapshot '%s': %w", cmd.Path, err)
	}

	return ManipulateResponse{Status: ResponseOK, RowsAffected: 1}, nil
}

// executePush handles the PUSH command.
func (e *Executor) executePush(ctx context.Context, cmd *PushStatement) (interface{}, error) {
	ts := cmd.Timestamp
	if ts == 0 {
		ts = e.clock.Now().UnixNano()
	}

	fieldValues, err := core.NewFieldValuesFromMap(cmd.Fields)
	if err != nil {
		return ManipulateResponse{}, fmt.Errorf("invalid field values for metric '%s': %w", cmd.Metric, err)
	}

	dp := core.DataPoint{Metric: cmd.Metric, Tags: cmd.Tags, Timestamp: ts, Fields: fieldValues}

	if err := e.engine.Put(ctx, dp); err != nil {
		return ManipulateResponse{}, err
	}

	return ManipulateResponse{Status: ResponseOK, RowsAffected: 1}, nil
}

// executePush handles the PUSH command batch.
func (e *Executor) executePushs(ctx context.Context, cmd *PushsStatement) (interface{}, error) {
	now := e.clock.Now().UnixNano()
	dps := make([]core.DataPoint, len(cmd.Items))
	for i, item := range cmd.Items {
		fieldValues, err := core.NewFieldValuesFromMap(item.Fields)
		if err != nil {
			return ManipulateResponse{}, fmt.Errorf("invalid field values for item %d: %w", i, err)
		}
		ts := now
		if item.Timestamp != 0 {
			ts = item.Timestamp
		}

		dps[i] = core.DataPoint{
			Metric:    item.Metric,
			Tags:      item.Tags,
			Fields:    fieldValues,
			Timestamp: ts,
		}
	}
	if err := e.engine.PutBatch(ctx, dps); err != nil {
		return ManipulateResponse{}, err
	}
	return ManipulateResponse{Status: ResponseOK, RowsAffected: uint64(len(cmd.Items))}, nil
}

func (e *Executor) QueryStream(ctx context.Context, cmd *QueryStatement) (core.QueryParams, core.QueryResultIteratorInterface, error) {
	// Translate AST-level AggregationSpecs to core-level AggregationSpecs
	coreAggSpecs := make([]core.AggregationSpec, len(cmd.AggregationSpecs))
	for i, spec := range cmd.AggregationSpecs {
		coreAggSpecs[i] = core.AggregationSpec{
			// Note: We might need validation here to ensure the function string is valid
			// before casting, but for now, we assume the parser and engine will handle it.
			Function: core.AggregationFunc(spec.Function),
			Field:    spec.Field,
			Alias:    spec.Alias,
		}
	}

	params := core.QueryParams{
		Metric:             cmd.Metric,
		StartTime:          cmd.StartTime,
		EndTime:            cmd.EndTime,
		Tags:               cmd.Tags,
		AggregationSpecs:   coreAggSpecs,
		IsRelative:         cmd.IsRelative,
		RelativeDuration:   cmd.RelativeDuration,
		DownsampleInterval: cmd.DownsampleInterval,
		Limit:              cmd.Limit,
		EmitEmptyWindows:   cmd.EmitEmptyWindows,
		Order:              cmd.SortOrder,
	}

	// If a cursor is provided in the query, decode it and set it in the params.
	if cmd.AfterCursor != "" {
		decodedKey, err := base64.StdEncoding.DecodeString(cmd.AfterCursor)
		if err != nil {
			return params, nil, fmt.Errorf("invalid 'after' cursor: not valid base64: %w", err)
		}
		params.AfterKey = decodedKey
	}

	iter, err := e.engine.Query(ctx, params)
	if err != nil {
		return params, nil, err
	}
	return params, iter, nil
}

// executeRemove handles the REMOVE command.
func (e *Executor) executeRemove(ctx context.Context, cmd *RemoveStatement) (interface{}, error) {
	switch cmd.Type {
	case RemoveTypeSeries:
		// This handles REMOVE SERIES "metric" TAGGED (...)
		if err := e.engine.DeleteSeries(ctx, cmd.Metric, cmd.Tags); err != nil {
			return ManipulateResponse{}, err
		}
		// DeleteSeries affects 1 series definition.
		return ManipulateResponse{Status: ResponseOK, RowsAffected: 1}, nil

	case RemoveTypePoint:
		// This handles REMOVE FROM "metric" TAGGED (...) AT <timestamp>
		// We can use DeletesByTimeRange with the same start and end time to delete a single point.
		if err := e.engine.DeletesByTimeRange(ctx, cmd.Metric, cmd.Tags, cmd.StartTime, cmd.StartTime); err != nil {
			return ManipulateResponse{}, fmt.Errorf("failed to delete point for series '%s' with tags %v: %w", cmd.Metric, cmd.Tags, err)
		}
		return ManipulateResponse{Status: ResponseOK, RowsAffected: 1}, nil

	case RemoveTypeRange:
		// This handles REMOVE FROM "metric" TAGGED (...) FROM <start> TO <end>
		if err := e.engine.DeletesByTimeRange(ctx, cmd.Metric, cmd.Tags, cmd.StartTime, cmd.EndTime); err != nil {
			return ManipulateResponse{}, fmt.Errorf("failed to delete range for series '%s' with tags %v: %w", cmd.Metric, cmd.Tags, err)
		}
		// The operation affects 1 series, but the number of deleted points is unknown.
		// We return 1 to indicate the series operation was attempted.
		return ManipulateResponse{Status: ResponseOK, RowsAffected: 1}, nil

	default:
		return ManipulateResponse{}, fmt.Errorf("unsupported REMOVE command type: %s", cmd.Type)
	}
}

// executeShow handles the SHOW command.
func (e *Executor) executeShow(ctx context.Context, cmd *ShowStatement) (interface{}, error) {
	var results []string
	var err error

	switch cmd.Type {
	case ShowMetrics:
		results, err = e.engine.GetMetrics()
	case ShowTagKeys:
		// Corresponds to SHOW TAG KEYS FROM <metric>
		results, err = e.engine.GetTagsForMetric(cmd.Metric)
	case ShowTagValues:
		// Corresponds to SHOW TAG VALUES [FROM <metric>] WITH KEY = <key>
		results, err = e.engine.GetTagValues(cmd.Metric, cmd.TagKey)
	default:
		return "", fmt.Errorf("unsupported SHOW command type: %s", cmd.Type)
	}

	if err != nil {
		return "", err // Propagate engine errors
	}

	if len(results) == 0 {
		return "(empty set)", nil
	}

	return strings.Join(results, "\n"), nil
}

// executeFlush handles the FLUSH command with its variations.
func (e *Executor) executeFlush(ctx context.Context, cmd *FlushStatement) (interface{}, error) {
	switch cmd.Type {
	case FlushMemtable:
		// Asynchronously trigger memtable rotation.
		if err := e.engine.ForceFlush(ctx, false); err != nil {
			return nil, fmt.Errorf("force flush memtable failed: %w", err)
		}
		return ManipulateResponse{Status: ResponseOK /*, Message: "Memtable flush to immutable list triggered."*/}, nil
	case FlushDisk:
		// Asynchronously trigger a compaction cycle check.
		e.engine.TriggerCompaction()
		return ManipulateResponse{Status: ResponseOK /*, Message: "Compaction check triggered."*/}, nil
	case FlushAll:
		// Synchronously flush memtable all the way to disk.
		if err := e.engine.ForceFlush(ctx, true); err != nil {
			return nil, fmt.Errorf("force flush all failed: %w", err)
		}
		return ManipulateResponse{Status: ResponseOK /*, Message: "Memtable flushed to disk successfully."*/}, nil
	default:
		return nil, fmt.Errorf("unknown flush type: %s", cmd.Type)
	}
}

// executeQuery handles the QUERY command.
func (e *Executor) executeQuery(ctx context.Context, cmd *QueryStatement) (interface{}, error) {
	// Translate AST-level AggregationSpecs to core-level AggregationSpecs
	_, iter, err := e.QueryStream(ctx, cmd)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var results []QueryResultLine
	var lastKey []byte
	for iter.Next() {
		// Get the raw key for the cursor *before* decoding the item.
		rawKey, _, _, _ := iter.UnderlyingAt()
		if rawKey != nil {
			lastKey = make([]byte, len(rawKey))
			copy(lastKey, rawKey)
		}

		item, err := iter.At()
		if err != nil {
			return nil, fmt.Errorf("error retrieving query result item: %w", err)
		}

		resultLine := QueryResultLine{
			Metric:           item.Metric,
			Tags:             item.Tags,
			Timestamp:        item.Timestamp,
			IsAggregated:     item.IsAggregated,
			WindowStartTime:  item.WindowStartTime,
			AggregatedValues: item.AggregatedValues,
		}
		if item.Fields != nil {
			resultLine.Fields = item.Fields
		}

		results = append(results, resultLine)

	}

	if err := iter.Error(); err != nil {
		return "", fmt.Errorf("query iterator failed: %w", err)
	}

	// --- Construct Structured Response ---
	response := QueryResponse{
		Results: results,
	}

	// If we hit the query limit, it's likely there's more data.
	// Provide the last key seen as the cursor for the next page.
	if cmd.Limit > 0 && int64(len(results)) == cmd.Limit && lastKey != nil {
		response.NextCursor = base64.StdEncoding.EncodeToString(lastKey)
	}

	// Marshal the structured response to JSON.
	jsonResponse, err := json.Marshal(response)
	return string(jsonResponse), err
}
