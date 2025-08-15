package nbql

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/INLOpen/nexusbase/api/nbql/parser"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/utils"
	"github.com/antlr4-go/antlr/v4"
)

// ASTBuilder implements the NexusVisitor interface to build the AST from the parse tree.
type ASTBuilder struct {
	*parser.BaseNexusVisitor
	errors []error
	clock  utils.Clock
}

// NewASTBuilder creates a new visitor.
func NewASTBuilder(clock utils.Clock) *ASTBuilder {
	return &ASTBuilder{
		BaseNexusVisitor: &parser.BaseNexusVisitor{},
		clock:            clock,
	}
}

// Errors returns all errors accumulated during the visit.
func (v *ASTBuilder) Errors() []error {
	return v.errors
}

// Visit is the entry point for the visitor.
func (v *ASTBuilder) Visit(tree antlr.ParseTree) interface{} {
	return tree.Accept(v)
}

// VisitStatement is the entry point. It delegates to the specific statement visitor.
func (v *ASTBuilder) VisitStatement(ctx *parser.StatementContext) interface{} {
	if ctx.PushStatement() != nil {
		return v.Visit(ctx.PushStatement())
	}
	if ctx.QueryStatement() != nil {
		return v.Visit(ctx.QueryStatement())
	}
	if ctx.RemoveStatement() != nil {
		return v.Visit(ctx.RemoveStatement())
	}
	if ctx.ShowStatement() != nil {
		return v.Visit(ctx.ShowStatement())
	}
	if ctx.FlushStatement() != nil {
		return v.Visit(ctx.FlushStatement())
	}
	if ctx.SnapshotStatement() != nil {
		return v.Visit(ctx.SnapshotStatement())
	}
	if ctx.RestoreStatement() != nil {
		return v.Visit(ctx.RestoreStatement())
	}
	return nil // Should not happen with a valid grammar
}

// VisitPushStatement builds a PushStatement from the parse tree.
func (v *ASTBuilder) VisitPushStatement(ctx *parser.PushStatementContext) interface{} {
	metric := v.visitMetricName(ctx.Metric_name())

	var ts int64
	if ctx.Timestamp() != nil {
		ts = v.visitTimestamp(ctx.Timestamp())
		if len(v.errors) > 0 {
			return nil // Stop processing if timestamp parsing failed
		}
	}

	var tags map[string]string
	if ctx.Tag_list() != nil {
		// Use comma-ok idiom for safer type assertion
		tagResult, ok := v.Visit(ctx.Tag_list()).(map[string]string)
		if !ok {
			v.addError(fmt.Errorf("internal error: failed to parse tag list"))
			return nil
		}
		tags = tagResult
	}

	var fields map[string]interface{}
	if ctx.Field_list() != nil {
		fieldResult, ok := v.Visit(ctx.Field_list()).(map[string]interface{})
		if !ok {
			v.addError(fmt.Errorf("internal error: failed to parse field list"))
			return nil
		}
		fields = fieldResult
	}

	return &PushStatement{
		Metric:    metric,
		Timestamp: ts,
		Tags:      tags,
		Fields:    fields,
	}
}

// VisitQueryStatement builds a QueryStatement from the parse tree.
func (v *ASTBuilder) VisitQueryStatement(ctx *parser.QueryStatementContext) interface{} {
	metric := v.visitMetricName(ctx.Metric_name())
	stmt := &QueryStatement{
		Metric: metric,
	}
	timeRangeCtx := ctx.Time_range()
	if timeRangeCtx.K_RELATIVE() != nil {
		// Handle FROM RELATIVE(1h)
		stmt.IsRelative = true
		stmt.RelativeDuration = timeRangeCtx.DURATION_LITERAL().GetText()
	} else {
		// Handle FROM ... TO ...
		stmt.StartTime = v.visitTimestamp(timeRangeCtx.Timestamp(0))
		if len(v.errors) > 0 {
			return nil
		}
		stmt.EndTime = v.visitTimestamp(timeRangeCtx.Timestamp(1))
		if len(v.errors) > 0 {
			return nil
		}
		// Validate time range, but allow endTime to be 0, which is the sentinel for NOW().
		if stmt.EndTime != 0 && stmt.StartTime > stmt.EndTime {
			v.addError(fmt.Errorf("start time cannot be after end time"))
			return nil
		}
	}

	if ctx.Tag_list() != nil {
		tagResult, ok := v.Visit(ctx.Tag_list()).(map[string]string)
		if !ok {
			v.addError(fmt.Errorf("internal error: failed to parse tag list for query"))
			return nil
		}
		stmt.Tags = tagResult
	}

	// Process the optional query_clauses rule.
	if clausesCtx := ctx.Query_clauses(); clausesCtx != nil {
		// Since AGGREGATE and ORDER are mutually exclusive in the grammar,
		// we can check for them separately. The ANTLR Go target generates
		// methods for all tokens in the rule, even if they are in different alternatives.
		if clausesCtx.K_AGGREGATE() != nil {
			stmt.AggregationSpecs, _ = v.Visit(clausesCtx.Aggregation_spec_list()).([]AggregationSpec)
			if clausesCtx.Duration() != nil {
				stmt.DownsampleInterval = clausesCtx.Duration().GetText()
			}
			if clausesCtx.K_EMPTY() != nil {
				stmt.EmitEmptyWindows = true
			}
		}

		if clausesCtx.K_ORDER() != nil {
			if clausesCtx.K_DESC() != nil {
				stmt.SortOrder = core.Descending
			} else {
				stmt.SortOrder = core.Ascending // Default for ORDER is ASC
			}
		}

		if clausesCtx.K_LIMIT() != nil {
			var err error
			stmt.Limit, err = strconv.ParseInt(clausesCtx.NUMBER().GetText(), 10, 64)
			if err != nil {
				v.addError(fmt.Errorf("invalid number for LIMIT: %s", clausesCtx.NUMBER().GetText()))
				return nil
			}
		}

		if clausesCtx.K_AFTER() != nil {
			stmt.AfterCursor = v.unquote(clausesCtx.STRING_LITERAL().GetText())
			if len(v.errors) > 0 {
				return nil
			}
		}
	}

	return stmt
}

// VisitFlushStatement builds a FlushStatement from the parse tree.
func (v *ASTBuilder) VisitFlushStatement(ctx *parser.FlushStatementContext) interface{} {
	// Default to FLUSH ALL if no specific type is given, matching the grammar's optional part.
	flushType := FlushAll
	if ctx.K_MEMTABLE() != nil {
		flushType = FlushMemtable
	} else if ctx.K_DISK() != nil {
		flushType = FlushDisk
	} else if ctx.K_ALL() != nil {
		flushType = FlushAll
	}
	return &FlushStatement{Type: flushType}
}

// VisitSnapshotStatement builds a SnapshotStatement from the parse tree.
func (v *ASTBuilder) VisitSnapshotStatement(ctx *parser.SnapshotStatementContext) interface{} {
	return &SnapshotStatement{}
}

// VisitRestoreStatement builds a RestoreStatement from the parse tree.
// Note: This requires the ANTLR parser to be regenerated from the latest Nexus.g4 grammar.
func (v *ASTBuilder) VisitRestoreStatement(ctx *parser.RestoreStatementContext) interface{} {
	path := v.unquote(ctx.STRING_LITERAL().GetText())
	if len(v.errors) > 0 {
		return nil
	}

	overwrite := false
	// K_OVERWRITE() will not be nil if 'WITH OVERWRITE' is present in the query.
	if ctx.K_OVERWRITE() != nil {
		overwrite = true
	}

	return &RestoreStatement{
		Path:      path,
		Overwrite: overwrite,
	}
}

// VisitRemoveStatement builds a RemoveStatement from the parse tree.
func (v *ASTBuilder) VisitRemoveStatement(ctx *parser.RemoveStatementContext) interface{} {
	// Case 1: REMOVE SERIES "metric" TAGGED (...)
	if spec := ctx.Series_specifier(); spec != nil {
		metric := v.visitMetricName(spec.Metric_name())
		var tags map[string]string
		if spec.Tag_list() != nil {
			tagResult, ok := v.Visit(spec.Tag_list()).(map[string]string)
			if !ok {
				v.addError(fmt.Errorf("internal error: failed to parse tag list for REMOVE SERIES"))
				return nil
			}
			tags = tagResult
		}
		return &RemoveStatement{
			Type:   RemoveTypeSeries,
			Metric: metric,
			Tags:   tags,
		}
	}

	// Case 2 & 3: REMOVE FROM "metric" TAGGED (...) ...
	metric := v.visitMetricName(ctx.Metric_name())
	tagResult, ok := v.Visit(ctx.Tag_list()).(map[string]string)
	if !ok {
		v.addError(fmt.Errorf("internal error: failed to parse tag list for REMOVE"))
		return nil
	}
	tags := tagResult

	// Case 2: ... AT <timestamp>
	if ctx.K_AT() != nil {
		ts := v.visitTimestamp(ctx.Timestamp(0))
		if len(v.errors) > 0 {
			return nil
		}
		return &RemoveStatement{
			Type:      RemoveTypePoint,
			Metric:    metric,
			Tags:      tags,
			StartTime: ts, // Use StartTime for the single point-in-time timestamp
		}
	}

	// Case 3: ... FROM <start> TO <end>
	startTime := v.visitTimestamp(ctx.Timestamp(0))
	endTime := v.visitTimestamp(ctx.Timestamp(1))
	if len(v.errors) > 0 {
		return nil
	}

	return &RemoveStatement{
		Type:      RemoveTypeRange,
		Metric:    metric,
		Tags:      tags,
		StartTime: startTime,
		EndTime:   endTime,
	}
}

// VisitShowStatement builds a ShowStatement from the parse tree.
func (v *ASTBuilder) VisitShowStatement(ctx *parser.ShowStatementContext) interface{} {
	// Case 1: SHOW METRICS
	if ctx.K_METRICS() != nil {
		return &ShowStatement{Type: ShowMetrics}
	}

	// Case 2: SHOW TAG KEYS FROM <metric>
	if ctx.K_KEYS() != nil {
		metricCtx := ctx.Metric_name()
		if metricCtx == nil {
			v.addError(fmt.Errorf("SHOW TAG KEYS requires a FROM <metric> clause"))
			return nil
		}
		metric := v.visitMetricName(metricCtx)
		return &ShowStatement{
			Type:   ShowTagKeys,
			Metric: metric,
		}
	}

	// Case 3: SHOW TAG VALUES [FROM <metric>] WITH KEY = <key>
	if ctx.K_VALUES() != nil {
		var metric string
		if ctx.Metric_name() != nil {
			metric = v.visitMetricName(ctx.Metric_name())
		}
		tagKey := v.unquote(ctx.Tag_value().GetText())
		return &ShowStatement{
			Type:   ShowTagValues,
			Metric: metric,
			TagKey: tagKey,
		}
	}

	v.addError(fmt.Errorf("unrecognized SHOW statement structure"))
	return nil
}

// --- Helper methods for visiting rule fragments ---

// VisitTag_list builds a map of tags.
func (v *ASTBuilder) VisitTag_list(ctx *parser.Tag_listContext) interface{} {
	tags := make(map[string]string)
	for _, assignCtx := range ctx.AllTag_assignment() {
		key := ""
		if assignCtx.IDENTIFIER() != nil {
			key = assignCtx.IDENTIFIER().GetText()
		} else {
			key = v.unquote(assignCtx.STRING_LITERAL().GetText())
		}
		value := v.unquote(assignCtx.Tag_value().GetText())
		tags[key] = value
	}
	return tags
}

// VisitField_list builds a map of fields.
func (v *ASTBuilder) VisitField_list(ctx *parser.Field_listContext) interface{} {
	fields := make(map[string]interface{})
	for _, assignCtx := range ctx.AllField_assignment() {
		key := assignCtx.IDENTIFIER().GetText()
		value := v.visitLiteralValue(assignCtx.Literal_value())
		fields[key] = value
	}
	return fields
}

// visitLiteralValue converts a literal token to its Go type.
func (v *ASTBuilder) visitLiteralValue(ctx parser.ILiteral_valueContext) interface{} {
	if ctx.NUMBER() != nil {
		text := ctx.NUMBER().GetText()
		if strings.Contains(text, ".") {
			f, err := strconv.ParseFloat(text, 64)
			if err != nil {
				v.addError(fmt.Errorf("invalid float literal: %s", text))
				return nil
			}
			return f
		}
		i, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			v.addError(fmt.Errorf("invalid integer literal: %s", text))
			return nil
		}
		return i
	}
	if ctx.STRING_LITERAL() != nil {
		return v.unquote(ctx.STRING_LITERAL().GetText())
	}
	if ctx.K_TRUE() != nil {
		return true
	}
	if ctx.K_FALSE() != nil {
		return false
	}
	if ctx.K_NULL() != nil {
		return nil
	}
	return nil
}

// VisitAggregation_spec_list builds a slice of AggregationSpec.
func (v *ASTBuilder) VisitAggregation_spec_list(ctx *parser.Aggregation_spec_listContext) interface{} {
	specs := []AggregationSpec{}
	for _, specCtx := range ctx.AllAggregation_spec() {
		specResult := v.Visit(specCtx)
		if spec, ok := specResult.(AggregationSpec); ok {
			specs = append(specs, spec)
		} else {
			v.addError(fmt.Errorf("internal error: failed to parse aggregation spec"))
			return nil // Stop processing if one spec fails
		}
	}
	return specs
}

// VisitAggregation_spec builds a single AggregationSpec.
func (v *ASTBuilder) VisitAggregation_spec(ctx *parser.Aggregation_specContext) interface{} {
	function := ctx.IDENTIFIER(0).GetText() // The aggregation function name
	argNode := ctx.GetChild(2)              // The argument inside the parentheses: IDENTIFIER or '*'
	field := argNode.GetPayload().(antlr.Token).GetText()

	spec := AggregationSpec{
		Function: function,
		Field:    field,
	}

	// Check for an alias (AS keyword)
	if ctx.K_AS() != nil {
		// The alias is always the last IDENTIFIER in the rule.
		allIdentifiers := ctx.AllIDENTIFIER()
		spec.Alias = allIdentifiers[len(allIdentifiers)-1].GetText()
	}
	return spec
}

// parseDuration extends time.ParseDuration to support days (d), weeks (w), and years (y).
// The grammar allows these units, but the standard library function does not.
func parseDuration(s string) (time.Duration, error) {
	// First, try the standard library parser for common units (h, m, s, ms, etc.)
	d, originalErr := time.ParseDuration(s)
	if originalErr == nil {
		return d, nil
	}

	// If that fails, it might be one of our custom units (d, w, y).
	if len(s) < 2 {
		// If it's too short for our custom units, return the original error.
		return 0, originalErr
	}

	unit := s[len(s)-1]
	// Only attempt custom parsing for our specific units.
	if unit != 'd' && unit != 'w' && unit != 'y' {
		// For any other error (e.g., "5x", "123"), return the original error from time.ParseDuration.
		return 0, originalErr
	}

	// The format is assumed to be [0-9]+[dwy].
	valueStr := s[:len(s)-1]

	value, err := strconv.ParseInt(valueStr, 10, 64)
	if err != nil {
		// The number part is invalid, return a new error.
		return 0, fmt.Errorf("invalid duration value in %q: %w", s, err)
	}

	var customDuration time.Duration
	switch unit {
	case 'd':
		customDuration = time.Hour * 24 * time.Duration(value)
	case 'w':
		customDuration = time.Hour * 24 * 7 * time.Duration(value)
	case 'y':
		// This is an approximation. A year is ~365.25 days.
		// For simplicity and consistency, we'll use 365 days.
		customDuration = time.Hour * 24 * 365 * time.Duration(value)
	}

	return customDuration, nil
}

func (v *ASTBuilder) visitTimestamp(ctx parser.ITimestampContext) int64 {
	result := ctx.Accept(v)
	if result == nil {
		return 0
	}
	ts, ok := result.(int64)
	if !ok {
		v.addError(fmt.Errorf("internal error: timestamp expression did not evaluate to int64"))
		return 0
	}
	return ts
}

func (v *ASTBuilder) VisitTimestampLiteral(ctx *parser.TimestampLiteralContext) interface{} {
	val, err := strconv.ParseInt(ctx.NUMBER().GetText(), 10, 64)
	if err != nil {
		v.addError(fmt.Errorf("invalid timestamp literal: %s", ctx.NUMBER().GetText()))
		return nil
	}
	return val
}

func (v *ASTBuilder) VisitTimestampNow(ctx *parser.TimestampNowContext) interface{} {
	// Return 0 as a special value for NOW(), to be resolved by the executor.
	return int64(0)
}

func (v *ASTBuilder) VisitTimestampNowRelative(ctx *parser.TimestampNowRelativeContext) interface{} {
	durationStr := ctx.DURATION_LITERAL().GetText()
	d, err := parseDuration(durationStr)
	if err != nil {
		v.addError(fmt.Errorf("invalid duration format: %s", durationStr))
		return int64(0)
	}
	if ctx.MINUS() != nil {
		d = -d
	}
	return v.clock.Now().Add(d).UnixNano()
}

func (v *ASTBuilder) VisitTimestampDateTime(ctx *parser.TimestampDateTimeContext) interface{} {
	unquotedStr := v.unquote(ctx.STRING_LITERAL().GetText())
	parsedTime, err := time.Parse("2006-01-02 15:04:05Z07:00", unquotedStr)
	if err != nil {
		parsedTime, err = time.Parse(time.RFC3339, unquotedStr)
	}
	if err != nil {
		v.addError(fmt.Errorf("invalid datetime format for DT(): %q", unquotedStr))
		return int64(0)
	}
	return parsedTime.UnixNano()
}

func (v *ASTBuilder) visitMetricName(ctx parser.IMetric_nameContext) string {
	if ctx.IDENTIFIER() != nil {
		return ctx.IDENTIFIER().GetText()
	}
	return v.unquote(ctx.STRING_LITERAL().GetText())
}

func (v *ASTBuilder) unquote(s string) string {
	if len(s) >= 2 {
		// Handle single-quoted strings manually, as strconv.Unquote does not support them.
		if s[0] == '\'' && s[len(s)-1] == '\'' {
			// The grammar defines '' as the escape sequence for a single quote.
			return strings.ReplaceAll(s[1:len(s)-1], "''", "'")
		}
	}

	// For double-quoted and back-ticked strings, strconv.Unquote works perfectly.
	unquoted, err := strconv.Unquote(s)
	if err != nil {
		v.addError(fmt.Errorf("invalid string literal: %s", s))
		return s // Return original string on error to avoid cascading issues
	}
	return unquoted
}

func (v *ASTBuilder) addError(err error) {
	v.errors = append(v.errors, err)
}

// VisitTerminal is a default visitor method.
func (v *ASTBuilder) VisitTerminal(node antlr.TerminalNode) interface{} { return nil }
