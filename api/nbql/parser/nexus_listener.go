// Code generated from ./Nexus.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // Nexus
import "github.com/antlr4-go/antlr/v4"

// NexusListener is a complete listener for a parse tree produced by NexusParser.
type NexusListener interface {
	antlr.ParseTreeListener

	// EnterStatement is called when entering the statement production.
	EnterStatement(c *StatementContext)

	// EnterSnapshotStatement is called when entering the snapshotStatement production.
	EnterSnapshotStatement(c *SnapshotStatementContext)

	// EnterRestoreStatement is called when entering the restoreStatement production.
	EnterRestoreStatement(c *RestoreStatementContext)

	// EnterPushStatement is called when entering the pushStatement production.
	EnterPushStatement(c *PushStatementContext)

	// EnterQueryStatement is called when entering the queryStatement production.
	EnterQueryStatement(c *QueryStatementContext)

	// EnterTime_range is called when entering the time_range production.
	EnterTime_range(c *Time_rangeContext)

	// EnterQuery_clauses is called when entering the query_clauses production.
	EnterQuery_clauses(c *Query_clausesContext)

	// EnterRemoveStatement is called when entering the removeStatement production.
	EnterRemoveStatement(c *RemoveStatementContext)

	// EnterShowStatement is called when entering the showStatement production.
	EnterShowStatement(c *ShowStatementContext)

	// EnterFlushStatement is called when entering the flushStatement production.
	EnterFlushStatement(c *FlushStatementContext)

	// EnterAggregation_spec_list is called when entering the aggregation_spec_list production.
	EnterAggregation_spec_list(c *Aggregation_spec_listContext)

	// EnterAggregation_spec is called when entering the aggregation_spec production.
	EnterAggregation_spec(c *Aggregation_specContext)

	// EnterSeries_specifier is called when entering the series_specifier production.
	EnterSeries_specifier(c *Series_specifierContext)

	// EnterMetric_name is called when entering the metric_name production.
	EnterMetric_name(c *Metric_nameContext)

	// EnterTag_list is called when entering the tag_list production.
	EnterTag_list(c *Tag_listContext)

	// EnterTag_assignment is called when entering the tag_assignment production.
	EnterTag_assignment(c *Tag_assignmentContext)

	// EnterTag_value is called when entering the tag_value production.
	EnterTag_value(c *Tag_valueContext)

	// EnterField_list is called when entering the field_list production.
	EnterField_list(c *Field_listContext)

	// EnterField_assignment is called when entering the field_assignment production.
	EnterField_assignment(c *Field_assignmentContext)

	// EnterTimestampLiteral is called when entering the TimestampLiteral production.
	EnterTimestampLiteral(c *TimestampLiteralContext)

	// EnterTimestampNow is called when entering the TimestampNow production.
	EnterTimestampNow(c *TimestampNowContext)

	// EnterTimestampNowRelative is called when entering the TimestampNowRelative production.
	EnterTimestampNowRelative(c *TimestampNowRelativeContext)

	// EnterTimestampDateTime is called when entering the TimestampDateTime production.
	EnterTimestampDateTime(c *TimestampDateTimeContext)

	// EnterDuration is called when entering the duration production.
	EnterDuration(c *DurationContext)

	// EnterValue is called when entering the value production.
	EnterValue(c *ValueContext)

	// EnterLiteral_value is called when entering the literal_value production.
	EnterLiteral_value(c *Literal_valueContext)

	// ExitStatement is called when exiting the statement production.
	ExitStatement(c *StatementContext)

	// ExitSnapshotStatement is called when exiting the snapshotStatement production.
	ExitSnapshotStatement(c *SnapshotStatementContext)

	// ExitRestoreStatement is called when exiting the restoreStatement production.
	ExitRestoreStatement(c *RestoreStatementContext)

	// ExitPushStatement is called when exiting the pushStatement production.
	ExitPushStatement(c *PushStatementContext)

	// ExitQueryStatement is called when exiting the queryStatement production.
	ExitQueryStatement(c *QueryStatementContext)

	// ExitTime_range is called when exiting the time_range production.
	ExitTime_range(c *Time_rangeContext)

	// ExitQuery_clauses is called when exiting the query_clauses production.
	ExitQuery_clauses(c *Query_clausesContext)

	// ExitRemoveStatement is called when exiting the removeStatement production.
	ExitRemoveStatement(c *RemoveStatementContext)

	// ExitShowStatement is called when exiting the showStatement production.
	ExitShowStatement(c *ShowStatementContext)

	// ExitFlushStatement is called when exiting the flushStatement production.
	ExitFlushStatement(c *FlushStatementContext)

	// ExitAggregation_spec_list is called when exiting the aggregation_spec_list production.
	ExitAggregation_spec_list(c *Aggregation_spec_listContext)

	// ExitAggregation_spec is called when exiting the aggregation_spec production.
	ExitAggregation_spec(c *Aggregation_specContext)

	// ExitSeries_specifier is called when exiting the series_specifier production.
	ExitSeries_specifier(c *Series_specifierContext)

	// ExitMetric_name is called when exiting the metric_name production.
	ExitMetric_name(c *Metric_nameContext)

	// ExitTag_list is called when exiting the tag_list production.
	ExitTag_list(c *Tag_listContext)

	// ExitTag_assignment is called when exiting the tag_assignment production.
	ExitTag_assignment(c *Tag_assignmentContext)

	// ExitTag_value is called when exiting the tag_value production.
	ExitTag_value(c *Tag_valueContext)

	// ExitField_list is called when exiting the field_list production.
	ExitField_list(c *Field_listContext)

	// ExitField_assignment is called when exiting the field_assignment production.
	ExitField_assignment(c *Field_assignmentContext)

	// ExitTimestampLiteral is called when exiting the TimestampLiteral production.
	ExitTimestampLiteral(c *TimestampLiteralContext)

	// ExitTimestampNow is called when exiting the TimestampNow production.
	ExitTimestampNow(c *TimestampNowContext)

	// ExitTimestampNowRelative is called when exiting the TimestampNowRelative production.
	ExitTimestampNowRelative(c *TimestampNowRelativeContext)

	// ExitTimestampDateTime is called when exiting the TimestampDateTime production.
	ExitTimestampDateTime(c *TimestampDateTimeContext)

	// ExitDuration is called when exiting the duration production.
	ExitDuration(c *DurationContext)

	// ExitValue is called when exiting the value production.
	ExitValue(c *ValueContext)

	// ExitLiteral_value is called when exiting the literal_value production.
	ExitLiteral_value(c *Literal_valueContext)
}
