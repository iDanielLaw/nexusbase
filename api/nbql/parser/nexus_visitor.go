// Code generated from ./Nexus.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // Nexus
import "github.com/antlr4-go/antlr/v4"

// A complete Visitor for a parse tree produced by NexusParser.
type NexusVisitor interface {
	antlr.ParseTreeVisitor

	// Visit a parse tree produced by NexusParser#statement.
	VisitStatement(ctx *StatementContext) interface{}

	// Visit a parse tree produced by NexusParser#snapshotStatement.
	VisitSnapshotStatement(ctx *SnapshotStatementContext) interface{}

	// Visit a parse tree produced by NexusParser#restoreStatement.
	VisitRestoreStatement(ctx *RestoreStatementContext) interface{}

	// Visit a parse tree produced by NexusParser#pushStatement.
	VisitPushStatement(ctx *PushStatementContext) interface{}

	// Visit a parse tree produced by NexusParser#queryStatement.
	VisitQueryStatement(ctx *QueryStatementContext) interface{}

	// Visit a parse tree produced by NexusParser#time_range.
	VisitTime_range(ctx *Time_rangeContext) interface{}

	// Visit a parse tree produced by NexusParser#query_clauses.
	VisitQuery_clauses(ctx *Query_clausesContext) interface{}

	// Visit a parse tree produced by NexusParser#removeStatement.
	VisitRemoveStatement(ctx *RemoveStatementContext) interface{}

	// Visit a parse tree produced by NexusParser#showStatement.
	VisitShowStatement(ctx *ShowStatementContext) interface{}

	// Visit a parse tree produced by NexusParser#flushStatement.
	VisitFlushStatement(ctx *FlushStatementContext) interface{}

	// Visit a parse tree produced by NexusParser#aggregation_spec_list.
	VisitAggregation_spec_list(ctx *Aggregation_spec_listContext) interface{}

	// Visit a parse tree produced by NexusParser#aggregation_spec.
	VisitAggregation_spec(ctx *Aggregation_specContext) interface{}

	// Visit a parse tree produced by NexusParser#series_specifier.
	VisitSeries_specifier(ctx *Series_specifierContext) interface{}

	// Visit a parse tree produced by NexusParser#metric_name.
	VisitMetric_name(ctx *Metric_nameContext) interface{}

	// Visit a parse tree produced by NexusParser#tag_list.
	VisitTag_list(ctx *Tag_listContext) interface{}

	// Visit a parse tree produced by NexusParser#tag_assignment.
	VisitTag_assignment(ctx *Tag_assignmentContext) interface{}

	// Visit a parse tree produced by NexusParser#tag_value.
	VisitTag_value(ctx *Tag_valueContext) interface{}

	// Visit a parse tree produced by NexusParser#field_list.
	VisitField_list(ctx *Field_listContext) interface{}

	// Visit a parse tree produced by NexusParser#field_assignment.
	VisitField_assignment(ctx *Field_assignmentContext) interface{}

	// Visit a parse tree produced by NexusParser#TimestampLiteral.
	VisitTimestampLiteral(ctx *TimestampLiteralContext) interface{}

	// Visit a parse tree produced by NexusParser#TimestampNow.
	VisitTimestampNow(ctx *TimestampNowContext) interface{}

	// Visit a parse tree produced by NexusParser#TimestampNowRelative.
	VisitTimestampNowRelative(ctx *TimestampNowRelativeContext) interface{}

	// Visit a parse tree produced by NexusParser#TimestampDateTime.
	VisitTimestampDateTime(ctx *TimestampDateTimeContext) interface{}

	// Visit a parse tree produced by NexusParser#duration.
	VisitDuration(ctx *DurationContext) interface{}

	// Visit a parse tree produced by NexusParser#value.
	VisitValue(ctx *ValueContext) interface{}

	// Visit a parse tree produced by NexusParser#literal_value.
	VisitLiteral_value(ctx *Literal_valueContext) interface{}
}
