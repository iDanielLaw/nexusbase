// Code generated from ./Nexus.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // Nexus
import "github.com/antlr4-go/antlr/v4"

// BaseNexusListener is a complete listener for a parse tree produced by NexusParser.
type BaseNexusListener struct{}

var _ NexusListener = &BaseNexusListener{}

// VisitTerminal is called when a terminal node is visited.
func (s *BaseNexusListener) VisitTerminal(node antlr.TerminalNode) {}

// VisitErrorNode is called when an error node is visited.
func (s *BaseNexusListener) VisitErrorNode(node antlr.ErrorNode) {}

// EnterEveryRule is called when any rule is entered.
func (s *BaseNexusListener) EnterEveryRule(ctx antlr.ParserRuleContext) {}

// ExitEveryRule is called when any rule is exited.
func (s *BaseNexusListener) ExitEveryRule(ctx antlr.ParserRuleContext) {}

// EnterStatement is called when production statement is entered.
func (s *BaseNexusListener) EnterStatement(ctx *StatementContext) {}

// ExitStatement is called when production statement is exited.
func (s *BaseNexusListener) ExitStatement(ctx *StatementContext) {}

// EnterSnapshotStatement is called when production snapshotStatement is entered.
func (s *BaseNexusListener) EnterSnapshotStatement(ctx *SnapshotStatementContext) {}

// ExitSnapshotStatement is called when production snapshotStatement is exited.
func (s *BaseNexusListener) ExitSnapshotStatement(ctx *SnapshotStatementContext) {}

// EnterRestoreStatement is called when production restoreStatement is entered.
func (s *BaseNexusListener) EnterRestoreStatement(ctx *RestoreStatementContext) {}

// ExitRestoreStatement is called when production restoreStatement is exited.
func (s *BaseNexusListener) ExitRestoreStatement(ctx *RestoreStatementContext) {}

// EnterPushStatement is called when production pushStatement is entered.
func (s *BaseNexusListener) EnterPushStatement(ctx *PushStatementContext) {}

// ExitPushStatement is called when production pushStatement is exited.
func (s *BaseNexusListener) ExitPushStatement(ctx *PushStatementContext) {}

// EnterQueryStatement is called when production queryStatement is entered.
func (s *BaseNexusListener) EnterQueryStatement(ctx *QueryStatementContext) {}

// ExitQueryStatement is called when production queryStatement is exited.
func (s *BaseNexusListener) ExitQueryStatement(ctx *QueryStatementContext) {}

// EnterTime_range is called when production time_range is entered.
func (s *BaseNexusListener) EnterTime_range(ctx *Time_rangeContext) {}

// ExitTime_range is called when production time_range is exited.
func (s *BaseNexusListener) ExitTime_range(ctx *Time_rangeContext) {}

// EnterQuery_clauses is called when production query_clauses is entered.
func (s *BaseNexusListener) EnterQuery_clauses(ctx *Query_clausesContext) {}

// ExitQuery_clauses is called when production query_clauses is exited.
func (s *BaseNexusListener) ExitQuery_clauses(ctx *Query_clausesContext) {}

// EnterRemoveStatement is called when production removeStatement is entered.
func (s *BaseNexusListener) EnterRemoveStatement(ctx *RemoveStatementContext) {}

// ExitRemoveStatement is called when production removeStatement is exited.
func (s *BaseNexusListener) ExitRemoveStatement(ctx *RemoveStatementContext) {}

// EnterShowStatement is called when production showStatement is entered.
func (s *BaseNexusListener) EnterShowStatement(ctx *ShowStatementContext) {}

// ExitShowStatement is called when production showStatement is exited.
func (s *BaseNexusListener) ExitShowStatement(ctx *ShowStatementContext) {}

// EnterFlushStatement is called when production flushStatement is entered.
func (s *BaseNexusListener) EnterFlushStatement(ctx *FlushStatementContext) {}

// ExitFlushStatement is called when production flushStatement is exited.
func (s *BaseNexusListener) ExitFlushStatement(ctx *FlushStatementContext) {}

// EnterAggregation_spec_list is called when production aggregation_spec_list is entered.
func (s *BaseNexusListener) EnterAggregation_spec_list(ctx *Aggregation_spec_listContext) {}

// ExitAggregation_spec_list is called when production aggregation_spec_list is exited.
func (s *BaseNexusListener) ExitAggregation_spec_list(ctx *Aggregation_spec_listContext) {}

// EnterAggregation_spec is called when production aggregation_spec is entered.
func (s *BaseNexusListener) EnterAggregation_spec(ctx *Aggregation_specContext) {}

// ExitAggregation_spec is called when production aggregation_spec is exited.
func (s *BaseNexusListener) ExitAggregation_spec(ctx *Aggregation_specContext) {}

// EnterSeries_specifier is called when production series_specifier is entered.
func (s *BaseNexusListener) EnterSeries_specifier(ctx *Series_specifierContext) {}

// ExitSeries_specifier is called when production series_specifier is exited.
func (s *BaseNexusListener) ExitSeries_specifier(ctx *Series_specifierContext) {}

// EnterMetric_name is called when production metric_name is entered.
func (s *BaseNexusListener) EnterMetric_name(ctx *Metric_nameContext) {}

// ExitMetric_name is called when production metric_name is exited.
func (s *BaseNexusListener) ExitMetric_name(ctx *Metric_nameContext) {}

// EnterTag_list is called when production tag_list is entered.
func (s *BaseNexusListener) EnterTag_list(ctx *Tag_listContext) {}

// ExitTag_list is called when production tag_list is exited.
func (s *BaseNexusListener) ExitTag_list(ctx *Tag_listContext) {}

// EnterTag_assignment is called when production tag_assignment is entered.
func (s *BaseNexusListener) EnterTag_assignment(ctx *Tag_assignmentContext) {}

// ExitTag_assignment is called when production tag_assignment is exited.
func (s *BaseNexusListener) ExitTag_assignment(ctx *Tag_assignmentContext) {}

// EnterTag_value is called when production tag_value is entered.
func (s *BaseNexusListener) EnterTag_value(ctx *Tag_valueContext) {}

// ExitTag_value is called when production tag_value is exited.
func (s *BaseNexusListener) ExitTag_value(ctx *Tag_valueContext) {}

// EnterField_list is called when production field_list is entered.
func (s *BaseNexusListener) EnterField_list(ctx *Field_listContext) {}

// ExitField_list is called when production field_list is exited.
func (s *BaseNexusListener) ExitField_list(ctx *Field_listContext) {}

// EnterField_assignment is called when production field_assignment is entered.
func (s *BaseNexusListener) EnterField_assignment(ctx *Field_assignmentContext) {}

// ExitField_assignment is called when production field_assignment is exited.
func (s *BaseNexusListener) ExitField_assignment(ctx *Field_assignmentContext) {}

// EnterTimestampLiteral is called when production TimestampLiteral is entered.
func (s *BaseNexusListener) EnterTimestampLiteral(ctx *TimestampLiteralContext) {}

// ExitTimestampLiteral is called when production TimestampLiteral is exited.
func (s *BaseNexusListener) ExitTimestampLiteral(ctx *TimestampLiteralContext) {}

// EnterTimestampNow is called when production TimestampNow is entered.
func (s *BaseNexusListener) EnterTimestampNow(ctx *TimestampNowContext) {}

// ExitTimestampNow is called when production TimestampNow is exited.
func (s *BaseNexusListener) ExitTimestampNow(ctx *TimestampNowContext) {}

// EnterTimestampNowRelative is called when production TimestampNowRelative is entered.
func (s *BaseNexusListener) EnterTimestampNowRelative(ctx *TimestampNowRelativeContext) {}

// ExitTimestampNowRelative is called when production TimestampNowRelative is exited.
func (s *BaseNexusListener) ExitTimestampNowRelative(ctx *TimestampNowRelativeContext) {}

// EnterTimestampDateTime is called when production TimestampDateTime is entered.
func (s *BaseNexusListener) EnterTimestampDateTime(ctx *TimestampDateTimeContext) {}

// ExitTimestampDateTime is called when production TimestampDateTime is exited.
func (s *BaseNexusListener) ExitTimestampDateTime(ctx *TimestampDateTimeContext) {}

// EnterDuration is called when production duration is entered.
func (s *BaseNexusListener) EnterDuration(ctx *DurationContext) {}

// ExitDuration is called when production duration is exited.
func (s *BaseNexusListener) ExitDuration(ctx *DurationContext) {}

// EnterValue is called when production value is entered.
func (s *BaseNexusListener) EnterValue(ctx *ValueContext) {}

// ExitValue is called when production value is exited.
func (s *BaseNexusListener) ExitValue(ctx *ValueContext) {}

// EnterLiteral_value is called when production literal_value is entered.
func (s *BaseNexusListener) EnterLiteral_value(ctx *Literal_valueContext) {}

// ExitLiteral_value is called when production literal_value is exited.
func (s *BaseNexusListener) ExitLiteral_value(ctx *Literal_valueContext) {}
