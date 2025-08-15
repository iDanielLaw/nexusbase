// Code generated from ./Nexus.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // Nexus
import "github.com/antlr4-go/antlr/v4"

type BaseNexusVisitor struct {
	*antlr.BaseParseTreeVisitor
}

func (v *BaseNexusVisitor) VisitStatement(ctx *StatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseNexusVisitor) VisitSnapshotStatement(ctx *SnapshotStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseNexusVisitor) VisitRestoreStatement(ctx *RestoreStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseNexusVisitor) VisitPushStatement(ctx *PushStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseNexusVisitor) VisitQueryStatement(ctx *QueryStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseNexusVisitor) VisitTime_range(ctx *Time_rangeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseNexusVisitor) VisitQuery_clauses(ctx *Query_clausesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseNexusVisitor) VisitRemoveStatement(ctx *RemoveStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseNexusVisitor) VisitShowStatement(ctx *ShowStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseNexusVisitor) VisitFlushStatement(ctx *FlushStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseNexusVisitor) VisitAggregation_spec_list(ctx *Aggregation_spec_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseNexusVisitor) VisitAggregation_spec(ctx *Aggregation_specContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseNexusVisitor) VisitSeries_specifier(ctx *Series_specifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseNexusVisitor) VisitMetric_name(ctx *Metric_nameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseNexusVisitor) VisitTag_list(ctx *Tag_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseNexusVisitor) VisitTag_assignment(ctx *Tag_assignmentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseNexusVisitor) VisitTag_value(ctx *Tag_valueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseNexusVisitor) VisitField_list(ctx *Field_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseNexusVisitor) VisitField_assignment(ctx *Field_assignmentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseNexusVisitor) VisitTimestampLiteral(ctx *TimestampLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseNexusVisitor) VisitTimestampNow(ctx *TimestampNowContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseNexusVisitor) VisitTimestampNowRelative(ctx *TimestampNowRelativeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseNexusVisitor) VisitTimestampDateTime(ctx *TimestampDateTimeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseNexusVisitor) VisitDuration(ctx *DurationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseNexusVisitor) VisitValue(ctx *ValueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseNexusVisitor) VisitLiteral_value(ctx *Literal_valueContext) interface{} {
	return v.VisitChildren(ctx)
}
