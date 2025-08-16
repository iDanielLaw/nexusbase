// Generated from d:/go/nexusbase/api/nbql/Nexus.g4 by ANTLR 4.13.1
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link NexusParser}.
 */
public interface NexusListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link NexusParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatement(NexusParser.StatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link NexusParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatement(NexusParser.StatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link NexusParser#pushStatement}.
	 * @param ctx the parse tree
	 */
	void enterPushStatement(NexusParser.PushStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link NexusParser#pushStatement}.
	 * @param ctx the parse tree
	 */
	void exitPushStatement(NexusParser.PushStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link NexusParser#queryStatement}.
	 * @param ctx the parse tree
	 */
	void enterQueryStatement(NexusParser.QueryStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link NexusParser#queryStatement}.
	 * @param ctx the parse tree
	 */
	void exitQueryStatement(NexusParser.QueryStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link NexusParser#time_range}.
	 * @param ctx the parse tree
	 */
	void enterTime_range(NexusParser.Time_rangeContext ctx);
	/**
	 * Exit a parse tree produced by {@link NexusParser#time_range}.
	 * @param ctx the parse tree
	 */
	void exitTime_range(NexusParser.Time_rangeContext ctx);
	/**
	 * Enter a parse tree produced by {@link NexusParser#query_clauses}.
	 * @param ctx the parse tree
	 */
	void enterQuery_clauses(NexusParser.Query_clausesContext ctx);
	/**
	 * Exit a parse tree produced by {@link NexusParser#query_clauses}.
	 * @param ctx the parse tree
	 */
	void exitQuery_clauses(NexusParser.Query_clausesContext ctx);
	/**
	 * Enter a parse tree produced by {@link NexusParser#removeStatement}.
	 * @param ctx the parse tree
	 */
	void enterRemoveStatement(NexusParser.RemoveStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link NexusParser#removeStatement}.
	 * @param ctx the parse tree
	 */
	void exitRemoveStatement(NexusParser.RemoveStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link NexusParser#showStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowStatement(NexusParser.ShowStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link NexusParser#showStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowStatement(NexusParser.ShowStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link NexusParser#flushStatement}.
	 * @param ctx the parse tree
	 */
	void enterFlushStatement(NexusParser.FlushStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link NexusParser#flushStatement}.
	 * @param ctx the parse tree
	 */
	void exitFlushStatement(NexusParser.FlushStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link NexusParser#aggregation_spec_list}.
	 * @param ctx the parse tree
	 */
	void enterAggregation_spec_list(NexusParser.Aggregation_spec_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link NexusParser#aggregation_spec_list}.
	 * @param ctx the parse tree
	 */
	void exitAggregation_spec_list(NexusParser.Aggregation_spec_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link NexusParser#aggregation_spec}.
	 * @param ctx the parse tree
	 */
	void enterAggregation_spec(NexusParser.Aggregation_specContext ctx);
	/**
	 * Exit a parse tree produced by {@link NexusParser#aggregation_spec}.
	 * @param ctx the parse tree
	 */
	void exitAggregation_spec(NexusParser.Aggregation_specContext ctx);
	/**
	 * Enter a parse tree produced by {@link NexusParser#series_specifier}.
	 * @param ctx the parse tree
	 */
	void enterSeries_specifier(NexusParser.Series_specifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link NexusParser#series_specifier}.
	 * @param ctx the parse tree
	 */
	void exitSeries_specifier(NexusParser.Series_specifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link NexusParser#metric_name}.
	 * @param ctx the parse tree
	 */
	void enterMetric_name(NexusParser.Metric_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link NexusParser#metric_name}.
	 * @param ctx the parse tree
	 */
	void exitMetric_name(NexusParser.Metric_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link NexusParser#tag_list}.
	 * @param ctx the parse tree
	 */
	void enterTag_list(NexusParser.Tag_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link NexusParser#tag_list}.
	 * @param ctx the parse tree
	 */
	void exitTag_list(NexusParser.Tag_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link NexusParser#tag_assignment}.
	 * @param ctx the parse tree
	 */
	void enterTag_assignment(NexusParser.Tag_assignmentContext ctx);
	/**
	 * Exit a parse tree produced by {@link NexusParser#tag_assignment}.
	 * @param ctx the parse tree
	 */
	void exitTag_assignment(NexusParser.Tag_assignmentContext ctx);
	/**
	 * Enter a parse tree produced by {@link NexusParser#tag_value}.
	 * @param ctx the parse tree
	 */
	void enterTag_value(NexusParser.Tag_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link NexusParser#tag_value}.
	 * @param ctx the parse tree
	 */
	void exitTag_value(NexusParser.Tag_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link NexusParser#field_list}.
	 * @param ctx the parse tree
	 */
	void enterField_list(NexusParser.Field_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link NexusParser#field_list}.
	 * @param ctx the parse tree
	 */
	void exitField_list(NexusParser.Field_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link NexusParser#field_assignment}.
	 * @param ctx the parse tree
	 */
	void enterField_assignment(NexusParser.Field_assignmentContext ctx);
	/**
	 * Exit a parse tree produced by {@link NexusParser#field_assignment}.
	 * @param ctx the parse tree
	 */
	void exitField_assignment(NexusParser.Field_assignmentContext ctx);
	/**
	 * Enter a parse tree produced by the {@code TimestampLiteral}
	 * labeled alternative in {@link NexusParser#timestamp}.
	 * @param ctx the parse tree
	 */
	void enterTimestampLiteral(NexusParser.TimestampLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code TimestampLiteral}
	 * labeled alternative in {@link NexusParser#timestamp}.
	 * @param ctx the parse tree
	 */
	void exitTimestampLiteral(NexusParser.TimestampLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code TimestampNow}
	 * labeled alternative in {@link NexusParser#timestamp}.
	 * @param ctx the parse tree
	 */
	void enterTimestampNow(NexusParser.TimestampNowContext ctx);
	/**
	 * Exit a parse tree produced by the {@code TimestampNow}
	 * labeled alternative in {@link NexusParser#timestamp}.
	 * @param ctx the parse tree
	 */
	void exitTimestampNow(NexusParser.TimestampNowContext ctx);
	/**
	 * Enter a parse tree produced by the {@code TimestampNowRelative}
	 * labeled alternative in {@link NexusParser#timestamp}.
	 * @param ctx the parse tree
	 */
	void enterTimestampNowRelative(NexusParser.TimestampNowRelativeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code TimestampNowRelative}
	 * labeled alternative in {@link NexusParser#timestamp}.
	 * @param ctx the parse tree
	 */
	void exitTimestampNowRelative(NexusParser.TimestampNowRelativeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code TimestampDateTime}
	 * labeled alternative in {@link NexusParser#timestamp}.
	 * @param ctx the parse tree
	 */
	void enterTimestampDateTime(NexusParser.TimestampDateTimeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code TimestampDateTime}
	 * labeled alternative in {@link NexusParser#timestamp}.
	 * @param ctx the parse tree
	 */
	void exitTimestampDateTime(NexusParser.TimestampDateTimeContext ctx);
	/**
	 * Enter a parse tree produced by {@link NexusParser#duration}.
	 * @param ctx the parse tree
	 */
	void enterDuration(NexusParser.DurationContext ctx);
	/**
	 * Exit a parse tree produced by {@link NexusParser#duration}.
	 * @param ctx the parse tree
	 */
	void exitDuration(NexusParser.DurationContext ctx);
	/**
	 * Enter a parse tree produced by {@link NexusParser#value}.
	 * @param ctx the parse tree
	 */
	void enterValue(NexusParser.ValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link NexusParser#value}.
	 * @param ctx the parse tree
	 */
	void exitValue(NexusParser.ValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link NexusParser#literal_value}.
	 * @param ctx the parse tree
	 */
	void enterLiteral_value(NexusParser.Literal_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link NexusParser#literal_value}.
	 * @param ctx the parse tree
	 */
	void exitLiteral_value(NexusParser.Literal_valueContext ctx);
}