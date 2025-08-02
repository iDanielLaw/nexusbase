// Generated from d:/go/tsdb-prototype/api/nbql/Nexus.g4 by ANTLR 4.13.1
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class NexusParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, K_PUSH=7, K_QUERY=8, K_REMOVE=9, 
		K_SHOW=10, K_SET=11, K_FROM=12, K_TO=13, K_AT=14, K_TAGGED=15, K_AGGREGATE=16, 
		K_BY=17, K_ON=18, K_LIMIT=19, K_SERIES=20, K_AFTER=21, K_EMPTY=22, K_WINDOWS=23, 
		K_METRICS=24, K_TAGS=25, K_TAG=26, K_KEYS=27, K_VALUES=28, K_WITH=29, 
		K_KEY=30, K_TIME=31, K_NOW=32, K_TRUE=33, K_FALSE=34, K_NULL=35, K_FLUSH=36, 
		K_MEMTABLE=37, K_DISK=38, K_ALL=39, K_ORDER=40, K_ASC=41, K_DESC=42, K_AS=43, 
		K_DT=44, K_RELATIVE=45, PLUS=46, MINUS=47, DURATION_LITERAL=48, NUMBER=49, 
		IDENTIFIER=50, STRING_LITERAL=51, WS=52, LINE_COMMENT=53;
	public static final int
		RULE_statement = 0, RULE_pushStatement = 1, RULE_queryStatement = 2, RULE_time_range = 3, 
		RULE_query_clauses = 4, RULE_removeStatement = 5, RULE_showStatement = 6, 
		RULE_flushStatement = 7, RULE_aggregation_spec_list = 8, RULE_aggregation_spec = 9, 
		RULE_series_specifier = 10, RULE_metric_name = 11, RULE_tag_list = 12, 
		RULE_tag_assignment = 13, RULE_tag_value = 14, RULE_field_list = 15, RULE_field_assignment = 16, 
		RULE_timestamp = 17, RULE_duration = 18, RULE_value = 19, RULE_literal_value = 20;
	private static String[] makeRuleNames() {
		return new String[] {
			"statement", "pushStatement", "queryStatement", "time_range", "query_clauses", 
			"removeStatement", "showStatement", "flushStatement", "aggregation_spec_list", 
			"aggregation_spec", "series_specifier", "metric_name", "tag_list", "tag_assignment", 
			"tag_value", "field_list", "field_assignment", "timestamp", "duration", 
			"value", "literal_value"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "';'", "'('", "')'", "'='", "','", "'*'", null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, "'+'", 
			"'-'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, "K_PUSH", "K_QUERY", "K_REMOVE", 
			"K_SHOW", "K_SET", "K_FROM", "K_TO", "K_AT", "K_TAGGED", "K_AGGREGATE", 
			"K_BY", "K_ON", "K_LIMIT", "K_SERIES", "K_AFTER", "K_EMPTY", "K_WINDOWS", 
			"K_METRICS", "K_TAGS", "K_TAG", "K_KEYS", "K_VALUES", "K_WITH", "K_KEY", 
			"K_TIME", "K_NOW", "K_TRUE", "K_FALSE", "K_NULL", "K_FLUSH", "K_MEMTABLE", 
			"K_DISK", "K_ALL", "K_ORDER", "K_ASC", "K_DESC", "K_AS", "K_DT", "K_RELATIVE", 
			"PLUS", "MINUS", "DURATION_LITERAL", "NUMBER", "IDENTIFIER", "STRING_LITERAL", 
			"WS", "LINE_COMMENT"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "Nexus.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public NexusParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StatementContext extends ParserRuleContext {
		public TerminalNode EOF() { return getToken(NexusParser.EOF, 0); }
		public PushStatementContext pushStatement() {
			return getRuleContext(PushStatementContext.class,0);
		}
		public QueryStatementContext queryStatement() {
			return getRuleContext(QueryStatementContext.class,0);
		}
		public RemoveStatementContext removeStatement() {
			return getRuleContext(RemoveStatementContext.class,0);
		}
		public ShowStatementContext showStatement() {
			return getRuleContext(ShowStatementContext.class,0);
		}
		public FlushStatementContext flushStatement() {
			return getRuleContext(FlushStatementContext.class,0);
		}
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(47);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case K_PUSH:
				{
				setState(42);
				pushStatement();
				}
				break;
			case K_QUERY:
				{
				setState(43);
				queryStatement();
				}
				break;
			case K_REMOVE:
				{
				setState(44);
				removeStatement();
				}
				break;
			case K_SHOW:
				{
				setState(45);
				showStatement();
				}
				break;
			case K_FLUSH:
				{
				setState(46);
				flushStatement();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(50);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__0) {
				{
				setState(49);
				match(T__0);
				}
			}

			setState(52);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PushStatementContext extends ParserRuleContext {
		public TerminalNode K_PUSH() { return getToken(NexusParser.K_PUSH, 0); }
		public Metric_nameContext metric_name() {
			return getRuleContext(Metric_nameContext.class,0);
		}
		public TerminalNode K_SET() { return getToken(NexusParser.K_SET, 0); }
		public Field_listContext field_list() {
			return getRuleContext(Field_listContext.class,0);
		}
		public TerminalNode K_TIME() { return getToken(NexusParser.K_TIME, 0); }
		public TimestampContext timestamp() {
			return getRuleContext(TimestampContext.class,0);
		}
		public TerminalNode K_TAGGED() { return getToken(NexusParser.K_TAGGED, 0); }
		public Tag_listContext tag_list() {
			return getRuleContext(Tag_listContext.class,0);
		}
		public PushStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pushStatement; }
	}

	public final PushStatementContext pushStatement() throws RecognitionException {
		PushStatementContext _localctx = new PushStatementContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_pushStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(54);
			match(K_PUSH);
			setState(55);
			metric_name();
			setState(58);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_TIME) {
				{
				setState(56);
				match(K_TIME);
				setState(57);
				timestamp();
				}
			}

			setState(62);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_TAGGED) {
				{
				setState(60);
				match(K_TAGGED);
				setState(61);
				tag_list();
				}
			}

			setState(64);
			match(K_SET);
			setState(65);
			field_list();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryStatementContext extends ParserRuleContext {
		public TerminalNode K_QUERY() { return getToken(NexusParser.K_QUERY, 0); }
		public Metric_nameContext metric_name() {
			return getRuleContext(Metric_nameContext.class,0);
		}
		public Time_rangeContext time_range() {
			return getRuleContext(Time_rangeContext.class,0);
		}
		public TerminalNode K_TAGGED() { return getToken(NexusParser.K_TAGGED, 0); }
		public Tag_listContext tag_list() {
			return getRuleContext(Tag_listContext.class,0);
		}
		public Query_clausesContext query_clauses() {
			return getRuleContext(Query_clausesContext.class,0);
		}
		public QueryStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryStatement; }
	}

	public final QueryStatementContext queryStatement() throws RecognitionException {
		QueryStatementContext _localctx = new QueryStatementContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_queryStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(67);
			match(K_QUERY);
			setState(68);
			metric_name();
			setState(69);
			time_range();
			setState(72);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_TAGGED) {
				{
				setState(70);
				match(K_TAGGED);
				setState(71);
				tag_list();
				}
			}

			setState(75);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 1099514314752L) != 0)) {
				{
				setState(74);
				query_clauses();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Time_rangeContext extends ParserRuleContext {
		public TerminalNode K_FROM() { return getToken(NexusParser.K_FROM, 0); }
		public List<TimestampContext> timestamp() {
			return getRuleContexts(TimestampContext.class);
		}
		public TimestampContext timestamp(int i) {
			return getRuleContext(TimestampContext.class,i);
		}
		public TerminalNode K_TO() { return getToken(NexusParser.K_TO, 0); }
		public TerminalNode K_RELATIVE() { return getToken(NexusParser.K_RELATIVE, 0); }
		public TerminalNode DURATION_LITERAL() { return getToken(NexusParser.DURATION_LITERAL, 0); }
		public Time_rangeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_time_range; }
	}

	public final Time_rangeContext time_range() throws RecognitionException {
		Time_rangeContext _localctx = new Time_rangeContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_time_range);
		try {
			setState(87);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(77);
				match(K_FROM);
				setState(78);
				timestamp();
				setState(79);
				match(K_TO);
				setState(80);
				timestamp();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(82);
				match(K_FROM);
				setState(83);
				match(K_RELATIVE);
				setState(84);
				match(T__1);
				setState(85);
				match(DURATION_LITERAL);
				setState(86);
				match(T__2);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Query_clausesContext extends ParserRuleContext {
		public TerminalNode K_AGGREGATE() { return getToken(NexusParser.K_AGGREGATE, 0); }
		public Aggregation_spec_listContext aggregation_spec_list() {
			return getRuleContext(Aggregation_spec_listContext.class,0);
		}
		public TerminalNode K_LIMIT() { return getToken(NexusParser.K_LIMIT, 0); }
		public TerminalNode NUMBER() { return getToken(NexusParser.NUMBER, 0); }
		public TerminalNode K_AFTER() { return getToken(NexusParser.K_AFTER, 0); }
		public TerminalNode STRING_LITERAL() { return getToken(NexusParser.STRING_LITERAL, 0); }
		public TerminalNode K_BY() { return getToken(NexusParser.K_BY, 0); }
		public DurationContext duration() {
			return getRuleContext(DurationContext.class,0);
		}
		public TerminalNode K_WITH() { return getToken(NexusParser.K_WITH, 0); }
		public TerminalNode K_EMPTY() { return getToken(NexusParser.K_EMPTY, 0); }
		public TerminalNode K_WINDOWS() { return getToken(NexusParser.K_WINDOWS, 0); }
		public TerminalNode K_ORDER() { return getToken(NexusParser.K_ORDER, 0); }
		public TerminalNode K_ASC() { return getToken(NexusParser.K_ASC, 0); }
		public TerminalNode K_DESC() { return getToken(NexusParser.K_DESC, 0); }
		public Query_clausesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query_clauses; }
	}

	public final Query_clausesContext query_clauses() throws RecognitionException {
		Query_clausesContext _localctx = new Query_clausesContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_query_clauses);
		int _la;
		try {
			setState(131);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case K_AGGREGATE:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(89);
				match(K_AGGREGATE);
				setState(92);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_BY) {
					{
					setState(90);
					match(K_BY);
					setState(91);
					duration();
					}
				}

				setState(94);
				match(T__1);
				setState(95);
				aggregation_spec_list();
				setState(96);
				match(T__2);
				setState(100);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_WITH) {
					{
					setState(97);
					match(K_WITH);
					setState(98);
					match(K_EMPTY);
					setState(99);
					match(K_WINDOWS);
					}
				}

				}
				setState(104);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_LIMIT) {
					{
					setState(102);
					match(K_LIMIT);
					setState(103);
					match(NUMBER);
					}
				}

				setState(108);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_AFTER) {
					{
					setState(106);
					match(K_AFTER);
					setState(107);
					match(STRING_LITERAL);
					}
				}

				}
				break;
			case K_ORDER:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(110);
				match(K_ORDER);
				setState(112);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_ASC || _la==K_DESC) {
					{
					setState(111);
					_la = _input.LA(1);
					if ( !(_la==K_ASC || _la==K_DESC) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				}
				setState(116);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_LIMIT) {
					{
					setState(114);
					match(K_LIMIT);
					setState(115);
					match(NUMBER);
					}
				}

				setState(120);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_AFTER) {
					{
					setState(118);
					match(K_AFTER);
					setState(119);
					match(STRING_LITERAL);
					}
				}

				}
				break;
			case K_LIMIT:
				enterOuterAlt(_localctx, 3);
				{
				{
				setState(122);
				match(K_LIMIT);
				setState(123);
				match(NUMBER);
				}
				setState(127);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_AFTER) {
					{
					setState(125);
					match(K_AFTER);
					setState(126);
					match(STRING_LITERAL);
					}
				}

				}
				break;
			case K_AFTER:
				enterOuterAlt(_localctx, 4);
				{
				{
				setState(129);
				match(K_AFTER);
				setState(130);
				match(STRING_LITERAL);
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RemoveStatementContext extends ParserRuleContext {
		public TerminalNode K_REMOVE() { return getToken(NexusParser.K_REMOVE, 0); }
		public Series_specifierContext series_specifier() {
			return getRuleContext(Series_specifierContext.class,0);
		}
		public List<TerminalNode> K_FROM() { return getTokens(NexusParser.K_FROM); }
		public TerminalNode K_FROM(int i) {
			return getToken(NexusParser.K_FROM, i);
		}
		public Metric_nameContext metric_name() {
			return getRuleContext(Metric_nameContext.class,0);
		}
		public TerminalNode K_TAGGED() { return getToken(NexusParser.K_TAGGED, 0); }
		public Tag_listContext tag_list() {
			return getRuleContext(Tag_listContext.class,0);
		}
		public TerminalNode K_AT() { return getToken(NexusParser.K_AT, 0); }
		public List<TimestampContext> timestamp() {
			return getRuleContexts(TimestampContext.class);
		}
		public TimestampContext timestamp(int i) {
			return getRuleContext(TimestampContext.class,i);
		}
		public TerminalNode K_TO() { return getToken(NexusParser.K_TO, 0); }
		public RemoveStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_removeStatement; }
	}

	public final RemoveStatementContext removeStatement() throws RecognitionException {
		RemoveStatementContext _localctx = new RemoveStatementContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_removeStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(133);
			match(K_REMOVE);
			setState(148);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case K_SERIES:
				{
				setState(134);
				series_specifier();
				}
				break;
			case K_FROM:
				{
				setState(135);
				match(K_FROM);
				setState(136);
				metric_name();
				setState(137);
				match(K_TAGGED);
				setState(138);
				tag_list();
				setState(146);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case K_AT:
					{
					setState(139);
					match(K_AT);
					setState(140);
					timestamp();
					}
					break;
				case K_FROM:
					{
					setState(141);
					match(K_FROM);
					setState(142);
					timestamp();
					setState(143);
					match(K_TO);
					setState(144);
					timestamp();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ShowStatementContext extends ParserRuleContext {
		public TerminalNode K_SHOW() { return getToken(NexusParser.K_SHOW, 0); }
		public TerminalNode K_METRICS() { return getToken(NexusParser.K_METRICS, 0); }
		public TerminalNode K_TAG() { return getToken(NexusParser.K_TAG, 0); }
		public TerminalNode K_KEYS() { return getToken(NexusParser.K_KEYS, 0); }
		public TerminalNode K_FROM() { return getToken(NexusParser.K_FROM, 0); }
		public Metric_nameContext metric_name() {
			return getRuleContext(Metric_nameContext.class,0);
		}
		public TerminalNode K_VALUES() { return getToken(NexusParser.K_VALUES, 0); }
		public TerminalNode K_WITH() { return getToken(NexusParser.K_WITH, 0); }
		public TerminalNode K_KEY() { return getToken(NexusParser.K_KEY, 0); }
		public Tag_valueContext tag_value() {
			return getRuleContext(Tag_valueContext.class,0);
		}
		public ShowStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showStatement; }
	}

	public final ShowStatementContext showStatement() throws RecognitionException {
		ShowStatementContext _localctx = new ShowStatementContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_showStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(150);
			match(K_SHOW);
			setState(166);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
			case 1:
				{
				setState(151);
				match(K_METRICS);
				}
				break;
			case 2:
				{
				setState(152);
				match(K_TAG);
				setState(153);
				match(K_KEYS);
				setState(154);
				match(K_FROM);
				setState(155);
				metric_name();
				}
				break;
			case 3:
				{
				setState(156);
				match(K_TAG);
				setState(157);
				match(K_VALUES);
				setState(160);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_FROM) {
					{
					setState(158);
					match(K_FROM);
					setState(159);
					metric_name();
					}
				}

				setState(162);
				match(K_WITH);
				setState(163);
				match(K_KEY);
				setState(164);
				match(T__3);
				setState(165);
				tag_value();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FlushStatementContext extends ParserRuleContext {
		public TerminalNode K_FLUSH() { return getToken(NexusParser.K_FLUSH, 0); }
		public TerminalNode K_MEMTABLE() { return getToken(NexusParser.K_MEMTABLE, 0); }
		public TerminalNode K_DISK() { return getToken(NexusParser.K_DISK, 0); }
		public TerminalNode K_ALL() { return getToken(NexusParser.K_ALL, 0); }
		public FlushStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_flushStatement; }
	}

	public final FlushStatementContext flushStatement() throws RecognitionException {
		FlushStatementContext _localctx = new FlushStatementContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_flushStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(168);
			match(K_FLUSH);
			setState(170);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 962072674304L) != 0)) {
				{
				setState(169);
				_la = _input.LA(1);
				if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 962072674304L) != 0)) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Aggregation_spec_listContext extends ParserRuleContext {
		public List<Aggregation_specContext> aggregation_spec() {
			return getRuleContexts(Aggregation_specContext.class);
		}
		public Aggregation_specContext aggregation_spec(int i) {
			return getRuleContext(Aggregation_specContext.class,i);
		}
		public Aggregation_spec_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aggregation_spec_list; }
	}

	public final Aggregation_spec_listContext aggregation_spec_list() throws RecognitionException {
		Aggregation_spec_listContext _localctx = new Aggregation_spec_listContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_aggregation_spec_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(172);
			aggregation_spec();
			setState(177);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__4) {
				{
				{
				setState(173);
				match(T__4);
				setState(174);
				aggregation_spec();
				}
				}
				setState(179);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Aggregation_specContext extends ParserRuleContext {
		public List<TerminalNode> IDENTIFIER() { return getTokens(NexusParser.IDENTIFIER); }
		public TerminalNode IDENTIFIER(int i) {
			return getToken(NexusParser.IDENTIFIER, i);
		}
		public TerminalNode K_AS() { return getToken(NexusParser.K_AS, 0); }
		public Aggregation_specContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aggregation_spec; }
	}

	public final Aggregation_specContext aggregation_spec() throws RecognitionException {
		Aggregation_specContext _localctx = new Aggregation_specContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_aggregation_spec);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(180);
			match(IDENTIFIER);
			setState(181);
			match(T__1);
			setState(182);
			_la = _input.LA(1);
			if ( !(_la==T__5 || _la==IDENTIFIER) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(183);
			match(T__2);
			setState(186);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_AS) {
				{
				setState(184);
				match(K_AS);
				setState(185);
				match(IDENTIFIER);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Series_specifierContext extends ParserRuleContext {
		public TerminalNode K_SERIES() { return getToken(NexusParser.K_SERIES, 0); }
		public Metric_nameContext metric_name() {
			return getRuleContext(Metric_nameContext.class,0);
		}
		public TerminalNode K_TAGGED() { return getToken(NexusParser.K_TAGGED, 0); }
		public Tag_listContext tag_list() {
			return getRuleContext(Tag_listContext.class,0);
		}
		public Series_specifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_series_specifier; }
	}

	public final Series_specifierContext series_specifier() throws RecognitionException {
		Series_specifierContext _localctx = new Series_specifierContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_series_specifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(188);
			match(K_SERIES);
			setState(189);
			metric_name();
			setState(192);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_TAGGED) {
				{
				setState(190);
				match(K_TAGGED);
				setState(191);
				tag_list();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Metric_nameContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(NexusParser.IDENTIFIER, 0); }
		public TerminalNode STRING_LITERAL() { return getToken(NexusParser.STRING_LITERAL, 0); }
		public Metric_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_metric_name; }
	}

	public final Metric_nameContext metric_name() throws RecognitionException {
		Metric_nameContext _localctx = new Metric_nameContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_metric_name);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(194);
			_la = _input.LA(1);
			if ( !(_la==IDENTIFIER || _la==STRING_LITERAL) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Tag_listContext extends ParserRuleContext {
		public List<Tag_assignmentContext> tag_assignment() {
			return getRuleContexts(Tag_assignmentContext.class);
		}
		public Tag_assignmentContext tag_assignment(int i) {
			return getRuleContext(Tag_assignmentContext.class,i);
		}
		public Tag_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tag_list; }
	}

	public final Tag_listContext tag_list() throws RecognitionException {
		Tag_listContext _localctx = new Tag_listContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_tag_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(196);
			match(T__1);
			setState(197);
			tag_assignment();
			setState(202);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__4) {
				{
				{
				setState(198);
				match(T__4);
				setState(199);
				tag_assignment();
				}
				}
				setState(204);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(205);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Tag_assignmentContext extends ParserRuleContext {
		public Tag_valueContext tag_value() {
			return getRuleContext(Tag_valueContext.class,0);
		}
		public TerminalNode IDENTIFIER() { return getToken(NexusParser.IDENTIFIER, 0); }
		public TerminalNode STRING_LITERAL() { return getToken(NexusParser.STRING_LITERAL, 0); }
		public Tag_assignmentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tag_assignment; }
	}

	public final Tag_assignmentContext tag_assignment() throws RecognitionException {
		Tag_assignmentContext _localctx = new Tag_assignmentContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_tag_assignment);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(207);
			_la = _input.LA(1);
			if ( !(_la==IDENTIFIER || _la==STRING_LITERAL) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(208);
			match(T__3);
			setState(209);
			tag_value();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Tag_valueContext extends ParserRuleContext {
		public TerminalNode STRING_LITERAL() { return getToken(NexusParser.STRING_LITERAL, 0); }
		public Tag_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tag_value; }
	}

	public final Tag_valueContext tag_value() throws RecognitionException {
		Tag_valueContext _localctx = new Tag_valueContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_tag_value);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(211);
			match(STRING_LITERAL);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Field_listContext extends ParserRuleContext {
		public List<Field_assignmentContext> field_assignment() {
			return getRuleContexts(Field_assignmentContext.class);
		}
		public Field_assignmentContext field_assignment(int i) {
			return getRuleContext(Field_assignmentContext.class,i);
		}
		public Field_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_field_list; }
	}

	public final Field_listContext field_list() throws RecognitionException {
		Field_listContext _localctx = new Field_listContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_field_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(213);
			match(T__1);
			setState(214);
			field_assignment();
			setState(219);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__4) {
				{
				{
				setState(215);
				match(T__4);
				setState(216);
				field_assignment();
				}
				}
				setState(221);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(222);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Field_assignmentContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(NexusParser.IDENTIFIER, 0); }
		public Literal_valueContext literal_value() {
			return getRuleContext(Literal_valueContext.class,0);
		}
		public Field_assignmentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_field_assignment; }
	}

	public final Field_assignmentContext field_assignment() throws RecognitionException {
		Field_assignmentContext _localctx = new Field_assignmentContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_field_assignment);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(224);
			match(IDENTIFIER);
			setState(225);
			match(T__3);
			setState(226);
			literal_value();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TimestampContext extends ParserRuleContext {
		public TimestampContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timestamp; }
	 
		public TimestampContext() { }
		public void copyFrom(TimestampContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TimestampNowContext extends TimestampContext {
		public TerminalNode K_NOW() { return getToken(NexusParser.K_NOW, 0); }
		public TimestampNowContext(TimestampContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TimestampLiteralContext extends TimestampContext {
		public TerminalNode NUMBER() { return getToken(NexusParser.NUMBER, 0); }
		public TimestampLiteralContext(TimestampContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TimestampDateTimeContext extends TimestampContext {
		public TerminalNode K_DT() { return getToken(NexusParser.K_DT, 0); }
		public TerminalNode STRING_LITERAL() { return getToken(NexusParser.STRING_LITERAL, 0); }
		public TimestampDateTimeContext(TimestampContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TimestampNowRelativeContext extends TimestampContext {
		public TerminalNode K_NOW() { return getToken(NexusParser.K_NOW, 0); }
		public TerminalNode DURATION_LITERAL() { return getToken(NexusParser.DURATION_LITERAL, 0); }
		public TerminalNode PLUS() { return getToken(NexusParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(NexusParser.MINUS, 0); }
		public TimestampNowRelativeContext(TimestampContext ctx) { copyFrom(ctx); }
	}

	public final TimestampContext timestamp() throws RecognitionException {
		TimestampContext _localctx = new TimestampContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_timestamp);
		int _la;
		try {
			setState(241);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
			case 1:
				_localctx = new TimestampLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(228);
				match(NUMBER);
				}
				break;
			case 2:
				_localctx = new TimestampNowContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(229);
				match(K_NOW);
				setState(230);
				match(T__1);
				setState(231);
				match(T__2);
				}
				break;
			case 3:
				_localctx = new TimestampNowRelativeContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(232);
				match(K_NOW);
				setState(233);
				match(T__1);
				setState(234);
				_la = _input.LA(1);
				if ( !(_la==PLUS || _la==MINUS) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(235);
				match(DURATION_LITERAL);
				setState(236);
				match(T__2);
				}
				break;
			case 4:
				_localctx = new TimestampDateTimeContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(237);
				match(K_DT);
				setState(238);
				match(T__1);
				setState(239);
				match(STRING_LITERAL);
				setState(240);
				match(T__2);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DurationContext extends ParserRuleContext {
		public TerminalNode DURATION_LITERAL() { return getToken(NexusParser.DURATION_LITERAL, 0); }
		public DurationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_duration; }
	}

	public final DurationContext duration() throws RecognitionException {
		DurationContext _localctx = new DurationContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_duration);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(243);
			match(DURATION_LITERAL);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ValueContext extends ParserRuleContext {
		public TerminalNode NUMBER() { return getToken(NexusParser.NUMBER, 0); }
		public ValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_value; }
	}

	public final ValueContext value() throws RecognitionException {
		ValueContext _localctx = new ValueContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_value);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(245);
			match(NUMBER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Literal_valueContext extends ParserRuleContext {
		public TerminalNode NUMBER() { return getToken(NexusParser.NUMBER, 0); }
		public TerminalNode STRING_LITERAL() { return getToken(NexusParser.STRING_LITERAL, 0); }
		public TerminalNode K_TRUE() { return getToken(NexusParser.K_TRUE, 0); }
		public TerminalNode K_FALSE() { return getToken(NexusParser.K_FALSE, 0); }
		public TerminalNode K_NULL() { return getToken(NexusParser.K_NULL, 0); }
		public Literal_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_literal_value; }
	}

	public final Literal_valueContext literal_value() throws RecognitionException {
		Literal_valueContext _localctx = new Literal_valueContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_literal_value);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(247);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 2814809896648704L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static final String _serializedATN =
		"\u0004\u00015\u00fa\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
		"\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004\u0002"+
		"\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007\u0002"+
		"\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b\u0002"+
		"\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007\u000f"+
		"\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007\u0012"+
		"\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0001\u0000\u0001\u0000"+
		"\u0001\u0000\u0001\u0000\u0001\u0000\u0003\u00000\b\u0000\u0001\u0000"+
		"\u0003\u00003\b\u0000\u0001\u0000\u0001\u0000\u0001\u0001\u0001\u0001"+
		"\u0001\u0001\u0001\u0001\u0003\u0001;\b\u0001\u0001\u0001\u0001\u0001"+
		"\u0003\u0001?\b\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0002"+
		"\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0003\u0002I\b\u0002"+
		"\u0001\u0002\u0003\u0002L\b\u0002\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0003\u0003X\b\u0003\u0001\u0004\u0001\u0004\u0001\u0004"+
		"\u0003\u0004]\b\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004"+
		"\u0001\u0004\u0001\u0004\u0003\u0004e\b\u0004\u0001\u0004\u0001\u0004"+
		"\u0003\u0004i\b\u0004\u0001\u0004\u0001\u0004\u0003\u0004m\b\u0004\u0001"+
		"\u0004\u0001\u0004\u0003\u0004q\b\u0004\u0001\u0004\u0001\u0004\u0003"+
		"\u0004u\b\u0004\u0001\u0004\u0001\u0004\u0003\u0004y\b\u0004\u0001\u0004"+
		"\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0003\u0004\u0080\b\u0004"+
		"\u0001\u0004\u0001\u0004\u0003\u0004\u0084\b\u0004\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005"+
		"\u0093\b\u0005\u0003\u0005\u0095\b\u0005\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0001\u0006\u0003\u0006\u00a1\b\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0001\u0006\u0003\u0006\u00a7\b\u0006\u0001\u0007\u0001\u0007\u0003"+
		"\u0007\u00ab\b\u0007\u0001\b\u0001\b\u0001\b\u0005\b\u00b0\b\b\n\b\f\b"+
		"\u00b3\t\b\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t\u0003\t\u00bb"+
		"\b\t\u0001\n\u0001\n\u0001\n\u0001\n\u0003\n\u00c1\b\n\u0001\u000b\u0001"+
		"\u000b\u0001\f\u0001\f\u0001\f\u0001\f\u0005\f\u00c9\b\f\n\f\f\f\u00cc"+
		"\t\f\u0001\f\u0001\f\u0001\r\u0001\r\u0001\r\u0001\r\u0001\u000e\u0001"+
		"\u000e\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0005\u000f\u00da"+
		"\b\u000f\n\u000f\f\u000f\u00dd\t\u000f\u0001\u000f\u0001\u000f\u0001\u0010"+
		"\u0001\u0010\u0001\u0010\u0001\u0010\u0001\u0011\u0001\u0011\u0001\u0011"+
		"\u0001\u0011\u0001\u0011\u0001\u0011\u0001\u0011\u0001\u0011\u0001\u0011"+
		"\u0001\u0011\u0001\u0011\u0001\u0011\u0001\u0011\u0003\u0011\u00f2\b\u0011"+
		"\u0001\u0012\u0001\u0012\u0001\u0013\u0001\u0013\u0001\u0014\u0001\u0014"+
		"\u0001\u0014\u0000\u0000\u0015\u0000\u0002\u0004\u0006\b\n\f\u000e\u0010"+
		"\u0012\u0014\u0016\u0018\u001a\u001c\u001e \"$&(\u0000\u0006\u0001\u0000"+
		")*\u0001\u0000%\'\u0002\u0000\u0006\u000622\u0001\u000023\u0001\u0000"+
		"./\u0003\u0000!#1133\u0107\u0000/\u0001\u0000\u0000\u0000\u00026\u0001"+
		"\u0000\u0000\u0000\u0004C\u0001\u0000\u0000\u0000\u0006W\u0001\u0000\u0000"+
		"\u0000\b\u0083\u0001\u0000\u0000\u0000\n\u0085\u0001\u0000\u0000\u0000"+
		"\f\u0096\u0001\u0000\u0000\u0000\u000e\u00a8\u0001\u0000\u0000\u0000\u0010"+
		"\u00ac\u0001\u0000\u0000\u0000\u0012\u00b4\u0001\u0000\u0000\u0000\u0014"+
		"\u00bc\u0001\u0000\u0000\u0000\u0016\u00c2\u0001\u0000\u0000\u0000\u0018"+
		"\u00c4\u0001\u0000\u0000\u0000\u001a\u00cf\u0001\u0000\u0000\u0000\u001c"+
		"\u00d3\u0001\u0000\u0000\u0000\u001e\u00d5\u0001\u0000\u0000\u0000 \u00e0"+
		"\u0001\u0000\u0000\u0000\"\u00f1\u0001\u0000\u0000\u0000$\u00f3\u0001"+
		"\u0000\u0000\u0000&\u00f5\u0001\u0000\u0000\u0000(\u00f7\u0001\u0000\u0000"+
		"\u0000*0\u0003\u0002\u0001\u0000+0\u0003\u0004\u0002\u0000,0\u0003\n\u0005"+
		"\u0000-0\u0003\f\u0006\u0000.0\u0003\u000e\u0007\u0000/*\u0001\u0000\u0000"+
		"\u0000/+\u0001\u0000\u0000\u0000/,\u0001\u0000\u0000\u0000/-\u0001\u0000"+
		"\u0000\u0000/.\u0001\u0000\u0000\u000002\u0001\u0000\u0000\u000013\u0005"+
		"\u0001\u0000\u000021\u0001\u0000\u0000\u000023\u0001\u0000\u0000\u0000"+
		"34\u0001\u0000\u0000\u000045\u0005\u0000\u0000\u00015\u0001\u0001\u0000"+
		"\u0000\u000067\u0005\u0007\u0000\u00007:\u0003\u0016\u000b\u000089\u0005"+
		"\u001f\u0000\u00009;\u0003\"\u0011\u0000:8\u0001\u0000\u0000\u0000:;\u0001"+
		"\u0000\u0000\u0000;>\u0001\u0000\u0000\u0000<=\u0005\u000f\u0000\u0000"+
		"=?\u0003\u0018\f\u0000><\u0001\u0000\u0000\u0000>?\u0001\u0000\u0000\u0000"+
		"?@\u0001\u0000\u0000\u0000@A\u0005\u000b\u0000\u0000AB\u0003\u001e\u000f"+
		"\u0000B\u0003\u0001\u0000\u0000\u0000CD\u0005\b\u0000\u0000DE\u0003\u0016"+
		"\u000b\u0000EH\u0003\u0006\u0003\u0000FG\u0005\u000f\u0000\u0000GI\u0003"+
		"\u0018\f\u0000HF\u0001\u0000\u0000\u0000HI\u0001\u0000\u0000\u0000IK\u0001"+
		"\u0000\u0000\u0000JL\u0003\b\u0004\u0000KJ\u0001\u0000\u0000\u0000KL\u0001"+
		"\u0000\u0000\u0000L\u0005\u0001\u0000\u0000\u0000MN\u0005\f\u0000\u0000"+
		"NO\u0003\"\u0011\u0000OP\u0005\r\u0000\u0000PQ\u0003\"\u0011\u0000QX\u0001"+
		"\u0000\u0000\u0000RS\u0005\f\u0000\u0000ST\u0005-\u0000\u0000TU\u0005"+
		"\u0002\u0000\u0000UV\u00050\u0000\u0000VX\u0005\u0003\u0000\u0000WM\u0001"+
		"\u0000\u0000\u0000WR\u0001\u0000\u0000\u0000X\u0007\u0001\u0000\u0000"+
		"\u0000Y\\\u0005\u0010\u0000\u0000Z[\u0005\u0011\u0000\u0000[]\u0003$\u0012"+
		"\u0000\\Z\u0001\u0000\u0000\u0000\\]\u0001\u0000\u0000\u0000]^\u0001\u0000"+
		"\u0000\u0000^_\u0005\u0002\u0000\u0000_`\u0003\u0010\b\u0000`d\u0005\u0003"+
		"\u0000\u0000ab\u0005\u001d\u0000\u0000bc\u0005\u0016\u0000\u0000ce\u0005"+
		"\u0017\u0000\u0000da\u0001\u0000\u0000\u0000de\u0001\u0000\u0000\u0000"+
		"eh\u0001\u0000\u0000\u0000fg\u0005\u0013\u0000\u0000gi\u00051\u0000\u0000"+
		"hf\u0001\u0000\u0000\u0000hi\u0001\u0000\u0000\u0000il\u0001\u0000\u0000"+
		"\u0000jk\u0005\u0015\u0000\u0000km\u00053\u0000\u0000lj\u0001\u0000\u0000"+
		"\u0000lm\u0001\u0000\u0000\u0000m\u0084\u0001\u0000\u0000\u0000np\u0005"+
		"(\u0000\u0000oq\u0007\u0000\u0000\u0000po\u0001\u0000\u0000\u0000pq\u0001"+
		"\u0000\u0000\u0000qt\u0001\u0000\u0000\u0000rs\u0005\u0013\u0000\u0000"+
		"su\u00051\u0000\u0000tr\u0001\u0000\u0000\u0000tu\u0001\u0000\u0000\u0000"+
		"ux\u0001\u0000\u0000\u0000vw\u0005\u0015\u0000\u0000wy\u00053\u0000\u0000"+
		"xv\u0001\u0000\u0000\u0000xy\u0001\u0000\u0000\u0000y\u0084\u0001\u0000"+
		"\u0000\u0000z{\u0005\u0013\u0000\u0000{|\u00051\u0000\u0000|\u007f\u0001"+
		"\u0000\u0000\u0000}~\u0005\u0015\u0000\u0000~\u0080\u00053\u0000\u0000"+
		"\u007f}\u0001\u0000\u0000\u0000\u007f\u0080\u0001\u0000\u0000\u0000\u0080"+
		"\u0084\u0001\u0000\u0000\u0000\u0081\u0082\u0005\u0015\u0000\u0000\u0082"+
		"\u0084\u00053\u0000\u0000\u0083Y\u0001\u0000\u0000\u0000\u0083n\u0001"+
		"\u0000\u0000\u0000\u0083z\u0001\u0000\u0000\u0000\u0083\u0081\u0001\u0000"+
		"\u0000\u0000\u0084\t\u0001\u0000\u0000\u0000\u0085\u0094\u0005\t\u0000"+
		"\u0000\u0086\u0095\u0003\u0014\n\u0000\u0087\u0088\u0005\f\u0000\u0000"+
		"\u0088\u0089\u0003\u0016\u000b\u0000\u0089\u008a\u0005\u000f\u0000\u0000"+
		"\u008a\u0092\u0003\u0018\f\u0000\u008b\u008c\u0005\u000e\u0000\u0000\u008c"+
		"\u0093\u0003\"\u0011\u0000\u008d\u008e\u0005\f\u0000\u0000\u008e\u008f"+
		"\u0003\"\u0011\u0000\u008f\u0090\u0005\r\u0000\u0000\u0090\u0091\u0003"+
		"\"\u0011\u0000\u0091\u0093\u0001\u0000\u0000\u0000\u0092\u008b\u0001\u0000"+
		"\u0000\u0000\u0092\u008d\u0001\u0000\u0000\u0000\u0093\u0095\u0001\u0000"+
		"\u0000\u0000\u0094\u0086\u0001\u0000\u0000\u0000\u0094\u0087\u0001\u0000"+
		"\u0000\u0000\u0095\u000b\u0001\u0000\u0000\u0000\u0096\u00a6\u0005\n\u0000"+
		"\u0000\u0097\u00a7\u0005\u0018\u0000\u0000\u0098\u0099\u0005\u001a\u0000"+
		"\u0000\u0099\u009a\u0005\u001b\u0000\u0000\u009a\u009b\u0005\f\u0000\u0000"+
		"\u009b\u00a7\u0003\u0016\u000b\u0000\u009c\u009d\u0005\u001a\u0000\u0000"+
		"\u009d\u00a0\u0005\u001c\u0000\u0000\u009e\u009f\u0005\f\u0000\u0000\u009f"+
		"\u00a1\u0003\u0016\u000b\u0000\u00a0\u009e\u0001\u0000\u0000\u0000\u00a0"+
		"\u00a1\u0001\u0000\u0000\u0000\u00a1\u00a2\u0001\u0000\u0000\u0000\u00a2"+
		"\u00a3\u0005\u001d\u0000\u0000\u00a3\u00a4\u0005\u001e\u0000\u0000\u00a4"+
		"\u00a5\u0005\u0004\u0000\u0000\u00a5\u00a7\u0003\u001c\u000e\u0000\u00a6"+
		"\u0097\u0001\u0000\u0000\u0000\u00a6\u0098\u0001\u0000\u0000\u0000\u00a6"+
		"\u009c\u0001\u0000\u0000\u0000\u00a7\r\u0001\u0000\u0000\u0000\u00a8\u00aa"+
		"\u0005$\u0000\u0000\u00a9\u00ab\u0007\u0001\u0000\u0000\u00aa\u00a9\u0001"+
		"\u0000\u0000\u0000\u00aa\u00ab\u0001\u0000\u0000\u0000\u00ab\u000f\u0001"+
		"\u0000\u0000\u0000\u00ac\u00b1\u0003\u0012\t\u0000\u00ad\u00ae\u0005\u0005"+
		"\u0000\u0000\u00ae\u00b0\u0003\u0012\t\u0000\u00af\u00ad\u0001\u0000\u0000"+
		"\u0000\u00b0\u00b3\u0001\u0000\u0000\u0000\u00b1\u00af\u0001\u0000\u0000"+
		"\u0000\u00b1\u00b2\u0001\u0000\u0000\u0000\u00b2\u0011\u0001\u0000\u0000"+
		"\u0000\u00b3\u00b1\u0001\u0000\u0000\u0000\u00b4\u00b5\u00052\u0000\u0000"+
		"\u00b5\u00b6\u0005\u0002\u0000\u0000\u00b6\u00b7\u0007\u0002\u0000\u0000"+
		"\u00b7\u00ba\u0005\u0003\u0000\u0000\u00b8\u00b9\u0005+\u0000\u0000\u00b9"+
		"\u00bb\u00052\u0000\u0000\u00ba\u00b8\u0001\u0000\u0000\u0000\u00ba\u00bb"+
		"\u0001\u0000\u0000\u0000\u00bb\u0013\u0001\u0000\u0000\u0000\u00bc\u00bd"+
		"\u0005\u0014\u0000\u0000\u00bd\u00c0\u0003\u0016\u000b\u0000\u00be\u00bf"+
		"\u0005\u000f\u0000\u0000\u00bf\u00c1\u0003\u0018\f\u0000\u00c0\u00be\u0001"+
		"\u0000\u0000\u0000\u00c0\u00c1\u0001\u0000\u0000\u0000\u00c1\u0015\u0001"+
		"\u0000\u0000\u0000\u00c2\u00c3\u0007\u0003\u0000\u0000\u00c3\u0017\u0001"+
		"\u0000\u0000\u0000\u00c4\u00c5\u0005\u0002\u0000\u0000\u00c5\u00ca\u0003"+
		"\u001a\r\u0000\u00c6\u00c7\u0005\u0005\u0000\u0000\u00c7\u00c9\u0003\u001a"+
		"\r\u0000\u00c8\u00c6\u0001\u0000\u0000\u0000\u00c9\u00cc\u0001\u0000\u0000"+
		"\u0000\u00ca\u00c8\u0001\u0000\u0000\u0000\u00ca\u00cb\u0001\u0000\u0000"+
		"\u0000\u00cb\u00cd\u0001\u0000\u0000\u0000\u00cc\u00ca\u0001\u0000\u0000"+
		"\u0000\u00cd\u00ce\u0005\u0003\u0000\u0000\u00ce\u0019\u0001\u0000\u0000"+
		"\u0000\u00cf\u00d0\u0007\u0003\u0000\u0000\u00d0\u00d1\u0005\u0004\u0000"+
		"\u0000\u00d1\u00d2\u0003\u001c\u000e\u0000\u00d2\u001b\u0001\u0000\u0000"+
		"\u0000\u00d3\u00d4\u00053\u0000\u0000\u00d4\u001d\u0001\u0000\u0000\u0000"+
		"\u00d5\u00d6\u0005\u0002\u0000\u0000\u00d6\u00db\u0003 \u0010\u0000\u00d7"+
		"\u00d8\u0005\u0005\u0000\u0000\u00d8\u00da\u0003 \u0010\u0000\u00d9\u00d7"+
		"\u0001\u0000\u0000\u0000\u00da\u00dd\u0001\u0000\u0000\u0000\u00db\u00d9"+
		"\u0001\u0000\u0000\u0000\u00db\u00dc\u0001\u0000\u0000\u0000\u00dc\u00de"+
		"\u0001\u0000\u0000\u0000\u00dd\u00db\u0001\u0000\u0000\u0000\u00de\u00df"+
		"\u0005\u0003\u0000\u0000\u00df\u001f\u0001\u0000\u0000\u0000\u00e0\u00e1"+
		"\u00052\u0000\u0000\u00e1\u00e2\u0005\u0004\u0000\u0000\u00e2\u00e3\u0003"+
		"(\u0014\u0000\u00e3!\u0001\u0000\u0000\u0000\u00e4\u00f2\u00051\u0000"+
		"\u0000\u00e5\u00e6\u0005 \u0000\u0000\u00e6\u00e7\u0005\u0002\u0000\u0000"+
		"\u00e7\u00f2\u0005\u0003\u0000\u0000\u00e8\u00e9\u0005 \u0000\u0000\u00e9"+
		"\u00ea\u0005\u0002\u0000\u0000\u00ea\u00eb\u0007\u0004\u0000\u0000\u00eb"+
		"\u00ec\u00050\u0000\u0000\u00ec\u00f2\u0005\u0003\u0000\u0000\u00ed\u00ee"+
		"\u0005,\u0000\u0000\u00ee\u00ef\u0005\u0002\u0000\u0000\u00ef\u00f0\u0005"+
		"3\u0000\u0000\u00f0\u00f2\u0005\u0003\u0000\u0000\u00f1\u00e4\u0001\u0000"+
		"\u0000\u0000\u00f1\u00e5\u0001\u0000\u0000\u0000\u00f1\u00e8\u0001\u0000"+
		"\u0000\u0000\u00f1\u00ed\u0001\u0000\u0000\u0000\u00f2#\u0001\u0000\u0000"+
		"\u0000\u00f3\u00f4\u00050\u0000\u0000\u00f4%\u0001\u0000\u0000\u0000\u00f5"+
		"\u00f6\u00051\u0000\u0000\u00f6\'\u0001\u0000\u0000\u0000\u00f7\u00f8"+
		"\u0007\u0005\u0000\u0000\u00f8)\u0001\u0000\u0000\u0000\u001b/2:>HKW\\"+
		"dhlptx\u007f\u0083\u0092\u0094\u00a0\u00a6\u00aa\u00b1\u00ba\u00c0\u00ca"+
		"\u00db\u00f1";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}