// Generated from d:/go/nexusbase/api/nbql/Nexus.g4 by ANTLR 4.13.1
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
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, K_OVERWRITE=7, K_RESTORE=8, 
		K_SNAPSHOT=9, K_PUSH=10, K_QUERY=11, K_REMOVE=12, K_SHOW=13, K_SET=14, 
		K_FROM=15, K_TO=16, K_AT=17, K_TAGGED=18, K_AGGREGATE=19, K_BY=20, K_ON=21, 
		K_LIMIT=22, K_SERIES=23, K_AFTER=24, K_EMPTY=25, K_WINDOWS=26, K_METRICS=27, 
		K_TAGS=28, K_TAG=29, K_KEYS=30, K_VALUES=31, K_WITH=32, K_KEY=33, K_TIME=34, 
		K_NOW=35, K_TRUE=36, K_FALSE=37, K_NULL=38, K_FLUSH=39, K_MEMTABLE=40, 
		K_DISK=41, K_ALL=42, K_ORDER=43, K_ASC=44, K_DESC=45, K_AS=46, K_DT=47, 
		K_RELATIVE=48, PLUS=49, MINUS=50, DURATION_LITERAL=51, NUMBER=52, IDENTIFIER=53, 
		STRING_LITERAL=54, WS=55, LINE_COMMENT=56;
	public static final int
		RULE_statement = 0, RULE_snapshotStatement = 1, RULE_restoreStatement = 2, 
		RULE_pushStatement = 3, RULE_queryStatement = 4, RULE_time_range = 5, 
		RULE_query_clauses = 6, RULE_removeStatement = 7, RULE_showStatement = 8, 
		RULE_flushStatement = 9, RULE_aggregation_spec_list = 10, RULE_aggregation_spec = 11, 
		RULE_series_specifier = 12, RULE_metric_name = 13, RULE_tag_list = 14, 
		RULE_tag_assignment = 15, RULE_tag_value = 16, RULE_field_list = 17, RULE_field_assignment = 18, 
		RULE_timestamp = 19, RULE_duration = 20, RULE_value = 21, RULE_literal_value = 22;
	private static String[] makeRuleNames() {
		return new String[] {
			"statement", "snapshotStatement", "restoreStatement", "pushStatement", 
			"queryStatement", "time_range", "query_clauses", "removeStatement", "showStatement", 
			"flushStatement", "aggregation_spec_list", "aggregation_spec", "series_specifier", 
			"metric_name", "tag_list", "tag_assignment", "tag_value", "field_list", 
			"field_assignment", "timestamp", "duration", "value", "literal_value"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "';'", "'('", "')'", "'='", "','", "'*'", null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, "'+'", "'-'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, "K_OVERWRITE", "K_RESTORE", 
			"K_SNAPSHOT", "K_PUSH", "K_QUERY", "K_REMOVE", "K_SHOW", "K_SET", "K_FROM", 
			"K_TO", "K_AT", "K_TAGGED", "K_AGGREGATE", "K_BY", "K_ON", "K_LIMIT", 
			"K_SERIES", "K_AFTER", "K_EMPTY", "K_WINDOWS", "K_METRICS", "K_TAGS", 
			"K_TAG", "K_KEYS", "K_VALUES", "K_WITH", "K_KEY", "K_TIME", "K_NOW", 
			"K_TRUE", "K_FALSE", "K_NULL", "K_FLUSH", "K_MEMTABLE", "K_DISK", "K_ALL", 
			"K_ORDER", "K_ASC", "K_DESC", "K_AS", "K_DT", "K_RELATIVE", "PLUS", "MINUS", 
			"DURATION_LITERAL", "NUMBER", "IDENTIFIER", "STRING_LITERAL", "WS", "LINE_COMMENT"
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
		public SnapshotStatementContext snapshotStatement() {
			return getRuleContext(SnapshotStatementContext.class,0);
		}
		public RestoreStatementContext restoreStatement() {
			return getRuleContext(RestoreStatementContext.class,0);
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
			setState(53);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case K_PUSH:
				{
				setState(46);
				pushStatement();
				}
				break;
			case K_QUERY:
				{
				setState(47);
				queryStatement();
				}
				break;
			case K_REMOVE:
				{
				setState(48);
				removeStatement();
				}
				break;
			case K_SHOW:
				{
				setState(49);
				showStatement();
				}
				break;
			case K_FLUSH:
				{
				setState(50);
				flushStatement();
				}
				break;
			case K_SNAPSHOT:
				{
				setState(51);
				snapshotStatement();
				}
				break;
			case K_RESTORE:
				{
				setState(52);
				restoreStatement();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(56);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__0) {
				{
				setState(55);
				match(T__0);
				}
			}

			setState(58);
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
	public static class SnapshotStatementContext extends ParserRuleContext {
		public TerminalNode K_SNAPSHOT() { return getToken(NexusParser.K_SNAPSHOT, 0); }
		public SnapshotStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_snapshotStatement; }
	}

	public final SnapshotStatementContext snapshotStatement() throws RecognitionException {
		SnapshotStatementContext _localctx = new SnapshotStatementContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_snapshotStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(60);
			match(K_SNAPSHOT);
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
	public static class RestoreStatementContext extends ParserRuleContext {
		public TerminalNode K_RESTORE() { return getToken(NexusParser.K_RESTORE, 0); }
		public TerminalNode K_FROM() { return getToken(NexusParser.K_FROM, 0); }
		public TerminalNode STRING_LITERAL() { return getToken(NexusParser.STRING_LITERAL, 0); }
		public TerminalNode K_WITH() { return getToken(NexusParser.K_WITH, 0); }
		public TerminalNode K_OVERWRITE() { return getToken(NexusParser.K_OVERWRITE, 0); }
		public RestoreStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_restoreStatement; }
	}

	public final RestoreStatementContext restoreStatement() throws RecognitionException {
		RestoreStatementContext _localctx = new RestoreStatementContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_restoreStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(62);
			match(K_RESTORE);
			setState(63);
			match(K_FROM);
			setState(64);
			match(STRING_LITERAL);
			setState(67);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_WITH) {
				{
				setState(65);
				match(K_WITH);
				setState(66);
				match(K_OVERWRITE);
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
		enterRule(_localctx, 6, RULE_pushStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(69);
			match(K_PUSH);
			setState(70);
			metric_name();
			setState(73);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_TIME) {
				{
				setState(71);
				match(K_TIME);
				setState(72);
				timestamp();
				}
			}

			setState(77);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_TAGGED) {
				{
				setState(75);
				match(K_TAGGED);
				setState(76);
				tag_list();
				}
			}

			setState(79);
			match(K_SET);
			setState(80);
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
		enterRule(_localctx, 8, RULE_queryStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(82);
			match(K_QUERY);
			setState(83);
			metric_name();
			setState(84);
			time_range();
			setState(87);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_TAGGED) {
				{
				setState(85);
				match(K_TAGGED);
				setState(86);
				tag_list();
				}
			}

			setState(90);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 8796114518016L) != 0)) {
				{
				setState(89);
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
		enterRule(_localctx, 10, RULE_time_range);
		try {
			setState(102);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(92);
				match(K_FROM);
				setState(93);
				timestamp();
				setState(94);
				match(K_TO);
				setState(95);
				timestamp();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(97);
				match(K_FROM);
				setState(98);
				match(K_RELATIVE);
				setState(99);
				match(T__1);
				setState(100);
				match(DURATION_LITERAL);
				setState(101);
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
		enterRule(_localctx, 12, RULE_query_clauses);
		int _la;
		try {
			setState(146);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case K_AGGREGATE:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(104);
				match(K_AGGREGATE);
				setState(107);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_BY) {
					{
					setState(105);
					match(K_BY);
					setState(106);
					duration();
					}
				}

				setState(109);
				match(T__1);
				setState(110);
				aggregation_spec_list();
				setState(111);
				match(T__2);
				setState(115);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_WITH) {
					{
					setState(112);
					match(K_WITH);
					setState(113);
					match(K_EMPTY);
					setState(114);
					match(K_WINDOWS);
					}
				}

				}
				setState(119);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_LIMIT) {
					{
					setState(117);
					match(K_LIMIT);
					setState(118);
					match(NUMBER);
					}
				}

				setState(123);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_AFTER) {
					{
					setState(121);
					match(K_AFTER);
					setState(122);
					match(STRING_LITERAL);
					}
				}

				}
				break;
			case K_ORDER:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(125);
				match(K_ORDER);
				setState(127);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_ASC || _la==K_DESC) {
					{
					setState(126);
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
				setState(131);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_LIMIT) {
					{
					setState(129);
					match(K_LIMIT);
					setState(130);
					match(NUMBER);
					}
				}

				setState(135);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_AFTER) {
					{
					setState(133);
					match(K_AFTER);
					setState(134);
					match(STRING_LITERAL);
					}
				}

				}
				break;
			case K_LIMIT:
				enterOuterAlt(_localctx, 3);
				{
				{
				setState(137);
				match(K_LIMIT);
				setState(138);
				match(NUMBER);
				}
				setState(142);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_AFTER) {
					{
					setState(140);
					match(K_AFTER);
					setState(141);
					match(STRING_LITERAL);
					}
				}

				}
				break;
			case K_AFTER:
				enterOuterAlt(_localctx, 4);
				{
				{
				setState(144);
				match(K_AFTER);
				setState(145);
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
		enterRule(_localctx, 14, RULE_removeStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(148);
			match(K_REMOVE);
			setState(163);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case K_SERIES:
				{
				setState(149);
				series_specifier();
				}
				break;
			case K_FROM:
				{
				setState(150);
				match(K_FROM);
				setState(151);
				metric_name();
				setState(152);
				match(K_TAGGED);
				setState(153);
				tag_list();
				setState(161);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case K_AT:
					{
					setState(154);
					match(K_AT);
					setState(155);
					timestamp();
					}
					break;
				case K_FROM:
					{
					setState(156);
					match(K_FROM);
					setState(157);
					timestamp();
					setState(158);
					match(K_TO);
					setState(159);
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
		enterRule(_localctx, 16, RULE_showStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(165);
			match(K_SHOW);
			setState(181);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,20,_ctx) ) {
			case 1:
				{
				setState(166);
				match(K_METRICS);
				}
				break;
			case 2:
				{
				setState(167);
				match(K_TAG);
				setState(168);
				match(K_KEYS);
				setState(169);
				match(K_FROM);
				setState(170);
				metric_name();
				}
				break;
			case 3:
				{
				setState(171);
				match(K_TAG);
				setState(172);
				match(K_VALUES);
				setState(175);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_FROM) {
					{
					setState(173);
					match(K_FROM);
					setState(174);
					metric_name();
					}
				}

				setState(177);
				match(K_WITH);
				setState(178);
				match(K_KEY);
				setState(179);
				match(T__3);
				setState(180);
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
		enterRule(_localctx, 18, RULE_flushStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(183);
			match(K_FLUSH);
			setState(185);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 7696581394432L) != 0)) {
				{
				setState(184);
				_la = _input.LA(1);
				if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 7696581394432L) != 0)) ) {
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
		enterRule(_localctx, 20, RULE_aggregation_spec_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(187);
			aggregation_spec();
			setState(192);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__4) {
				{
				{
				setState(188);
				match(T__4);
				setState(189);
				aggregation_spec();
				}
				}
				setState(194);
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
		enterRule(_localctx, 22, RULE_aggregation_spec);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(195);
			match(IDENTIFIER);
			setState(196);
			match(T__1);
			setState(197);
			_la = _input.LA(1);
			if ( !(_la==T__5 || _la==IDENTIFIER) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(198);
			match(T__2);
			setState(201);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_AS) {
				{
				setState(199);
				match(K_AS);
				setState(200);
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
		enterRule(_localctx, 24, RULE_series_specifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(203);
			match(K_SERIES);
			setState(204);
			metric_name();
			setState(207);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_TAGGED) {
				{
				setState(205);
				match(K_TAGGED);
				setState(206);
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
		enterRule(_localctx, 26, RULE_metric_name);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(209);
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
		enterRule(_localctx, 28, RULE_tag_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(211);
			match(T__1);
			setState(212);
			tag_assignment();
			setState(217);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__4) {
				{
				{
				setState(213);
				match(T__4);
				setState(214);
				tag_assignment();
				}
				}
				setState(219);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(220);
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
		enterRule(_localctx, 30, RULE_tag_assignment);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(222);
			_la = _input.LA(1);
			if ( !(_la==IDENTIFIER || _la==STRING_LITERAL) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(223);
			match(T__3);
			setState(224);
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
		enterRule(_localctx, 32, RULE_tag_value);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(226);
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
		enterRule(_localctx, 34, RULE_field_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(228);
			match(T__1);
			setState(229);
			field_assignment();
			setState(234);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__4) {
				{
				{
				setState(230);
				match(T__4);
				setState(231);
				field_assignment();
				}
				}
				setState(236);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(237);
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
		enterRule(_localctx, 36, RULE_field_assignment);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(239);
			match(IDENTIFIER);
			setState(240);
			match(T__3);
			setState(241);
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
		enterRule(_localctx, 38, RULE_timestamp);
		int _la;
		try {
			setState(256);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,27,_ctx) ) {
			case 1:
				_localctx = new TimestampLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(243);
				match(NUMBER);
				}
				break;
			case 2:
				_localctx = new TimestampNowContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(244);
				match(K_NOW);
				setState(245);
				match(T__1);
				setState(246);
				match(T__2);
				}
				break;
			case 3:
				_localctx = new TimestampNowRelativeContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(247);
				match(K_NOW);
				setState(248);
				match(T__1);
				setState(249);
				_la = _input.LA(1);
				if ( !(_la==PLUS || _la==MINUS) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(250);
				match(DURATION_LITERAL);
				setState(251);
				match(T__2);
				}
				break;
			case 4:
				_localctx = new TimestampDateTimeContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(252);
				match(K_DT);
				setState(253);
				match(T__1);
				setState(254);
				match(STRING_LITERAL);
				setState(255);
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
		enterRule(_localctx, 40, RULE_duration);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(258);
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
		enterRule(_localctx, 42, RULE_value);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(260);
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
		enterRule(_localctx, 44, RULE_literal_value);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(262);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 22518479173189632L) != 0)) ) {
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
		"\u0004\u00018\u0109\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
		"\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004\u0002"+
		"\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007\u0002"+
		"\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b\u0002"+
		"\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007\u000f"+
		"\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007\u0012"+
		"\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002\u0015\u0007\u0015"+
		"\u0002\u0016\u0007\u0016\u0001\u0000\u0001\u0000\u0001\u0000\u0001\u0000"+
		"\u0001\u0000\u0001\u0000\u0001\u0000\u0003\u00006\b\u0000\u0001\u0000"+
		"\u0003\u00009\b\u0000\u0001\u0000\u0001\u0000\u0001\u0001\u0001\u0001"+
		"\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0003\u0002"+
		"D\b\u0002\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003"+
		"J\b\u0003\u0001\u0003\u0001\u0003\u0003\u0003N\b\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001"+
		"\u0004\u0003\u0004X\b\u0004\u0001\u0004\u0003\u0004[\b\u0004\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005g\b\u0005\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0003\u0006l\b\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006t\b\u0006"+
		"\u0001\u0006\u0001\u0006\u0003\u0006x\b\u0006\u0001\u0006\u0001\u0006"+
		"\u0003\u0006|\b\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u0080\b\u0006"+
		"\u0001\u0006\u0001\u0006\u0003\u0006\u0084\b\u0006\u0001\u0006\u0001\u0006"+
		"\u0003\u0006\u0088\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0003\u0006\u008f\b\u0006\u0001\u0006\u0001\u0006\u0003\u0006"+
		"\u0093\b\u0006\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
		"\u0001\u0007\u0001\u0007\u0003\u0007\u00a2\b\u0007\u0003\u0007\u00a4\b"+
		"\u0007\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b"+
		"\u0001\b\u0001\b\u0003\b\u00b0\b\b\u0001\b\u0001\b\u0001\b\u0001\b\u0003"+
		"\b\u00b6\b\b\u0001\t\u0001\t\u0003\t\u00ba\b\t\u0001\n\u0001\n\u0001\n"+
		"\u0005\n\u00bf\b\n\n\n\f\n\u00c2\t\n\u0001\u000b\u0001\u000b\u0001\u000b"+
		"\u0001\u000b\u0001\u000b\u0001\u000b\u0003\u000b\u00ca\b\u000b\u0001\f"+
		"\u0001\f\u0001\f\u0001\f\u0003\f\u00d0\b\f\u0001\r\u0001\r\u0001\u000e"+
		"\u0001\u000e\u0001\u000e\u0001\u000e\u0005\u000e\u00d8\b\u000e\n\u000e"+
		"\f\u000e\u00db\t\u000e\u0001\u000e\u0001\u000e\u0001\u000f\u0001\u000f"+
		"\u0001\u000f\u0001\u000f\u0001\u0010\u0001\u0010\u0001\u0011\u0001\u0011"+
		"\u0001\u0011\u0001\u0011\u0005\u0011\u00e9\b\u0011\n\u0011\f\u0011\u00ec"+
		"\t\u0011\u0001\u0011\u0001\u0011\u0001\u0012\u0001\u0012\u0001\u0012\u0001"+
		"\u0012\u0001\u0013\u0001\u0013\u0001\u0013\u0001\u0013\u0001\u0013\u0001"+
		"\u0013\u0001\u0013\u0001\u0013\u0001\u0013\u0001\u0013\u0001\u0013\u0001"+
		"\u0013\u0001\u0013\u0003\u0013\u0101\b\u0013\u0001\u0014\u0001\u0014\u0001"+
		"\u0015\u0001\u0015\u0001\u0016\u0001\u0016\u0001\u0016\u0000\u0000\u0017"+
		"\u0000\u0002\u0004\u0006\b\n\f\u000e\u0010\u0012\u0014\u0016\u0018\u001a"+
		"\u001c\u001e \"$&(*,\u0000\u0006\u0001\u0000,-\u0001\u0000(*\u0002\u0000"+
		"\u0006\u000655\u0001\u000056\u0001\u000012\u0003\u0000$&4466\u0117\u0000"+
		"5\u0001\u0000\u0000\u0000\u0002<\u0001\u0000\u0000\u0000\u0004>\u0001"+
		"\u0000\u0000\u0000\u0006E\u0001\u0000\u0000\u0000\bR\u0001\u0000\u0000"+
		"\u0000\nf\u0001\u0000\u0000\u0000\f\u0092\u0001\u0000\u0000\u0000\u000e"+
		"\u0094\u0001\u0000\u0000\u0000\u0010\u00a5\u0001\u0000\u0000\u0000\u0012"+
		"\u00b7\u0001\u0000\u0000\u0000\u0014\u00bb\u0001\u0000\u0000\u0000\u0016"+
		"\u00c3\u0001\u0000\u0000\u0000\u0018\u00cb\u0001\u0000\u0000\u0000\u001a"+
		"\u00d1\u0001\u0000\u0000\u0000\u001c\u00d3\u0001\u0000\u0000\u0000\u001e"+
		"\u00de\u0001\u0000\u0000\u0000 \u00e2\u0001\u0000\u0000\u0000\"\u00e4"+
		"\u0001\u0000\u0000\u0000$\u00ef\u0001\u0000\u0000\u0000&\u0100\u0001\u0000"+
		"\u0000\u0000(\u0102\u0001\u0000\u0000\u0000*\u0104\u0001\u0000\u0000\u0000"+
		",\u0106\u0001\u0000\u0000\u0000.6\u0003\u0006\u0003\u0000/6\u0003\b\u0004"+
		"\u000006\u0003\u000e\u0007\u000016\u0003\u0010\b\u000026\u0003\u0012\t"+
		"\u000036\u0003\u0002\u0001\u000046\u0003\u0004\u0002\u00005.\u0001\u0000"+
		"\u0000\u00005/\u0001\u0000\u0000\u000050\u0001\u0000\u0000\u000051\u0001"+
		"\u0000\u0000\u000052\u0001\u0000\u0000\u000053\u0001\u0000\u0000\u0000"+
		"54\u0001\u0000\u0000\u000068\u0001\u0000\u0000\u000079\u0005\u0001\u0000"+
		"\u000087\u0001\u0000\u0000\u000089\u0001\u0000\u0000\u00009:\u0001\u0000"+
		"\u0000\u0000:;\u0005\u0000\u0000\u0001;\u0001\u0001\u0000\u0000\u0000"+
		"<=\u0005\t\u0000\u0000=\u0003\u0001\u0000\u0000\u0000>?\u0005\b\u0000"+
		"\u0000?@\u0005\u000f\u0000\u0000@C\u00056\u0000\u0000AB\u0005 \u0000\u0000"+
		"BD\u0005\u0007\u0000\u0000CA\u0001\u0000\u0000\u0000CD\u0001\u0000\u0000"+
		"\u0000D\u0005\u0001\u0000\u0000\u0000EF\u0005\n\u0000\u0000FI\u0003\u001a"+
		"\r\u0000GH\u0005\"\u0000\u0000HJ\u0003&\u0013\u0000IG\u0001\u0000\u0000"+
		"\u0000IJ\u0001\u0000\u0000\u0000JM\u0001\u0000\u0000\u0000KL\u0005\u0012"+
		"\u0000\u0000LN\u0003\u001c\u000e\u0000MK\u0001\u0000\u0000\u0000MN\u0001"+
		"\u0000\u0000\u0000NO\u0001\u0000\u0000\u0000OP\u0005\u000e\u0000\u0000"+
		"PQ\u0003\"\u0011\u0000Q\u0007\u0001\u0000\u0000\u0000RS\u0005\u000b\u0000"+
		"\u0000ST\u0003\u001a\r\u0000TW\u0003\n\u0005\u0000UV\u0005\u0012\u0000"+
		"\u0000VX\u0003\u001c\u000e\u0000WU\u0001\u0000\u0000\u0000WX\u0001\u0000"+
		"\u0000\u0000XZ\u0001\u0000\u0000\u0000Y[\u0003\f\u0006\u0000ZY\u0001\u0000"+
		"\u0000\u0000Z[\u0001\u0000\u0000\u0000[\t\u0001\u0000\u0000\u0000\\]\u0005"+
		"\u000f\u0000\u0000]^\u0003&\u0013\u0000^_\u0005\u0010\u0000\u0000_`\u0003"+
		"&\u0013\u0000`g\u0001\u0000\u0000\u0000ab\u0005\u000f\u0000\u0000bc\u0005"+
		"0\u0000\u0000cd\u0005\u0002\u0000\u0000de\u00053\u0000\u0000eg\u0005\u0003"+
		"\u0000\u0000f\\\u0001\u0000\u0000\u0000fa\u0001\u0000\u0000\u0000g\u000b"+
		"\u0001\u0000\u0000\u0000hk\u0005\u0013\u0000\u0000ij\u0005\u0014\u0000"+
		"\u0000jl\u0003(\u0014\u0000ki\u0001\u0000\u0000\u0000kl\u0001\u0000\u0000"+
		"\u0000lm\u0001\u0000\u0000\u0000mn\u0005\u0002\u0000\u0000no\u0003\u0014"+
		"\n\u0000os\u0005\u0003\u0000\u0000pq\u0005 \u0000\u0000qr\u0005\u0019"+
		"\u0000\u0000rt\u0005\u001a\u0000\u0000sp\u0001\u0000\u0000\u0000st\u0001"+
		"\u0000\u0000\u0000tw\u0001\u0000\u0000\u0000uv\u0005\u0016\u0000\u0000"+
		"vx\u00054\u0000\u0000wu\u0001\u0000\u0000\u0000wx\u0001\u0000\u0000\u0000"+
		"x{\u0001\u0000\u0000\u0000yz\u0005\u0018\u0000\u0000z|\u00056\u0000\u0000"+
		"{y\u0001\u0000\u0000\u0000{|\u0001\u0000\u0000\u0000|\u0093\u0001\u0000"+
		"\u0000\u0000}\u007f\u0005+\u0000\u0000~\u0080\u0007\u0000\u0000\u0000"+
		"\u007f~\u0001\u0000\u0000\u0000\u007f\u0080\u0001\u0000\u0000\u0000\u0080"+
		"\u0083\u0001\u0000\u0000\u0000\u0081\u0082\u0005\u0016\u0000\u0000\u0082"+
		"\u0084\u00054\u0000\u0000\u0083\u0081\u0001\u0000\u0000\u0000\u0083\u0084"+
		"\u0001\u0000\u0000\u0000\u0084\u0087\u0001\u0000\u0000\u0000\u0085\u0086"+
		"\u0005\u0018\u0000\u0000\u0086\u0088\u00056\u0000\u0000\u0087\u0085\u0001"+
		"\u0000\u0000\u0000\u0087\u0088\u0001\u0000\u0000\u0000\u0088\u0093\u0001"+
		"\u0000\u0000\u0000\u0089\u008a\u0005\u0016\u0000\u0000\u008a\u008b\u0005"+
		"4\u0000\u0000\u008b\u008e\u0001\u0000\u0000\u0000\u008c\u008d\u0005\u0018"+
		"\u0000\u0000\u008d\u008f\u00056\u0000\u0000\u008e\u008c\u0001\u0000\u0000"+
		"\u0000\u008e\u008f\u0001\u0000\u0000\u0000\u008f\u0093\u0001\u0000\u0000"+
		"\u0000\u0090\u0091\u0005\u0018\u0000\u0000\u0091\u0093\u00056\u0000\u0000"+
		"\u0092h\u0001\u0000\u0000\u0000\u0092}\u0001\u0000\u0000\u0000\u0092\u0089"+
		"\u0001\u0000\u0000\u0000\u0092\u0090\u0001\u0000\u0000\u0000\u0093\r\u0001"+
		"\u0000\u0000\u0000\u0094\u00a3\u0005\f\u0000\u0000\u0095\u00a4\u0003\u0018"+
		"\f\u0000\u0096\u0097\u0005\u000f\u0000\u0000\u0097\u0098\u0003\u001a\r"+
		"\u0000\u0098\u0099\u0005\u0012\u0000\u0000\u0099\u00a1\u0003\u001c\u000e"+
		"\u0000\u009a\u009b\u0005\u0011\u0000\u0000\u009b\u00a2\u0003&\u0013\u0000"+
		"\u009c\u009d\u0005\u000f\u0000\u0000\u009d\u009e\u0003&\u0013\u0000\u009e"+
		"\u009f\u0005\u0010\u0000\u0000\u009f\u00a0\u0003&\u0013\u0000\u00a0\u00a2"+
		"\u0001\u0000\u0000\u0000\u00a1\u009a\u0001\u0000\u0000\u0000\u00a1\u009c"+
		"\u0001\u0000\u0000\u0000\u00a2\u00a4\u0001\u0000\u0000\u0000\u00a3\u0095"+
		"\u0001\u0000\u0000\u0000\u00a3\u0096\u0001\u0000\u0000\u0000\u00a4\u000f"+
		"\u0001\u0000\u0000\u0000\u00a5\u00b5\u0005\r\u0000\u0000\u00a6\u00b6\u0005"+
		"\u001b\u0000\u0000\u00a7\u00a8\u0005\u001d\u0000\u0000\u00a8\u00a9\u0005"+
		"\u001e\u0000\u0000\u00a9\u00aa\u0005\u000f\u0000\u0000\u00aa\u00b6\u0003"+
		"\u001a\r\u0000\u00ab\u00ac\u0005\u001d\u0000\u0000\u00ac\u00af\u0005\u001f"+
		"\u0000\u0000\u00ad\u00ae\u0005\u000f\u0000\u0000\u00ae\u00b0\u0003\u001a"+
		"\r\u0000\u00af\u00ad\u0001\u0000\u0000\u0000\u00af\u00b0\u0001\u0000\u0000"+
		"\u0000\u00b0\u00b1\u0001\u0000\u0000\u0000\u00b1\u00b2\u0005 \u0000\u0000"+
		"\u00b2\u00b3\u0005!\u0000\u0000\u00b3\u00b4\u0005\u0004\u0000\u0000\u00b4"+
		"\u00b6\u0003 \u0010\u0000\u00b5\u00a6\u0001\u0000\u0000\u0000\u00b5\u00a7"+
		"\u0001\u0000\u0000\u0000\u00b5\u00ab\u0001\u0000\u0000\u0000\u00b6\u0011"+
		"\u0001\u0000\u0000\u0000\u00b7\u00b9\u0005\'\u0000\u0000\u00b8\u00ba\u0007"+
		"\u0001\u0000\u0000\u00b9\u00b8\u0001\u0000\u0000\u0000\u00b9\u00ba\u0001"+
		"\u0000\u0000\u0000\u00ba\u0013\u0001\u0000\u0000\u0000\u00bb\u00c0\u0003"+
		"\u0016\u000b\u0000\u00bc\u00bd\u0005\u0005\u0000\u0000\u00bd\u00bf\u0003"+
		"\u0016\u000b\u0000\u00be\u00bc\u0001\u0000\u0000\u0000\u00bf\u00c2\u0001"+
		"\u0000\u0000\u0000\u00c0\u00be\u0001\u0000\u0000\u0000\u00c0\u00c1\u0001"+
		"\u0000\u0000\u0000\u00c1\u0015\u0001\u0000\u0000\u0000\u00c2\u00c0\u0001"+
		"\u0000\u0000\u0000\u00c3\u00c4\u00055\u0000\u0000\u00c4\u00c5\u0005\u0002"+
		"\u0000\u0000\u00c5\u00c6\u0007\u0002\u0000\u0000\u00c6\u00c9\u0005\u0003"+
		"\u0000\u0000\u00c7\u00c8\u0005.\u0000\u0000\u00c8\u00ca\u00055\u0000\u0000"+
		"\u00c9\u00c7\u0001\u0000\u0000\u0000\u00c9\u00ca\u0001\u0000\u0000\u0000"+
		"\u00ca\u0017\u0001\u0000\u0000\u0000\u00cb\u00cc\u0005\u0017\u0000\u0000"+
		"\u00cc\u00cf\u0003\u001a\r\u0000\u00cd\u00ce\u0005\u0012\u0000\u0000\u00ce"+
		"\u00d0\u0003\u001c\u000e\u0000\u00cf\u00cd\u0001\u0000\u0000\u0000\u00cf"+
		"\u00d0\u0001\u0000\u0000\u0000\u00d0\u0019\u0001\u0000\u0000\u0000\u00d1"+
		"\u00d2\u0007\u0003\u0000\u0000\u00d2\u001b\u0001\u0000\u0000\u0000\u00d3"+
		"\u00d4\u0005\u0002\u0000\u0000\u00d4\u00d9\u0003\u001e\u000f\u0000\u00d5"+
		"\u00d6\u0005\u0005\u0000\u0000\u00d6\u00d8\u0003\u001e\u000f\u0000\u00d7"+
		"\u00d5\u0001\u0000\u0000\u0000\u00d8\u00db\u0001\u0000\u0000\u0000\u00d9"+
		"\u00d7\u0001\u0000\u0000\u0000\u00d9\u00da\u0001\u0000\u0000\u0000\u00da"+
		"\u00dc\u0001\u0000\u0000\u0000\u00db\u00d9\u0001\u0000\u0000\u0000\u00dc"+
		"\u00dd\u0005\u0003\u0000\u0000\u00dd\u001d\u0001\u0000\u0000\u0000\u00de"+
		"\u00df\u0007\u0003\u0000\u0000\u00df\u00e0\u0005\u0004\u0000\u0000\u00e0"+
		"\u00e1\u0003 \u0010\u0000\u00e1\u001f\u0001\u0000\u0000\u0000\u00e2\u00e3"+
		"\u00056\u0000\u0000\u00e3!\u0001\u0000\u0000\u0000\u00e4\u00e5\u0005\u0002"+
		"\u0000\u0000\u00e5\u00ea\u0003$\u0012\u0000\u00e6\u00e7\u0005\u0005\u0000"+
		"\u0000\u00e7\u00e9\u0003$\u0012\u0000\u00e8\u00e6\u0001\u0000\u0000\u0000"+
		"\u00e9\u00ec\u0001\u0000\u0000\u0000\u00ea\u00e8\u0001\u0000\u0000\u0000"+
		"\u00ea\u00eb\u0001\u0000\u0000\u0000\u00eb\u00ed\u0001\u0000\u0000\u0000"+
		"\u00ec\u00ea\u0001\u0000\u0000\u0000\u00ed\u00ee\u0005\u0003\u0000\u0000"+
		"\u00ee#\u0001\u0000\u0000\u0000\u00ef\u00f0\u00055\u0000\u0000\u00f0\u00f1"+
		"\u0005\u0004\u0000\u0000\u00f1\u00f2\u0003,\u0016\u0000\u00f2%\u0001\u0000"+
		"\u0000\u0000\u00f3\u0101\u00054\u0000\u0000\u00f4\u00f5\u0005#\u0000\u0000"+
		"\u00f5\u00f6\u0005\u0002\u0000\u0000\u00f6\u0101\u0005\u0003\u0000\u0000"+
		"\u00f7\u00f8\u0005#\u0000\u0000\u00f8\u00f9\u0005\u0002\u0000\u0000\u00f9"+
		"\u00fa\u0007\u0004\u0000\u0000\u00fa\u00fb\u00053\u0000\u0000\u00fb\u0101"+
		"\u0005\u0003\u0000\u0000\u00fc\u00fd\u0005/\u0000\u0000\u00fd\u00fe\u0005"+
		"\u0002\u0000\u0000\u00fe\u00ff\u00056\u0000\u0000\u00ff\u0101\u0005\u0003"+
		"\u0000\u0000\u0100\u00f3\u0001\u0000\u0000\u0000\u0100\u00f4\u0001\u0000"+
		"\u0000\u0000\u0100\u00f7\u0001\u0000\u0000\u0000\u0100\u00fc\u0001\u0000"+
		"\u0000\u0000\u0101\'\u0001\u0000\u0000\u0000\u0102\u0103\u00053\u0000"+
		"\u0000\u0103)\u0001\u0000\u0000\u0000\u0104\u0105\u00054\u0000\u0000\u0105"+
		"+\u0001\u0000\u0000\u0000\u0106\u0107\u0007\u0005\u0000\u0000\u0107-\u0001"+
		"\u0000\u0000\u0000\u001c58CIMWZfksw{\u007f\u0083\u0087\u008e\u0092\u00a1"+
		"\u00a3\u00af\u00b5\u00b9\u00c0\u00c9\u00cf\u00d9\u00ea\u0100";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}