grammar Nexus;

// --- Parser Rules ---

// Entry point: a single statement ending with EOF
statement: (pushStatement | queryStatement | removeStatement | showStatement | flushStatement | snapshotStatement | restoreStatement) ';'? EOF;

// --- Command Structures ---

snapshotStatement: K_SNAPSHOT;

restoreStatement: K_RESTORE K_FROM STRING_LITERAL (K_WITH K_OVERWRITE)?;

pushStatement: K_PUSH metric_name (K_TIME timestamp)? (K_TAGGED tag_list)? K_SET field_list;

queryStatement: K_QUERY metric_name time_range (K_TAGGED tag_list)? query_clauses?;

time_range:
    K_FROM timestamp K_TO timestamp
    | K_FROM K_RELATIVE '(' DURATION_LITERAL ')'
;

query_clauses:
    (K_AGGREGATE (K_BY duration)? '(' aggregation_spec_list ')' (K_WITH K_EMPTY K_WINDOWS)?) (K_LIMIT NUMBER)? (K_AFTER STRING_LITERAL)?
    | (K_ORDER (K_ASC | K_DESC)?) (K_LIMIT NUMBER)? (K_AFTER STRING_LITERAL)?
    | (K_LIMIT NUMBER) (K_AFTER STRING_LITERAL)?
    | (K_AFTER STRING_LITERAL)
;

removeStatement: K_REMOVE (
    series_specifier
    | K_FROM metric_name K_TAGGED tag_list (
        K_AT timestamp
        | K_FROM timestamp K_TO timestamp
      )
);

showStatement: K_SHOW (
    K_METRICS
    | K_TAG K_KEYS K_FROM metric_name
    | K_TAG K_VALUES (K_FROM metric_name)? K_WITH K_KEY '=' tag_value
);

flushStatement: K_FLUSH (K_MEMTABLE | K_DISK | K_ALL)?;

// --- Reusable Rule Fragments ---
aggregation_spec_list: aggregation_spec (',' aggregation_spec)*;
aggregation_spec: IDENTIFIER '(' (IDENTIFIER | '*') ')' (K_AS IDENTIFIER)?; // e.g., avg(latency) as avg_latency

series_specifier: K_SERIES metric_name (K_TAGGED tag_list)?;
metric_name: IDENTIFIER | STRING_LITERAL;
tag_list: '(' tag_assignment (',' tag_assignment)* ')';
tag_assignment: (IDENTIFIER | STRING_LITERAL) '=' tag_value;
tag_value: STRING_LITERAL;

field_list: '(' field_assignment (',' field_assignment)* ')';
field_assignment: IDENTIFIER '=' literal_value;

timestamp
    : NUMBER                                  # TimestampLiteral
    | K_NOW '(' ')'                           # TimestampNow
    | K_NOW '(' (PLUS | MINUS) DURATION_LITERAL ')'   # TimestampNowRelative
    | K_DT '(' STRING_LITERAL ')'             # TimestampDateTime
    ;

duration: DURATION_LITERAL;
value: NUMBER;

literal_value:
    NUMBER
    | STRING_LITERAL
    | K_TRUE
    | K_FALSE
    | K_NULL;

// --- Lexer Rules (Tokens) ---

K_OVERWRITE: O V E R W R I T E;
K_RESTORE: R E S T O R E;
K_SNAPSHOT: S N A P S H O T;
K_PUSH: P U S H;
K_QUERY: Q U E R Y;
K_REMOVE: R E M O V E;
K_SHOW: S H O W;
K_SET: S E T;
K_FROM: F R O M;
K_TO: T O;
K_AT: A T;
K_TAGGED: T A G G E D;
K_AGGREGATE: A G G R E G A T E;
K_BY: B Y;
K_ON: O N;
K_LIMIT: L I M I T;
K_SERIES: S E R I E S;
K_AFTER: A F T E R;
K_EMPTY: E M P T Y;
K_WINDOWS: W I N D O W S;
K_METRICS: M E T R I C S;
K_TAGS: T A G S; // Deprecated but kept for now, use K_TAG + K_KEYS/K_VALUES
K_TAG: T A G;
K_KEYS: K E Y S;
K_VALUES: V A L U E S;
K_WITH: W I T H;
K_KEY: K E Y;
K_TIME: T I M E;
K_NOW: N O W;
K_TRUE: T R U E;
K_FALSE: F A L S E;
K_NULL: N U L L;
K_FLUSH: F L U S H;
K_MEMTABLE: M E M T A B L E;
K_DISK: D I S K;
K_ALL: A L L;
K_ORDER: O R D E R;
K_ASC: A S C;
K_DESC: D E S C;
K_AS: A S;
K_DT: D T;

K_RELATIVE: R E L A T I V E;

PLUS: '+';
MINUS: '-';

DURATION_LITERAL: [0-9]+[smhdwy];
NUMBER: [0-9]+ ('.' [0-9]+)?;
IDENTIFIER: [a-zA-Z_] [a-zA-Z0-9_.:-]*;
STRING_LITERAL:
    '"' ( ~["\r\n] | '""' )* '"'
    | '\'' ( ~['\r\n] | '\'\'' )* '\''
;

WS: [ \t\r\n]+ -> skip;
LINE_COMMENT: '--' .*? '\n' -> skip;

// --- Case-Insensitive Keyword Fragments ---

fragment A: 'a' | 'A';
fragment B: 'b' | 'B';
fragment C: 'c' | 'C';
fragment D: 'd' | 'D';
fragment E: 'e' | 'E';
fragment F: 'f' | 'F';
fragment G: 'g' | 'G';
fragment H: 'h' | 'H';
fragment I: 'i' | 'I';
fragment J: 'j' | 'J';
fragment K: 'k' | 'K';
fragment L: 'l' | 'L';
fragment M: 'm' | 'M';
fragment N: 'n' | 'N';
fragment O: 'o' | 'O';
fragment P: 'p' | 'P';
fragment Q: 'q' | 'Q';
fragment R: 'r' | 'R';
fragment S: 's' | 'S';
fragment T: 't' | 'T';
fragment U: 'u' | 'U';
fragment V: 'v' | 'V';
fragment W: 'w' | 'W';
fragment X: 'x' | 'X';
fragment Y: 'y' | 'Y';
fragment Z: 'z' | 'Z';