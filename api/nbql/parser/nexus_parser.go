// Code generated from ./Nexus.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // Nexus
import (
	"fmt"
	"strconv"
	"sync"

	"github.com/antlr4-go/antlr/v4"
)

// Suppress unused import errors
var _ = fmt.Printf
var _ = strconv.Itoa
var _ = sync.Once{}

type NexusParser struct {
	*antlr.BaseParser
}

var NexusParserStaticData struct {
	once                   sync.Once
	serializedATN          []int32
	LiteralNames           []string
	SymbolicNames          []string
	RuleNames              []string
	PredictionContextCache *antlr.PredictionContextCache
	atn                    *antlr.ATN
	decisionToDFA          []*antlr.DFA
}

func nexusParserInit() {
	staticData := &NexusParserStaticData
	staticData.LiteralNames = []string{
		"", "';'", "'('", "')'", "'='", "','", "'*'", "", "", "", "", "", "",
		"", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
		"", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
		"", "", "'+'", "'-'",
	}
	staticData.SymbolicNames = []string{
		"", "", "", "", "", "", "", "K_OVERWRITE", "K_RESTORE", "K_SNAPSHOT",
		"K_PUSH", "K_QUERY", "K_REMOVE", "K_SHOW", "K_SET", "K_FROM", "K_TO",
		"K_AT", "K_TAGGED", "K_AGGREGATE", "K_BY", "K_ON", "K_LIMIT", "K_SERIES",
		"K_AFTER", "K_EMPTY", "K_WINDOWS", "K_METRICS", "K_TAGS", "K_TAG", "K_KEYS",
		"K_VALUES", "K_WITH", "K_KEY", "K_TIME", "K_NOW", "K_TRUE", "K_FALSE",
		"K_NULL", "K_FLUSH", "K_MEMTABLE", "K_DISK", "K_ALL", "K_ORDER", "K_ASC",
		"K_DESC", "K_AS", "K_DT", "K_RELATIVE", "PLUS", "MINUS", "DURATION_LITERAL",
		"NUMBER", "IDENTIFIER", "STRING_LITERAL", "WS", "LINE_COMMENT",
	}
	staticData.RuleNames = []string{
		"statement", "snapshotStatement", "restoreStatement", "pushStatement",
		"queryStatement", "time_range", "query_clauses", "removeStatement",
		"showStatement", "flushStatement", "aggregation_spec_list", "aggregation_spec",
		"series_specifier", "metric_name", "tag_list", "tag_assignment", "tag_value",
		"field_list", "field_assignment", "timestamp", "duration", "value",
		"literal_value",
	}
	staticData.PredictionContextCache = antlr.NewPredictionContextCache()
	staticData.serializedATN = []int32{
		4, 1, 56, 265, 2, 0, 7, 0, 2, 1, 7, 1, 2, 2, 7, 2, 2, 3, 7, 3, 2, 4, 7,
		4, 2, 5, 7, 5, 2, 6, 7, 6, 2, 7, 7, 7, 2, 8, 7, 8, 2, 9, 7, 9, 2, 10, 7,
		10, 2, 11, 7, 11, 2, 12, 7, 12, 2, 13, 7, 13, 2, 14, 7, 14, 2, 15, 7, 15,
		2, 16, 7, 16, 2, 17, 7, 17, 2, 18, 7, 18, 2, 19, 7, 19, 2, 20, 7, 20, 2,
		21, 7, 21, 2, 22, 7, 22, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 3, 0,
		54, 8, 0, 1, 0, 3, 0, 57, 8, 0, 1, 0, 1, 0, 1, 1, 1, 1, 1, 2, 1, 2, 1,
		2, 1, 2, 1, 2, 3, 2, 68, 8, 2, 1, 3, 1, 3, 1, 3, 1, 3, 3, 3, 74, 8, 3,
		1, 3, 1, 3, 3, 3, 78, 8, 3, 1, 3, 1, 3, 1, 3, 1, 4, 1, 4, 1, 4, 1, 4, 1,
		4, 3, 4, 88, 8, 4, 1, 4, 3, 4, 91, 8, 4, 1, 5, 1, 5, 1, 5, 1, 5, 1, 5,
		1, 5, 1, 5, 1, 5, 1, 5, 1, 5, 3, 5, 103, 8, 5, 1, 6, 1, 6, 1, 6, 3, 6,
		108, 8, 6, 1, 6, 1, 6, 1, 6, 1, 6, 1, 6, 1, 6, 3, 6, 116, 8, 6, 1, 6, 1,
		6, 3, 6, 120, 8, 6, 1, 6, 1, 6, 3, 6, 124, 8, 6, 1, 6, 1, 6, 3, 6, 128,
		8, 6, 1, 6, 1, 6, 3, 6, 132, 8, 6, 1, 6, 1, 6, 3, 6, 136, 8, 6, 1, 6, 1,
		6, 1, 6, 1, 6, 1, 6, 3, 6, 143, 8, 6, 1, 6, 1, 6, 3, 6, 147, 8, 6, 1, 7,
		1, 7, 1, 7, 1, 7, 1, 7, 1, 7, 1, 7, 1, 7, 1, 7, 1, 7, 1, 7, 1, 7, 1, 7,
		3, 7, 162, 8, 7, 3, 7, 164, 8, 7, 1, 8, 1, 8, 1, 8, 1, 8, 1, 8, 1, 8, 1,
		8, 1, 8, 1, 8, 1, 8, 3, 8, 176, 8, 8, 1, 8, 1, 8, 1, 8, 1, 8, 3, 8, 182,
		8, 8, 1, 9, 1, 9, 3, 9, 186, 8, 9, 1, 10, 1, 10, 1, 10, 5, 10, 191, 8,
		10, 10, 10, 12, 10, 194, 9, 10, 1, 11, 1, 11, 1, 11, 1, 11, 1, 11, 1, 11,
		3, 11, 202, 8, 11, 1, 12, 1, 12, 1, 12, 1, 12, 3, 12, 208, 8, 12, 1, 13,
		1, 13, 1, 14, 1, 14, 1, 14, 1, 14, 5, 14, 216, 8, 14, 10, 14, 12, 14, 219,
		9, 14, 1, 14, 1, 14, 1, 15, 1, 15, 1, 15, 1, 15, 1, 16, 1, 16, 1, 17, 1,
		17, 1, 17, 1, 17, 5, 17, 233, 8, 17, 10, 17, 12, 17, 236, 9, 17, 1, 17,
		1, 17, 1, 18, 1, 18, 1, 18, 1, 18, 1, 19, 1, 19, 1, 19, 1, 19, 1, 19, 1,
		19, 1, 19, 1, 19, 1, 19, 1, 19, 1, 19, 1, 19, 1, 19, 3, 19, 257, 8, 19,
		1, 20, 1, 20, 1, 21, 1, 21, 1, 22, 1, 22, 1, 22, 0, 0, 23, 0, 2, 4, 6,
		8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42,
		44, 0, 6, 1, 0, 44, 45, 1, 0, 40, 42, 2, 0, 6, 6, 53, 53, 1, 0, 53, 54,
		1, 0, 49, 50, 3, 0, 36, 38, 52, 52, 54, 54, 279, 0, 53, 1, 0, 0, 0, 2,
		60, 1, 0, 0, 0, 4, 62, 1, 0, 0, 0, 6, 69, 1, 0, 0, 0, 8, 82, 1, 0, 0, 0,
		10, 102, 1, 0, 0, 0, 12, 146, 1, 0, 0, 0, 14, 148, 1, 0, 0, 0, 16, 165,
		1, 0, 0, 0, 18, 183, 1, 0, 0, 0, 20, 187, 1, 0, 0, 0, 22, 195, 1, 0, 0,
		0, 24, 203, 1, 0, 0, 0, 26, 209, 1, 0, 0, 0, 28, 211, 1, 0, 0, 0, 30, 222,
		1, 0, 0, 0, 32, 226, 1, 0, 0, 0, 34, 228, 1, 0, 0, 0, 36, 239, 1, 0, 0,
		0, 38, 256, 1, 0, 0, 0, 40, 258, 1, 0, 0, 0, 42, 260, 1, 0, 0, 0, 44, 262,
		1, 0, 0, 0, 46, 54, 3, 6, 3, 0, 47, 54, 3, 8, 4, 0, 48, 54, 3, 14, 7, 0,
		49, 54, 3, 16, 8, 0, 50, 54, 3, 18, 9, 0, 51, 54, 3, 2, 1, 0, 52, 54, 3,
		4, 2, 0, 53, 46, 1, 0, 0, 0, 53, 47, 1, 0, 0, 0, 53, 48, 1, 0, 0, 0, 53,
		49, 1, 0, 0, 0, 53, 50, 1, 0, 0, 0, 53, 51, 1, 0, 0, 0, 53, 52, 1, 0, 0,
		0, 54, 56, 1, 0, 0, 0, 55, 57, 5, 1, 0, 0, 56, 55, 1, 0, 0, 0, 56, 57,
		1, 0, 0, 0, 57, 58, 1, 0, 0, 0, 58, 59, 5, 0, 0, 1, 59, 1, 1, 0, 0, 0,
		60, 61, 5, 9, 0, 0, 61, 3, 1, 0, 0, 0, 62, 63, 5, 8, 0, 0, 63, 64, 5, 15,
		0, 0, 64, 67, 5, 54, 0, 0, 65, 66, 5, 32, 0, 0, 66, 68, 5, 7, 0, 0, 67,
		65, 1, 0, 0, 0, 67, 68, 1, 0, 0, 0, 68, 5, 1, 0, 0, 0, 69, 70, 5, 10, 0,
		0, 70, 73, 3, 26, 13, 0, 71, 72, 5, 34, 0, 0, 72, 74, 3, 38, 19, 0, 73,
		71, 1, 0, 0, 0, 73, 74, 1, 0, 0, 0, 74, 77, 1, 0, 0, 0, 75, 76, 5, 18,
		0, 0, 76, 78, 3, 28, 14, 0, 77, 75, 1, 0, 0, 0, 77, 78, 1, 0, 0, 0, 78,
		79, 1, 0, 0, 0, 79, 80, 5, 14, 0, 0, 80, 81, 3, 34, 17, 0, 81, 7, 1, 0,
		0, 0, 82, 83, 5, 11, 0, 0, 83, 84, 3, 26, 13, 0, 84, 87, 3, 10, 5, 0, 85,
		86, 5, 18, 0, 0, 86, 88, 3, 28, 14, 0, 87, 85, 1, 0, 0, 0, 87, 88, 1, 0,
		0, 0, 88, 90, 1, 0, 0, 0, 89, 91, 3, 12, 6, 0, 90, 89, 1, 0, 0, 0, 90,
		91, 1, 0, 0, 0, 91, 9, 1, 0, 0, 0, 92, 93, 5, 15, 0, 0, 93, 94, 3, 38,
		19, 0, 94, 95, 5, 16, 0, 0, 95, 96, 3, 38, 19, 0, 96, 103, 1, 0, 0, 0,
		97, 98, 5, 15, 0, 0, 98, 99, 5, 48, 0, 0, 99, 100, 5, 2, 0, 0, 100, 101,
		5, 51, 0, 0, 101, 103, 5, 3, 0, 0, 102, 92, 1, 0, 0, 0, 102, 97, 1, 0,
		0, 0, 103, 11, 1, 0, 0, 0, 104, 107, 5, 19, 0, 0, 105, 106, 5, 20, 0, 0,
		106, 108, 3, 40, 20, 0, 107, 105, 1, 0, 0, 0, 107, 108, 1, 0, 0, 0, 108,
		109, 1, 0, 0, 0, 109, 110, 5, 2, 0, 0, 110, 111, 3, 20, 10, 0, 111, 115,
		5, 3, 0, 0, 112, 113, 5, 32, 0, 0, 113, 114, 5, 25, 0, 0, 114, 116, 5,
		26, 0, 0, 115, 112, 1, 0, 0, 0, 115, 116, 1, 0, 0, 0, 116, 119, 1, 0, 0,
		0, 117, 118, 5, 22, 0, 0, 118, 120, 5, 52, 0, 0, 119, 117, 1, 0, 0, 0,
		119, 120, 1, 0, 0, 0, 120, 123, 1, 0, 0, 0, 121, 122, 5, 24, 0, 0, 122,
		124, 5, 54, 0, 0, 123, 121, 1, 0, 0, 0, 123, 124, 1, 0, 0, 0, 124, 147,
		1, 0, 0, 0, 125, 127, 5, 43, 0, 0, 126, 128, 7, 0, 0, 0, 127, 126, 1, 0,
		0, 0, 127, 128, 1, 0, 0, 0, 128, 131, 1, 0, 0, 0, 129, 130, 5, 22, 0, 0,
		130, 132, 5, 52, 0, 0, 131, 129, 1, 0, 0, 0, 131, 132, 1, 0, 0, 0, 132,
		135, 1, 0, 0, 0, 133, 134, 5, 24, 0, 0, 134, 136, 5, 54, 0, 0, 135, 133,
		1, 0, 0, 0, 135, 136, 1, 0, 0, 0, 136, 147, 1, 0, 0, 0, 137, 138, 5, 22,
		0, 0, 138, 139, 5, 52, 0, 0, 139, 142, 1, 0, 0, 0, 140, 141, 5, 24, 0,
		0, 141, 143, 5, 54, 0, 0, 142, 140, 1, 0, 0, 0, 142, 143, 1, 0, 0, 0, 143,
		147, 1, 0, 0, 0, 144, 145, 5, 24, 0, 0, 145, 147, 5, 54, 0, 0, 146, 104,
		1, 0, 0, 0, 146, 125, 1, 0, 0, 0, 146, 137, 1, 0, 0, 0, 146, 144, 1, 0,
		0, 0, 147, 13, 1, 0, 0, 0, 148, 163, 5, 12, 0, 0, 149, 164, 3, 24, 12,
		0, 150, 151, 5, 15, 0, 0, 151, 152, 3, 26, 13, 0, 152, 153, 5, 18, 0, 0,
		153, 161, 3, 28, 14, 0, 154, 155, 5, 17, 0, 0, 155, 162, 3, 38, 19, 0,
		156, 157, 5, 15, 0, 0, 157, 158, 3, 38, 19, 0, 158, 159, 5, 16, 0, 0, 159,
		160, 3, 38, 19, 0, 160, 162, 1, 0, 0, 0, 161, 154, 1, 0, 0, 0, 161, 156,
		1, 0, 0, 0, 162, 164, 1, 0, 0, 0, 163, 149, 1, 0, 0, 0, 163, 150, 1, 0,
		0, 0, 164, 15, 1, 0, 0, 0, 165, 181, 5, 13, 0, 0, 166, 182, 5, 27, 0, 0,
		167, 168, 5, 29, 0, 0, 168, 169, 5, 30, 0, 0, 169, 170, 5, 15, 0, 0, 170,
		182, 3, 26, 13, 0, 171, 172, 5, 29, 0, 0, 172, 175, 5, 31, 0, 0, 173, 174,
		5, 15, 0, 0, 174, 176, 3, 26, 13, 0, 175, 173, 1, 0, 0, 0, 175, 176, 1,
		0, 0, 0, 176, 177, 1, 0, 0, 0, 177, 178, 5, 32, 0, 0, 178, 179, 5, 33,
		0, 0, 179, 180, 5, 4, 0, 0, 180, 182, 3, 32, 16, 0, 181, 166, 1, 0, 0,
		0, 181, 167, 1, 0, 0, 0, 181, 171, 1, 0, 0, 0, 182, 17, 1, 0, 0, 0, 183,
		185, 5, 39, 0, 0, 184, 186, 7, 1, 0, 0, 185, 184, 1, 0, 0, 0, 185, 186,
		1, 0, 0, 0, 186, 19, 1, 0, 0, 0, 187, 192, 3, 22, 11, 0, 188, 189, 5, 5,
		0, 0, 189, 191, 3, 22, 11, 0, 190, 188, 1, 0, 0, 0, 191, 194, 1, 0, 0,
		0, 192, 190, 1, 0, 0, 0, 192, 193, 1, 0, 0, 0, 193, 21, 1, 0, 0, 0, 194,
		192, 1, 0, 0, 0, 195, 196, 5, 53, 0, 0, 196, 197, 5, 2, 0, 0, 197, 198,
		7, 2, 0, 0, 198, 201, 5, 3, 0, 0, 199, 200, 5, 46, 0, 0, 200, 202, 5, 53,
		0, 0, 201, 199, 1, 0, 0, 0, 201, 202, 1, 0, 0, 0, 202, 23, 1, 0, 0, 0,
		203, 204, 5, 23, 0, 0, 204, 207, 3, 26, 13, 0, 205, 206, 5, 18, 0, 0, 206,
		208, 3, 28, 14, 0, 207, 205, 1, 0, 0, 0, 207, 208, 1, 0, 0, 0, 208, 25,
		1, 0, 0, 0, 209, 210, 7, 3, 0, 0, 210, 27, 1, 0, 0, 0, 211, 212, 5, 2,
		0, 0, 212, 217, 3, 30, 15, 0, 213, 214, 5, 5, 0, 0, 214, 216, 3, 30, 15,
		0, 215, 213, 1, 0, 0, 0, 216, 219, 1, 0, 0, 0, 217, 215, 1, 0, 0, 0, 217,
		218, 1, 0, 0, 0, 218, 220, 1, 0, 0, 0, 219, 217, 1, 0, 0, 0, 220, 221,
		5, 3, 0, 0, 221, 29, 1, 0, 0, 0, 222, 223, 7, 3, 0, 0, 223, 224, 5, 4,
		0, 0, 224, 225, 3, 32, 16, 0, 225, 31, 1, 0, 0, 0, 226, 227, 5, 54, 0,
		0, 227, 33, 1, 0, 0, 0, 228, 229, 5, 2, 0, 0, 229, 234, 3, 36, 18, 0, 230,
		231, 5, 5, 0, 0, 231, 233, 3, 36, 18, 0, 232, 230, 1, 0, 0, 0, 233, 236,
		1, 0, 0, 0, 234, 232, 1, 0, 0, 0, 234, 235, 1, 0, 0, 0, 235, 237, 1, 0,
		0, 0, 236, 234, 1, 0, 0, 0, 237, 238, 5, 3, 0, 0, 238, 35, 1, 0, 0, 0,
		239, 240, 5, 53, 0, 0, 240, 241, 5, 4, 0, 0, 241, 242, 3, 44, 22, 0, 242,
		37, 1, 0, 0, 0, 243, 257, 5, 52, 0, 0, 244, 245, 5, 35, 0, 0, 245, 246,
		5, 2, 0, 0, 246, 257, 5, 3, 0, 0, 247, 248, 5, 35, 0, 0, 248, 249, 5, 2,
		0, 0, 249, 250, 7, 4, 0, 0, 250, 251, 5, 51, 0, 0, 251, 257, 5, 3, 0, 0,
		252, 253, 5, 47, 0, 0, 253, 254, 5, 2, 0, 0, 254, 255, 5, 54, 0, 0, 255,
		257, 5, 3, 0, 0, 256, 243, 1, 0, 0, 0, 256, 244, 1, 0, 0, 0, 256, 247,
		1, 0, 0, 0, 256, 252, 1, 0, 0, 0, 257, 39, 1, 0, 0, 0, 258, 259, 5, 51,
		0, 0, 259, 41, 1, 0, 0, 0, 260, 261, 5, 52, 0, 0, 261, 43, 1, 0, 0, 0,
		262, 263, 7, 5, 0, 0, 263, 45, 1, 0, 0, 0, 28, 53, 56, 67, 73, 77, 87,
		90, 102, 107, 115, 119, 123, 127, 131, 135, 142, 146, 161, 163, 175, 181,
		185, 192, 201, 207, 217, 234, 256,
	}
	deserializer := antlr.NewATNDeserializer(nil)
	staticData.atn = deserializer.Deserialize(staticData.serializedATN)
	atn := staticData.atn
	staticData.decisionToDFA = make([]*antlr.DFA, len(atn.DecisionToState))
	decisionToDFA := staticData.decisionToDFA
	for index, state := range atn.DecisionToState {
		decisionToDFA[index] = antlr.NewDFA(state, index)
	}
}

// NexusParserInit initializes any static state used to implement NexusParser. By default the
// static state used to implement the parser is lazily initialized during the first call to
// NewNexusParser(). You can call this function if you wish to initialize the static state ahead
// of time.
func NexusParserInit() {
	staticData := &NexusParserStaticData
	staticData.once.Do(nexusParserInit)
}

// NewNexusParser produces a new parser instance for the optional input antlr.TokenStream.
func NewNexusParser(input antlr.TokenStream) *NexusParser {
	NexusParserInit()
	this := new(NexusParser)
	this.BaseParser = antlr.NewBaseParser(input)
	staticData := &NexusParserStaticData
	this.Interpreter = antlr.NewParserATNSimulator(this, staticData.atn, staticData.decisionToDFA, staticData.PredictionContextCache)
	this.RuleNames = staticData.RuleNames
	this.LiteralNames = staticData.LiteralNames
	this.SymbolicNames = staticData.SymbolicNames
	this.GrammarFileName = "Nexus.g4"

	return this
}

// NexusParser tokens.
const (
	NexusParserEOF              = antlr.TokenEOF
	NexusParserT__0             = 1
	NexusParserT__1             = 2
	NexusParserT__2             = 3
	NexusParserT__3             = 4
	NexusParserT__4             = 5
	NexusParserT__5             = 6
	NexusParserK_OVERWRITE      = 7
	NexusParserK_RESTORE        = 8
	NexusParserK_SNAPSHOT       = 9
	NexusParserK_PUSH           = 10
	NexusParserK_QUERY          = 11
	NexusParserK_REMOVE         = 12
	NexusParserK_SHOW           = 13
	NexusParserK_SET            = 14
	NexusParserK_FROM           = 15
	NexusParserK_TO             = 16
	NexusParserK_AT             = 17
	NexusParserK_TAGGED         = 18
	NexusParserK_AGGREGATE      = 19
	NexusParserK_BY             = 20
	NexusParserK_ON             = 21
	NexusParserK_LIMIT          = 22
	NexusParserK_SERIES         = 23
	NexusParserK_AFTER          = 24
	NexusParserK_EMPTY          = 25
	NexusParserK_WINDOWS        = 26
	NexusParserK_METRICS        = 27
	NexusParserK_TAGS           = 28
	NexusParserK_TAG            = 29
	NexusParserK_KEYS           = 30
	NexusParserK_VALUES         = 31
	NexusParserK_WITH           = 32
	NexusParserK_KEY            = 33
	NexusParserK_TIME           = 34
	NexusParserK_NOW            = 35
	NexusParserK_TRUE           = 36
	NexusParserK_FALSE          = 37
	NexusParserK_NULL           = 38
	NexusParserK_FLUSH          = 39
	NexusParserK_MEMTABLE       = 40
	NexusParserK_DISK           = 41
	NexusParserK_ALL            = 42
	NexusParserK_ORDER          = 43
	NexusParserK_ASC            = 44
	NexusParserK_DESC           = 45
	NexusParserK_AS             = 46
	NexusParserK_DT             = 47
	NexusParserK_RELATIVE       = 48
	NexusParserPLUS             = 49
	NexusParserMINUS            = 50
	NexusParserDURATION_LITERAL = 51
	NexusParserNUMBER           = 52
	NexusParserIDENTIFIER       = 53
	NexusParserSTRING_LITERAL   = 54
	NexusParserWS               = 55
	NexusParserLINE_COMMENT     = 56
)

// NexusParser rules.
const (
	NexusParserRULE_statement             = 0
	NexusParserRULE_snapshotStatement     = 1
	NexusParserRULE_restoreStatement      = 2
	NexusParserRULE_pushStatement         = 3
	NexusParserRULE_queryStatement        = 4
	NexusParserRULE_time_range            = 5
	NexusParserRULE_query_clauses         = 6
	NexusParserRULE_removeStatement       = 7
	NexusParserRULE_showStatement         = 8
	NexusParserRULE_flushStatement        = 9
	NexusParserRULE_aggregation_spec_list = 10
	NexusParserRULE_aggregation_spec      = 11
	NexusParserRULE_series_specifier      = 12
	NexusParserRULE_metric_name           = 13
	NexusParserRULE_tag_list              = 14
	NexusParserRULE_tag_assignment        = 15
	NexusParserRULE_tag_value             = 16
	NexusParserRULE_field_list            = 17
	NexusParserRULE_field_assignment      = 18
	NexusParserRULE_timestamp             = 19
	NexusParserRULE_duration              = 20
	NexusParserRULE_value                 = 21
	NexusParserRULE_literal_value         = 22
)

// IStatementContext is an interface to support dynamic dispatch.
type IStatementContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	EOF() antlr.TerminalNode
	PushStatement() IPushStatementContext
	QueryStatement() IQueryStatementContext
	RemoveStatement() IRemoveStatementContext
	ShowStatement() IShowStatementContext
	FlushStatement() IFlushStatementContext
	SnapshotStatement() ISnapshotStatementContext
	RestoreStatement() IRestoreStatementContext

	// IsStatementContext differentiates from other interfaces.
	IsStatementContext()
}

type StatementContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyStatementContext() *StatementContext {
	var p = new(StatementContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_statement
	return p
}

func InitEmptyStatementContext(p *StatementContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_statement
}

func (*StatementContext) IsStatementContext() {}

func NewStatementContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *StatementContext {
	var p = new(StatementContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = NexusParserRULE_statement

	return p
}

func (s *StatementContext) GetParser() antlr.Parser { return s.parser }

func (s *StatementContext) EOF() antlr.TerminalNode {
	return s.GetToken(NexusParserEOF, 0)
}

func (s *StatementContext) PushStatement() IPushStatementContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IPushStatementContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IPushStatementContext)
}

func (s *StatementContext) QueryStatement() IQueryStatementContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IQueryStatementContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IQueryStatementContext)
}

func (s *StatementContext) RemoveStatement() IRemoveStatementContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IRemoveStatementContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IRemoveStatementContext)
}

func (s *StatementContext) ShowStatement() IShowStatementContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IShowStatementContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IShowStatementContext)
}

func (s *StatementContext) FlushStatement() IFlushStatementContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFlushStatementContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFlushStatementContext)
}

func (s *StatementContext) SnapshotStatement() ISnapshotStatementContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ISnapshotStatementContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ISnapshotStatementContext)
}

func (s *StatementContext) RestoreStatement() IRestoreStatementContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IRestoreStatementContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IRestoreStatementContext)
}

func (s *StatementContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *StatementContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *StatementContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterStatement(s)
	}
}

func (s *StatementContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitStatement(s)
	}
}

func (s *StatementContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitStatement(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *NexusParser) Statement() (localctx IStatementContext) {
	localctx = NewStatementContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 0, NexusParserRULE_statement)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	p.SetState(53)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetTokenStream().LA(1) {
	case NexusParserK_PUSH:
		{
			p.SetState(46)
			p.PushStatement()
		}

	case NexusParserK_QUERY:
		{
			p.SetState(47)
			p.QueryStatement()
		}

	case NexusParserK_REMOVE:
		{
			p.SetState(48)
			p.RemoveStatement()
		}

	case NexusParserK_SHOW:
		{
			p.SetState(49)
			p.ShowStatement()
		}

	case NexusParserK_FLUSH:
		{
			p.SetState(50)
			p.FlushStatement()
		}

	case NexusParserK_SNAPSHOT:
		{
			p.SetState(51)
			p.SnapshotStatement()
		}

	case NexusParserK_RESTORE:
		{
			p.SetState(52)
			p.RestoreStatement()
		}

	default:
		p.SetError(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
		goto errorExit
	}
	p.SetState(56)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	if _la == NexusParserT__0 {
		{
			p.SetState(55)
			p.Match(NexusParserT__0)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	}
	{
		p.SetState(58)
		p.Match(NexusParserEOF)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// ISnapshotStatementContext is an interface to support dynamic dispatch.
type ISnapshotStatementContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	K_SNAPSHOT() antlr.TerminalNode

	// IsSnapshotStatementContext differentiates from other interfaces.
	IsSnapshotStatementContext()
}

type SnapshotStatementContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptySnapshotStatementContext() *SnapshotStatementContext {
	var p = new(SnapshotStatementContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_snapshotStatement
	return p
}

func InitEmptySnapshotStatementContext(p *SnapshotStatementContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_snapshotStatement
}

func (*SnapshotStatementContext) IsSnapshotStatementContext() {}

func NewSnapshotStatementContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *SnapshotStatementContext {
	var p = new(SnapshotStatementContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = NexusParserRULE_snapshotStatement

	return p
}

func (s *SnapshotStatementContext) GetParser() antlr.Parser { return s.parser }

func (s *SnapshotStatementContext) K_SNAPSHOT() antlr.TerminalNode {
	return s.GetToken(NexusParserK_SNAPSHOT, 0)
}

func (s *SnapshotStatementContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SnapshotStatementContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *SnapshotStatementContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterSnapshotStatement(s)
	}
}

func (s *SnapshotStatementContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitSnapshotStatement(s)
	}
}

func (s *SnapshotStatementContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitSnapshotStatement(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *NexusParser) SnapshotStatement() (localctx ISnapshotStatementContext) {
	localctx = NewSnapshotStatementContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 2, NexusParserRULE_snapshotStatement)
	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(60)
		p.Match(NexusParserK_SNAPSHOT)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IRestoreStatementContext is an interface to support dynamic dispatch.
type IRestoreStatementContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	K_RESTORE() antlr.TerminalNode
	K_FROM() antlr.TerminalNode
	STRING_LITERAL() antlr.TerminalNode
	K_WITH() antlr.TerminalNode
	K_OVERWRITE() antlr.TerminalNode

	// IsRestoreStatementContext differentiates from other interfaces.
	IsRestoreStatementContext()
}

type RestoreStatementContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyRestoreStatementContext() *RestoreStatementContext {
	var p = new(RestoreStatementContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_restoreStatement
	return p
}

func InitEmptyRestoreStatementContext(p *RestoreStatementContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_restoreStatement
}

func (*RestoreStatementContext) IsRestoreStatementContext() {}

func NewRestoreStatementContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *RestoreStatementContext {
	var p = new(RestoreStatementContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = NexusParserRULE_restoreStatement

	return p
}

func (s *RestoreStatementContext) GetParser() antlr.Parser { return s.parser }

func (s *RestoreStatementContext) K_RESTORE() antlr.TerminalNode {
	return s.GetToken(NexusParserK_RESTORE, 0)
}

func (s *RestoreStatementContext) K_FROM() antlr.TerminalNode {
	return s.GetToken(NexusParserK_FROM, 0)
}

func (s *RestoreStatementContext) STRING_LITERAL() antlr.TerminalNode {
	return s.GetToken(NexusParserSTRING_LITERAL, 0)
}

func (s *RestoreStatementContext) K_WITH() antlr.TerminalNode {
	return s.GetToken(NexusParserK_WITH, 0)
}

func (s *RestoreStatementContext) K_OVERWRITE() antlr.TerminalNode {
	return s.GetToken(NexusParserK_OVERWRITE, 0)
}

func (s *RestoreStatementContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *RestoreStatementContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *RestoreStatementContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterRestoreStatement(s)
	}
}

func (s *RestoreStatementContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitRestoreStatement(s)
	}
}

func (s *RestoreStatementContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitRestoreStatement(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *NexusParser) RestoreStatement() (localctx IRestoreStatementContext) {
	localctx = NewRestoreStatementContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 4, NexusParserRULE_restoreStatement)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(62)
		p.Match(NexusParserK_RESTORE)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(63)
		p.Match(NexusParserK_FROM)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(64)
		p.Match(NexusParserSTRING_LITERAL)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	p.SetState(67)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	if _la == NexusParserK_WITH {
		{
			p.SetState(65)
			p.Match(NexusParserK_WITH)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(66)
			p.Match(NexusParserK_OVERWRITE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IPushStatementContext is an interface to support dynamic dispatch.
type IPushStatementContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	K_PUSH() antlr.TerminalNode
	Metric_name() IMetric_nameContext
	K_SET() antlr.TerminalNode
	Field_list() IField_listContext
	K_TIME() antlr.TerminalNode
	Timestamp() ITimestampContext
	K_TAGGED() antlr.TerminalNode
	Tag_list() ITag_listContext

	// IsPushStatementContext differentiates from other interfaces.
	IsPushStatementContext()
}

type PushStatementContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyPushStatementContext() *PushStatementContext {
	var p = new(PushStatementContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_pushStatement
	return p
}

func InitEmptyPushStatementContext(p *PushStatementContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_pushStatement
}

func (*PushStatementContext) IsPushStatementContext() {}

func NewPushStatementContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *PushStatementContext {
	var p = new(PushStatementContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = NexusParserRULE_pushStatement

	return p
}

func (s *PushStatementContext) GetParser() antlr.Parser { return s.parser }

func (s *PushStatementContext) K_PUSH() antlr.TerminalNode {
	return s.GetToken(NexusParserK_PUSH, 0)
}

func (s *PushStatementContext) Metric_name() IMetric_nameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IMetric_nameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IMetric_nameContext)
}

func (s *PushStatementContext) K_SET() antlr.TerminalNode {
	return s.GetToken(NexusParserK_SET, 0)
}

func (s *PushStatementContext) Field_list() IField_listContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IField_listContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IField_listContext)
}

func (s *PushStatementContext) K_TIME() antlr.TerminalNode {
	return s.GetToken(NexusParserK_TIME, 0)
}

func (s *PushStatementContext) Timestamp() ITimestampContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ITimestampContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ITimestampContext)
}

func (s *PushStatementContext) K_TAGGED() antlr.TerminalNode {
	return s.GetToken(NexusParserK_TAGGED, 0)
}

func (s *PushStatementContext) Tag_list() ITag_listContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ITag_listContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ITag_listContext)
}

func (s *PushStatementContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PushStatementContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *PushStatementContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterPushStatement(s)
	}
}

func (s *PushStatementContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitPushStatement(s)
	}
}

func (s *PushStatementContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitPushStatement(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *NexusParser) PushStatement() (localctx IPushStatementContext) {
	localctx = NewPushStatementContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 6, NexusParserRULE_pushStatement)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(69)
		p.Match(NexusParserK_PUSH)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(70)
		p.Metric_name()
	}
	p.SetState(73)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	if _la == NexusParserK_TIME {
		{
			p.SetState(71)
			p.Match(NexusParserK_TIME)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(72)
			p.Timestamp()
		}

	}
	p.SetState(77)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	if _la == NexusParserK_TAGGED {
		{
			p.SetState(75)
			p.Match(NexusParserK_TAGGED)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(76)
			p.Tag_list()
		}

	}
	{
		p.SetState(79)
		p.Match(NexusParserK_SET)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(80)
		p.Field_list()
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IQueryStatementContext is an interface to support dynamic dispatch.
type IQueryStatementContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	K_QUERY() antlr.TerminalNode
	Metric_name() IMetric_nameContext
	Time_range() ITime_rangeContext
	K_TAGGED() antlr.TerminalNode
	Tag_list() ITag_listContext
	Query_clauses() IQuery_clausesContext

	// IsQueryStatementContext differentiates from other interfaces.
	IsQueryStatementContext()
}

type QueryStatementContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyQueryStatementContext() *QueryStatementContext {
	var p = new(QueryStatementContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_queryStatement
	return p
}

func InitEmptyQueryStatementContext(p *QueryStatementContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_queryStatement
}

func (*QueryStatementContext) IsQueryStatementContext() {}

func NewQueryStatementContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *QueryStatementContext {
	var p = new(QueryStatementContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = NexusParserRULE_queryStatement

	return p
}

func (s *QueryStatementContext) GetParser() antlr.Parser { return s.parser }

func (s *QueryStatementContext) K_QUERY() antlr.TerminalNode {
	return s.GetToken(NexusParserK_QUERY, 0)
}

func (s *QueryStatementContext) Metric_name() IMetric_nameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IMetric_nameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IMetric_nameContext)
}

func (s *QueryStatementContext) Time_range() ITime_rangeContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ITime_rangeContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ITime_rangeContext)
}

func (s *QueryStatementContext) K_TAGGED() antlr.TerminalNode {
	return s.GetToken(NexusParserK_TAGGED, 0)
}

func (s *QueryStatementContext) Tag_list() ITag_listContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ITag_listContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ITag_listContext)
}

func (s *QueryStatementContext) Query_clauses() IQuery_clausesContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IQuery_clausesContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IQuery_clausesContext)
}

func (s *QueryStatementContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *QueryStatementContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *QueryStatementContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterQueryStatement(s)
	}
}

func (s *QueryStatementContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitQueryStatement(s)
	}
}

func (s *QueryStatementContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitQueryStatement(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *NexusParser) QueryStatement() (localctx IQueryStatementContext) {
	localctx = NewQueryStatementContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 8, NexusParserRULE_queryStatement)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(82)
		p.Match(NexusParserK_QUERY)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(83)
		p.Metric_name()
	}
	{
		p.SetState(84)
		p.Time_range()
	}
	p.SetState(87)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	if _la == NexusParserK_TAGGED {
		{
			p.SetState(85)
			p.Match(NexusParserK_TAGGED)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(86)
			p.Tag_list()
		}

	}
	p.SetState(90)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	if (int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&8796114518016) != 0 {
		{
			p.SetState(89)
			p.Query_clauses()
		}

	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// ITime_rangeContext is an interface to support dynamic dispatch.
type ITime_rangeContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	K_FROM() antlr.TerminalNode
	AllTimestamp() []ITimestampContext
	Timestamp(i int) ITimestampContext
	K_TO() antlr.TerminalNode
	K_RELATIVE() antlr.TerminalNode
	DURATION_LITERAL() antlr.TerminalNode

	// IsTime_rangeContext differentiates from other interfaces.
	IsTime_rangeContext()
}

type Time_rangeContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyTime_rangeContext() *Time_rangeContext {
	var p = new(Time_rangeContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_time_range
	return p
}

func InitEmptyTime_rangeContext(p *Time_rangeContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_time_range
}

func (*Time_rangeContext) IsTime_rangeContext() {}

func NewTime_rangeContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *Time_rangeContext {
	var p = new(Time_rangeContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = NexusParserRULE_time_range

	return p
}

func (s *Time_rangeContext) GetParser() antlr.Parser { return s.parser }

func (s *Time_rangeContext) K_FROM() antlr.TerminalNode {
	return s.GetToken(NexusParserK_FROM, 0)
}

func (s *Time_rangeContext) AllTimestamp() []ITimestampContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(ITimestampContext); ok {
			len++
		}
	}

	tst := make([]ITimestampContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(ITimestampContext); ok {
			tst[i] = t.(ITimestampContext)
			i++
		}
	}

	return tst
}

func (s *Time_rangeContext) Timestamp(i int) ITimestampContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ITimestampContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(ITimestampContext)
}

func (s *Time_rangeContext) K_TO() antlr.TerminalNode {
	return s.GetToken(NexusParserK_TO, 0)
}

func (s *Time_rangeContext) K_RELATIVE() antlr.TerminalNode {
	return s.GetToken(NexusParserK_RELATIVE, 0)
}

func (s *Time_rangeContext) DURATION_LITERAL() antlr.TerminalNode {
	return s.GetToken(NexusParserDURATION_LITERAL, 0)
}

func (s *Time_rangeContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *Time_rangeContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *Time_rangeContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterTime_range(s)
	}
}

func (s *Time_rangeContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitTime_range(s)
	}
}

func (s *Time_rangeContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitTime_range(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *NexusParser) Time_range() (localctx ITime_rangeContext) {
	localctx = NewTime_rangeContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 10, NexusParserRULE_time_range)
	p.SetState(102)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 7, p.GetParserRuleContext()) {
	case 1:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(92)
			p.Match(NexusParserK_FROM)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(93)
			p.Timestamp()
		}
		{
			p.SetState(94)
			p.Match(NexusParserK_TO)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(95)
			p.Timestamp()
		}

	case 2:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(97)
			p.Match(NexusParserK_FROM)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(98)
			p.Match(NexusParserK_RELATIVE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(99)
			p.Match(NexusParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(100)
			p.Match(NexusParserDURATION_LITERAL)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(101)
			p.Match(NexusParserT__2)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case antlr.ATNInvalidAltNumber:
		goto errorExit
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IQuery_clausesContext is an interface to support dynamic dispatch.
type IQuery_clausesContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	K_AGGREGATE() antlr.TerminalNode
	Aggregation_spec_list() IAggregation_spec_listContext
	K_LIMIT() antlr.TerminalNode
	NUMBER() antlr.TerminalNode
	K_AFTER() antlr.TerminalNode
	STRING_LITERAL() antlr.TerminalNode
	K_BY() antlr.TerminalNode
	Duration() IDurationContext
	K_WITH() antlr.TerminalNode
	K_EMPTY() antlr.TerminalNode
	K_WINDOWS() antlr.TerminalNode
	K_ORDER() antlr.TerminalNode
	K_ASC() antlr.TerminalNode
	K_DESC() antlr.TerminalNode

	// IsQuery_clausesContext differentiates from other interfaces.
	IsQuery_clausesContext()
}

type Query_clausesContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyQuery_clausesContext() *Query_clausesContext {
	var p = new(Query_clausesContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_query_clauses
	return p
}

func InitEmptyQuery_clausesContext(p *Query_clausesContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_query_clauses
}

func (*Query_clausesContext) IsQuery_clausesContext() {}

func NewQuery_clausesContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *Query_clausesContext {
	var p = new(Query_clausesContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = NexusParserRULE_query_clauses

	return p
}

func (s *Query_clausesContext) GetParser() antlr.Parser { return s.parser }

func (s *Query_clausesContext) K_AGGREGATE() antlr.TerminalNode {
	return s.GetToken(NexusParserK_AGGREGATE, 0)
}

func (s *Query_clausesContext) Aggregation_spec_list() IAggregation_spec_listContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IAggregation_spec_listContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IAggregation_spec_listContext)
}

func (s *Query_clausesContext) K_LIMIT() antlr.TerminalNode {
	return s.GetToken(NexusParserK_LIMIT, 0)
}

func (s *Query_clausesContext) NUMBER() antlr.TerminalNode {
	return s.GetToken(NexusParserNUMBER, 0)
}

func (s *Query_clausesContext) K_AFTER() antlr.TerminalNode {
	return s.GetToken(NexusParserK_AFTER, 0)
}

func (s *Query_clausesContext) STRING_LITERAL() antlr.TerminalNode {
	return s.GetToken(NexusParserSTRING_LITERAL, 0)
}

func (s *Query_clausesContext) K_BY() antlr.TerminalNode {
	return s.GetToken(NexusParserK_BY, 0)
}

func (s *Query_clausesContext) Duration() IDurationContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IDurationContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IDurationContext)
}

func (s *Query_clausesContext) K_WITH() antlr.TerminalNode {
	return s.GetToken(NexusParserK_WITH, 0)
}

func (s *Query_clausesContext) K_EMPTY() antlr.TerminalNode {
	return s.GetToken(NexusParserK_EMPTY, 0)
}

func (s *Query_clausesContext) K_WINDOWS() antlr.TerminalNode {
	return s.GetToken(NexusParserK_WINDOWS, 0)
}

func (s *Query_clausesContext) K_ORDER() antlr.TerminalNode {
	return s.GetToken(NexusParserK_ORDER, 0)
}

func (s *Query_clausesContext) K_ASC() antlr.TerminalNode {
	return s.GetToken(NexusParserK_ASC, 0)
}

func (s *Query_clausesContext) K_DESC() antlr.TerminalNode {
	return s.GetToken(NexusParserK_DESC, 0)
}

func (s *Query_clausesContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *Query_clausesContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *Query_clausesContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterQuery_clauses(s)
	}
}

func (s *Query_clausesContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitQuery_clauses(s)
	}
}

func (s *Query_clausesContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitQuery_clauses(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *NexusParser) Query_clauses() (localctx IQuery_clausesContext) {
	localctx = NewQuery_clausesContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 12, NexusParserRULE_query_clauses)
	var _la int

	p.SetState(146)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetTokenStream().LA(1) {
	case NexusParserK_AGGREGATE:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(104)
			p.Match(NexusParserK_AGGREGATE)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(107)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if _la == NexusParserK_BY {
			{
				p.SetState(105)
				p.Match(NexusParserK_BY)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(106)
				p.Duration()
			}

		}
		{
			p.SetState(109)
			p.Match(NexusParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(110)
			p.Aggregation_spec_list()
		}
		{
			p.SetState(111)
			p.Match(NexusParserT__2)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(115)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if _la == NexusParserK_WITH {
			{
				p.SetState(112)
				p.Match(NexusParserK_WITH)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(113)
				p.Match(NexusParserK_EMPTY)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(114)
				p.Match(NexusParserK_WINDOWS)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		}

		p.SetState(119)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if _la == NexusParserK_LIMIT {
			{
				p.SetState(117)
				p.Match(NexusParserK_LIMIT)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(118)
				p.Match(NexusParserNUMBER)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		}
		p.SetState(123)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if _la == NexusParserK_AFTER {
			{
				p.SetState(121)
				p.Match(NexusParserK_AFTER)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(122)
				p.Match(NexusParserSTRING_LITERAL)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		}

	case NexusParserK_ORDER:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(125)
			p.Match(NexusParserK_ORDER)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(127)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if _la == NexusParserK_ASC || _la == NexusParserK_DESC {
			{
				p.SetState(126)
				_la = p.GetTokenStream().LA(1)

				if !(_la == NexusParserK_ASC || _la == NexusParserK_DESC) {
					p.GetErrorHandler().RecoverInline(p)
				} else {
					p.GetErrorHandler().ReportMatch(p)
					p.Consume()
				}
			}

		}

		p.SetState(131)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if _la == NexusParserK_LIMIT {
			{
				p.SetState(129)
				p.Match(NexusParserK_LIMIT)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(130)
				p.Match(NexusParserNUMBER)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		}
		p.SetState(135)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if _la == NexusParserK_AFTER {
			{
				p.SetState(133)
				p.Match(NexusParserK_AFTER)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(134)
				p.Match(NexusParserSTRING_LITERAL)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		}

	case NexusParserK_LIMIT:
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(137)
			p.Match(NexusParserK_LIMIT)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(138)
			p.Match(NexusParserNUMBER)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

		p.SetState(142)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if _la == NexusParserK_AFTER {
			{
				p.SetState(140)
				p.Match(NexusParserK_AFTER)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(141)
				p.Match(NexusParserSTRING_LITERAL)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}

		}

	case NexusParserK_AFTER:
		p.EnterOuterAlt(localctx, 4)
		{
			p.SetState(144)
			p.Match(NexusParserK_AFTER)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(145)
			p.Match(NexusParserSTRING_LITERAL)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	default:
		p.SetError(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
		goto errorExit
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IRemoveStatementContext is an interface to support dynamic dispatch.
type IRemoveStatementContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	K_REMOVE() antlr.TerminalNode
	Series_specifier() ISeries_specifierContext
	AllK_FROM() []antlr.TerminalNode
	K_FROM(i int) antlr.TerminalNode
	Metric_name() IMetric_nameContext
	K_TAGGED() antlr.TerminalNode
	Tag_list() ITag_listContext
	K_AT() antlr.TerminalNode
	AllTimestamp() []ITimestampContext
	Timestamp(i int) ITimestampContext
	K_TO() antlr.TerminalNode

	// IsRemoveStatementContext differentiates from other interfaces.
	IsRemoveStatementContext()
}

type RemoveStatementContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyRemoveStatementContext() *RemoveStatementContext {
	var p = new(RemoveStatementContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_removeStatement
	return p
}

func InitEmptyRemoveStatementContext(p *RemoveStatementContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_removeStatement
}

func (*RemoveStatementContext) IsRemoveStatementContext() {}

func NewRemoveStatementContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *RemoveStatementContext {
	var p = new(RemoveStatementContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = NexusParserRULE_removeStatement

	return p
}

func (s *RemoveStatementContext) GetParser() antlr.Parser { return s.parser }

func (s *RemoveStatementContext) K_REMOVE() antlr.TerminalNode {
	return s.GetToken(NexusParserK_REMOVE, 0)
}

func (s *RemoveStatementContext) Series_specifier() ISeries_specifierContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ISeries_specifierContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ISeries_specifierContext)
}

func (s *RemoveStatementContext) AllK_FROM() []antlr.TerminalNode {
	return s.GetTokens(NexusParserK_FROM)
}

func (s *RemoveStatementContext) K_FROM(i int) antlr.TerminalNode {
	return s.GetToken(NexusParserK_FROM, i)
}

func (s *RemoveStatementContext) Metric_name() IMetric_nameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IMetric_nameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IMetric_nameContext)
}

func (s *RemoveStatementContext) K_TAGGED() antlr.TerminalNode {
	return s.GetToken(NexusParserK_TAGGED, 0)
}

func (s *RemoveStatementContext) Tag_list() ITag_listContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ITag_listContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ITag_listContext)
}

func (s *RemoveStatementContext) K_AT() antlr.TerminalNode {
	return s.GetToken(NexusParserK_AT, 0)
}

func (s *RemoveStatementContext) AllTimestamp() []ITimestampContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(ITimestampContext); ok {
			len++
		}
	}

	tst := make([]ITimestampContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(ITimestampContext); ok {
			tst[i] = t.(ITimestampContext)
			i++
		}
	}

	return tst
}

func (s *RemoveStatementContext) Timestamp(i int) ITimestampContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ITimestampContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(ITimestampContext)
}

func (s *RemoveStatementContext) K_TO() antlr.TerminalNode {
	return s.GetToken(NexusParserK_TO, 0)
}

func (s *RemoveStatementContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *RemoveStatementContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *RemoveStatementContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterRemoveStatement(s)
	}
}

func (s *RemoveStatementContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitRemoveStatement(s)
	}
}

func (s *RemoveStatementContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitRemoveStatement(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *NexusParser) RemoveStatement() (localctx IRemoveStatementContext) {
	localctx = NewRemoveStatementContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 14, NexusParserRULE_removeStatement)
	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(148)
		p.Match(NexusParserK_REMOVE)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	p.SetState(163)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetTokenStream().LA(1) {
	case NexusParserK_SERIES:
		{
			p.SetState(149)
			p.Series_specifier()
		}

	case NexusParserK_FROM:
		{
			p.SetState(150)
			p.Match(NexusParserK_FROM)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(151)
			p.Metric_name()
		}
		{
			p.SetState(152)
			p.Match(NexusParserK_TAGGED)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(153)
			p.Tag_list()
		}
		p.SetState(161)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}

		switch p.GetTokenStream().LA(1) {
		case NexusParserK_AT:
			{
				p.SetState(154)
				p.Match(NexusParserK_AT)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(155)
				p.Timestamp()
			}

		case NexusParserK_FROM:
			{
				p.SetState(156)
				p.Match(NexusParserK_FROM)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(157)
				p.Timestamp()
			}
			{
				p.SetState(158)
				p.Match(NexusParserK_TO)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(159)
				p.Timestamp()
			}

		default:
			p.SetError(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
			goto errorExit
		}

	default:
		p.SetError(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
		goto errorExit
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IShowStatementContext is an interface to support dynamic dispatch.
type IShowStatementContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	K_SHOW() antlr.TerminalNode
	K_METRICS() antlr.TerminalNode
	K_TAG() antlr.TerminalNode
	K_KEYS() antlr.TerminalNode
	K_FROM() antlr.TerminalNode
	Metric_name() IMetric_nameContext
	K_VALUES() antlr.TerminalNode
	K_WITH() antlr.TerminalNode
	K_KEY() antlr.TerminalNode
	Tag_value() ITag_valueContext

	// IsShowStatementContext differentiates from other interfaces.
	IsShowStatementContext()
}

type ShowStatementContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyShowStatementContext() *ShowStatementContext {
	var p = new(ShowStatementContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_showStatement
	return p
}

func InitEmptyShowStatementContext(p *ShowStatementContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_showStatement
}

func (*ShowStatementContext) IsShowStatementContext() {}

func NewShowStatementContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *ShowStatementContext {
	var p = new(ShowStatementContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = NexusParserRULE_showStatement

	return p
}

func (s *ShowStatementContext) GetParser() antlr.Parser { return s.parser }

func (s *ShowStatementContext) K_SHOW() antlr.TerminalNode {
	return s.GetToken(NexusParserK_SHOW, 0)
}

func (s *ShowStatementContext) K_METRICS() antlr.TerminalNode {
	return s.GetToken(NexusParserK_METRICS, 0)
}

func (s *ShowStatementContext) K_TAG() antlr.TerminalNode {
	return s.GetToken(NexusParserK_TAG, 0)
}

func (s *ShowStatementContext) K_KEYS() antlr.TerminalNode {
	return s.GetToken(NexusParserK_KEYS, 0)
}

func (s *ShowStatementContext) K_FROM() antlr.TerminalNode {
	return s.GetToken(NexusParserK_FROM, 0)
}

func (s *ShowStatementContext) Metric_name() IMetric_nameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IMetric_nameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IMetric_nameContext)
}

func (s *ShowStatementContext) K_VALUES() antlr.TerminalNode {
	return s.GetToken(NexusParserK_VALUES, 0)
}

func (s *ShowStatementContext) K_WITH() antlr.TerminalNode {
	return s.GetToken(NexusParserK_WITH, 0)
}

func (s *ShowStatementContext) K_KEY() antlr.TerminalNode {
	return s.GetToken(NexusParserK_KEY, 0)
}

func (s *ShowStatementContext) Tag_value() ITag_valueContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ITag_valueContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ITag_valueContext)
}

func (s *ShowStatementContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ShowStatementContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *ShowStatementContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterShowStatement(s)
	}
}

func (s *ShowStatementContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitShowStatement(s)
	}
}

func (s *ShowStatementContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitShowStatement(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *NexusParser) ShowStatement() (localctx IShowStatementContext) {
	localctx = NewShowStatementContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 16, NexusParserRULE_showStatement)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(165)
		p.Match(NexusParserK_SHOW)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	p.SetState(181)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 20, p.GetParserRuleContext()) {
	case 1:
		{
			p.SetState(166)
			p.Match(NexusParserK_METRICS)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 2:
		{
			p.SetState(167)
			p.Match(NexusParserK_TAG)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(168)
			p.Match(NexusParserK_KEYS)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(169)
			p.Match(NexusParserK_FROM)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(170)
			p.Metric_name()
		}

	case 3:
		{
			p.SetState(171)
			p.Match(NexusParserK_TAG)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(172)
			p.Match(NexusParserK_VALUES)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(175)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if _la == NexusParserK_FROM {
			{
				p.SetState(173)
				p.Match(NexusParserK_FROM)
				if p.HasError() {
					// Recognition error - abort rule
					goto errorExit
				}
			}
			{
				p.SetState(174)
				p.Metric_name()
			}

		}
		{
			p.SetState(177)
			p.Match(NexusParserK_WITH)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(178)
			p.Match(NexusParserK_KEY)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(179)
			p.Match(NexusParserT__3)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(180)
			p.Tag_value()
		}

	case antlr.ATNInvalidAltNumber:
		goto errorExit
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IFlushStatementContext is an interface to support dynamic dispatch.
type IFlushStatementContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	K_FLUSH() antlr.TerminalNode
	K_MEMTABLE() antlr.TerminalNode
	K_DISK() antlr.TerminalNode
	K_ALL() antlr.TerminalNode

	// IsFlushStatementContext differentiates from other interfaces.
	IsFlushStatementContext()
}

type FlushStatementContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyFlushStatementContext() *FlushStatementContext {
	var p = new(FlushStatementContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_flushStatement
	return p
}

func InitEmptyFlushStatementContext(p *FlushStatementContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_flushStatement
}

func (*FlushStatementContext) IsFlushStatementContext() {}

func NewFlushStatementContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *FlushStatementContext {
	var p = new(FlushStatementContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = NexusParserRULE_flushStatement

	return p
}

func (s *FlushStatementContext) GetParser() antlr.Parser { return s.parser }

func (s *FlushStatementContext) K_FLUSH() antlr.TerminalNode {
	return s.GetToken(NexusParserK_FLUSH, 0)
}

func (s *FlushStatementContext) K_MEMTABLE() antlr.TerminalNode {
	return s.GetToken(NexusParserK_MEMTABLE, 0)
}

func (s *FlushStatementContext) K_DISK() antlr.TerminalNode {
	return s.GetToken(NexusParserK_DISK, 0)
}

func (s *FlushStatementContext) K_ALL() antlr.TerminalNode {
	return s.GetToken(NexusParserK_ALL, 0)
}

func (s *FlushStatementContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *FlushStatementContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *FlushStatementContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterFlushStatement(s)
	}
}

func (s *FlushStatementContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitFlushStatement(s)
	}
}

func (s *FlushStatementContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitFlushStatement(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *NexusParser) FlushStatement() (localctx IFlushStatementContext) {
	localctx = NewFlushStatementContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 18, NexusParserRULE_flushStatement)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(183)
		p.Match(NexusParserK_FLUSH)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	p.SetState(185)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	if (int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&7696581394432) != 0 {
		{
			p.SetState(184)
			_la = p.GetTokenStream().LA(1)

			if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&7696581394432) != 0) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}

	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IAggregation_spec_listContext is an interface to support dynamic dispatch.
type IAggregation_spec_listContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	AllAggregation_spec() []IAggregation_specContext
	Aggregation_spec(i int) IAggregation_specContext

	// IsAggregation_spec_listContext differentiates from other interfaces.
	IsAggregation_spec_listContext()
}

type Aggregation_spec_listContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyAggregation_spec_listContext() *Aggregation_spec_listContext {
	var p = new(Aggregation_spec_listContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_aggregation_spec_list
	return p
}

func InitEmptyAggregation_spec_listContext(p *Aggregation_spec_listContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_aggregation_spec_list
}

func (*Aggregation_spec_listContext) IsAggregation_spec_listContext() {}

func NewAggregation_spec_listContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *Aggregation_spec_listContext {
	var p = new(Aggregation_spec_listContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = NexusParserRULE_aggregation_spec_list

	return p
}

func (s *Aggregation_spec_listContext) GetParser() antlr.Parser { return s.parser }

func (s *Aggregation_spec_listContext) AllAggregation_spec() []IAggregation_specContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IAggregation_specContext); ok {
			len++
		}
	}

	tst := make([]IAggregation_specContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IAggregation_specContext); ok {
			tst[i] = t.(IAggregation_specContext)
			i++
		}
	}

	return tst
}

func (s *Aggregation_spec_listContext) Aggregation_spec(i int) IAggregation_specContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IAggregation_specContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IAggregation_specContext)
}

func (s *Aggregation_spec_listContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *Aggregation_spec_listContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *Aggregation_spec_listContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterAggregation_spec_list(s)
	}
}

func (s *Aggregation_spec_listContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitAggregation_spec_list(s)
	}
}

func (s *Aggregation_spec_listContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitAggregation_spec_list(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *NexusParser) Aggregation_spec_list() (localctx IAggregation_spec_listContext) {
	localctx = NewAggregation_spec_listContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 20, NexusParserRULE_aggregation_spec_list)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(187)
		p.Aggregation_spec()
	}
	p.SetState(192)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	for _la == NexusParserT__4 {
		{
			p.SetState(188)
			p.Match(NexusParserT__4)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(189)
			p.Aggregation_spec()
		}

		p.SetState(194)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IAggregation_specContext is an interface to support dynamic dispatch.
type IAggregation_specContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	AllIDENTIFIER() []antlr.TerminalNode
	IDENTIFIER(i int) antlr.TerminalNode
	K_AS() antlr.TerminalNode

	// IsAggregation_specContext differentiates from other interfaces.
	IsAggregation_specContext()
}

type Aggregation_specContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyAggregation_specContext() *Aggregation_specContext {
	var p = new(Aggregation_specContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_aggregation_spec
	return p
}

func InitEmptyAggregation_specContext(p *Aggregation_specContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_aggregation_spec
}

func (*Aggregation_specContext) IsAggregation_specContext() {}

func NewAggregation_specContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *Aggregation_specContext {
	var p = new(Aggregation_specContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = NexusParserRULE_aggregation_spec

	return p
}

func (s *Aggregation_specContext) GetParser() antlr.Parser { return s.parser }

func (s *Aggregation_specContext) AllIDENTIFIER() []antlr.TerminalNode {
	return s.GetTokens(NexusParserIDENTIFIER)
}

func (s *Aggregation_specContext) IDENTIFIER(i int) antlr.TerminalNode {
	return s.GetToken(NexusParserIDENTIFIER, i)
}

func (s *Aggregation_specContext) K_AS() antlr.TerminalNode {
	return s.GetToken(NexusParserK_AS, 0)
}

func (s *Aggregation_specContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *Aggregation_specContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *Aggregation_specContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterAggregation_spec(s)
	}
}

func (s *Aggregation_specContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitAggregation_spec(s)
	}
}

func (s *Aggregation_specContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitAggregation_spec(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *NexusParser) Aggregation_spec() (localctx IAggregation_specContext) {
	localctx = NewAggregation_specContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 22, NexusParserRULE_aggregation_spec)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(195)
		p.Match(NexusParserIDENTIFIER)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(196)
		p.Match(NexusParserT__1)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(197)
		_la = p.GetTokenStream().LA(1)

		if !(_la == NexusParserT__5 || _la == NexusParserIDENTIFIER) {
			p.GetErrorHandler().RecoverInline(p)
		} else {
			p.GetErrorHandler().ReportMatch(p)
			p.Consume()
		}
	}
	{
		p.SetState(198)
		p.Match(NexusParserT__2)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	p.SetState(201)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	if _la == NexusParserK_AS {
		{
			p.SetState(199)
			p.Match(NexusParserK_AS)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(200)
			p.Match(NexusParserIDENTIFIER)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// ISeries_specifierContext is an interface to support dynamic dispatch.
type ISeries_specifierContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	K_SERIES() antlr.TerminalNode
	Metric_name() IMetric_nameContext
	K_TAGGED() antlr.TerminalNode
	Tag_list() ITag_listContext

	// IsSeries_specifierContext differentiates from other interfaces.
	IsSeries_specifierContext()
}

type Series_specifierContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptySeries_specifierContext() *Series_specifierContext {
	var p = new(Series_specifierContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_series_specifier
	return p
}

func InitEmptySeries_specifierContext(p *Series_specifierContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_series_specifier
}

func (*Series_specifierContext) IsSeries_specifierContext() {}

func NewSeries_specifierContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *Series_specifierContext {
	var p = new(Series_specifierContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = NexusParserRULE_series_specifier

	return p
}

func (s *Series_specifierContext) GetParser() antlr.Parser { return s.parser }

func (s *Series_specifierContext) K_SERIES() antlr.TerminalNode {
	return s.GetToken(NexusParserK_SERIES, 0)
}

func (s *Series_specifierContext) Metric_name() IMetric_nameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IMetric_nameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IMetric_nameContext)
}

func (s *Series_specifierContext) K_TAGGED() antlr.TerminalNode {
	return s.GetToken(NexusParserK_TAGGED, 0)
}

func (s *Series_specifierContext) Tag_list() ITag_listContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ITag_listContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ITag_listContext)
}

func (s *Series_specifierContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *Series_specifierContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *Series_specifierContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterSeries_specifier(s)
	}
}

func (s *Series_specifierContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitSeries_specifier(s)
	}
}

func (s *Series_specifierContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitSeries_specifier(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *NexusParser) Series_specifier() (localctx ISeries_specifierContext) {
	localctx = NewSeries_specifierContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 24, NexusParserRULE_series_specifier)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(203)
		p.Match(NexusParserK_SERIES)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(204)
		p.Metric_name()
	}
	p.SetState(207)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	if _la == NexusParserK_TAGGED {
		{
			p.SetState(205)
			p.Match(NexusParserK_TAGGED)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(206)
			p.Tag_list()
		}

	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IMetric_nameContext is an interface to support dynamic dispatch.
type IMetric_nameContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	IDENTIFIER() antlr.TerminalNode
	STRING_LITERAL() antlr.TerminalNode

	// IsMetric_nameContext differentiates from other interfaces.
	IsMetric_nameContext()
}

type Metric_nameContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyMetric_nameContext() *Metric_nameContext {
	var p = new(Metric_nameContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_metric_name
	return p
}

func InitEmptyMetric_nameContext(p *Metric_nameContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_metric_name
}

func (*Metric_nameContext) IsMetric_nameContext() {}

func NewMetric_nameContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *Metric_nameContext {
	var p = new(Metric_nameContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = NexusParserRULE_metric_name

	return p
}

func (s *Metric_nameContext) GetParser() antlr.Parser { return s.parser }

func (s *Metric_nameContext) IDENTIFIER() antlr.TerminalNode {
	return s.GetToken(NexusParserIDENTIFIER, 0)
}

func (s *Metric_nameContext) STRING_LITERAL() antlr.TerminalNode {
	return s.GetToken(NexusParserSTRING_LITERAL, 0)
}

func (s *Metric_nameContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *Metric_nameContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *Metric_nameContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterMetric_name(s)
	}
}

func (s *Metric_nameContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitMetric_name(s)
	}
}

func (s *Metric_nameContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitMetric_name(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *NexusParser) Metric_name() (localctx IMetric_nameContext) {
	localctx = NewMetric_nameContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 26, NexusParserRULE_metric_name)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(209)
		_la = p.GetTokenStream().LA(1)

		if !(_la == NexusParserIDENTIFIER || _la == NexusParserSTRING_LITERAL) {
			p.GetErrorHandler().RecoverInline(p)
		} else {
			p.GetErrorHandler().ReportMatch(p)
			p.Consume()
		}
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// ITag_listContext is an interface to support dynamic dispatch.
type ITag_listContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	AllTag_assignment() []ITag_assignmentContext
	Tag_assignment(i int) ITag_assignmentContext

	// IsTag_listContext differentiates from other interfaces.
	IsTag_listContext()
}

type Tag_listContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyTag_listContext() *Tag_listContext {
	var p = new(Tag_listContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_tag_list
	return p
}

func InitEmptyTag_listContext(p *Tag_listContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_tag_list
}

func (*Tag_listContext) IsTag_listContext() {}

func NewTag_listContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *Tag_listContext {
	var p = new(Tag_listContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = NexusParserRULE_tag_list

	return p
}

func (s *Tag_listContext) GetParser() antlr.Parser { return s.parser }

func (s *Tag_listContext) AllTag_assignment() []ITag_assignmentContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(ITag_assignmentContext); ok {
			len++
		}
	}

	tst := make([]ITag_assignmentContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(ITag_assignmentContext); ok {
			tst[i] = t.(ITag_assignmentContext)
			i++
		}
	}

	return tst
}

func (s *Tag_listContext) Tag_assignment(i int) ITag_assignmentContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ITag_assignmentContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(ITag_assignmentContext)
}

func (s *Tag_listContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *Tag_listContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *Tag_listContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterTag_list(s)
	}
}

func (s *Tag_listContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitTag_list(s)
	}
}

func (s *Tag_listContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitTag_list(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *NexusParser) Tag_list() (localctx ITag_listContext) {
	localctx = NewTag_listContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 28, NexusParserRULE_tag_list)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(211)
		p.Match(NexusParserT__1)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(212)
		p.Tag_assignment()
	}
	p.SetState(217)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	for _la == NexusParserT__4 {
		{
			p.SetState(213)
			p.Match(NexusParserT__4)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(214)
			p.Tag_assignment()
		}

		p.SetState(219)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)
	}
	{
		p.SetState(220)
		p.Match(NexusParserT__2)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// ITag_assignmentContext is an interface to support dynamic dispatch.
type ITag_assignmentContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	Tag_value() ITag_valueContext
	IDENTIFIER() antlr.TerminalNode
	STRING_LITERAL() antlr.TerminalNode

	// IsTag_assignmentContext differentiates from other interfaces.
	IsTag_assignmentContext()
}

type Tag_assignmentContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyTag_assignmentContext() *Tag_assignmentContext {
	var p = new(Tag_assignmentContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_tag_assignment
	return p
}

func InitEmptyTag_assignmentContext(p *Tag_assignmentContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_tag_assignment
}

func (*Tag_assignmentContext) IsTag_assignmentContext() {}

func NewTag_assignmentContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *Tag_assignmentContext {
	var p = new(Tag_assignmentContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = NexusParserRULE_tag_assignment

	return p
}

func (s *Tag_assignmentContext) GetParser() antlr.Parser { return s.parser }

func (s *Tag_assignmentContext) Tag_value() ITag_valueContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ITag_valueContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ITag_valueContext)
}

func (s *Tag_assignmentContext) IDENTIFIER() antlr.TerminalNode {
	return s.GetToken(NexusParserIDENTIFIER, 0)
}

func (s *Tag_assignmentContext) STRING_LITERAL() antlr.TerminalNode {
	return s.GetToken(NexusParserSTRING_LITERAL, 0)
}

func (s *Tag_assignmentContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *Tag_assignmentContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *Tag_assignmentContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterTag_assignment(s)
	}
}

func (s *Tag_assignmentContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitTag_assignment(s)
	}
}

func (s *Tag_assignmentContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitTag_assignment(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *NexusParser) Tag_assignment() (localctx ITag_assignmentContext) {
	localctx = NewTag_assignmentContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 30, NexusParserRULE_tag_assignment)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(222)
		_la = p.GetTokenStream().LA(1)

		if !(_la == NexusParserIDENTIFIER || _la == NexusParserSTRING_LITERAL) {
			p.GetErrorHandler().RecoverInline(p)
		} else {
			p.GetErrorHandler().ReportMatch(p)
			p.Consume()
		}
	}
	{
		p.SetState(223)
		p.Match(NexusParserT__3)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(224)
		p.Tag_value()
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// ITag_valueContext is an interface to support dynamic dispatch.
type ITag_valueContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	STRING_LITERAL() antlr.TerminalNode

	// IsTag_valueContext differentiates from other interfaces.
	IsTag_valueContext()
}

type Tag_valueContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyTag_valueContext() *Tag_valueContext {
	var p = new(Tag_valueContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_tag_value
	return p
}

func InitEmptyTag_valueContext(p *Tag_valueContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_tag_value
}

func (*Tag_valueContext) IsTag_valueContext() {}

func NewTag_valueContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *Tag_valueContext {
	var p = new(Tag_valueContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = NexusParserRULE_tag_value

	return p
}

func (s *Tag_valueContext) GetParser() antlr.Parser { return s.parser }

func (s *Tag_valueContext) STRING_LITERAL() antlr.TerminalNode {
	return s.GetToken(NexusParserSTRING_LITERAL, 0)
}

func (s *Tag_valueContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *Tag_valueContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *Tag_valueContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterTag_value(s)
	}
}

func (s *Tag_valueContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitTag_value(s)
	}
}

func (s *Tag_valueContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitTag_value(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *NexusParser) Tag_value() (localctx ITag_valueContext) {
	localctx = NewTag_valueContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 32, NexusParserRULE_tag_value)
	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(226)
		p.Match(NexusParserSTRING_LITERAL)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IField_listContext is an interface to support dynamic dispatch.
type IField_listContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	AllField_assignment() []IField_assignmentContext
	Field_assignment(i int) IField_assignmentContext

	// IsField_listContext differentiates from other interfaces.
	IsField_listContext()
}

type Field_listContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyField_listContext() *Field_listContext {
	var p = new(Field_listContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_field_list
	return p
}

func InitEmptyField_listContext(p *Field_listContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_field_list
}

func (*Field_listContext) IsField_listContext() {}

func NewField_listContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *Field_listContext {
	var p = new(Field_listContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = NexusParserRULE_field_list

	return p
}

func (s *Field_listContext) GetParser() antlr.Parser { return s.parser }

func (s *Field_listContext) AllField_assignment() []IField_assignmentContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IField_assignmentContext); ok {
			len++
		}
	}

	tst := make([]IField_assignmentContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IField_assignmentContext); ok {
			tst[i] = t.(IField_assignmentContext)
			i++
		}
	}

	return tst
}

func (s *Field_listContext) Field_assignment(i int) IField_assignmentContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IField_assignmentContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IField_assignmentContext)
}

func (s *Field_listContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *Field_listContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *Field_listContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterField_list(s)
	}
}

func (s *Field_listContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitField_list(s)
	}
}

func (s *Field_listContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitField_list(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *NexusParser) Field_list() (localctx IField_listContext) {
	localctx = NewField_listContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 34, NexusParserRULE_field_list)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(228)
		p.Match(NexusParserT__1)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(229)
		p.Field_assignment()
	}
	p.SetState(234)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_la = p.GetTokenStream().LA(1)

	for _la == NexusParserT__4 {
		{
			p.SetState(230)
			p.Match(NexusParserT__4)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(231)
			p.Field_assignment()
		}

		p.SetState(236)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)
	}
	{
		p.SetState(237)
		p.Match(NexusParserT__2)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IField_assignmentContext is an interface to support dynamic dispatch.
type IField_assignmentContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	IDENTIFIER() antlr.TerminalNode
	Literal_value() ILiteral_valueContext

	// IsField_assignmentContext differentiates from other interfaces.
	IsField_assignmentContext()
}

type Field_assignmentContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyField_assignmentContext() *Field_assignmentContext {
	var p = new(Field_assignmentContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_field_assignment
	return p
}

func InitEmptyField_assignmentContext(p *Field_assignmentContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_field_assignment
}

func (*Field_assignmentContext) IsField_assignmentContext() {}

func NewField_assignmentContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *Field_assignmentContext {
	var p = new(Field_assignmentContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = NexusParserRULE_field_assignment

	return p
}

func (s *Field_assignmentContext) GetParser() antlr.Parser { return s.parser }

func (s *Field_assignmentContext) IDENTIFIER() antlr.TerminalNode {
	return s.GetToken(NexusParserIDENTIFIER, 0)
}

func (s *Field_assignmentContext) Literal_value() ILiteral_valueContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ILiteral_valueContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ILiteral_valueContext)
}

func (s *Field_assignmentContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *Field_assignmentContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *Field_assignmentContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterField_assignment(s)
	}
}

func (s *Field_assignmentContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitField_assignment(s)
	}
}

func (s *Field_assignmentContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitField_assignment(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *NexusParser) Field_assignment() (localctx IField_assignmentContext) {
	localctx = NewField_assignmentContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 36, NexusParserRULE_field_assignment)
	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(239)
		p.Match(NexusParserIDENTIFIER)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(240)
		p.Match(NexusParserT__3)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}
	{
		p.SetState(241)
		p.Literal_value()
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// ITimestampContext is an interface to support dynamic dispatch.
type ITimestampContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser
	// IsTimestampContext differentiates from other interfaces.
	IsTimestampContext()
}

type TimestampContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyTimestampContext() *TimestampContext {
	var p = new(TimestampContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_timestamp
	return p
}

func InitEmptyTimestampContext(p *TimestampContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_timestamp
}

func (*TimestampContext) IsTimestampContext() {}

func NewTimestampContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *TimestampContext {
	var p = new(TimestampContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = NexusParserRULE_timestamp

	return p
}

func (s *TimestampContext) GetParser() antlr.Parser { return s.parser }

func (s *TimestampContext) CopyAll(ctx *TimestampContext) {
	s.CopyFrom(&ctx.BaseParserRuleContext)
}

func (s *TimestampContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *TimestampContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type TimestampNowContext struct {
	TimestampContext
}

func NewTimestampNowContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *TimestampNowContext {
	var p = new(TimestampNowContext)

	InitEmptyTimestampContext(&p.TimestampContext)
	p.parser = parser
	p.CopyAll(ctx.(*TimestampContext))

	return p
}

func (s *TimestampNowContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *TimestampNowContext) K_NOW() antlr.TerminalNode {
	return s.GetToken(NexusParserK_NOW, 0)
}

func (s *TimestampNowContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterTimestampNow(s)
	}
}

func (s *TimestampNowContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitTimestampNow(s)
	}
}

func (s *TimestampNowContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitTimestampNow(s)

	default:
		return t.VisitChildren(s)
	}
}

type TimestampLiteralContext struct {
	TimestampContext
}

func NewTimestampLiteralContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *TimestampLiteralContext {
	var p = new(TimestampLiteralContext)

	InitEmptyTimestampContext(&p.TimestampContext)
	p.parser = parser
	p.CopyAll(ctx.(*TimestampContext))

	return p
}

func (s *TimestampLiteralContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *TimestampLiteralContext) NUMBER() antlr.TerminalNode {
	return s.GetToken(NexusParserNUMBER, 0)
}

func (s *TimestampLiteralContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterTimestampLiteral(s)
	}
}

func (s *TimestampLiteralContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitTimestampLiteral(s)
	}
}

func (s *TimestampLiteralContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitTimestampLiteral(s)

	default:
		return t.VisitChildren(s)
	}
}

type TimestampDateTimeContext struct {
	TimestampContext
}

func NewTimestampDateTimeContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *TimestampDateTimeContext {
	var p = new(TimestampDateTimeContext)

	InitEmptyTimestampContext(&p.TimestampContext)
	p.parser = parser
	p.CopyAll(ctx.(*TimestampContext))

	return p
}

func (s *TimestampDateTimeContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *TimestampDateTimeContext) K_DT() antlr.TerminalNode {
	return s.GetToken(NexusParserK_DT, 0)
}

func (s *TimestampDateTimeContext) STRING_LITERAL() antlr.TerminalNode {
	return s.GetToken(NexusParserSTRING_LITERAL, 0)
}

func (s *TimestampDateTimeContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterTimestampDateTime(s)
	}
}

func (s *TimestampDateTimeContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitTimestampDateTime(s)
	}
}

func (s *TimestampDateTimeContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitTimestampDateTime(s)

	default:
		return t.VisitChildren(s)
	}
}

type TimestampNowRelativeContext struct {
	TimestampContext
}

func NewTimestampNowRelativeContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *TimestampNowRelativeContext {
	var p = new(TimestampNowRelativeContext)

	InitEmptyTimestampContext(&p.TimestampContext)
	p.parser = parser
	p.CopyAll(ctx.(*TimestampContext))

	return p
}

func (s *TimestampNowRelativeContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *TimestampNowRelativeContext) K_NOW() antlr.TerminalNode {
	return s.GetToken(NexusParserK_NOW, 0)
}

func (s *TimestampNowRelativeContext) DURATION_LITERAL() antlr.TerminalNode {
	return s.GetToken(NexusParserDURATION_LITERAL, 0)
}

func (s *TimestampNowRelativeContext) PLUS() antlr.TerminalNode {
	return s.GetToken(NexusParserPLUS, 0)
}

func (s *TimestampNowRelativeContext) MINUS() antlr.TerminalNode {
	return s.GetToken(NexusParserMINUS, 0)
}

func (s *TimestampNowRelativeContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterTimestampNowRelative(s)
	}
}

func (s *TimestampNowRelativeContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitTimestampNowRelative(s)
	}
}

func (s *TimestampNowRelativeContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitTimestampNowRelative(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *NexusParser) Timestamp() (localctx ITimestampContext) {
	localctx = NewTimestampContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 38, NexusParserRULE_timestamp)
	var _la int

	p.SetState(256)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 27, p.GetParserRuleContext()) {
	case 1:
		localctx = NewTimestampLiteralContext(p, localctx)
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(243)
			p.Match(NexusParserNUMBER)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 2:
		localctx = NewTimestampNowContext(p, localctx)
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(244)
			p.Match(NexusParserK_NOW)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(245)
			p.Match(NexusParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(246)
			p.Match(NexusParserT__2)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 3:
		localctx = NewTimestampNowRelativeContext(p, localctx)
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(247)
			p.Match(NexusParserK_NOW)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(248)
			p.Match(NexusParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(249)
			_la = p.GetTokenStream().LA(1)

			if !(_la == NexusParserPLUS || _la == NexusParserMINUS) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}
		{
			p.SetState(250)
			p.Match(NexusParserDURATION_LITERAL)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(251)
			p.Match(NexusParserT__2)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case 4:
		localctx = NewTimestampDateTimeContext(p, localctx)
		p.EnterOuterAlt(localctx, 4)
		{
			p.SetState(252)
			p.Match(NexusParserK_DT)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(253)
			p.Match(NexusParserT__1)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(254)
			p.Match(NexusParserSTRING_LITERAL)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(255)
			p.Match(NexusParserT__2)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case antlr.ATNInvalidAltNumber:
		goto errorExit
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IDurationContext is an interface to support dynamic dispatch.
type IDurationContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	DURATION_LITERAL() antlr.TerminalNode

	// IsDurationContext differentiates from other interfaces.
	IsDurationContext()
}

type DurationContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyDurationContext() *DurationContext {
	var p = new(DurationContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_duration
	return p
}

func InitEmptyDurationContext(p *DurationContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_duration
}

func (*DurationContext) IsDurationContext() {}

func NewDurationContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *DurationContext {
	var p = new(DurationContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = NexusParserRULE_duration

	return p
}

func (s *DurationContext) GetParser() antlr.Parser { return s.parser }

func (s *DurationContext) DURATION_LITERAL() antlr.TerminalNode {
	return s.GetToken(NexusParserDURATION_LITERAL, 0)
}

func (s *DurationContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *DurationContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *DurationContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterDuration(s)
	}
}

func (s *DurationContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitDuration(s)
	}
}

func (s *DurationContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitDuration(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *NexusParser) Duration() (localctx IDurationContext) {
	localctx = NewDurationContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 40, NexusParserRULE_duration)
	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(258)
		p.Match(NexusParserDURATION_LITERAL)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IValueContext is an interface to support dynamic dispatch.
type IValueContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	NUMBER() antlr.TerminalNode

	// IsValueContext differentiates from other interfaces.
	IsValueContext()
}

type ValueContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyValueContext() *ValueContext {
	var p = new(ValueContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_value
	return p
}

func InitEmptyValueContext(p *ValueContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_value
}

func (*ValueContext) IsValueContext() {}

func NewValueContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *ValueContext {
	var p = new(ValueContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = NexusParserRULE_value

	return p
}

func (s *ValueContext) GetParser() antlr.Parser { return s.parser }

func (s *ValueContext) NUMBER() antlr.TerminalNode {
	return s.GetToken(NexusParserNUMBER, 0)
}

func (s *ValueContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ValueContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *ValueContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterValue(s)
	}
}

func (s *ValueContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitValue(s)
	}
}

func (s *ValueContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitValue(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *NexusParser) Value() (localctx IValueContext) {
	localctx = NewValueContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 42, NexusParserRULE_value)
	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(260)
		p.Match(NexusParserNUMBER)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// ILiteral_valueContext is an interface to support dynamic dispatch.
type ILiteral_valueContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	NUMBER() antlr.TerminalNode
	STRING_LITERAL() antlr.TerminalNode
	K_TRUE() antlr.TerminalNode
	K_FALSE() antlr.TerminalNode
	K_NULL() antlr.TerminalNode

	// IsLiteral_valueContext differentiates from other interfaces.
	IsLiteral_valueContext()
}

type Literal_valueContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyLiteral_valueContext() *Literal_valueContext {
	var p = new(Literal_valueContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_literal_value
	return p
}

func InitEmptyLiteral_valueContext(p *Literal_valueContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = NexusParserRULE_literal_value
}

func (*Literal_valueContext) IsLiteral_valueContext() {}

func NewLiteral_valueContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *Literal_valueContext {
	var p = new(Literal_valueContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = NexusParserRULE_literal_value

	return p
}

func (s *Literal_valueContext) GetParser() antlr.Parser { return s.parser }

func (s *Literal_valueContext) NUMBER() antlr.TerminalNode {
	return s.GetToken(NexusParserNUMBER, 0)
}

func (s *Literal_valueContext) STRING_LITERAL() antlr.TerminalNode {
	return s.GetToken(NexusParserSTRING_LITERAL, 0)
}

func (s *Literal_valueContext) K_TRUE() antlr.TerminalNode {
	return s.GetToken(NexusParserK_TRUE, 0)
}

func (s *Literal_valueContext) K_FALSE() antlr.TerminalNode {
	return s.GetToken(NexusParserK_FALSE, 0)
}

func (s *Literal_valueContext) K_NULL() antlr.TerminalNode {
	return s.GetToken(NexusParserK_NULL, 0)
}

func (s *Literal_valueContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *Literal_valueContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *Literal_valueContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.EnterLiteral_value(s)
	}
}

func (s *Literal_valueContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NexusListener); ok {
		listenerT.ExitLiteral_value(s)
	}
}

func (s *Literal_valueContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case NexusVisitor:
		return t.VisitLiteral_value(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *NexusParser) Literal_value() (localctx ILiteral_valueContext) {
	localctx = NewLiteral_valueContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 44, NexusParserRULE_literal_value)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(262)
		_la = p.GetTokenStream().LA(1)

		if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&22518479173189632) != 0) {
			p.GetErrorHandler().RecoverInline(p)
		} else {
			p.GetErrorHandler().ReportMatch(p)
			p.Consume()
		}
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}
