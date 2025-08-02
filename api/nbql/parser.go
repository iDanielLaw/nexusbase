package nbql

import (
	"fmt"

	//go:generate java -jar /data/antlr-4.13.2-complete.jar -Dlanguage=Go -o ./parser -visitor -package parser ./Nexus.g4

	"github.com/INLOpen/nexusbase/api/nbql/parser"
	"github.com/INLOpen/nexusbase/utils"
	"github.com/antlr4-go/antlr/v4"
)

// Parse takes an NBQL string and returns the corresponding AST Command.
func Parse(input string) (Command, error) {
	return ParseWithClock(input, &utils.SystemClock{})
}

func ParseWithClock(input string, clock utils.Clock) (Command, error) {
	// Create the ANTLR input stream
	is := antlr.NewInputStream(input)

	// Create the Lexer
	lexer := parser.NewNexusLexer(is)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

	// Create the Parser
	p := parser.NewNexusParser(stream)

	// Add a custom error listener
	errorListener := &NBQLErrorListener{}
	p.RemoveErrorListeners() // Remove default listener
	p.AddErrorListener(errorListener)

	// Start parsing from the 'statement' rule
	tree := p.Statement()

	if len(errorListener.errors) > 0 {
		return nil, fmt.Errorf("parsing failed: %s", errorListener.errors[0])
	}

	// Create our custom visitor to build the AST
	visitor := NewASTBuilder(clock)
	ast := visitor.Visit(tree)

	// After visiting, check if the visitor encountered any semantic errors (e.g., bad number format).
	visitorErrors := visitor.Errors()
	if len(visitorErrors) > 0 {
		return nil, visitorErrors[0] // Return the first semantic error encountered
	}
	cmd, ok := ast.(Command)
	if !ok {
		return nil, fmt.Errorf("parsed statement is not a valid command")
	}

	return cmd, nil
}

// NBQLErrorListener captures syntax errors from the parser.
type NBQLErrorListener struct {
	*antlr.DefaultErrorListener
	errors []string
}

func (l *NBQLErrorListener) SyntaxError(recognizer antlr.Recognizer, offendingSymbol interface{}, line, column int, msg string, e antlr.RecognitionException) {
	l.errors = append(l.errors, fmt.Sprintf("line %d:%d %s", line, column, msg))
}
