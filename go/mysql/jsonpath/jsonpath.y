%{

package jsonpath

import (
	"fmt"
    "regexp"
    "strconv"
)

%}

%union {
	path      Path
	pathElem  PathElem
	pathElems []PathElem
	str       string
	num       int
}

%token <str> IDENTIFIER QUOTED_STRING
%token <num> INTEGER
%token DOT LBRACKET RBRACKET

%type <path> pathExpression
%type <pathElems> pathLegs
%type <pathElem> pathLeg member arrayLocation
%type <str> keyName

%%

pathExpression:
	/* empty */
	{
		yylex.(*yyLex).path = Path{}
	}
|	pathLegs
	{
		yylex.(*yyLex).path = Path($1)
	}

pathLegs:
	pathLeg
	{
		$$ = []PathElem{$1}
	}
|	pathLegs pathLeg
	{
		$$ = append($1, $2)
	}

pathLeg:
	member
	{
		$$ = $1
	}
|	arrayLocation
	{
		$$ = $1
	}

member:
	DOT keyName
	{
		$$ = PathElemProperty{Name: $2}
	}

arrayLocation:
	LBRACKET INTEGER RBRACKET
	{
		$$ = PathElemIndex{Index: $2}
	}

keyName:
	IDENTIFIER
	{
		$$ = $1
	}
|	QUOTED_STRING
	{
		$$ = $1
	}

%%

func init() {
    yyErrorVerbose = true
}

const eof = 0

type yyLex struct {
	text    string // Input text to be lexed
	off     int    // The current byte offset into the input
	lastErr error  // The most recent error encountered, if any
    path    Path   // The final path value after parsing
}

func (l *yyLex) Lex(lval *yySymType) int {
    if l.off >= len(l.text) {
        return eof
    }
    var tokType, tokSize, err = matchToken(l.text[l.off:])
    if err != nil {
        l.lastErr = err
        return eof
    }

    var tokValue = l.text[l.off:l.off+tokSize]
    switch tokType {
    case IDENTIFIER:
        lval.str = tokValue
    case QUOTED_STRING:
        // Strip quotes and unescape the string. MySQL doesn't specify the full grammar
        // of legal string escapes and the Go string escape syntax is close enough to
        // the JavaScript one (which is the obvious assumption for JSON paths?) that we
        // can probably get away with this.
        var str, err = strconv.Unquote(tokValue)
        if err != nil {
            l.lastErr = fmt.Errorf("")
            return eof
        }
        lval.str = str
    case INTEGER:
        lval.num, _ = strconv.Atoi(tokValue)
    }

    l.off += tokSize
    return tokType
}

func (l *yyLex) Error(err string) {
    l.lastErr = fmt.Errorf("parse error: %s", err)
}

type tokenMatcher struct {
    Type   int            // The token type
    Regexp *regexp.Regexp // The token match regexp
}

func matchToken(text string) (tokType int, tokSize int, err error) {
    for _, m := range tokenMatchers {
        if loc := m.Regexp.FindStringIndex(text); loc != nil && loc[0] == 0 {
            return m.Type, loc[1], nil
        }
    }
    return 0, 0, fmt.Errorf("invalid token")
}

var tokenMatchers = []tokenMatcher{
    {Type: DOT, Regexp: regexp.MustCompile(`\.`)},
    {Type: LBRACKET, Regexp: regexp.MustCompile(`\[`)},
    {Type: RBRACKET, Regexp: regexp.MustCompile(`\]`)},
    {Type: INTEGER, Regexp: regexp.MustCompile(`[0-9]+`)},
    // ECMAScript identifier: starts with letter, $, or _, followed by letters, digits, $, or _
    {Type: IDENTIFIER, Regexp: regexp.MustCompile(`[a-zA-Z_$][a-zA-Z0-9_$]*`)},
    // Double-quoted string: handles escaped characters including \" and \\
    {Type: QUOTED_STRING, Regexp: regexp.MustCompile(`"(?:[^"\\]|\\.)*"`)},
}
