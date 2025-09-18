package jsonpath

import (
	"fmt"
	"strings"
)

type Path []PathElem

func (p Path) String() string {
	var buf = new(strings.Builder)
	buf.WriteString("$")
	for _, e := range p {
		buf.WriteString(e.String())
	}
	return buf.String()
}

type PathElem interface {
	isPathElem()

	String() string
}

type PathElemProperty struct {
	Name string
}

func (PathElemProperty) isPathElem() {}

func (e PathElemProperty) String() string {
	return fmt.Sprintf(".%q", e.Name)
}

type PathElemIndex struct {
	Index int
}

func (PathElemIndex) isPathElem() {}

func (e PathElemIndex) String() string {
	return fmt.Sprintf("[%d]", e.Index)
}

// Parse parses a MySQL JSON path expression using the yacc parser.
//
//go:generate goyacc -o jsonpath.go jsonpath.y
func Parse(text string) (Path, error) {
	// Trim leading $ if present
	text = strings.TrimPrefix(text, "$")

	var lexer = &yyLex{text: text}
	yyParse(lexer)
	if lexer.lastErr != nil {
		return nil, lexer.lastErr
	}
	return lexer.path, nil
}
