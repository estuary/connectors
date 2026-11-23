package connector

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// partitionByPunctuation is what an expression may hold outside of quotes
// besides names, numbers, whitespace and parentheses.
const partitionByPunctuation = "[],.+-*/%=<>!:?|"

// validatePartitionBy rejects a partition_by that would not stay an expression
// list inside the parentheses createTargetTable renders it into as-is. Whether
// it is a usable partition key is left to ClickHouse, except for aliases: it
// accepts them, then cannot insert into the table or re-read its definition.
func validatePartitionBy(expr string) error {
	// One entry per open parenthesis: whether it is CAST's, the one place an
	// AS is not an alias.
	var parens []bool
	var prevWord string

	for i := 0; i < len(expr); {
		var c = expr[i]
		if isWordByte(c) {
			var start = i
			for i < len(expr) && isWordByte(expr[i]) {
				i++
			}
			prevWord = expr[start:i]
			if strings.EqualFold(prevWord, "select") {
				return errors.New("must not contain a query")
			} else if strings.EqualFold(prevWord, "as") && (len(parens) == 0 || !parens[len(parens)-1]) {
				return errors.New("must not contain an alias")
			}
			continue
		} else if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			i++
			continue
		}

		switch {
		case c == '\'' || c == '"' || c == '`':
			if i = quotedEnd(expr, i); i < 0 {
				return errors.New("has an unterminated quote")
			}
		case c == '(':
			parens = append(parens, strings.EqualFold(prevWord, "cast"))
			i++
		case c == ')':
			if len(parens) == 0 {
				return errors.New("has unbalanced parentheses")
			}
			parens = parens[:len(parens)-1]
			i++
		case strings.HasPrefix(expr[i:], "--") || strings.HasPrefix(expr[i:], "/*"):
			return errors.New("must not contain comments")
		case strings.IndexByte(partitionByPunctuation, c) >= 0:
			i++
		default:
			r, _ := utf8.DecodeRuneInString(expr[i:])
			return fmt.Errorf("must not contain %q outside of a quoted name or string", r)
		}
		prevWord = ""
	}
	if len(parens) != 0 {
		return errors.New("has unbalanced parentheses")
	}
	return nil
}

func isWordByte(c byte) bool {
	return c == '_' || '0' <= c && c <= '9' || 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z'
}

// quotedEnd returns the index just past the quoted string or name opening at
// expr[start], or -1 if it is unterminated. As in ClickHouse's lexer, a
// backslash escapes the next byte and a doubled quote does not close.
func quotedEnd(expr string, start int) int {
	var quote = expr[start]
	for i := start + 1; i < len(expr); i++ {
		switch expr[i] {
		case '\\':
			i++
		case quote:
			if i+1 < len(expr) && expr[i+1] == quote {
				i++
				continue
			}
			return i + 1
		}
	}
	return -1
}

// partitionClauseExpr is the partitionExpr to render into a CREATE TABLE.
func partitionClauseExpr(rawResourceConfig json.RawMessage) (string, error) {
	expr, err := partitionExpr(rawResourceConfig)
	if err != nil || expr == "" {
		return "", err
	}
	if err := validatePartitionBy(expr); err != nil {
		return "", fmt.Errorf("'partition_by' expression %q is not valid: it %w", expr, err)
	}
	return expr, nil
}
