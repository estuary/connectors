// Package tableglob provides glob-style pattern matching against tuples
// of (schema, table) names. Patterns are compiled to regular expressions
// up front so they can be applied to large lists of table names.
package tableglob

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

// InvalidPatternError is returned by Compile when a pattern is malformed.
type InvalidPatternError struct {
	Pattern string
	Cause   error
}

func (e *InvalidPatternError) Error() string {
	return fmt.Sprintf("invalid table pattern %q: %s", e.Pattern, e.Cause)
}

func (e *InvalidPatternError) Unwrap() error { return e.Cause }

// Pattern is a compiled table-name glob pattern.
type Pattern struct {
	// table is the compiled regexp for the table-name component of the
	// pattern. Always non-nil.
	table *regexp.Regexp
	// schema is the compiled regexp for the schema-name component, or nil
	// if the pattern is unqualified (no unescaped '.').
	schema *regexp.Regexp
}

// Compile parses a glob-style table pattern and returns the compiled form.
// Returns an *InvalidPatternError if the pattern is malformed.
//
// Single-component syntax:
//
//   - '*' matches any run of characters (including the empty string)
//   - '?' matches a single character
//   - '\c' matches the character c literally, allowing the meta characters
//     '*', '?', '\', and '.' to appear in a pattern (so '\*' matches a
//     literal '*' and '\.' matches a literal '.')
//
// All other characters in the pattern match themselves. A trailing '\'
// with no following character is malformed.
//
// Schema/table split: if the pattern contains an unescaped '.', that
// period separates the schema component from the table component. The
// part before the '.' matches against the schema name and the part after
// matches against the table name. A pattern with no unescaped '.' is
// unqualified and matches against just the table name (the schema is
// ignored). At most one unescaped '.' is allowed, and neither resulting
// component may be empty.
//
// Note that this means '\.' continues to mean "literal period inside a
// name component" in qualified patterns, the same role it plays in the
// unqualified single-component case. Examples:
//
//	"foo"          matches table "foo" in any schema
//	"myschema.foo" matches table "foo" in schema "myschema"
//	"*.users"      matches table "users" in any schema
//	"app_*"        matches any table whose name starts with "app_"
//	"foo\.bar"     matches table "foo.bar" (literal period) in any schema
//	"ms.foo\.bar"  matches table "foo.bar" in schema "ms"
func Compile(pattern string) (*Pattern, error) {
	var schemaRE, tableRE, err = compileGlob(pattern)
	if err != nil {
		return nil, &InvalidPatternError{Pattern: pattern, Cause: err}
	}
	return &Pattern{schema: schemaRE, table: tableRE}, nil
}

// MatchTable reports whether the (schema, table) pair matches the pattern.
// If the pattern is unqualified, the schema argument is ignored.
func (p *Pattern) MatchTable(schema, table string) bool {
	if p.schema != nil && !p.schema.MatchString(schema) {
		return false
	}
	return p.table.MatchString(table)
}

// compileGlob translates a glob pattern into one or two anchored compiled
// regular expressions. If the pattern contains a single unescaped '.',
// returns (schemaRE, tableRE, nil) for use as a qualified matcher.
// Otherwise returns (nil, tableRE, nil) for use as an unqualified
// table-name matcher.
func compileGlob(glob string) (schemaRE, tableRE *regexp.Regexp, err error) {
	var b strings.Builder
	var partStart int

	b.WriteByte('^')
	for i := 0; i < len(glob); {
		// Scan a literal run and flush it through QuoteMeta.
		var lit = i
		for lit < len(glob) {
			var c = glob[lit]
			if c == '*' || c == '?' || c == '\\' || c == '.' {
				break
			}
			lit++
		}
		if lit > i {
			b.WriteString(regexp.QuoteMeta(glob[i:lit]))
			i = lit
		}
		if i >= len(glob) {
			break
		}

		switch glob[i] {
		case '*':
			b.WriteString(`.*`)
			i++
		case '?':
			b.WriteByte('.')
			i++
		case '\\':
			if i+1 >= len(glob) {
				return nil, nil, fmt.Errorf("trailing backslash")
			}
			// Consume a full rune so multi-byte UTF-8 escapes (e.g. '\é')
			// are preserved literally.
			var _, sz = utf8.DecodeRuneInString(glob[i+1:])
			b.WriteString(regexp.QuoteMeta(glob[i+1 : i+1+sz]))
			i += 1 + sz
		case '.':
			if schemaRE != nil {
				return nil, nil, fmt.Errorf("more than one unescaped '.'")
			}
			if i == partStart {
				return nil, nil, fmt.Errorf("empty schema component")
			}
			b.WriteByte('$')
			schemaRE, err = regexp.Compile(b.String())
			if err != nil {
				return nil, nil, err
			}
			b.Reset()
			b.WriteByte('^')
			partStart = i + 1
			i++
		}
	}

	if len(glob) == partStart {
		if schemaRE != nil {
			return nil, nil, fmt.Errorf("empty table component")
		}
		return nil, nil, fmt.Errorf("empty pattern")
	}
	b.WriteByte('$')
	tableRE, err = regexp.Compile(b.String())
	if err != nil {
		return nil, nil, err
	}
	return schemaRE, tableRE, nil
}
