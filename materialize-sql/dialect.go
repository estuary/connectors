package sql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Dialect encapsulates many specifics of an Endpoint's interpretation of SQL.
type Dialect struct {
	TableLocatorer
	ColumnLocatorer
	Identifierer
	Literaler
	Placeholderer
	TypeMapper

	// MaxFieldCharLength is provided as the "maxFieldLength" parameter to boilerplate.NewValidator.
	MaxColumnCharLength int
	// CaseInsensitiveColumns is provided as the "caseInsensitiveFields" parameter to boilerplate.NewValidator.
	CaseInsensitiveColumns bool

	// MigratableTypes is a map of current column DDL as key, and a slice of DDLs
	// which the key type can be migrated to.
	// For example, "decimal": {"string"} means decimal columns can be migrated to string type
	MigratableTypes MigrationSpecs
}

// TableLocatorer produces an InfoTableLocation for a given path.
type TableLocatorer interface {
	TableLocator(path []string) InfoTableLocation
}

// InfoTableLocation represents how to find a table in the INFORMATION_SCHEMA view for the endpoint.
// INFORMATION_SCHEMA has a "table_schema" and "table_name" column, and those columns correspond to
// the TableSchema and TableName properties here. They should _not_ be quoted, but _should_ have any
// connector-specific transforms applied. For example, in Redshift every identifier is lower-case in
// INFORMATION_SCHEMA, so materialize-redshift's dialect must provide these values in their
// lower-case form.
//
// The TableSchema is often part of the binding's resource path, but not always. Some
// materializations do not require or support specifying an explicit schema for a binding, and in
// these cases it is up to the dialect to produce an appropriate value for TableSchema, usually
// based on some default of the database.
type InfoTableLocation struct {
	TableSchema string
	TableName   string
}

// ColumnLocatorer translates a field name from a Flow collection spec into the value used to locate
// that field in the INFORMATION_SCHEMA view for the endpoint. This is similar to how an
// InfoTableLocation must apply connector-specific transforms for the TableSchema and TableName.
type ColumnLocatorer interface {
	ColumnLocator(field string) string
}

// Identifierer takes path components and returns a raw SQL identifier for the
// Endpoint with necessary quoting applied.
type Identifierer interface {
	Identifier(path ...string) string
}

// Literaler takes a string or integer and returns a raw SQL literal for the
// Endpoint with required quoting and escaping already applied.
type Literaler interface {
	Literal(in any) string
}

// Placeholderer returns the appropriate Endpoint placeholder representation
// for a parameter at the given zero-offset index.
type Placeholderer interface {
	Placeholder(index int) string
}

// A TypeMapper is a function that maps a Projection into a MappedType.
// For example, it might map a `string` Projection into a `TEXT` SQL type.
type TypeMapper interface {
	MapType(*Projection) (MappedType, error)
}

// TableLocatorFn is a function that implements TableLocatorer.
type TableLocatorFn func(path []string) InfoTableLocation

func (f TableLocatorFn) TableLocator(path []string) InfoTableLocation { return f(path) }

// ColumnLocatorFn is a function that implements ColumnLocatorer.
type ColumnLocatorFn func(field string) string

func (f ColumnLocatorFn) ColumnLocator(field string) string { return f(field) }

// IdentifierFn is a function that implements Identifierer.
type IdentifierFn func(path ...string) string

func (f IdentifierFn) Identifier(path ...string) string { return f(path...) }

// LiteralFn is a function that implements Literaler.
type LiteralFn func(in any) string

func (f LiteralFn) Literal(in any) string { return f(in) }

// ToLiteralFn builds a LiteralFn from a string handler to apply quoting and
// escaping to strings. Integer literals are rendered without any quoting.
func ToLiteralFn(fn func(s string) string) LiteralFn {
	return func(in any) string {
		switch v := in.(type) {
		case string:
			return fn(v)
		case int64:
			return strconv.Itoa(int(v))
		case uint64:
			return strconv.FormatUint(v, 10)
		case float64: // numbers with a 0 decimal part like "1.0", which are considered integers
			return strconv.FormatFloat(v, 'f', -1, 64)
		default:
			panic(fmt.Sprintf("unhandled literal type %T (value: %v)", in, in))
		}
	}
}

// PlaceholderFn is a function that implements Placeholderer.
type PlaceholderFn func(index int) string

func (f PlaceholderFn) Placeholder(index int) string { return f(index) }

// TypeMapperFn is a function that implements TypeMapper.
type TypeMapperFn func(*Projection) (MappedType, error)

func (f TypeMapperFn) MapType(p *Projection) (MappedType, error) { return f(p) }

// Compile-time check that wrapping functions with typed  Fn() implementations
// can be used to build a Dialect.
var _ = Dialect{
	TableLocatorer:  TableLocatorFn(func(path []string) InfoTableLocation { return InfoTableLocation{} }),
	ColumnLocatorer: ColumnLocatorFn(func(field string) string { return field }),
	Placeholderer:   PlaceholderFn(func(index int) string { return "" }),
	Literaler:       ToLiteralFn(func(s string) string { return "" }),
	Identifierer:    IdentifierFn(func(path ...string) string { return "" }),
	TypeMapper:      DDLMapper{},
}

// PassThroughTransform returns a function that evaluates `if_` over its input
// and, if true, returns its input unmodified. Otherwise it returns the
// result of `else_` over its input.
func PassThroughTransform(
	if_ func(string) bool,
	else_ func(string) string,
) func(string) string {
	return func(s string) string {
		if if_(s) {
			return s
		}
		return else_(s)
	}
}

// IsSimpleIdentifier returns true on identifier components that typically do not need quoting:
// strings having only lower-case letters, decimal numbers, and underscore.
var IsSimpleIdentifier = regexp.MustCompile(`^[_\p{Ll}]+[_\p{Ll}\p{Nd}]*$`).MatchString

// QuoteTransform returns a function that wraps its input with `quote` on either side,
// and replaces all occurrences of `quote` _within_ the string with `escape`.
func QuoteTransform(quote string, escape string) func(string) string {
	return func(s string) string {
		return quote + strings.ReplaceAll(s, quote, escape) + quote
	}
}

// QuoteTransformEscapedBackslash first escapes any backslashes in the input
// with a second backslash, then applies the transform per QuoteTransform. This
// is common for systems that use a backslash as an escape character and require
// it to be escaped as well.
func QuoteTransformEscapedBackslash(quote string, escape string) func(string) string {
	return func(s string) string {
		return QuoteTransform(quote, escape)(strings.ReplaceAll(s, `\`, `\\`))
	}
}

// JoinTransform returns a function that takes component `parts` and processes
// each through the `delegate` transform.
// Then, it joins their results around `joiner`.
func JoinTransform(
	joiner string,
	delegate func(string) string,
) func(parts ...string) string {
	return func(parts ...string) string {
		var cp = make([]string, len(parts))
		for i := range parts {
			cp[i] = delegate(parts[i])
		}
		return strings.Join(cp, joiner)
	}
}
