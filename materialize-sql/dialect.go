package sql

import (
	"regexp"
	"strings"
)

// Dialect encapsulates many specifics of an Endpoint's interpretation of SQL.
type Dialect struct {
	Identifierer
	Literaler
	Placeholderer
	TypeMapper
	AlwaysNullableTypeMapper
}

// Identifierer takes path components and returns a raw SQL identifier for the
// Endpoint with necessary quoting applied.
type Identifierer interface {
	Identifier(path ...string) string
}

// Literaler takes a string and returns a raw SQL literal for the Endpoint
// with required quoting and escaping already applied.
type Literaler interface {
	Literal(str string) string
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

// AlwaysNullableTypeMapper is like a TypeMapper, but always considers the mapped type to be
// nullable. This is useful when adding new columns to a table that must be nullable regardless of
// their JSON schema, or for comparing compatible type changes which do not need to consider
// nullability.
type AlwaysNullableTypeMapper interface {
	MapTypeNullable(*Projection) (MappedType, error)
}

// IdentifierFn is a function that implements Identifierer.
type IdentifierFn func(path ...string) string

func (f IdentifierFn) Identifier(path ...string) string { return f(path...) }

// LiteralFn is a function that implements Literaler.
type LiteralFn func(s string) string

func (f LiteralFn) Literal(str string) string { return f(str) }

// PlaceholderFn is a function that implements Placeholderer.
type PlaceholderFn func(index int) string

func (f PlaceholderFn) Placeholder(index int) string { return f(index) }

// TypeMapperFn is a function that implements TypeMapper.
type TypeMapperFn func(*Projection) (MappedType, error)

func (f TypeMapperFn) MapType(p *Projection) (MappedType, error) { return f(p) }

// Compile-time check that wrapping functions with typed  Fn() implementations
// can be used to build a Dialect.
var _ = Dialect{
	Placeholderer:            PlaceholderFn(func(index int) string { return "" }),
	Literaler:                LiteralFn(func(s string) string { return "" }),
	Identifierer:             IdentifierFn(func(path ...string) string { return "" }),
	TypeMapper:               &MaybeNullableMapper{},
	AlwaysNullableTypeMapper: &AlwaysNullableMapper{},
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
