package sql

import (
	"fmt"
	"math"
	"slices"
	"strings"

	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// FlatType is a flattened, database-friendly representation of a document location's type.
// It differs from JSON types by:
// * Having a single type, with cases like "JSON string OR integer" delegated to a MULTIPLE case.
// * Hoisting JSON `null` out of the type representation and into a separate orthogonal concern.
type FlatType string

// FlatType constants that are used by TypeMapper
const (
	ARRAY          FlatType = "array"
	BINARY         FlatType = "binary"
	BOOLEAN        FlatType = "boolean"
	INTEGER        FlatType = "integer"
	MULTIPLE       FlatType = "multiple"
	NEVER          FlatType = "never"
	NUMBER         FlatType = "number"
	OBJECT         FlatType = "object"
	STRING         FlatType = "string"
	STRING_INTEGER FlatType = "string_integer"
	STRING_NUMBER  FlatType = "string_number"
)

// Projection lifts a pf.Projection into a form that's more easily worked with for SQL column mapping.
type Projection struct {
	pf.Projection
	// Comment for this projection.
	Comment string
}

// BuildProjections returns the Projections extracted from a Binding.
func BuildProjections(spec *pf.MaterializationSpec_Binding) (keys, values []Projection, document *Projection) {
	var do = func(field string) Projection {
		return buildProjection(spec.Collection.GetProjection(field))
	}

	for _, field := range spec.FieldSelection.Keys {
		keys = append(keys, do(field))
	}
	for _, field := range spec.FieldSelection.Values {
		values = append(values, do(field))
	}
	if field := spec.FieldSelection.Document; field != "" {
		document = new(Projection)
		*document = do(field)
	}

	return
}

func buildProjection(p *pf.Projection) Projection {
	var out = Projection{
		Projection: *p,
	}

	var source = "auto-generated"
	if out.Explicit {
		source = "user-provided"
	}
	out.Comment = fmt.Sprintf("%s projection of JSON at: %s with inferred types: %s",
		source, out.Ptr, out.Inference.Types)

	if out.Inference.Description != "" {
		out.Comment = out.Inference.Description + "\n" + out.Comment
	}
	if out.Inference.Title != "" {
		out.Comment = out.Inference.Title + "\n" + out.Comment
	}

	return out
}

// AsFlatType returns the Projection's FlatType.
func (p *Projection) AsFlatType() (_ FlatType, mustExist bool) {
	mustExist = p.Inference.Exists == pf.Inference_MUST
	if slices.Contains(p.Inference.Types, "null") {
		mustExist = false
	}

	// Compatible numeric formatted strings can be materialized as either integers or numbers,
	// depending on the format string.
	if format, ok := m.AsFormattedNumeric(&p.Projection); ok && !p.IsPrimaryKey {
		switch format {
		case m.StringFormatInteger:
			return STRING_INTEGER, mustExist
		case m.StringFormatNumber:
			return STRING_NUMBER, mustExist
		}
	}

	var types []FlatType
	for _, ty := range p.Inference.Types {
		switch ty {
		case "string":

			if p.Inference.String_.ContentEncoding == "base64" {
				types = append(types, BINARY)
			} else {
				types = append(types, STRING)
			}
		case "integer":
			types = append(types, INTEGER)
		case "number", "fractional":
			types = append(types, NUMBER)
		case "boolean":
			types = append(types, BOOLEAN)
		case "object":
			types = append(types, OBJECT)
		case "array":
			types = append(types, ARRAY)
		}
	}

	switch len(types) {
	case 0:
		return NEVER, false
	case 1:
		return types[0], mustExist
	default:
		return MULTIPLE, mustExist
	}
}

// CompatibleColumn is a type that can be tested for compatibility with an
// existing field.
type CompatibleColumnType interface {
	Compatible(existing boilerplate.ExistingField) bool
}

// CompatibleStringEqualFold is a CompatibleColumn that is equal to the case
// folded field type.
type CompatibleStringEqualFold struct {
	Inner string
}

func (s *CompatibleStringEqualFold) Compatible(existing boilerplate.ExistingField) bool {
	return strings.EqualFold(existing.Type, s.Inner)
}

// CompatibleColumnTypes is a list of column types that the mapped type
// corresponding to the Flow field's schema is compatible with. By default the
// DDL used to create the column is included in this list, so any additional
// column types to be considered compatible for validation should be added using
// the AlsoCompatibleWith variadic option when configuring the type mapping.
// Most often this is used when the endpoint's information schema describes the
// column in a different way than the DDL used to create it. Types are
// case-insensitive.
type CompatibleColumnTypes []CompatibleColumnType

type DDLer interface {
	DDL() string
}

type StringDDL string

func (d StringDDL) DDL() string {
	return string(d)
}

type MappedType struct {
	// DDL is the "CREATE TABLE" DDL type for this mapping, suited for direct inclusion in raw SQL
	// for new table creation.
	DDL string
	// NullableDDL is DDL type for this mapping, always in its nullable form, to be used for
	// existing table alterations.
	NullableDDL string
	// Converter of tuple elements for this mapping, into SQL runtime values.
	Converter ElementConverter `json:"-"`
	// The list of compatible column types for this mapping. The
	// DDL used to create the column is included by default. Any columns with
	// types not in this list or the migratable list will produce an "incompatible" constraint.
	CompatibleColumnTypes CompatibleColumnTypes
	// If the column is using user-defined DDL or not. The selected field will
	// always pass validation if this is true.
	UserDefinedDDL bool
	// Type that represents the desired data type, corresponds to the .DDL
	// field but may be a type with structured data used for migration.
	TargetType DDLer
	// Complete list of migration specifications for the endpoint.
	MigratableTypes MigrationSpecs
}

func (m MappedType) String() string {
	return m.NullableDDL
}

func (m MappedType) Compatible(existing boilerplate.ExistingField) bool {
	if m.UserDefinedDDL {
		// Fields with custom DDL set are always said to be compatible, since it
		// is not practical in general to correlate the database's
		// representation of an existing column and the user's DDL definition.
		return true
	}

	return slices.ContainsFunc(m.CompatibleColumnTypes, func(column CompatibleColumnType) bool {
		return column.Compatible(existing)
	})
}

func (m MappedType) CanMigrate(existing boilerplate.ExistingField) bool {
	res := m.MigratableTypes.FindMigrationSpec(existing, m) != nil
	return res
}

// MapProjectionFn is a function that converts a Projection into the column DDL
// and element converter for storing values into the column.
type MapProjectionFn func(p *Projection) (DDLer, CompatibleColumnTypes, ElementConverter)

var _ TypeMapper = DDLMapper{}

// DDLMapper completes the column definition from the column DDL, by adding the
// "not null" and "nullable" text (if applicable), the comment for the column,
// and handling any field configuration options.
type DDLMapper struct {
	notNullText  string
	nullableText string
	m            map[FlatType]MapProjectionFn
}

type FlatTypeMappings map[FlatType]MapProjectionFn

func NewDDLMapper(mappings FlatTypeMappings, opts ...DDLMapperOption) DDLMapper {
	out := DDLMapper{m: make(map[FlatType]MapProjectionFn)}

	for _, o := range opts {
		o(&out)
	}

	for ft, mapper := range mappings {
		out.m[ft] = mapper
	}

	return out
}

type DDLMapperOption func(*DDLMapper)

// WithNotNullText sets the "not null" DDL that will be used for columns with
// fields that can never be NULL.
func WithNotNullText(notNullText string) DDLMapperOption {
	return func(m *DDLMapper) {
		m.notNullText = notNullText
	}
}

// WithNullableText sets the "nullable" DDL that will be used for columns with
// fields that can be NULL. Most databases will assume that a column may contain
// null as long as it isn't declared with a NOT NULL constraint, but some
// databases (e.g. ms sql server) make that behavior configurable, requiring the
// DDL to explicitly declare a column with NULL if it may contain null values.
func WithNullableText(nullableText string) DDLMapperOption {
	return func(m *DDLMapper) {
		m.nullableText = nullableText
	}
}

type mapStaticConfig struct {
	compatible CompatibleColumnTypes
	converter  ElementConverter
}

type MapStaticOption func(*mapStaticConfig)

// AlsoCompatibleWith sets additional column types that the mapped type should
// be considered compatible with.
func AlsoCompatibleWith(compatibleTypes ...string) MapStaticOption {
	return func(c *mapStaticConfig) {
		for _, compat := range compatibleTypes {
			c.compatible = append(c.compatible, &CompatibleStringEqualFold{compat})
		}
	}
}

// Using converter sets a custom ElementConverter for the mapped type.
func UsingConverter(converter ElementConverter) MapStaticOption {
	return func(c *mapStaticConfig) {
		c.converter = converter
	}
}

// MapStatic creates a ProjectionMapper that always returns the same DDL. All
// mapping configurations must eventually resolve with a MapStatic that defines
// the DDL for a column as the "leaf" MapProjectionFn in a possibly compound
// arrangement of additional MapProjectionFn's, such as MapPrimaryKey or
// MapString.
func MapStatic(ddl string, opts ...MapStaticOption) MapProjectionFn {
	cfg := mapStaticConfig{
		// A projection is always valid with a reported endpoint column type
		// that matches it exactly.
		compatible: []CompatibleColumnType{&CompatibleStringEqualFold{ddl}},
	}

	for _, o := range opts {
		o(&cfg)
	}

	return func(p *Projection) (DDLer, CompatibleColumnTypes, ElementConverter) {
		return StringDDL(ddl), cfg.compatible, cfg.converter
	}
}

// MapPrimaryKey specifies an alternate MapProjectionFn for the the column if it
// is a primary key. This is useful for cases where specific databases require
// certain variations of a type for primary keys. For example, MySQL does not
// accept TEXT as a primary key, but a VARCHAR with specified size works.
func MapPrimaryKey(pkMapper, delegate MapProjectionFn) MapProjectionFn {
	return func(p *Projection) (DDLer, CompatibleColumnTypes, ElementConverter) {
		if p.IsPrimaryKey {
			return pkMapper(p)
		}
		return delegate(p)
	}
}

// StringMappings defines how string fields are mapped to database column types.
type StringMappings struct {
	// Fallback is the default mapping function for string fields. It is used
	// when there is no format or content type that applies. Typically this
	// should be the database's TEXT column or similar.
	Fallback MapProjectionFn
	// WithFormat provides a mapping function for string fields that have a
	// matching format annotation that may use a special column type, ex:
	// "date" or "date-time".
	WithFormat map[string]MapProjectionFn
	// WithContentType provides a mapping function for string fields that have a
	// matching Content-Type annotation. As an example, this is occasionally
	// used by materializations for creating a special column type for the
	// "checkpoints" column, since that field has a specific Content-Type set.
	//
	// Content-Type is different than Content-Encoding. The only
	// Content-Encoding that is currently handled is base64, and that will
	// create a BINARY flat type that should be handled like other flat types.
	WithContentType map[string]MapProjectionFn
}

// StringTypeMapper is a special MapProjectionFn for string type columns, which
// can take the format or content type into account when deciding how to handle
// the column.
func MapString(m StringMappings) MapProjectionFn {
	return func(p *Projection) (DDLer, CompatibleColumnTypes, ElementConverter) {
		delegate := m.Fallback

		if p.Inference.String_ != nil {
			if h, ok := m.WithFormat[p.Inference.String_.Format]; ok {
				delegate = h
			} else if h, ok := m.WithContentType[p.Inference.String_.ContentType]; ok {
				delegate = h
			}
		}

		return delegate(p)
	}
}

// MapStringMaxLen uses an alternate mapping for string fields where string
// inference is available and the string is longer than the cutoff.
func MapStringMaxLen(def, alt MapProjectionFn, cutoff uint32) MapProjectionFn {
	return func(p *Projection) (DDLer, CompatibleColumnTypes, ElementConverter) {
		if p.Inference.String_ != nil &&
			p.Inference.String_.MaxLength > cutoff {
			return alt(p)
		}

		return def(p)
	}
}

// MapSignedInt64 uses an alternate mapping for numeric fields where numeric
// inference is available and the minimum or maximum of the inferred range is
// outside the bounds of what will fit in a signed 64 bit integer.
func MapSignedInt64(def, alt MapProjectionFn) MapProjectionFn {
	return func(p *Projection) (DDLer, CompatibleColumnTypes, ElementConverter) {
		if p.Inference.Numeric != nil &&
			(p.Inference.Numeric.Minimum < math.MinInt64 || p.Inference.Numeric.Maximum > math.MaxInt64) {
			// Numeric Minimum and Maximums are stated as powers of 10, so
			// comparisons to exact integer values aren't precise, but this
			// logic should still work out since 1e19 is greater than 1<<63, and
			// that's the next power of 10 bigger than the maximum signed 64 bit
			// integer.
			return alt(p)
		}

		return def(p)
	}
}

type FieldConfig struct {
	// CastToString_ will materialize the field as a string representation of
	// its value.
	CastToString_ bool `json:"castToString"`
	// IgnoreStringFormat is a legacy configuration used as an alias for
	// castToString.
	IgnoreStringFormat bool `json:"ignoreStringFormat"`
	// DDL allows for user-defined DDL overrides for creating the column.
	DDL string `json:"DDL"`
}

func (fc FieldConfig) CastToString() bool {
	return fc.CastToString_ || fc.IgnoreStringFormat
}

func (fc FieldConfig) Validate() error {
	return nil
}

func (d DDLMapper) MapType(p *Projection, fc FieldConfig) MappedType {
	ft, mustExist := p.AsFlatType()

	h, ok := d.m[ft]
	if !ok {
		// This can only happen due to a programming error, where a Flow field
		// schema type is not present in the dialect's set of mappings.
		panic(fmt.Sprintf("internal error: unable to map field %s with type %s", p.Field, p.Inference.Types))
	}

	ddl, compatibleTypes, converter := h(p)
	if converter == nil {
		converter = passThrough
	}

	if fc.IgnoreStringFormat || fc.CastToString_ {
		// Materialize this field as a "plain" string, and convert its values to
		// strings if configured.
		p := *p
		p.Inference.String_ = &pf.Inference_String{}
		ddl, compatibleTypes, _ = d.m[STRING](&p)
		converter = ToStr
	}

	if fc.DDL != "" {
		// User has specified a custom DDL, so use that.
		ddl = StringDDL(fc.DDL)
	}

	out := MappedType{
		DDL:                   ddl.DDL(),
		NullableDDL:           ddl.DDL(),
		Converter:             converter,
		CompatibleColumnTypes: compatibleTypes,
		UserDefinedDDL:        fc.DDL != "",
		TargetType:            ddl,
	}

	if mustExist && d.notNullText != "" {
		out.DDL += " " + d.notNullText
	} else if d.nullableText != "" {
		out.DDL += " " + d.nullableText
		out.NullableDDL = out.DDL
	}

	return out
}
