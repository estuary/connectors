package sql

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
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
	// RawFieldConfig is (optional) field configuration supplied within the field selection.
	RawFieldConfig json.RawMessage
}

// BuildProjections returns the Projections extracted from a Binding.
func BuildProjections(spec *pf.MaterializationSpec_Binding) (keys, values []Projection, document *Projection) {
	var do = func(field string) Projection {
		return buildProjection(spec.Collection.GetProjection(field), spec.FieldSelection.FieldConfigJsonMap[field])
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

func buildProjection(p *pf.Projection, rawFieldConfig json.RawMessage) Projection {
	var out = Projection{
		Projection:     *p,
		RawFieldConfig: rawFieldConfig,
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
	if format, ok := boilerplate.AsFormattedNumeric(&p.Projection); ok && !p.IsPrimaryKey {
		switch format {
		case boilerplate.StringFormatInteger:
			return STRING_INTEGER, mustExist
		case boilerplate.StringFormatNumber:
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

type MappedType struct {
	// DDL is the "CREATE TABLE" DDL type for this mapping, suited for direct inclusion in raw SQL
	// for new table creation.
	DDL string
	// NullableDDL is DDL type for this mapping, always in its nullable form, to be used for
	// existing table alterations.
	NullableDDL string
	// Converter of tuple elements for this mapping, into SQL runtime values.
	Converter ElementConverter `json:"-"`
}

// MapProjectionFn is a function that converts a Projection into the column DDL
// and element converter for storing values into the column.
type MapProjectionFn func(p *Projection) (string, ElementConverter)

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

// MapStatic creates a ProjectionMapper that always returns the same DDL and
// optionally sets an ElementConverter.
func MapStatic(ddl string, converter ...ElementConverter) MapProjectionFn {
	var c ElementConverter
	if converter != nil {
		c = converter[0]
	}

	return func(p *Projection) (string, ElementConverter) {
		return ddl, c
	}
}

// MapPrimaryKey specifies an alternate ProjectionMapper for the the column if
// it is a primary key. This is useful for cases where specific databases
// require certain variations of a type for primary keys. For example, MySQL
// does not accept TEXT as a primary key, but a VARCHAR with specified size
// works.
func MapPrimaryKey(pkMapper, delegate MapProjectionFn) MapProjectionFn {
	return func(p *Projection) (string, ElementConverter) {
		if p.IsPrimaryKey {
			return pkMapper(p)
		}
		return delegate(p)
	}
}

type StringMappings struct {
	Fallback        MapProjectionFn
	WithFormat      map[string]MapProjectionFn
	WithContentType map[string]MapProjectionFn
}

// StringTypeMapper is a special ProjectionMapper for string type columns, which
// can take the format or content type into account when deciding how to handle
// the column.
func MapString(m StringMappings) MapProjectionFn {
	return func(p *Projection) (string, ElementConverter) {
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

type fieldConfig struct {
	// CastToString will materialize the field as a string representation of its
	// value.
	CastToString bool `json:"castToString"`
	// IgnoreStringFormat is a legacy configuration used as an alias for
	// castToString.
	IgnoreStringFormat bool `json:"ignoreStringFormat"`
	// DDL allows for user-defined DDL overrides for creating the column.
	DDL string `json:"DDL"`
}

func (d DDLMapper) MapType(p *Projection) (MappedType, error) {
	ft, mustExist := p.AsFlatType()

	h, ok := d.m[ft]
	if !ok {
		return MappedType{}, fmt.Errorf("unable to map field %s with type %s", p.Field, p.Inference.Types)
	}

	ddl, converter := h(p)
	if converter == nil {
		converter = passThrough
	}

	var fc fieldConfig
	if p.RawFieldConfig != nil {
		if err := json.Unmarshal(p.RawFieldConfig, &fc); err != nil {
			return MappedType{}, fmt.Errorf("unmarshaling field config: %w", err)
		}
	}

	if fc.IgnoreStringFormat || fc.CastToString {
		// Materialize this field as a string, and convert its values to strings
		// if configured.
		ddl, _ = d.m[STRING](p)
		converter = ToStr
	}

	if fc.DDL != "" {
		// User has specified a custom DDL, so use that.
		ddl = fc.DDL
	}

	out := MappedType{
		DDL:         ddl,
		NullableDDL: ddl,
		Converter:   converter,
	}

	if mustExist && d.notNullText != "" {
		out.DDL += " " + d.notNullText
	} else if d.nullableText != "" {
		out.DDL += " " + d.nullableText
		out.NullableDDL = out.DDL
	}

	return out, nil
}

type constrainter struct {
	dialect Dialect
}

func (constrainter) NewConstraints(p *pf.Projection, deltaUpdates bool) *pm.Response_Validated_Constraint {
	_, isNumeric := boilerplate.AsFormattedNumeric(p)

	var constraint = pm.Response_Validated_Constraint{}
	switch {
	case p.IsPrimaryKey:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "All Locations that are part of the collections key are required"
	case p.IsRootDocumentProjection() && deltaUpdates:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The root document should usually be materialized"
	case p.IsRootDocumentProjection():
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "The root document must be materialized"
	case len(p.Inference.Types) == 0:
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize a field with no types"
	case p.Field == "_meta/op":
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The operation type should usually be materialized"
	case strings.HasPrefix(p.Field, "_meta/"):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "Metadata fields fields are able to be materialized"
	case p.Inference.IsSingleScalarType() || isNumeric:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The projection has a single scalar type"
	case slices.Equal(p.Inference.Types, []string{"null"}):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize a field where the only possible type is 'null'"
	case p.Inference.IsSingleType() && slices.Contains(p.Inference.Types, "object"):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "Object fields may be materialized"
	default:
		// Any other case is one where the field is an array or has multiple types.
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "This field is able to be materialized"
	}

	return &constraint
}

func (c constrainter) Compatible(existing boilerplate.EndpointField, proposed *pf.Projection, rawFieldConfig json.RawMessage) (bool, error) {
	var fc fieldConfig
	if rawFieldConfig != nil {
		if err := json.Unmarshal(rawFieldConfig, &fc); err != nil {
			return false, fmt.Errorf("unmarshaling field config: %w", err)
		}
	}

	if fc.DDL != "" || fc.CastToString || fc.IgnoreStringFormat {
		// Fields with custom DDL set are always said to be compatible, since it
		// is not practical in general to correlate the database's
		// representation of an existing column and the user's DDL definition.
		// Additionally, fields that are being cast to strings are always
		// compatible, since these values could go into a variety of different
		// columns types. The expectation is that using these advanced features
		// will also involve re-backfilling tables or manually
		// altering/migrating tables as desired.
		return true, nil
	}

	return c.dialect.ValidateColumn(existing, *proposed)
}

func (c constrainter) DescriptionForType(p *pf.Projection, rawFieldConfig json.RawMessage) (string, error) {
	pp := buildProjection(p, rawFieldConfig)

	mapped, err := c.dialect.MapType(&pp)
	if err != nil {
		return "", fmt.Errorf("mapping type: %w", err)
	}

	return mapped.NullableDDL, nil
}
