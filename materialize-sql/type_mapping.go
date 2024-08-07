package sql

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

// FlatType is a flattened, database-friendly representation of a document location's type.
// It differs from JSON types by:
// * Having a single type, with cases like "JSON string OR integer" delegated to a MULTIPLE case.
// * Hoisting JSON `null` out of the type representation and into a separate orthogonal concern.
type FlatType string

// FlatType constants that are used by DDLMapper.
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

type ColumnDef struct {
	// DDL is the "CREATE TABLE" DDL type for this mapping, suited for direct
	// inclusion in raw SQL for new table creation.
	DDL string
	// NullableDDL is DDL type for this mapping, always in its nullable form, to
	// be used for existing table alterations.
	NullableDDL string
	// Comment for this column.
	Comment string
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

// ProjectionMapper is a function that converts a Projection into the column DDL
// and element converter for storing values into the column.
type ProjectionMapper func(p boilerplate.Projection) (string, boilerplate.ElementConverter)

// DDLMapper completes the column definition from the column DDL, by adding the
// "not null" and "nullable" text (if applicable), the comment for the column,
// and handling any field configuration options.
type DDLMapper struct {
	notNullText  string
	nullableText string
	m            map[FlatType]ProjectionMapper
}

type FlatTypeMappings map[FlatType]ProjectionMapper

func NewDDLMapper(mappings FlatTypeMappings, opts ...DDLMapperOption) *DDLMapper {
	out := &DDLMapper{m: make(map[FlatType]ProjectionMapper)}

	for ft, mapper := range mappings {
		out.m[ft] = mapper
	}

	for _, o := range opts {
		o(out)
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
func MapStatic(ddl string, converter ...boilerplate.ElementConverter) ProjectionMapper {
	var c boilerplate.ElementConverter
	if converter != nil {
		c = converter[0]
	}

	return func(p boilerplate.Projection) (string, boilerplate.ElementConverter) {
		return ddl, c
	}
}

// MapPrimaryKey specifies an alternate ProjectionMapper for the the column if
// it is a primary key. This is useful for cases where specific databases
// require certain variations of a type for primary keys. For example, MySQL
// does not accept TEXT as a primary key, but a VARCHAR with specified size
// works.
func MapPrimaryKey(pkMapper, delegate ProjectionMapper) ProjectionMapper {
	return func(p boilerplate.Projection) (string, boilerplate.ElementConverter) {
		if p.IsPrimaryKey {
			return pkMapper(p)
		}
		return delegate(p)
	}
}

// StringLenStep creates a stringStep with the given startAt and mapper.
// Typically startAt should be the number of digits that the smaller type could
// hold + 1.
func StringLenStep(startAt int, ddl string, converter ...boilerplate.ElementConverter) stringStep {
	return stringStep{
		startAt: startAt,
		mapper:  MapStatic(ddl, converter...),
	}
}

type stringStep struct {
	startAt int
	mapper  ProjectionMapper
}

// MapOnStringMaxLength determines the projection mapper to use based on the
// maximum length of the field per its string inference. The default mapper is
// used if there is no maximum length available from inference. Multiple "steps"
// can be provided, and the ProjectionMapper from the smallest step per its
// startAt will be used. The default applies to all projections that do not have
// an inferred maximum length greater than or equal to any of the steps.
func MapOnStringMaxLength(defaultMapper ProjectionMapper, steps ...stringStep) ProjectionMapper {
	if !sort.SliceIsSorted(steps, func(i, j int) bool {
		return steps[i].startAt < steps[j].startAt
	}) {
		panic("MapOnStringMaxLengths steps must be sorted by StartAt")
	}

	if len(steps) == 0 {
		panic("MapOnStringMaxLengths must have at least one step")
	}

	return func(p boilerplate.Projection) (string, boilerplate.ElementConverter) {
		var maxLength int
		if p.Inference.String_ != nil {
			maxLength = int(p.Inference.String_.MaxLength)
		}

		for i := len(steps) - 1; i >= 0; i-- {
			if maxLength >= steps[i].startAt {
				return steps[i].mapper(p)
			}
		}

		return defaultMapper(p)
	}
}

type StringMappings struct {
	Fallback        ProjectionMapper
	WithFormat      map[string]ProjectionMapper
	WithContentType map[string]ProjectionMapper
}

// StringTypeMapper is a special ProjectionMapper for string type columns, which
// can take the format or content type into account when deciding how to handle
// the column.
func MapString(m StringMappings) ProjectionMapper {
	return func(p boilerplate.Projection) (string, boilerplate.ElementConverter) {
		delegate := m.Fallback

		if h, ok := m.WithFormat[p.Inference.String_.Format]; ok {
			delegate = h
		} else if h, ok := m.WithContentType[p.Inference.String_.ContentType]; ok {
			delegate = h
		}

		return delegate(p)
	}
}

func (d *DDLMapper) MapType(p boilerplate.Projection, fc *fieldConfig) (ColumnDef, boilerplate.ElementConverter) {
	var ft FlatType

	if p.IsNumericString() {
		switch *p.NumericString {
		case boilerplate.StringFormatInteger:
			ft = STRING_INTEGER
		case boilerplate.StringFormatNumber:
			ft = STRING_NUMBER
		}
	} else if len(p.TypesWithoutNull) == 0 {
		// This is generally not possible due to validation constraints
		// forbidding these types of fields.
		ft = NEVER
	} else if len(p.TypesWithoutNull) > 1 {
		ft = MULTIPLE
	} else {
		// Single non-null JSON type.
		switch p.TypesWithoutNull[0] {
		case "array":
			ft = ARRAY
		case "boolean":
			ft = BOOLEAN
		case "integer":
			ft = INTEGER
		case "number":
			ft = NUMBER
		case "object":
			ft = OBJECT
		case "string":
			if p.Inference.String_.ContentEncoding == "base64" {
				ft = BINARY
			} else {
				ft = STRING
			}
		}
	}

	h, ok := d.m[ft]
	if !ok {
		panic(fmt.Sprintf("application logic error in MapType: no handler for flat type %q", ft))
	}

	ddl, converter := h(p)

	if fc != nil && (fc.IgnoreStringFormat || fc.CastToString) {
		// Materialize this field as a string, and convert its values to strings
		// if configured.
		h, ok := d.m[STRING]
		if !ok {
			panic(fmt.Sprintf("application logic error in MapType: no handler for flat type %q", STRING))
		}

		ddl, _ = h(p)
		converter = boilerplate.ToStr
	}

	if fc != nil && fc.DDL != "" {
		// User has specified a custom DDL, so use that.
		ddl = fc.DDL
	}

	out := ColumnDef{
		DDL:         ddl,
		NullableDDL: ddl,
	}

	if p.MustExist && d.notNullText != "" {
		out.DDL += " " + d.notNullText
	} else if d.nullableText != "" {
		out.DDL += " " + d.nullableText
		out.NullableDDL = out.DDL
	}

	var source = "auto-generated"
	if p.Explicit {
		source = "user-provided"
	}
	out.Comment = fmt.Sprintf("%s projection of JSON at: %s with inferred types: %s",
		source, p.Ptr, p.Inference.Types)

	if p.Inference.Description != "" {
		out.Comment = p.Inference.Description + "\n" + out.Comment
	}
	if p.Inference.Title != "" {
		out.Comment = p.Inference.Title + "\n" + out.Comment
	}

	return out, converter
}

type constrainter struct {
	dialect Dialect
}

func (constrainter) NewConstraints(p boilerplate.Projection, deltaUpdates bool, fc *fieldConfig) *pm.Response_Validated_Constraint {
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
	case p.Inference.IsSingleScalarType() || p.IsNumericString():
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

func (c constrainter) Compatible(existing boilerplate.EndpointField, proposed boilerplate.Projection, fc *fieldConfig) (bool, error) {
	if fc != nil && (fc.DDL != "" || fc.CastToString || fc.IgnoreStringFormat) {
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

	return c.dialect.ValidateColumn(existing, proposed.Projection)
}

func (c constrainter) DescriptionForType(p boilerplate.Projection, fc *fieldConfig) (string, error) {
	mapped, _ := c.dialect.MapType(p, fc)
	return mapped.NullableDDL, nil
}
