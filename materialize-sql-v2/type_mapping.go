package sql

import (
	"encoding/json"
	"fmt"
	"math"
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

// CompatibleColumnTypes is a list of column types that the mapped type
// corresponding to the Flow field's schema is compatible with. By default the
// DDL used to create the column is included in this list, so any additional
// column types to be considered compatible for validation should be added using
// the AlsoCompatibleWith variadic option when configuring the type mapping.
// Most often this is used when the endpoint's information schema describes the
// column in a different way than the DDL used to create it. Types are
// case-insensitive.
type CompatibleColumnTypes []string

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
	// types not in this list or the migratable list will produce an "unsatisfiable" constraint.
	CompatibleColumnTypes CompatibleColumnTypes
	// If the column is using user-defined DDL or not. The selected field will
	// always pass validation if this is true.
	UserDefinedDDL bool
}

// MapProjectionFn is a function that converts a Projection into the column DDL
// and element converter for storing values into the column.
type MapProjectionFn func(p *Projection) (string, CompatibleColumnTypes, ElementConverter)

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

type mapStaticOption func(*mapStaticConfig)

// AlsoCompatibleWith sets additional column types that the mapped type should
// be considered compatible with.
func AlsoCompatibleWith(compatibleTypes ...string) mapStaticOption {
	return func(c *mapStaticConfig) {
		c.compatible = append(c.compatible, compatibleTypes...)
	}
}

// Using converter sets a custom ElementConverter for the mapped type.
func UsingConverter(converter ElementConverter) mapStaticOption {
	return func(c *mapStaticConfig) {
		c.converter = converter
	}
}

// MapStatic creates a ProjectionMapper that always returns the same DDL. All
// mapping configurations must eventually resolve with a MapStatic that defines
// the DDL for a column as the "leaf" MapProjectionFn in a possibly compound
// arrangement of additional MapProjectionFn's, such as MapPrimaryKey or
// MapString.
func MapStatic(ddl string, opts ...mapStaticOption) MapProjectionFn {
	cfg := mapStaticConfig{
		// A projection is always valid with a reported endpoint column type
		// that matches it exactly.
		compatible: []string{ddl},
	}

	for _, o := range opts {
		o(&cfg)
	}

	return func(p *Projection) (string, CompatibleColumnTypes, ElementConverter) {
		return ddl, cfg.compatible, cfg.converter
	}
}

// MapPrimaryKey specifies an alternate MapProjectionFn for the the column if it
// is a primary key. This is useful for cases where specific databases require
// certain variations of a type for primary keys. For example, MySQL does not
// accept TEXT as a primary key, but a VARCHAR with specified size works.
func MapPrimaryKey(pkMapper, delegate MapProjectionFn) MapProjectionFn {
	return func(p *Projection) (string, CompatibleColumnTypes, ElementConverter) {
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
	return func(p *Projection) (string, CompatibleColumnTypes, ElementConverter) {
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
	return func(p *Projection) (string, CompatibleColumnTypes, ElementConverter) {
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
	return func(p *Projection) (string, CompatibleColumnTypes, ElementConverter) {
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

	ddl, compatibleTypes, converter := h(p)
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
		// Materialize this field as a "plain" string, and convert its values to
		// strings if configured.
		p := *p
		p.Inference.String_ = &pf.Inference_String{}
		ddl, compatibleTypes, _ = d.m[STRING](&p)
		converter = ToStr
	}

	if fc.DDL != "" {
		// User has specified a custom DDL, so use that.
		ddl = fc.DDL
	}

	out := MappedType{
		DDL:                   ddl,
		NullableDDL:           ddl,
		Converter:             converter,
		CompatibleColumnTypes: compatibleTypes,
		UserDefinedDDL:        fc.DDL != "",
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
	dialect      Dialect
	featureFlags map[string]bool
}

func (constrainter) NewConstraints(p *pf.Projection, deltaUpdates bool, fc json.RawMessage) (*pm.Response_Validated_Constraint, error) {
	_, isNumeric := boilerplate.AsFormattedNumeric(p)

	var constraint = pm.Response_Validated_Constraint{}
	switch {
	case p.IsPrimaryKey:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "All Locations that are part of the collections key are recommended"
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
		constraint.Reason = "Metadata fields are able to be materialized"
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

	return &constraint, nil
}

func (c constrainter) compatibleType(existing boilerplate.ExistingField, proposed *pf.Projection, rawFieldConfig json.RawMessage) (bool, error) {
	proj := buildProjection(proposed, rawFieldConfig)
	mapped, err := c.dialect.MapType(&proj)
	if err != nil {
		return false, fmt.Errorf("mapping type: %w", err)
	}

	if mapped.UserDefinedDDL {
		// Fields with custom DDL set are always said to be compatible, since it
		// is not practical in general to correlate the database's
		// representation of an existing column and the user's DDL definition.
		return true, nil
	}

	isCompatibleType := slices.ContainsFunc(mapped.CompatibleColumnTypes, func(compatibleType string) bool {
		return strings.EqualFold(existing.Type, compatibleType)
	})

	return isCompatibleType, nil
}

func (c constrainter) migratable(existing boilerplate.ExistingField, proposed *pf.Projection, rawFieldConfig json.RawMessage) (bool, *MigrationSpec, error) {
	if proposed.IsRootDocumentProjection() {
		// Do not allow migration of the root document column. There is
		// currently no known case where this would be useful, and in cases of
		// pre-existing materializations where the document field was
		// materialized as stringified JSON migrating it to a JSON column will
		// most likely break the materialization, since the migrated value will
		// be a JSON string instead of an object.
		return false, nil, nil
	}

	proj := buildProjection(proposed, rawFieldConfig)
	mapped, err := c.dialect.MapType(&proj)
	if err != nil {
		return false, nil, fmt.Errorf("mapping type: %w", err)
	}

	if migrationSpec := c.dialect.MigratableTypes.FindMigrationSpec(existing.Type, mapped.NullableDDL); migrationSpec != nil {
		return true, migrationSpec, nil
	}

	return false, nil, nil
}

func (c constrainter) Compatible(existing boilerplate.ExistingField, proposed *pf.Projection, rawFieldConfig json.RawMessage) (bool, error) {
	if compatible, err := c.compatibleType(existing, proposed, rawFieldConfig); err != nil {
		return false, err
	} else if compatible {
		return true, nil
	} else {
		migratable, _, err := c.migratable(existing, proposed, rawFieldConfig)
		return migratable, err
	}
}

func (c constrainter) DescriptionForType(p *pf.Projection, rawFieldConfig json.RawMessage) (string, error) {
	pp := buildProjection(p, rawFieldConfig)

	mapped, err := c.dialect.MapType(&pp)
	if err != nil {
		return "", fmt.Errorf("mapping type: %w", err)
	}

	return mapped.NullableDDL, nil
}
