package sql

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// FlatType is a flattened, database-friendly representation of a document location's type.
// It differs from JSON types by:
// * Having a single type, with cases like "JSON string OR integer" delegated to a MULTIPLE case.
// * Hoisting JSON `null` out of the type representation and into a separate orthogonal concern.
type FlatType string

// FlatType constants that are used by ColumnMapper
const (
	ARRAY    FlatType = "array"
	BINARY   FlatType = "binary"
	BOOLEAN  FlatType = "boolean"
	INTEGER  FlatType = "integer"
	MULTIPLE FlatType = "multiple"
	NEVER    FlatType = "never"
	NUMBER   FlatType = "number"
	OBJECT   FlatType = "object"
	STRING   FlatType = "string"
)

// effectiveTypeFormats is the listing of custom string formats Flow supports for treating strings
// as some other datatype. The format must match a JSON type and must also match the underlying
// string of a FlatType.
var effectiveTypeFormats = []string{string(INTEGER), string(NUMBER)}

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
		var p = Projection{
			Projection:     *spec.Collection.GetProjection(field),
			RawFieldConfig: spec.FieldSelection.FieldConfigJson[field],
		}

		var source = "auto-generated"
		if p.Explicit {
			source = "user-provided"
		}
		p.Comment = fmt.Sprintf("%s projection of JSON at: %s with inferred types: %s",
			source, p.Ptr, p.Inference.Types)

		if p.Inference.Description != "" {
			p.Comment = p.Inference.Description + "\n" + p.Comment
		}
		if p.Inference.Title != "" {
			p.Comment = p.Inference.Title + "\n" + p.Comment
		}

		return p
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

// AsFlatType returns the Projection's FlatType.
func (p *Projection) AsFlatType() (_ FlatType, mustExist bool) {
	mustExist = p.Inference.Exists == pf.Inference_MUST

	types, hasNull := jsonTypesToFlatTypes(effectiveJsonTypes(&p.Projection))
	if hasNull {
		mustExist = false
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

func jsonTypesToFlatTypes(jsonTypes []string) ([]FlatType, bool) {
	var flatTypes []FlatType
	var hasNullType bool
	for _, ty := range jsonTypes {
		switch ty {
		case "string":
			flatTypes = append(flatTypes, STRING)
		case "integer":
			flatTypes = append(flatTypes, INTEGER)
		case "number":
			flatTypes = append(flatTypes, NUMBER)
		case "boolean":
			flatTypes = append(flatTypes, BOOLEAN)
		case "object":
			flatTypes = append(flatTypes, OBJECT)
		case "array":
			flatTypes = append(flatTypes, ARRAY)
		case "null":
			hasNullType = true
		}
	}

	return flatTypes, hasNullType
}

// effectiveJsonTypes potentially "condenses" the list of JSON types from a projection's Inference
// into fewer types. Currently this supports cases where the projected field has exactly two types
// with one of them being a string. The string must have a "format" matching the JSON type of the
// non-string type, and the format must be one of the allowed formats that Flow handles. Fields for
// collection keys are not condensed.
func effectiveJsonTypes(projection *pf.Projection) []string {
	if projection.IsPrimaryKey || projection.Inference.String_ == nil {
		return projection.Inference.Types
	}

	format := projection.Inference.String_.Format
	if !SliceContains(format, effectiveTypeFormats) {
		return projection.Inference.Types
	}

	applicableType, ok := singleNonStringField(projection.Inference.Types)
	if !ok {
		return projection.Inference.Types
	}

	if format != applicableType {
		return projection.Inference.Types
	}

	return []string{applicableType}
}

func singleNonStringField(fields []string) (string, bool) {
	if len(fields) != 2 {
		return "", false
	}

	if !SliceContains(pf.JsonTypeString, fields) {
		return "", false
	}

	for _, f := range fields {
		if f != pf.JsonTypeString {
			return f, true
		}
	}

	// Should never get here - would require both types to be "string".
	return "", false
}

type MappedType struct {
	// DDL is the "CREATE TABLE" DDL type for this mapping, suited for direct inclusion in raw SQL.
	DDL string
	// Converter of tuple elements for this mapping, into SQL runtime values.
	Converter ElementConverter
	// ParsedFieldConfig is a Dialect-defined parsed implementation of the (optional)
	// additional field configuration supplied within the field selection.
	ParsedFieldConfig interface{}
}

// ElementConverter maps from a TupleElement into a runtime type instance that's compatible with the SQL driver.
type ElementConverter func(tuple.TupleElement) (interface{}, error)

// TupleConverter maps from a Tuple into a slice of runtime type instances that are compatible with the SQL driver.
type TupleConverter func(tuple.Tuple) ([]interface{}, error)

// NewTupleConverter builds a TupleConverter from an ordered list of ElementConverter.
func NewTupleConverter(e ...ElementConverter) TupleConverter {
	return func(t tuple.Tuple) (out []interface{}, err error) {
		out = make([]interface{}, len(e))
		for i := range e {
			if out[i], err = e[i](t[i]); err != nil {
				return nil, fmt.Errorf("converting tuple index %d: %w", i, err)
			}
		}
		return out, nil
	}
}

// StaticMapper is a TypeMapper which returns a consistent MappedType.
type StaticMapper MappedType

var _ TypeMapper = StaticMapper{}

type StaticMapperOption func(*StaticMapper)

func (sm StaticMapper) MapType(*Projection) (MappedType, error) {
	return (MappedType)(sm), nil
}

func NewStaticMapper(ddl string, opts ...StaticMapperOption) StaticMapper {
	sm := StaticMapper{
		DDL:               ddl,
		Converter:         func(te tuple.TupleElement) (interface{}, error) { return te, nil },
		ParsedFieldConfig: nil,
	}

	for _, o := range opts {
		o(&sm)
	}

	return sm
}

func WithElementConverter(converter ElementConverter) StaticMapperOption {
	return func(sm *StaticMapper) {
		sm.Converter = converter
	}
}

// StringCastConverter builds an ElementConverter from the string-handling callback. Any non-string
// types are returned without modification. This should be used for converting fields that have a
// string type and one additional type. The callback should convert the string into the desired type
// per the endpoint's requirements.
func StringCastConverter(fn func(string) (interface{}, error)) ElementConverter {
	return func(te tuple.TupleElement) (interface{}, error) {
		switch tt := te.(type) {
		case string:
			return fn(tt)
		default:
			return te, nil
		}
	}
}

// StdStrToInt builds an ElementConverter that attempts to convert a string into an int64 value. It
// can be used for endpoints that do not require more digits than an int64 can provide.
func StdStrToInt() ElementConverter {
	return StringCastConverter(func(str string) (interface{}, error) {
		out, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("could not convert %q to int64: %w", str, err)
		}

		return out, nil
	})
}

// StdStrToFloat builds an ElementConverter that attempts to convert a string into an float64 value.
// It can be used for endpoints that do not require more digits than an float64 can provide.
func StdStrToFloat() ElementConverter {
	return StringCastConverter(func(str string) (interface{}, error) {
		out, err := strconv.ParseFloat(str, 64)
		if err != nil {
			return nil, fmt.Errorf("could not convert %q to float64: %w", str, err)
		}

		return out, nil
	})
}

// NullableMapper wraps a ColumnMapper to add "NULL" and/or "NOT NULL" to the generated SQL type
// depending on the nullability of the column. Most databases will assume that a column may contain
// null as long as it isn't declared with a NOT NULL constraint, but some databases (e.g. ms sql
// server) make that behavior configurable, requiring the DDL to explicitly declare a column with
// NULL if it may contain null values. This wrapper will handle either or both cases.
type NullableMapper struct {
	NotNullText, NullableText string
	Delegate                  TypeMapper
}

var _ TypeMapper = NullableMapper{}

func (m NullableMapper) MapType(p *Projection) (mapped MappedType, err error) {
	if mapped, err = m.Delegate.MapType(p); err != nil {
		return
	}
	// We are temporarily creating all non-pk columns as nullable, this is to allow
	// fields to be made nullable from a collection without causing issues. Once we
	// support automatic schema migration of tables to mark fields as nullable
	// after creation, we can revert back to the more strict behaviour,
	// see: https://github.com/estuary/connectors/issues/447
	// note that for primary keys we are leaving it up to the engine to decide
	// whether they can be nullable or not (e.g. postgres will assume not
	// null for primary keys)
	if !p.IsPrimaryKey && m.NullableText != "" {
		mapped.DDL += " " + m.NullableText
	}

	return
}

// StringTypeMapper is a special TypeMapper for string type columns, which can take the format
// and/or content type into account when deciding what sql column type to generate.
type StringTypeMapper struct {
	WithFormat      map[string]TypeMapper
	WithContentType map[string]TypeMapper
	Fallback        TypeMapper
}

var _ TypeMapper = StringTypeMapper{}

func (m StringTypeMapper) MapType(p *Projection) (MappedType, error) {
	if flat, _ := p.AsFlatType(); flat != STRING && m.Fallback == nil {
		return ErrorMapper{}.MapType(p)
	} else if flat != STRING {
		return m.Fallback.MapType(p)
	} else if delegate, ok := m.WithFormat[p.Inference.String_.Format]; ok {
		return delegate.MapType(p)
	} else if delegate, ok := m.WithContentType[p.Inference.String_.ContentType]; ok {
		return delegate.MapType(p)
	} else if m.Fallback == nil {
		return ErrorMapper{}.MapType(p)
	} else {
		return m.Fallback.MapType(p)
	}
}

// ProjectionTypeMapper selects an inner TypeMapper based on a Projection's FlatType.
type ProjectionTypeMapper map[FlatType]TypeMapper

var _ TypeMapper = ProjectionTypeMapper{}

func (m ProjectionTypeMapper) MapType(p *Projection) (MappedType, error) {
	var flat, _ = p.AsFlatType()

	if delegate, ok := m[flat]; ok {
		return delegate.MapType(p)
	} else {
		return ErrorMapper{}.MapType(p)
	}
}

// MaxLengthMapper checks if the projection is a STRING type Projection having a MaxLength.
// If it is, it invokes WithLength with the MaxLength to map the Projection and returns
// the result. Otherwise, it invokes and returns Fallback.
type MaxLengthMapper struct {
	WithLength           TypeMapper
	WithLengthFmtPattern string
	Fallback             TypeMapper
}

var _ TypeMapper = MaxLengthMapper{}

func (m MaxLengthMapper) MapType(p *Projection) (MappedType, error) {
	var flat, _ = p.AsFlatType()

	if flat != STRING || p.Inference.String_.MaxLength == 0 {
		return m.Fallback.MapType(p)
	} else if mapped, err := m.WithLength.MapType(p); err != nil {
		return MappedType{}, err
	} else {
		mapped.DDL = fmt.Sprintf(m.WithLengthFmtPattern, mapped.DDL, p.Inference.String_.MaxLength)
		return mapped, nil
	}
}

// ErrorMapper returns a mapping error for the Projection.
type ErrorMapper struct{}

var _ TypeMapper = ErrorMapper{}

func (m ErrorMapper) MapType(p *Projection) (MappedType, error) {
	return MappedType{}, fmt.Errorf("unable to map field %s with type %s", p.Field, p.Inference.Types)
}
