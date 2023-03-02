package sql

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"github.com/estuary/connectors/go/protocol"
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

// Projection lifts a pf.Projection into a form that's more easily worked with for SQL column mapping.
type Projection struct {
	*protocol.Projection
	// Comment for this projection.
	Comment string
	// RawFieldConfig is (optional) field configuration supplied within the field selection.
	RawFieldConfig json.RawMessage
}

// BuildProjections returns the Projections extracted from a Binding.
func BuildProjections(spec protocol.ApplyBinding) (keys, values []Projection, document *Projection) {
	var do = func(field string) Projection {
		var p = Projection{
			Projection:     spec.Collection.GetProjection(field),
			RawFieldConfig: spec.FieldSelection.FieldConfig[field],
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
	if field := spec.FieldSelection.Document; field != nil {
		document = new(Projection)
		*document = do(*field)
	}

	return
}

// AsFlatType returns the Projection's FlatType.
func (p *Projection) AsFlatType() (_ FlatType, mustExist bool) {
	mustExist = p.Inference.Exists == protocol.MustExist

	var types []FlatType
	for _, ty := range effectiveJsonTypes(p.Projection) {
		switch ty {
		case "string":
			types = append(types, STRING)
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
		case "null":
			mustExist = false
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

// effectiveJsonTypes potentially maps the provided JSON types into alternate types for
// materialization. It currently supports strings formatted as numeric values, either as stand-alone
// string types or as a string type formatted as numeric + that numeric type. It does not apply to
// keyed fields.
func effectiveJsonTypes(projection *protocol.Projection) []string {
	if !projection.IsPrimaryKey && projection.Inference.String_ != nil {
		switch {
		case projection.Inference.String_.Format == "integer" && reflect.DeepEqual(projection.Inference.Types, []string{"integer", "string"}):
			return []string{"integer"}
		case projection.Inference.String_.Format == "number" && reflect.DeepEqual(projection.Inference.Types, []string{"number", "string"}):
			return []string{"number"}
		case projection.Inference.String_.Format == "integer" && reflect.DeepEqual(projection.Inference.Types, []string{"string"}):
			return []string{"integer"}
		case projection.Inference.String_.Format == "number" && reflect.DeepEqual(projection.Inference.Types, []string{"string"}):
			return []string{"number"}
		default:
			// Fallthrough, types are returned as-is.
		}
	}

	return projection.Inference.Types
}

type MappedType struct {
	// DDL is the "CREATE TABLE" DDL type for this mapping, suited for direct inclusion in raw SQL.
	DDL string
	// Converter of JSON values for this mapping, into SQL runtime values.
	Converter ValueConverter
	// ParsedFieldConfig is a Dialect-defined parsed implementation of the (optional)
	// additional field configuration supplied within the field selection.
	ParsedFieldConfig interface{}
}

// ValueConverter maps from a JSON value into a runtime type instance that's compatible with the SQL
// driver.
type ValueConverter func(interface{}) (interface{}, error)

// ValuesConverter maps from a slice of JSON values into a slice of runtime type instances that are
// compatible with the SQL driver.
type ValuesConverter func([]interface{}) ([]interface{}, error)

// NewValuesConverter builds a ValuesConverter from an ordered list of ValueConverter.
func NewValuesConverter(e ...ValueConverter) ValuesConverter {
	return func(t []interface{}) (out []interface{}, err error) {
		out = make([]interface{}, len(e))
		for i := range e {
			if out[i], err = e[i](t[i]); err != nil {
				return nil, fmt.Errorf("converting value index %d: %w", i, err)
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
		Converter:         func(te interface{}) (interface{}, error) { return te, nil },
		ParsedFieldConfig: nil,
	}

	for _, o := range opts {
		o(&sm)
	}

	return sm
}

func WithElementConverter(converter ValueConverter) StaticMapperOption {
	return func(sm *StaticMapper) {
		sm.Converter = converter
	}
}

// JsonBytesConverter serializes a value to raw JSON bytes for storage in an endpoint compatible
// with JSON bytes.
func JsonBytesConverter(v interface{}) (interface{}, error) {
	switch ii := v.(type) {
	case []byte:
		return json.RawMessage(ii), nil
	case json.RawMessage:
		return ii, nil
	case nil:
		return json.RawMessage(nil), nil
	default:
		bytes, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("could not serialize %q as json bytes: %w", v, err)
		}

		return json.RawMessage(bytes), nil
	}
}

// StringCastConverter builds an ElementConverter from the string-handling callback. Any non-string
// types are returned without modification. This should be used for converting fields that have a
// string type and one additional type. The callback should convert the string into the desired type
// per the endpoint's requirements.
func StringCastConverter(fn func(string) (interface{}, error)) ValueConverter {
	return func(v interface{}) (interface{}, error) {
		switch tt := v.(type) {
		case string:
			return fn(tt)
		default:
			return v, nil
		}
	}
}

// StdStrToInt builds an ElementConverter that attempts to convert a string into an int64 value. It
// can be used for endpoints that do not require more digits than an int64 can provide.
func StdStrToInt() ValueConverter {
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
func StdStrToFloat() ValueConverter {
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
	} else if _, notNull := p.AsFlatType(); notNull && m.NotNullText != "" {
		mapped.DDL += " " + m.NotNullText
	} else if m.NullableText != "" {
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
