package boilerplate

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// ElementConverter converts a Flow collection value to a value that is ready to
// store in the destination.
type ElementConverter func(tuple.TupleElement) (any, error)

// MappedType represents all of the information needed to materialize a field.
type MappedType[T any] struct {
	// EndpointType is the destination-specific information about how to create
	// the "column" (or equivalent concept) in the destination system.
	EndpointType T
	// Converter is the function that will convert the Flow collection value
	// into a value compatible with the destination.
	Converter ElementConverter
}

// Projection wraps a Flow projection with additional useful computed values
// that are commonly used to define destination-specific types.
type Projection struct {
	pf.Projection

	// MustExist means that the value can never be present with a value of
	// "null", and that it also can never be completely absent. Most systems
	// will use this to determine if the destination should be nullable or not.
	MustExist bool
	// TypesWithoutNull is a list of JSON types for the field excluding the
	// explicit "null".
	TypesWithoutNull []string
	// NumericString is a special case of fields having schemas with strings
	// with numeric formats possibly combined with those specific numeric types,
	// where the values should usually be materialized as the numeric type. For
	// example, a field with a schema of `{type: ["string", "number"], format:
	// "number"}` should be materialized as a "number", and the strings may need
	// to be converted to numbers.
	NumericString *StringWithNumericFormat
}

func (p Projection) IsNumericString() bool {
	return p.NumericString != nil
}

func parseFieldConfig[FC any](field string, fieldConfigJsonMap map[string]json.RawMessage) (*FC, error) {
	raw, ok := fieldConfigJsonMap[field]
	if !ok {
		return nil, nil
	}

	parsedFieldConfig := new(FC)
	if err := json.Unmarshal(raw, parsedFieldConfig); err != nil {
		return nil, err
	}

	return parsedFieldConfig, nil
}

// MapTypeFn is a function that maps Flow fields to destination-specific types,
// using a Projection and parsed field configuration. fc will be `nil` if there
// is no user-defined field configuration. Implementations may return `nil` for
// the ElementConverter to use the PassThrough as a default.
type MapTypeFn[T any, FC any] func(p Projection, fc *FC) (T, ElementConverter)

type TypeMapper[T any, FC any] struct {
	mapFn MapTypeFn[T, FC]
}

func NewTypeMapper[T any, FC any](mapFn MapTypeFn[T, FC]) TypeMapper[T, FC] {
	return TypeMapper[T, FC]{mapFn: mapFn}
}

// Map maps a Flow field to a destination-specific type.
func (m TypeMapper[T, FC]) Map(p *pf.Projection, fieldConfigJsonMap map[string]json.RawMessage) (*MappedType[T], error) {
	parsedFieldConfig, err := parseFieldConfig[FC](p.Field, fieldConfigJsonMap)
	if err != nil {
		return nil, fmt.Errorf("parsing field config: %w", err)
	}

	mapped, converter := m.mapFn(buildProjection(p), parsedFieldConfig)
	if converter == nil {
		converter = PassThrough
	}

	return &MappedType[T]{
		EndpointType: mapped,
		Converter:    converter,
	}, nil
}

func buildProjection(p *pf.Projection) Projection {
	out := Projection{
		Projection: *p,
		MustExist:  p.Inference.Exists == pf.Inference_MUST,
	}

	if format, ok := AsFormattedNumeric(p); ok && !p.IsPrimaryKey {
		out.NumericString = &format
	}

	for _, ty := range p.Inference.Types {
		switch ty {
		case "null":
			out.MustExist = false
		default:
			out.TypesWithoutNull = append(out.TypesWithoutNull, ty)
		}
	}

	return out
}

// PassThrough passes the value through unchanged. This is the default
// converter, unless an alternate is specified.
func PassThrough(te tuple.TupleElement) (any, error) {
	return te, nil
}

// ToJsonBytes encodes a value to raw JSON bytes. Pre-encoded []byte values and
// json.RawMessages are passed through as-is, and other values are serialized as
// JSON.
func ToJsonBytes(te tuple.TupleElement) (any, error) {
	switch ii := te.(type) {
	case []byte:
		// Runtime tuple encodings for the pre-encoded JSON of object and array
		// field types.
		return json.RawMessage(ii), nil
	case json.RawMessage:
		// The document field.
		return ii, nil
	case nil:
		// An absent or "null" field.
		return nil, nil
	default:
		// Everything else is serialized to its JSON representation.
		bytes, err := json.Marshal(te)
		if err != nil {
			return nil, fmt.Errorf("could not serialize %q as json bytes: %w", te, err)
		}

		return json.RawMessage(bytes), nil
	}
}

// ToJsonString converts the input to "stringified" JSON, using ToJsonBytes for
// the serialization. A `nil` input returns a `nil` output.
func ToJsonString(te tuple.TupleElement) (any, error) {
	if te == nil {
		return nil, nil
	}

	bytes, err := ToJsonBytes(te)
	if err != nil {
		return nil, err
	}

	return string(bytes.(json.RawMessage)), nil
}

// ToStr converts a value to a string. A `nil` input returns a `nil` output.
func ToStr(te tuple.TupleElement) (any, error) {
	switch value := te.(type) {
	case json.RawMessage:
		return string(value), nil
	case []byte:
		return string(value), nil
	case string:
		return value, nil
	case bool:
		return strconv.FormatBool(value), nil
	case int64:
		return strconv.Itoa(int(value)), nil
	case uint64:
		return strconv.FormatUint(value, 10), nil
	case float64:
		return strconv.FormatFloat(value, 'f', -1, 64), nil
	case nil:
		return nil, nil
	default:
		return nil, fmt.Errorf("could not convert %#v (%T) to string in ToStr", te, te)
	}
}

// DecodeBase64String converts a base64-encoded string into a byte array.
func DecodeBase64String(te tuple.TupleElement) (any, error) {
	switch t := te.(type) {
	case string:
		bytes, err := base64.StdEncoding.DecodeString(t)
		if err != nil {
			return nil, fmt.Errorf("decoding bytes from string: %w", err)
		}

		return bytes, nil
	case nil:
		return nil, nil
	default:
		return nil, fmt.Errorf("convertBase64 unsupported type %T (%#v)", te, te)
	}
}

// StringCastConverter builds an ElementConverter from the string-handling
// callback. Any non-string types are returned without modification.
func StringCastConverter(fn func(string) (any, error)) ElementConverter {
	return func(te tuple.TupleElement) (any, error) {
		switch tt := te.(type) {
		case string:
			return fn(tt)
		default:
			return te, nil
		}
	}
}

// StrToInt converts a string into a big.Int. Strings formatted as integers are
// often larger than the maximum that can be represented by an int64. The
// concrete type of the return value is a big.Int which will be serialized
// appropriately as JSON, but may not be directly usable as a parameter
// otherwise.
var StrToInt ElementConverter = StringCastConverter(func(str string) (any, error) {
	// Strings ending in a 0 decimal part like "1.0" or "3.00" are
	// considered valid as integers per JSON specification so we must handle
	// this possibility here. Anything after the decimal is discarded on the
	// assumption that Flow has validated the data and verified that the
	// decimal component is all 0's.
	if idx := strings.Index(str, "."); idx != -1 {
		str = str[:idx]
	}

	var i big.Int
	out, ok := i.SetString(str, 10)
	if !ok {
		return nil, fmt.Errorf("could not convert %q to big.Int", str)
	}
	return out, nil
})

// StrToFloat converts a string into an float64 value. `format: number` strings
// may have special values `NaN`, `Infinity`, and `-Infinity`, and note that a
// native float64-parsed value of these types is not JSON-encodable. The caller
// must provide specific sentinels to return for these special values, and those
// sentinel values may depend on what's supported by the endpoint system.
func StrToFloat(nan, posInfinity, negInfinity any) ElementConverter {
	return StringCastConverter(func(str string) (any, error) {
		switch str {
		case "NaN":
			return nan, nil
		case "Infinity":
			return posInfinity, nil
		case "-Infinity":
			return negInfinity, nil
		default:
			out, err := strconv.ParseFloat(str, 64)
			if err != nil {
				return nil, fmt.Errorf("could not convert %q to float64: %w", str, err)
			}
			return out, nil
		}
	})
}

const (
	minimumTimestamp = "0001-01-01T00:00:00Z"
	minimumDate      = "0001-01-01"
)

// ClampDatetime provides handling for endpoints that do not accept "0000" as a year by replacing
// these datetimes with minimumTimestamp.
var ClampDatetime ElementConverter = StringCastConverter(func(str string) (any, error) {
	if parsed, err := time.Parse(time.RFC3339Nano, str); err != nil {
		return nil, err
	} else if parsed.Year() == 0 {
		return minimumTimestamp, nil
	}
	return str, nil
})

// ClampDate is like ClampDatetime but just for dates.
var ClampDate ElementConverter = StringCastConverter(func(str string) (any, error) {
	if parsed, err := time.Parse(time.DateOnly, str); err != nil {
		return nil, err
	} else if parsed.Year() == 0 {
		return minimumDate, nil
	}
	return str, nil
})

// HydrateObject is used to create an object from JSON bytes.
func HydrateObject(te tuple.TupleElement) (any, error) {
	var out any

	switch b := te.(type) {
	case []byte:
		if err := json.Unmarshal(b, &out); err != nil {
			return nil, err
		}
	case json.RawMessage:
		if err := json.Unmarshal(b, &out); err != nil {
			return nil, err
		}
	case nil:
		return nil, nil
	default:
		return nil, fmt.Errorf("invalid type %T (%#v) for variant", te, te)
	}

	return out, nil
}

type StringWithNumericFormat string

const (
	StringFormatInteger StringWithNumericFormat = "stringFormatInteger"
	StringFormatNumber  StringWithNumericFormat = "stringFormatNumber"
)

func AsFormattedNumeric(projection *pf.Projection) (StringWithNumericFormat, bool) {
	typesMatch := func(actual, allowed []string) bool {
		for _, t := range actual {
			if !slices.Contains(allowed, t) {
				return false
			}
		}
		return true
	}

	if !projection.IsPrimaryKey && projection.Inference.String_ != nil {
		switch {
		case projection.Inference.String_.Format == "integer" && typesMatch(projection.Inference.Types, []string{"integer", "null", "string"}):
			return StringFormatInteger, true
		case projection.Inference.String_.Format == "number" && typesMatch(projection.Inference.Types, []string{"null", "number", "string"}):
			return StringFormatNumber, true
		default:
			// Fallthrough.
		}
	}

	// Not a formatted numeric field.
	return "", false
}
