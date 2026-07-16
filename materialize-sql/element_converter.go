package sql

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/estuary/connectors/go/logsanitize"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
)

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

func passThrough(te tuple.TupleElement) (any, error) { return te, nil }

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
		return nil, fmt.Errorf("could not convert %s (%T) to string in ToStr", logsanitize.Goval(te), te)
	}
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
			return nil, fmt.Errorf("could not serialize %s as json bytes: %w", logsanitize.Quoted(te), err)
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

// StrToInt builds an ElementConverter that attempts to convert a string into a big.Int. Strings
// formatted as integers are often larger than the maximum that can be represented by an int64. The
// concrete type of the return value is a big.Int which will be represented correctly as JSON, but
// may not be directly usable by a SQL driver as a parameter.
var StrToInt ElementConverter = StringCastConverter(func(str string) (interface{}, error) {
	// Strings ending in a 0 decimal part like "1.0" or "3.00" are considered valid as integers
	// per JSON specification so we must handle this possibility here. Anything after the
	// decimal is discarded on the assumption that Flow has validated the data and verified that
	// the decimal component is all 0's.
	if idx := strings.Index(str, "."); idx != -1 {
		str = str[:idx]
	}

	var i big.Int
	out, ok := i.SetString(str, 10)
	if !ok {
		return nil, fmt.Errorf("could not convert %s to big.Int", logsanitize.Quoted(str))
	}
	return out, nil
})

// StrToFloat builds an ElementConverter that attempts to convert a string into an float64 value.
// It can be used for endpoints that do not require more digits than an float64 can provide.
// `format: number` strings may have special values `NaN`, `Infinity`, and `-Infinity`,
// and note that a native float64-parsed value of these types is not JSON-encodable.
// The caller must provide specific sentinels to return for these special values,
// and those sentinel values may depend on what's supported by the endpoint system.
func StrToFloat(nan, posInfinity, negInfinity interface{}) ElementConverter {
	return StringCastConverter(func(str string) (interface{}, error) {
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
				return nil, fmt.Errorf("could not convert %s to float64: %w", logsanitize.Quoted(str), err)
			}
			return out, nil
		}
	})
}

const (
	minimumTimestamp = "0001-01-01T00:00:00Z"
	minimumDate      = "0001-01-01"
)

// flexibleTimestampLayouts are the near-RFC3339 variants accepted in addition to RFC3339Nano.
// Flow's schema inference can tag a field as `format: date-time` based on some values while
// other values in the same field use these variants, so rejecting them crash-loops
// otherwise-healthy shards. Only variants that fully specify the instant are accepted: a
// timestamp without a timezone offset may be local time, and assuming a zone for it would
// silently shift the data.
var flexibleTimestampLayouts = []string{
	"2006-01-02 15:04:05.999999999Z07:00",
}

// ParseRFC3339Nano parses str as RFC3339 or a near-RFC3339 variant, returning the parsed
// time and its canonical RFC3339 rendering. Inputs that are already valid RFC3339 are returned
// byte-identical (after repairing a lowercase "z"); only fallback layouts are reformatted.
func ParseRFC3339Nano(str string) (time.Time, string, error) {
	str = strings.Replace(str, "z", "Z", 1)
	if parsed, err := time.Parse(time.RFC3339Nano, str); err == nil {
		return parsed, str, nil
	}
	for _, layout := range flexibleTimestampLayouts {
		if parsed, err := time.Parse(layout, str); err == nil {
			return parsed, parsed.Format(time.RFC3339Nano), nil
		}
	}
	return time.Time{}, "", fmt.Errorf("could not parse %s as RFC3339 date-time", logsanitize.Quoted(str))
}

// NormalizeDatetimeString rewrites near-RFC3339 datetimes (lowercase "z", space separator)
// to canonical RFC3339. Values it cannot parse are passed through unmodified,
// since this converter has always been a pass-through and endpoints may accept formats it does
// not recognize.
var NormalizeDatetimeString ElementConverter = StringCastConverter(func(str string) (interface{}, error) {
	if _, canonical, err := ParseRFC3339Nano(str); err == nil {
		return canonical, nil
	}
	return strings.Replace(str, "z", "Z", 1), nil
})

// ClampDatetime provides handling for endpoints that do not accept "0000" as a year by replacing
// these datetimes with minimumTimestamp.
var ClampDatetime ElementConverter = StringCastConverter(func(str string) (interface{}, error) {
	parsed, canonical, err := ParseRFC3339Nano(str)
	if err != nil {
		return nil, err
	} else if parsed.Year() == 0 {
		return minimumTimestamp, nil
	}
	return canonical, nil
})

// ClampDate is like ClampDatetime but just for dates.
var ClampDate ElementConverter = StringCastConverter(func(str string) (interface{}, error) {
	if parsed, err := time.Parse(time.DateOnly, str); err != nil {
		return nil, err
	} else if parsed.Year() == 0 {
		return minimumDate, nil
	}
	return str, nil
})

// Base64Decoder decodes a base64-encoded tuple element into raw bytes. It is
// used by materializations that store binary fields in native byte-typed
// columns and bind the decoded bytes directly to their driver, rather than
// decoding via SQL on the server side.
var Base64Decoder ElementConverter = func(te tuple.TupleElement) (any, error) {
	switch v := te.(type) {
	case nil:
		return nil, nil
	case []byte:
		return base64.StdEncoding.AppendDecode(nil, v)
	case string:
		return base64.StdEncoding.DecodeString(v)
	case json.RawMessage:
		// A base64 value that arrives through a json.RawMessage is still
		// wrapped in JSON string quotes.
		return base64.StdEncoding.AppendDecode(nil, bytes.Trim(v, `"`))
	default:
		return nil, fmt.Errorf("Base64Decoder: unexpected value type %T", te)
	}
}

// Base64ToHex re-encodes a base64-encoded tuple element as a lowercase hex
// string. This is used by warehouses (e.g. Redshift) whose bulk loaders parse
// VARBYTE/BINARY values from JSON or CSV staging files as hex by default. By
// emitting hex from the connector, the loader can decode binary directly into
// the native column without an intermediate staging step.
var Base64ToHex ElementConverter = func(te tuple.TupleElement) (any, error) {
	raw, err := Base64Decoder(te)
	if err != nil {
		return nil, fmt.Errorf("Base64ToHex: %w", err)
	}
	if raw == nil {
		return nil, nil
	}
	return hex.EncodeToString(raw.([]byte)), nil
}
