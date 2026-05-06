package main

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// columnFamily is the single column family used by the connector. The
// specific name used isn't important; it can be anything. But keeping it
// short minimizes per-cell overhead.
const columnFamily = "f"

type mappedType struct {
	field    string
	flatType boilerplate.FlatType
	encodeFn func(tuple.TupleElement) ([]byte, error)
}

func (m mappedType) String() string {
	switch m.flatType.(type) {
	case boilerplate.FlatTypeBoolean:
		return "bool"
	case boilerplate.FlatTypeInteger:
		return "integer"
	case boilerplate.FlatTypeNumber:
		return "number"
	case boilerplate.FlatTypeString:
		return "string"
	case boilerplate.FlatTypeBinary:
		return "binary"
	case boilerplate.FlatTypeStringFormatInteger:
		return "string-format-integer"
	case boilerplate.FlatTypeStringFormatNumber:
		return "string-format-number"
	case boilerplate.FlatTypeArray, boilerplate.FlatTypeObject, boilerplate.FlatTypeMultiple, boilerplate.FlatTypeNever:
		return "json"
	default:
		panic(fmt.Sprintf("unknown flat type: %T", m.flatType))
	}
}

func (m mappedType) Compatible(existing boilerplate.ExistingField) bool { return true }
func (m mappedType) CanMigrate(existing boilerplate.ExistingField) bool { return false }

type fieldConfig struct{}

func (fieldConfig) Validate() error    { return nil }
func (fieldConfig) CastToString() bool { return false }

func mapType(p boilerplate.Projection) mappedType {
	out := mappedType{field: p.Field, flatType: p.FlatType}

	numericExceedsInt64 := func(n pf.Inference_Numeric) bool {
		return n.Maximum > math.MaxInt64 || n.Minimum < math.MinInt64
	}

	switch ft := p.FlatType.(type) {
	case boilerplate.FlatTypeBoolean:
		out.encodeFn = encodeBool
	case boilerplate.FlatTypeInteger:
		if numericExceedsInt64(ft.InferenceNumeric) {
			out.encodeFn = encodeIntegerAsString
		} else {
			out.encodeFn = encodeInteger
		}
	case boilerplate.FlatTypeNumber:
		out.encodeFn = encodeNumber
	case boilerplate.FlatTypeString:
		out.encodeFn = encodeString
	case boilerplate.FlatTypeBinary:
		out.encodeFn = encodeBinary
	case boilerplate.FlatTypeStringFormatInteger:
		// 18 digits always fit in int64; a 19-digit value can exceed MaxInt64.
		if numericExceedsInt64(ft.InferenceNumeric) || ft.InferenceString.MaxLength > 18 {
			out.encodeFn = encodeIntegerAsString
		} else {
			out.encodeFn = encodeStringFormatInt
		}
	case boilerplate.FlatTypeStringFormatNumber:
		// float64 reliably preserves ~17 significant decimal digits;
		// strings longer than that may carry more precision than the
		// 8-byte IEEE 754 encoding can hold.
		if ft.InferenceString.MaxLength > 17 {
			out.encodeFn = encodeNumberAsString
		} else {
			out.encodeFn = encodeStringFormatNum
		}
	case boilerplate.FlatTypeArray, boilerplate.FlatTypeObject, boilerplate.FlatTypeMultiple, boilerplate.FlatTypeNever:
		out.encodeFn = encodeJSON
	default:
		panic(fmt.Sprintf("unhandled flat type %T for field %q", p.FlatType, p.Field))
	}

	return out
}

func (m mappedType) encode(te tuple.TupleElement) ([]byte, error) {
	if te == nil {
		return []byte{}, nil
	}

	return m.encodeFn(te)
}

func encodeBool(te tuple.TupleElement) ([]byte, error) {
	if b, ok := te.(bool); !ok {
		return nil, fmt.Errorf("bool field expected bool for field, got %T", te)
	} else if b {
		return []byte{0x01}, nil
	} else {
		return []byte{0x00}, nil
	}
}

func encodeInteger(te tuple.TupleElement) ([]byte, error) {
	out := make([]byte, 8)
	switch v := te.(type) {
	case int:
		binary.BigEndian.PutUint64(out, uint64(int64(v)))
	case int64:
		binary.BigEndian.PutUint64(out, uint64(v))
	case uint64:
		binary.BigEndian.PutUint64(out, v)
	case float64:
		binary.BigEndian.PutUint64(out, uint64(int64(v))) // Truncate trailing zero fraction
	default:
		return nil, fmt.Errorf("integer field expected numeric, got %T", te)
	}

	return out, nil
}

func encodeIntegerAsString(te tuple.TupleElement) ([]byte, error) {
	switch v := te.(type) {
	case string:
		if idx := strings.Index(v, "."); idx != -1 {
			v = v[:idx]
		}
		return []byte(v), nil
	case int:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int64:
		return strconv.AppendInt(nil, v, 10), nil
	case uint64:
		return strconv.AppendUint(nil, v, 10), nil
	case float64:
		return strconv.AppendInt(nil, int64(v), 10), nil
	default:
		return nil, fmt.Errorf("integer field expected string or numeric, got %T", te)
	}
}

func encodeNumber(te tuple.TupleElement) ([]byte, error) {
	var f float64
	switch v := te.(type) {
	case float64:
		f = v
	case int:
		f = float64(v)
	case int64:
		f = float64(v)
	case uint64:
		f = float64(v)
	case string:
		switch v {
		case "NaN":
			f = math.NaN()
		case "Infinity":
			f = math.Inf(1)
		case "-Infinity":
			f = math.Inf(-1)
		default:
			return nil, fmt.Errorf("number field got unexpected string %q", v)
		}
	default:
		return nil, fmt.Errorf("number field expected numeric, got %T", te)
	}

	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, math.Float64bits(f))

	return out, nil
}

func encodeNumberAsString(te tuple.TupleElement) ([]byte, error) {
	switch v := te.(type) {
	case string:
		return []byte(v), nil
	case float64:
		switch {
		case math.IsNaN(v):
			return []byte("NaN"), nil
		case math.IsInf(v, 1):
			return []byte("Infinity"), nil
		case math.IsInf(v, -1):
			return []byte("-Infinity"), nil
		default:
			return strconv.AppendFloat(nil, v, 'g', -1, 64), nil
		}
	case int:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int64:
		return strconv.AppendInt(nil, v, 10), nil
	case uint64:
		return strconv.AppendUint(nil, v, 10), nil
	default:
		return nil, fmt.Errorf("number field expected string or numeric, got %T", te)
	}
}

func encodeString(te tuple.TupleElement) ([]byte, error) {
	if s, ok := te.(string); ok {
		return []byte(s), nil
	} else {
		return nil, fmt.Errorf("string field expected string, got %T", te)
	}
}

func encodeBinary(te tuple.TupleElement) ([]byte, error) {
	if s, ok := te.(string); ok {
		return base64.StdEncoding.DecodeString(s)
	} else {
		return nil, fmt.Errorf("binary field expected string, got %T", te)
	}
}

func encodeStringFormatInt(te tuple.TupleElement) ([]byte, error) {
	s, ok := te.(string)
	if !ok {
		return encodeInteger(te)
	}

	if idx := strings.Index(s, "."); idx != -1 {
		s = s[:idx]
	}

	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return encodeInteger(n)
	} else if u, err := strconv.ParseUint(s, 10, 64); err == nil {
		// Fall back to unsigned for values that overflow int64 but fit uint64.
		return encodeInteger(u)
	}

	return nil, fmt.Errorf("parsing integer-formatted string %q", s)
}

func encodeStringFormatNum(te tuple.TupleElement) ([]byte, error) {
	var f float64
	switch v := te.(type) {
	case string:
		switch v {
		case "NaN":
			f = math.NaN()
		case "Infinity":
			f = math.Inf(1)
		case "-Infinity":
			f = math.Inf(-1)
		default:
			parsed, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, fmt.Errorf("parsing number-formatted string %q: %w", v, err)
			}
			f = parsed
		}
	case float64:
		f = v
	case int:
		f = float64(v)
	case int64:
		f = float64(v)
	case uint64:
		f = float64(v)
	default:
		return nil, fmt.Errorf("string-format-number field expected string, got %T", te)
	}

	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, math.Float64bits(f))

	return out, nil
}

func encodeJSON(te tuple.TupleElement) ([]byte, error) {
	if b, ok := te.([]byte); ok {
		// Pre-serialized JSON; don't re-encode.
		return b, nil
	}

	// Everything else gets JSON-marshaled to its textual JSON form.
	return json.Marshal(te)
}
