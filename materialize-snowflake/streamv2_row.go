package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"unicode/utf8"
)

// rowColumn is one column of a binding's row object, as the streaming v2 write
// path writes it.
type rowColumn struct {
	// prefix introduces the column's value: its quoted and escaped name followed
	// by a colon, preceded by the comma which separates it from the value before
	// it. The first value written skips that leading byte, which is cheaper than
	// tracking a separator across a loop whose iterations are skipped for nil.
	prefix []byte
	// name reports the column in an encoding failure, where the prefix's escaped
	// form would be noise.
	name string
}

// rowColumnsOf precomputes the row-object prefix of each named column, so that
// every stored row pays neither the quoting nor the escaping of the names it
// repeats.
func rowColumnsOf(names []string) []rowColumn {
	var columns = make([]rowColumn, len(names))
	for i, name := range names {
		var prefix = appendJSONString([]byte{','}, name)
		columns[i] = rowColumn{prefix: append(prefix, ':'), name: name}
	}
	return columns
}

// appendRowJSON writes one converted document into buf as a JSON object keyed by
// column name, and returns the extended buffer. Values which arrived already
// encoded are copied through, and the rest are written by type rather than
// reflected over, which is what keeps a full batch's encoding off the profile.
//
// A column whose value is nil is omitted from the object entirely. The column
// then defaults to SQL NULL, which is what the staged paths store for a nil.
// Writing the nil out instead would encode a JSON null, and for a VARIANT column
// the SDK ingests that as a VARIANT holding null — a value which answers
// `IS NULL` and `TYPEOF` differently from the SQL NULL the same document produces
// through a staged file.
//
// A value JSON cannot represent fails the row, leaving buf as it was given: a
// half-written row would otherwise corrupt the payload of every row batched
// alongside it, rather than just failing this one.
func appendRowJSON(buf []byte, columns []rowColumn, converted []any) ([]byte, error) {
	var start = len(buf)
	buf = append(buf, '{')

	var first = true
	for i, value := range converted {
		if value == nil {
			continue
		}

		var prefix = columns[i].prefix
		if first {
			prefix, first = prefix[1:], false
		}
		buf = append(buf, prefix...)

		var err error
		if buf, err = appendValueJSON(buf, value); err != nil {
			return buf[:start], fmt.Errorf("column %s: %w", columns[i].name, err)
		}
	}

	return append(buf, '}'), nil
}

// appendValueJSON writes one converted value as JSON. The cases are the value
// types the dialect's converters produce, plus the raw tuple elements of the
// column types they leave unconverted; anything else is left to the standard
// encoder, which covers the *big.Int of a STRING_INTEGER column and whatever a
// future converter introduces.
//
// []byte and json.RawMessage are both byte slices but cannot share a case: a
// BINARY column's bytes have to be base64'd, where a VARIANT column's are already
// JSON and are copied through. A type switch separates them by exact dynamic
// type, so each gets the treatment its column needs.
func appendValueJSON(buf []byte, value any) ([]byte, error) {
	switch v := value.(type) {
	case string:
		return appendJSONString(buf, v), nil
	case json.RawMessage:
		// An empty payload is left to the standard encoder, which writes a nil
		// RawMessage as null and refuses an empty non-nil one.
		if len(v) == 0 {
			return appendMarshaled(buf, value)
		}
		return appendPreEncoded(buf, v)
	case []byte:
		if v == nil {
			return append(buf, "null"...), nil
		}
		buf = append(buf, '"')
		buf = base64.StdEncoding.AppendEncode(buf, v)
		return append(buf, '"'), nil
	case bool:
		return strconv.AppendBool(buf, v), nil
	case int64:
		return strconv.AppendInt(buf, v, 10), nil
	case uint64:
		return strconv.AppendUint(buf, v, 10), nil
	case float64:
		return appendJSONFloat(buf, v)
	case *big.Int:
		// The standard encoder reaches the same digits through this type's
		// json.Marshaler, at three allocations a row for a column type — an
		// integer too wide for INTEGER, carried as a string — which is common
		// enough in collection schemas to be worth its own case.
		if v == nil {
			return append(buf, "null"...), nil
		}
		return v.Append(buf, 10), nil
	default:
		return appendMarshaled(buf, value)
	}
}

func appendMarshaled(buf []byte, value any) ([]byte, error) {
	var encoded, err = json.Marshal(value)
	if err != nil {
		return buf, err
	}
	return append(buf, encoded...), nil
}

// appendPreEncoded copies an already-encoded value through, compacting it only
// if it carries a line break. Nothing in the append framing is delimited by one
// today, but a row is written into a shared payload buffer and travels to the
// sidecar as bytes, so its freedom from line breaks should be this writer's own
// property rather than an undocumented guarantee of whoever encoded the value.
// The scan costs nothing measurable next to writing the bytes; compacting every
// value unconditionally — which is what the standard encoder did here — costs
// more than the whole writer.
//
// These bytes are JSON because the runtime's own document serialization made them
// so, by way of the ToJsonBytes converter: their syntax is the encoder's warrant,
// not something re-established here. The sidecar's decode of the payload is what
// refuses anything else, at the cost of the batch the value travelled in.
func appendPreEncoded(buf []byte, raw json.RawMessage) ([]byte, error) {
	if bytes.IndexByte(raw, '\n') < 0 && bytes.IndexByte(raw, '\r') < 0 {
		return append(buf, raw...), nil
	}

	var compacted bytes.Buffer
	if err := json.Compact(&compacted, raw); err != nil {
		return buf, err
	}
	return append(buf, compacted.Bytes()...), nil
}

// appendJSONFloat writes a float in the shortest form which round-trips,
// switching to an exponent at the magnitudes the standard encoder switches at so
// that a value reaches Snowflake in the form the staged paths write it in.
//
// NaN and ±Inf are refused, as the standard encoder refuses them: written out
// literally they are not JSON at all, and would take down every row batched with
// them rather than this one. They are practically unreachable — a JSON number
// cannot be NaN, and the dialect's StrToFloat converter maps the `format: number`
// specials to sentinel *strings* — but an unreachable input must not be able to
// corrupt a payload.
func appendJSONFloat(buf []byte, f float64) ([]byte, error) {
	if math.IsNaN(f) {
		return buf, fmt.Errorf("cannot encode NaN as JSON")
	} else if math.IsInf(f, 0) {
		return buf, fmt.Errorf("cannot encode %v as JSON", f)
	}

	var format byte = 'f'
	if abs := math.Abs(f); abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		format = 'e'
	}
	buf = strconv.AppendFloat(buf, f, format, -1, 64)

	if format == 'e' {
		// Trim the leading zero of a two-digit negative exponent: 1e-09 -> 1e-9.
		if n := len(buf); n >= 4 && buf[n-4] == 'e' && buf[n-3] == '-' && buf[n-2] == '0' {
			buf[n-2] = buf[n-1]
			buf = buf[:n-1]
		}
	}
	return buf, nil
}

const hexDigits = "0123456789abcdef"

// appendJSONString writes s as a quoted JSON string, escaping only what JSON
// requires: the quote, the backslash, and the control characters. Bytes which are
// not valid UTF-8 become the replacement character, as the standard encoder makes
// them, so that the payload is always the valid UTF-8 the sidecar decodes it as.
//
// The `<`, `>` and `&` which the standard encoder escapes for the benefit of
// HTML embedding are written literally. The decoded string is the same either
// way, so a test of this writer compares decoded values rather than bytes.
func appendJSONString(buf []byte, s string) []byte {
	buf = append(buf, '"')

	var start int
	for i := 0; i < len(s); {
		var c = s[i]
		if c >= utf8.RuneSelf {
			var r, size = utf8.DecodeRuneInString(s[i:])
			if r == utf8.RuneError && size == 1 {
				buf = append(buf, s[start:i]...)
				buf = append(buf, `�`...)
				i += size
				start = i
				continue
			}
			i += size
			continue
		} else if c >= 0x20 && c != '"' && c != '\\' {
			i++
			continue
		}

		buf = append(buf, s[start:i]...)
		switch c {
		case '"', '\\':
			buf = append(buf, '\\', c)
		case '\n':
			buf = append(buf, '\\', 'n')
		case '\r':
			buf = append(buf, '\\', 'r')
		case '\t':
			buf = append(buf, '\\', 't')
		default:
			buf = append(buf, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
		}
		i++
		start = i
	}

	buf = append(buf, s[start:]...)
	return append(buf, '"')
}
