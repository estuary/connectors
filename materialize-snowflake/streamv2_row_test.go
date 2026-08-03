package main

import (
	"bytes"
	"encoding/json"
	"math"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

// rowViaMap encodes a row the way the streaming v2 write path did before it wrote
// row JSON itself: a map keyed by column name, handed to encoding/json. It is the
// reference appendRowJSON is held equivalent to, and the baseline the benchmark
// at the end of this file measures the direct writer against.
func rowViaMap(names []string, converted []any) ([]byte, error) {
	var row = make(map[string]any, len(names))
	for i, name := range names {
		if converted[i] != nil {
			row[name] = converted[i]
		}
	}
	return json.Marshal(row)
}

// decodedJSON decodes JSON with numbers left as their literal text, so that
// comparing two encodings of the same row neither loses the precision of an
// integer wider than a float64 nor accepts a number written in a different form.
func decodedJSON(t *testing.T, encoded []byte) any {
	t.Helper()

	var decoder = json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()

	var out any
	require.NoError(t, decoder.Decode(&out), "decoding %s", encoded)
	return out
}

// requireEquivalent asserts the direct writer and the map-and-marshal path encode
// the same row to the same *values*.
//
// Byte equality is deliberately not the claim: encoding/json escapes `<`, `>` and
// `&` for the benefit of HTML embedding, and writes an invalid UTF-8 byte as the
// escape sequence `\ufffd` where the direct writer writes the replacement
// character itself. Those differ as bytes and agree as JSON.
func requireEquivalent(t *testing.T, names []string, converted []any) []byte {
	t.Helper()

	var want, err = rowViaMap(names, converted)
	require.NoError(t, err)

	got, err := appendRowJSON(nil, rowColumnsOf(names), converted)
	require.NoError(t, err)

	require.Equal(t, decodedJSON(t, want), decodedJSON(t, got))
	return got
}

func TestStreamV2RowValueEquivalence(t *testing.T) {
	var bigInt, ok = new(big.Int).SetString("12345678901234567890123456789012345678", 10)
	require.True(t, ok)

	for _, tc := range []struct {
		name  string
		value any
	}{
		{"bool true", true},
		{"bool false", false},

		{"int64 max", int64(math.MaxInt64)},
		{"int64 min", int64(math.MinInt64)},
		{"int64 zero", int64(0)},
		// A uint64 above MaxInt64 is what an INTEGER column carries for the upper
		// half of the unsigned range, and is not an int64 at any width.
		{"uint64 above MaxInt64", uint64(math.MaxUint64)},

		{"float64 whole", float64(42)},
		{"float64 fraction", 3.141592653589793},
		{"float64 negative zero", math.Copysign(0, -1)},
		{"float64 tiny", 1e-300},
		{"float64 huge", 1.79e308},
		// The magnitudes at which the standard encoder switches to an exponent.
		{"float64 at the exponent floor", 1e-6},
		{"float64 below the exponent floor", 9.99e-7},
		{"float64 at the exponent ceiling", 1e21},
		{"float64 below the exponent ceiling", 9.99e20},

		{"string plain", "hello"},
		{"string empty", ""},
		{"string with quotes and backslashes", `he said "hi" \ then C:\path\`},
		{"string with control characters", "\x00\x01\a\b\f\n\r\t\x1f"},
		{"string non-ASCII", "héllo 世界 🎉"},
		{"string with HTML syntax", `<a href="x">& more</a>`},
		{"string with line separators", "before\u2028\u2029after"},
		{"string invalid UTF-8", "bad \xff\xfe bytes"},
		{"string lone surrogate bytes", "lone \xed\xa0\x80 surrogate"},
		// StrToFloat maps the `format: number` specials to these sentinels, so a
		// FLOAT column's value can be a string.
		{"string NaN sentinel", "NaN"},
		{"string inf sentinel", "inf"},
		{"string -inf sentinel", "-inf"},

		{"bytes", []byte{0x00, 0x01, 0x02, 0xff}},
		{"bytes empty", []byte{}},
		{"bytes nil", []byte(nil)},

		{"raw object", json.RawMessage(`{"a":1,"b":{"c":[1,2,3]}}`)},
		{"raw array", json.RawMessage(`[1,"two",null]`)},
		{"raw null", json.RawMessage(`null`)},
		{"raw bare string", json.RawMessage(`"just a string"`)},
		{"raw number", json.RawMessage(`123.45`)},
		{"raw bool", json.RawMessage(`true`)},
		{"raw nil", json.RawMessage(nil)},
		{"raw with whitespace", json.RawMessage(`{"a": 1,  "b": [ 2 ]}`)},
		{"raw with line breaks", json.RawMessage("{\"a\": 1,\n\"b\": 2\r\n}")},

		{"big.Int", bigInt},
		{"big.Int negative", new(big.Int).Neg(bigInt)},

		// A type the table of converted values does not name still has to be
		// encoded rather than dropped or mis-encoded.
		{"int falls back to the standard encoder", 42},
		{"struct falls back to the standard encoder", struct {
			A string `json:"a"`
		}{"x"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireEquivalent(t, []string{"COL"}, []any{tc.value})
		})
	}
}

func TestStreamV2RowColumnNames(t *testing.T) {
	// Column names reach the writer unquoted, so whatever a Snowflake identifier
	// may hold has to be escaped as a JSON key.
	var names = []string{`plain`, `with space`, `with "quotes"`, `with\backslash`, `héllo`, `with<html>&`}
	var converted = []any{"a", "b", "c", "d", "e", "f"}

	requireEquivalent(t, names, converted)
}

func TestStreamV2RowOmitsNilColumns(t *testing.T) {
	var names = []string{"A", "B", "C", "D"}

	t.Run("a nil column is omitted", func(t *testing.T) {
		// Not written as a JSON null: a VARIANT column would then hold a VARIANT
		// null, which answers IS NULL and TYPEOF differently from the SQL NULL an
		// omitted column defaults to.
		var got = requireEquivalent(t, names, []any{"a", nil, json.RawMessage(`{"x":1}`), nil})
		require.Equal(t, `{"A":"a","C":{"x":1}}`, string(got))
	})

	t.Run("a row of nils is an empty object", func(t *testing.T) {
		var got = requireEquivalent(t, names, []any{nil, nil, nil, nil})
		require.Equal(t, `{}`, string(got))
	})

	t.Run("a nil leading column does not leave a separator", func(t *testing.T) {
		var got = requireEquivalent(t, names, []any{nil, int64(2), int64(3), nil})
		require.Equal(t, `{"B":2,"C":3}`, string(got))
	})
}

func TestStreamV2RowRefusesUnrepresentableFloats(t *testing.T) {
	var columns = rowColumnsOf([]string{"A", "B"})

	for _, tc := range []struct {
		name  string
		value float64
	}{
		{"NaN", math.NaN()},
		{"+Inf", math.Inf(1)},
		{"-Inf", math.Inf(-1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The standard encoder refuses these, so such a row fails the
			// transaction today. Writing the value out literally instead would put
			// bare NaN into the payload — invalid JSON, which Snowflake's decode of
			// the append refuses in full, failing every row batched alongside it.
			var prior = append([]byte(nil), `[{"A":1}`...)

			var got, err = appendRowJSON(prior, columns, []any{"a", tc.value})
			require.ErrorContains(t, err, "column B")
			require.ErrorContains(t, err, "as JSON")

			// The rows already batched are left exactly as they were.
			require.Equal(t, `[{"A":1}`, string(got))
		})
	}
}

func TestStreamV2RowPreEncodedLineBreaks(t *testing.T) {
	var columns = rowColumnsOf([]string{"DOC"})

	t.Run("a line break is compacted away", func(t *testing.T) {
		var raw = json.RawMessage("{\"a\": 1,\n  \"b\": \"text\\nwith an escaped break\"\r\n}")

		var got, err = appendRowJSON(nil, columns, []any{raw})
		require.NoError(t, err)
		require.NotContains(t, string(got), "\n")
		require.NotContains(t, string(got), "\r")
		// The escaped break within a string survives, being no threat to framing.
		require.Contains(t, string(got), `with an escaped break`)

		require.Equal(t, decodedJSON(t, []byte(`{"DOC":{"a":1,"b":"text\nwith an escaped break"}}`)), decodedJSON(t, got))
	})

	t.Run("a value without one is copied verbatim", func(t *testing.T) {
		// The guard is a scan, not a compaction: insignificant whitespace within a
		// pre-encoded value is passed through untouched, which is what makes the
		// common case free.
		var raw = json.RawMessage(`{"a": 1,  "b": [ 2 ]}`)

		var got, err = appendRowJSON(nil, columns, []any{raw})
		require.NoError(t, err)
		require.Equal(t, `{"DOC":`+string(raw)+`}`, string(got))
	})

	t.Run("an unparseable value with one fails the row", func(t *testing.T) {
		var got, err = appendRowJSON([]byte("{"), columns, []any{json.RawMessage("{\n")})
		require.ErrorContains(t, err, "column DOC")
		require.Equal(t, "{", string(got))
	})
}
