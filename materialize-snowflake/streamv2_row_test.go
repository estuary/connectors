package connector

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strings"
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

func TestStreamV2BatchPayload(t *testing.T) {
	var newBinding = func() *streamV2Binding {
		return &streamV2Binding{columns: rowColumnsOf([]string{"KEY", "VAL", "DOC"})}
	}

	t.Run("rows accumulate into one framed payload", func(t *testing.T) {
		var b = newBinding()

		var total int
		for i, row := range [][]any{
			{"one", int64(1), json.RawMessage(`{"a":1}`)},
			{"two", nil, json.RawMessage(`{"a":2}`)},
			{"three", int64(3), nil},
		} {
			// Documents are indexed from the channel's first, so this batch begins
			// partway through one.
			var grew, err = b.bufferRow(int64(100+i), row)
			require.NoError(t, err)
			require.Equal(t, i+1, b.bufRows)
			total += grew
		}

		// The batch's own first document index, which its offset token is rendered
		// from, is the one the batch's first row was buffered under.
		require.Equal(t, int64(100), b.bufFirst)

		// The payload wants only its closing bracket, which appendBatch adds as it
		// hands the buffer off.
		require.Equal(t,
			`[{"KEY":"one","VAL":1,"DOC":{"a":1}},{"KEY":"two","DOC":{"a":2}},{"KEY":"three","VAL":3}`,
			string(b.buf))
		require.Equal(t, len(b.buf), total, "the reported growth must account for every buffered byte")
	})

	t.Run("a refused row leaves the rows before it buffered", func(t *testing.T) {
		var b = newBinding()

		var _, err = b.bufferRow(1, []any{"one", int64(1), nil})
		require.NoError(t, err)

		grew, err := b.bufferRow(1, []any{"two", math.NaN(), nil})
		require.ErrorContains(t, err, "column VAL")
		require.Zero(t, grew)
		require.Equal(t, 1, b.bufRows)
		require.Equal(t, `[{"KEY":"one","VAL":1}`, string(b.buf))
	})

	t.Run("a refused first row leaves no batch started", func(t *testing.T) {
		var b = newBinding()

		var _, err = b.bufferRow(1, []any{"one", math.Inf(1), nil})
		require.ErrorContains(t, err, "column VAL")
		require.Zero(t, b.bufRows)
		require.Empty(t, b.buf)
	})

	t.Run("a batch releases every byte it reported buffering", func(t *testing.T) {
		// The manager tracks buffered bytes as a running total across bindings, so a
		// batch which reports more or fewer bytes than it releases drifts that total
		// for the life of the session, and with it the ceiling which bounds how much
		// a wide materialization holds in memory at once.
		var b = newBinding()

		var reported int
		for i := range 3 {
			var grew, err = b.bufferRow(1, []any{fmt.Sprintf("key-%d", i), int64(i), nil})
			require.NoError(t, err)
			reported += grew
		}

		var payload, rows, released = b.finishBatch()
		require.Equal(t, reported, released)
		require.Equal(t, 3, rows)
		require.Equal(t, byte(']'), payload[len(payload)-1])
		require.Zero(t, b.bufRows)
		require.Empty(t, b.buf)
	})

	t.Run("the next batch's buffer is sized from the last", func(t *testing.T) {
		var b = newBinding()

		var _, err = b.bufferRow(1, []any{strings.Repeat("k", 32*1024), nil, nil})
		require.NoError(t, err)

		// What appendBatch does with the buffer it hands off: the size it records
		// spares the next batch the growth this one paid.
		b.bufHint = len(b.buf)
		b.buf, b.bufRows = nil, 0

		_, err = b.bufferRow(1, []any{"small", nil, nil})
		require.NoError(t, err)
		require.GreaterOrEqual(t, cap(b.buf), b.bufHint)
	})
}

// benchmarkRow is a batch row of the shape a wide binding stores: twenty columns
// whose values are what the dialect's converters produce, two of them the
// pre-encoded JSON of a VARIANT column.
func benchmarkRow(i int) ([]string, []any) {
	var names = []string{
		"KEY", "NAME", "EMAIL", "STATUS", "COUNTRY",
		"COUNT", "AMOUNT", "RATIO", "ENABLED", "BIG",
		"CREATED_AT", "UPDATED_AT", "DELETED_AT", "DAY", "NOTE",
		"BLOB", "TAGS", "FLOW_DOCUMENT", "SEQ", "UNSIGNED",
	}

	var big, _ = new(big.Int).SetString("12345678901234567890123456789012345678", 10)
	var converted = []any{
		fmt.Sprintf("key-%d", i), "A Name", "user@example.com", "active", "US",
		int64(i), 1234.56, 0.5, i%2 == 0, big,
		"2026-08-03T12:34:56.789Z", "2026-08-03T12:34:56.789Z", nil, "2026-08-03", "a note with \"quotes\" and ünïcödé",
		[]byte("some binary bytes"), json.RawMessage(`["one","two","three"]`),
		json.RawMessage(fmt.Sprintf(`{"id":%d,"nested":{"a":1,"b":[1,2,3],"c":"text"},"flag":true}`, i)),
		int64(i * 3), uint64(math.MaxUint64 - uint64(i)),
	}

	return names, converted
}

// BenchmarkStreamV2RowEncoding measures a full batch's encoding both ways: the
// direct writer against the map-and-marshal path it replaced, which paid a second
// scan of every row to frame the batch. The direct writer runs in roughly an
// eighth of the CPU and a twentieth of the allocations — about 8 ms against 70 ms
// for 10k rows on a Xeon 8581C.
//
// Its allocations are the growth of the one payload buffer, plus two a row for the
// decimal conversion of the *big.Int column: with no such column a whole batch
// encodes in three allocations.
func BenchmarkStreamV2RowEncoding(b *testing.B) {
	const batchRows = 10_000

	var names, _ = benchmarkRow(0)
	var rows = make([][]any, batchRows)
	for i := range rows {
		_, rows[i] = benchmarkRow(i)
	}
	var columns = rowColumnsOf(names)

	b.Run("direct", func(b *testing.B) {
		b.ReportAllocs()
		// The binding buffers and frames the batch exactly as the write path has it
		// do, so what this measures is what ships — including the payload buffer
		// each batch is sized into.
		var binding = &streamV2Binding{columns: columns}
		for b.Loop() {
			for i, row := range rows {
				if _, err := binding.bufferRow(int64(i), row); err != nil {
					b.Fatal(err)
				}
			}
			binding.finishBatch()
		}
	})

	b.Run("viaMap", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var encoded = make([][]byte, 0, len(rows))
			for _, row := range rows {
				var data, err = rowViaMap(names, row)
				if err != nil {
					b.Fatal(err)
				}
				encoded = append(encoded, data)
			}
			// The batch's rows were then scanned a second time to frame them.
			var size = 2
			for _, row := range encoded {
				size += len(row) + 1
			}
			var payload = make([]byte, 0, size)
			payload = append(payload, '[')
			for i, row := range encoded {
				if i > 0 {
					payload = append(payload, ',')
				}
				payload = append(payload, row...)
			}
			payload = append(payload, ']')
			_ = payload
		}
	})
}
