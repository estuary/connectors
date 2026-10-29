package connector

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// rowViaMap encodes a row the way the streaming v2 write path did before it wrote
// row JSON itself: a map keyed by column name, handed to encoding/json. It is the
// reference the encoder is held equivalent to, and the baseline the benchmark
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

// requireEquivalent asserts a batch and the map-and-marshal path encode the same
// row to the same *values*.
//
// Byte equality is deliberately not the claim: encoding/json escapes `<`, `>` and
// `&` for the benefit of HTML embedding, and the two escape U+2028, U+2029 and
// invalid UTF-8 differently. Those differ as bytes and agree as JSON.
func requireEquivalent(t *testing.T, names []string, converted []any) []byte {
	t.Helper()

	var want, err = rowViaMap(names, converted)
	require.NoError(t, err)

	got, err := encodeTestRows(t, names, converted)
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
		require.Equal(t, `{"A":"a","C":{"x":1}}`+"\n", string(got))
	})

	t.Run("a row of nils is an empty object", func(t *testing.T) {
		var got = requireEquivalent(t, names, []any{nil, nil, nil, nil})
		require.Equal(t, "{}\n", string(got))
	})

	t.Run("a nil leading column does not leave a separator", func(t *testing.T) {
		var got = requireEquivalent(t, names, []any{nil, int64(2), int64(3), nil})
		require.Equal(t, `{"B":2,"C":3}`+"\n", string(got))
	})
}

func TestStreamV2RowRejectsUnrepresentableFloats(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value float64
	}{
		{"NaN", math.NaN()},
		{"+Inf", math.Inf(1)},
		{"-Inf", math.Inf(-1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The standard encoder rejects these, so such a row fails the
			// transaction. Writing the value out literally instead would put bare
			// NaN into the batch — invalid JSON, which Snowflake's decode of the
			// append rejects in full, failing every row batched alongside it.
			var _, err = encodeTestRows(t, []string{"A", "B"}, []any{"a", tc.value})
			require.ErrorContains(t, err, `field "B"`)
			require.ErrorContains(t, err, "unsupported value")
		})
	}
}

func TestStreamV2RowPreEncodedLineBreaks(t *testing.T) {
	t.Run("a line break is compacted away", func(t *testing.T) {
		var raw = json.RawMessage("{\"a\": 1,\n  \"b\": \"text\\nwith an escaped break\"\r\n}")

		var got, err = encodeTestRows(t, []string{"DOC"}, []any{raw})
		require.NoError(t, err)
		// The row's only line break is the one that terminates it.
		require.Equal(t, len(got)-1, bytes.IndexByte(got, '\n'))
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

		var got, err = encodeTestRows(t, []string{"DOC"}, []any{raw})
		require.NoError(t, err)
		require.Equal(t, `{"DOC":`+string(raw)+"}\n", string(got))
	})

	t.Run("an unparseable value with one fails the row", func(t *testing.T) {
		var _, err = encodeTestRows(t, []string{"DOC"}, []any{json.RawMessage("{\n")})
		require.ErrorContains(t, err, `field "DOC"`)
	})

	t.Run("an empty value fails the row", func(t *testing.T) {
		var _, err = encodeTestRows(t, []string{"DOC"}, []any{json.RawMessage{}})
		require.ErrorContains(t, err, `field "DOC"`)
	})
}

func TestStreamV2BatchFile(t *testing.T) {
	var names = []string{"KEY", "VAL", "DOC"}

	t.Run("rows accumulate into one framed file", func(t *testing.T) {
		var b = streamV2Batch{columns: names}

		for i, row := range [][]any{
			{"one", int64(1), json.RawMessage(`{"a":1}`)},
			{"two", nil, json.RawMessage(`{"a":2}`)},
			{"three", int64(3), nil},
		} {
			require.NoError(t, b.writeRow(row))
			require.Equal(t, i+1, b.rows)
		}

		var file = finishTestBatch(t, &b)
		require.Equal(t, 3, file.rows)
		// Keys are written in name order.
		var want = "{\"DOC\":{\"a\":1},\"KEY\":\"one\",\"VAL\":1}\n{\"DOC\":{\"a\":2},\"KEY\":\"two\"}\n{\"KEY\":\"three\",\"VAL\":3}\n"
		require.Equal(t, want, string(readTestBatchFile(t, file)))
		require.Equal(t, len(want), file.uncompressedBytes)

		var info, err = os.Stat(file.path)
		require.NoError(t, err)
		require.Equal(t, int(info.Size()), file.compressedBytes)
	})

	t.Run("a rejected row leaves the rows before it", func(t *testing.T) {
		var b = streamV2Batch{columns: names}

		require.NoError(t, b.writeRow([]any{"one", int64(1), nil}))
		require.ErrorContains(t, b.writeRow([]any{"two", math.NaN(), nil}), `field "VAL"`)
		require.Equal(t, 1, b.rows)

		var file = finishTestBatch(t, &b)
		require.Equal(t, "{\"KEY\":\"one\",\"VAL\":1}\n", string(readTestBatchFile(t, file)))
	})

	t.Run("a rejected first row leaves the batch empty", func(t *testing.T) {
		var b = streamV2Batch{columns: names}

		require.ErrorContains(t, b.writeRow([]any{"one", math.Inf(1), nil}), `field "VAL"`)
		require.True(t, b.empty())
		finishTestBatch(t, &b)
	})

	t.Run("finishing closes the file and empties the batch", func(t *testing.T) {
		var b = streamV2Batch{columns: names}
		for i := range 3 {
			require.NoError(t, b.writeRow([]any{fmt.Sprintf("key-%d", i), int64(i), nil}))
		}

		var file = finishTestBatch(t, &b)
		require.Equal(t, 3, file.rows)
		require.True(t, b.empty())
		require.Nil(t, b.writer)
		require.Zero(t, b.size())

		// The next row starts a file of its own.
		require.NoError(t, b.writeRow([]any{"next", nil, nil}))
		var next = finishTestBatch(t, &b)
		require.NotEqual(t, file.path, next.path)
		require.Equal(t, "{\"KEY\":\"next\"}\n", string(readTestBatchFile(t, next)))
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

// BenchmarkStreamV2RowEncoding measures a full batch both ways: written to its
// compressed file as the write path does, against encoding by the map-and-marshal
// path, which paid a second scan of every row to frame the batch.
func BenchmarkStreamV2RowEncoding(b *testing.B) {
	const batchRows = 10_000

	var names, _ = benchmarkRow(0)
	var rows = make([][]any, batchRows)
	for i := range rows {
		_, rows[i] = benchmarkRow(i)
	}

	b.Run("batch", func(b *testing.B) {
		b.ReportAllocs()
		var batch = streamV2Batch{columns: names}
		for b.Loop() {
			for _, row := range rows {
				if err := batch.writeRow(row); err != nil {
					b.Fatal(err)
				}
			}
			var file, err = batch.finish()
			if err != nil {
				b.Fatal(err)
			}
			os.Remove(file.path)
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
			var size = 0
			for _, row := range encoded {
				size += len(row) + 1
			}
			var payload = make([]byte, 0, size)
			for _, row := range encoded {
				payload = append(payload, row...)
				payload = append(payload, '\n')
			}
			_ = payload
		}
	})
}

// encodeTestRows writes rows through a batch and returns the rows its file holds.
func encodeTestRows(t *testing.T, names []string, rows ...[]any) ([]byte, error) {
	t.Helper()

	var b = streamV2Batch{columns: names}
	for _, row := range rows {
		if err := b.writeRow(row); err != nil {
			finishTestBatch(t, &b)
			return nil, err
		}
	}
	return readTestBatchFile(t, finishTestBatch(t, &b)), nil
}

// finishTestBatch finishes a batch that has a file open, and removes the file
// once the test ends.
func finishTestBatch(t *testing.T, b *streamV2Batch) streamV2BatchFile {
	t.Helper()

	var file, err = b.finish()
	require.NoError(t, err)
	t.Cleanup(func() { os.Remove(file.path) })
	return file
}

// readTestBatchFile returns the decompressed rows of a finished batch's file.
func readTestBatchFile(t *testing.T, file streamV2BatchFile) []byte {
	t.Helper()

	var compressed, err = os.ReadFile(file.path)
	require.NoError(t, err)
	rows, err := fakeSnowpipeGunzip(compressed)
	require.NoError(t, err)
	return rows
}
