package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteEscapedField(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want string
	}{
		{"plain ascii", "hello", "hello"},
		{"backslash", `a\b`, `a\\b`},
		{"newline", "a\nb", `a\n` + "b"},
		{"carriage return", "a\rb", `a\r` + "b"},
		{"tab", "a\tb", `a\t` + "b"},
		{"null byte", "a\x00b", `a\0` + "b"},
		{"comma", "a,b", `a\,b`},
		{"\\N is preserved as the literal text \\N (not SQL NULL)", `\N`, `\\N`},
		{"all specials together", "\\\n\r\t\x00,", `\\\n\r\t\0\,`},
		{"multibyte utf-8 untouched", "héllo・🙂", "héllo・🙂"},
		{"empty string", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			writeEscapedField(&buf, tc.in)
			require.Equal(t, tc.want, buf.String())
		})
	}
}

func TestWriteInfileField(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   any
		want string
	}{
		{"nil becomes \\N", nil, `\N`},
		{"empty string is empty (distinct from NULL)", "", ""},
		{"string is escaped", "a,b", `a\,b`},
		{"[]byte is escaped like a string", []byte("a\nb"), `a\n` + "b"},
		{"true is 1", true, "1"},
		{"false is 0", false, "0"},
		{"int is JSON-encoded", 42, "42"},
		{"float is JSON-encoded", 1.5, "1.5"},
		{"map is JSON-encoded and escaped", map[string]string{"k": "v,w"}, `{"k":"v\,w"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			require.NoError(t, writeInfileField(&buf, tc.in))
			require.Equal(t, tc.want, buf.String())
		})
	}
}

// TestInfileRoundTrip verifies that the encoded bytes parse back to the
// original field values when interpreted under MySQL's LOAD DATA semantics
// with FIELDS TERMINATED BY ',' ESCAPED BY '\\' LINES TERMINATED BY '\n'.
func TestInfileRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name string
		row  []any
		want []string
	}{
		{
			name: "plain row",
			row:  []any{"a", "b", "c"},
			want: []string{"a", "b", "c"},
		},
		{
			name: "embedded comma",
			row:  []any{"hello,world", "x"},
			want: []string{"hello,world", "x"},
		},
		{
			name: "embedded newline",
			row:  []any{"line1\nline2"},
			want: []string{"line1\nline2"},
		},
		{
			name: "embedded backslash",
			row:  []any{`a\b`, `c\\d`},
			want: []string{`a\b`, `c\\d`},
		},
		{
			name: "literal \\N stays literal",
			row:  []any{`\N`},
			want: []string{`\N`},
		},
		{
			name: "nil is parsed as NULL sentinel",
			row:  []any{nil, "x"},
			want: []string{"\x00NULL\x00", "x"}, // sentinel below
		},
		{
			name: "tab and CR",
			row:  []any{"a\tb\rc"},
			want: []string{"a\tb\rc"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			for j, v := range tc.row {
				if j > 0 {
					buf.WriteByte(',')
				}
				require.NoError(t, writeInfileField(&buf, v))
			}
			buf.WriteByte('\n')

			got := parseLoadDataLine(t, buf.Bytes())
			require.Equal(t, tc.want, got)
		})
	}
}

// parseLoadDataLine implements the subset of MySQL LOAD DATA parsing exercised
// by the connector: a single line terminated by '\n', fields separated by ',',
// with '\\' as the escape character and no enclosure. The returned slice has
// one entry per field; SQL NULL values are represented by the sentinel
// "\x00NULL\x00" so they're distinguishable from the literal string "NULL".
func parseLoadDataLine(t *testing.T, line []byte) []string {
	t.Helper()
	require.Greater(t, len(line), 0)
	require.Equal(t, byte('\n'), line[len(line)-1], "line must be \\n-terminated")
	line = line[:len(line)-1]

	var fields []string
	var cur bytes.Buffer
	isNull := false
	flush := func() {
		if isNull {
			fields = append(fields, "\x00NULL\x00")
		} else {
			fields = append(fields, cur.String())
		}
		cur.Reset()
		isNull = false
	}

	for i := 0; i < len(line); i++ {
		c := line[i]
		switch c {
		case '\\':
			require.Less(t, i+1, len(line), "dangling escape")
			next := line[i+1]
			i++
			switch next {
			case 'N':
				require.Equal(t, 0, cur.Len(), "\\N must be a whole field")
				isNull = true
			case 'n':
				cur.WriteByte('\n')
			case 'r':
				cur.WriteByte('\r')
			case 't':
				cur.WriteByte('\t')
			case '0':
				cur.WriteByte(0)
			default:
				// Per MySQL docs, an unrecognized escape yields the byte itself.
				cur.WriteByte(next)
			}
		case ',':
			flush()
		default:
			cur.WriteByte(c)
		}
	}
	flush()
	return fields
}
