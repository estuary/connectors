package connector

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestPoint(t *testing.T) {
	binding := measurementBinding{
		resource: resource{Measurement: "readings", TimestampField: "ts", TagFields: []string{"sensor"}},
		fields: pf.FieldSelection{
			Keys:   []string{"sensor", "ts"},
			Values: []string{"count", "meta", "note", "ok", "samples", "temp"},
		},
		numericFields: map[string]string{"temp": "number"},
	}
	line, err := binding.point(
		tuple.Tuple{"lab 1", "2026-01-01T01:00:00.123456789+01:00"},
		tuple.Tuple{int64(1), []byte(`{ "fw": "1.2" }`), `say "hi", ok`, true, []byte(`[1, true, "a"]`), 20.5},
		json.RawMessage(`{"ignored":"not selected"}`),
	)
	require.NoError(t, err)
	require.Equal(t, `readings,sensor=lab\ 1 count=1i,meta="{\"fw\":\"1.2\"}",note="say \"hi\", ok",ok=true,samples="[1,true,\"a\"]",temp=20.5 1767225600123456789`, line)

	line, err = binding.point(tuple.Tuple{"s1", "2026-01-01T00:01:00Z"}, tuple.Tuple{nil, nil, nil, nil, nil, int64(21)}, nil)
	require.NoError(t, err)
	require.Equal(t, `readings,sensor=s1 temp=21 1767225660000000000`, line)

	line, err = binding.point(tuple.Tuple{"s1", "2026-01-01T00:00:00Z"}, tuple.Tuple{nil, nil, nil, nil, nil, nil}, nil)
	require.NoError(t, err)
	require.Empty(t, line)
}

func TestPointEscaping(t *testing.T) {
	binding := measurementBinding{
		resource: resource{Measurement: `lab, east=1\west"`, TimestampField: "ts", TagFields: []string{`tag, key=\"`}},
		fields: pf.FieldSelection{
			Keys:   []string{`tag, key=\"`, "ts"},
			Values: []string{`field, key=\"`},
		},
	}
	line, err := binding.point(tuple.Tuple{`room, 1=\west"`, "2026-01-01T00:00:00Z"}, tuple.Tuple{`a\b, c="d"`}, nil)
	require.NoError(t, err)
	require.Equal(t, `lab\,\ east=1\west",tag\,\ key\=\"=room\,\ 1\=\west" field\,\ key\=\"="a\\b, c=\"d\"" 1767225600000000000`, line)
}

func TestPointTagsAndDocument(t *testing.T) {
	binding := measurementBinding{
		resource: resource{
			Measurement: "orders", TimestampField: defaultTimestampField,
			TagFields: []string{"id", "active", "ratio", "missing"},
		},
		fields: pf.FieldSelection{
			Keys:     []string{"id"},
			Values:   []string{"active", "flow_published_at", "missing", "ratio"},
			Document: "flow_document",
		},
	}
	line, err := binding.point(tuple.Tuple{int64(123)}, tuple.Tuple{false, "2026-01-01T00:00:00Z", nil, 1.25}, json.RawMessage("{\n  \"id\": 123, \"text\": \"a\\\\b\"\n}"))
	require.NoError(t, err)
	require.Equal(t, `orders,id=123,active=false,ratio=1.25 flow_document="{\"id\":123,\"text\":\"a\\\\b\"}" 1767225600000000000`, line)

	// An explicit tag list leaves unlisted collection keys as fields.
	binding.resource.TagFields = []string{}
	binding.fields.Document = ""
	line, err = binding.point(tuple.Tuple{int64(123)}, tuple.Tuple{false, "2026-01-01T00:00:00Z", nil, 1.25}, nil)
	require.NoError(t, err)
	require.Equal(t, `orders id=123i,active=false,ratio=1.25 1767225600000000000`, line)
}

func TestPointInvalidTimestamp(t *testing.T) {
	binding := measurementBinding{
		resource: resource{Measurement: "readings", TimestampField: "ts"},
		fields:   pf.FieldSelection{Keys: []string{"ts"}, Values: []string{"value"}},
	}
	for _, tc := range []struct {
		name  string
		stamp any
		err   string
	}{
		{"missing", nil, "must contain a date-time string"},
		{"number", int64(1), "must contain a date-time string"},
		{"date only", "2026-01-01", "parsing timestamp"},
		{"invalid date", "2026-02-30T00:00:00Z", "parsing timestamp"},
		{"too early", "1677-09-21T00:12:43.145224193Z", "outside InfluxDB's nanosecond time range"},
		{"too late", "2262-04-11T23:47:16.854775807Z", "outside InfluxDB's nanosecond time range"},
		{"overflow", "3000-01-01T00:00:00Z", "outside InfluxDB's nanosecond time range"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := binding.point(tuple.Tuple{tc.stamp}, tuple.Tuple{int64(1)}, nil)
			require.ErrorContains(t, err, tc.err)
		})
	}
	for _, stamp := range []string{"1677-09-21T00:12:43.145224194Z", "2262-04-11T23:47:16.854775806Z", "1969-12-31T23:59:59.999999999Z"} {
		_, err := binding.point(tuple.Tuple{stamp}, tuple.Tuple{int64(1)}, nil)
		require.NoError(t, err)
	}
}

func TestNumericTagEncoding(t *testing.T) {
	// Numeric tags use decimal notation without losing integral float precision.
	for _, tc := range []struct {
		name  string
		value any
		want  string
		err   string
	}{
		{name: "integer", value: int64(1000000), want: "1000000"},
		{name: "integral float", value: float64(1000000), want: "1000000"},
		{name: "non-round integral float", value: float64(1234567), want: "1234567"},
		{name: "negative integral float", value: float64(-1234567), want: "-1234567"},
		{name: "negative zero", value: math.Copysign(0, -1), want: "0"},
		{name: "minimum integer", value: float64(math.MinInt64), want: "-9223372036854775808"},
		{name: "near maximum integer", value: math.Nextafter(float64(math.MaxInt64), 0), want: "9223372036854774784"},
		{name: "integer upper bound", value: float64(math.MaxInt64), want: "9223372036854776000"},
		{name: "above integer range", value: 1e21, want: "1000000000000000000000"},
		{name: "below integer range", value: -1e21, want: "-1000000000000000000000"},
		{name: "large fraction", value: 1234567.5, want: "1234567.5"},
		{name: "small fraction", value: 1e-7, want: "0.0000001"},
		{name: "infinity", value: math.Inf(1), err: "finite numbers"},
		{name: "negative infinity", value: math.Inf(-1), err: "finite numbers"},
		{name: "NaN", value: math.NaN(), err: "finite numbers"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := encodeTag(tc.value)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			}
		})
	}
}

func TestFieldEncoding(t *testing.T) {
	for _, tc := range []struct {
		name        string
		value       any
		numericType string
		want        string
		err         string
	}{
		{name: "null", value: nil},
		{name: "empty string", value: "", want: `""`},
		{name: "negative integer", value: int64(-42), want: "-42i"},
		{name: "zero", value: int64(0), want: "0i"},
		{name: "integer precision", value: int64(9007199254740993), want: "9007199254740993i"},
		{name: "min integer", value: int64(math.MinInt64), want: "-9223372036854775808i"},
		{name: "max integer", value: int64(math.MaxInt64), want: "9223372036854775807i"},
		{name: "unsigned overflow", value: uint64(math.MaxUint64), err: "signed 64-bit range"},
		{name: "number from integer", value: int64(21), numericType: "number", want: "21"},
		{name: "number", value: 1.25, numericType: "number", want: "1.25"},
		{name: "integer from float", value: float64(1), numericType: "integer", want: "1i"},
		{name: "nonintegral integer", value: 1.5, numericType: "integer", err: "expected an integer"},
		{name: "float integer overflow", value: float64(math.MaxInt64), numericType: "integer", err: "signed 64-bit range"},
		{name: "float integer NaN", value: math.NaN(), numericType: "integer", err: "expected an integer"},
		{name: "false", value: false, want: "false"},
		{name: "string escaping", value: `\"x", y=z`, want: `"\\\"x\", y=z"`},
		{name: "JSON null", value: []byte(`null`)},
		{name: "JSON integer", value: []byte(`9007199254740993`), want: "9007199254740993i"},
		{name: "JSON integer overflow", value: []byte(`9223372036854775808`), err: "signed 64-bit range"},
		{name: "JSON number", value: []byte(`21`), numericType: "number", want: "21"},
		{name: "JSON float", value: []byte(`1.25`), want: "1.25"},
		{name: "JSON string", value: []byte(`"hello"`), want: `"hello"`},
		{name: "JSON boolean", value: []byte(`true`), want: "true"},
		{name: "JSON decimal integer", value: []byte(`1.0`), numericType: "integer", want: "1i"},
		{name: "document string", value: json.RawMessage(`"hello"`), want: `"\"hello\""`},
		{name: "document number", value: json.RawMessage(`123`), want: `"123"`},
		{name: "invalid document", value: json.RawMessage(`{`), err: "invalid JSON document"},
		{name: "object", value: []byte("{\n\"n\": 9007199254740993, \"s\": \"a\\nb\"\n}"), want: `"{\"n\":9007199254740993,\"s\":\"a\\nb\"}"`},
		{name: "array", value: []byte(`[1, null, {"a": true}]`), want: `"[1,null,{\"a\":true}]"`},
		{name: "empty JSON", value: []byte{}, err: "invalid JSON"},
		{name: "invalid JSON", value: []byte(`{`), err: "invalid JSON"},
		{name: "multiple JSON values", value: []byte(`1 2`), err: "invalid JSON"},
		{name: "infinity", value: math.Inf(1), err: "finite numbers"},
		{name: "NaN", value: math.NaN(), err: "finite numbers"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := encodeField(tc.value, tc.numericType)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			}
		})
	}
}

func TestPointNewlines(t *testing.T) {
	binding := measurementBinding{
		resource: resource{Measurement: "readings", TimestampField: "ts", TagFields: []string{"sensor"}},
		fields:   pf.FieldSelection{Keys: []string{"sensor", "ts"}, Values: []string{"value"}},
	}
	for _, tag := range []string{"a\nb", "a\rb"} {
		_, err := binding.point(tuple.Tuple{tag, "2026-01-01T00:00:00Z"}, tuple.Tuple{"ok"}, nil)
		require.ErrorContains(t, err, "does not support newlines")
	}
	binding.resource.Measurement = "read\nings"
	_, err := binding.point(tuple.Tuple{"s1", "2026-01-01T00:00:00Z"}, tuple.Tuple{"ok"}, nil)
	require.ErrorContains(t, err, "does not support newlines")
	binding.resource.Measurement = "readings"
	binding.fields.Values = []string{"val\nue"}
	_, err = binding.point(tuple.Tuple{"s1", "2026-01-01T00:00:00Z"}, tuple.Tuple{"ok"}, nil)
	require.ErrorContains(t, err, "does not support newlines")

	// Quoted string field values may hold newlines.
	binding.fields.Values = []string{"value"}
	line, err := binding.point(tuple.Tuple{"s1", "2026-01-01T00:00:00Z"}, tuple.Tuple{"line 1\nline 2\r"}, nil)
	require.NoError(t, err)
	require.Equal(t, "readings,sensor=s1 value=\"line 1\nline 2\r\" 1767225600000000000", line)
}

func TestPointRejectsTrailingBackslashes(t *testing.T) {
	for _, tc := range []struct{ name, measurement, tag, field, value, err string }{
		{"measurement", `files\`, "path", "size", `C:\temp`, "measurement"},
		{"tag key", "files", `path\`, "size", `C:\temp`, `tag "path\\"`},
		{"tag value", "files", "path", "size", `C:\`, `tag "path"`},
		{"field key", "files", "path", `size\`, `C:\temp`, `field "size\\"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binding := measurementBinding{
				resource: resource{Measurement: tc.measurement, TimestampField: "ts", TagFields: []string{tc.tag}},
				fields:   pf.FieldSelection{Keys: []string{tc.tag, "ts"}, Values: []string{tc.field}},
			}
			_, err := binding.point(tuple.Tuple{tc.value, "2026-01-01T00:00:00Z"}, tuple.Tuple{int64(5)}, nil)
			require.ErrorContains(t, err, tc.err)
			require.ErrorContains(t, err, "ending in a backslash")
		})
	}

	// A backslash before other characters is kept as written.
	binding := measurementBinding{
		resource: resource{Measurement: "files", TimestampField: "ts", TagFields: []string{"path"}},
		fields:   pf.FieldSelection{Keys: []string{"path", "ts"}, Values: []string{"size"}},
	}
	line, err := binding.point(tuple.Tuple{`C:\temp`, "2026-01-01T00:00:00Z"}, tuple.Tuple{int64(5)}, nil)
	require.NoError(t, err)
	require.Equal(t, `files,path=C:\temp size=5i 1767225600000000000`, line)
}
