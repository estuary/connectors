package connector

import (
	"bytes"
	"encoding/csv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseQueryPoints(t *testing.T) {
	var input bytes.Buffer
	w := csv.NewWriter(&input)
	for _, field := range []struct{ name, value, datatype string }{
		{"count", "1", "long"},
		{"temp", "21", "double"},
		{"ok", "false", "boolean"},
		{"note", `say "hi", ok`, "string"},
		{"meta", `{"fw":"1.2"}`, "string"},
	} {
		require.NoError(t, w.Write([]string{"#datatype", "string", "long", "dateTime:RFC3339", field.datatype, "string", "string", "string"}))
		require.NoError(t, w.Write([]string{"", "result", "table", "_time", "_value", "_field", "_measurement", "sensor"}))
		require.NoError(t, w.Write([]string{"", "_result", "0", "2026-01-01T00:00:00Z", field.value, field.name, "readings", "lab 1"}))
		require.NoError(t, w.Write([]string{"", "_result", "1", "2026-01-01T00:00:00.000000001Z", field.value, field.name, "readings", "lab 1"}))
		require.NoError(t, w.Write(nil))
	}
	w.Flush()
	require.NoError(t, w.Error())

	points, err := parseQueryPoints(&input)
	require.NoError(t, err)
	require.Len(t, points, 2)
	var times []string
	for _, point := range points {
		require.Equal(t, "readings", point.Measurement)
		require.Equal(t, map[string]string{"sensor": "lab 1"}, point.Tags)
		require.Equal(t, map[string]any{
			"count": int64(1), "temp": float64(21), "ok": false, "note": `say "hi", ok`, "meta": `{"fw":"1.2"}`,
		}, point.Fields)
		require.Equal(t, map[string]string{
			"count": "long", "temp": "double", "ok": "boolean", "note": "string", "meta": "string",
		}, point.Types)
		times = append(times, point.Time)
	}
	require.ElementsMatch(t, []string{"2026-01-01T00:00:00Z", "2026-01-01T00:00:00.000000001Z"}, times)
}

func TestParseQueryPointsErrors(t *testing.T) {
	const response = "#datatype,string,long,dateTime:RFC3339,double,string,string\n" +
		",result,table,_time,_value,_field,_measurement\n" +
		",_result,0,2026-01-01T00:00:00Z,20.5,temp,readings\n"
	for _, tc := range []struct{ name, input, err string }{
		{"invalid number", strings.ReplaceAll(response, "20.5", "broken"), "decoding field"},
		{"unknown type", strings.ReplaceAll(response, "double", "unknown"), "unknown query datatype"},
		{"invalid time", strings.ReplaceAll(response, "2026-01-01T00:00:00Z", "broken"), "parsing time"},
		{"missing measurement", strings.ReplaceAll(response, "readings", ""), "no measurement or field"},
		{"missing header", ",_result,0,2026-01-01T00:00:00Z,20.5,temp,readings\n", "CSV header and datatypes"},
		{"duplicate field", response + ",_result,0,2026-01-01T00:00:00Z,20.5,temp,readings\n", "duplicate field"},
		{"stream error", ",error,reference\n,query failed,123\n", "query error"},
		{"malformed CSV", "\"unterminated", "parse error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseQueryPoints(strings.NewReader(tc.input))
			require.ErrorContains(t, err, tc.err)
		})
	}
}
