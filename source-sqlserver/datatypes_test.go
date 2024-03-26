package main

import (
	"context"
	"fmt"
	"regexp"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/sqlcapture/tests"
)

// TestDatatypes tests discovery and value capture with various database column types,
func TestDatatypes(t *testing.T) {
	var ctx, tb = context.Background(), sqlserverTestBackend(t)
	tb.config.Timezone = "America/Chicago" // Interpret DATETIME column values here
	tests.TestDatatypes(ctx, t, tb, []tests.DatatypeTestCase{
		{ColumnType: `integer`, ExpectType: `{"type":["integer","null"]}`, InputValue: `123`, ExpectValue: `123`},
		{ColumnType: `integer not null`, ExpectType: `{"type":"integer"}`, InputValue: `-0451`, ExpectValue: `-451`},

		{ColumnType: `bigint`, ExpectType: `{"type":["integer","null"]}`, InputValue: `1234567891234567`, ExpectValue: `1234567891234567`},
		{ColumnType: `int`, ExpectType: `{"type":["integer","null"]}`, InputValue: `2147483647`, ExpectValue: `2147483647`},
		{ColumnType: `smallint`, ExpectType: `{"type":["integer","null"]}`, InputValue: `32767`, ExpectValue: `32767`},
		{ColumnType: `tinyint`, ExpectType: `{"type":["integer","null"]}`, InputValue: `255`, ExpectValue: `255`},

		{ColumnType: `numeric(10,5)`, ExpectType: `{"type":["string","null"],"format":"number"}`, InputValue: `12345.12`, ExpectValue: `"12345.12000"`},
		{ColumnType: `decimal(5,2)`, ExpectType: `{"type":["string","null"],"format":"number"}`, InputValue: `123`, ExpectValue: `"123.00"`},
		{ColumnType: `money`, ExpectType: `{"type":["string","null"],"format":"number"}`, InputValue: `$1234567891234.29`, ExpectValue: `"1234567891234.2900"`},
		{ColumnType: `smallmoney`, ExpectType: `{"type":["string","null"],"format":"number"}`, InputValue: `$3148.29`, ExpectValue: `"3148.2900"`},

		{ColumnType: `bit`, ExpectType: `{"type":["boolean","null"]}`, InputValue: `TRUE`, ExpectValue: `true`},

		{ColumnType: `float`, ExpectType: `{"type":["number","null"]}`, InputValue: `1234.50`, ExpectValue: `1234.5`},
		{ColumnType: `real`, ExpectType: `{"type":["number","null"]}`, InputValue: `1234.50`, ExpectValue: `1234.5`},

		{ColumnType: `char(6)`, ExpectType: `{"type":["string","null"]}`, InputValue: `asdf`, ExpectValue: `"asdf  "`},
		{ColumnType: `varchar(6)`, ExpectType: `{"type":["string","null"]}`, InputValue: `asdf`, ExpectValue: `"asdf"`},
		{ColumnType: `text`, ExpectType: `{"type":["string","null"]}`, InputValue: `asdf`, ExpectValue: `"asdf"`},
		{ColumnType: `nchar(6)`, ExpectType: `{"type":["string","null"]}`, InputValue: `asdf`, ExpectValue: `"asdf  "`},
		{ColumnType: `nvarchar(6)`, ExpectType: `{"type":["string","null"]}`, InputValue: `asdf`, ExpectValue: `"asdf"`},
		{ColumnType: `ntext`, ExpectType: `{"type":["string","null"]}`, InputValue: `asdf`, ExpectValue: `"asdf"`},

		{ColumnType: `binary(8)`, ExpectType: `{"type":["string","null"],"contentEncoding":"base64"}`, InputValue: []byte{0x12, 0x34, 0x56, 0x78}, ExpectValue: `"EjRWeAAAAAA="`},
		{ColumnType: `varbinary(8)`, ExpectType: `{"type":["string","null"],"contentEncoding":"base64"}`, InputValue: []byte{0x12, 0x34, 0x56, 0x78}, ExpectValue: `"EjRWeA=="`},
		{ColumnType: `image`, ExpectType: `{"type":["string","null"],"contentEncoding":"base64"}`, InputValue: []byte{0x12, 0x34, 0x56, 0x78}, ExpectValue: `"EjRWeA=="`},

		{ColumnType: `date`, ExpectType: `{"type":["string","null"],"format":"date"}`, InputValue: `1991-08-31`, ExpectValue: `"1991-08-31"`},
		{ColumnType: `time`, ExpectType: `{"type":["string","null"]}`, InputValue: `12:34:54.125`, ExpectValue: `"12:34:54.125"`},
		{ColumnType: `datetimeoffset`, ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: `1991-08-31T12:34:54.125-06:00`, ExpectValue: `"1991-08-31T12:34:54.125-06:00"`},

		{ColumnType: `datetime`, ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: `1991-08-31T12:34:56.789`, ExpectValue: `"1991-08-31T12:34:56.79-05:00"`},
		{ColumnType: `datetime2`, ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: `1991-08-31T12:34:56.789`, ExpectValue: `"1991-08-31T12:34:56.789-05:00"`},
		{ColumnType: `smalldatetime`, ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: `1991-08-31T12:34:56.789`, ExpectValue: `"1991-08-31T12:35:00-05:00"`},

		{ColumnType: `uniqueidentifier`, ExpectType: `{"type":["string","null"],"format":"uuid"}`, InputValue: `8292f3cb-0cce-41e8-86aa-ae09bcc988e9`, ExpectValue: `"8292f3cb-0cce-41e8-86aa-ae09bcc988e9"`},

		{ColumnType: `xml`, ExpectType: `{"type":["string","null"]}`, InputValue: `<hello>world</hello>`, ExpectValue: `"\u003chello\u003eworld\u003c/hello\u003e"`},

		{ColumnType: `hierarchyid`, ExpectType: `{"type":["string","null"],"contentEncoding":"base64"}`, InputValue: `/1/2/3/`, ExpectValue: `"W14="`},
	})
}

func TestScanKeyTypes(t *testing.T) {
	var ctx, tb = context.Background(), sqlserverTestBackend(t)
	for idx, tc := range []struct {
		Name       string
		ColumnType string
		Values     []interface{}
	}{
		{"Integer", "INTEGER", []any{0, -3, 2, 1723}},
		{"DateTimeOffset", "DATETIMEOFFSET", []any{"1991-08-31T12:34:54.125-06:00", "1991-08-31T12:34:54.126-06:00", "2000-01-01T01:01:01Z"}},
		{"UniqueIdentifier", "UNIQUEIDENTIFIER", []any{
			"00ffffff-ffff-ffff-ffff-ffffffffffff",
			"ff00ffff-ffff-ffff-ffff-ffffffffffff",
			"ffff00ff-ffff-ffff-ffff-ffffffffffff",
			"ffffff00-ffff-ffff-ffff-ffffffffffff",
			"ffffffff-00ff-ffff-ffff-ffffffffffff",
			"ffffffff-ff00-ffff-ffff-ffffffffffff",
			"ffffffff-ffff-00ff-ffff-ffffffffffff",
			"ffffffff-ffff-ff00-ffff-ffffffffffff",
			"ffffffff-ffff-ffff-00ff-ffffffffffff",
			"ffffffff-ffff-ffff-ff00-ffffffffffff",
			"ffffffff-ffff-ffff-ffff-00ffffffffff",
			"ffffffff-ffff-ffff-ffff-ff00ffffffff",
			"ffffffff-ffff-ffff-ffff-ffff00ffffff",
			"ffffffff-ffff-ffff-ffff-ffffff00ffff",
			"ffffffff-ffff-ffff-ffff-ffffffff00ff",
			"ffffffff-ffff-ffff-ffff-ffffffffff00",
		}},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			var uniqueID = fmt.Sprintf("88929806%04d", idx)
			var tableName = tb.CreateTable(ctx, t, uniqueID, fmt.Sprintf("(k %s PRIMARY KEY, v TEXT)", tc.ColumnType))
			var rows [][]interface{}
			for idx, val := range tc.Values {
				rows = append(rows, []interface{}{val, fmt.Sprintf("Data %d", idx)})
			}
			tb.Insert(ctx, t, tableName, rows)

			var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
			cs.EndpointSpec.(*Config).Advanced.BackfillChunkSize = 1
			var summary, _ = tests.RestartingBackfillCapture(ctx, t, cs)
			cupaloy.SnapshotT(t, summary)
		})
	}
}
