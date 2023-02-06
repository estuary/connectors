package main

import (
	"context"
	"testing"

	"github.com/estuary/connectors/sqlcapture/tests"
)

// TestDatatypes tests discovery and value capture with various database column types,
func TestDatatypes(t *testing.T) {
	var ctx, tb = context.Background(), sqlserverTestBackend(t)
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
		{ColumnType: `time`, ExpectType: `{"type":["string","null"],"format":"time"}`, InputValue: `12:34:54.125`, ExpectValue: `"12:34:54.125"`},
		{ColumnType: `datetimeoffset`, ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: `1991-08-31T12:34:54.125-06:00`, ExpectValue: `"1991-08-31T12:34:54.125-06:00"`},

		// TODO(wgd): Figure out how we want to handle 'datetime' columns. They're just
		// as awful as every timezone-unaware datetime type always is.
		//{ColumnType: `datetime`, ExpectType: `{"type":["","null"]}`, InputValue: ``, ExpectValue: ``},
		//{ColumnType: `datetime2`, ExpectType: `{"type":["","null"]}`, InputValue: ``, ExpectValue: ``},
		//{ColumnType: `smalldatetime`, ExpectType: `{"type":["","null"]}`, InputValue: ``, ExpectValue: ``},

		{ColumnType: `uniqueidentifier`, ExpectType: `{"type":["string","null"],"format":"uuid"}`, InputValue: `8292f3cb-0cce-41e8-86aa-ae09bcc988e9`, ExpectValue: `"8292f3cb-0cce-41e8-86aa-ae09bcc988e9"`},

		{ColumnType: `xml`, ExpectType: `{"type":["string","null"]}`, InputValue: `<hello>world</hello>`, ExpectValue: `"\u003chello\u003eworld\u003c/hello\u003e"`},
	})
}
