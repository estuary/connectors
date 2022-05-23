package main

import (
	"context"
	"testing"

	"github.com/estuary/connectors/sqlcapture/tests"
)

// TestDatatypes runs the discovery test on various datatypes.
func TestDatatypes(t *testing.T) {
	var ctx = context.Background()
	tests.TestDatatypes(ctx, t, TestBackend, []tests.DatatypeTestCase{
		{ColumnType: "integer", ExpectType: `{"type":["integer","null"]}`, InputValue: 123, ExpectValue: `123`},
		{ColumnType: "integer", ExpectType: `{"type":["integer","null"]}`, InputValue: nil, ExpectValue: `null`},
		{ColumnType: "integer not null", ExpectType: `{"type":"integer"}`, InputValue: 123, ExpectValue: `123`},
		{ColumnType: "varchar(32)", ExpectType: `{"type":["string","null"]}`, InputValue: "hello", ExpectValue: `"hello"`},
		{ColumnType: "text", ExpectType: `{"type":["string","null"]}`, InputValue: "hello", ExpectValue: `"hello"`},

		// Integer Types
		{ColumnType: "tinyint", ExpectType: `{"type":["integer","null"]}`, InputValue: 123, ExpectValue: `123`},
		{ColumnType: "smallint", ExpectType: `{"type":["integer","null"]}`, InputValue: 123, ExpectValue: `123`},
		{ColumnType: "mediumint", ExpectType: `{"type":["integer","null"]}`, InputValue: 123, ExpectValue: `123`},
		{ColumnType: "int", ExpectType: `{"type":["integer","null"]}`, InputValue: 123, ExpectValue: `123`},
		{ColumnType: "bigint", ExpectType: `{"type":["integer","null"]}`, InputValue: -1234567890123456789, ExpectValue: `-1234567890123456789`},

		// MySQL "boolean" type is a synonym for tinyint(1)
		{ColumnType: "boolean", ExpectType: `{"type":["integer","null"]}`, InputValue: 0, ExpectValue: `0`},
		{ColumnType: "boolean", ExpectType: `{"type":["integer","null"]}`, InputValue: 1, ExpectValue: `1`},
		{ColumnType: "boolean", ExpectType: `{"type":["integer","null"]}`, InputValue: true, ExpectValue: `1`},
		{ColumnType: "boolean", ExpectType: `{"type":["integer","null"]}`, InputValue: false, ExpectValue: `0`},

		// MySQL `BIT(n)` acts like an integer most of the time, but binlog replication sees
		// them as a `[]byte`. We translate this, and the `bit(14)` test case is intended to
		// verify correct endianness for multi-byte representations.
		{ColumnType: "bit(5)", ExpectType: `{"type":["integer","null"]}`, InputValue: 0b11010, ExpectValue: `26`},
		{ColumnType: "bit(14)", ExpectType: `{"type":["integer","null"]}`, InputValue: 0b11010101101111, ExpectValue: `13679`},

		// Floating-Point Types
		{ColumnType: "float", ExpectType: `{"type":["number","null"]}`, InputValue: 123.456, ExpectValue: `123.456`},
		{ColumnType: "double precision", ExpectType: `{"type":["number","null"]}`, InputValue: 123.456, ExpectValue: `123.456`},
		{ColumnType: "real", ExpectType: `{"type":["number","null"]}`, InputValue: 123.456, ExpectValue: `123.456`},

		// Fixed-Precision Decimals
		{ColumnType: "decimal(5,2)", ExpectType: `{"type":["string","null"]}`, InputValue: 123.45, ExpectValue: `"123.45"`},
		{ColumnType: "decimal(15,2)", ExpectType: `{"type":["string","null"]}`, InputValue: 1234567890123.451, ExpectValue: `"1234567890123.45"`},
		{ColumnType: "numeric(5,2)", ExpectType: `{"type":["string","null"]}`, InputValue: 123.45, ExpectValue: `"123.45"`},
		{ColumnType: "numeric(15,2)", ExpectType: `{"type":["string","null"]}`, InputValue: 1234567890123.451, ExpectValue: `"1234567890123.45"`},

		// MySQL strips trailing spaces from CHAR on retrieval, and doesn't do that for VARCHAR
		{ColumnType: "char(5)", ExpectType: `{"type":["string","null"]}`, InputValue: "foo", ExpectValue: `"foo"`},
		{ColumnType: "char(5)", ExpectType: `{"type":["string","null"]}`, InputValue: "foo  ", ExpectValue: `"foo"`},
		{ColumnType: "varchar(5)", ExpectType: `{"type":["string","null"]}`, InputValue: "foo", ExpectValue: `"foo"`},
		{ColumnType: "varchar(5)", ExpectType: `{"type":["string","null"]}`, InputValue: "foo  ", ExpectValue: `"foo  "`},
		{ColumnType: "tinytext", ExpectType: `{"type":["string","null"]}`, InputValue: "foo", ExpectValue: `"foo"`},
		{ColumnType: "text", ExpectType: `{"type":["string","null"]}`, InputValue: "foo", ExpectValue: `"foo"`},
		{ColumnType: "mediumtext", ExpectType: `{"type":["string","null"]}`, InputValue: "foo", ExpectValue: `"foo"`},
		{ColumnType: "longtext", ExpectType: `{"type":["string","null"]}`, InputValue: "foo", ExpectValue: `"foo"`},

		// TODO(wgd): The BINARY(n) type has a mild inconsistency in its treatment of trailing null bytes
		// between backfill and replication.
		{ColumnType: "binary(5)", ExpectType: `{"type":["string","null"],"contentEncoding":"base64"}`, InputValue: []byte{0x12, 0x34, 0x56, 0x78, 0x9A}, ExpectValue: `"EjRWeJo="`},
		{ColumnType: "varbinary(5)", ExpectType: `{"type":["string","null"],"contentEncoding":"base64"}`, InputValue: []byte{0x12, 0x34, 0x56, 0x78}, ExpectValue: `"EjRWeA=="`},
		{ColumnType: "tinyblob", ExpectType: `{"type":["string","null"],"contentEncoding":"base64"}`, InputValue: []byte{0x12, 0x34, 0x56, 0x78}, ExpectValue: `"EjRWeA=="`},
		{ColumnType: "blob", ExpectType: `{"type":["string","null"],"contentEncoding":"base64"}`, InputValue: []byte{0x12, 0x34, 0x56, 0x78}, ExpectValue: `"EjRWeA=="`},
		{ColumnType: "mediumblob", ExpectType: `{"type":["string","null"],"contentEncoding":"base64"}`, InputValue: []byte{0x12, 0x34, 0x56, 0x78}, ExpectValue: `"EjRWeA=="`},
		{ColumnType: "longblob", ExpectType: `{"type":["string","null"],"contentEncoding":"base64"}`, InputValue: []byte{0x12, 0x34, 0x56, 0x78}, ExpectValue: `"EjRWeA=="`},

		// TODO(wgd): Enums are reported differently in backfills vs replication. Backfill queries return
		// the string value of the column, while replicated change events appear to hold an integer index.
		// {ColumnType: "enum('small', 'medium', 'large')", ExpectType: `{"type":["string","null"]}`, InputValue: "medium", ExpectValue: `"medium"`},

		// TODO(wgd): Sets are reported differently in backfills vs replication. Backfill queries return
		// the string value of the column, while replicated change events appear to hold a bitfield integer.
		// {ColumnType: "SET('one', 'two')", ExpectType: `{"type":["string","null"]}`, InputValue: "one,two", ExpectValue: `"one,two"`},

		{ColumnType: "date", ExpectType: `{"type":["string","null"]}`, InputValue: "1991-08-31", ExpectValue: `"1991-08-31"`},
		{ColumnType: "datetime", ExpectType: `{"type":["string","null"]}`, InputValue: "1991-08-31 12:34:56", ExpectValue: `"1991-08-31 12:34:56"`},
		{ColumnType: "datetime", ExpectType: `{"type":["string","null"]}`, InputValue: "1991-08-31 12:34:56.987654", ExpectValue: `"1991-08-31 12:34:57"`},
		// TODO(wgd): Timestamps are reported differently in backfills vs replication because backfill
		// queries do time-zone conversion while the replicated events appear to be un-converted.
		// {ColumnType: "timestamp", ExpectType: `{"type":["string","null"]}`, InputValue: "1991-08-31 12:34:56", ExpectValue: `"1991-08-31 12:34:56"`},
		// {ColumnType: "timestamp", ExpectType: `{"type":["string","null"]}`, InputValue: "1991-08-31 12:34:56.987654", ExpectValue: `"1991-08-31 12:34:56.987654"`},
		{ColumnType: "time", ExpectType: `{"type":["string","null"]}`, InputValue: "765:43:21", ExpectValue: `"765:43:21"`},
		{ColumnType: "year", ExpectType: `{"type":["integer","null"]}`, InputValue: "2003", ExpectValue: `2003`},

		{ColumnType: "json", ExpectType: `{}`, InputValue: `{"type": "test", "data": 123}`, ExpectValue: `{"data":123,"type":"test"}`},
	})
}
