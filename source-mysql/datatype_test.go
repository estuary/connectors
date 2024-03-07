package main

import (
	"context"
	"fmt"
	"regexp"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/sqlcapture/tests"
)

// TestDatatypes runs the discovery test on various datatypes.
func TestDatatypes(t *testing.T) {
	var tb, ctx = mysqlTestBackend(t), context.Background()

	// Tell MySQL to act as though we're running in Chicago. This has an effect (in very
	// different ways) on the processing of DATETIME and TIMESTAMP values.
	tb.Query(ctx, t, "SET GLOBAL time_zone = 'America/Chicago';")
	tb.Query(ctx, t, "SET SESSION time_zone = 'America/Chicago';")

	// For testing inputs of "zero" value datetime and timestamp 'NO_ZERO_DATE' must be disabled for
	// the "zero" values to be inserted into the test database tables.
	tb.Query(ctx, t, "SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'NO_ZERO_DATE',''));")

	tests.TestDatatypes(ctx, t, tb, []tests.DatatypeTestCase{
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
		{ColumnType: "float(53)", ExpectType: `{"type":["number","null"]}`, InputValue: 0.1234567891234, ExpectValue: `0.1234567891234`},
		{ColumnType: "double precision", ExpectType: `{"type":["number","null"]}`, InputValue: 123.456, ExpectValue: `123.456`},
		{ColumnType: "real", ExpectType: `{"type":["number","null"]}`, InputValue: 123.456, ExpectValue: `123.456`},

		// Fixed-Precision Decimals
		{ColumnType: "decimal(5,2)", ExpectType: `{"type":["string","null"],"format":"number"}`, InputValue: 123.45, ExpectValue: `"123.45"`},
		{ColumnType: "decimal(15,2)", ExpectType: `{"type":["string","null"],"format":"number"}`, InputValue: 1234567890123.451, ExpectValue: `"1234567890123.45"`},
		{ColumnType: "numeric(5,2)", ExpectType: `{"type":["string","null"],"format":"number"}`, InputValue: 123.45, ExpectValue: `"123.45"`},
		{ColumnType: "numeric(15,2)", ExpectType: `{"type":["string","null"],"format":"number"}`, InputValue: 1234567890123.451, ExpectValue: `"1234567890123.45"`},

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

		{ColumnType: `enum('sm', 'med', 'lg')`, ExpectType: `{"type":["string","null"],"enum":["","sm","med","lg",null]}`, InputValue: nil, ExpectValue: `null`},
		{ColumnType: `enum('sm', 'med', 'lg') not null`, ExpectType: `{"type":"string","enum":["","sm","med","lg"]}`, InputValue: "sm", ExpectValue: `"sm"`},
		{ColumnType: `enum('s,m', 'med', '\'lg\'')`, ExpectType: `{"type":["string","null"],"enum":["","s,m","med","'lg'",null]}`, InputValue: `'lg'`, ExpectValue: `"'lg'"`},
		{ColumnType: `enum('s,m', 'med', '\'lg\'')`, ExpectType: `{"type":["string","null"],"enum":["","s,m","med","'lg'",null]}`, InputValue: `invalid`, ExpectValue: `""`},

		{ColumnType: "set('a', 'b', 'c')", ExpectType: `{"type":["string","null"]}`, InputValue: "b", ExpectValue: `"b"`},
		{ColumnType: "set('a', 'b', 'c')", ExpectType: `{"type":["string","null"]}`, InputValue: "a,c", ExpectValue: `"a,c"`},

		{ColumnType: "date", ExpectType: `{"type":["string","null"]}`, InputValue: "1991-08-31", ExpectValue: `"1991-08-31"`},
		{ColumnType: "time", ExpectType: `{"type":["string","null"]}`, InputValue: "765:43:21", ExpectValue: `"765:43:21"`},
		{ColumnType: "year", ExpectType: `{"type":["integer","null"]}`, InputValue: "2003", ExpectValue: `2003`},

		// The DATETIME column type will be stored verbatim by MySQL, and we will interpret it as being in
		// the time zone specified as the `time_zone` system variable. In this case that's 'America/Chicago'
		// so when captured values are emitted as UTC they will have 5 or 6 hours (depending on the date
		// of the input) added at capture time by the connector.
		//
		// This test fails on MariaDB because it truncates fractional seconds rather than rounding (see also: https://jira.mariadb.org/browse/MDEV-16991).
		{ColumnType: "datetime", ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: "1991-08-31 12:34:56.987654", ExpectValue: `"1991-08-31T17:34:57Z"`},
		{ColumnType: "datetime", ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: "1991-08-31 12:34:56", ExpectValue: `"1991-08-31T17:34:56Z"`},
		{ColumnType: "datetime", ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: "1992-01-01 12:34:56", ExpectValue: `"1992-01-01T18:34:56Z"`},
		// Handle the special "zero" value datetime by converting it to a valid sentinel RFC3339 datetime.
		{ColumnType: "datetime", ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: "0000-00-00 00:00:00", ExpectValue: `"0001-01-01T00:00:00Z"`},
		{ColumnType: "datetime(6)", ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: "0000-00-00 00:00:00", ExpectValue: `"0001-01-01T00:00:00Z"`},

		// The TIMESTAMP column type will be converted by MySQL from the local time zone (which as mentioned
		// above was set to 'America/Chicago' and acts as UTC-5 or UTC-6 depending on the date) to UTC for
		// storage, and we will capture that value as UTC. Thus the captured value will have 5 hours added
		// at input time by the MySQL database.
		{ColumnType: "timestamp", ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: "1991-08-31 12:34:56", ExpectValue: `"1991-08-31T17:34:56Z"`},
		{ColumnType: "timestamp", ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: "1991-08-31 12:34:56.987654", ExpectValue: `"1991-08-31T17:34:57Z"`},
		{ColumnType: "timestamp(4)", ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: "1991-08-31 12:34:56.987654", ExpectValue: `"1991-08-31T17:34:56.9877Z"`},
		{ColumnType: "timestamp", ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: "1992-01-01 12:34:56", ExpectValue: `"1992-01-01T18:34:56Z"`},
		// Handle the special "zero" value timestamp by converting it to a valid sentinel RFC3339 timestamp.
		{ColumnType: "timestamp", ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: "0000-00-00 00:00:00", ExpectValue: `"0001-01-01T00:00:00Z"`},

		// This test fails on MariaDB, because the 'JSON' column type is just an alias for LONGTEXT
		// and will result in the original input JSON being captured as a string. See also:
		// https://mariadb.com/kb/en/json-data-type/#differences-between-mysql-json-strings-and-mariadb-json-strings
		{ColumnType: "json", ExpectType: `{}`, InputValue: `{"type": "test", "data": 123}`, ExpectValue: `{"data":123,"type":"test"}`},
	})
}

func TestDatetimes(t *testing.T) {
	var tb, ctx = mysqlTestBackend(t), context.Background()

	// In Chicago noon should map to 17:00 or 18:00 UTC in summer/winter respectively due to DST.
	t.Run("chicago", func(t *testing.T) {
		tb.Query(ctx, t, "SET GLOBAL time_zone = 'America/Chicago';")
		tb.Query(ctx, t, "SET SESSION time_zone = 'America/Chicago';")
		tests.TestDatatypes(ctx, t, tb, []tests.DatatypeTestCase{
			{ColumnType: "datetime", ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: "1991-08-31 12:34:56", ExpectValue: `"1991-08-31T17:34:56Z"`},
			{ColumnType: "datetime", ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: "1992-01-01 12:34:56", ExpectValue: `"1992-01-01T18:34:56Z"`},
			{ColumnType: "timestamp", ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: "1991-08-31 12:34:56", ExpectValue: `"1991-08-31T17:34:56Z"`},
			{ColumnType: "timestamp", ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: "1992-01-01 12:34:56", ExpectValue: `"1992-01-01T18:34:56Z"`},
		})
	})

	// In fixed offset UTC-6 noon should always map to 18:00 UTC regardless of the time of the year.
	t.Run("utc_minus_6", func(t *testing.T) {
		tb.Query(ctx, t, "SET GLOBAL time_zone = '-6:00';") // Leading zero deliberately omitted to make sure MySQL normalizes it into something we can parse
		tb.Query(ctx, t, "SET SESSION time_zone = '-6:00';")
		tests.TestDatatypes(ctx, t, tb, []tests.DatatypeTestCase{
			{ColumnType: "datetime", ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: "1991-08-31 12:34:56", ExpectValue: `"1991-08-31T18:34:56Z"`},
			{ColumnType: "datetime", ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: "1992-01-01 12:34:56", ExpectValue: `"1992-01-01T18:34:56Z"`},
			{ColumnType: "timestamp", ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: "1991-08-31 12:34:56", ExpectValue: `"1991-08-31T18:34:56Z"`},
			{ColumnType: "timestamp", ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: "1992-01-01 12:34:56", ExpectValue: `"1992-01-01T18:34:56Z"`},
		})
	})

	// In Manila noon should map to 04:00 UTC regardless of the time of the year, because Philippines
	// Standard Time stopped observing DST in 1990.
	t.Run("manila", func(t *testing.T) {
		tb.Query(ctx, t, "SET GLOBAL time_zone = 'Asia/Manila';")
		tb.Query(ctx, t, "SET SESSION time_zone = 'Asia/Manila';")
		tests.TestDatatypes(ctx, t, tb, []tests.DatatypeTestCase{
			{ColumnType: "datetime", ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: "1991-08-31 12:34:56", ExpectValue: `"1991-08-31T04:34:56Z"`},
			{ColumnType: "datetime", ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: "1992-01-01 12:34:56", ExpectValue: `"1992-01-01T04:34:56Z"`},
			{ColumnType: "timestamp", ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: "1991-08-31 12:34:56", ExpectValue: `"1991-08-31T04:34:56Z"`},
			{ColumnType: "timestamp", ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: "1992-01-01 12:34:56", ExpectValue: `"1992-01-01T04:34:56Z"`},
		})
	})
}

func TestScanKeyDatetimes(t *testing.T) {
	var tb, ctx = mysqlTestBackend(t), context.Background()
	tb.Query(ctx, t, "SET GLOBAL time_zone = 'America/Chicago';")
	tb.Query(ctx, t, "SET SESSION time_zone = 'America/Chicago';")

	var uniqueID = "42322082"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(ts DATETIME(3) PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, tableName, [][]interface{}{
		{"1991-08-31 12:34:56.000", "aood"},
		{"1991-08-31 12:34:56.111", "xwxt"},
		{"1991-08-31 12:34:56.222", "tpxi"},
		{"1991-08-31 12:34:56.333", "jvqz"},
		{"1991-08-31 12:34:56.444", "juwf"},
		{"1991-08-31 12:34:56.555", "znzn"},
		{"1991-08-31 12:34:56.666", "zocp"},
		{"1991-08-31 12:34:56.777", "pxoi"},
		{"1991-08-31 12:34:56.888", "vdug"},
		{"1991-08-31 12:34:56.999", "xerk"},
	})
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

	// Reduce the backfill chunk size to 1 row. Since the capture will be killed and
	// restarted after each scan key update, this means we'll advance over the keys
	// one by one.
	cs.EndpointSpec.(*Config).Advanced.BackfillChunkSize = 1

	var summary, _ = tests.RestartingBackfillCapture(ctx, t, cs)
	cupaloy.SnapshotT(t, summary)
}

func TestScanKeyTypes(t *testing.T) {
	var tb, ctx = mysqlTestBackend(t), context.Background()
	for idx, tc := range []struct {
		Name       string
		ColumnType string
		Values     []interface{}
	}{
		{"Bool", "BOOLEAN", []interface{}{"1", "0"}},
		{"Integer", "INTEGER", []interface{}{0, -3, 2, 1723}},
		{"SmallInt", "SMALLINT", []interface{}{0, -3, 2, 1723}},
		{"BigInt", "BIGINT", []interface{}{0, -3, 2, 1723}},
		{"Real", "REAL", []interface{}{-0.9, -1.0, -1.1, 0.0, 0.9, 1.0, 1.111, 1.222, 1.333, 1.444, 1.555, 1.666, 1.777, 1.888, 1.999, 2.000}},
		{"Double", "DOUBLE PRECISION", []interface{}{-0.9, -1.0, -1.1, 0.0, 0.9, 1.0, 1.111, 1.222, 1.333, 1.444, 1.555, 1.666, 1.777, 1.888, 1.999, 2.000}},
		{"Decimal", "DECIMAL(4,3)", []interface{}{-0.9, -1.0, -1.1, 0.0, 0.9, 1.0, 1.111, 1.222, 1.333, 1.444, 1.555, 1.666, 1.777, 1.888, 1.999, 2.000}},
		{"Numeric", "NUMERIC(4,3)", []interface{}{-0.9, -1.0, -1.1, 0.0, 0.9, 1.0, 1.111, 1.222, 1.333, 1.444, 1.555, 1.666, 1.777, 1.888, 1.999, 2.000}},
		{"VarChar", "VARCHAR(10)", []interface{}{"", "a", "d", "g", "B", "E", "H", "_c", "_f", "_i"}},
		{"Char", "CHAR(3)", []interface{}{"", "a", "d", "g", "B", "E", "H", "_c", "_f", "_i"}},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			var uniqueID = fmt.Sprintf("75416126%04d", idx)
			var tableName = tb.CreateTable(ctx, t, uniqueID, fmt.Sprintf("(k %s PRIMARY KEY, data TEXT)", tc.ColumnType))
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
