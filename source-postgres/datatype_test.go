package main

import (
	"context"
	"fmt"
	"regexp"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/connectors/sqlcapture/tests"
)

const arraySchemaPatternNotNullable = `{"type":"array","items":%s}`
const arraySchemaPattern = `{"type":["array","null"],"items":%s}`

// TestDatatypes runs the generic datatype discovery and round-tripping test on various datatypes.
func TestDatatypes(t *testing.T) {
	var ctx = context.Background()
	tests.TestDatatypes(ctx, t, postgresTestBackend(t), []tests.DatatypeTestCase{
		// Basic Boolean/Numeric/Text Types
		{ColumnType: `boolean`, ExpectType: `{"type":["boolean","null"]}`, InputValue: `false`, ExpectValue: `false`},
		{ColumnType: `boolean`, ExpectType: `{"type":["boolean","null"]}`, InputValue: `yes`, ExpectValue: `true`},
		{ColumnType: `integer`, ExpectType: `{"type":["integer","null"]}`, InputValue: `123`, ExpectValue: `123`},
		{ColumnType: `integer`, ExpectType: `{"type":["integer","null"]}`, InputValue: 123, ExpectValue: `123`},
		{ColumnType: `integer`, ExpectType: `{"type":["integer","null"]}`, InputValue: nil, ExpectValue: `null`},
		{ColumnType: `integer not null`, ExpectType: `{"type":"integer"}`, InputValue: `123`, ExpectValue: `123`},
		{ColumnType: `smallint`, ExpectType: `{"type":["integer","null"]}`, InputValue: `123`, ExpectValue: `123`},
		{ColumnType: `bigint`, ExpectType: `{"type":["integer","null"]}`, InputValue: `123`, ExpectValue: `123`},
		{ColumnType: `serial`, ExpectType: `{"type":"integer"}`, InputValue: `123`, ExpectValue: `123`},      // non-nullable
		{ColumnType: `smallserial`, ExpectType: `{"type":"integer"}`, InputValue: `123`, ExpectValue: `123`}, // non-nullable
		{ColumnType: `bigserial`, ExpectType: `{"type":"integer"}`, InputValue: `123`, ExpectValue: `123`},   // non-nullable
		{ColumnType: `oid not null`, ExpectType: `{"type":"integer"}`, InputValue: `54321`, ExpectValue: `54321`},
		{ColumnType: `oid`, ExpectType: `{"type":["integer","null"]}`, InputValue: `54321`, ExpectValue: `54321`},
		{ColumnType: `real`, ExpectType: `{"type":["number","string","null"],"format":"number"}`, InputValue: `123.456`, ExpectValue: `123.456`},
		{ColumnType: `double precision`, ExpectType: `{"type":["number","string","null"],"format":"number"}`, InputValue: `123.456`, ExpectValue: `123.456`},
		{ColumnType: `real`, ExpectType: `{"type":["number","string","null"],"format":"number"}`, InputValue: `NaN`, ExpectValue: `"NaN"`},
		{ColumnType: `double precision`, ExpectType: `{"type":["number","string","null"],"format":"number"}`, InputValue: `NaN`, ExpectValue: `"NaN"`},

		{ColumnType: `decimal`, ExpectType: `{"type":["string","null"],"format":"number"}`, InputValue: `123.456`, ExpectValue: `"123.456"`},
		{ColumnType: `numeric`, ExpectType: `{"type":["string","null"],"format":"number"}`, InputValue: `123.456`, ExpectValue: `"123.456"`},
		{ColumnType: `numeric`, ExpectType: `{"type":["string","null"],"format":"number"}`, InputValue: `NaN`, ExpectValue: `"NaN"`},
		{ColumnType: `numeric`, ExpectType: `{"type":["string","null"],"format":"number"}`, InputValue: `-infinity`, ExpectValue: `"-Infinity"`},
		{ColumnType: `numeric(4,2)`, ExpectType: `{"type":["string","null"],"format":"number"}`, InputValue: `12.34`, ExpectValue: `"12.34"`},
		{ColumnType: `character varying(10)`, ExpectType: `{"type":["string","null"],"maxLength":10}`, InputValue: `foo`, ExpectValue: `"foo"`},
		{ColumnType: `varchar(10)`, ExpectType: `{"type":["string","null"],"maxLength":10}`, InputValue: `foo`, ExpectValue: `"foo"`},
		{ColumnType: `varchar`, ExpectType: `{"type":["string","null"]}`, InputValue: `foo`, ExpectValue: `"foo"`},
		{ColumnType: `character(10)`, ExpectType: `{"type":["string","null"],"minLength":10,"maxLength":10}`, InputValue: `foo`, ExpectValue: `"foo       "`},
		{ColumnType: `char(10)`, ExpectType: `{"type":["string","null"],"minLength":10,"maxLength":10}`, InputValue: `foo`, ExpectValue: `"foo       "`},
		{ColumnType: `char`, ExpectType: `{"type":["string","null"],"minLength":1,"maxLength":1}`, InputValue: `f`, ExpectValue: `"f"`},
		{ColumnType: `text`, ExpectType: `{"type":["string","null"]}`, InputValue: `foo`, ExpectValue: `"foo"`},
		{ColumnType: `text`, ExpectType: `{"type":["string","null"]}`, InputValue: nil, ExpectValue: `null`},
		{ColumnType: `bytea`, ExpectType: `{"type":["string","null"],"contentEncoding":"base64"}`, InputValue: `\xDEADBEEF`, ExpectValue: `"3q2+7w=="`},
		{ColumnType: `bit`, ExpectType: `{"type":["string","null"]}`, InputValue: `1`, ExpectValue: `"1"`},
		{ColumnType: `bit(3)`, ExpectType: `{"type":["string","null"]}`, InputValue: `101`, ExpectValue: `"101"`},
		{ColumnType: `bit varying`, ExpectType: `{"type":["string","null"]}`, InputValue: `1101`, ExpectValue: `"1101"`},
		{ColumnType: `bit varying(5)`, ExpectType: `{"type":["string","null"]}`, InputValue: `10111`, ExpectValue: `"10111"`},

		// Date/Time/Timestamp Types
		{ColumnType: `date`, ExpectType: `{"type":["string","null"],"format":"date"}`, InputValue: `'January 8, 1999'`, ExpectValue: `"1999-01-08"`},
		{ColumnType: `date`, ExpectType: `{"type":["string","null"],"format":"date"}`, InputValue: `'January 8, 22021'`, ExpectValue: `"0000-01-01"`},
		{ColumnType: `date`, ExpectType: `{"type":["string","null"],"format":"date"}`, InputValue: nil, ExpectValue: `null`},
		{ColumnType: `timestamp`, ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: `'January 8, 1999'`, ExpectValue: `"1999-01-08T00:00:00Z"`},
		{ColumnType: `timestamp without time zone`, ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: `'January 8, 1999'`, ExpectValue: `"1999-01-08T00:00:00Z"`},
		{ColumnType: `timestamp`, ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: `'infinity'`, ExpectValue: `"9999-12-31T23:59:59Z"`},
		{ColumnType: `timestamp`, ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: `'-infinity'`, ExpectValue: `"0000-01-01T00:00:00Z"`},
		{ColumnType: `timestamp`, ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: `'epoch'`, ExpectValue: `"1970-01-01T00:00:00Z"`},
		{ColumnType: `timestamp`, ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: `'20222-08-31T00:00:00Z'`, ExpectValue: `"0000-01-01T00:00:00Z"`},
		{ColumnType: `timestamp`, ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: `'January 8, 99 BC'`, ExpectValue: `"0000-01-01T00:00:00Z"`},
		{ColumnType: `timestamp with time zone`, ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: `'January 8, 1999 UTC'`, ExpectValue: `"1999-01-07T16:00:00-08:00"`},
		{ColumnType: `time`, ExpectType: `{"type":["string","null"],"format":"time"}`, InputValue: `'04:05:06 PST'`, ExpectValue: `"04:05:06Z"`},
		{ColumnType: `time without time zone`, ExpectType: `{"type":["string","null"],"format":"time"}`, InputValue: `'04:05:06 PST'`, ExpectValue: `"04:05:06Z"`},
		{ColumnType: `time`, ExpectType: `{"type":["string","null"],"format":"time"}`, InputValue: nil, ExpectValue: `null`},
		{ColumnType: `time with time zone`, ExpectType: `{"type":["string","null"],"format":"time"}`, InputValue: `'04:05:06 PST'`, ExpectValue: `"04:05:06-08:00"`},
		{ColumnType: `time with time zone`, ExpectType: `{"type":["string","null"],"format":"time"}`, InputValue: `'04:05:06 UTC'`, ExpectValue: `"04:05:06Z"`},
		{ColumnType: `time with time zone`, ExpectType: `{"type":["string","null"],"format":"time"}`, InputValue: `'04:05:06.123 UTC'`, ExpectValue: `"04:05:06.123Z"`},
		{ColumnType: `time with time zone`, ExpectType: `{"type":["string","null"],"format":"time"}`, InputValue: `'04:05:06.123+0330'`, ExpectValue: `"04:05:06.123+03:30"`},
		{ColumnType: `time with time zone`, ExpectType: `{"type":["string","null"],"format":"time"}`, InputValue: `'04:05:06.123+03:30:50'`, ExpectValue: `"04:05:06.123+03:30:50"`},
		{ColumnType: `interval`, ExpectType: `{"type":["string","null"]}`, InputValue: `'2 months 1 day 5 minutes 6 seconds'`, ExpectValue: `"2 mon 1 day 00:05:06"`},
		{ColumnType: `interval`, ExpectType: `{"type":["string","null"]}`, InputValue: `'2 months 1 day 5 minutes 6 seconds 1 microsecond'`, ExpectValue: `"2 mon 1 day 00:05:06.000001"`},

		// Geometry Types
		{ColumnType: `point`, ExpectType: `{"type":["string","null"]}`, InputValue: `(1, 2)`, ExpectValue: `"(1,2)"`},
		{ColumnType: `line`, ExpectType: `{"type":["string","null"]}`, InputValue: `{1, 2, 3}`, ExpectValue: `"{1,2,3}"`},
		{ColumnType: `lseg`, ExpectType: `{"type":["string","null"]}`, InputValue: `[(1, 2), (3, 4)]`, ExpectValue: `"[(1,2),(3,4)]"`},
		{ColumnType: `box`, ExpectType: `{"type":["string","null"]}`, InputValue: `((1, 2), (3, 4))`, ExpectValue: `"(3,4),(1,2)"`},
		{ColumnType: `path`, ExpectType: `{"type":["string","null"]}`, InputValue: `[(1, 2), (3, 4), (5, 6)]`, ExpectValue: `"[(1,2),(3,4),(5,6)]"`},
		{ColumnType: `polygon`, ExpectType: `{"type":["string","null"]}`, InputValue: `((0, 0), (0, 1), (1, 0))`, ExpectValue: `"((0,0),(0,1),(1,0))"`},
		{ColumnType: `circle`, ExpectType: `{"type":["string","null"]}`, InputValue: `((1, 2), 3)`, ExpectValue: `"\u003c(1,2),3\u003e"`},

		// Miscellaneous domain-specific datatypes
		{ColumnType: `money`, ExpectType: `{"type":["string","null"]}`, InputValue: 123.45, ExpectValue: `"$123.45"`},
		{ColumnType: `money`, ExpectType: `{"type":["string","null"]}`, InputValue: `$123.45`, ExpectValue: `"$123.45"`},
		{ColumnType: `inet`, ExpectType: `{"type":["string","null"]}`, InputValue: `192.168.100.0/24`, ExpectValue: `"192.168.100.0/24"`},
		{ColumnType: `inet`, ExpectType: `{"type":["string","null"]}`, InputValue: `2001:4f8:3:ba::/64`, ExpectValue: `"2001:4f8:3:ba::/64"`},
		{ColumnType: `cidr`, ExpectType: `{"type":["string","null"]}`, InputValue: `192.168.100.0/24`, ExpectValue: `"192.168.100.0/24"`},
		{ColumnType: `cidr`, ExpectType: `{"type":["string","null"]}`, InputValue: `2001:4f8:3:ba::/64`, ExpectValue: `"2001:4f8:3:ba::/64"`},
		{ColumnType: `macaddr`, ExpectType: `{"type":["string","null"]}`, InputValue: `08:00:2b:01:02:03`, ExpectValue: `"08:00:2b:01:02:03"`},
		{ColumnType: `macaddr8`, ExpectType: `{"type":["string","null"]}`, InputValue: `08-00-2b-01-02-03-04-05`, ExpectValue: `"08:00:2b:01:02:03:04:05"`},
		{ColumnType: `uuid`, ExpectType: `{"type":["string","null"],"format":"uuid"}`, InputValue: `a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11`, ExpectValue: `"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"`},
		{ColumnType: `tsvector`, ExpectType: `{"type":["string","null"]}`, InputValue: `a fat cat`, ExpectValue: `"'a' 'cat' 'fat'"`},
		{ColumnType: `tsquery`, ExpectType: `{"type":["string","null"]}`, InputValue: `fat & cat`, ExpectValue: `"'fat' \u0026 'cat'"`},

		// The difference between JSON and JSONB is that JSONB is stored in a binary format internally on
		// the server while JSON is stored as the raw input text. In both cases we receive JSON from the
		// server and preserve it as a json.RawMessage, but the final document serialization still does
		// some whitespace noramlization. Thus both JSON and JSONB have unnecessary spaces removed but
		// only JSONB has the fields reordered.
		{ColumnType: `json`, ExpectType: `{}`, InputValue: `{    "type": "hello world", "data": 123}`, ExpectValue: `{"type":"hello world","data":123}`},
		{ColumnType: `jsonb`, ExpectType: `{}`, InputValue: `{    "type": "hello world", "data": 123}`, ExpectValue: `{"data":123,"type":"hello world"}`},
		{ColumnType: `jsonpath`, ExpectType: `{"type":["string","null"]}`, InputValue: `$foo`, ExpectValue: `"$\"foo\""`},
		{ColumnType: `xml`, ExpectType: `{"type":["string","null"]}`, InputValue: `<foo>bar &gt; baz</foo>`, ExpectValue: `"\u003cfoo\u003ebar \u0026gt; baz\u003c/foo\u003e"`},

		// PostgreSQL doesn't actually check array sizes or dimensionality. Per the documentation:
		//    [...] declaring the array size or number of dimensions in CREATE TABLE is
		//    simply documentation; it does not affect run-time behavior.
		// This set of test cases exercises that expectation.
		{ColumnType: `integer[3][3]`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["integer","null"]}`), InputValue: `{}`, ExpectValue: `[]`},
		{ColumnType: `integer[3][3]`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["integer","null"]}`), InputValue: `{{{{{{1}}}}}}`, ExpectValue: `[1]`},
		{ColumnType: `integer[3][3]`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["integer","null"]}`), InputValue: `{1,2,3,4,5,6}`, ExpectValue: `[1,2,3,4,5,6]`},
		{ColumnType: `integer[3][3]`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["integer","null"]}`), InputValue: `{{1,2,3},{4,5,6},{7,8,9}}`, ExpectValue: `[1,2,3,4,5,6,7,8,9]`},

		// Sanity-check various types of array for discovery and round-tripping
		{ColumnType: `bigint ARRAY NOT NULL`, ExpectType: fmt.Sprintf(arraySchemaPatternNotNullable, `{"type":["integer","null"]}`), InputValue: `{1,2,null,4}`, ExpectValue: `[1,2,null,4]`},
		{ColumnType: `bigint ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["integer","null"]}`), InputValue: `{1,2,null,4}`, ExpectValue: `[1,2,null,4]`},
		{ColumnType: `bigint ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["integer","null"]}`), InputValue: nil, ExpectValue: `null`},
		{ColumnType: `boolean ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["boolean","null"]}`), InputValue: `{true, false, null, true}`, ExpectValue: `[true,false,null,true]`},
		{ColumnType: `bytea ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["string","null"],"contentEncoding":"base64"}`), InputValue: `{abcd, efgh}`, ExpectValue: `["YWJjZA==","ZWZnaA=="]`},
		{ColumnType: `varchar(12) ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["string","null"],"maxLength":12}`), InputValue: `{foo, bar}`, ExpectValue: `["foo","bar"]`},
		{ColumnType: `char(5) ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["string","null"],"minLength":5,"maxLength":5}`), InputValue: `{foo, bar}`, ExpectValue: `["foo  ","bar  "]`},
		{ColumnType: `cidr ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["string","null"]}`), InputValue: `{192.168.100.0/24, 2001:4f8:3:ba::/64}`, ExpectValue: `["192.168.100.0/24","2001:4f8:3:ba::/64"]`},
		{ColumnType: `date ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["string","null"],"format":"date"}`), InputValue: []interface{}{`2022-01-09`, `2022-01-10`}, ExpectValue: `["2022-01-09","2022-01-10"]`},
		{ColumnType: `double precision ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["number","string","null"],"format":"number"}`), InputValue: []interface{}{1.23, 4.56}, ExpectValue: `[1.23,4.56]`},
		{ColumnType: `inet ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["string","null"]}`), InputValue: []interface{}{`192.168.100.0/24`, `2001:4f8:3:ba::/64`}, ExpectValue: `["192.168.100.0/24","2001:4f8:3:ba::/64"]`},
		{ColumnType: `integer ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["integer","null"]}`), InputValue: []interface{}{1, 2, nil, 4}, ExpectValue: `[1,2,null,4]`},
		{ColumnType: `numeric ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["string","null"],"format":"number"}`), InputValue: []interface{}{`123.456`, `-789.0123`}, ExpectValue: `["123.456","-789.0123"]`},
		{ColumnType: `real ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["number","string","null"],"format":"number"}`), InputValue: []interface{}{123.456, 789.012}, ExpectValue: `[123.456,789.012]`},
		{ColumnType: `smallint ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["integer","null"]}`), InputValue: []interface{}{123, 456, 789}, ExpectValue: `[123,456,789]`},
		{ColumnType: `text ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["string","null"]}`), InputValue: []interface{}{`Hello, world!`, `asdf`}, ExpectValue: `["Hello, world!","asdf"]`},
		{ColumnType: `uuid ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["string","null"],"format":"uuid"}`), InputValue: []interface{}{`a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11`}, ExpectValue: `["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"]`},

		// Built-in range types.
		{ColumnType: `int4range`, ExpectType: `{"type":["string","null"]}`, InputValue: `[1,5)`, ExpectValue: `"[1,5)"`},
		{ColumnType: `int8range`, ExpectType: `{"type":["string","null"]}`, InputValue: `[,]`, ExpectValue: `"(,)"`},
		{ColumnType: `numrange`, ExpectType: `{"type":["string","null"]}`, InputValue: `(1.1,5.5]`, ExpectValue: `"(1.1,5.5]"`},
		{ColumnType: `tsrange`, ExpectType: `{"type":["string","null"]}`, InputValue: `[2010-01-01 11:30 UTC,2010-01-01 15:00 UTC)`, ExpectValue: `"[2010-01-01T11:30:00Z,2010-01-01T15:00:00Z)"`},
		{ColumnType: `tstzrange`, ExpectType: `{"type":["string","null"]}`, InputValue: `[2010-01-01 11:30 UTC,2010-01-01 15:00 UTC)`, ExpectValue: `"[2010-01-01T03:30:00-08:00,2010-01-01T07:00:00-08:00)"`},
		{ColumnType: `daterange`, ExpectType: `{"type":["string","null"]}`, InputValue: `(2010-01-01,2010-01-02]`, ExpectValue: `"[2010-01-02T00:00:00Z,2010-01-03T00:00:00Z)"`},

		// Extension types
		{ColumnType: `citext`, ExpectType: `{"type":["string","null"]}`, InputValue: `Hello, world!`, ExpectValue: `"Hello, world!"`},
		{ColumnType: `citext[]`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["string","null"]}`), InputValue: `{"Hello, world!",asdf}`, ExpectValue: `["Hello, world!","asdf"]`},
	})
}

func TestScanKeyTimestamps(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = "26812649"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(ts TIMESTAMP PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, tableName, [][]interface{}{
		{"1991-08-31T12:34:56.000Z", "aood"},
		{"1991-08-31T12:34:56.111Z", "xwxt"},
		{"1991-08-31T12:34:56.222Z", "tpxi"},
		{"1991-08-31T12:34:56.333Z", "jvqz"},
		{"1991-08-31T12:34:56.444Z", "juwf"},
		{"1991-08-31T12:34:56.555Z", "znzn"},
		{"1991-08-31T12:34:56.666Z", "zocp"},
		{"1991-08-31T12:34:56.777Z", "pxoi"},
		{"1991-08-31T12:34:56.888Z", "vdug"},
		{"1991-08-31T12:34:56.999Z", "xerk"},
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
	var tb, ctx = postgresTestBackend(t), context.Background()
	for idx, tc := range []struct {
		Name       string
		ColumnType string
		Values     []interface{}
	}{
		{"Bool", "BOOLEAN", []any{"true", "false"}},
		{"Integer", "INTEGER", []any{0, -3, 2, 1723}},
		{"SmallInt", "SMALLINT", []any{0, -3, 2, 1723}},
		{"BigInt", "BIGINT", []any{0, -3, 2, 1723}},
		{"Serial", "SERIAL", []any{0, -3, 2, 1723}},
		{"SmallSerial", "SMALLSERIAL", []any{0, -3, 2, 1723}},
		{"BigSerial", "BIGSERIAL", []any{0, -3, 2, 1723}},
		{"Real", "REAL", []any{-0.9, -1.0, -1.1, 0.0, 0.9, 1.0, 1.111, 1.222, 1.333, 1.444, 1.555, 1.666, 1.777, 1.888, 1.999, 2.000}},
		{"Double", "DOUBLE PRECISION", []any{-0.9, -1.0, -1.1, 0.0, 0.9, 1.0, 1.111, 1.222, 1.333, 1.444, 1.555, 1.666, 1.777, 1.888, 1.999, 2.000}},
		{"Decimal", "DECIMAL", []any{-0.9, -1.0, -1.1, 0.0, 0.9, 1.0, 1.111, 1.222, 1.333, 1.444, 1.555, 1.666, 1.777, 1.888, 1.999, 2.000}},
		{"Numeric", "NUMERIC(4,3)", []any{-0.9, -1.0, -1.1, 0.0, 0.9, 1.0, 1.111, 1.222, 1.333, 1.444, 1.555, 1.666, 1.777, 1.888, 1.999, 2.000}},
		{"VarChar", "VARCHAR(10)", []any{"", "   ", "a", "b", "c", "A", "B", "C", "_a", "_b", "_c"}},
		{"Char", "CHAR(3)", []any{"   ", "a", "b", "c", "A", "B", "C", "_a", "_b", "_c"}},
		{"Text", "TEXT", []any{"", "   ", "a", "b", "c", "A", "B", "C", "_a", "_b", "_c"}},
		{"UUID", "UUID", []any{"66b968a7-aeca-4401-8239-5d57958d1572", "4ab4044a-9aab-415c-96c6-17fa338060fa", "c32fb585-fc7f-4347-8fe2-97448f4e93cd"}},
		{"MAC6", "macaddr", []any{"47:7a:51:62:c3:aa", "6f:cf:f3:49:e0:b1", "65:d0:73:5a:5a:c9", "88:61:08:5b:ae:54"}},
		{"MAC8", "macaddr8", []any{"5f:99:67:ac:0d:df:88:3a ", "70:26:74:4c:ac:2c:38:59", "e3:70:99:89:b9:d8:e8:21 ", "a5:35:55:da:06:38:39:43"}},
		{"OID", "oid", []any{12345, 12344, 12346, 54321, 54323, 54322}},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			var uniqueID = fmt.Sprintf("2804%04d", idx)
			var tableName = tb.CreateTable(ctx, t, uniqueID, fmt.Sprintf("(key %s PRIMARY KEY, data TEXT)", tc.ColumnType))
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

func TestEnumScanKey(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()

	tb.Query(ctx, t, `DROP TYPE IF EXISTS UserEnum CASCADE`)
	tb.Query(ctx, t, `CREATE TYPE UserEnum AS ENUM ('red', 'green', 'blue')`)
	t.Cleanup(func() { tb.Query(ctx, t, `DROP TYPE UserEnum CASCADE`) })

	var uniqueID = "97825976"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER, color UserEnum, data TEXT, PRIMARY KEY (id, color))")

	tb.Insert(ctx, t, tableName, [][]any{
		{1, "green", "data"}, {8, "blue", "data"}, {3, "red", "data"},
		{0, "blue", "data"}, {1, "blue", "data"}, {0, "red", "data"},
		{7, "red", "data"}, {6, "green", "data"}, {2, "blue", "data"},
		{1, "red", "data"}, {2, "red", "data"}, {0, "green", "data"},
		{4, "green", "data"}, {2, "green", "data"}, {5, "blue", "data"},
	})

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.EndpointSpec.(*Config).Advanced.BackfillChunkSize = 1
	var summary, _ = tests.RestartingBackfillCapture(ctx, t, cs)
	cupaloy.SnapshotT(t, summary)
}

// TestSpecialTemporalValues exercises various 'special values' of the date/time
// column types to ensure that they all get captured as something reasonable.
func TestSpecialTemporalValues(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = "33220241"
	// In theory we ought to test interval values 'infinity' and '-infinity' too, but the PGX client library fails to parse those at all right now.
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, a_date DATE, a_time TIME, a_timestamp TIMESTAMP)")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = &st.OrderedCaptureValidator{}

	sqlcapture.TestShutdownAfterCaughtUp = true
	t.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = false })

	// Backfill
	tb.Insert(ctx, t, tableName, [][]interface{}{
		{0, "epoch", nil, "epoch"},
		{1, "infinity", nil, "infinity"},
		{2, "-infinity", nil, "-infinity"},
		{3, nil, "allballs", nil},
	})
	cs.Capture(ctx, t, nil)

	// Replication
	tb.Insert(ctx, t, tableName, [][]interface{}{
		{10, "epoch", nil, "epoch"},
		{11, "infinity", nil, "infinity"},
		{12, "-infinity", nil, "-infinity"},
		{13, nil, "allballs", nil},
	})
	cs.Capture(ctx, t, nil)
	cupaloy.SnapshotT(t, cs.Summary())
}

// TestLongYearTimestamps validates that timestamps with 5-digit years can be
// properly captured both during backfill and replication.
func TestLongYearTimestamps(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = "15366164"
	var tableDef = "(id INTEGER PRIMARY KEY, created_at TIMESTAMPTZ, description TEXT)"
	var tableName = tb.CreateTable(ctx, t, uniqueID, tableDef)

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = &st.OrderedCaptureValidator{}
	sqlcapture.TestShutdownAfterCaughtUp = true
	t.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = false })

	// Initial backfill with normal and 5-digit year timestamps
	tb.Insert(ctx, t, tableName, [][]any{
		{1, "2023-01-01 12:30:45+00", "Recent date"},
		{2, "12345-06-07 08:09:10+00", "Far future date"},
	})
	cs.Capture(ctx, t, nil)

	// Replication with more normal and 5-digit year timestamps
	tb.Insert(ctx, t, tableName, [][]any{
		{4, "2024-05-15 09:30:00+00", "Another recent date"},
		{5, "54321-10-11 12:13:14+00", "Even further future"},
	})
	cs.Capture(ctx, t, nil)
	cupaloy.SnapshotT(t, cs.Summary())
}
