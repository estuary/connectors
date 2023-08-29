package main

import (
	"context"
	"fmt"
	"regexp"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/sqlcapture/tests"
)

const arraySchemaPatternNotNullable = `{"required":["dimensions","elements"],"type":"object","properties":{"dimensions":{"type":"array","items":{"type":"integer"}},"elements":{"type":"array","items":%s}}}`
const arraySchemaPattern = `{"required":["dimensions","elements"],"type":["object","null"],"properties":{"dimensions":{"type":"array","items":{"type":"integer"}},"elements":{"type":"array","items":%s}}}`

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
		{ColumnType: `real`, ExpectType: `{"type":["number","null"]}`, InputValue: `123.456`, ExpectValue: `123.456`},
		{ColumnType: `double precision`, ExpectType: `{"type":["number","null"]}`, InputValue: `123.456`, ExpectValue: `123.456`},

		// TODO(wgd): The 'decimal' and 'numeric' types are generally used because precision
		// and accuracy actually matter. I'm leery of just casting these to a float, so for
		// now I'm letting the `pgtype.Numeric.EncodeText()` implementation turn them into a
		// string. Revisit whether this is correct behavior at some point.
		// TODO(johnny): This will fail schema validation. They need to be output as JSON numbers (doubles).
		{ColumnType: `decimal`, ExpectType: `{"type":["string","null"],"format":"number"}`, InputValue: `123.456`, ExpectValue: `"123456e-3"`},
		{ColumnType: `numeric`, ExpectType: `{"type":["string","null"],"format":"number"}`, InputValue: `123.456`, ExpectValue: `"123456e-3"`},
		{ColumnType: `numeric(4,2)`, ExpectType: `{"type":["string","null"],"format":"number"}`, InputValue: `12.34`, ExpectValue: `"1234e-2"`},
		{ColumnType: `character varying(10)`, ExpectType: `{"type":["string","null"]}`, InputValue: `foo`, ExpectValue: `"foo"`},
		{ColumnType: `varchar(10)`, ExpectType: `{"type":["string","null"]}`, InputValue: `foo`, ExpectValue: `"foo"`},
		{ColumnType: `varchar`, ExpectType: `{"type":["string","null"]}`, InputValue: `foo`, ExpectValue: `"foo"`},
		{ColumnType: `character(10)`, ExpectType: `{"type":["string","null"]}`, InputValue: `foo`, ExpectValue: `"foo       "`},
		{ColumnType: `char(10)`, ExpectType: `{"type":["string","null"]}`, InputValue: `foo`, ExpectValue: `"foo       "`},
		{ColumnType: `char`, ExpectType: `{"type":["string","null"]}`, InputValue: `f`, ExpectValue: `"f"`},
		{ColumnType: `text`, ExpectType: `{"type":["string","null"]}`, InputValue: `foo`, ExpectValue: `"foo"`},
		{ColumnType: `bytea`, ExpectType: `{"type":["string","null"],"contentEncoding":"base64"}`, InputValue: `\xDEADBEEF`, ExpectValue: `"3q2+7w=="`},
		{ColumnType: `bit`, ExpectType: `{"type":["string","null"]}`, InputValue: `1`, ExpectValue: `"1"`},
		{ColumnType: `bit(3)`, ExpectType: `{"type":["string","null"]}`, InputValue: `101`, ExpectValue: `"101"`},
		{ColumnType: `bit varying`, ExpectType: `{"type":["string","null"]}`, InputValue: `1101`, ExpectValue: `"1101"`},
		{ColumnType: `bit varying(5)`, ExpectType: `{"type":["string","null"]}`, InputValue: `10111`, ExpectValue: `"10111"`},

		// Domain-Specific Data Types
		{ColumnType: `money`, ExpectType: `{"type":["string","null"]}`, InputValue: 123.45, ExpectValue: `"$123.45"`},
		{ColumnType: `money`, ExpectType: `{"type":["string","null"]}`, InputValue: `$123.45`, ExpectValue: `"$123.45"`},
		{ColumnType: `date`, ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: `'January 8, 1999'`, ExpectValue: `"1999-01-08T00:00:00Z"`},
		{ColumnType: `timestamp`, ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: `'January 8, 1999'`, ExpectValue: `"1999-01-08T00:00:00Z"`},
		{ColumnType: `timestamp without time zone`, ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: `'January 8, 1999'`, ExpectValue: `"1999-01-08T00:00:00Z"`},
		{ColumnType: `timestamp`, ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: `'infinity'`, ExpectValue: `"9999-12-31T23:59:59Z"`},
		{ColumnType: `timestamp`, ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: `'-infinity'`, ExpectValue: `"0000-01-01T00:00:00Z"`},
		{ColumnType: `timestamp`, ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: `'epoch'`, ExpectValue: `"1970-01-01T00:00:00Z"`},
		{ColumnType: `timestamp`, ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: `'20222-08-31T00:00:00Z'`, ExpectValue: `"0000-01-01T00:00:00Z"`},
		{ColumnType: `timestamp`, ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: `'January 8, 99 BC'`, ExpectValue: `"0000-01-01T00:00:00Z"`},

		// TODO(wgd): The 'timestamp with time zone' type produces inconsistent results between
		// table scanning and replication events. They're both valid timestamps, but they differ.
		// {ColumnType: `timestamp with time zone`, ExpectType: `{"type":["string","null"],"format":"date-time"}`, InputValue: `'January 8, 1999'`, ExpectValue: `"1999-01-07T16:00:00-08:00"`},
		{ColumnType: `time`, ExpectType: `{"type":["integer","null"]}`, InputValue: `'04:05:06 PST'`, ExpectValue: `14706000000`},
		{ColumnType: `time without time zone`, ExpectType: `{"type":["integer","null"]}`, InputValue: `'04:05:06 PST'`, ExpectValue: `14706000000`},
		{ColumnType: `time with time zone`, ExpectType: `{"type":["string","null"],"format":"time"}`, InputValue: `'04:05:06 PST'`, ExpectValue: `"04:05:06-08:00"`},
		{ColumnType: `time with time zone`, ExpectType: `{"type":["string","null"],"format":"time"}`, InputValue: `'04:05:06 UTC'`, ExpectValue: `"04:05:06Z"`},
		{ColumnType: `time with time zone`, ExpectType: `{"type":["string","null"],"format":"time"}`, InputValue: `'04:05:06.123 UTC'`, ExpectValue: `"04:05:06.123Z"`},
		{ColumnType: `time with time zone`, ExpectType: `{"type":["string","null"],"format":"time"}`, InputValue: `'04:05:06.123+0330'`, ExpectValue: `"04:05:06.123+03:30"`},
		{ColumnType: `time with time zone`, ExpectType: `{"type":["string","null"],"format":"time"}`, InputValue: `'04:05:06.123+03:30:50'`, ExpectValue: `"04:05:06.123+03:30:50"`},
		{ColumnType: `interval`, ExpectType: `{"type":["string","null"]}`, InputValue: `'2 months 1 day 5 minutes 6 seconds'`, ExpectValue: `"2 mon 1 day 00:05:06.000000"`},
		{ColumnType: `point`, ExpectType: `{"type":["string","null"]}`, InputValue: `(1, 2)`, ExpectValue: `"(1,2)"`},
		{ColumnType: `line`, ExpectType: `{"type":["string","null"]}`, InputValue: `{1, 2, 3}`, ExpectValue: `"{1,2,3}"`},
		{ColumnType: `lseg`, ExpectType: `{"type":["string","null"]}`, InputValue: `[(1, 2), (3, 4)]`, ExpectValue: `"(1,2),(3,4)"`},
		{ColumnType: `box`, ExpectType: `{"type":["string","null"]}`, InputValue: `((1, 2), (3, 4))`, ExpectValue: `"(3,4),(1,2)"`},
		{ColumnType: `path`, ExpectType: `{"type":["string","null"]}`, InputValue: `[(1, 2), (3, 4), (5, 6)]`, ExpectValue: `"[(1,2),(3,4),(5,6)]"`},
		{ColumnType: `polygon`, ExpectType: `{"type":["string","null"]}`, InputValue: `((0, 0), (0, 1), (1, 0))`, ExpectValue: `"((0,0),(0,1),(1,0))"`},
		{ColumnType: `circle`, ExpectType: `{"type":["string","null"]}`, InputValue: `((1, 2), 3)`, ExpectValue: `"\u003c(1,2),3\u003e"`},
		{ColumnType: `inet`, ExpectType: `{"type":["string","null"]}`, InputValue: `192.168.100.0/24`, ExpectValue: `"192.168.100.0/24"`},
		{ColumnType: `inet`, ExpectType: `{"type":["string","null"]}`, InputValue: `2001:4f8:3:ba::/64`, ExpectValue: `"2001:4f8:3:ba::/64"`},
		{ColumnType: `cidr`, ExpectType: `{"type":["string","null"]}`, InputValue: `192.168.100.0/24`, ExpectValue: `"192.168.100.0/24"`},
		{ColumnType: `cidr`, ExpectType: `{"type":["string","null"]}`, InputValue: `2001:4f8:3:ba::/64`, ExpectValue: `"2001:4f8:3:ba::/64"`},
		{ColumnType: `macaddr`, ExpectType: `{"type":["string","null"]}`, InputValue: `08:00:2b:01:02:03`, ExpectValue: `"08:00:2b:01:02:03"`},
		{ColumnType: `macaddr8`, ExpectType: `{"type":["string","null"]}`, InputValue: `08-00-2b-01-02-03-04-05`, ExpectValue: `"08:00:2b:01:02:03:04:05"`},
		{ColumnType: `uuid`, ExpectType: `{"type":["string","null"],"format":"uuid"}`, InputValue: `a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11`, ExpectValue: `"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"`},
		{ColumnType: `tsvector`, ExpectType: `{"type":["string","null"]}`, InputValue: `a fat cat`, ExpectValue: `"'a' 'cat' 'fat'"`},
		{ColumnType: `tsquery`, ExpectType: `{"type":["string","null"]}`, InputValue: `fat & cat`, ExpectValue: `"'fat' \u0026 'cat'"`},

		// TODO(wgd): JSON values read by pgx/pgtype are currently unmarshalled (by the `pgtype.JSON.Get()` method)
		// into an `interface{}`, which means that the un-normalized JSON text coming from PostgreSQL is getting
		// normalized in various ways by the JSON -> interface{} -> JSON round-trip. For `jsonb` this is probably
		// fine since the value has already been decomposed into a binary format within PostgreSQL, but we might
		// possibly want to try and fix this for `json` at some point?
		{ColumnType: `json`, ExpectType: `{}`, InputValue: `{"type": "test", "data": 123}`, ExpectValue: `{"data":123,"type":"test"}`},
		{ColumnType: `jsonb`, ExpectType: `{}`, InputValue: `{"type": "test", "data": 123}`, ExpectValue: `{"data":123,"type":"test"}`},
		{ColumnType: `jsonpath`, ExpectType: `{"type":["string","null"]}`, InputValue: `$foo`, ExpectValue: `"$\"foo\""`},
		{ColumnType: `xml`, ExpectType: `{"type":["string","null"]}`, InputValue: `<foo>bar &gt; baz</foo>`, ExpectValue: `"\u003cfoo\u003ebar \u0026gt; baz\u003c/foo\u003e"`},

		// PostgreSQL doesn't actually check array sizes or dimensionality. Per the documentation:
		//    [...] declaring the array size or number of dimensions in CREATE TABLE is
		//    simply documentation; it does not affect run-time behavior.
		// This set of test cases exercises that expectation.
		{ColumnType: `integer[3][3]`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["integer","null"]}`), InputValue: `{{{{{{1}}}}}}`, ExpectValue: `{"dimensions":[1,1,1,1,1,1],"elements":[1]}`},
		{ColumnType: `integer[3][3]`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["integer","null"]}`), InputValue: `{1,2,3,4,5,6}`, ExpectValue: `{"dimensions":[6],"elements":[1,2,3,4,5,6]}`},
		{ColumnType: `integer[3][3]`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["integer","null"]}`), InputValue: `{{1,2,3},{4,5,6},{7,8,9}}`, ExpectValue: `{"dimensions":[3,3],"elements":[1,2,3,4,5,6,7,8,9]}`},

		// Sanity-check various types of array for discovery and round-tripping
		{ColumnType: `bigint ARRAY NOT NULL`, ExpectType: fmt.Sprintf(arraySchemaPatternNotNullable, `{"type":["integer","null"]}`), InputValue: `{1,2,null,4}`, ExpectValue: `{"dimensions":[4],"elements":[1,2,null,4]}`},
		{ColumnType: `bigint ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["integer","null"]}`), InputValue: `{1,2,null,4}`, ExpectValue: `{"dimensions":[4],"elements":[1,2,null,4]}`},
		{ColumnType: `bigint ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["integer","null"]}`), InputValue: nil, ExpectValue: `null`},
		{ColumnType: `boolean ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["boolean","null"]}`), InputValue: `{true, false, null, true}`, ExpectValue: `{"dimensions":[4],"elements":[true,false,null,true]}`},
		{ColumnType: `bytea ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["string","null"],"contentEncoding":"base64"}`), InputValue: `{abcd, efgh}`, ExpectValue: `{"dimensions":[2],"elements":["YWJjZA==","ZWZnaA=="]}`},
		{ColumnType: `varchar(12) ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["string","null"]}`), InputValue: `{foo, bar}`, ExpectValue: `{"dimensions":[2],"elements":["foo","bar"]}`},
		{ColumnType: `char(5) ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["string","null"]}`), InputValue: `{foo, bar}`, ExpectValue: `{"dimensions":[2],"elements":["foo  ","bar  "]}`},
		{ColumnType: `cidr ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["string","null"]}`), InputValue: `{192.168.100.0/24, 2001:4f8:3:ba::/64}`, ExpectValue: `{"dimensions":[2],"elements":["192.168.100.0/24","2001:4f8:3:ba::/64"]}`},
		{ColumnType: `date ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["string","null"],"format":"date-time"}`), InputValue: []interface{}{`2022-01-09`, `2022-01-10`}, ExpectValue: `{"dimensions":[2],"elements":["2022-01-09","2022-01-10"]}`},
		{ColumnType: `double precision ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["number","null"]}`), InputValue: []interface{}{1.23, 4.56}, ExpectValue: `{"dimensions":[2],"elements":[1.23,4.56]}`},
		{ColumnType: `inet ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["string","null"]}`), InputValue: []interface{}{`192.168.100.0/24`, `2001:4f8:3:ba::/64`}, ExpectValue: `{"dimensions":[2],"elements":["192.168.100.0/24","2001:4f8:3:ba::/64"]}`},
		{ColumnType: `integer ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["integer","null"]}`), InputValue: []interface{}{1, 2, nil, 4}, ExpectValue: `{"dimensions":[4],"elements":[1,2,null,4]}`},
		{ColumnType: `numeric ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["string","null"],"format":"number"}`), InputValue: []interface{}{`123.456`, `-789.0123`}, ExpectValue: `{"dimensions":[2],"elements":["123456e-3","-7890123e-4"]}`},
		{ColumnType: `real ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["number","null"]}`), InputValue: []interface{}{123.456, 789.012}, ExpectValue: `{"dimensions":[2],"elements":[123.456,789.012]}`},
		{ColumnType: `smallint ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["integer","null"]}`), InputValue: []interface{}{123, 456, 789}, ExpectValue: `{"dimensions":[3],"elements":[123,456,789]}`},
		{ColumnType: `text ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["string","null"]}`), InputValue: []interface{}{`Hello, world!`, `asdf`}, ExpectValue: `{"dimensions":[2],"elements":["Hello, world!","asdf"]}`},
		{ColumnType: `uuid ARRAY`, ExpectType: fmt.Sprintf(arraySchemaPattern, `{"type":["string","null"],"format":"uuid"}`), InputValue: []interface{}{`a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11`}, ExpectValue: `{"dimensions":[1],"elements":["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"]}`},

		// Built-in range types.
		{ColumnType: `int4range`, ExpectType: `{"type":["string","null"]}`, InputValue: `[1,5)`, ExpectValue: `"[1,5)"`},
		{ColumnType: `int8range`, ExpectType: `{"type":["string","null"]}`, InputValue: `[,]`, ExpectValue: `"(,)"`},
		{ColumnType: `numrange`, ExpectType: `{"type":["string","null"]}`, InputValue: `(1.1,5.5]`, ExpectValue: `"(11e-1,55e-1]"`},
		{ColumnType: `tsrange`, ExpectType: `{"type":["string","null"]}`, InputValue: `[2010-01-01 11:30,2010-01-01 15:00)`, ExpectValue: `"[2010-01-01 11:30:00,2010-01-01 15:00:00)"`},
		{ColumnType: `tstzrange`, ExpectType: `{"type":["string","null"]}`, InputValue: `[2010-01-01 11:30,2010-01-01 15:00)`, ExpectValue: `"[2010-01-01 11:30:00Z,2010-01-01 15:00:00Z)"`},
		{ColumnType: `daterange`, ExpectType: `{"type":["string","null"]}`, InputValue: `(2010-01-01,2010-01-02]`, ExpectValue: `"[2010-01-02,2010-01-03)"`},
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
		{"Bool", "BOOLEAN", []interface{}{"true", "false"}},
		{"Integer", "INTEGER", []interface{}{0, -3, 2, 1723}},
		{"SmallInt", "SMALLINT", []interface{}{0, -3, 2, 1723}},
		{"BigInt", "BIGINT", []interface{}{0, -3, 2, 1723}},
		{"Serial", "SERIAL", []interface{}{0, -3, 2, 1723}},
		{"SmallSerial", "SMALLSERIAL", []interface{}{0, -3, 2, 1723}},
		{"BigSerial", "BIGSERIAL", []interface{}{0, -3, 2, 1723}},
		{"Real", "REAL", []interface{}{-0.9, -1.0, -1.1, 0.0, 0.9, 1.0, 1.111, 1.222, 1.333, 1.444, 1.555, 1.666, 1.777, 1.888, 1.999, 2.000}},
		{"Double", "DOUBLE PRECISION", []interface{}{-0.9, -1.0, -1.1, 0.0, 0.9, 1.0, 1.111, 1.222, 1.333, 1.444, 1.555, 1.666, 1.777, 1.888, 1.999, 2.000}},
		{"Decimal", "DECIMAL", []interface{}{-0.9, -1.0, -1.1, 0.0, 0.9, 1.0, 1.111, 1.222, 1.333, 1.444, 1.555, 1.666, 1.777, 1.888, 1.999, 2.000}},
		{"Numeric", "NUMERIC(4,3)", []interface{}{-0.9, -1.0, -1.1, 0.0, 0.9, 1.0, 1.111, 1.222, 1.333, 1.444, 1.555, 1.666, 1.777, 1.888, 1.999, 2.000}},
		{"VarChar", "VARCHAR(10)", []interface{}{"", "   ", "a", "b", "c", "A", "B", "C", "_a", "_b", "_c"}},
		{"Char", "CHAR(3)", []interface{}{"   ", "a", "b", "c", "A", "B", "C", "_a", "_b", "_c"}},
		{"Text", "TEXT", []interface{}{"", "   ", "a", "b", "c", "A", "B", "C", "_a", "_b", "_c"}},
		{"UUID", "UUID", []interface{}{"66b968a7-aeca-4401-8239-5d57958d1572", "4ab4044a-9aab-415c-96c6-17fa338060fa", "c32fb585-fc7f-4347-8fe2-97448f4e93cd"}},
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
