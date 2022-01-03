package main

import (
	"context"
	"testing"

	"github.com/estuary/connectors/sqlcapture/tests"
)

// TestDatatypes runs the generic datatype discovery and round-tripping test on various datatypes.
func TestDatatypes(t *testing.T) {
	var ctx = context.Background()
	tests.TestDatatypes(ctx, t, TestBackend, []tests.DatatypeTestCase{
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
		{ColumnType: `decimal`, ExpectType: `{"type":["number","null"]}`, InputValue: `123.456`, ExpectValue: `"123456e-3"`},
		{ColumnType: `numeric`, ExpectType: `{"type":["number","null"]}`, InputValue: `123.456`, ExpectValue: `"123456e-3"`},
		{ColumnType: `numeric(4,2)`, ExpectType: `{"type":["number","null"]}`, InputValue: `12.34`, ExpectValue: `"1234e-2"`},

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
		// TODO(wgd): The 'timestamp with time zone' type produces inconsistent results between
		// table scanning and replication events. They're both valid timestamps, but they differ.
		// {ColumnType: `timestamp with time zone`, ExpectType: `{"type":"string","format":"date-time"}`, InputValue: `'January 8, 1999'`, ExpectValue: `"1999-01-07T16:00:00-08:00"`},
		{ColumnType: `time`, ExpectType: `{"type":["integer","null"]}`, InputValue: `'04:05:06 PST'`, ExpectValue: `14706000000`},
		{ColumnType: `time without time zone`, ExpectType: `{"type":["integer","null"]}`, InputValue: `'04:05:06 PST'`, ExpectValue: `14706000000`},
		{ColumnType: `time with time zone`, ExpectType: `{"type":["string","null"],"format":"time"}`, InputValue: `'04:05:06 PST'`, ExpectValue: `"04:05:06-08"`},
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

		// TODO(wgd): Should arrays be strings or should we decode them?
		{ColumnType: `integer[3][3]`, ExpectType: `{"type":["string","null"]}`, InputValue: `{{1,2,3},{4,5,6},{7,8,9}}`, ExpectValue: `"{{1,2,3},{4,5,6},{7,8,9}}"`},
		{ColumnType: `smallint[3][3]`, ExpectType: `{"type":["string","null"]}`, InputValue: `{{1,2,3},{4,5,6},{7,8,9}}`, ExpectValue: `"{{1,2,3},{4,5,6},{7,8,9}}"`},
		{ColumnType: `real[][]`, ExpectType: `{"type":["string","null"]}`, InputValue: `{{1,2,3},{4,5,6},{7,8,9}}`, ExpectValue: `"{{1,2,3},{4,5,6},{7,8,9}}"`},
		{ColumnType: `text[]`, ExpectType: `{"type":["string","null"]}`, InputValue: `{"foo", "bar", "baz"}`, ExpectValue: `"{foo,bar,baz}"`},

		// TODO(wgd): Add enumeration test case?
	})
}
