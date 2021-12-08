package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/estuary/protocols/airbyte"
)

type datatypeTestcase struct {
	ColumnType  string
	OutputType  string
	ColumnValue string
	OutputValue string
}

var datatypeTestcases = []datatypeTestcase{
	// Basic Boolean/Numeric/Text Types
	{`boolean`, `{"type":["boolean","null"]}`, `false`, `false`},
	{`boolean`, `{"type":["boolean","null"]}`, `'yes'`, `true`},
	{`integer`, `{"type":["integer","null"]}`, `123`, `123`},
	{`integer`, `{"type":["integer","null"]}`, `null`, `null`},
	{`integer not null`, `{"type":"integer"}`, `123`, `123`},
	{`smallint`, `{"type":["integer","null"]}`, `123`, `123`},
	{`bigint`, `{"type":["integer","null"]}`, `123`, `123`},
	{`serial`, `{"type":"integer"}`, `123`, `123`},      // non-nullable
	{`smallserial`, `{"type":"integer"}`, `123`, `123`}, // non-nullable
	{`bigserial`, `{"type":"integer"}`, `123`, `123`},   // non-nullable
	{`real`, `{"type":["number","null"]}`, `123.456`, `123.456`},
	{`double precision`, `{"type":["number","null"]}`, `123.456`, `123.456`},

	// TODO(wgd): The 'decimal' and 'numeric' types are generally used because precision
	// and accuracy actually matter. I'm leery of just casting these to a float, so for
	// now I'm letting the `pgtype.Numeric.EncodeText()` implementation turn them into a
	// string. Revisit whether this is correct behavior at some point.
	// TODO(johnny): This will fail schema validation. They need to be output as JSON numbers (doubles).
	{`decimal`, `{"type":["number","null"]}`, `123.456`, `"123456e-3"`},
	{`numeric`, `{"type":["number","null"]}`, `123.456`, `"123456e-3"`},
	{`numeric(4,2)`, `{"type":["number","null"]}`, `12.34`, `"1234e-2"`},

	{`character varying(10)`, `{"type":["string","null"]}`, `'foo'`, `"foo"`},
	{`varchar(10)`, `{"type":["string","null"]}`, `'foo'`, `"foo"`},
	{`varchar`, `{"type":["string","null"]}`, `'foo'`, `"foo"`},
	{`character(10)`, `{"type":["string","null"]}`, `'foo'`, `"foo       "`},
	{`char(10)`, `{"type":["string","null"]}`, `'foo'`, `"foo       "`},
	{`char`, `{"type":["string","null"]}`, `'f'`, `"f"`},
	{`text`, `{"type":["string","null"]}`, `'foo'`, `"foo"`},
	{`bytea`, `{"type":["string","null"],"contentEncoding":"base64"}`, `'\xDEADBEEF'`, `"3q2+7w=="`},
	{`bit`, `{"type":["string","null"]}`, `B'1'`, `"1"`},
	{`bit(3)`, `{"type":["string","null"]}`, `B'101'`, `"101"`},
	{`bit varying`, `{"type":["string","null"]}`, `B'1101'`, `"1101"`},
	{`bit varying(5)`, `{"type":["string","null"]}`, `B'10111'`, `"10111"`},

	// Domain-Specific Data Types
	{`money`, `{"type":["string","null"]}`, `123.45`, `"$123.45"`},
	{`money`, `{"type":["string","null"]}`, `'$123.45'`, `"$123.45"`},
	{`date`, `{"type":["string","null"],"format":"date-time"}`, `'January 8, 1999'`, `"1999-01-08T00:00:00Z"`},
	{`timestamp`, `{"type":["string","null"],"format":"date-time"}`, `'January 8, 1999'`, `"1999-01-08T00:00:00Z"`},
	{`timestamp without time zone`, `{"type":["string","null"],"format":"date-time"}`, `'January 8, 1999'`, `"1999-01-08T00:00:00Z"`},
	// TODO(wgd): The 'timestamp with time zone' type produces inconsistent results between
	// table scanning and replication events. They're both valid timestamps, but they differ.
	//{`timestamp with time zone`, `{"type":"string","format":"date-time"}`, `'January 8, 1999'`, `"1999-01-07T16:00:00-08:00"`},
	{`time`, `{"type":["integer","null"]}`, `'04:05:06 PST'`, `14706000000`},
	{`time without time zone`, `{"type":["integer","null"]}`, `'04:05:06 PST'`, `14706000000`},
	{`time with time zone`, `{"type":["string","null"],"format":"time"}`, `'04:05:06 PST'`, `"04:05:06-08"`},
	{`interval`, `{"type":["string","null"]}`, `'2 months 1 day 5 minutes 6 seconds'`, `"2 mon 1 day 00:05:06.000000"`},

	{`point`, `{"type":["string","null"]}`, `'(1, 2)'`, `"(1,2)"`},
	{`line`, `{"type":["string","null"]}`, `'{1, 2, 3}'`, `"{1,2,3}"`},
	{`lseg`, `{"type":["string","null"]}`, `'[(1, 2), (3, 4)]'`, `"(1,2),(3,4)"`},
	{`box`, `{"type":["string","null"]}`, `'((1, 2), (3, 4))'`, `"(3,4),(1,2)"`},
	{`path`, `{"type":["string","null"]}`, `'[(1, 2), (3, 4), (5, 6)]'`, `"[(1,2),(3,4),(5,6)]"`},
	{`polygon`, `{"type":["string","null"]}`, `'((0, 0), (0, 1), (1, 0))'`, `"((0,0),(0,1),(1,0))"`},
	{`circle`, `{"type":["string","null"]}`, `'((1, 2), 3)'`, `"\u003c(1,2),3\u003e"`},

	{`inet`, `{"type":["string","null"]}`, `'192.168.100.0/24'`, `"192.168.100.0/24"`},
	{`inet`, `{"type":["string","null"]}`, `'2001:4f8:3:ba::/64'`, `"2001:4f8:3:ba::/64"`},
	{`cidr`, `{"type":["string","null"]}`, `'192.168.100.0/24'`, `"192.168.100.0/24"`},
	{`cidr`, `{"type":["string","null"]}`, `'2001:4f8:3:ba::/64'`, `"2001:4f8:3:ba::/64"`},
	{`macaddr`, `{"type":["string","null"]}`, `'08:00:2b:01:02:03'`, `"08:00:2b:01:02:03"`},
	{`macaddr8`, `{"type":["string","null"]}`, `'08-00-2b-01-02-03-04-05'`, `"08:00:2b:01:02:03:04:05"`},
	{`uuid`, `{"type":["string","null"],"format":"uuid"}`, `'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'`, `"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"`},

	{`tsvector`, `{"type":["string","null"]}`, `'a fat cat'`, `"'a' 'cat' 'fat'"`},
	{`tsquery`, `{"type":["string","null"]}`, `'fat & cat'`, `"'fat' \u0026 'cat'"`},

	// TODO(wgd): JSON values read by pgx/pgtype are currently unmarshalled (by the `pgtype.JSON.Get()` method)
	// into an `interface{}`, which means that the un-normalized JSON text coming from PostgreSQL is getting
	// normalized in various ways by the JSON -> interface{} -> JSON round-trip. For `jsonb` this is probably
	// fine since the value has already been decomposed into a binary format within PostgreSQL, but we might
	// possibly want to try and fix this for `json` at some point?
	{`json`, `{}`, `'{"type": "test", "data": 123}'`, `{"data":123,"type":"test"}`},
	{`jsonb`, `{}`, `'{"type": "test", "data": 123}'`, `{"data":123,"type":"test"}`},
	{`jsonpath`, `{"type":["string","null"]}`, `'$foo'`, `"$\"foo\""`},
	{`xml`, `{"type":["string","null"]}`, `'<foo>bar &gt; baz</foo>'`, `"\u003cfoo\u003ebar \u0026gt; baz\u003c/foo\u003e"`},

	// TODO(wgd): Should arrays be strings or should we decode them?
	{`integer[3][3]`, `{"type":["string","null"]}`, `'{{1,2,3},{4,5,6},{7,8,9}}'`, `"{{1,2,3},{4,5,6},{7,8,9}}"`},
	{`smallint[3][3]`, `{"type":["string","null"]}`, `'{{1,2,3},{4,5,6},{7,8,9}}'`, `"{{1,2,3},{4,5,6},{7,8,9}}"`},
	{`real[][]`, `{"type":["string","null"]}`, `'{{1,2,3},{4,5,6},{7,8,9}}'`, `"{{1,2,3},{4,5,6},{7,8,9}}"`},
	{`text[]`, `{"type":["string","null"]}`, `'{"foo", "bar", "baz"}'`, `"{foo,bar,baz}"`},

	// TODO(wgd): Add enumeration test case?
}

func TestDatatypes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	var cfg, ctx = TestDefaultConfig, context.Background()

	for idx, tc := range datatypeTestcases {
		t.Run(fmt.Sprintf("%d_%s", idx, sanitizeName(tc.ColumnType)), func(t *testing.T) {
			var table = createTestTable(ctx, t, "", fmt.Sprintf("(a INTEGER PRIMARY KEY, b %s)", tc.ColumnType))

			// Perform discovery and verify that the generated JSON schema looks correct
			t.Run("discovery", func(t *testing.T) {
				var discoveredCatalog, err = DiscoverCatalog(ctx, cfg)
				if err != nil {
					t.Fatal(err)
				}
				var stream *airbyte.Stream
				for idx := range discoveredCatalog.Streams {
					if strings.EqualFold(discoveredCatalog.Streams[idx].Name, table) {
						stream = &discoveredCatalog.Streams[idx]
						break
					}
				}
				if stream == nil {
					t.Errorf("column type %q: no stream named %q discovered", tc.ColumnType, table)
					return
				}
				var expectedSchema = fmt.Sprintf(`{"properties":{"a":{"type":"integer"},"b":%s},"required":["a"],"type":"object"}`, tc.OutputType)
				if string(stream.JSONSchema) != expectedSchema {
					t.Errorf("column type %q did not produce expected schema: %s", tc.ColumnType, expectedSchema)
					t.Errorf("column type %q resulted in schema: %s", tc.ColumnType, stream.JSONSchema)
					return
				}
			})

			// Insert a test row and scan it back out, then do the same via replication
			t.Run("roundtrip", func(t *testing.T) {
				var catalog, state = testCatalog(table), PersistentState{}

				t.Run("scan", func(t *testing.T) {
					dbQuery(ctx, t, fmt.Sprintf(`INSERT INTO %s VALUES (1, %s);`, table, tc.ColumnValue))
					var output, _ = performCapture(ctx, t, &cfg, &catalog, &state)
					verifyRoundTrip(t, output, tc.ColumnType, tc.ColumnValue, tc.OutputValue)
				})

				t.Run("replication", func(t *testing.T) {
					dbQuery(ctx, t, fmt.Sprintf(`INSERT INTO %s VALUES (2, %s);`, table, tc.ColumnValue))
					var output, _ = performCapture(ctx, t, &cfg, &catalog, &state)
					verifyRoundTrip(t, output, tc.ColumnType, tc.ColumnValue, tc.OutputValue)
				})
			})
		})
	}
}

type datatypeTestRecord struct {
	Stream string `json:"stream"`
	Data   struct {
		After struct {
			Value json.RawMessage `json:"b"`
		} `json:"after"`
	} `json:"data"`
}

func verifyRoundTrip(t *testing.T, output string, colType, colValue, expectedValue string) {
	t.Helper()

	// Extract the value record from the full output
	var record *datatypeTestRecord
	for _, line := range strings.Split(output, "\n") {
		if strings.HasPrefix(line, `{"type":"RECORD","record":`) && strings.HasSuffix(line, `}`) {
			line = strings.TrimPrefix(line, `{"type":"RECORD","record":`)
			line = strings.TrimSuffix(line, `}`)

			record = new(datatypeTestRecord)
			if err := json.Unmarshal([]byte(line), record); err != nil {
				t.Errorf("error unmarshalling result record: %v", err)
				return
			}
		}
	}
	if record == nil {
		t.Errorf("result record not found in output (input %q of type %q)", colValue, colType)
		return
	}

	if string(record.Data.After.Value) != expectedValue {
		t.Errorf("result mismatch for type %q: input %q, got %q, expected %q", colType, colValue,
			string(record.Data.After.Value), expectedValue)
	}
}

func sanitizeName(name string) string {
	var xs = []byte(name)
	for i := range xs {
		if '0' <= xs[i] && xs[i] <= '9' {
			continue
		}
		if 'a' <= xs[i] && xs[i] <= 'z' {
			continue
		}
		if 'A' <= xs[i] && xs[i] <= 'Z' {
			continue
		}
		xs[i] = '_'
	}
	return strings.TrimRight(string(xs), "_")
}
