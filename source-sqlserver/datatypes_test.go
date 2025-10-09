package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/connectors/sqlcapture"
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

		{ColumnType: `char(6)`, ExpectType: `{"type":["string","null"],"minLength":6,"maxLength":6}`, InputValue: `asdf`, ExpectValue: `"asdf  "`},
		{ColumnType: `varchar(6)`, ExpectType: `{"type":["string","null"],"maxLength":6}`, InputValue: `asdf`, ExpectValue: `"asdf"`},
		{ColumnType: `text`, ExpectType: `{"type":["string","null"]}`, InputValue: `asdf`, ExpectValue: `"asdf"`},
		{ColumnType: `nchar(6)`, ExpectType: `{"type":["string","null"],"minLength":6,"maxLength":6}`, InputValue: `asdf`, ExpectValue: `"asdf  "`},
		{ColumnType: `nvarchar(6)`, ExpectType: `{"type":["string","null"],"maxLength":6}`, InputValue: `asdf`, ExpectValue: `"asdf"`},
		{ColumnType: `ntext`, ExpectType: `{"type":["string","null"]}`, InputValue: `asdf`, ExpectValue: `"asdf"`},

		{ColumnType: `binary(8)`, ExpectType: `{"type":["string","null"],"contentEncoding":"base64","minLength":12,"maxLength":12}`, InputValue: []byte{0x12, 0x34, 0x56, 0x78}, ExpectValue: `"EjRWeAAAAAA="`},
		{ColumnType: `varbinary(8)`, ExpectType: `{"type":["string","null"],"contentEncoding":"base64","maxLength":12}`, InputValue: []byte{0x12, 0x34, 0x56, 0x78}, ExpectValue: `"EjRWeA=="`},
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
		{"Numeric", "NUMERIC(10,5)", []any{"-1234", "0", "-3", "2", "12345.12", "12345.121", "12345.1205", "12345.12059", "12346"}},
		{"Decimal", "DECIMAL(10,5)", []any{"-1234", "0", "-3", "2", "12345.12", "12345.121", "12345.1205", "12345.12059", "12346"}},
		{"DateTime", "DATETIME", []any{"1991-08-31T12:34:54.111", "1991-08-31T12:34:54.333", "2000-01-01T01:01:01"}},
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

// TestOversizedFields tests the behavior of the connector when it encounters
// individual column values larger than the connector maximum.
//
// SQL Server has some special cases for certain "Large Object" data types.
// For instance 'max text repl size' controls the maximum size of values in
// image, text, ntext, varchar(max), nvarchar(max), varbinary(max), and xml
// columns of tables which are enabled for replication.
//
// So to a first approximation, we should expect that those column types are
// the ones we need to worry about truncating because they're the ones which
// can grow arbitrarily large.
func TestOversizedFields(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	var tb, ctx = sqlserverTestBackend(t), context.Background()
	var uniqueID = "81647037"
	var tableDef = "(id INTEGER PRIMARY KEY, v_text TEXT, v_varchar VARCHAR(max), v_image IMAGE, v_binary VARBINARY(max), v_xml XML)"
	var tableName = tb.CreateTable(ctx, t, uniqueID, tableDef)

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = new(st.ChecksumValidator)
	sqlcapture.TestShutdownAfterCaughtUp = true
	t.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = false })

	var largeText = strings.Repeat("data", 4194304)            // 16MiB string
	var largeBinary = []byte(largeText)                        // 16MiB binary
	var largeXML = fmt.Sprintf(`<hello>%s</hello>`, largeText) // ~16MiB XML object

	// Backfill
	tb.Insert(ctx, t, tableName, [][]any{
		{100, largeText, largeText, largeBinary, largeBinary, largeXML},
	})
	cs.Capture(ctx, t, nil)

	// Replication
	tb.Insert(ctx, t, tableName, [][]any{
		{200, largeText, largeText, largeBinary, largeBinary, largeXML},
	})
	cs.Capture(ctx, t, nil)

	cupaloy.SnapshotT(t, cs.Summary())
}

// TestBitNotNullDeletion exercises deletions from a table with BIT NOT NULL columns, because
// we have seen multiple cases where production captures have observed "impossible" null values
// in deletion events containing BIT NOT NULLs. This test at least demonstrates that it isn't
// normal or expected for that to happen.
func TestBitNotNullDeletion(t *testing.T) {
	var tb, ctx = sqlserverTestBackend(t), context.Background()
	var uniqueID = "32831599"
	var tableDef = "(id INTEGER PRIMARY KEY, v_a BIT, v_b BIT NOT NULL, v_c BIT NOT NULL DEFAULT 0, v_d BIT NOT NULL DEFAULT 1)"
	var tableName = tb.CreateTable(ctx, t, uniqueID, tableDef)

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = new(st.OrderedCaptureValidator)
	sqlcapture.TestShutdownAfterCaughtUp = true
	t.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = false })

	// Backfill
	tb.Insert(ctx, t, tableName, [][]any{
		{100, true, true, true, true},
		{101, false, false, false, false},
		{102, nil, true, true, true},
		{103, nil, false, false, false},
	})
	cs.Capture(ctx, t, nil)

	// Replication
	tb.Delete(ctx, t, tableName, "id", 100)
	tb.Delete(ctx, t, tableName, "id", 101)
	tb.Delete(ctx, t, tableName, "id", 102)
	tb.Delete(ctx, t, tableName, "id", 103)
	tb.Insert(ctx, t, tableName, [][]any{
		{200, true, true, true, true},
		{201, false, false, false, false},
		{202, nil, true, true, true},
		{203, nil, false, false, false},
	})
	tb.Delete(ctx, t, tableName, "id", 200)
	tb.Delete(ctx, t, tableName, "id", 201)
	tb.Delete(ctx, t, tableName, "id", 202)
	tb.Delete(ctx, t, tableName, "id", 203)
	cs.Capture(ctx, t, nil)

	cupaloy.SnapshotT(t, cs.Summary())
}

// TestRowversionTypes exercises the TIMESTAMP / ROWVERSION column type using both names.
// We test this separately from the main datatypes test because these IDs are usually
// tracked implicitly by the database and aren't specified by the client.
func TestRowversionTypes(t *testing.T) {
	var tb, ctx = sqlserverTestBackend(t), context.Background()
	for _, columnType := range []string{"timestamp", "rowversion"} {
		t.Run(columnType, func(t *testing.T) {
			var uniqueID = uniqueTableID(t)
			var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data VARCHAR(32), rv "+columnType+")")

			var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
			cs.EndpointSpec.(*Config).Advanced.FeatureFlags = "discover_rowversion_as_bytes"
			setShutdownAfterCaughtUp(t, true)

			t.Run("discovery", func(t *testing.T) {
				cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
			})

			t.Run("capture", func(t *testing.T) {
				cs.Sanitizers[`"rv":"<REDACTED>"`] = regexp.MustCompile(`"rv":"[a-zA-Z0-9+/=]+"`)
				tb.Query(ctx, t, fmt.Sprintf("INSERT INTO %s (id, data) VALUES (1, 'first'), (2, 'second')", tableName))
				cs.Capture(ctx, t, nil)
				tb.Query(ctx, t, fmt.Sprintf("INSERT INTO %s (id, data) VALUES (3, 'third'), (4, 'fourth')", tableName))
				tb.Query(ctx, t, fmt.Sprintf("UPDATE %s SET data = 'updated' WHERE id = 2", tableName))
				cs.Capture(ctx, t, nil)
				cupaloy.SnapshotT(t, cs.Summary())
			})
		})
	}
}
