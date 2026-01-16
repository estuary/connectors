package main

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/go/capture/blackbox"
	"github.com/stretchr/testify/require"
)

// TestIntegerTypes exercises integer column types.
func TestIntegerTypes(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(
		id INTEGER PRIMARY KEY,
		a_integer INTEGER,
		a_bigint BIGINT,
		a_int INT,
		a_smallint SMALLINT,
		a_tinyint TINYINT
	)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, 123, 1234567891234567, 2147483647, 32767, 255),
		(2, -123, -1234567891234567, -2147483648, -32768, 0),
		(3, NULL, NULL, NULL, NULL, NULL)`)
	tc.Discover("Discover")
	tc.Run("Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(101, 456, 9876543219876543, 1000000, 1000, 128),
		(102, NULL, NULL, NULL, NULL, NULL)`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestDecimalTypes exercises decimal and money column types.
func TestDecimalTypes(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(
		id INTEGER PRIMARY KEY,
		a_numeric NUMERIC(10,5),
		a_decimal DECIMAL(5,2),
		a_money MONEY,
		a_smallmoney SMALLMONEY
	)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, 12345.12, 123, $1234567891234.29, $3148.29),
		(2, -12345.12, -123, -$1234567891234.29, -$3148.29),
		(3, 0, 0, $0, $0),
		(4, NULL, NULL, NULL, NULL)`)
	tc.Discover("Discover")
	tc.Run("Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(101, 99999.99999, 999.99, $922337203685477.5807, $214748.3647),
		(102, NULL, NULL, NULL, NULL)`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestFloatingPointTypes exercises float and real column types.
func TestFloatingPointTypes(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(
		id INTEGER PRIMARY KEY,
		a_float FLOAT,
		a_real REAL
	)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, 1234.5, 1234.5),
		(2, -1234.5, -1234.5),
		(3, 0, 0),
		(4, 1.23456789012345e10, 1.234567e10),
		(5, NULL, NULL)`)
	tc.Discover("Discover")
	tc.Run("Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(101, 9.87654321e-10, 9.876e-10),
		(102, NULL, NULL)`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestBooleanTypes exercises the bit column type.
func TestBooleanTypes(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(
		id INTEGER PRIMARY KEY,
		a_bit BIT
	)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, 1),
		(2, 0),
		(3, NULL)`)
	tc.Discover("Discover")
	tc.Run("Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(101, 1),
		(102, 0),
		(103, NULL)`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestStringTypes exercises character and text column types.
func TestStringTypes(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(
		id INTEGER PRIMARY KEY,
		a_char CHAR(6),
		a_varchar VARCHAR(6),
		a_text TEXT,
		a_nchar NCHAR(6),
		a_nvarchar NVARCHAR(6),
		a_ntext NTEXT
	)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, 'asdf', 'asdf', 'asdf', N'asdf', N'asdf', N'asdf'),
		(2, 'hello', 'hello', 'longer text value here', N'hello', N'hello', N'longer text value here'),
		(3, '', '', '', N'', N'', N''),
		(4, NULL, NULL, NULL, NULL, NULL, NULL)`)
	tc.Discover("Discover")
	tc.Run("Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(101, 'xyz', 'xyz', 'replication text', N'xyz', N'xyz', N'replication text'),
		(102, NULL, NULL, NULL, NULL, NULL, NULL)`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestBinaryTypes exercises binary column types.
func TestBinaryTypes(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(
		id INTEGER PRIMARY KEY,
		a_binary BINARY(8),
		a_varbinary VARBINARY(8),
		a_image IMAGE
	)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, 0x1234567800000000, 0x12345678, 0x12345678),
		(2, 0xDEADBEEFCAFEBABE, 0xDEADBEEF, 0xDEADBEEFCAFEBABE),
		(3, NULL, NULL, NULL)`)
	tc.Discover("Discover")
	tc.Run("Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(101, 0xABCDEF0123456789, 0xABCDEF01, 0xABCDEF0123456789),
		(102, NULL, NULL, NULL)`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestTemporalTypes exercises date and time column types.
func TestTemporalTypes(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	// Set timezone to America/Chicago to match the old test behavior
	require.NoError(t, tc.Capture.EditConfig("timezone", "America/Chicago"))
	db.CreateTable(t, `<NAME>`, `(
		id INTEGER PRIMARY KEY,
		a_date DATE,
		a_time TIME,
		a_datetimeoffset DATETIMEOFFSET,
		a_datetime DATETIME,
		a_datetime2 DATETIME2,
		a_smalldatetime SMALLDATETIME
	)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, '1991-08-31', '12:34:54.125', '1991-08-31T12:34:54.125-06:00', '1991-08-31T12:34:56.789', '1991-08-31T12:34:56.789', '1991-08-31T12:34:56.789'),
		(2, '2024-01-15', '00:00:00', '2024-01-15T00:00:00+00:00', '2024-01-15T00:00:00', '2024-01-15T00:00:00', '2024-01-15T00:00:00'),
		(3, '1900-01-01', '23:59:59.999', '1900-01-01T23:59:59.999+05:30', '1900-01-01T23:59:59', '1900-01-01T23:59:59.9999999', '1900-01-01T23:59:00'),
		(4, NULL, NULL, NULL, NULL, NULL, NULL)`)
	tc.Discover("Discover")
	tc.Run("Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(101, '2000-06-15', '06:30:00', '2000-06-15T06:30:00-07:00', '2000-06-15T06:30:00', '2000-06-15T06:30:00', '2000-06-15T06:30:00'),
		(102, NULL, NULL, NULL, NULL, NULL, NULL)`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestMiscTypes exercises uniqueidentifier, xml, and hierarchyid column types.
func TestMiscTypes(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(
		id INTEGER PRIMARY KEY,
		a_uniqueidentifier UNIQUEIDENTIFIER,
		a_xml XML,
		a_hierarchyid HIERARCHYID
	)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, '8292f3cb-0cce-41e8-86aa-ae09bcc988e9', '<hello>world</hello>', '/1/2/3/'),
		(2, '00000000-0000-0000-0000-000000000000', '<root><child attr="value">text</child></root>', '/'),
		(3, 'ffffffff-ffff-ffff-ffff-ffffffffffff', '<empty/>', '/1/'),
		(4, NULL, NULL, NULL)`)
	tc.Discover("Discover")
	tc.Run("Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(101, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '<data>replication</data>', '/2/1/'),
		(102, NULL, NULL, NULL)`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestNotNullTypes exercises NOT NULL variants of various types to verify schema generation.
func TestNotNullTypes(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(
		id INTEGER PRIMARY KEY,
		a_integer INTEGER NOT NULL,
		a_bigint BIGINT NOT NULL,
		a_bit BIT NOT NULL,
		a_varchar VARCHAR(32) NOT NULL,
		a_decimal DECIMAL(10,2) NOT NULL,
		a_float FLOAT NOT NULL,
		a_datetime2 DATETIME2 NOT NULL,
		a_uniqueidentifier UNIQUEIDENTIFIER NOT NULL
	)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, 123, 1234567891234567, 1, 'hello', 123.45, 1234.5, '2024-01-15T12:30:00', '8292f3cb-0cce-41e8-86aa-ae09bcc988e9'),
		(2, -456, -9876543219876543, 0, 'world', -678.90, -5678.9, '1991-08-31T00:00:00', '00000000-0000-0000-0000-000000000000')`)
	tc.Discover("Discover")
	tc.Run("Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(101, 789, 111222333444555, 1, 'test', 999.99, 9999.9, '2000-06-15T18:45:00', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestScanKeyTypes(t *testing.T) {
	for _, testCase := range []struct {
		Name       string
		ColumnType string
		Values     []string // Individual row tuples, joined with ", " for INSERT
	}{
		{"Integer", "INTEGER", []string{
			"(0, 'Data 0')", "(-3, 'Data 1')", "(2, 'Data 2')", "(1723, 'Data 3')",
		}},
		{"DateTimeOffset", "DATETIMEOFFSET", []string{
			"('1991-08-31T12:34:54.125-06:00', 'Data 0')",
			"('1991-08-31T12:34:54.126-06:00', 'Data 1')",
			"('2000-01-01T01:01:01Z', 'Data 2')",
		}},
		{"UniqueIdentifier", "UNIQUEIDENTIFIER", []string{
			"('00ffffff-ffff-ffff-ffff-ffffffffffff', 'Data 0')",
			"('ff00ffff-ffff-ffff-ffff-ffffffffffff', 'Data 1')",
			"('ffff00ff-ffff-ffff-ffff-ffffffffffff', 'Data 2')",
			"('ffffff00-ffff-ffff-ffff-ffffffffffff', 'Data 3')",
			"('ffffffff-00ff-ffff-ffff-ffffffffffff', 'Data 4')",
			"('ffffffff-ff00-ffff-ffff-ffffffffffff', 'Data 5')",
			"('ffffffff-ffff-00ff-ffff-ffffffffffff', 'Data 6')",
			"('ffffffff-ffff-ff00-ffff-ffffffffffff', 'Data 7')",
			"('ffffffff-ffff-ffff-00ff-ffffffffffff', 'Data 8')",
			"('ffffffff-ffff-ffff-ff00-ffffffffffff', 'Data 9')",
			"('ffffffff-ffff-ffff-ffff-00ffffffffff', 'Data 10')",
			"('ffffffff-ffff-ffff-ffff-ff00ffffffff', 'Data 11')",
			"('ffffffff-ffff-ffff-ffff-ffff00ffffff', 'Data 12')",
			"('ffffffff-ffff-ffff-ffff-ffffff00ffff', 'Data 13')",
			"('ffffffff-ffff-ffff-ffff-ffffffff00ff', 'Data 14')",
			"('ffffffff-ffff-ffff-ffff-ffffffffff00', 'Data 15')",
		}},
		{"Numeric", "NUMERIC(10,5)", []string{
			"(-1234, 'Data 0')", "(0, 'Data 1')", "(-3, 'Data 2')", "(2, 'Data 3')",
			"(12345.12, 'Data 4')", "(12345.121, 'Data 5')", "(12345.1205, 'Data 6')",
			"(12345.12059, 'Data 7')", "(12346, 'Data 8')",
		}},
		{"Decimal", "DECIMAL(10,5)", []string{
			"(-1234, 'Data 0')", "(0, 'Data 1')", "(-3, 'Data 2')", "(2, 'Data 3')",
			"(12345.12, 'Data 4')", "(12345.121, 'Data 5')", "(12345.1205, 'Data 6')",
			"(12345.12059, 'Data 7')", "(12346, 'Data 8')",
		}},
		{"DateTime", "DATETIME", []string{
			"('1991-08-31T12:34:54.111', 'Data 0')",
			"('1991-08-31T12:34:54.333', 'Data 1')",
			"('2000-01-01T01:01:01', 'Data 2')",
		}},
	} {
		t.Run(testCase.Name, func(t *testing.T) {
			var db, tc = blackboxTestSetup(t)
			db.CreateTable(t, `<NAME>`, fmt.Sprintf(`(k %s PRIMARY KEY, v TEXT)`, testCase.ColumnType))
			db.Exec(t, `INSERT INTO <NAME> VALUES `+strings.Join(testCase.Values, ", "))

			require.NoError(t, tc.Capture.EditConfig("advanced.backfill_chunk_size", 1))
			tc.Discover("Discover Tables")
			tc.Run("Backfill", -1)
			cupaloy.SnapshotT(t, tc.Transcript.String())
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
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, v_text TEXT, v_varchar VARCHAR(max), v_binary VARBINARY(max), v_xml XML)`)

	// Use REPLICATE to generate large strings server-side, avoiding query length limits.
	// The alphabet is 26 chars, so we need truncateColumnThreshold/26 repetitions for ~8MB.
	// Must cast input to VARCHAR(MAX) or REPLICATE truncates at 8000 bytes.
	var reps = truncateColumnThreshold / 26
	var largeText = fmt.Sprintf(`REPLICATE(CAST('ABCDEFGHIJKLMNOPQRSTUVWXYZ' AS VARCHAR(MAX)), %d)`, reps)
	var largeXML = fmt.Sprintf(`'<data>' + REPLICATE(CAST('ABCDEFGHIJKLMNOPQRSTUVWXYZ' AS VARCHAR(MAX)), %d) + '</data>'`, reps)
	var largeBinary = fmt.Sprintf(`CONVERT(VARBINARY(MAX), REPLICATE(CAST('ABCDEFGHIJKLMNOPQRSTUVWXYZ' AS VARCHAR(MAX)), %d))`, reps)

	db.Exec(t, fmt.Sprintf(`INSERT INTO <NAME> VALUES (100, %s, %s, %s, %s)`, largeText, largeText, largeBinary, largeXML))
	tc.Discover("Discover Tables")
	tc.Run("Backfill", -1)

	db.Exec(t, fmt.Sprintf(`INSERT INTO <NAME> VALUES (200, %s, %s, %s, %s)`, largeText, largeText, largeBinary, largeXML))
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestBitNotNullDeletion exercises deletions from a table with BIT NOT NULL columns.
func TestBitNotNullDeletion(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, v_a BIT, v_b BIT NOT NULL, v_c BIT NOT NULL DEFAULT 0, v_d BIT NOT NULL DEFAULT 1)`)

	// Backfill
	db.Exec(t, `INSERT INTO <NAME> VALUES (100, 1, 1, 1, 1), (101, 0, 0, 0, 0), (102, NULL, 1, 1, 1), (103, NULL, 0, 0, 0)`)
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill", -1)

	// Replication with deletes
	db.Exec(t, `DELETE FROM <NAME> WHERE id IN (100, 101, 102, 103)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (200, 1, 1, 1, 1), (201, 0, 0, 0, 0), (202, NULL, 1, 1, 1), (203, NULL, 0, 0, 0)`)
	db.Exec(t, `DELETE FROM <NAME> WHERE id IN (200, 201, 202, 203)`)
	tc.Run("Replication With Deletes", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestRowversionTypes exercises the TIMESTAMP / ROWVERSION column type using both names.
// We test this separately from the main datatypes test because these IDs are usually
// tracked implicitly by the database and aren't specified by the client.
func TestRowversionTypes(t *testing.T) {
	for _, columnType := range []string{"timestamp", "rowversion"} {
		t.Run(columnType, func(t *testing.T) {
			var db, tc = blackboxTestSetup(t)
			db.CreateTable(t, `<NAME>`, fmt.Sprintf(`(id INTEGER PRIMARY KEY, data VARCHAR(32), rv %s)`, columnType))

			require.NoError(t, tc.Capture.EditConfig("advanced.feature_flags", "discover_rowversion_as_bytes"))

			// Add sanitizer for rowversion values
			tc.DocumentSanitizers = append(tc.DocumentSanitizers, blackbox.JSONSanitizer{
				Matcher:     regexp.MustCompile(`"rv":"[a-zA-Z0-9+/=]+"`),
				Replacement: `"rv":"<REDACTED>"`,
			})

			tc.DiscoverFull("Discover Table Schema")
			db.Exec(t, `INSERT INTO <NAME> (id, data) VALUES (1, 'first'), (2, 'second')`)
			tc.Run("Initial Backfill", -1)
			db.Exec(t, `INSERT INTO <NAME> (id, data) VALUES (3, 'third'), (4, 'fourth')`)
			db.Exec(t, `UPDATE <NAME> SET data = 'updated' WHERE id = 2`)
			tc.Run("Replication", -1)
			cupaloy.SnapshotT(t, tc.Transcript.String())
		})
	}
}
