package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

func TestScanKeyTimestamps(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(ts TIMESTAMP PRIMARY KEY, data TEXT)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		('1991-08-31T12:34:56.000Z', 'aood'),
		('1991-08-31T12:34:56.111Z', 'xwxt'),
		('1991-08-31T12:34:56.222Z', 'tpxi'),
		('1991-08-31T12:34:56.333Z', 'jvqz'),
		('1991-08-31T12:34:56.444Z', 'juwf'),
		('1991-08-31T12:34:56.555Z', 'znzn'),
		('1991-08-31T12:34:56.666Z', 'zocp'),
		('1991-08-31T12:34:56.777Z', 'pxoi'),
		('1991-08-31T12:34:56.888Z', 'vdug'),
		('1991-08-31T12:34:56.999Z', 'xerk')`)

	// Use chunk size 1 to exercise timestamp scan key serialization on every iteration
	require.NoError(t, tc.Capture.EditConfig("advanced.backfill_chunk_size", 1))
	tc.Discover("Discover Tables")
	tc.Run("Backfill With Timestamp Scan Key", transactionCountBaseline+11)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestScanKeyTypes(t *testing.T) {
	for _, testCase := range []struct {
		Name       string
		ColumnType string
		Values     []string // Individual row tuples, joined with ", " for INSERT
	}{
		{"Bool", "BOOLEAN", []string{
			`('true', 'Data 0')`, `('false', 'Data 1')`,
		}},
		{"Integer", "INTEGER", []string{
			`(0, 'Data 0')`, `(-3, 'Data 1')`, `(2, 'Data 2')`, `(1723, 'Data 3')`,
		}},
		{"SmallInt", "SMALLINT", []string{
			`(0, 'Data 0')`, `(-3, 'Data 1')`, `(2, 'Data 2')`, `(1723, 'Data 3')`,
		}},
		{"BigInt", "BIGINT", []string{
			`(0, 'Data 0')`, `(-3, 'Data 1')`, `(2, 'Data 2')`, `(1723, 'Data 3')`,
		}},
		{"Serial", "SERIAL", []string{
			`(0, 'Data 0')`, `(-3, 'Data 1')`, `(2, 'Data 2')`, `(1723, 'Data 3')`,
		}},
		{"SmallSerial", "SMALLSERIAL", []string{
			`(0, 'Data 0')`, `(-3, 'Data 1')`, `(2, 'Data 2')`, `(1723, 'Data 3')`,
		}},
		{"BigSerial", "BIGSERIAL", []string{
			`(0, 'Data 0')`, `(-3, 'Data 1')`, `(2, 'Data 2')`, `(1723, 'Data 3')`,
		}},
		{"Real", "REAL", []string{
			`(-0.9, 'Data 0')`, `(-1.0, 'Data 1')`, `(-1.1, 'Data 2')`, `(0.0, 'Data 3')`,
			`(0.9, 'Data 4')`, `(1.0, 'Data 5')`, `(1.111, 'Data 6')`, `(1.222, 'Data 7')`,
			`(1.333, 'Data 8')`, `(1.444, 'Data 9')`, `(1.555, 'Data 10')`, `(1.666, 'Data 11')`,
			`(1.777, 'Data 12')`, `(1.888, 'Data 13')`, `(1.999, 'Data 14')`, `(2.000, 'Data 15')`,
		}},
		{"Double", "DOUBLE PRECISION", []string{
			`(-0.9, 'Data 0')`, `(-1.0, 'Data 1')`, `(-1.1, 'Data 2')`, `(0.0, 'Data 3')`,
			`(0.9, 'Data 4')`, `(1.0, 'Data 5')`, `(1.111, 'Data 6')`, `(1.222, 'Data 7')`,
			`(1.333, 'Data 8')`, `(1.444, 'Data 9')`, `(1.555, 'Data 10')`, `(1.666, 'Data 11')`,
			`(1.777, 'Data 12')`, `(1.888, 'Data 13')`, `(1.999, 'Data 14')`, `(2.000, 'Data 15')`,
		}},
		{"Decimal", "DECIMAL", []string{
			`(-0.9, 'Data 0')`, `(-1.0, 'Data 1')`, `(-1.1, 'Data 2')`, `(0.0, 'Data 3')`,
			`(0.9, 'Data 4')`, `(1.0, 'Data 5')`, `(1.111, 'Data 6')`, `(1.222, 'Data 7')`,
			`(1.333, 'Data 8')`, `(1.444, 'Data 9')`, `(1.555, 'Data 10')`, `(1.666, 'Data 11')`,
			`(1.777, 'Data 12')`, `(1.888, 'Data 13')`, `(1.999, 'Data 14')`, `(2.000, 'Data 15')`,
		}},
		{"Numeric", "NUMERIC(4,3)", []string{
			`(-0.9, 'Data 0')`, `(-1.0, 'Data 1')`, `(-1.1, 'Data 2')`, `(0.0, 'Data 3')`,
			`(0.9, 'Data 4')`, `(1.0, 'Data 5')`, `(1.111, 'Data 6')`, `(1.222, 'Data 7')`,
			`(1.333, 'Data 8')`, `(1.444, 'Data 9')`, `(1.555, 'Data 10')`, `(1.666, 'Data 11')`,
			`(1.777, 'Data 12')`, `(1.888, 'Data 13')`, `(1.999, 'Data 14')`, `(2.000, 'Data 15')`,
		}},
		{"VarChar", "VARCHAR(10)", []string{
			`('', 'Data 0')`, `('   ', 'Data 1')`, `('a', 'Data 2')`, `('b', 'Data 3')`,
			`('c', 'Data 4')`, `('A', 'Data 5')`, `('B', 'Data 6')`, `('C', 'Data 7')`,
			`('_a', 'Data 8')`, `('_b', 'Data 9')`, `('_c', 'Data 10')`,
		}},
		{"Char", "CHAR(3)", []string{
			`('   ', 'Data 0')`, `('a', 'Data 1')`, `('b', 'Data 2')`, `('c', 'Data 3')`,
			`('A', 'Data 4')`, `('B', 'Data 5')`, `('C', 'Data 6')`,
			`('_a', 'Data 7')`, `('_b', 'Data 8')`, `('_c', 'Data 9')`,
		}},
		{"Text", "TEXT", []string{
			`('', 'Data 0')`, `('   ', 'Data 1')`, `('a', 'Data 2')`, `('b', 'Data 3')`,
			`('c', 'Data 4')`, `('A', 'Data 5')`, `('B', 'Data 6')`, `('C', 'Data 7')`,
			`('_a', 'Data 8')`, `('_b', 'Data 9')`, `('_c', 'Data 10')`,
		}},
		{"UUID", "UUID", []string{
			`('66b968a7-aeca-4401-8239-5d57958d1572', 'Data 0')`,
			`('4ab4044a-9aab-415c-96c6-17fa338060fa', 'Data 1')`,
			`('c32fb585-fc7f-4347-8fe2-97448f4e93cd', 'Data 2')`,
		}},
		{"MAC6", "macaddr", []string{
			`('47:7a:51:62:c3:aa', 'Data 0')`, `('6f:cf:f3:49:e0:b1', 'Data 1')`,
			`('65:d0:73:5a:5a:c9', 'Data 2')`, `('88:61:08:5b:ae:54', 'Data 3')`,
		}},
		{"MAC8", "macaddr8", []string{
			`('5f:99:67:ac:0d:df:88:3a', 'Data 0')`, `('70:26:74:4c:ac:2c:38:59', 'Data 1')`,
			`('e3:70:99:89:b9:d8:e8:21', 'Data 2')`, `('a5:35:55:da:06:38:39:43', 'Data 3')`,
		}},
		{"OID", "oid", []string{
			`(12345, 'Data 0')`, `(12344, 'Data 1')`, `(12346, 'Data 2')`,
			`(54321, 'Data 3')`, `(54323, 'Data 4')`, `(54322, 'Data 5')`,
		}},
	} {
		t.Run(testCase.Name, func(t *testing.T) {
			var db, tc = blackboxTestSetup(t)
			db.CreateTable(t, `<NAME>`, fmt.Sprintf(`(key %s PRIMARY KEY, data TEXT)`, testCase.ColumnType))
			db.Exec(t, `INSERT INTO <NAME> VALUES `+strings.Join(testCase.Values, ", "))

			require.NoError(t, tc.Capture.EditConfig("advanced.backfill_chunk_size", 1))
			tc.Discover("Discover Tables")
			tc.Run("Backfill", transactionCountBaseline+len(testCase.Values)+1)
			cupaloy.SnapshotT(t, tc.Transcript.String())
		})
	}
}

func TestEnumScanKey(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.QuietExec(t, `DROP TYPE IF EXISTS UserEnum CASCADE`)
	db.Exec(t, `CREATE TYPE UserEnum AS ENUM ('red', 'green', 'blue')`)
	t.Cleanup(func() { db.QuietExec(t, `DROP TYPE UserEnum CASCADE`) })

	db.CreateTable(t, `<NAME>`, `(id INTEGER, color UserEnum, data TEXT, PRIMARY KEY (id, color))`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, 'green', 'data'), (8, 'blue', 'data'), (3, 'red', 'data'),
		(0, 'blue', 'data'), (1, 'blue', 'data'), (0, 'red', 'data'),
		(7, 'red', 'data'), (6, 'green', 'data'), (2, 'blue', 'data'),
		(1, 'red', 'data'), (2, 'red', 'data'), (0, 'green', 'data'),
		(4, 'green', 'data'), (2, 'green', 'data'), (5, 'blue', 'data')`)

	// Use chunk size 1 to exercise enum scan key serialization on every iteration
	require.NoError(t, tc.Capture.EditConfig("advanced.backfill_chunk_size", 1))
	tc.Discover("Discover Tables")
	tc.Run("Backfill With Enum Scan Key", transactionCountBaseline+16)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestSpecialTemporalValues exercises various 'special values' of the date/time
// column types to ensure that they all get captured as something reasonable.
// In theory we ought to test interval values 'infinity' and '-infinity' too,
// but the PGX client library fails to parse those at all right now.
func TestSpecialTemporalValues(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, a_date DATE, a_time TIME, a_timestamp TIMESTAMP)`)

	// Backfill with special temporal values
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(0, 'epoch', NULL, 'epoch'),
		(1, 'infinity', NULL, 'infinity'),
		(2, '-infinity', NULL, '-infinity'),
		(3, NULL, 'allballs', NULL)`)
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill", transactionCountBaseline+1)

	// Replication with same special values
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(10, 'epoch', NULL, 'epoch'),
		(11, 'infinity', NULL, 'infinity'),
		(12, '-infinity', NULL, '-infinity'),
		(13, NULL, 'allballs', NULL)`)
	tc.Run("Replication", transactionCountBaseline)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestBinaryTypes exercises boolean, bytea, and bit types.
func TestBinaryTypes(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(
		id INTEGER PRIMARY KEY,
		a_bool BOOLEAN,
		a_bytea BYTEA,
		a_bit BIT,
		a_bit3 BIT(3),
		a_varbit BIT VARYING,
		a_varbit5 BIT VARYING(5)
	)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, true, '\xDEADBEEF', B'1', B'101', B'1101', B'10111'),
		(2, false, '\xCAFEBABE', B'0', B'010', B'0010', B'01000'),
		(3, NULL, NULL, NULL, NULL, NULL, NULL)`)
	tc.Discover("Discover")
	tc.Run("Backfill", transactionCountBaseline+1)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(101, true, '\xDEADBEEF', B'1', B'101', B'1101', B'10111'),
		(102, false, '\xCAFEBABE', B'0', B'010', B'0010', B'01000'),
		(103, NULL, NULL, NULL, NULL, NULL, NULL)`)
	tc.Run("Replication", transactionCountBaseline)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestLongYearTimestamps validates that timestamps with 5-digit years can be
// properly captured both during backfill and replication.
func TestLongYearTimestamps(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, created_at TIMESTAMPTZ, description TEXT)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (1, '2023-01-01 12:30:45+00', 'Recent date'), (2, '12345-06-07 08:09:10+00', 'Far future date')`)
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill", transactionCountBaseline+1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (4, '2024-05-15 09:30:00+00', 'Another recent date'), (5, '54321-10-11 12:13:14+00', 'Even further future')`)
	tc.Run("Replication", transactionCountBaseline)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestIntegerTypes exercises integer, smallint, bigint, serial, and oid types.
func TestIntegerTypes(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(
		id INTEGER PRIMARY KEY,
		a_integer INTEGER,
		a_smallint SMALLINT,
		a_bigint BIGINT,
		a_oid OID
	)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, 123, 456, 789012345678, 54321),
		(2, -2147483648, -32768, -9223372036854775808, 0),
		(3, 2147483647, 32767, 9223372036854775807, 4294967295),
		(4, NULL, NULL, NULL, NULL)`)
	tc.Discover("Discover")
	tc.Run("Backfill", transactionCountBaseline+1)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(101, 123, 456, 789012345678, 54321),
		(102, -2147483648, -32768, -9223372036854775808, 0),
		(103, 2147483647, 32767, 9223372036854775807, 4294967295),
		(104, NULL, NULL, NULL, NULL)`)
	tc.Run("Replication", transactionCountBaseline)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestFloatingPointTypes exercises real and double precision types.
func TestFloatingPointTypes(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(
		id INTEGER PRIMARY KEY,
		a_real REAL,
		a_double DOUBLE PRECISION
	)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, 123.456, 123.456789012345),
		(2, -123.456, -123.456789012345),
		(3, 'NaN', 'NaN'),
		(4, 'Infinity', 'Infinity'),
		(5, '-Infinity', '-Infinity'),
		(6, NULL, NULL)`)
	tc.Discover("Discover")
	tc.Run("Backfill", transactionCountBaseline+1)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(101, 123.456, 123.456789012345),
		(102, -123.456, -123.456789012345),
		(103, 'NaN', 'NaN'),
		(104, 'Infinity', 'Infinity'),
		(105, '-Infinity', '-Infinity'),
		(106, NULL, NULL)`)
	tc.Run("Replication", transactionCountBaseline)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestNumericTypes exercises decimal and numeric types.
func TestNumericTypes(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(
		id INTEGER PRIMARY KEY,
		a_decimal DECIMAL,
		a_numeric NUMERIC,
		a_numeric_prec NUMERIC(10,4)
	)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, 123.456, 123.456, 123456.7890),
		(2, -999.999, -999.999, -999999.9999),
		(3, 'NaN', 'NaN', 'NaN'),
		(4, 'Infinity', 'Infinity', NULL),
		(5, '-Infinity', '-Infinity', NULL),
		(6, NULL, NULL, NULL)`)
	tc.Discover("Discover")
	tc.Run("Backfill", transactionCountBaseline+1)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(101, 123.456, 123.456, 123456.7890),
		(102, -999.999, -999.999, -999999.9999),
		(103, 'NaN', 'NaN', 'NaN'),
		(104, 'Infinity', 'Infinity', NULL),
		(105, '-Infinity', '-Infinity', NULL),
		(106, NULL, NULL, NULL)`)
	tc.Run("Replication", transactionCountBaseline)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestStringTypes exercises varchar, char, and text types.
func TestStringTypes(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(
		id INTEGER PRIMARY KEY,
		a_varchar VARCHAR(100),
		a_varchar_unbound VARCHAR,
		a_char CHAR(10),
		a_char1 CHAR,
		a_text TEXT
	)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, 'hello', 'world', 'fixed', 'X', 'some text here'),
		(2, '', '', '          ', ' ', ''),
		(3, 'special: "quotes" and ''apostrophes''', 'tabs	and
newlines', 'unicode: ñ', '✓', 'emoji: 🎉'),
		(4, NULL, NULL, NULL, NULL, NULL)`)
	tc.Discover("Discover")
	tc.Run("Backfill", transactionCountBaseline+1)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(101, 'hello', 'world', 'fixed', 'X', 'some text here'),
		(102, '', '', '          ', ' ', ''),
		(103, 'special: "quotes" and ''apostrophes''', 'tabs	and
newlines', 'unicode: ñ', '✓', 'emoji: 🎉'),
		(104, NULL, NULL, NULL, NULL, NULL)`)
	tc.Run("Replication", transactionCountBaseline)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestTemporalTypes exercises date, time, timestamp, and interval types.
func TestTemporalTypes(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(
		id INTEGER PRIMARY KEY,
		a_date DATE,
		a_time TIME,
		a_timetz TIME WITH TIME ZONE,
		a_timestamp TIMESTAMP,
		a_timestamptz TIMESTAMPTZ,
		a_interval INTERVAL
	)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, '2024-01-15', '14:30:00', '14:30:00+05:30', '2024-01-15 14:30:00', '2024-01-15 14:30:00+00', '1 year 2 months 3 days'),
		(2, '1999-12-31', '23:59:59.999999', '23:59:59.999999-08:00', '1999-12-31 23:59:59.999999', '1999-12-31 23:59:59.999999+00', '5 hours 30 minutes'),
		(3, 'epoch', 'allballs', '00:00:00+00', 'epoch', 'epoch', '0'),
		(4, 'infinity', NULL, NULL, 'infinity', 'infinity', NULL),
		(5, '-infinity', NULL, NULL, '-infinity', '-infinity', NULL),
		(6, NULL, NULL, NULL, NULL, NULL, NULL)`)
	tc.Discover("Discover")
	tc.Run("Backfill", transactionCountBaseline+1)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(101, '2024-01-15', '14:30:00', '14:30:00+05:30', '2024-01-15 14:30:00', '2024-01-15 14:30:00+00', '1 year 2 months 3 days'),
		(102, '1999-12-31', '23:59:59.999999', '23:59:59.999999-08:00', '1999-12-31 23:59:59.999999', '1999-12-31 23:59:59.999999+00', '5 hours 30 minutes'),
		(103, 'epoch', 'allballs', '00:00:00+00', 'epoch', 'epoch', '0'),
		(104, 'infinity', NULL, NULL, 'infinity', 'infinity', NULL),
		(105, '-infinity', NULL, NULL, '-infinity', '-infinity', NULL),
		(106, NULL, NULL, NULL, NULL, NULL, NULL)`)
	tc.Run("Replication", transactionCountBaseline)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestGeometryTypes exercises PostgreSQL geometric types.
func TestGeometryTypes(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(
		id INTEGER PRIMARY KEY,
		a_point POINT,
		a_line LINE,
		a_lseg LSEG,
		a_box BOX,
		a_path PATH,
		a_polygon POLYGON,
		a_circle CIRCLE
	)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, '(1,2)', '{1,2,3}', '[(1,2),(3,4)]', '((1,2),(3,4))', '[(1,2),(3,4),(5,6)]', '((0,0),(0,1),(1,0))', '<(1,2),3>'),
		(2, '(0,0)', '{1,0,0}', '[(0,0),(1,1)]', '((0,0),(1,1))', '((0,0))', '((0,0),(1,1),(2,0))', '<(0,0),1>'),
		(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL)`)
	tc.Discover("Discover")
	tc.Run("Backfill", transactionCountBaseline+1)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(101, '(1,2)', '{1,2,3}', '[(1,2),(3,4)]', '((1,2),(3,4))', '[(1,2),(3,4),(5,6)]', '((0,0),(0,1),(1,0))', '<(1,2),3>'),
		(102, '(0,0)', '{1,0,0}', '[(0,0),(1,1)]', '((0,0),(1,1))', '((0,0))', '((0,0),(1,1),(2,0))', '<(0,0),1>'),
		(103, NULL, NULL, NULL, NULL, NULL, NULL, NULL)`)
	tc.Run("Replication", transactionCountBaseline)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestNetworkTypes exercises inet, cidr, and MAC address types.
func TestNetworkTypes(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(
		id INTEGER PRIMARY KEY,
		a_inet INET,
		a_cidr CIDR,
		a_macaddr MACADDR,
		a_macaddr8 MACADDR8
	)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, '192.168.1.1', '192.168.1.0/24', '08:00:2b:01:02:03', '08:00:2b:01:02:03:04:05'),
		(2, '2001:4f8:3:ba::1', '2001:4f8:3:ba::/64', 'ff:ff:ff:ff:ff:ff', 'ff:ff:ff:ff:ff:ff:ff:ff'),
		(3, '10.0.0.1/8', '10.0.0.0/8', '00:00:00:00:00:00', '00:00:00:00:00:00:00:00'),
		(4, NULL, NULL, NULL, NULL)`)
	tc.Discover("Discover")
	tc.Run("Backfill", transactionCountBaseline+1)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(101, '192.168.1.1', '192.168.1.0/24', '08:00:2b:01:02:03', '08:00:2b:01:02:03:04:05'),
		(102, '2001:4f8:3:ba::1', '2001:4f8:3:ba::/64', 'ff:ff:ff:ff:ff:ff', 'ff:ff:ff:ff:ff:ff:ff:ff'),
		(103, '10.0.0.1/8', '10.0.0.0/8', '00:00:00:00:00:00', '00:00:00:00:00:00:00:00'),
		(104, NULL, NULL, NULL, NULL)`)
	tc.Run("Replication", transactionCountBaseline)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestMiscTypes exercises uuid, text search, and money types.
func TestMiscTypes(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(
		id INTEGER PRIMARY KEY,
		a_uuid UUID,
		a_tsvector TSVECTOR,
		a_tsquery TSQUERY,
		a_money MONEY
	)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'a fat cat sat', 'fat & cat', '$123.45'),
		(2, '00000000-0000-0000-0000-000000000000', 'the quick brown fox', 'quick | fox', '$0.00'),
		(3, 'ffffffff-ffff-ffff-ffff-ffffffffffff', '''simple''', 'simple', '-$999.99'),
		(4, NULL, NULL, NULL, NULL)`)
	tc.Discover("Discover")
	tc.Run("Backfill", transactionCountBaseline+1)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(101, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'a fat cat sat', 'fat & cat', '$123.45'),
		(102, '00000000-0000-0000-0000-000000000000', 'the quick brown fox', 'quick | fox', '$0.00'),
		(103, 'ffffffff-ffff-ffff-ffff-ffffffffffff', '''simple''', 'simple', '-$999.99'),
		(104, NULL, NULL, NULL, NULL)`)
	tc.Run("Replication", transactionCountBaseline)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestJSONTypes exercises json, jsonb, jsonpath, and xml types.
func TestJSONTypes(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(
		id INTEGER PRIMARY KEY,
		a_json JSON,
		a_jsonb JSONB,
		a_jsonpath JSONPATH,
		a_xml XML
	)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, '{"name": "test", "value": 123}', '{"name": "test", "value": 123}', '$.name', '<root><item>hello</item></root>'),
		(2, '[1, 2, 3]', '[1, 2, 3]', '$[0]', '<empty/>'),
		(3, 'null', 'null', '$', '<data attr="value">content</data>'),
		(4, NULL, NULL, NULL, NULL)`)
	tc.Discover("Discover")
	tc.Run("Backfill", transactionCountBaseline+1)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(101, '{"name": "test", "value": 123}', '{"name": "test", "value": 123}', '$.name', '<root><item>hello</item></root>'),
		(102, '[1, 2, 3]', '[1, 2, 3]', '$[0]', '<empty/>'),
		(103, 'null', 'null', '$', '<data attr="value">content</data>'),
		(104, NULL, NULL, NULL, NULL)`)
	tc.Run("Replication", transactionCountBaseline)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestArrayTypes exercises various PostgreSQL array types.
func TestArrayTypes(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(
		id INTEGER PRIMARY KEY,
		a_integer INTEGER[],
		a_text TEXT[],
		a_boolean BOOLEAN[],
		a_double DOUBLE PRECISION[],
		a_uuid UUID[]
	)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, '{1,2,3}', '{"hello","world"}', '{true,false,true}', '{1.1,2.2,3.3}', '{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}'),
		(2, '{}', '{}', '{}', '{}', '{}'),
		(3, '{1,NULL,3}', '{NULL,"test",NULL}', '{true,NULL,false}', '{NULL,2.2,NULL}', '{NULL}'),
		(4, '{{1,2},{3,4}}', '{{"a","b"},{"c","d"}}', NULL, NULL, NULL),
		(5, NULL, NULL, NULL, NULL, NULL)`)
	tc.Discover("Discover")
	tc.Run("Backfill", transactionCountBaseline+1)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(101, '{1,2,3}', '{"hello","world"}', '{true,false,true}', '{1.1,2.2,3.3}', '{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}'),
		(102, '{}', '{}', '{}', '{}', '{}'),
		(103, '{1,NULL,3}', '{NULL,"test",NULL}', '{true,NULL,false}', '{NULL,2.2,NULL}', '{NULL}'),
		(104, '{{1,2},{3,4}}', '{{"a","b"},{"c","d"}}', NULL, NULL, NULL),
		(105, NULL, NULL, NULL, NULL, NULL)`)
	tc.Run("Replication", transactionCountBaseline)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestRangeTypes exercises PostgreSQL range types.
func TestRangeTypes(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(
		id INTEGER PRIMARY KEY,
		a_int4range INT4RANGE,
		a_int8range INT8RANGE,
		a_numrange NUMRANGE,
		a_tsrange TSRANGE,
		a_tstzrange TSTZRANGE,
		a_daterange DATERANGE
	)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, '[1,10)', '[1,1000000000)', '(0,1]', '[2024-01-01,2024-12-31)', '[2024-01-01 00:00:00+00,2024-12-31 23:59:59+00)', '[2024-01-01,2024-12-31)'),
		(2, '(,)', '(,)', '(,)', '(,)', '(,)', '(,)'),
		(3, '[5,5]', '[100,100]', '[1.5,1.5]', NULL, NULL, NULL),
		(4, NULL, NULL, NULL, NULL, NULL, NULL)`)
	tc.Discover("Discover")
	tc.Run("Backfill", transactionCountBaseline+1)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(101, '[1,10)', '[1,1000000000)', '(0,1]', '[2024-01-01,2024-12-31)', '[2024-01-01 00:00:00+00,2024-12-31 23:59:59+00)', '[2024-01-01,2024-12-31)'),
		(102, '(,)', '(,)', '(,)', '(,)', '(,)', '(,)'),
		(103, '[5,5]', '[100,100]', '[1.5,1.5]', NULL, NULL, NULL),
		(104, NULL, NULL, NULL, NULL, NULL, NULL)`)
	tc.Run("Replication", transactionCountBaseline)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestExtensionTypes exercises extension types like citext.
func TestExtensionTypes(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(
		id INTEGER PRIMARY KEY,
		a_citext CITEXT,
		a_citext_arr CITEXT[]
	)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, 'Hello World', '{"Hello","World"}'),
		(2, 'UPPERCASE', '{"UPPER","CASE"}'),
		(3, 'lowercase', '{"lower","case"}'),
		(4, NULL, NULL)`)
	tc.Discover("Discover")
	tc.Run("Backfill", transactionCountBaseline+1)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(101, 'Hello World', '{"Hello","World"}'),
		(102, 'UPPERCASE', '{"UPPER","CASE"}'),
		(103, 'lowercase', '{"lower","case"}'),
		(104, NULL, NULL)`)
	tc.Run("Replication", transactionCountBaseline)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestSerialTypes exercises serial types which are always NOT NULL.
func TestSerialTypes(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(
		id INTEGER PRIMARY KEY,
		a_serial SERIAL,
		a_smallserial SMALLSERIAL,
		a_bigserial BIGSERIAL
	)`)
	// Serial types auto-generate values, but we can override them
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, 100, 200, 300),
		(2, 101, 201, 301),
		(3, 102, 202, 302)`)
	tc.Discover("Discover")
	tc.Run("Backfill", transactionCountBaseline+1)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(101, 1000, 2000, 3000),
		(102, 1001, 2001, 3001),
		(103, 1002, 2002, 3002)`)
	tc.Run("Replication", transactionCountBaseline)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestNotNullTypes exercises NOT NULL variants of types where nullability affects the schema.
func TestNotNullTypes(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(
		id INTEGER PRIMARY KEY,
		a_integer INTEGER NOT NULL,
		a_text TEXT NOT NULL,
		a_boolean BOOLEAN NOT NULL,
		a_numeric NUMERIC NOT NULL,
		a_uuid UUID NOT NULL,
		a_jsonb JSONB NOT NULL,
		a_integer_arr INTEGER[] NOT NULL
	)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, 123, 'hello', true, 123.456, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{"key": "value"}', '{1,2,3}'),
		(2, -456, 'world', false, -789.012, '00000000-0000-0000-0000-000000000000', '[]', '{}')`)
	tc.Discover("Discover")
	tc.Run("Backfill", transactionCountBaseline+1)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(101, 123, 'hello', true, 123.456, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{"key": "value"}', '{1,2,3}'),
		(102, -456, 'world', false, -789.012, '00000000-0000-0000-0000-000000000000', '[]', '{}')`)
	tc.Run("Replication", transactionCountBaseline)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}
