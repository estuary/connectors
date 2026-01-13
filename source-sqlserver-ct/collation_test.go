package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/stretchr/testify/require"
)

func TestVarcharKeyDiscovery(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id VARCHAR(32) PRIMARY KEY, data TEXT)`)
	tc.DiscoverFull("Discover Tables")
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestCollatedCapture runs a complete capture from a table with an `xCHAR
// COLLATE SQL_Latin1_General_CP1_CI_AS` primary key, using key values which
// are known to exhibit interesting sort behavior.
//
// For the CHAR and VARCHAR column types we know that the capture will use
// our special order-preserving key encoding, but the NCHAR/NVARCHAR cases
// are considered "unpredictable" and we fall back to trusting the database
// ordering of keys and avoid doing any key sorting ourselves.
func TestCollatedCapture(t *testing.T) {
	// Test runes that exhibit interesting sort behavior under SQL_Latin1_General_CP1_CI_AS collation.
	// Selected to include characters that sort differently from Unicode code point order.
	var testRunes = []rune("?abc€Š\u2019•ÀÁÂÖ×Øß÷ÿ") // 15 distinct runes

	for _, columnType := range []string{"Char", "VarChar", "NChar", "NVarChar"} {
		t.Run(columnType, func(t *testing.T) {
			var db, tc = blackboxTestSetup(t)
			db.CreateTable(t, `<NAME>`, fmt.Sprintf(`(id %s(32) COLLATE SQL_Latin1_General_CP1_CI_AS PRIMARY KEY, data TEXT)`, columnType))
			require.NoError(t, tc.Capture.EditConfig("advanced.backfill_chunk_size", 1))

			// Insert 30 rows (2 per test rune) with interesting collation behavior
			for idx, r := range testRunes {
				// Escape single quotes for SQL by doubling them
				var runeStr = strings.ReplaceAll(string(r), "'", "''")
				db.Exec(t, fmt.Sprintf(`INSERT INTO <NAME> VALUES ('a%sa %03d', 'Row A%03d'), ('a%sb %03d', 'Row B%03d')`,
					runeStr, idx, idx, runeStr, idx, idx))
			}

			tc.Discover("Discover")
			tc.Run("Backfill", -1)
			cupaloy.SnapshotT(t, tc.Transcript.String())
		})
	}
}

// TestCaptureWithCompoundTextAndIntegerKey verifies that a table with a
// compound primary key which mixes text and integers still works.
func TestCaptureWithCompoundTextAndIntegerKey(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(k1 VARCHAR(32), k2 INTEGER, k3 VARCHAR(8), k4 INTEGER, data TEXT, PRIMARY KEY (k1, k2, k3, k4))`)
	require.NoError(t, tc.Capture.EditConfig("advanced.backfill_chunk_size", 1))

	// Insert 24 rows (3 iterations × 8 rows)
	for i := range 3 {
		db.Exec(t, fmt.Sprintf(`INSERT INTO <NAME> VALUES
			('aaa', 100, 'b', %[1]d, 'data'), ('aaa', 100, 'c', %[1]d, 'data'),
			('aaa', 200, 'b', %[1]d, 'data'), ('aaa', 200, 'c', %[1]d, 'data'),
			('zzz', 100, 'b', %[1]d, 'data'), ('zzz', 100, 'c', %[1]d, 'data'),
			('zzz', 200, 'b', %[1]d, 'data'), ('zzz', 200, 'c', %[1]d, 'data')`,
			i))
	}

	tc.Discover("Discover")
	tc.Run("Backfill", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestColumnCollations verifies that our encoding of text column keys can
// be round-tripped through FDB serialization/deserialization without loss,
// and that for collations which are considered "predictable" the serialized
// key ordering matches the server result ordering.
func TestColumnCollations(t *testing.T) {
	// Generate a set of unique test strings which we expect to exhibit
	// interesting sort behavior. Many of these runes have been selected
	// deliberately to exercise code points which various SQL Server code
	// pages sort differently from Unicode code point order.
	var testRunes = "abc?@-_,·°€丠両░▒▓█αßσµ☆★"
	var testStrings []string
	for idx, r := range testRunes {
		testStrings = append(testStrings, fmt.Sprintf("%s %06d", string([]rune{'a', r, 'a'}), idx*3+0))
		testStrings = append(testStrings, fmt.Sprintf("%s %06d", string([]rune{'b', r, 'b'}), idx*3+1))
		testStrings = append(testStrings, fmt.Sprintf("%s %06d", string([]rune{'c', r, 'c'}), idx*3+2))
	}

	for _, tc := range []struct {
		ColumnType    string
		CollationName string
	}{
		{"VARCHAR", "SQL_Latin1_General_CP1_CI_AS"}, // SQL Server Default
		{"VARCHAR", "SQL_Latin1_General_CP437_BIN"},
		{"VARCHAR", "Latin1_General_100_BIN2_UTF8"},
		{"VARCHAR", "Chinese_PRC_BIN2"},
		{"NVARCHAR", "SQL_Latin1_General_CP1_CI_AS"}, // SQL Server Default
		{"NVARCHAR", "SQL_Latin1_General_CP437_BIN"},
		{"NVARCHAR", "Latin1_General_100_BIN2_UTF8"},
		{"NVARCHAR", "Chinese_PRC_BIN2"},
	} {
		var subtestName = fmt.Sprintf("%s_%s", tc.ColumnType, tc.CollationName)
		t.Run(subtestName, func(t *testing.T) {
			var db, _ = blackboxTestSetup(t)
			var ctx = context.Background()
			db.CreateTable(t, `<NAME>`, fmt.Sprintf(`(id %s(32) COLLATE %s PRIMARY KEY, data TEXT)`, tc.ColumnType, tc.CollationName))

			// Insert test strings
			for idx, str := range testStrings {
				// Escape single quotes for SQL
				var escaped = strings.ReplaceAll(str, "'", "''")
				db.Exec(t, fmt.Sprintf(`INSERT INTO <NAME> VALUES ('%s', 'This is test row %d with contents %q')`, escaped, idx, str))
			}

			var resultRows, err = db.conn.QueryContext(ctx, fmt.Sprintf(`SELECT id, data FROM %s ORDER BY id`, db.Expand(`<NAME>`)))
			require.NoError(t, err)
			defer resultRows.Close()

			// Simulate what we'd end up with after all the discovery->backfill plumbing.
			var keyColumns = []string{"id"}
			var idCollation = &sqlserverTextColumnType{strings.ToLower(tc.ColumnType), tc.CollationName, tc.ColumnType + "(32)", 32}
			var columnTypes = map[string]any{
				"id":   idCollation,
				"data": &sqlserverTextColumnType{"text", "SQL_Latin1_General_CP1_CI_AS", "TEXT", 0},
			}

			var prevID string
			var prevKey []byte
			for resultRows.Next() {
				var id, data string
				require.NoError(t, resultRows.Scan(&id, &data))
				var rowKey, err = sqlcapture.EncodeRowKey(keyColumns, map[string]any{"id": id, "data": data}, columnTypes, encodeKeyFDB)
				require.NoError(t, err)

				roundTrippedKey, err := sqlcapture.UnpackTuple(rowKey, decodeKeyFDB)
				require.NoError(t, err)
				if len(roundTrippedKey) != 1 || id != roundTrippedKey[0] {
					t.Fatalf("key round-trip failure: originally %#v but now %#v", id, roundTrippedKey)
				}

				if predictableCollation(idCollation) && bytes.Compare(prevKey, rowKey) >= 0 {
					t.Fatalf("key ordering failure: we think %q >= %q (in `%s(32) COLLATE %s` column) but the server disagrees", prevID, id, tc.ColumnType, tc.CollationName)
				}
				prevID, prevKey = id, rowKey
			}
		})
	}
}
