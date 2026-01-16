package tests

import (
	"fmt"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

func TestCollation(t *testing.T, setup testSetupFunc) {
	t.Run("VarcharKeyDiscovery", func(t *testing.T) { testVarcharKeyDiscovery(t, setup) })
	t.Run("CollatedCapture", func(t *testing.T) { testCollatedCapture(t, setup) })
	t.Run("CaptureWithCompoundTextAndIntegerKey", func(t *testing.T) { testCaptureWithCompoundTextAndIntegerKey(t, setup) })
}

func testVarcharKeyDiscovery(t *testing.T, setup testSetupFunc) {
	var db, tc = setup(t)
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
func testCollatedCapture(t *testing.T, setup testSetupFunc) {
	// Test runes that exhibit interesting sort behavior under SQL_Latin1_General_CP1_CI_AS collation.
	// Selected to include characters that sort differently from Unicode code point order.
	var testRunes = []rune("?abc€Š\u2019•ÀÁÂÖ×Øß÷ÿ") // 15 distinct runes

	for _, columnType := range []string{"Char", "VarChar", "NChar", "NVarChar"} {
		t.Run(columnType, func(t *testing.T) {
			var db, tc = setup(t)
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
func testCaptureWithCompoundTextAndIntegerKey(t *testing.T, setup testSetupFunc) {
	var db, tc = setup(t)
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
