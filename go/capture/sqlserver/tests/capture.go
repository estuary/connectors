package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/go/capture/blackbox"
	"github.com/stretchr/testify/require"
)

func TestCapture(t *testing.T, setup testSetupFunc) {
	t.Run("ColumnNameQuoting", func(t *testing.T) { testColumnNameQuoting(t, setup) })
	t.Run("TextCollation", func(t *testing.T) { testTextCollation(t, setup) })
	t.Run("DiscoveryIrrelevantConstraints", func(t *testing.T) { testDiscoveryIrrelevantConstraints(t, setup) })
	t.Run("UUIDCaptureOrder", func(t *testing.T) { testUUIDCaptureOrder(t, setup) })
	t.Run("ManyTables", func(t *testing.T) { testManyTables(t, setup) })
	t.Run("DeletedTextColumn", func(t *testing.T) { testDeletedTextColumn(t, setup) })
	t.Run("ComputedColumn", func(t *testing.T) { testComputedColumn(t, setup) })
	t.Run("DroppedAndRecreatedTable", func(t *testing.T) { testDroppedAndRecreatedTable(t, setup) })
	t.Run("PrimaryKeyUpdate", func(t *testing.T) { testPrimaryKeyUpdate(t, setup) })
	t.Run("ComputedPrimaryKey", func(t *testing.T) { testComputedPrimaryKey(t, setup) })
	t.Run("SourceTag", func(t *testing.T) { testSourceTag(t, setup) })
	t.Run("PopulateSourceTsMs", func(t *testing.T) { testPopulateSourceTsMs(t, setup) })
}

func testColumnNameQuoting(t *testing.T, setup testSetupFunc) {
	var db, tc = setup(t)
	db.CreateTable(t, `<NAME>`, `([id] INTEGER, [data] INTEGER, [CAPITALIZED] INTEGER, [unique] INTEGER, [type] INTEGER, PRIMARY KEY ([id], [data], [CAPITALIZED], [unique], [type]))`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 0, 0, 0, 0), (1, 1, 1, 1, 1), (2, 2, 2, 2, 2)`)
	tc.Discover("Discover Tables")
	tc.Run("Capture", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func testTextCollation(t *testing.T, setup testSetupFunc) {
	var db, tc = setup(t)
	db.CreateTable(t, `<NAME>`, `(id VARCHAR(8) PRIMARY KEY, data TEXT)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES ('AAA', '1'), ('BBB', '2'), ('-J C', '3'), ('H R', '4')`)
	tc.Discover("Discover Tables")
	tc.Run("Capture", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestDiscoveryIrrelevantConstraints verifies that discovery works correctly
// even when there are other non-primary-key constraints on a table.
func testDiscoveryIrrelevantConstraints(t *testing.T, setup testSetupFunc) {
	var db, tc = setup(t)
	db.CreateTable(t, `<NAME>`, `(id VARCHAR(8) PRIMARY KEY, foo INTEGER UNIQUE, data TEXT)`)
	tc.DiscoverFull("Discover Tables")
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func testUUIDCaptureOrder(t *testing.T, setup testSetupFunc) {
	var db, tc = setup(t)
	db.CreateTable(t, `<NAME>`, `(id UNIQUEIDENTIFIER PRIMARY KEY, data TEXT)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		('00ffffff-ffff-ffff-ffff-ffffffffffff', 'sixteen'),
		('ff00ffff-ffff-ffff-ffff-ffffffffffff', 'fifteen'),
		('ffff00ff-ffff-ffff-ffff-ffffffffffff', 'fourteen'),
		('ffffff00-ffff-ffff-ffff-ffffffffffff', 'thirteen'),
		('ffffffff-00ff-ffff-ffff-ffffffffffff', 'twelve'),
		('ffffffff-ff00-ffff-ffff-ffffffffffff', 'eleven'),
		('ffffffff-ffff-00ff-ffff-ffffffffffff', 'ten'),
		('ffffffff-ffff-ff00-ffff-ffffffffffff', 'nine'),
		('ffffffff-ffff-ffff-00ff-ffffffffffff', 'seven'),
		('ffffffff-ffff-ffff-ff00-ffffffffffff', 'eight'),
		('ffffffff-ffff-ffff-ffff-00ffffffffff', 'one'),
		('ffffffff-ffff-ffff-ffff-ff00ffffffff', 'two'),
		('ffffffff-ffff-ffff-ffff-ffff00ffffff', 'three'),
		('ffffffff-ffff-ffff-ffff-ffffff00ffff', 'four'),
		('ffffffff-ffff-ffff-ffff-ffffffff00ff', 'five'),
		('ffffffff-ffff-ffff-ffff-ffffffffff00', 'six')`)
	tc.Discover("Discover Tables")
	tc.Run("Capture", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func testManyTables(t *testing.T, setup testSetupFunc) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	var db, tc = setup(t)

	// Create 20 tables
	for i := 0; i < 20; i++ {
		db.CreateTable(t, fmt.Sprintf(`<NAME>_%03d`, i), `(id INTEGER PRIMARY KEY, data TEXT)`)
	}

	// Insert initial data
	for i := 0; i < 20; i++ {
		db.Exec(t, fmt.Sprintf(`INSERT INTO <NAME>_%03d VALUES (0, 'table %d row zero'), (1, 'table %d row one')`, i, i, i))
	}
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill", -1)

	// More inserts
	for i := 0; i < 20; i++ {
		db.Exec(t, fmt.Sprintf(`INSERT INTO <NAME>_%03d VALUES (2, 'table %d row two'), (3, 'table %d row three')`, i, i, i))
	}
	tc.Run("Replication 1", -1)

	// Partial inserts
	for i := 0; i < 10; i++ {
		db.Exec(t, fmt.Sprintf(`INSERT INTO <NAME>_%03d VALUES (4, 'table %d row four'), (5, 'table %d row five')`, i, i, i))
	}
	tc.Run("Replication 2", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func testDeletedTextColumn(t *testing.T, setup testSetupFunc) {
	var db, tc = setup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, v_text TEXT NOT NULL, v_varchar VARCHAR(32), v_int INTEGER)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'zero', 'zero', 100), (1, 'one', 'one', 101)`)
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (2, 'two', 'two', 102), (3, 'three', 'three', 103)`)
	db.Exec(t, `DELETE FROM <NAME> WHERE id = 1`)
	db.Exec(t, `DELETE FROM <NAME> WHERE id = 2`)
	tc.Run("Replication With Deletes", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func testComputedColumn(t *testing.T, setup testSetupFunc) {
	var db, tc = setup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, a VARCHAR(32), b VARCHAR(32), computed AS ISNULL(a, ISNULL(b, 'default')))`)
	tc.DiscoverFull("Discover Table Schema")
	db.Exec(t, `INSERT INTO <NAME> (id, a, b) VALUES (0, 'a0', 'b0'), (1, NULL, 'b1'), (2, 'a2', NULL), (3, NULL, NULL)`)
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> (id, a, b) VALUES (4, 'a4', 'b4'), (5, NULL, 'b5'), (6, 'a6', NULL), (7, NULL, NULL)`)
	tc.Run("Replication Inserts", -1)
	db.Exec(t, `UPDATE <NAME> SET a = 'a4-modified' WHERE id = 4`)
	db.Exec(t, `UPDATE <NAME> SET b = 'b5-modified' WHERE id = 5`)
	db.Exec(t, `UPDATE <NAME> SET a = 'a6-modified' WHERE id = 6`)
	tc.Run("Updates", -1)
	db.Exec(t, `DELETE FROM <NAME> WHERE id IN (4, 5, 6, 7)`)
	tc.Run("Deletes", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func testDroppedAndRecreatedTable(t *testing.T, setup testSetupFunc) {
	var db, tc = setup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'zero'), (1, 'one'), (2, 'two')`)
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (3, 'three'), (4, 'four'), (5, 'five')`)
	tc.Run("Some Replication", -1)

	// Drop and recreate
	db.Exec(t, `DROP TABLE <NAME>`)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'zero'), (1, 'one'), (2, 'two')`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (6, 'six'), (7, 'seven'), (8, 'eight')`)
	tc.Run("After Drop/Recreate", -1)

	db.Exec(t, `INSERT INTO <NAME> VALUES (9, 'nine'), (10, 'ten'), (11, 'eleven')`)
	tc.Run("More Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func testPrimaryKeyUpdate(t *testing.T, setup testSetupFunc) {
	var db, tc = setup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'zero'), (1, 'one'), (2, 'two')`)
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (3, 'three'), (4, 'four'), (5, 'five')`)
	tc.Run("Some Replication", -1)
	db.Exec(t, `UPDATE <NAME> SET id = 6 WHERE id = 1`)
	db.Exec(t, `UPDATE <NAME> SET id = 7 WHERE id = 4`)
	tc.Run("Primary Key Updates", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func testComputedPrimaryKey(t *testing.T, setup testSetupFunc) {
	var db, tc = setup(t)
	db.CreateTable(t, `<NAME>`, `(actual_id INTEGER NOT NULL, data VARCHAR(32), id AS actual_id PRIMARY KEY)`)
	tc.DiscoverFull("Discover Table Schema")
	db.Exec(t, `INSERT INTO <NAME> (actual_id, data) VALUES (0, 'zero'), (1, 'one'), (2, 'two')`)
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> (actual_id, data) VALUES (3, 'three'), (4, 'four'), (5, 'five')`)
	tc.Run("Replication", -1)
	db.Exec(t, `DELETE FROM <NAME> WHERE id = 1 OR id = 4`)
	tc.Run("Replicated Deletes", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestSourceTag verifies the output of a capture with /advanced/source_tag set
func testSourceTag(t *testing.T, setup testSetupFunc) {
	var db, tc = setup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	require.NoError(t, tc.Capture.EditConfig("advanced.source_tag", "example_source_tag_1234"))
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'zero'), (1, 'one')`)
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (2, 'two'), (3, 'three')`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// testPopulateSourceTsMs verifies the populate_source_ts_ms advanced option:
// CDC events carry transaction commit time as _meta/source/ts_ms; backfill
// rows and flag-off captures do not.
func testPopulateSourceTsMs(t *testing.T, setup testSetupFunc) {
	var db, tc = setup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)

	// Redact ts_ms values in snapshots so timestamps don't destabilize the
	// snapshot across runs; equality and ordering are asserted programmatically
	// against the raw capture output below.
	tc.DocumentSanitizers = append(tc.DocumentSanitizers, blackbox.JSONSanitizer{
		Matcher:     regexp.MustCompile(`"ts_ms":\d+`),
		Replacement: `"ts_ms":REDACTED`,
	})

	// Backfill (flag off): no ts_ms expected.
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'zero'), (1, 'one')`)
	tc.Discover("Discover Tables")
	var backfillBytes = tc.Run("Initial Backfill (flag off)", -1)
	require.Empty(t, extractTsMsValues(t, backfillBytes), "backfill events must not have ts_ms")

	// Replication (flag off): no ts_ms expected.
	db.Exec(t, `INSERT INTO <NAME> VALUES (2, 'two')`)
	var replOffBytes = tc.Run("Replication (flag off)", -1)
	require.Empty(t, extractTsMsValues(t, replOffBytes), "flag-off replication events must not have ts_ms")

	require.NoError(t, tc.Capture.EditConfig("advanced.populate_source_ts_ms", true))

	// Single transaction with multiple rows: all rows must share ts_ms.
	db.Exec(t, `INSERT INTO <NAME> VALUES (3, 'three'), (4, 'four'), (5, 'five')`)
	var multiTxnBytes = tc.Run("Replication (multi-row transaction, flag on)", -1)
	var multiTxnTsMs = extractTsMsValues(t, multiTxnBytes)
	require.Len(t, multiTxnTsMs, 3, "expected ts_ms on all three CDC rows")
	for _, v := range multiTxnTsMs {
		require.NotZero(t, v, "ts_ms should be populated with a real commit time")
		require.Equal(t, multiTxnTsMs[0], v, "rows from one transaction must share ts_ms")
	}

	// Separate transactions: ts_ms must be monotonically non-decreasing.
	time.Sleep(50 * time.Millisecond)
	db.Exec(t, `INSERT INTO <NAME> VALUES (6, 'six')`)
	time.Sleep(50 * time.Millisecond)
	db.Exec(t, `INSERT INTO <NAME> VALUES (7, 'seven')`)
	var sepTxnBytes = tc.Run("Replication (separate transactions, flag on)", -1)
	var sepTxnTsMs = extractTsMsValues(t, sepTxnBytes)
	require.Len(t, sepTxnTsMs, 2, "expected ts_ms on both CDC rows")
	require.LessOrEqual(t, sepTxnTsMs[0], sepTxnTsMs[1], "ts_ms must be monotonically non-decreasing across transactions")

	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// extractTsMsValues returns the _meta/source/ts_ms value for every emitted
// document in the raw capture output, in emission order. Documents without
// ts_ms are skipped.
func extractTsMsValues(t *testing.T, raw []byte) []int64 {
	t.Helper()
	var values []int64
	for line := range bytes.SplitSeq(raw, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		// Each captured document is emitted as ["binding-name", {document}, ...].
		var parts []json.RawMessage
		require.NoError(t, json.Unmarshal(line, &parts))
		if len(parts) < 2 {
			continue
		}
		var doc struct {
			Meta struct {
				Source struct {
					TsMs *int64 `json:"ts_ms"`
				} `json:"source"`
			} `json:"_meta"`
		}
		require.NoError(t, json.Unmarshal(parts[1], &doc))
		if doc.Meta.Source.TsMs != nil {
			values = append(values, *doc.Meta.Source.TsMs)
		}
	}
	return values
}
