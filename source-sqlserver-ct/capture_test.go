package main

import (
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/stretchr/testify/require"
)

// TestNullHandling exercises how "test", "", and NULL values are serialized
// in backfill, replicated inserts, and deletes. The delete case is particularly
// interesting because the LEFT OUTER JOIN with the source table produces NULLs
// for non-key columns when the row no longer exists.
func TestNullHandling(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data VARCHAR(32))`)

	// Backfill: "test", "", and NULL
	db.Exec(t, `INSERT INTO <NAME> VALUES (1, 'test'), (2, ''), (3, NULL)`)
	tc.Discover("Discover Tables")
	tc.Run("Backfill", -1)

	// Replicated inserts: "test", "", and NULL
	db.Exec(t, `INSERT INTO <NAME> VALUES (4, 'test'), (5, ''), (6, NULL)`)
	tc.Run("Replicated Inserts", -1)

	// Delete all rows
	db.Exec(t, `DELETE FROM <NAME> WHERE id IN (1, 2, 3, 4, 5, 6)`)
	tc.Run("Deletes", -1)

	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestChangeTrackingDisabled(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'zero'), (1, 'one'), (2, 'two')`)
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (3, 'three'), (4, 'four'), (5, 'five')`)
	tc.Run("Some Replication", -1)

	// Disable change tracking on the table
	db.Exec(t, `ALTER TABLE <NAME> DISABLE CHANGE_TRACKING`)
	tc.Run("After CT Disabled", -1)

	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestBackfillWithoutKey sets up a table with a primary key but configures
// the binding to use the "Without Primary Key" backfill mode.
func TestBackfillWithoutKey(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data VARCHAR(2000))`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'A'), (1, 'bbb'), (2, 'CDEFGH')`)
	tc.Discover("Discover Tables")
	require.NoError(t, tc.Capture.EditBinding(0, "resource.mode", string(sqlcapture.BackfillModeWithoutKey)))
	tc.Run("Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (3, 'more'), (4, 'data')`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestDiscoverOnlyEnabled tests discovery table filtering when only CT-enabled tables should be discovered.
func TestDiscoverOnlyEnabled(t *testing.T) {
	t.Run("Default", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		// Create three tables, one (table B) without change tracking
		db.CreateTable(t, `<NAME>_a`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.CreateTable(t, `<NAME>_b`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.CreateTable(t, `<NAME>_c`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.QuietExec(t, `ALTER TABLE <NAME>_b DISABLE CHANGE_TRACKING`)

		tc.Discover("Discover Tables (Default behavior: only CT-enabled)")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
	t.Run("DiscoverWithoutCT", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		// Create three tables, one (table B) without change tracking
		db.CreateTable(t, `<NAME>_a`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.CreateTable(t, `<NAME>_b`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.CreateTable(t, `<NAME>_c`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.QuietExec(t, `ALTER TABLE <NAME>_b DISABLE CHANGE_TRACKING`)

		require.NoError(t, tc.Capture.EditConfig("advanced.discover_tables_without_ct", true))
		tc.Discover("Discover Tables (Non-default behavior: includ tables without CT)")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
}

func TestKeylessDiscovery(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTableWithoutCT(t, `<NAME>`, `(a INTEGER, b VARCHAR(2000), c REAL NOT NULL, d VARCHAR(255))`)
	tc.DiscoverFull("Discover Tables")
	cupaloy.SnapshotT(t, tc.Transcript.String())
}
