package main

import (
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/stretchr/testify/require"
)

// TestSpec verifies the connector's spec output against a snapshot.
func TestSpec(t *testing.T) {
	var _, tc = blackboxTestSetup(t)
	tc.Spec("Get Connector Spec")
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestSimpleDiscovery exercises discovery of a single, simple table.
func TestSimpleDiscovery(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(a INTEGER PRIMARY KEY, b VARCHAR(2000), c REAL NOT NULL, d VARCHAR(255))`)
	tc.DiscoverFull("Discover Tables")
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestSimpleCapture exercises the simplest possible backfill of a small table.
func TestSimpleCapture(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data VARCHAR(2000))`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'A'), (1, 'bbb'), (2, 'CDEFGH')`)
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestReplicationInserts runs two captures, where the first will perform the
// initial table scan and the second capture will use replication to receive
// additional inserts performed after the first capture.
func TestReplicationInserts(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data VARCHAR(2000))`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'A'), (1, 'bbb'), (2, 'CDEFGH')`)
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (1002, 'some'), (1000, 'more'), (1001, 'rows')`)
	tc.Run("Replication Inserts", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestReplicationUpdates runs two captures, where the first will perform the
// initial table scan and the second capture will use replication to receive
// additional inserts and row updates performed after the first capture.
func TestReplicationUpdates(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data VARCHAR(2000))`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'A'), (1, 'bbb'), (2, 'CDEFGH')`)
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (1002, 'some'), (1000, 'more'), (1001, 'rows')`)
	db.Exec(t, `UPDATE <NAME> SET data = 'updated' WHERE id = 1`)
	db.Exec(t, `UPDATE <NAME> SET data = 'updated' WHERE id = 1002`)
	tc.Run("Replication Updates", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestReplicationDeletes runs two captures, where the first will perform the
// initial table scan and the second capture will use replication to receive
// additional inserts and deletions performed after the first capture.
func TestReplicationDeletes(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data VARCHAR(2000))`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'A'), (1, 'bbb'), (2, 'CDEFGH')`)
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (1002, 'some'), (1000, 'more'), (1001, 'rows')`)
	db.Exec(t, `DELETE FROM <NAME> WHERE id = 1`)
	db.Exec(t, `DELETE FROM <NAME> WHERE id = 1002`)
	tc.Run("Replication Deletes", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestEmptyTable leaves the table empty during the initial table backfill
// and only adds data after replication has begun.
func TestEmptyTable(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data VARCHAR(2000))`)
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill (Empty)", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (1002, 'some'), (1000, 'more'), (1001, 'rows')`)
	tc.Run("Replication Inserts", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestIgnoredStreams checks that replicated changes are only reported
// for tables which are configured in the catalog.
func TestIgnoredStreams(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>_a`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.CreateTable(t, `<NAME>_b`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.Exec(t, `INSERT INTO <NAME>_a VALUES (0, 'zero'), (1, 'one')`)
	db.Exec(t, `INSERT INTO <NAME>_b VALUES (2, 'two'), (3, 'three')`)
	tc.Discover("Discover Tables")
	require.NoError(t, tc.Capture.EditBinding(0, "disable", true)) // Disable table A
	tc.Run("Backfill (Without A)", -1)
	db.Exec(t, `INSERT INTO <NAME>_a VALUES (4, 'four')`)
	db.Exec(t, `INSERT INTO <NAME>_b VALUES (5, 'five')`)
	tc.Run("Replication (Without A)", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestMultipleStreams exercises captures with multiple stream configured, as
// well as adding/removing/re-adding a stream.
func TestMultipleStreams(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>_a`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.CreateTable(t, `<NAME>_b`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.CreateTable(t, `<NAME>_c`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.Exec(t, `INSERT INTO <NAME>_a VALUES (0, 'zero'), (1, 'one')`)
	db.Exec(t, `INSERT INTO <NAME>_b VALUES (2, 'two'), (3, 'three')`)
	db.Exec(t, `INSERT INTO <NAME>_c VALUES (4, 'four'), (5, 'five')`)
	tc.Discover("Discover Tables")
	// Bindings sorted alphabetically: 0=A, 1=B, 2=C
	require.NoError(t, tc.Capture.EditBinding(1, "disable", true)) // Disable B
	require.NoError(t, tc.Capture.EditBinding(2, "disable", true)) // Disable C
	tc.Run("Backfill (Only A)", -1)
	require.NoError(t, tc.Capture.EditBinding(1, "disable", false)) // Enable B
	require.NoError(t, tc.Capture.EditBinding(2, "disable", false)) // Enable C
	tc.Run("Backfill (Add B+C)", -1)                                // -1 to ensure all backfills finish
	require.NoError(t, tc.Capture.EditBinding(1, "disable", true))  // Disable B
	tc.Run("No-op Capture (Removed B)", -1)
	db.Exec(t, `INSERT INTO <NAME>_a VALUES (6, 'six')`)
	db.Exec(t, `INSERT INTO <NAME>_b VALUES (7, 'seven')`)
	db.Exec(t, `INSERT INTO <NAME>_c VALUES (8, 'eight')`)
	tc.Run("Replication (A+C only)", -1)
	require.NoError(t, tc.Capture.EditBinding(1, "disable", false)) // Enable B
	db.Exec(t, `INSERT INTO <NAME>_a VALUES (9, 'nine')`)
	tc.Run("Backfill B (Re-enabled) and Replicate A Changes", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestMissingTable verifies that things fail cleanly if a capture
// binding doesn't actually exist.
//
// TODO(wgd): We need to do better at snapshotting capture error messages
// in the black-box testing framework.
func TestMissingTable(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>_a`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.CreateTable(t, `<NAME>_b`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	tc.Discover("Discover Tables")
	db.Exec(t, `DROP TABLE <NAME>_b`)
	tc.Run("Capture", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// Since discovered bindings for keyless tables specify the keyless backfill mode,
// the collection key shouldn't impact correctness of the capture even if multiple
// rows have the same collection key value.
func TestDuplicatedScanKey(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id VARCHAR(8), data VARCHAR(2000))`)
	db.Exec(t, `INSERT INTO <NAME> VALUES ('AAA', '1'), ('BBB', '2'), ('BBB', '3'), ('CCC', '4')`)
	tc.Discover("Discover Tables")
	require.NoError(t, tc.Capture.EditConfig("advanced.backfill_chunk_size", 1))
	require.NoError(t, tc.Capture.EditCollection(db.Expand(`<ID>`), "key", []string{"/id"}))
	tc.Run("Capture", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestReplicationOnly(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER, data TEXT)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')`)
	tc.Discover("Discover Tables")
	require.NoError(t, tc.Capture.EditConfig("advanced.backfill_chunk_size", 1))
	require.NoError(t, tc.Capture.EditBinding(0, "resource.mode", string(sqlcapture.BackfillModeOnlyChanges)))
	tc.Run("Backfill (no documents expected)", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (5, 'five'), (6, 'six'), (7, 'seven'), (8, 'eight')`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestKeylessDiscovery(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(a INTEGER, b VARCHAR(2000), c REAL NOT NULL, d VARCHAR(255))`)
	tc.DiscoverFull("Discover Tables")
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestKeylessCapture(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER, data TEXT)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (1, 'one'), (2, 'two')`)
	tc.Discover("Discover Tables")
	require.NoError(t, tc.Capture.EditConfig("advanced.backfill_chunk_size", 1))
	tc.Run("Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (3, 'three'), (4, 'four')`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestCatalogPrimaryKey sets up a table with no primary key in the database
// and instead specifies one in the catalog configuration.
func TestCatalogPrimaryKey(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER, name TEXT, value INTEGER)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (1, 'alice', 100), (2, 'bob', 200)`)
	tc.Discover("Discover Tables")
	require.NoError(t, tc.Capture.EditConfig("advanced.backfill_chunk_size", 1))
	require.NoError(t, tc.Capture.EditBinding(0, "resource.mode", string(sqlcapture.BackfillModeAutomatic)))
	require.NoError(t, tc.Capture.EditBinding(0, "resource.primary_key", []string{"id"}))
	require.NoError(t, tc.Capture.EditCollection(db.Expand("<ID>"), "key", []string{"/id"}))
	tc.Run("Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (3, 'carol', 300), (4, 'dave', 400)`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestPrimaryKeyOverride sets up a table with a primary key, but
// then overrides that via the catalog configuration.
func TestPrimaryKeyOverride(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, name TEXT, value INTEGER)`)
	// Names out of id order: charlie(1), alice(2), bob(3), dave(4)
	// By name: alice, bob, charlie, dave -> ids 2, 3, 1, 4
	db.Exec(t, `INSERT INTO <NAME> VALUES (1, 'charlie', 100), (2, 'alice', 200)`)
	tc.Discover("Discover Tables")
	require.NoError(t, tc.Capture.EditConfig("advanced.backfill_chunk_size", 1))
	require.NoError(t, tc.Capture.EditBinding(0, "resource.primary_key", []string{"name"}))
	require.NoError(t, tc.Capture.EditCollection(db.Expand("<ID>"), "key", []string{"/name"}))
	tc.Run("Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (3, 'bob', 300), (4, 'dave', 400)`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}
