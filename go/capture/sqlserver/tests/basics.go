package tests

import (
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

func TestBasics(t *testing.T, setup testSetupFunc) {
	t.Run("SimpleDiscovery", func(t *testing.T) { testSimpleDiscovery(t, setup) })
	t.Run("SimpleCapture", func(t *testing.T) { testSimpleCapture(t, setup) })
	t.Run("ReplicationInserts", func(t *testing.T) { testReplicationInserts(t, setup) })
	t.Run("ReplicationUpdates", func(t *testing.T) { testReplicationUpdates(t, setup) })
	t.Run("ReplicationDeletes", func(t *testing.T) { testReplicationDeletes(t, setup) })
	t.Run("EmptyTable", func(t *testing.T) { testEmptyTable(t, setup) })
	t.Run("IgnoredStreams", func(t *testing.T) { testIgnoredStreams(t, setup) })
	t.Run("MultipleStreams", func(t *testing.T) { testMultipleStreams(t, setup) })
	t.Run("MissingTable", func(t *testing.T) { testMissingTable(t, setup) })
	t.Run("PrimaryKeyOverride", func(t *testing.T) { testPrimaryKeyOverride(t, setup) })
}

// TestSimpleDiscovery exercises discovery of a single, simple table.
func testSimpleDiscovery(t *testing.T, setup testSetupFunc) {
	var db, tc = setup(t)
	db.CreateTable(t, `<NAME>`, `(a INTEGER PRIMARY KEY, b VARCHAR(2000), c REAL NOT NULL, d VARCHAR(255))`)
	tc.DiscoverFull("Discover Tables")
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestSimpleCapture exercises the simplest possible backfill of a small table.
func testSimpleCapture(t *testing.T, setup testSetupFunc) {
	var db, tc = setup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data VARCHAR(2000))`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'A'), (1, 'bbb'), (2, 'CDEFGH')`)
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestReplicationInserts runs two captures, where the first will perform the
// initial table scan and the second capture will use replication to receive
// additional inserts performed after the first capture.
func testReplicationInserts(t *testing.T, setup testSetupFunc) {
	var db, tc = setup(t)
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
func testReplicationUpdates(t *testing.T, setup testSetupFunc) {
	var db, tc = setup(t)
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
func testReplicationDeletes(t *testing.T, setup testSetupFunc) {
	var db, tc = setup(t)
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
func testEmptyTable(t *testing.T, setup testSetupFunc) {
	var db, tc = setup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data VARCHAR(2000))`)
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill (Empty)", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (1002, 'some'), (1000, 'more'), (1001, 'rows')`)
	tc.Run("Replication Inserts", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestIgnoredStreams checks that replicated changes are only reported
// for tables which are configured in the catalog.
func testIgnoredStreams(t *testing.T, setup testSetupFunc) {
	var db, tc = setup(t)
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
func testMultipleStreams(t *testing.T, setup testSetupFunc) {
	var db, tc = setup(t)
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
func testMissingTable(t *testing.T, setup testSetupFunc) {
	var db, tc = setup(t)
	db.CreateTable(t, `<NAME>_a`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.CreateTable(t, `<NAME>_b`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	tc.Discover("Discover Tables")
	db.Exec(t, `DROP TABLE <NAME>_b`)
	tc.Run("Capture", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestPrimaryKeyOverride sets up a table with a primary key, but
// then overrides that via the catalog configuration.
func testPrimaryKeyOverride(t *testing.T, setup testSetupFunc) {
	var db, tc = setup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, name VARCHAR(32), value INTEGER)`)
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
