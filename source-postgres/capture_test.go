package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/jackc/pglogrepl"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// TestReplicaIdentity exercises the 'REPLICA IDENTITY' setting of a table,
// which controls whether change events include full row contents or just the
// primary keys of the "before" state.
func TestReplicaIdentity(t *testing.T) {
	var db, tc = postgresBlackboxSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'A'), (1, 'bbb'), (2, 'CDEFGH'), (3, 'Three'), (4, 'Four')`)
	tc.Discover("Discover Test Table")
	tc.Run("Initial Backfill", -1)

	// Default REPLICA IDENTITY logs only the old primary key for deletions and updates.
	db.Exec(t, `DELETE FROM <NAME> WHERE id = 1`)
	db.Exec(t, `UPDATE <NAME> SET data = 'UPDATED' WHERE id = 2`)
	tc.Run("Default Replica Identity", -1)

	// Increase to REPLICA IDENTITY FULL, and repeat. Expect to see complete modified tuples logged.
	db.Exec(t, `ALTER TABLE <NAME> REPLICA IDENTITY FULL`)
	db.Exec(t, `DELETE FROM <NAME> WHERE id = 3`)
	db.Exec(t, `UPDATE <NAME> SET data = 'UPDATED' WHERE id = 4`)
	tc.Run("Replica Identity Full", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestToastColumns(t *testing.T) {
	var db, tc = postgresBlackboxSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, other INTEGER, data TEXT)`)

	// Table is created with REPLICA IDENTITY DEFAULT, which does *not* include
	// unchanged TOAST fields within the replication log. Postgres will attempt
	// to compress values by default. Tell it to use TOAST but disable
	// compression so our fixture spills out of the table.
	db.Exec(t, `ALTER TABLE <NAME> ALTER COLUMN data SET STORAGE EXTERNAL`)

	// Create a data fixture which is (barely) larger that Postgres's desired inline storage size.
	const toastThreshold = 2048
	var data = strings.Repeat("data", toastThreshold/4)

	// Initial capture backfill.
	db.Exec(t, `INSERT INTO <NAME> VALUES (1, 32, 'smol')`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (2, 42, '`+data+`')`)
	tc.Discover("Discover Test Table")
	tc.Run("Initial Backfill", -1)

	// Insert TOAST value, update TOAST value, and change an unrelated value.
	db.Exec(t, `INSERT INTO <NAME> VALUES (3, 52, '`+data+`')`)               // Insert TOAST.
	db.Exec(t, `INSERT INTO <NAME> VALUES (4, 62, 'more smol')`)              // Insert non-TOAST.
	db.Exec(t, `UPDATE <NAME> SET data = 'UPDATE ONE `+data+`' WHERE id = 1`) // Update non-TOAST => TOAST.
	db.Exec(t, `UPDATE <NAME> SET data = 'UPDATE TWO `+data+`' WHERE id = 2`) // Update TOAST => TOAST.
	db.Exec(t, `UPDATE <NAME> SET data = 'UPDATE smol' WHERE id = 3`)         // Update TOAST => non-TOAST.
	db.Exec(t, `UPDATE <NAME> SET other = 72 WHERE id = 1`)                   // Update other (TOAST); data _not_ expected.
	db.Exec(t, `UPDATE <NAME> SET other = 82 WHERE id = 3`)                   // Update other (non-TOAST).
	tc.Run("Default Replica Identity", -1)

	db.Exec(t, `ALTER TABLE <NAME> REPLICA IDENTITY FULL`)
	db.Exec(t, `UPDATE <NAME> SET other = 92 WHERE id = 1`)                   // Update other (TOAST); data *is* expected.
	db.Exec(t, `UPDATE <NAME> SET other = 102 WHERE id = 3`)                  // Update other (non-TOAST).
	db.Exec(t, `UPDATE <NAME> SET data = 'smol smol' WHERE id = 1`)           // Update TOAST => non-TOAST.
	db.Exec(t, `UPDATE <NAME> SET data = 'UPDATE SIX `+data+`' WHERE id = 4`) // Update non-TOAST => TOAST.
	tc.Run("Replica Identity Full", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestSlotLSNAdvances checks that the `restart_lsn` of a replication slot
// advances during normal connector operation.
func TestSlotLSNAdvances(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	var db, tc = postgresBlackboxSetup(t)

	// Disable SHUTDOWN_AFTER_POLLING so the capture runs indefinitely until cancelled
	//
	// TODO(wgd): This is terrible and hacky and we should probably handle environment
	// variables on captures better.
	os.Unsetenv("SHUTDOWN_AFTER_POLLING")
	t.Cleanup(func() { os.Setenv("SHUTDOWN_AFTER_POLLING", "yes") })

	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	tc.Discover("Discover Table")

	const slotName = "flow_slot"

	// Start a "traffic generator" which will generate 2 QPS of inserts
	var ctx = context.Background()
	var trafficCtx, cancelTraffic = context.WithCancel(ctx)
	defer cancelTraffic()
	go func() {
		for i := 0; trafficCtx.Err() == nil; i++ {
			db.QuietExec(t, fmt.Sprintf(`INSERT INTO <NAME> VALUES (%d, 'Row %d')`, i, i))
			time.Sleep(500 * time.Millisecond)
		}
	}()
	time.Sleep(1 * time.Second)

	// Start the capture running in a separate goroutine with a cancellable context
	var captureCtx, cancelCapture = context.WithCancel(ctx)
	defer cancelCapture()
	go tc.Capture.RunWithContext(captureCtx, -1)
	time.Sleep(1 * time.Second)

	// Capture the current `restart_lsn` of the replication slot. At this point any
	// startup-related behavior should have stabilized, so if the slot LSN changes
	// again this will demonstrate that we're advancing it reliably.
	var initialLSN pglogrepl.LSN
	db.QueryRow(t, fmt.Sprintf(`SELECT restart_lsn FROM pg_catalog.pg_replication_slots WHERE slot_name = '%s'`, slotName), &initialLSN)

	// Periodically check whether the slot's `restart_lsn` has updated. Since we're
	// sending 'Standby Status Update' messages every 10 seconds we'll check every
	// 2s but give it up to a minute to succeed (note that this is worst-case time,
	// the test will end as soon as the LSN advances -- typically after 12-22s)
	const (
		passDeadline = 60 * time.Second
		pollInterval = 2 * time.Second
	)
	for i := 0; i < int(passDeadline/pollInterval); i++ {
		var currentLSN pglogrepl.LSN
		db.QueryRow(t, fmt.Sprintf(`SELECT restart_lsn FROM pg_catalog.pg_replication_slots WHERE slot_name = '%s'`, slotName), &currentLSN)
		logrus.WithFields(logrus.Fields{"initial": initialLSN, "current": currentLSN}).Info("checking slot LSN")
		if currentLSN > initialLSN {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Errorf("slot %q restart LSN failed to advance after %s", slotName, passDeadline.String())
}

func TestViewDiscovery(t *testing.T) {
	var db, tc = postgresBlackboxSetup(t)

	var cleanup = func() {
		db.QuietExec(t, `DROP VIEW IF EXISTS <NAME>_simpleview`)
		db.QuietExec(t, `DROP MATERIALIZED VIEW IF EXISTS <NAME>_matview`)
	}
	cleanup()
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, grp INTEGER, data TEXT)`)
	db.Exec(t, `CREATE VIEW <NAME>_simpleview AS SELECT id, data FROM <NAME> WHERE grp = 1`)
	db.Exec(t, `CREATE MATERIALIZED VIEW <NAME>_matview AS SELECT id, data FROM <NAME> WHERE grp = 2`)
	t.Cleanup(cleanup)

	var discovery = tc.DiscoverFull("Discover Tables")
	for _, binding := range discovery {
		if strings.Contains(string(binding), "_simpleview") {
			t.Errorf("view returned by catalog discovery")
		}
		if strings.Contains(string(binding), "_matview") {
			t.Errorf("materialized view returned by catalog discovery")
		}
	}
}

func TestSkipBackfills(t *testing.T) {
	var db, tc = postgresBlackboxSetup(t)
	db.CreateTable(t, `<NAME>_a`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.CreateTable(t, `<NAME>_b`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.CreateTable(t, `<NAME>_c`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.Exec(t, `INSERT INTO <NAME>_a VALUES (1, 'one'), (2, 'two')`)
	db.Exec(t, `INSERT INTO <NAME>_b VALUES (3, 'three'), (4, 'four')`)
	db.Exec(t, `INSERT INTO <NAME>_c VALUES (5, 'five'), (6, 'six')`)
	tc.Discover("Discover Tables")

	// Skip A and C, only B will be backfilled
	require.NoError(t, tc.Capture.EditConfig("advanced.skip_backfills", db.Expand(`<NAME>_a,<NAME>_c`)))
	tc.Run("Initial Backfill (Only B)", -1)

	// All three tables should see replication events
	db.Exec(t, `INSERT INTO <NAME>_a VALUES (7, 'seven'), (8, 'eight')`)
	db.Exec(t, `INSERT INTO <NAME>_b VALUES (9, 'nine'), (10, 'ten')`)
	db.Exec(t, `INSERT INTO <NAME>_c VALUES (11, 'eleven'), (12, 'twelve')`)
	tc.Run("Replication (All Tables)", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestTruncatedTables exercises table truncation behavior.
// Currently truncation is ignored and further changes are replicated normally.
func TestTruncatedTables(t *testing.T) {
	var db, tc = postgresBlackboxSetup(t)

	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (1, 'one'), (2, 'two')`)
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill", -1)

	db.Exec(t, `INSERT INTO <NAME> VALUES (3, 'three'), (4, 'four')`)
	tc.Run("Normal Replication", -1)

	db.Exec(t, `TRUNCATE TABLE <NAME>`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (5, 'five'), (6, 'six')`)
	tc.Run("After Truncation", -1)

	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestTrickyColumnNames exercises the capture of a table with difficult column names.
func TestTrickyColumnNames(t *testing.T) {
	var db, tc = postgresBlackboxSetup(t)
	db.CreateTable(t, `<NAME>`, `("Meta/""wtf""~ID" INTEGER PRIMARY KEY, "table" TEXT, data TEXT)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (1, 'one', 'aaa'), (2, 'two', 'bbb')`)
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (3, 'three', 'eee'), (4, 'four', 'fff')`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestCursorResume sets up a capture with a (string, int) primary key and a backfill
// chunk size of 1 row, so that every row backfilled goes through FDB key serialization.
func TestCursorResume(t *testing.T) {
	var db, tc = postgresBlackboxSetup(t)
	db.CreateTable(t, `<NAME>`, `(epoch VARCHAR(8), count INTEGER, data TEXT, PRIMARY KEY (epoch, count))`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		('aaa', 1, 'bvzf'), ('aaa', 2, 'ukwh'), ('aaa', 3, 'lntg'), ('bbb', -100, 'bycz'),
		('bbb', 2, 'ajgp'), ('bbb', 333, 'zljj'), ('bbb', 4096, 'lhnw'), ('bbb', 800000, 'iask'),
		('ccc', 1234, 'bikh'), ('ddd', -10000, 'dhqc'), ('x', 1, 'djsf'), ('y', 1, 'iwnx'),
		('z', 1, 'qmjp'), ('', 0, 'xakg'), ('', -1, 'kvxr'), ('   ', 3, 'gboj')`)
	require.NoError(t, tc.Capture.EditConfig("advanced.backfill_chunk_size", 1))
	tc.Discover("Discover Tables")
	tc.Run("Backfill Data", -1) // Run until the connector decides to shut down
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestComplexDataset tests the interaction between backfill and replication when the
// connector restarts mid-backfill. It features a composite primary key with mixed types
// (VARCHAR, INTEGER), a dataset divided across multiple backfill chunks, checkpoint
// manipulation to simulate restarts at specific points, and "concurrent" changes
// to both already-scanned and not-yet-scanned row ranges.
//
// The key behaviors being verified:
//   - Rows modified BEFORE the backfill cursor appear via replication
//   - Rows modified AFTER the backfill cursor are filtered (picked up in backfill)
//   - The backfill correctly resumes from the cursor position
func TestComplexDataset(t *testing.T) {
	var db, tc = postgresBlackboxSetup(t)

	// Generate 25 rows: 5 groups (A-E) × 5 ids (0-4) each
	db.CreateTable(t, `<NAME>`, `(grp VARCHAR(8), id INTEGER, data TEXT, PRIMARY KEY (grp, id))`)
	db.Exec(t, `INSERT INTO <NAME>
		SELECT chr(65 + grp), id, chr(65 + grp) || '_' || id
		FROM generate_series(0, 4) grp, generate_series(0, 4) id`)

	// Configure small backfill chunk size to create multiple chunks
	require.NoError(t, tc.Capture.EditConfig("advanced.backfill_chunk_size", 3))
	tc.Discover("Discover Tables")

	// Initial backfill runs to completion
	tc.Run("Initial Backfill", -1)

	// Rewind checkpoint to simulate restart at cursor position ('B', 4).
	// This means rows (A,0) through (B,4) have been "scanned" (10 rows).
	var streamKey = db.Expand(`bindingStateV1.<SCHEMA>%2Fcomplexdataset_<ID>`)
	require.NoError(t, tc.Capture.EditCheckpoint(streamKey+".backfilled", 10))
	require.NoError(t, tc.Capture.EditCheckpoint(streamKey+".mode", "UnfilteredBackfill"))
	require.NoError(t, tc.Capture.EditCheckpoint(streamKey+".scanned", "AkIAFQQ=")) // base64 of FDB tuple ('B', 4)

	// Insert a row BEFORE cursor (grp='A'): should appear via replication
	db.Exec(t, `INSERT INTO <NAME> VALUES ('A', 99, 'inserted_before_cursor')`)

	// Insert a row AFTER cursor (grp='D'): will be picked up in backfill, filtered from replication
	db.Exec(t, `INSERT INTO <NAME> VALUES ('D', 99, 'inserted_after_cursor')`)

	// Update a row BEFORE cursor (grp='B', id=2): should appear via replication
	db.Exec(t, `UPDATE <NAME> SET data = 'updated_before_cursor' WHERE grp = 'B' AND id = 2`)

	// Update a row AFTER cursor (grp='D', id=2): will be filtered, picked up fresh in backfill
	db.Exec(t, `UPDATE <NAME> SET data = 'updated_after_cursor' WHERE grp = 'D' AND id = 2`)

	// Delete a row BEFORE cursor (grp='A', id=3): should appear via replication
	db.Exec(t, `DELETE FROM <NAME> WHERE grp = 'A' AND id = 3`)

	// Delete a row AFTER cursor (grp='E', id=3): will be filtered (row won't exist in backfill)
	db.Exec(t, `DELETE FROM <NAME> WHERE grp = 'E' AND id = 3`)

	// Resume capture - should see replication events for "before cursor" changes,
	// then backfill from ('B',4) onwards picking up the "after cursor" changes
	tc.Run("Resume After Checkpoint Rewind", -1)

	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestUserTypes exercises discovery and capture of tables using various user-defined types.
func TestUserTypes(t *testing.T) {
	t.Run("Domain", func(t *testing.T) {
		var db, tc = postgresBlackboxSetup(t)
		db.QuietExec(t, `DROP DOMAIN IF EXISTS UserDomain CASCADE`)
		db.Exec(t, `CREATE DOMAIN UserDomain AS TEXT`)
		t.Cleanup(func() { db.QuietExec(t, `DROP DOMAIN IF EXISTS UserDomain CASCADE`) })

		db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, value UserDomain)`)
		tc.DiscoverFull("Discover Tables")
		db.Exec(t, `INSERT INTO <NAME> VALUES (1, 'hello'), (2, 'world')`)
		tc.Run("Initial Backfill", -1)
		db.Exec(t, `INSERT INTO <NAME> VALUES (3, 'foo'), (4, 'bar'), (5, 'baz')`)
		tc.Run("Replication", -1)
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})

	t.Run("Enum", func(t *testing.T) {
		var db, tc = postgresBlackboxSetup(t)
		db.QuietExec(t, `DROP TYPE IF EXISTS UserEnum CASCADE`)
		db.Exec(t, `CREATE TYPE UserEnum AS ENUM ('red', 'green', 'blue')`)
		t.Cleanup(func() { db.QuietExec(t, `DROP TYPE IF EXISTS UserEnum CASCADE`) })

		db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, value UserEnum)`)
		tc.DiscoverFull("Discover Tables")
		db.Exec(t, `INSERT INTO <NAME> VALUES (1, 'red'), (2, 'green'), (3, 'blue')`)
		tc.Run("Initial Backfill", -1)
		db.Exec(t, `INSERT INTO <NAME> VALUES (4, 'blue'), (5, 'red'), (6, 'green')`)
		tc.Run("Replication", -1)
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})

	t.Run("Tuple", func(t *testing.T) {
		var db, tc = postgresBlackboxSetup(t)
		db.QuietExec(t, `DROP TYPE IF EXISTS UserTuple CASCADE`)
		db.Exec(t, `CREATE TYPE UserTuple AS (epoch INTEGER, count INTEGER, data TEXT)`)
		t.Cleanup(func() { db.QuietExec(t, `DROP TYPE IF EXISTS UserTuple CASCADE`) })

		db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, value UserTuple)`)
		tc.DiscoverFull("Discover Tables")
		db.Exec(t, `INSERT INTO <NAME> VALUES (1, '(1234, 5678, ''hello'')'), (2, '(3456, 9876, ''world'')')`)
		tc.Run("Initial Backfill", -1)
		db.Exec(t, `INSERT INTO <NAME> VALUES (3, '(34, 64, ''asdf'')'), (4, '(83, 12, ''fdsa'')')`)
		tc.Run("Replication", -1)
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})

	t.Run("Range", func(t *testing.T) {
		var db, tc = postgresBlackboxSetup(t)
		db.QuietExec(t, `DROP TYPE IF EXISTS UserRange CASCADE`)
		db.Exec(t, `CREATE TYPE UserRange AS RANGE (subtype = int4)`)
		t.Cleanup(func() { db.QuietExec(t, `DROP TYPE IF EXISTS UserRange CASCADE`) })

		db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, value UserRange)`)
		tc.DiscoverFull("Discover Tables")
		db.Exec(t, `INSERT INTO <NAME> VALUES (1, '(1, 2]'), (2, '[3,)')`)
		tc.Run("Initial Backfill", -1)
		db.Exec(t, `INSERT INTO <NAME> VALUES (3, '(,4]'), (4, '[5,6)')`)
		tc.Run("Replication", -1)
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
}

// TestCaptureCapitalization exercises tables with quoted names containing capital letters.
func TestCaptureCapitalization(t *testing.T) {
	var db, tc = postgresBlackboxSetup(t)

	var cleanup = func() {
		db.QuietExec(t, `DROP TABLE IF EXISTS "<SCHEMA>"."tbl_<ID>_AAaaAA"`)
		db.QuietExec(t, `DROP TABLE IF EXISTS "<SCHEMA>"."tbl_<ID>_bbBBbb"`)
	}
	cleanup()
	t.Cleanup(cleanup)

	db.Exec(t, `CREATE TABLE "<SCHEMA>"."tbl_<ID>_AAaaAA" (id INTEGER PRIMARY KEY, data TEXT)`)
	db.Exec(t, `CREATE TABLE "<SCHEMA>"."tbl_<ID>_bbBBbb" (id INTEGER PRIMARY KEY, data TEXT)`)
	db.Exec(t, `INSERT INTO "<SCHEMA>"."tbl_<ID>_AAaaAA" VALUES (0, 'hello'), (1, 'asdf');`)
	db.Exec(t, `INSERT INTO "<SCHEMA>"."tbl_<ID>_bbBBbb" VALUES (2, 'world'), (3, 'fdsa');`)

	tc.Discover("Discover Tables")
	tc.Run("Backfill Data", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestCaptureOversizedFields(t *testing.T) {
	// The string "datadatadata" over and over, ending with "ABCD|EFGH...", where | represents the text truncation boundary
	var largeText = strings.Repeat("data", (truncateColumnThreshold/4)-1) + "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	var largeJSON = fmt.Sprintf(`{"text": "%s"}`, largeText)

	var db, tc = postgresBlackboxSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, v_text TEXT, v_bytea BYTEA, v_json JSON, v_jsonb JSONB)`)
	db.QuietExec(t, `INSERT INTO <NAME> (id, v_text)  VALUES (10, '`+largeText+`')`)
	db.QuietExec(t, `INSERT INTO <NAME> (id, v_bytea) VALUES (11, '`+largeText+`')`)
	db.QuietExec(t, `INSERT INTO <NAME> (id, v_json)  VALUES (12, '`+largeJSON+`')`)
	db.QuietExec(t, `INSERT INTO <NAME> (id, v_jsonb) VALUES (13, '`+largeJSON+`')`)
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill", -1)
	db.QuietExec(t, `INSERT INTO <NAME> (id, v_text)  VALUES (20, '`+largeText+`')`)
	db.QuietExec(t, `INSERT INTO <NAME> (id, v_bytea) VALUES (21, '`+largeText+`')`)
	db.QuietExec(t, `INSERT INTO <NAME> (id, v_json)  VALUES (22, '`+largeJSON+`')`)
	db.QuietExec(t, `INSERT INTO <NAME> (id, v_jsonb) VALUES (23, '`+largeJSON+`')`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestCaptureAfterSlotDropped(t *testing.T) {
	var db, tc = postgresBlackboxSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)

	// Run a normal capture
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'zero'), (1, 'one')`)
	tc.Discover("Discover Tables")
	tc.Run("Initial Capture", -1)

	// Drop the replication slot while the task is offline. At startup it should
	// fail because it has a non-empty resume cursor but the slot no longer exists.
	db.Exec(t, `INSERT INTO <NAME> VALUES (2, 'two'), (3, 'three')`)
	db.Exec(t, `SELECT pg_drop_replication_slot('flow_slot')`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (4, 'four'), (5, 'five')`)
	tc.Run("Capture After Slot Dropped (Should Fail)", -1)

	// A subsequent capture run should still be failing since we haven't fixed it.
	db.Exec(t, `INSERT INTO <NAME> VALUES (6, 'six'), (7, 'seven')`)
	tc.Run("Capture Still Failing", -1)

	// Bump the backfill counter to trigger a fresh backfill with a new replication slot.
	require.NoError(t, tc.Capture.EditBinding(0, "backfill", 1))
	tc.Run("Capture After Backfill Counter Bumped", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestCaptureDomainJSONB exercises an edge case where a user-defined domain type
// has a concrete type which uses a custom decoder registered in the PGX type map.
func TestCaptureDomainJSONB(t *testing.T) {
	var db, tc = postgresBlackboxSetup(t)
	db.QuietExec(t, `DROP DOMAIN IF EXISTS UserDomain CASCADE`)
	db.Exec(t, `CREATE DOMAIN UserDomain AS JSONB`)
	t.Cleanup(func() { db.QuietExec(t, `DROP DOMAIN IF EXISTS UserDomain CASCADE`) })

	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data UserDomain NOT NULL)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, '{}'), (1, '{"foo": "bar"}')`)
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (2, '{"baz": [1, 2, 3]}'), (3, '{"asdf": {"a": 1, "b": 2}}')`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestDroppedAndRecreatedTable(t *testing.T) {
	var db, tc = postgresBlackboxSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	tc.Discover("Discover Tables")
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'zero'), (1, 'one')`)
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (2, 'two'), (3, 'three')`)
	tc.Run("Some Replication", -1)
	db.Exec(t, `DROP TABLE <NAME>`)
	db.Exec(t, `CREATE TABLE <NAME> (id INTEGER PRIMARY KEY, data TEXT)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'zero'), (1, 'one')`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (4, 'four'), (5, 'five')`)
	tc.Run("Dropped and Recreated", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (6, 'six'), (7, 'seven')`)
	tc.Run("More Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestCIText(t *testing.T) {
	var db, tc = postgresBlackboxSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data CITEXT, arr CITEXT[])`)
	tc.DiscoverFull("Discover Tables")
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'zero', '{a,b}'), (1, 'one', '{c,d}')`)
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (2, 'two', '{e,f}'), (3, 'three', '{g,h}')`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestPrimaryKeyUpdate(t *testing.T) {
	var db, tc = postgresBlackboxSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	tc.Discover("Discover Tables")
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'zero'), (1, 'one')`)
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (3, 'three'), (4, 'four')`)
	tc.Run("Replication", -1)
	db.Exec(t, `UPDATE <NAME> SET id = 5 WHERE id = 1`)
	db.Exec(t, `UPDATE <NAME> SET id = 6 WHERE id = 4`)
	tc.Run("Primary Key Updates", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestGeneratedColumn(t *testing.T) {
	var db, tc = postgresBlackboxSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, a VARCHAR(32), b VARCHAR(32), generated VARCHAR(64) GENERATED ALWAYS AS (COALESCE(a, b)) STORED)`)
	tc.DiscoverFull("Discover Tables")
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'a0', 'b0'), (1, null, 'b1'), (2, 'a2', null), (3, null, null)`)
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (4, 'a4', 'b4'), (5, null, 'b5'), (6, 'a6', null), (7, null, null)`)
	tc.Run("Replication Inserts", -1)
	db.Exec(t, `UPDATE <NAME> SET a = 'a-modified' WHERE id IN (4, 6)`)
	db.Exec(t, `UPDATE <NAME> SET b = 'b-modified' WHERE id = 5`)
	tc.Run("Updates", -1)
	db.Exec(t, `DELETE FROM <NAME> WHERE id IN (4, 5, 6, 7)`)
	tc.Run("Deletes", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestFeatureFlagFlattenArrays exercises the handling of array columns in both
// discovery and captures, with the 'flatten_arrays' feature flag explicitly
// enabled and disabled.
func TestFeatureFlagFlattenArrays(t *testing.T) {
	for _, testCase := range []struct {
		name string
		flag string
	}{
		{"Enabled", "flatten_arrays"},
		{"Disabled", "no_flatten_arrays"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var db, tc = postgresBlackboxSetup(t)
			db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, text_array TEXT[], int_array INTEGER[], nested_array INTEGER[][])`)
			db.Exec(t, `INSERT INTO <NAME> VALUES
				(1, ARRAY['a','b','c'], ARRAY[1,2,3], ARRAY[[1,2],[3,4]]),
				(2, ARRAY['foo','bar'], ARRAY[10,20], ARRAY[[5,6],[7,8]]),
				(3, '{}', '{}', ARRAY[[]]::INTEGER[][]),
				(4, NULL, NULL, NULL)`)

			require.NoError(t, tc.Capture.EditConfig("advanced.feature_flags", testCase.flag))
			tc.DiscoverFull("Discover Tables")
			tc.Run("Capture", -1)
			cupaloy.SnapshotT(t, tc.Transcript.String())
		})
	}
}

func TestXMINBackfill(t *testing.T) {
	var db, tc = postgresBlackboxSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)

	// Insert some initial rows, then establish the current server XID.
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'zero'), (1, 'one'), (2, 'two')`)

	var lowerXID uint64
	db.QueryRow(t, `SELECT txid_snapshot_xmin(txid_current_snapshot())`, &lowerXID)

	// Changes from after the minimum backfill XID can be observed.
	db.Exec(t, `INSERT INTO <NAME> VALUES (3, 'three'), (4, 'four'), (5, 'five')`)
	db.Exec(t, `DELETE FROM <NAME> WHERE id = 0`) // Except this delete won't be, since it's a delete
	db.Exec(t, `UPDATE <NAME> SET data = 'one-modified' WHERE id = 1`)

	// Configure the minimum backfill XID and run the capture
	require.NoError(t, tc.Capture.EditConfig("advanced.min_backfill_xid", fmt.Sprintf("%d", uint32(lowerXID))))
	tc.Discover("Discover Tables")
	tc.Run("Backfill With XMIN Filter", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestMultidimensionalArrays(t *testing.T) {
	var db, tc = postgresBlackboxSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, arr TEXT[])`)

	require.NoError(t, tc.Capture.EditConfig("advanced.feature_flags", "multidimensional_arrays"))
	tc.DiscoverFull("Discover Tables")

	// Backfill capture with various array shapes
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, NULL),
		(2, '{}'),
		(3, '{}'),
		(4, '{x}'),
		(5, '{{{x}}}'),
		(6, '{a,b,c,d}'),
		(7, '{{a,b},{c,d}}'),
		(8, '{{{a,b,c,d},{e,f,g,h},{i,j,k,l}},{{m,n,o,p},{q,r,s,t},{u,v,w,x}}}'),
		(9, '{{{{a,b},{c,d}},{{e,f},{g,h}},{{i,j},{k,l}}},{{{m,n},{o,p}},{{q,r},{s,t}},{{u,v},{w,x}}}}')`)
	tc.Run("Backfill", -1)

	// Replication capture with the same array shapes
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(11, NULL),
		(12, '{}'),
		(13, '{}'),
		(14, '{x}'),
		(15, '{{{x}}}'),
		(16, '{a,b,c,d}'),
		(17, '{{a,b},{c,d}}'),
		(18, '{{{a,b,c,d},{e,f,g,h},{i,j,k,l}},{{m,n,o,p},{q,r,s,t},{u,v,w,x}}}'),
		(19, '{{{{a,b},{c,d}},{{e,f},{g,h}},{{i,j},{k,l}}},{{{m,n},{o,p}},{{q,r},{s,t}},{{u,v},{w,x}}}}')`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestFeatureFlagEmitSourcedSchemas runs a capture with the `emit_sourced_schemas` feature flag set.
func TestFeatureFlagEmitSourcedSchemas(t *testing.T) {
	for _, testCase := range []struct {
		name string
		flag string
	}{
		{"Default", ""},
		{"Enabled", "emit_sourced_schemas"},
		{"Disabled", "no_emit_sourced_schemas"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var db, tc = postgresBlackboxSetup(t)
			db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data VARCHAR(32))`)
			db.Exec(t, `INSERT INTO <NAME> VALUES (1, 'hello'), (2, 'world')`)

			if testCase.flag != "" {
				require.NoError(t, tc.Capture.EditConfig("advanced.feature_flags", testCase.flag))
			}
			tc.Discover("Discover Tables")
			tc.Run("Capture", -1)
			cupaloy.SnapshotT(t, tc.Transcript.String())
		})
	}
}

// TestPartitionedCTIDBackfill tests the handling of partitioned tables with CTID-based backfill.
func TestPartitionedCTIDBackfill(t *testing.T) {
	var db, tc = postgresBlackboxSetup(t)

	// Create a partitioned table with three partitions
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT) PARTITION BY RANGE (id)`)
	db.Exec(t, `CREATE TABLE <NAME>_1 PARTITION OF <NAME> FOR VALUES FROM (0) TO (10)`)
	db.Exec(t, `CREATE TABLE <NAME>_2 PARTITION OF <NAME> FOR VALUES FROM (10) TO (20)`)
	db.Exec(t, `CREATE TABLE <NAME>_3 PARTITION OF <NAME> FOR VALUES FROM (20) TO (30)`)
	t.Cleanup(func() {
		db.QuietExec(t, `DROP TABLE IF EXISTS <NAME>_1`)
		db.QuietExec(t, `DROP TABLE IF EXISTS <NAME>_2`)
		db.QuietExec(t, `DROP TABLE IF EXISTS <NAME>_3`)
	})

	// Insert test rows spanning all three partitions
	db.Exec(t, `INSERT INTO <NAME> SELECT n, 'data value ' || n FROM generate_series(1, 29) AS n`)

	// Configure for CTID-based backfill (without primary key)
	require.NoError(t, tc.Capture.EditConfig("advanced.backfill_chunk_size", 5))
	tc.Discover("Discover Tables")
	require.NoError(t, tc.Capture.EditBinding(0, "resource.mode", "Without Primary Key"))
	tc.Run("CTID Backfill", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestCaptureAsPartitions verifies that the "Capture Partitioned Tables as Partitions" advanced setting works as intended.
func TestCaptureAsPartitions(t *testing.T) {
	var db, tc = postgresBlackboxSetup(t)

	// Create a partitioned table with quarterly partitions
	db.CreateTable(t, `<NAME>`, `(logdate DATE PRIMARY KEY, value TEXT) PARTITION BY RANGE (logdate)`)
	var cleanup = func() {
		db.QuietExec(t, `DROP TABLE IF EXISTS <NAME>_2023q1`)
		db.QuietExec(t, `DROP TABLE IF EXISTS <NAME>_2023q2`)
		db.QuietExec(t, `DROP TABLE IF EXISTS <NAME>_2023q3`)
		db.QuietExec(t, `DROP TABLE IF EXISTS <NAME>_2023q4`)
	}
	cleanup()
	db.Exec(t, `CREATE TABLE <NAME>_2023q1 PARTITION OF <NAME> FOR VALUES FROM ('2023-01-01') TO ('2023-04-01')`)
	db.Exec(t, `CREATE TABLE <NAME>_2023q2 PARTITION OF <NAME> FOR VALUES FROM ('2023-04-01') TO ('2023-07-01')`)
	db.Exec(t, `CREATE TABLE <NAME>_2023q3 PARTITION OF <NAME> FOR VALUES FROM ('2023-07-01') TO ('2023-10-01')`)
	db.Exec(t, `CREATE TABLE <NAME>_2023q4 PARTITION OF <NAME> FOR VALUES FROM ('2023-10-01') TO ('2024-01-01')`)
	t.Cleanup(cleanup)

	// Recreate the publication without the `publish_via_partition_root` flag for this test only
	t.Cleanup(func() {
		db.QuietExec(t, `DROP PUBLICATION IF EXISTS flow_publication`)
		db.QuietExec(t, `CREATE PUBLICATION flow_publication FOR ALL TABLES`)
		db.QuietExec(t, `ALTER PUBLICATION flow_publication SET (publish_via_partition_root = true)`)
	})
	db.QuietExec(t, `DROP PUBLICATION IF EXISTS flow_publication`)
	db.QuietExec(t, `CREATE PUBLICATION flow_publication FOR ALL TABLES`)

	// Enable CaptureAsPartitions and discover
	require.NoError(t, tc.Capture.EditConfig("advanced.capture_as_partitions", true))
	tc.DiscoverFull("Discover Partitions")

	// Insert test data into the partitions for backfill
	db.Exec(t, `INSERT INTO <NAME> VALUES
		('2023-01-15', 'Q1 data 1'),
		('2023-02-20', 'Q1 data 2'),
		('2023-05-10', 'Q2 data 1'),
		('2023-06-15', 'Q2 data 2'),
		('2023-08-05', 'Q3 data 1'),
		('2023-09-25', 'Q3 data 2'),
		('2023-11-12', 'Q4 data 1'),
		('2023-12-28', 'Q4 data 2')`)
	tc.Run("Backfill", -1)

	// Insert more test data into the partitions for replication
	db.Exec(t, `INSERT INTO <NAME> VALUES
		('2023-03-01', 'Q1 replication'),
		('2023-04-01', 'Q2 replication'),
		('2023-07-01', 'Q3 replication'),
		('2023-10-01', 'Q4 replication')`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestUnpairedSurrogatesInJSON tests the handling of unpaired surrogate codepoints
// inside of JSON values. Because apparently those will be emitted as-is by the Go
// JSON serializer when present in a json.RawMessage but are considered an error by
// Rust's serde_json. We have logic which sanitizes these.
//
// As far as I'm aware this can only occur with a JSON column using \uXXXX escapes.
// Values containing the raw UTF-8 bytes representing those codepoints are rejected
// by the database, and similarly JSONB columns decode the escapes and reject them.
func TestUnpairedSurrogatesInJSON(t *testing.T) {
	var db, tc = postgresBlackboxSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data JSON)`)

	// Backfill inserts with unpaired surrogates in JSON
	db.Exec(t, `INSERT INTO <NAME> VALUES (100, '{"text": "normal"}')`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (101, '{"text": "\ud83d\u200b\ude14"}')`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (102, '{"text": "\uDeAd"}')`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (103, '{"\uDeAd": "\ud83d\udE14"}')`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (104, '{"text": "foo \uDEAD bar \uDEAD baz"}')`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (105, '[{"type":"text","text":"foo \"bar\\udfs\" /baz"}]')`)
	tc.Discover("Discover Tables")
	tc.Run("Backfill", -1)

	// Replication inserts with the same values
	db.Exec(t, `INSERT INTO <NAME> VALUES (200, '{"text": "normal"}')`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (201, '{"text": "\ud83d\u200b\ude14"}')`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (202, '{"text": "\uDeAd"}')`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (203, '{"\uDeAd": "\ud83d\udE14"}')`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (204, '{"text": "foo \uDEAD bar \uDEAD baz"}')`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (205, '[{"type":"text","text":"foo \"bar\\udfs\" /baz"}]')`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestSourceTag verifies the output of a capture with /advanced/source_tag set
func TestSourceTag(t *testing.T) {
	var db, tc = postgresBlackboxSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	require.NoError(t, tc.Capture.EditConfig("advanced.source_tag", "example_source_tag_1234"))
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'zero'), (1, 'one')`)
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (2, 'two'), (3, 'three')`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestBackfillPriority checks that tables with higher priority values are
// backfilled completely before tables with lower priority values.
func TestBackfillPriority(t *testing.T) {
	var db, tc = postgresBlackboxSetup(t)

	// Create five tables with 5 rows each
	db.CreateTable(t, `<NAME>_def`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.CreateTable(t, `<NAME>_hi1`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.CreateTable(t, `<NAME>_hi2`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.CreateTable(t, `<NAME>_lo1`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.CreateTable(t, `<NAME>_lo2`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.Exec(t, `INSERT INTO <NAME>_def SELECT n, 'Row ' || n || ' in def' FROM generate_series(0, 4) AS n`)
	db.Exec(t, `INSERT INTO <NAME>_hi1 SELECT n, 'Row ' || n || ' in hi1' FROM generate_series(0, 4) AS n`)
	db.Exec(t, `INSERT INTO <NAME>_hi2 SELECT n, 'Row ' || n || ' in hi2' FROM generate_series(0, 4) AS n`)
	db.Exec(t, `INSERT INTO <NAME>_lo1 SELECT n, 'Row ' || n || ' in lo1' FROM generate_series(0, 4) AS n`)
	db.Exec(t, `INSERT INTO <NAME>_lo2 SELECT n, 'Row ' || n || ' in lo2' FROM generate_series(0, 4) AS n`)
	tc.Discover("Discover Tables")

	// Assign priorities to the bindings (sorted alphabetically: def, hi1, hi2, lo1, lo2)
	// - Two tables with priority 10 (high priority)
	// - One table with default priority (0)
	// - Two tables with priority -10 (low priority)
	require.NoError(t, tc.Capture.EditBinding(1, "resource.priority", 10))  // hi1
	require.NoError(t, tc.Capture.EditBinding(2, "resource.priority", 10))  // hi2
	require.NoError(t, tc.Capture.EditBinding(3, "resource.priority", -10)) // lo1
	require.NoError(t, tc.Capture.EditBinding(4, "resource.priority", -10)) // lo2

	tc.Run("Backfill 1 (High Priority)", 5) // Run until first two tables are backfilled
	tc.Run("Backfill 2 (Default)", 4) // Run until third table is backfilled
	tc.Run("Backfill 3 (Low Priority)", -1) // Run until everything else is backfilled
	cupaloy.SnapshotT(t, tc.Transcript.String())
}
