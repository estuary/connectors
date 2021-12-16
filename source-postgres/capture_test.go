package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/connectors/sqlcapture/tests"
	"github.com/jackc/pglogrepl"
	"github.com/sirupsen/logrus"
)

// TestGeneric runs the generic sqlcapture test suite.
func TestGeneric(t *testing.T) {
	tests.Run(context.Background(), t, TestBackend)
}

// TestReplicaIdentity exercises the 'REPLICA IDENTITY' setting of a table,
// which controls whether change events include full row contents or just the
// primary keys of the "before" state.
func TestReplicaIdentity(t *testing.T) {
	var tb, ctx = TestBackend, context.Background()
	var tableName = tb.CreateTable(ctx, t, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	var catalog, state = tests.ConfiguredCatalog(tableName), sqlcapture.PersistentState{}

	tb.Insert(ctx, t, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}, {3, "Four"}, {4, "5"}})
	tests.VerifiedCapture(ctx, t, tb, &catalog, &state, "init")

	// Default REPLICA IDENTITY logs only the old primary key for deletions and updates.
	tb.Delete(ctx, t, tableName, "id", 1)
	tb.Update(ctx, t, tableName, "id", 2, "data", "UPDATED")

	// Increase to REPLICA IDENTITY FULL, and repeat. Expect to see complete modified tuples logged.
	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL;", tableName))
	tb.Delete(ctx, t, tableName, "id", 3)
	tb.Update(ctx, t, tableName, "id", 4, "data", "UPDATED")

	tests.VerifiedCapture(ctx, t, tb, &catalog, &state, "")
}

// TestComplexDataset tries to throw together a bunch of different bits of complexity
// to synthesize something vaguely "realistic". It features a multiple-column primary
// key, a dataset large enough that the initial table scan gets divided across many
// "chunks", two connector restarts at different points in the initial table scan, and
// some concurrent modifications to row ranges already-scanned and not-yet-scanned.
func TestComplexDataset(t *testing.T) {
	var tb, ctx = TestBackend, context.Background()
	var tableName = tb.CreateTable(ctx, t, "", "(year INTEGER, state TEXT, fullname TEXT, population INTEGER, PRIMARY KEY (year, state))")
	var catalog, state = tests.ConfiguredCatalog(tableName), sqlcapture.PersistentState{}

	tests.LoadCSV(ctx, t, tb, tableName, "statepop.csv", 0)
	var states = tests.VerifiedCapture(ctx, t, tb, &catalog, &state, "init")
	state = states[10] // Restart in between (1960, 'IA') and (1960, 'ID')

	tb.Insert(ctx, t, tableName, [][]interface{}{
		{1930, "XX", "No Such State", 1234},   // An insert prior to the first restart, which will be reported once replication begins
		{1970, "XX", "No Such State", 12345},  // An insert between the two restarts, which will be visible in the table scan and should be filtered during replication
		{1990, "XX", "No Such State", 123456}, // An insert after the second restart, which will be visible in the table scan and should be filtered during replication
	})
	states = tests.VerifiedCapture(ctx, t, tb, &catalog, &state, "restart1")
	state = states[6] // Restart in between (1980, 'SD') and (1980, 'TN')

	tb.Query(ctx, t, fmt.Sprintf("DELETE FROM %s WHERE state = 'XX';", tableName))
	tb.Insert(ctx, t, tableName, [][]interface{}{
		{1930, "XX", "No Such State", 1234},   // Deleting/reinserting this row will be reported since they happened after that portion of the table was scanned
		{1970, "XX", "No Such State", 12345},  // Deleting/reinserting this row will be reported since they happened after that portion of the table was scanned
		{1990, "XX", "No Such State", 123456}, // Deleting/reinserting this row will be filtered since this portion of the table has yet to be scanned
	})

	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL;", tableName))

	// We've scanned through (1980, 'IA'), and will see updates for N% states at that date or before,
	// and creations for N% state records after that date which reflect the update.
	tb.Query(ctx, t, fmt.Sprintf("UPDATE %s SET fullname = 'New ' || fullname WHERE state IN ('NJ', 'NY');", tableName))
	// We'll see a deletion since this row has already been scanned through.
	tb.Query(ctx, t, fmt.Sprintf("DELETE FROM %s WHERE state = 'XX' AND year = 1970;", tableName))

	tests.VerifiedCapture(ctx, t, tb, &catalog, &state, "restart2")
}

// TestSlotLSNAdvances checks that the `restart_lsn` of a replication slot
// eventually advances in response to connector restarts.
func TestSlotLSNAdvances(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	var tb, ctx, state = TestBackend, context.Background(), sqlcapture.PersistentState{}
	var table = tb.CreateTable(ctx, t, "one", "(id INTEGER PRIMARY KEY, data TEXT)")
	var catalog = tests.ConfiguredCatalog(table)

	var lsnQuery = `SELECT restart_lsn FROM pg_catalog.pg_replication_slots WHERE slot_name = $1;`
	var slotName = *TestReplicationSlot

	// Capture the current `restart_lsn` of the replication slot prior to our test
	var beforeLSN pglogrepl.LSN
	if err := TestDatabase.QueryRow(ctx, lsnQuery, slotName).Scan(&beforeLSN); err != nil {
		logrus.WithFields(logrus.Fields{"slot": slotName, "err": err}).Error("failed to query restart_lsn")
	}

	// Insert some data, get the initial table scan out of the way, and wait long enough
	// that the database must have written a new snapshot.
	//
	// PostgreSQL `BackgroundWriterMain()` will trigger `LogStandbySnapshot()` whenever
	// "important records" have been inserted in the WAL and >15s have elapsed since
	// the last such snapshot. We need this to happen because these snapshots include
	// an `XLOG_RUNNING_XACTS` record, which is the trigger for the mechanism allowing
	// a replication slot's `restart_lsn` to advance in response to StandbyStatusUpdate
	// messages.
	tb.Insert(ctx, t, table, [][]interface{}{{0, "zero"}, {1, "one"}, {2, "two"}})
	tests.VerifiedCapture(ctx, t, tb, &catalog, &state, "capture1")
	logrus.Info("waiting so a standby snapshot can occur")
	time.Sleep(20 * time.Second)

	// Perform a capture and then check if `restart_lsn` has advanced. If not,
	// keep trying until it does or the retry count is hit.
	const retryCount = 50
	for iter := 0; iter < retryCount; iter++ {
		tests.VerifiedCapture(ctx, t, tb, &catalog, &state, "captureN")

		var afterLSN pglogrepl.LSN
		if err := TestDatabase.QueryRow(ctx, lsnQuery, slotName).Scan(&afterLSN); err != nil {
			logrus.WithFields(logrus.Fields{"slot": slotName, "err": err}).Error("failed to query restart_lsn")
		}
		logrus.WithFields(logrus.Fields{"iter": iter, "before": beforeLSN, "after": afterLSN}).Debug("checking slot LSN")
		if afterLSN > beforeLSN {
			return
		}

		time.Sleep(200 * time.Millisecond)
	}
	t.Errorf("slot %q restart LSN failed to advance after %d retries", *TestReplicationSlot, retryCount)
}
