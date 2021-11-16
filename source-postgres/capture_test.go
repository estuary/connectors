package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/sirupsen/logrus"
)

// TestSimpleCapture initializes a DB table with a few rows, then runs a capture
// which should emit all rows during table-scanning, start replication, and then
// shut down due to a lack of further events.
func TestSimpleCapture(t *testing.T) {
	var cfg, ctx = TestDefaultConfig, shortTestContext(t)
	var tableName = createTestTable(ctx, t, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	var catalog, state = testCatalog(tableName), PersistentState{}

	// Add data, perform capture, verify result
	dbInsert(ctx, t, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}, {3, "Four"}, {4, "5"}})
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "")
}

// TestTailing performs a capture in 'tail' mode, which is how it actually runs
// under Flow outside of development tests. This means that after the initial
// backfilling completes, the capture will run indefinitely without writing
// any more watermarks.
func TestTailing(t *testing.T) {
	var cfg, ctx = TestDefaultConfig, shortTestContext(t)
	var tableName = createTestTable(ctx, t, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	var catalog, state = testCatalog(tableName), PersistentState{}
	catalog.Tail = true

	// Initial data which must be backfilled
	dbInsert(ctx, t, tableName, [][]interface{}{{0, "A"}, {10, "bbb"}, {20, "CDEFGHIJKLMNOP"}, {30, "Four"}, {40, "5"}})

	// Run the capture
	var captureCtx, cancelCapture = context.WithCancel(ctx)
	go verifiedCapture(captureCtx, t, &cfg, &catalog, &state, "")
	time.Sleep(1 * time.Second)

	// Some more changes occurring after the backfill completes
	dbInsert(ctx, t, tableName, [][]interface{}{{5, "asdf"}, {100, "lots"}})
	dbQuery(ctx, t, fmt.Sprintf("DELETE FROM %s WHERE id = 20;", tableName))
	dbQuery(ctx, t, fmt.Sprintf("UPDATE %s SET data = 'updated' WHERE id = 30;", tableName))

	// Let the capture catch up and then terminate it
	time.Sleep(1 * time.Second)
	cancelCapture()
}

// TestReplicationInserts runs two captures, where the first will perform the
// initial table scan and the second capture will use replication to receive
// additional inserts performed after the first capture.
func TestReplicationInserts(t *testing.T) {
	var cfg, ctx = TestDefaultConfig, shortTestContext(t)
	var tableName = createTestTable(ctx, t, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	var catalog, state = testCatalog(tableName), PersistentState{}

	dbInsert(ctx, t, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}, {3, "Four"}, {4, "5"}})
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "init")
	dbInsert(ctx, t, tableName, [][]interface{}{{1002, "some"}, {1000, "more"}, {1001, "rows"}})
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "")
}

// TestReplicationDeletes runs two captures, where the first will perform the
// initial table scan and the second capture will use replication to receive
// additional inserts and deletions performed after the first capture.
func TestReplicationDeletes(t *testing.T) {
	var cfg, ctx = TestDefaultConfig, shortTestContext(t)
	var tableName = createTestTable(ctx, t, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	var catalog, state = testCatalog(tableName), PersistentState{}

	dbInsert(ctx, t, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}, {3, "Four"}, {4, "5"}})
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "init")
	dbInsert(ctx, t, tableName, [][]interface{}{{1002, "some"}, {1000, "more"}, {1001, "rows"}})
	dbQuery(ctx, t, fmt.Sprintf("DELETE FROM %s WHERE id = 1 OR id = 1002;", tableName))
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "")
}

// TestReplicationUpdates runs two captures, where the first will perform the
// initial table scan and the second capture will use replication to receive
// additional inserts and row updates performed after the first capture.
func TestReplicationUpdates(t *testing.T) {
	var cfg, ctx = TestDefaultConfig, shortTestContext(t)
	var tableName = createTestTable(ctx, t, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	var catalog, state = testCatalog(tableName), PersistentState{}

	dbInsert(ctx, t, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}})
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "init")
	dbInsert(ctx, t, tableName, [][]interface{}{{1002, "some"}, {1000, "more"}, {1001, "rows"}})
	dbQuery(ctx, t, fmt.Sprintf("UPDATE %s SET data = 'updated' WHERE id = 1 OR id = 1002;", tableName))
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "")
}

// TestEmptyTable leaves the table empty during the initial table snapshotting
// and only adds data after replication has begun.
func TestEmptyTable(t *testing.T) {
	var cfg, ctx = TestDefaultConfig, shortTestContext(t)
	var tableName = createTestTable(ctx, t, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	var catalog, state = testCatalog(tableName), PersistentState{}

	verifiedCapture(ctx, t, &cfg, &catalog, &state, "init")
	dbInsert(ctx, t, tableName, [][]interface{}{{1002, "some"}, {1000, "more"}, {1001, "rows"}})
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "")
}

// TestComplexDataset tries to throw together a bunch of different bits of complexity
// to synthesize something vaguely "realistic". It features a multiple-column primary
// key, a dataset large enough that the initial table scan gets divided across many
// "chunks", two connector restarts at different points in the initial table scan, and
// some concurrent modifications to row ranges already-scanned and not-yet-scanned.
func TestComplexDataset(t *testing.T) {
	var cfg, ctx = TestDefaultConfig, shortTestContext(t)
	var tableName = createTestTable(ctx, t, "", "(year INTEGER, state TEXT, fullname TEXT, population INTEGER, PRIMARY KEY (year, state))")
	var catalog, state = testCatalog(tableName), PersistentState{}

	dbLoadCSV(ctx, t, tableName, "statepop.csv", 0)
	var states = verifiedCapture(ctx, t, &cfg, &catalog, &state, "init")
	state = states[10] // Restart in between (1960, 'IA') and (1960, 'ID')

	dbInsert(ctx, t, tableName, [][]interface{}{
		{1930, "XX", "No Such State", 1234},   // An insert prior to the first restart, which will be reported once replication begins
		{1970, "XX", "No Such State", 12345},  // An insert between the two restarts, which will be visible in the table scan and should be filtered during replication
		{1990, "XX", "No Such State", 123456}, // An insert after the second restart, which will be visible in the table scan and should be filtered during replication
	})
	states = verifiedCapture(ctx, t, &cfg, &catalog, &state, "restart1")
	state = states[6] // Restart in between (1980, 'SD') and (1980, 'TN')

	dbQuery(ctx, t, fmt.Sprintf("DELETE FROM %s WHERE state = 'XX';", tableName))
	dbInsert(ctx, t, tableName, [][]interface{}{
		{1930, "XX", "No Such State", 1234},   // Deleting/reinserting this row will be reported since they happened after that portion of the table was scanned
		{1970, "XX", "No Such State", 12345},  // Deleting/reinserting this row will be reported since they happened after that portion of the table was scanned
		{1990, "XX", "No Such State", 123456}, // Deleting/reinserting this row will be filtered since this portion of the table has yet to be scanned
	})
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "restart2")
}

// TestMultipleStreams exercises captures with multiple stream configured, as
// well as adding/removing/re-adding a stream.
func TestMultipleStreams(t *testing.T) {
	var cfg, ctx, state = TestDefaultConfig, shortTestContext(t), PersistentState{}
	var table1 = createTestTable(ctx, t, "one", "(id INTEGER PRIMARY KEY, data TEXT)")
	var table2 = createTestTable(ctx, t, "two", "(id INTEGER PRIMARY KEY, data TEXT)")
	var table3 = createTestTable(ctx, t, "three", "(id INTEGER PRIMARY KEY, data TEXT)")
	dbInsert(ctx, t, table1, [][]interface{}{{0, "zero"}, {1, "one"}, {2, "two"}})
	dbInsert(ctx, t, table2, [][]interface{}{{3, "three"}, {4, "four"}, {5, "five"}})
	dbInsert(ctx, t, table3, [][]interface{}{{6, "six"}, {7, "seven"}, {8, "eight"}})
	var catalog1 = testCatalog(table1)
	var catalog123 = testCatalog(table1, table2, table3)
	var catalog13 = testCatalog(table1, table3)
	verifiedCapture(ctx, t, &cfg, &catalog1, &state, "capture1")   // Scan table1
	verifiedCapture(ctx, t, &cfg, &catalog123, &state, "capture2") // Add table2 and table3
	verifiedCapture(ctx, t, &cfg, &catalog13, &state, "capture3")  // Forget about table2
	dbInsert(ctx, t, table1, [][]interface{}{{9, "nine"}})
	dbInsert(ctx, t, table2, [][]interface{}{{10, "ten"}})
	dbInsert(ctx, t, table3, [][]interface{}{{11, "eleven"}})
	verifiedCapture(ctx, t, &cfg, &catalog13, &state, "capture4")  // Replicate changes from table1 and table3 only
	verifiedCapture(ctx, t, &cfg, &catalog123, &state, "capture5") // Re-scan table2 including the new row
}

// TestCatalogPrimaryKey sets up a table with no primary key in the database
// and instead specifies one in the catalog configuration.
func TestCatalogPrimaryKey(t *testing.T) {
	var cfg, ctx, state = TestDefaultConfig, shortTestContext(t), PersistentState{}
	var table = createTestTable(ctx, t, "", "(year INTEGER, state TEXT, fullname TEXT, population INTEGER)")
	dbLoadCSV(ctx, t, table, "statepop.csv", 100)
	var catalog = testCatalog(table)
	catalog.Streams[0].PrimaryKey = [][]string{{"fullname"}, {"year"}}
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "capture1")
	dbInsert(ctx, t, table, [][]interface{}{
		{1930, "XX", "No Such State", 1234},
		{1970, "XX", "No Such State", 12345},
		{1990, "XX", "No Such State", 123456},
	})
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "capture2")
}

// TestCatalogPrimaryKeyOverride sets up a table with a primary key, but
// then overrides that via the catalog configuration.
func TestCatalogPrimaryKeyOverride(t *testing.T) {
	var cfg, ctx, state = TestDefaultConfig, shortTestContext(t), PersistentState{}
	var table = createTestTable(ctx, t, "", "(year INTEGER, state TEXT, fullname TEXT, population INTEGER, PRIMARY KEY (year, state))")
	dbLoadCSV(ctx, t, table, "statepop.csv", 100)
	var catalog = testCatalog(table)
	catalog.Streams[0].PrimaryKey = [][]string{{"fullname"}, {"year"}}
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "capture1")
	dbInsert(ctx, t, table, [][]interface{}{
		{1930, "XX", "No Such State", 1234},
		{1970, "XX", "No Such State", 12345},
		{1990, "XX", "No Such State", 123456},
	})
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "capture2")
}

// TestIgnoredStreams checks that replicated changes are only reported
// for tables which are configured in the catalog.
func TestIgnoredStreams(t *testing.T) {
	var cfg, ctx, state = TestDefaultConfig, shortTestContext(t), PersistentState{}
	var table1 = createTestTable(ctx, t, "one", "(id INTEGER PRIMARY KEY, data TEXT)")
	var table2 = createTestTable(ctx, t, "two", "(id INTEGER PRIMARY KEY, data TEXT)")
	dbInsert(ctx, t, table1, [][]interface{}{{0, "zero"}, {1, "one"}, {2, "two"}})
	dbInsert(ctx, t, table2, [][]interface{}{{3, "three"}, {4, "four"}, {5, "five"}})
	var catalog = testCatalog(table1)
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "capture1") // Scan table1
	dbInsert(ctx, t, table1, [][]interface{}{{6, "six"}})
	dbInsert(ctx, t, table2, [][]interface{}{{7, "seven"}})
	dbInsert(ctx, t, table1, [][]interface{}{{8, "eight"}})
	dbInsert(ctx, t, table2, [][]interface{}{{9, "nine"}})
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "capture2") // Replicate table1 events, ignoring table2
}

// TestSlotLSNAdvances checks that the `restart_lsn` of a replication slot
// eventually advances in response to connector restarts.
func TestSlotLSNAdvances(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	var cfg, ctx, state = TestDefaultConfig, longTestContext(t, 60*time.Second), PersistentState{}
	var table = createTestTable(ctx, t, "one", "(id INTEGER PRIMARY KEY, data TEXT)")
	var catalog = testCatalog(table)

	// Capture the current `restart_lsn` of the replication slot prior to our test
	var beforeLSN pglogrepl.LSN
	if err := TestDatabase.QueryRow(ctx, `SELECT restart_lsn FROM pg_catalog.pg_replication_slots WHERE slot_name = $1;`, *TestReplicationSlot).Scan(&beforeLSN); err != nil {
		logrus.WithField("slot", *TestReplicationSlot).WithField("err", err).Error("failed to query restart_lsn")
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
	dbInsert(ctx, t, table, [][]interface{}{{0, "zero"}, {1, "one"}, {2, "two"}})
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "capture1")
	logrus.Info("waiting so a standby snapshot can occur")
	time.Sleep(20 * time.Second)

	// Perform a capture and then check if `restart_lsn` has advanced. If not,
	// keep trying until it does or the retry count is hit.
	const retryCount = 50
	for iter := 0; iter < retryCount; iter++ {
		verifiedCapture(ctx, t, &cfg, &catalog, &state, "captureN")

		var afterLSN pglogrepl.LSN
		if err := TestDatabase.QueryRow(ctx, `SELECT restart_lsn FROM pg_catalog.pg_replication_slots WHERE slot_name = $1;`, *TestReplicationSlot).Scan(&afterLSN); err != nil {
			logrus.WithField("slot", *TestReplicationSlot).WithField("err", err).Error("failed to query restart_lsn")
		}
		logrus.WithFields(logrus.Fields{"iter": iter, "before": beforeLSN, "after": afterLSN}).Info("checking slot LSN")
		if afterLSN > beforeLSN {
			return
		}

		time.Sleep(200 * time.Millisecond)
	}
	t.Errorf("slot %q restart LSN failed to advance after %d retries", *TestReplicationSlot, retryCount)
}
