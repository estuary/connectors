package main

import (
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
	cfg, ctx := TestDefaultConfig, shortTestContext(t)
	tableName := createTestTable(ctx, t, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	catalog, state := testCatalog(tableName), PersistentState{}

	// Add data, perform capture, verify result
	dbInsert(ctx, t, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}, {3, "Four"}, {4, "5"}})
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "")
}

// TestReplicationInserts runs two captures, where the first will perform the
// initial table scan and the second capture will use replication to receive
// additional inserts performed after the first capture.
func TestReplicationInserts(t *testing.T) {
	cfg, ctx := TestDefaultConfig, shortTestContext(t)
	tableName := createTestTable(ctx, t, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	catalog, state := testCatalog(tableName), PersistentState{}

	dbInsert(ctx, t, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}, {3, "Four"}, {4, "5"}})
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "init")
	dbInsert(ctx, t, tableName, [][]interface{}{{1002, "some"}, {1000, "more"}, {1001, "rows"}})
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "")
}

// TestReplicationDeletes runs two captures, where the first will perform the
// initial table scan and the second capture will use replication to receive
// additional inserts and deletions performed after the first capture.
func TestReplicationDeletes(t *testing.T) {
	cfg, ctx := TestDefaultConfig, shortTestContext(t)
	tableName := createTestTable(ctx, t, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	catalog, state := testCatalog(tableName), PersistentState{}

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
	cfg, ctx := TestDefaultConfig, shortTestContext(t)
	tableName := createTestTable(ctx, t, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	catalog, state := testCatalog(tableName), PersistentState{}

	dbInsert(ctx, t, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}})
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "init")
	dbInsert(ctx, t, tableName, [][]interface{}{{1002, "some"}, {1000, "more"}, {1001, "rows"}})
	dbQuery(ctx, t, fmt.Sprintf("UPDATE %s SET data = 'updated' WHERE id = 1 OR id = 1002;", tableName))
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "")
}

// TestEmptyTable leaves the table empty during the initial table snapshotting
// and only adds data after replication has begun.
func TestEmptyTable(t *testing.T) {
	cfg, ctx := TestDefaultConfig, shortTestContext(t)
	tableName := createTestTable(ctx, t, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	catalog, state := testCatalog(tableName), PersistentState{}

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
	cfg, ctx := TestDefaultConfig, shortTestContext(t)
	tableName := createTestTable(ctx, t, "", "(year INTEGER, state TEXT, fullname TEXT, population INTEGER, PRIMARY KEY (year, state))")
	catalog, state := testCatalog(tableName), PersistentState{}

	dbLoadCSV(ctx, t, tableName, "statepop.csv", 0)
	states := verifiedCapture(ctx, t, &cfg, &catalog, &state, "init")
	state = states[10] // Restart in between (1960, 'IA') and (1960, 'ID')

	dbInsert(ctx, t, tableName, [][]interface{}{
		{1930, "XX", "No Such State", 1234},   // An insert prior to the first restart, which will be reported once replication begins
		{1970, "XX", "No Such State", 12345},  // An insert between the two restarts, which will be visible in the table scan and should be filtered during replication
		{1990, "XX", "No Such State", 123456}, // An insert after the second restart, which will be visible in the table scan and should be filtered during replication
	})
	states = verifiedCapture(ctx, t, &cfg, &catalog, &state, "restart1")
	state = states[4] // Restart in between (1980, 'SD') and (1980, 'TN')

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
	cfg, ctx, state := TestDefaultConfig, shortTestContext(t), PersistentState{}
	table1 := createTestTable(ctx, t, "one", "(id INTEGER PRIMARY KEY, data TEXT)")
	table2 := createTestTable(ctx, t, "two", "(id INTEGER PRIMARY KEY, data TEXT)")
	table3 := createTestTable(ctx, t, "three", "(id INTEGER PRIMARY KEY, data TEXT)")
	dbInsert(ctx, t, table1, [][]interface{}{{0, "zero"}, {1, "one"}, {2, "two"}})
	dbInsert(ctx, t, table2, [][]interface{}{{3, "three"}, {4, "four"}, {5, "five"}})
	dbInsert(ctx, t, table3, [][]interface{}{{6, "six"}, {7, "seven"}, {8, "eight"}})
	catalog1 := testCatalog(table1)
	catalog123 := testCatalog(table1, table2, table3)
	catalog13 := testCatalog(table1, table3)
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
	cfg, ctx, state := TestDefaultConfig, shortTestContext(t), PersistentState{}
	table := createTestTable(ctx, t, "", "(year INTEGER, state TEXT, fullname TEXT, population INTEGER)")
	dbLoadCSV(ctx, t, table, "statepop.csv", 100)
	catalog := testCatalog(table)
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
	cfg, ctx, state := TestDefaultConfig, shortTestContext(t), PersistentState{}
	table := createTestTable(ctx, t, "", "(year INTEGER, state TEXT, fullname TEXT, population INTEGER, PRIMARY KEY (year, state))")
	dbLoadCSV(ctx, t, table, "statepop.csv", 100)
	catalog := testCatalog(table)
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
	cfg, ctx, state := TestDefaultConfig, shortTestContext(t), PersistentState{}
	table1 := createTestTable(ctx, t, "one", "(id INTEGER PRIMARY KEY, data TEXT)")
	table2 := createTestTable(ctx, t, "two", "(id INTEGER PRIMARY KEY, data TEXT)")
	dbInsert(ctx, t, table1, [][]interface{}{{0, "zero"}, {1, "one"}, {2, "two"}})
	dbInsert(ctx, t, table2, [][]interface{}{{3, "three"}, {4, "four"}, {5, "five"}})
	catalog := testCatalog(table1)
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "capture1") // Scan table1
	dbInsert(ctx, t, table1, [][]interface{}{{6, "six"}})
	dbInsert(ctx, t, table2, [][]interface{}{{7, "seven"}})
	dbInsert(ctx, t, table1, [][]interface{}{{8, "eight"}})
	dbInsert(ctx, t, table2, [][]interface{}{{9, "nine"}})
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "capture2") // Replicate table1 events, ignoring table2
}

// TestSlotLSNAdvances checks that the `restart_lsn` of a replication slot
// has advanced after a connector restart.
func TestSlotLSNAdvances(t *testing.T) {
	cfg, ctx, state := TestDefaultConfig, longTestContext(t, 30*time.Second), PersistentState{}
	table := createTestTable(ctx, t, "one", "(id INTEGER PRIMARY KEY, data TEXT)")
	catalog := testCatalog(table)

	// Capture the current `restart_lsn` of the replication slot prior to our test
	var beforeLSN pglogrepl.LSN
	if err := TestDatabase.QueryRow(ctx, `SELECT restart_lsn FROM pg_catalog.pg_replication_slots WHERE slot_name = $1;`, *TestReplicationSlot).Scan(&beforeLSN); err != nil {
		logrus.WithField("slot", *TestReplicationSlot).WithField("err", err).Error("failed to query restart_lsn")
	}

	// Insert some data, note down a deadline 20s after that data was inserted, and
	// get the initial table scan out of the way.
	//
	// PostgreSQL `BackgroundWriterMain()` will trigger `LogStandbySnapshot()` whenever
	// "important records" have been inserted in the WAL and >15s have elapsed since
	// the last such snapshot. We need this to happen because these snapshots include
	// an `XLOG_RUNNING_XACTS` record, which is the trigger for the mechanism allowing
	// a replication slot's `restart_lsn` to advance in response to StandbyStatusUpdate
	// messages.
	dbInsert(ctx, t, table, [][]interface{}{{0, "zero"}, {1, "one"}, {2, "two"}})
	deadline := time.Now().Add(20 * time.Second)
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "capture1")

	// Wait until we're confident a snapshot must have taken place, then create a
	// few more change events. The next capture will advance 'CurrentLSN' in response
	// to these 'Commit's.
	logrus.Info("waiting so a standby snapshot can occur")
	time.Sleep(time.Until(deadline))
	dbInsert(ctx, t, table, [][]interface{}{{3, "three"}, {4, "four"}, {5, "five"}})

	// Perform two captures. The first one will receive all the change events up
	// to this point and emit state updates advancing 'CurrentLSN'. The second
	// capture will then treat the initial 'CurrentLSN' as "acknowledged" and
	// relay it to the database in StandbyStatusUpdate messages. These status
	// updates will interact with the XLOG_RUNNING_XACTS record in the WAL to
	// actually advance the replication slot's `restart_lsn`.
	cfg.PollTimeoutSeconds = 2
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "capture2")
	verifiedCapture(ctx, t, &cfg, &catalog, &state, "capture3")

	// Finally, check the `restart_lsn` of the replication slot and verify that
	// it's more recent than it used to be.
	var afterLSN pglogrepl.LSN
	if err := TestDatabase.QueryRow(ctx, `SELECT restart_lsn FROM pg_catalog.pg_replication_slots WHERE slot_name = $1;`, *TestReplicationSlot).Scan(&afterLSN); err != nil {
		logrus.WithField("slot", *TestReplicationSlot).WithField("err", err).Error("failed to query restart_lsn")
	}
	logrus.WithField("before", beforeLSN).WithField("after", afterLSN).Info("done")
	if afterLSN <= beforeLSN {
		t.Errorf("slot %q restart LSN didn't advance (before=%q, after=%q)", *TestReplicationSlot, beforeLSN, afterLSN)
	}
}
