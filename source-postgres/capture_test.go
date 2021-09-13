package main

import (
	"fmt"
	"testing"
)

// TestSimpleCapture initializes a DB table with a few rows, then runs a capture
// which should emit all rows during table-scanning, start replication, and then
// shut down due to a lack of further events.
func TestSimpleCapture(t *testing.T) {
	cfg, ctx := TestDefaultConfig, shortTestContext(t)
	tableName := createTestTable(t, ctx, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	catalog, state := testCatalog(tableName), PersistentState{}

	// Add data, perform capture, verify result
	dbInsert(t, ctx, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}, {3, "Four"}, {4, "5"}})
	verifiedCapture(t, ctx, &cfg, &catalog, &state, "")
}

// TestReplicationInserts runs two captures, where the first will perform the
// initial table scan and the second capture will use replication to receive
// additional inserts performed after the first capture.
func TestReplicationInserts(t *testing.T) {
	cfg, ctx := TestDefaultConfig, shortTestContext(t)
	tableName := createTestTable(t, ctx, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	catalog, state := testCatalog(tableName), PersistentState{}

	dbInsert(t, ctx, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}, {3, "Four"}, {4, "5"}})
	verifiedCapture(t, ctx, &cfg, &catalog, &state, "init")
	dbInsert(t, ctx, tableName, [][]interface{}{{1002, "some"}, {1000, "more"}, {1001, "rows"}})
	verifiedCapture(t, ctx, &cfg, &catalog, &state, "")
}

// TestReplicationDeletes runs two captures, where the first will perform the
// initial table scan and the second capture will use replication to receive
// additional inserts and deletions performed after the first capture.
func TestReplicationDeletes(t *testing.T) {
	cfg, ctx := TestDefaultConfig, shortTestContext(t)
	tableName := createTestTable(t, ctx, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	catalog, state := testCatalog(tableName), PersistentState{}

	dbInsert(t, ctx, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}, {3, "Four"}, {4, "5"}})
	verifiedCapture(t, ctx, &cfg, &catalog, &state, "init")
	dbInsert(t, ctx, tableName, [][]interface{}{{1002, "some"}, {1000, "more"}, {1001, "rows"}})
	dbQuery(t, ctx, fmt.Sprintf("DELETE FROM %s WHERE id = 1 OR id = 1002;", tableName))
	verifiedCapture(t, ctx, &cfg, &catalog, &state, "")
}

// TestReplicationUpdates runs two captures, where the first will perform the
// initial table scan and the second capture will use replication to receive
// additional inserts and row updates performed after the first capture.
func TestReplicationUpdates(t *testing.T) {
	cfg, ctx := TestDefaultConfig, shortTestContext(t)
	tableName := createTestTable(t, ctx, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	catalog, state := testCatalog(tableName), PersistentState{}

	dbInsert(t, ctx, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}})
	verifiedCapture(t, ctx, &cfg, &catalog, &state, "init")
	dbInsert(t, ctx, tableName, [][]interface{}{{1002, "some"}, {1000, "more"}, {1001, "rows"}})
	dbQuery(t, ctx, fmt.Sprintf("UPDATE %s SET data = 'updated' WHERE id = 1 OR id = 1002;", tableName))
	verifiedCapture(t, ctx, &cfg, &catalog, &state, "")
}

// TestEmptyTable leaves the table empty during the initial table snapshotting
// and only adds data after replication has begun.
func TestEmptyTable(t *testing.T) {
	cfg, ctx := TestDefaultConfig, shortTestContext(t)
	tableName := createTestTable(t, ctx, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	catalog, state := testCatalog(tableName), PersistentState{}

	verifiedCapture(t, ctx, &cfg, &catalog, &state, "init")
	dbInsert(t, ctx, tableName, [][]interface{}{{1002, "some"}, {1000, "more"}, {1001, "rows"}})
	verifiedCapture(t, ctx, &cfg, &catalog, &state, "")
}

// TestComplexDataset tries to throw together a bunch of different bits of complexity
// to synthesize something vaguely "realistic". It features a multiple-column primary
// key, a dataset large enough that the initial table scan gets divided across many
// "chunks", two connector restarts at different points in the initial table scan, and
// some concurrent modifications to row ranges already-scanned and not-yet-scanned.
func TestComplexDataset(t *testing.T) {
	cfg, ctx := TestDefaultConfig, shortTestContext(t)
	tableName := createTestTable(t, ctx, "", "(year INTEGER, state TEXT, fullname TEXT, population INTEGER, PRIMARY KEY (year, state))")
	catalog, state := testCatalog(tableName), PersistentState{}

	dbLoadCSV(t, ctx, tableName, "statepop.csv")
	states := verifiedCapture(t, ctx, &cfg, &catalog, &state, "init")
	state = states[10] // Restart in between (1960, 'IA') and (1960, 'ID')

	dbInsert(t, ctx, tableName, [][]interface{}{
		{1930, "XX", "No Such State", 1234},   // An insert prior to the first restart, which will be reported once replication begins
		{1970, "XX", "No Such State", 12345},  // An insert between the two restarts, which will be visible in the table scan and should be filtered during replication
		{1990, "XX", "No Such State", 123456}, // An insert after the second restart, which will be visible in the table scan and should be filtered during replication
	})
	states = verifiedCapture(t, ctx, &cfg, &catalog, &state, "restart1")
	state = states[4] // Restart in between (1980, 'SD') and (1980, 'TN')

	dbQuery(t, ctx, fmt.Sprintf("DELETE FROM %s WHERE state = 'XX';", tableName))
	dbInsert(t, ctx, tableName, [][]interface{}{
		{1930, "XX", "No Such State", 1234},   // Deleting/reinserting this row will be reported since they happened after that portion of the table was scanned
		{1970, "XX", "No Such State", 12345},  // Deleting/reinserting this row will be reported since they happened after that portion of the table was scanned
		{1990, "XX", "No Such State", 123456}, // Deleting/reinserting this row will be filtered since this portion of the table has yet to be scanned
	})
	verifiedCapture(t, ctx, &cfg, &catalog, &state, "restart2")
}
