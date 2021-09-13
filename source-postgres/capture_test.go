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
	result, _ := performCapture(t, ctx, &cfg, &catalog, &state)
	verifySnapshot(t, "", result)
}

// TestSecondCapture runs two captures, as the name implies, with more data added
// to the table between the first and second run, and the second capture beginning
// from the final state of the first one. As a result, the second capture should
// immediately start replication and emit the new data via that pathway.
func TestSecondCapture(t *testing.T) {
	cfg, ctx := TestDefaultConfig, shortTestContext(t)
	tableName := createTestTable(t, ctx, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	catalog, state := testCatalog(tableName), PersistentState{}

	// Add data, perform capture, repeat
	dbInsert(t, ctx, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}, {3, "Four"}, {4, "5"}})
	firstResult, states := performCapture(t, ctx, &cfg, &catalog, &state)
	verifySnapshot(t, "first", firstResult)
	state = states[len(states)-1]

	dbInsert(t, ctx, tableName, [][]interface{}{{1002, "some"}, {1000, "more"}, {1001, "rows"}})
	secondResult, _ := performCapture(t, ctx, &cfg, &catalog, &state)
	verifySnapshot(t, "second", secondResult)
}

// TestDeleteEvent is like TestSecondCapture, but with some rows being deleted and
// recreated prior to the second capture run.
func TestDeleteEvents(t *testing.T) {
	cfg, ctx := TestDefaultConfig, shortTestContext(t)
	tableName := createTestTable(t, ctx, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	catalog, state := testCatalog(tableName), PersistentState{}

	// Add data, perform capture, repeat
	dbInsert(t, ctx, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}, {3, "Four"}, {4, "5"}})
	firstResult, states := performCapture(t, ctx, &cfg, &catalog, &state)
	verifySnapshot(t, "first", firstResult)
	state = states[len(states)-1]

	dbInsert(t, ctx, tableName, [][]interface{}{{1002, "some"}, {1000, "more"}, {1001, "rows"}})
	dbQuery(t, ctx, fmt.Sprintf("DELETE FROM %s WHERE id = 1 OR id = 1002;", tableName))
	secondResult, _ := performCapture(t, ctx, &cfg, &catalog, &state)
	verifySnapshot(t, "second", secondResult)
}
