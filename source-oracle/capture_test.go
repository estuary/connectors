package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/connectors/sqlcapture/tests"
	"github.com/sirupsen/logrus"
)

func TestViewDiscovery(t *testing.T) {
	var tb, ctx = oracleTestBackend(t), context.Background()
	var tableName = tb.CreateTable(ctx, t, "", "(id INTEGER PRIMARY KEY, grp INTEGER, data VARCHAR(2000))")

	var view = tableName + "_simpleview"
	tb.Query(ctx, t, false, fmt.Sprintf(`DROP VIEW %s`, view))
	tb.Query(ctx, t, true, fmt.Sprintf(`CREATE VIEW %s AS SELECT id, data FROM %s WHERE grp = 1`, view, tableName))
	t.Cleanup(func() {
		logrus.WithField("view", view).Debug("dropping view")
		tb.Query(ctx, t, false, fmt.Sprintf(`DROP VIEW %s`, view))
	})

	var bindings = tb.CaptureSpec(ctx, t).Discover(ctx, t, regexp.MustCompile(regexp.QuoteMeta(strings.TrimPrefix(tableName, "test."))))
	for _, binding := range bindings {
		logrus.WithField("name", binding.RecommendedName).Debug("discovered stream")
		if strings.Contains(string(binding.RecommendedName), "_simpleview") {
			t.Errorf("view returned by catalog discovery")
		}
		if strings.Contains(string(binding.RecommendedName), "_matview") {
			t.Errorf("materialized view returned by catalog discovery")
		}
	}
}

func TestSkipBackfills(t *testing.T) {
	// Set up three tables with some data in them, a catalog which captures all three,
	// but a configuration which specifies that tables A and C should skip backfilling
	// and only capture new changes.
	var tb, ctx = oracleTestBackend(t), context.Background()
	var uniqueA, uniqueB, uniqueC = "18110541", "24310805", "38410024"
	var tableA = tb.CreateTable(ctx, t, uniqueA, "(id INTEGER PRIMARY KEY, data VARCHAR(2000))")
	var tableB = tb.CreateTable(ctx, t, uniqueB, "(id INTEGER PRIMARY KEY, data VARCHAR(2000))")
	var tableC = tb.CreateTable(ctx, t, uniqueC, "(id INTEGER PRIMARY KEY, data VARCHAR(2000))")
	tb.config.Advanced.SkipBackfills = fmt.Sprintf("%s,%s", tableA, tableC)
	tb.Insert(ctx, t, tableA, [][]any{{1, "one"}, {2, "two"}, {3, "three"}})
	tb.Insert(ctx, t, tableB, [][]any{{4, "four"}, {5, "five"}, {6, "six"}})
	tb.Insert(ctx, t, tableC, [][]any{{7, "seven"}, {8, "eight"}, {9, "nine"}})

	// Run an initial capture, which should capture all three tables but only backfill events from table B
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueA), regexp.MustCompile(uniqueB), regexp.MustCompile(uniqueC))
	t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Insert additional data and verify that all three tables report new events
	tb.Insert(ctx, t, tableA, [][]any{{10, "ten"}, {11, "eleven"}, {12, "twelve"}})
	tb.Insert(ctx, t, tableB, [][]any{{13, "thirteen"}, {14, "fourteen"}, {15, "fifteen"}})
	tb.Insert(ctx, t, tableC, [][]any{{16, "sixteen"}, {17, "seventeen"}, {18, "eighteen"}})
	t.Run("main", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

func TestTruncatedTables(t *testing.T) {
	// Set up two tables with some data in them
	var tb, ctx = oracleTestBackend(t), context.Background()
	var uniqueA, uniqueB = "14026504", "29415894"
	var tableA = tb.CreateTable(ctx, t, uniqueA, "(id INTEGER PRIMARY KEY, data VARCHAR(2000))")
	var tableB = tb.CreateTable(ctx, t, uniqueB, "(id INTEGER PRIMARY KEY, data VARCHAR(2000))")
	tb.Insert(ctx, t, tableA, [][]any{{1, "one"}, {2, "two"}, {3, "three"}})
	tb.Insert(ctx, t, tableB, [][]any{{4, "four"}, {5, "five"}, {6, "six"}})

	// Set up and run a capture of Table A only
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueA))
	t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Add data to table A and truncate table B. Captures should still succeed because we
	// don't care about truncates to non-active tables.
	tb.Insert(ctx, t, tableA, [][]any{{7, "seven"}, {8, "eight"}, {9, "nine"}})
	tb.Query(ctx, t, true, fmt.Sprintf("TRUNCATE TABLE %s", tableB))
	t.Run("capture1", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Truncating table A will cause the capture to fail though, as it should.
	tb.Query(ctx, t, true, fmt.Sprintf("TRUNCATE TABLE %s", tableA))
	tb.Insert(ctx, t, tableA, [][]any{{10, "ten"}, {11, "eleven"}, {12, "twelve"}})
	t.Run("capture2", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

func TestTrickyColumnNames(t *testing.T) {
	// Create a table with some 'difficult' column names (a reserved word, a capitalized
	// name, and one containing special characters which also happens to be the primary key).
	var tb, ctx = oracleTestBackend(t), context.Background()
	var uniqueA, uniqueB = "39256824", "42531495"
	var tableA = tb.CreateTable(ctx, t, uniqueA, `("Meta/""wtf""~ID" INTEGER PRIMARY KEY, data VARCHAR(2000))`)
	var tableB = tb.CreateTable(ctx, t, uniqueB, `("table" INTEGER PRIMARY KEY, data VARCHAR(2000))`)
	tb.Insert(ctx, t, tableA, [][]any{{1, "aaa"}, {2, "bbb"}})
	tb.Insert(ctx, t, tableB, [][]any{{3, "ccc"}, {4, "ddd"}})

	// Discover the catalog and verify that the table schemas looks correct
	t.Run("discover", func(t *testing.T) {
		tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueA), regexp.MustCompile(uniqueB))
	})

	// Perform an initial backfill
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueA), regexp.MustCompile(uniqueB))
	t.Run("backfill", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Add more data and read it via replication
	tb.Insert(ctx, t, tableA, [][]any{{5, "eee"}, {6, "fff"}})
	tb.Insert(ctx, t, tableB, [][]any{{7, "ggg"}, {8, "hhh"}})
	t.Run("replication", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

// TestCursorResume sets up a capture with a (string, int) primary key and
// and repeatedly restarts it after each row of capture output.
func TestCursorResume(t *testing.T) {
	var tb, ctx = oracleTestBackend(t), context.Background()
	var uniqueID = "95911555"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(epoch VARCHAR(8), count INTEGER, data VARCHAR(2000), PRIMARY KEY (epoch, count))")
	tb.Insert(ctx, t, tableName, [][]any{
		{"aaa", 1, "bvzf"}, {"aaa", 2, "ukwh"}, {"aaa", 3, "lntg"}, {"bbb", -100, "bycz"},
		{"bbb", 2, "ajgp"}, {"bbb", 333, "zljj"}, {"bbb", 4096, "lhnw"}, {"bbb", 800000, "iask"},
		{"ccc", 1234, "bikh"}, {"ddd", -10000, "dhqc"}, {"x", 1, "djsf"}, {"y", 1, "iwnx"},
		{"z", 1, "qmjp"}, {"", 0, "xakg"}, {"", -1, "kvxr"}, {"   ", 3, "gboj"},
	})
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

	// Reduce the backfill chunk size to 1 row. Since the capture will be killed and
	// restarted after each scan key update, this means we'll advance over the keys
	// one by one.
	cs.EndpointSpec.(*Config).Advanced.BackfillChunkSize = 1
	var summary, _ = tests.RestartingBackfillCapture(ctx, t, cs)
	cupaloy.SnapshotT(t, summary)
}

// TestComplexDataset tries to throw together a bunch of different bits of complexity
// to synthesize something vaguely "realistic". It features a multiple-column primary
// key, a dataset large enough that the initial table scan gets divided across many
// "chunks", two connector restarts at different points in the initial table scan, and
// some concurrent modifications to row ranges already-scanned and not-yet-scanned.
func TestComplexDataset(t *testing.T) {
	var tb, ctx = oracleTestBackend(t), context.Background()
	var uniqueID = "86827053"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(year INTEGER, state VARCHAR(2000), fullname VARCHAR(2000), population INTEGER, PRIMARY KEY (year, state))")
	tests.LoadCSV(ctx, t, tb, tableName, "statepop.csv", 0)
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

	// Reduce the backfill chunk size to 10 rows for this test.
	cs.EndpointSpec.(*Config).Advanced.BackfillChunkSize = 10

	t.Run("init", func(t *testing.T) {
		var summary, states = tests.RestartingBackfillCapture(ctx, t, cs)
		cupaloy.SnapshotT(t, summary)
		cs.Checkpoint = states[13] // Next restart between (1940, 'NV') and (1940, 'NY')
		logrus.WithField("checkpoint", string(cs.Checkpoint)).Warn("restart at")
	})

	tb.Insert(ctx, t, tableName, [][]any{
		{1930, "XX", "No Such State", 1234},   // An insert prior to the first restart, which will be reported once replication begins
		{1970, "XX", "No Such State", 12345},  // An insert between the two restarts, which will be visible in the table scan and should be filtered during replication
		{1990, "XX", "No Such State", 123456}, // An insert after the second restart, which will be visible in the table scan and should be filtered during replication
	})
	t.Run("restart1", func(t *testing.T) {
		var summary, states = tests.RestartingBackfillCapture(ctx, t, cs)
		cupaloy.SnapshotT(t, summary)
		cs.Checkpoint = states[10] // Next restart in the middle of 1980 data
	})

	tb.Query(ctx, t, true, fmt.Sprintf("DELETE FROM %s WHERE state = 'XX'", tableName))
	tb.Insert(ctx, t, tableName, [][]any{
		{1930, "XX", "No Such State", 1234},   // Deleting/reinserting this row will be reported since they happened after that portion of the table was scanned
		{1970, "XX", "No Such State", 12345},  // Deleting/reinserting this row will be reported since they happened after that portion of the table was scanned
		{1990, "XX", "No Such State", 123456}, // Deleting/reinserting this row will be filtered since this portion of the table has yet to be scanned
	})

	tb.Query(ctx, t, true, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tableName))

	// We've scanned through (1980, 'IA'), and will see updates for N% states at that date or before,
	// and creations for N% state records after that date which reflect the update.
	tb.Query(ctx, t, true, fmt.Sprintf("UPDATE %s SET fullname = 'New ' || fullname WHERE state IN ('NJ', 'NY')", tableName))
	// We'll see a deletion since this row has already been scanned through.
	tb.Query(ctx, t, true, fmt.Sprintf("DELETE FROM %s WHERE state = 'XX' AND year = 1970", tableName))

	t.Run("restart2", func(t *testing.T) {
		var summary, _ = tests.RestartingBackfillCapture(ctx, t, cs)
		cupaloy.SnapshotT(t, summary)
	})
}

func TestCaptureCapitalization(t *testing.T) {
	var tb, ctx = oracleTestBackend(t), context.Background()

	var uniqueA, uniqueB = "69943814", "73423348"
	var tablePrefix = strings.TrimPrefix(t.Name(), "Test")
	var tableA = tablePrefix + "_AaAaA_" + uniqueA                  // Name containing capital letters
	var tableB = strings.ToLower(tablePrefix + "_BbBbB_" + uniqueB) // Name which is all lowercase (like all our other test table names)

	var cleanup = func() {
		tb.Query(ctx, t, false, fmt.Sprintf(`DROP TABLE "%s"."%s"`, testSchemaName, tableA))
		tb.Query(ctx, t, false, fmt.Sprintf(`DROP TABLE "%s"."%s"`, testSchemaName, tableB))
	}
	cleanup()
	t.Cleanup(cleanup)

	tb.Query(ctx, t, true, fmt.Sprintf(`CREATE TABLE "%s"."%s" (id INTEGER PRIMARY KEY, data VARCHAR(2000))`, testSchemaName, tableA))
	tb.Query(ctx, t, true, fmt.Sprintf(`CREATE TABLE "%s"."%s" (id INTEGER PRIMARY KEY, data VARCHAR(2000))`, testSchemaName, tableB))

	tb.Query(ctx, t, true, fmt.Sprintf(`INSERT INTO "%s"."%s" VALUES (0, 'hello'), (1, 'asdf')`, testSchemaName, tableA))
	tb.Query(ctx, t, true, fmt.Sprintf(`INSERT INTO "%s"."%s" VALUES (2, 'world'), (3, 'fdsa')`, testSchemaName, tableB))

	tests.VerifiedCapture(ctx, t, tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueA), regexp.MustCompile(uniqueB)))
}

func TestCaptureOversizedFields(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	var tb, ctx = oracleTestBackend(t), context.Background()
	var uniqueID = "64819605"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, tdata VARCHAR(2000), bdata BYTEA, jdata JSON, jbdata JSONB)")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = new(st.ChecksumValidator)

	var largeText = strings.Repeat("data", 4194304)         // 16MiB string
	var largeJSON = fmt.Sprintf(`{"text":"%s"}`, largeText) // ~16MiB JSON object
	tb.Insert(ctx, t, tableName, [][]any{
		{0, largeText, []byte(largeText), largeJSON, largeJSON},
		{1, largeText, []byte(largeText), largeJSON, largeJSON},
		{2, largeText, []byte(largeText), largeJSON, largeJSON},
		{3, largeText, []byte(largeText), largeJSON, largeJSON},
	})
	tests.VerifiedCapture(ctx, t, cs)

	t.Run("Replication", func(t *testing.T) {
		tb.Insert(ctx, t, tableName, [][]any{
			{4, largeText, []byte(largeText), largeJSON, largeJSON},
			{5, largeText, []byte(largeText), largeJSON, largeJSON},
			{6, largeText, []byte(largeText), largeJSON, largeJSON},
			{7, largeText, []byte(largeText), largeJSON, largeJSON},
		})
		tests.VerifiedCapture(ctx, t, cs)
	})
}
