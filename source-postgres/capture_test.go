package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/connectors/sqlcapture/tests"
	"github.com/jackc/pglogrepl"
	"github.com/sirupsen/logrus"
)

// TestReplicaIdentity exercises the 'REPLICA IDENTITY' setting of a table,
// which controls whether change events include full row contents or just the
// primary keys of the "before" state.
func TestReplicaIdentity(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = "64422387"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}, {3, "Four"}, {4, "5"}})

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Default REPLICA IDENTITY logs only the old primary key for deletions and updates.
	tb.Delete(ctx, t, tableName, "id", 1)
	tb.Update(ctx, t, tableName, "id", 2, "data", "UPDATED")

	// Increase to REPLICA IDENTITY FULL, and repeat. Expect to see complete modified tuples logged.
	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL;", tableName))
	tb.Delete(ctx, t, tableName, "id", 3)
	tb.Update(ctx, t, tableName, "id", 4, "data", "UPDATED")

	t.Run("main", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

func TestToastColumns(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = "74018723"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, other INTEGER, data TEXT)")

	// Table is created with REPLICA IDENTITY DEFAULT, which does *not* include
	// unchanged TOAST fields within the replication log.
	// Postgres will attempt to compress values by default.
	// Tell it to use TOAST but disable compression so our fixture spills out of the table.
	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN data SET STORAGE EXTERNAL;", tableName))

	// Create a data fixture which is (barely) larger that Postgres's desired inline storage size.
	const toastThreshold = 2048
	var data = strings.Repeat("data", toastThreshold/4)

	// Initial capture backfill.
	tb.Insert(ctx, t, tableName, [][]interface{}{{1, 32, "smol"}})
	tb.Insert(ctx, t, tableName, [][]interface{}{{2, 42, data}})
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Insert TOAST value, update TOAST value, and change an unrelated value.
	tb.Insert(ctx, t, tableName, [][]interface{}{{3, 52, data}})        // Insert TOAST.
	tb.Insert(ctx, t, tableName, [][]interface{}{{4, 62, "more smol"}}) // Insert non-TOAST.
	tb.Update(ctx, t, tableName, "id", 1, "data", "UPDATE ONE "+data)   // Update non-TOAST => TOAST.
	tb.Update(ctx, t, tableName, "id", 2, "data", "UPDATE TWO "+data)   // Update TOAST => TOAST.
	tb.Update(ctx, t, tableName, "id", 3, "data", "UPDATE smol")        // Update TOAST => non-TOAST.
	tb.Update(ctx, t, tableName, "id", 1, "other", 72)                  // Update other (TOAST); data _not_ expected.
	tb.Update(ctx, t, tableName, "id", 3, "other", 82)                  // Update other (non-TOAST).
	t.Run("ident-default", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL;", tableName))
	tb.Update(ctx, t, tableName, "id", 1, "other", 92)                // Update other (TOAST); data *is* expected.
	tb.Update(ctx, t, tableName, "id", 3, "other", 102)               // Update other (non-TOAST).
	tb.Update(ctx, t, tableName, "id", 1, "data", "smol smol")        // Update TOAST => non-TOAST.
	tb.Update(ctx, t, tableName, "id", 4, "data", "UPDATE SIX "+data) // Update non-TOAST => TOAST.
	t.Run("ident-full", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

// TestSlotLSNAdvances checks that the `restart_lsn` of a replication slot
// advances during normal connector operation.
func TestSlotLSNAdvances(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = "99718274"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

	var lsnQuery = `SELECT restart_lsn FROM pg_catalog.pg_replication_slots WHERE slot_name = $1;`
	var slotName = tb.config.Advanced.SlotName

	// Start a "traffic generator" which will generate 2 QPS of inserts
	var trafficCtx, cancelTraffic = context.WithCancel(ctx)
	defer cancelTraffic()
	go func(ctx context.Context) {
		for i := 0; ctx.Err() == nil; i++ {
			tb.Insert(ctx, t, tableName, [][]any{{i, fmt.Sprintf("Row %d", i)}})
			time.Sleep(500 * time.Millisecond)
		}
	}(trafficCtx)
	time.Sleep(1 * time.Second)

	// Start the capture running in a separate thread as well
	var captureCtx, cancelCapture = context.WithCancel(ctx)
	defer cancelCapture()
	go cs.Capture(captureCtx, t, nil)
	time.Sleep(1 * time.Second)

	// Capture the current `restart_lsn` of the replication slot. At this point any
	// startup-related behavior should have stabilized, so if the slot LSN changes
	// again this will demonstrate that we're advancing it reliably.
	var initialLSN pglogrepl.LSN
	if err := tb.control.QueryRow(ctx, lsnQuery, slotName).Scan(&initialLSN); err != nil {
		logrus.WithFields(logrus.Fields{"slot": slotName, "err": err}).Error("failed to query restart_lsn")
	}

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
		if err := tb.control.QueryRow(ctx, lsnQuery, slotName).Scan(&currentLSN); err != nil {
			logrus.WithFields(logrus.Fields{"slot": slotName, "err": err}).Error("failed to query restart_lsn")
		}
		logrus.WithFields(logrus.Fields{"initial": initialLSN, "current": currentLSN}).Info("checking slot LSN")
		if currentLSN > initialLSN {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Errorf("slot %q restart LSN failed to advance after %s", slotName, passDeadline.String())
}

func TestViewDiscovery(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	var tableName = tb.CreateTable(ctx, t, "", "(id INTEGER PRIMARY KEY, grp INTEGER, data TEXT)")

	var view = tableName + "_simpleview"
	tb.Query(ctx, t, fmt.Sprintf(`CREATE VIEW %s AS SELECT id, data FROM %s WHERE grp = 1;`, view, tableName))
	t.Cleanup(func() {
		logrus.WithField("view", view).Debug("dropping view")
		tb.Query(ctx, t, fmt.Sprintf(`DROP VIEW IF EXISTS %s;`, view))
	})

	var matview = tableName + "_matview"
	tb.Query(ctx, t, fmt.Sprintf(`CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM %s WHERE grp = 1;`, matview, tableName))
	t.Cleanup(func() {
		logrus.WithField("view", matview).Debug("dropping materialized view")
		tb.Query(ctx, t, fmt.Sprintf(`DROP MATERIALIZED VIEW IF EXISTS %s;`, matview))
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
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueA, uniqueB, uniqueC = "18110541", "24310805", "38410024"
	var tableA = tb.CreateTable(ctx, t, uniqueA, "(id INTEGER PRIMARY KEY, data TEXT)")
	var tableB = tb.CreateTable(ctx, t, uniqueB, "(id INTEGER PRIMARY KEY, data TEXT)")
	var tableC = tb.CreateTable(ctx, t, uniqueC, "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.config.Advanced.SkipBackfills = fmt.Sprintf("%s,%s", tableA, tableC)
	tb.Insert(ctx, t, tableA, [][]interface{}{{1, "one"}, {2, "two"}, {3, "three"}})
	tb.Insert(ctx, t, tableB, [][]interface{}{{4, "four"}, {5, "five"}, {6, "six"}})
	tb.Insert(ctx, t, tableC, [][]interface{}{{7, "seven"}, {8, "eight"}, {9, "nine"}})

	// Run an initial capture, which should capture all three tables but only backfill events from table B
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueA), regexp.MustCompile(uniqueB), regexp.MustCompile(uniqueC))
	t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Insert additional data and verify that all three tables report new events
	tb.Insert(ctx, t, tableA, [][]interface{}{{10, "ten"}, {11, "eleven"}, {12, "twelve"}})
	tb.Insert(ctx, t, tableB, [][]interface{}{{13, "thirteen"}, {14, "fourteen"}, {15, "fifteen"}})
	tb.Insert(ctx, t, tableC, [][]interface{}{{16, "sixteen"}, {17, "seventeen"}, {18, "eighteen"}})
	t.Run("main", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

func TestTruncatedTables(t *testing.T) {
	// Set up two tables with some data in them
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueA, uniqueB = "14026504", "29415894"
	var tableA = tb.CreateTable(ctx, t, uniqueA, "(id INTEGER PRIMARY KEY, data TEXT)")
	var tableB = tb.CreateTable(ctx, t, uniqueB, "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, tableA, [][]interface{}{{1, "one"}, {2, "two"}, {3, "three"}})
	tb.Insert(ctx, t, tableB, [][]interface{}{{4, "four"}, {5, "five"}, {6, "six"}})

	// Set up and run a capture of Table A only
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueA))
	t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Add data to table A and truncate table B. Captures should still succeed because we
	// don't care about truncates to non-active tables.
	tb.Insert(ctx, t, tableA, [][]interface{}{{7, "seven"}, {8, "eight"}, {9, "nine"}})
	tb.Query(ctx, t, fmt.Sprintf("TRUNCATE TABLE %s;", tableB))
	t.Run("capture1", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Truncating table A will cause the capture to fail though, as it should.
	tb.Query(ctx, t, fmt.Sprintf("TRUNCATE TABLE %s;", tableA))
	tb.Insert(ctx, t, tableA, [][]interface{}{{10, "ten"}, {11, "eleven"}, {12, "twelve"}})
	t.Run("capture2", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

func TestTrickyColumnNames(t *testing.T) {
	// Create a table with some 'difficult' column names (a reserved word, a capitalized
	// name, and one containing special characters which also happens to be the primary key).
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueA, uniqueB = "39256824", "42531495"
	var tableA = tb.CreateTable(ctx, t, uniqueA, `("Meta/""wtf""~ID" INTEGER PRIMARY KEY, data TEXT)`)
	var tableB = tb.CreateTable(ctx, t, uniqueB, `("table" INTEGER PRIMARY KEY, data TEXT)`)
	tb.Insert(ctx, t, tableA, [][]interface{}{{1, "aaa"}, {2, "bbb"}})
	tb.Insert(ctx, t, tableB, [][]interface{}{{3, "ccc"}, {4, "ddd"}})

	// Discover the catalog and verify that the table schemas looks correct
	t.Run("discover", func(t *testing.T) {
		tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueA), regexp.MustCompile(uniqueB))
	})

	// Perform an initial backfill
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueA), regexp.MustCompile(uniqueB))
	t.Run("backfill", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Add more data and read it via replication
	tb.Insert(ctx, t, tableA, [][]interface{}{{5, "eee"}, {6, "fff"}})
	tb.Insert(ctx, t, tableB, [][]interface{}{{7, "ggg"}, {8, "hhh"}})
	t.Run("replication", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

// TestCursorResume sets up a capture with a (string, int) primary key and
// and repeatedly restarts it after each row of capture output.
func TestCursorResume(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = "95911555"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(epoch VARCHAR(8), count INTEGER, data TEXT, PRIMARY KEY (epoch, count))")
	tb.Insert(ctx, t, tableName, [][]interface{}{
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
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = "86827053"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(year INTEGER, state TEXT, fullname TEXT, population INTEGER, PRIMARY KEY (year, state))")
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

	tb.Insert(ctx, t, tableName, [][]interface{}{
		{1930, "XX", "No Such State", 1234},   // An insert prior to the first restart, which will be reported once replication begins
		{1970, "XX", "No Such State", 12345},  // An insert between the two restarts, which will be visible in the table scan and should be filtered during replication
		{1990, "XX", "No Such State", 123456}, // An insert after the second restart, which will be visible in the table scan and should be filtered during replication
	})
	t.Run("restart1", func(t *testing.T) {
		var summary, states = tests.RestartingBackfillCapture(ctx, t, cs)
		cupaloy.SnapshotT(t, summary)
		cs.Checkpoint = states[10] // Next restart in the middle of 1980 data
	})

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

	t.Run("restart2", func(t *testing.T) {
		var summary, _ = tests.RestartingBackfillCapture(ctx, t, cs)
		cupaloy.SnapshotT(t, summary)
	})
}

func TestUserTypes(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()

	t.Run("Domain", func(t *testing.T) {
		tb.Query(ctx, t, `DROP DOMAIN IF EXISTS UserDomain CASCADE`)
		tb.Query(ctx, t, `CREATE DOMAIN UserDomain AS TEXT`)
		t.Cleanup(func() { tb.Query(ctx, t, `DROP DOMAIN IF EXISTS UserDomain CASCADE`) })

		var uniqueID = "68961947"
		var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, value UserDomain)")
		t.Run("Discovery", func(t *testing.T) {
			var cs = tb.CaptureSpec(ctx, t)
			cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
		})

		t.Run("Capture", func(t *testing.T) {
			tb.Insert(ctx, t, tableName, [][]any{{1, "hello"}, {2, "world"}})
			var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
			tests.VerifiedCapture(ctx, t, cs)
			t.Run("Replication", func(t *testing.T) {
				tb.Insert(ctx, t, tableName, [][]any{{3, "foo"}, {4, "bar"}, {5, "baz"}})
				tests.VerifiedCapture(ctx, t, cs)
			})
		})
	})

	t.Run("Enum", func(t *testing.T) {
		tb.Query(ctx, t, `DROP TYPE IF EXISTS UserEnum CASCADE`)
		tb.Query(ctx, t, `CREATE TYPE UserEnum AS ENUM ('red', 'green', 'blue')`)
		t.Cleanup(func() { tb.Query(ctx, t, `DROP TYPE UserEnum CASCADE`) })

		var uniqueID = "64812435"
		var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, value UserEnum)")
		t.Run("Discovery", func(t *testing.T) {
			var cs = tb.CaptureSpec(ctx, t)
			cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
		})
		t.Run("Capture", func(t *testing.T) {
			tb.Insert(ctx, t, tableName, [][]any{{1, "red"}, {2, "green"}, {3, "blue"}})
			var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
			tests.VerifiedCapture(ctx, t, cs)
			t.Run("Replication", func(t *testing.T) {
				tb.Insert(ctx, t, tableName, [][]any{{4, "blue"}, {5, "red"}, {6, "green"}})
				tests.VerifiedCapture(ctx, t, cs)
			})
		})
	})

	t.Run("Tuple", func(t *testing.T) {
		tb.Query(ctx, t, `DROP TYPE IF EXISTS UserTuple CASCADE`)
		tb.Query(ctx, t, `CREATE TYPE UserTuple AS (epoch INTEGER, count INTEGER, data TEXT)`)
		t.Cleanup(func() { tb.Query(ctx, t, `DROP TYPE UserTuple CASCADE`) })

		var uniqueID = "51424093"
		var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, value UserTuple)")

		t.Run("Discovery", func(t *testing.T) {
			var cs = tb.CaptureSpec(ctx, t)
			cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
		})

		t.Run("Capture", func(t *testing.T) {
			tb.Insert(ctx, t, tableName, [][]any{
				{1, "(1234, 5678, 'hello')"},
				{2, "(3456, 9876, 'world')"},
			})
			var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
			tests.VerifiedCapture(ctx, t, cs)
			t.Run("Replication", func(t *testing.T) {
				tb.Insert(ctx, t, tableName, [][]any{
					{3, "(34, 64, 'asdf')"},
					{4, "(83, 12, 'fdsa')"},
				})
				tests.VerifiedCapture(ctx, t, cs)
			})
		})
	})

	t.Run("Range", func(t *testing.T) {
		tb.Query(ctx, t, `DROP TYPE IF EXISTS UserRange CASCADE`)
		tb.Query(ctx, t, `CREATE TYPE UserRange AS RANGE (subtype = int4)`)
		t.Cleanup(func() { tb.Query(ctx, t, `DROP TYPE UserRange CASCADE`) })

		var uniqueID = "91324557"
		var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, value UserRange)")

		t.Run("Discovery", func(t *testing.T) {
			var cs = tb.CaptureSpec(ctx, t)
			cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
		})

		t.Run("Capture", func(t *testing.T) {
			tb.Insert(ctx, t, tableName, [][]any{
				{1, "(1, 2]"},
				{2, "[3,)"},
			})
			var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
			tests.VerifiedCapture(ctx, t, cs)
			t.Run("Replication", func(t *testing.T) {
				tb.Insert(ctx, t, tableName, [][]any{
					{3, "(,4]"},
					{4, "[5,6)"},
				})
				tests.VerifiedCapture(ctx, t, cs)
			})
		})
	})
}

func TestCaptureCapitalization(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()

	var uniqueA, uniqueB = "69943814", "73423348"
	var tablePrefix = strings.TrimPrefix(t.Name(), "Test")
	var tableA = tablePrefix + "_AaAaA_" + uniqueA                  // Name containing capital letters
	var tableB = strings.ToLower(tablePrefix + "_BbBbB_" + uniqueB) // Name which is all lowercase (like all our other test table names)

	var cleanup = func() {
		tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE IF EXISTS "%s"."%s";`, testSchemaName, tableA))
		tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE IF EXISTS "%s"."%s";`, testSchemaName, tableB))
	}
	cleanup()
	t.Cleanup(cleanup)

	tb.Query(ctx, t, fmt.Sprintf(`CREATE TABLE "%s"."%s" (id INTEGER PRIMARY KEY, data TEXT);`, testSchemaName, tableA))
	tb.Query(ctx, t, fmt.Sprintf(`CREATE TABLE "%s"."%s" (id INTEGER PRIMARY KEY, data TEXT);`, testSchemaName, tableB))

	tb.Query(ctx, t, fmt.Sprintf(`INSERT INTO "%s"."%s" VALUES (0, 'hello'), (1, 'asdf');`, testSchemaName, tableA))
	tb.Query(ctx, t, fmt.Sprintf(`INSERT INTO "%s"."%s" VALUES (2, 'world'), (3, 'fdsa');`, testSchemaName, tableB))

	tests.VerifiedCapture(ctx, t, tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueA), regexp.MustCompile(uniqueB)))
}

func TestCaptureOversizedFields(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = "64819605"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, tdata TEXT, bdata BYTEA, jdata JSON, jbdata JSONB)")
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
