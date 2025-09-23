package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/connectors/sqlcapture/tests"
	"github.com/jackc/pglogrepl"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
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
	if *dbCaptureAddress != *dbControlAddress {
		// If the database used for test control operations is not the same database
		// we're capturing from (that is, we're testing some sort of replicated setup)
		// then issuing a query against pg_replication_slots using the test control
		// connection won't tell us whether the replica slot is advancing, so this
		// test will fail even if it's working correctly.
		//
		// In theory the test could be rewritten to establish a temporary control
		// connection to the capture database and check there, but it's not worth
		// the effort in general since we know this logic works in non-replicated
		// setups and I've verified it manually in the standby replica scenario.
		t.Skip("skipping test in replicated test scenario")
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
		var summary, _ = tests.RestartingBackfillCapture(ctx, t, cs)
		cupaloy.SnapshotT(t, summary)

		// Rewind the backfill state to a specific reproducible point
		var state sqlcapture.PersistentState
		require.NoError(t, json.Unmarshal(cs.Checkpoint, &state))
		state.Streams["test%2Fcomplexdataset_86827053"].BackfilledCount = 130
		state.Streams["test%2Fcomplexdataset_86827053"].Mode = sqlcapture.TableStateUnfilteredBackfill
		state.Streams["test%2Fcomplexdataset_86827053"].Scanned = []byte{0x16, 0x07, 0x94, 0x02, 0x4e, 0x56, 0x00}
		var bs, err = json.Marshal(&state)
		require.NoError(t, err)
		cs.Checkpoint = bs
	})

	tb.Insert(ctx, t, tableName, [][]interface{}{
		{1930, "XX", "No Such State", 1234},   // An insert prior to the first restart, which will be reported once replication begins
		{1970, "XX", "No Such State", 12345},  // An insert between the two restarts, which will be visible in the table scan and should be filtered during replication
		{1990, "XX", "No Such State", 123456}, // An insert after the second restart, which will be visible in the table scan and should be filtered during replication
	})
	t.Run("restart1", func(t *testing.T) {
		var summary, _ = tests.RestartingBackfillCapture(ctx, t, cs)
		cupaloy.SnapshotT(t, summary)

		// Rewind the backfill state to a specific reproducible point
		var state sqlcapture.PersistentState
		require.NoError(t, json.Unmarshal(cs.Checkpoint, &state))
		state.Streams["test%2Fcomplexdataset_86827053"].BackfilledCount = 230
		state.Streams["test%2Fcomplexdataset_86827053"].Mode = sqlcapture.TableStateUnfilteredBackfill
		state.Streams["test%2Fcomplexdataset_86827053"].Scanned = []byte{0x16, 0x07, 0xbc, 0x02, 0x4e, 0x48, 0x00}
		var bs, err = json.Marshal(&state)
		require.NoError(t, err)
		cs.Checkpoint = bs
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

	var largeText = strings.Repeat("data", 4194304)          // 16MiB string
	var largeJSON = fmt.Sprintf(`{"text": "%s"}`, largeText) // ~16MiB JSON object
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

func TestCaptureAfterSlotDropped(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = "46115540"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

	// Run a normal capture
	tb.Insert(ctx, t, tableName, [][]any{{0, "zero"}, {1, "one"}})
	t.Run("capture1", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Drop the replication slot while the task is offline. At startup it should
	// fail because it has a non-empty resume cursor but the slot no longer exists.
	tb.Insert(ctx, t, tableName, [][]any{{2, "two"}, {3, "three"}})
	tb.Query(ctx, t, "SELECT pg_drop_replication_slot('flow_slot');")
	tb.Insert(ctx, t, tableName, [][]any{{4, "four"}, {5, "five"}})
	t.Run("capture2", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// A subsequent capture run should still be failing since we haven't fixed it.
	tb.Insert(ctx, t, tableName, [][]any{{6, "six"}, {7, "seven"}})
	t.Run("capture3", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Append a version to the state key to simulate bumping the backfill counter for this binding.
	cs.Bindings[0].StateKey += ".v2"
	t.Run("capture4", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

// TestCaptureDomainJSONB exercises an edge case where a user-defined domain type
// has a concrete type which uses a custom decoder registered in the PGX type map.
func TestCaptureDomainJSONB(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = "40925847"

	tb.Query(ctx, t, `DROP DOMAIN IF EXISTS UserDomain CASCADE`)
	tb.Query(ctx, t, `CREATE DOMAIN UserDomain AS JSONB`)
	t.Cleanup(func() { tb.Query(ctx, t, `DROP DOMAIN IF EXISTS UserDomain CASCADE`) })

	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data UserDomain NOT NULL)")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

	tb.Insert(ctx, t, tableName, [][]any{{0, `{}`}, {1, `{"foo": "bar"}`}})
	t.Run("capture1", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
	tb.Insert(ctx, t, tableName, [][]any{{2, `{"baz": [1, 2, 3]}`}, {3, `{"asdf": {"a": 1, "b": 2}}`}})
	t.Run("capture2", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

func TestDroppedAndRecreatedTable(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = "37815596"
	var tableDef = "(id INTEGER PRIMARY KEY, data TEXT)"
	var tableName = tb.CreateTable(ctx, t, uniqueID, tableDef)

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = &st.OrderedCaptureValidator{}
	sqlcapture.TestShutdownAfterCaughtUp = true
	t.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = false })

	// Initial backfill
	tb.Insert(ctx, t, tableName, [][]any{{0, "zero"}, {1, "one"}, {2, "two"}})
	cs.Capture(ctx, t, nil)

	// Some replication
	tb.Insert(ctx, t, tableName, [][]any{{3, "three"}, {4, "four"}, {5, "five"}})
	cs.Capture(ctx, t, nil)

	// Drop and recreate the table, then fill it with some new data.
	tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE %s;`, tableName))
	tb.Query(ctx, t, fmt.Sprintf(`CREATE TABLE %s%s;`, tableName, tableDef))
	tb.Insert(ctx, t, tableName, [][]any{{0, "zero"}, {1, "one"}, {2, "two"}})
	tb.Insert(ctx, t, tableName, [][]any{{6, "six"}, {7, "seven"}, {8, "eight"}})
	cs.Capture(ctx, t, nil)

	// Followed by some more replication
	tb.Insert(ctx, t, tableName, [][]any{{9, "nine"}, {10, "ten"}, {11, "eleven"}})
	cs.Capture(ctx, t, nil)

	cupaloy.SnapshotT(t, cs.Summary())
}

func TestCIText(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = "58810479"
	var tableDef = "(id INTEGER PRIMARY KEY, data CITEXT, arr CITEXT[])"
	var tableName = tb.CreateTable(ctx, t, uniqueID, tableDef)

	t.Run("Discovery", func(t *testing.T) {
		var cs = tb.CaptureSpec(ctx, t)
		cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
	})

	t.Run("Capture", func(t *testing.T) {
		var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
		cs.Validator = &st.OrderedCaptureValidator{}
		sqlcapture.TestShutdownAfterCaughtUp = true
		t.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = false })

		// Initial backfill
		tb.Insert(ctx, t, tableName, [][]any{{0, "zero", "{a,b}"}, {1, "one", "{c,d}"}, {2, "two", "{e,f}"}})
		cs.Capture(ctx, t, nil)

		// Some replication
		tb.Insert(ctx, t, tableName, [][]any{{3, "three", "{g,h}"}, {4, "four", "{i,j}"}, {5, "five", "{k,l}"}})
		cs.Capture(ctx, t, nil)

		// Snapshot
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

func TestPrimaryKeyUpdate(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = "63510878"
	var tableDef = "(id INTEGER PRIMARY KEY, data TEXT)"
	var tableName = tb.CreateTable(ctx, t, uniqueID, tableDef)

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = &st.OrderedCaptureValidator{}
	sqlcapture.TestShutdownAfterCaughtUp = true
	t.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = false })

	// Initial backfill
	tb.Insert(ctx, t, tableName, [][]any{{0, "zero"}, {1, "one"}, {2, "two"}})
	cs.Capture(ctx, t, nil)

	// Some replication
	tb.Insert(ctx, t, tableName, [][]any{{3, "three"}, {4, "four"}, {5, "five"}})
	cs.Capture(ctx, t, nil)

	// Primary key updates
	tb.Update(ctx, t, tableName, "id", 1, "id", 6)
	tb.Update(ctx, t, tableName, "id", 4, "id", 7)
	cs.Capture(ctx, t, nil)

	cupaloy.SnapshotT(t, cs.Summary())
}

func TestGeneratedColumn(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = "91418628"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, a VARCHAR(32), b VARCHAR(32), generated VARCHAR(64) GENERATED ALWAYS AS (COALESCE(a, b)) STORED)")

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = &st.OrderedCaptureValidator{}
	t.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = false })
	sqlcapture.TestShutdownAfterCaughtUp = true

	t.Run("discovery", func(t *testing.T) {
		cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
	})

	t.Run("capture", func(t *testing.T) {
		tb.Insert(ctx, t, tableName, [][]any{{0, "a0", "b0"}, {1, nil, "b1"}, {2, "a2", nil}, {3, nil, nil}})
		cs.Capture(ctx, t, nil)
		tb.Insert(ctx, t, tableName, [][]any{{4, "a4", "b4"}, {5, nil, "b5"}, {6, "a6", nil}, {7, nil, nil}})
		cs.Capture(ctx, t, nil)
		tb.Update(ctx, t, tableName, "id", 4, "a", "a4-modified")
		tb.Update(ctx, t, tableName, "id", 5, "b", "b5-modified")
		tb.Update(ctx, t, tableName, "id", 6, "a", "a6-modified")
		cs.Capture(ctx, t, nil)
		tb.Delete(ctx, t, tableName, "id", 4)
		tb.Delete(ctx, t, tableName, "id", 5)
		tb.Delete(ctx, t, tableName, "id", 6)
		tb.Delete(ctx, t, tableName, "id", 7)
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestFeatureFlagFlattenArrays exercises the handling of array columns in both
// discovery and captures, with the 'flatten_arrays' feature flag explicitly
// enabled and disabled.
func TestFeatureFlagFlattenArrays(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = "70143951"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, text_array TEXT[], int_array INTEGER[], nested_array INTEGER[][])")

	tb.Insert(ctx, t, tableName, [][]any{
		{1, []string{"a", "b", "c"}, []int{1, 2, 3}, [][]int{{1, 2}, {3, 4}}},
		{2, []string{"foo", "bar"}, []int{10, 20}, [][]int{{5, 6}, {7, 8}}},
		{3, []string{}, []int{}, [][]int{{}}},
		{4, nil, nil, nil},
	})

	for _, tc := range []struct {
		name string
		flag string
	}{
		{"Enabled", "flatten_arrays"},
		{"Disabled", "no_flatten_arrays"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cs = tb.CaptureSpec(ctx, t)
			cs.EndpointSpec.(*Config).Advanced.FeatureFlags = tc.flag

			t.Run("Discovery", func(t *testing.T) {
				cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
			})

			t.Run("Capture", func(t *testing.T) {
				cs.Bindings = tests.DiscoverBindings(ctx, t, tb, regexp.MustCompile(uniqueID))
				cs.Validator = &st.OrderedCaptureValidator{}
				sqlcapture.TestShutdownAfterCaughtUp = true
				t.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = false })

				cs.Capture(ctx, t, nil)
				cupaloy.SnapshotT(t, cs.Summary())
			})
		})
	}
}

func TestXMINBackfill(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = "60228969"
	var tableDef = "(id INTEGER PRIMARY KEY, data TEXT)"
	var tableName = tb.CreateTable(ctx, t, uniqueID, tableDef)

	// Insert some initial rows, then establish the current server XID.
	tb.Insert(ctx, t, tableName, [][]any{{0, "zero"}, {1, "one"}, {2, "two"}})

	var lowerXID uint64
	const queryXID = "SELECT txid_snapshot_xmin(txid_current_snapshot())"
	require.NoError(t, tb.control.QueryRow(ctx, queryXID).Scan(&lowerXID))

	// Changes from after the minimum backfill XID can be observed.
	tb.Insert(ctx, t, tableName, [][]any{{3, "three"}, {4, "four"}, {5, "five"}})
	tb.Delete(ctx, t, tableName, "id", 0) // Except this delete won't be, since it's a delete
	tb.Update(ctx, t, tableName, "id", 1, "data", "one-modified")

	// Run the capture and verify results
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = &st.OrderedCaptureValidator{}
	cs.EndpointSpec.(*Config).Advanced.MinimumBackfillXID = fmt.Sprintf("%d", uint32(lowerXID))
	tests.VerifiedCapture(ctx, t, cs)
}

func TestMultidimensionalArrays(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = "82375147"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, arr TEXT[])")

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = &st.OrderedCaptureValidator{}
	cs.EndpointSpec.(*Config).Advanced.FeatureFlags = "multidimensional_arrays"
	t.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = false })
	sqlcapture.TestShutdownAfterCaughtUp = true

	t.Run("Discovery", func(t *testing.T) {
		cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
	})

	t.Run("Capture", func(t *testing.T) {
		tb.Insert(ctx, t, tableName, [][]any{
			{1, nil},
			{2, []string{}},
			{3, [][]string{}},
			{4, []string{"x"}},
			{5, [][][]string{{{"x"}}}},
			{6, []string{"a", "b", "c", "d"}},
			{7, [][]string{{"a", "b"}, {"c", "d"}}},
			{8, [][][]string{{{"a", "b", "c", "d"}, {"e", "f", "g", "h"}, {"i", "j", "k", "l"}}, {{"m", "n", "o", "p"}, {"q", "r", "s", "t"}, {"u", "v", "w", "x"}}}},                           // 2x3x4 array
			{9, [][][][]string{{{{"a", "b"}, {"c", "d"}}, {{"e", "f"}, {"g", "h"}}, {{"i", "j"}, {"k", "l"}}}, {{{"m", "n"}, {"o", "p"}}, {{"q", "r"}, {"s", "t"}}, {{"u", "v"}, {"w", "x"}}}}}, // 2x3x2x2 array
		})
		cs.Capture(ctx, t, nil)

		tb.Insert(ctx, t, tableName, [][]any{
			{11, nil},
			{12, []string{}},
			{13, [][]string{}},
			{14, []string{"x"}},
			{15, [][][]string{{{"x"}}}},
			{16, []string{"a", "b", "c", "d"}},
			{17, [][]string{{"a", "b"}, {"c", "d"}}},
			{18, [][][]string{{{"a", "b", "c", "d"}, {"e", "f", "g", "h"}, {"i", "j", "k", "l"}}, {{"m", "n", "o", "p"}, {"q", "r", "s", "t"}, {"u", "v", "w", "x"}}}},                           // 2x3x4 array
			{19, [][][][]string{{{{"a", "b"}, {"c", "d"}}, {{"e", "f"}, {"g", "h"}}, {{"i", "j"}, {"k", "l"}}}, {{{"m", "n"}, {"o", "p"}}, {{"q", "r"}, {"s", "t"}}, {{"u", "v"}, {"w", "x"}}}}}, // 2x3x2x2 array
		})
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())

	})
}

// TestFeatureFlagEmitSourcedSchemas runs a capture with the `emit_sourced_schemas` feature flag set.
func TestFeatureFlagEmitSourcedSchemas(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = "64029092"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data VARCHAR(32))")

	tb.Insert(ctx, t, tableName, [][]any{{1, "hello"}, {2, "world"}})

	for _, tc := range []struct {
		name string
		flag string
	}{
		{"Default", ""},
		{"Enabled", "emit_sourced_schemas"},
		{"Disabled", "no_emit_sourced_schemas"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cs = tb.CaptureSpec(ctx, t)
			cs.EndpointSpec.(*Config).Advanced.FeatureFlags = tc.flag
			cs.Bindings = tests.DiscoverBindings(ctx, t, tb, regexp.MustCompile(uniqueID))

			sqlcapture.TestShutdownAfterCaughtUp = true
			t.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = false })

			cs.Capture(ctx, t, nil)
			cupaloy.SnapshotT(t, cs.Summary())
		})
	}
}

// TestPartitionedCTIDBackfill tests the handling of partitioned tables with CTID-based backfill.
func TestPartitionedCTIDBackfill(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = uniqueTableID(t)
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT) PARTITION BY RANGE (id)")

	// Create partitions and insert a bunch of test rows spanning all three
	tb.Query(ctx, t, fmt.Sprintf(`CREATE TABLE %[1]s_1 PARTITION OF %[1]s FOR VALUES FROM (0) TO (1000000);`, tableName))
	tb.Query(ctx, t, fmt.Sprintf(`CREATE TABLE %[1]s_2 PARTITION OF %[1]s FOR VALUES FROM (1000000) TO (2000000);`, tableName))
	tb.Query(ctx, t, fmt.Sprintf(`CREATE TABLE %[1]s_3 PARTITION OF %[1]s FOR VALUES FROM (2000000) TO (3000000);`, tableName))
	tb.Query(ctx, t, fmt.Sprintf(`INSERT INTO %s SELECT count.n, 'data value ' || count.n FROM generate_series(1,2999999,1) count(n)`, tableName))

	// Run a CTID backfill of the root table
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = &st.ChecksumValidator{}
	cs.EndpointSpec.(*Config).Advanced.BackfillChunkSize = 50000 // Default prod chunk size since this table has millions of rows
	setShutdownAfterCaughtUp(t, true)
	setResourceBackfillMode(t, cs.Bindings[0], sqlcapture.BackfillModeWithoutKey)
	cs.Capture(ctx, t, nil)
	cupaloy.SnapshotT(t, cs.Summary())
}

// TestMessageOverflow tests the handling of a message exceeding the replication buffer overflow threshold.
func TestMessageOverflow(t *testing.T) {
	t.Skip("skipping until wgd rethinks limits here")
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = uniqueTableID(t)
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	setShutdownAfterCaughtUp(t, true)
	tb.Query(ctx, t, fmt.Sprintf(`INSERT INTO %s VALUES (0, 'zero')`, tableName))
	cs.Capture(ctx, t, nil)
	tb.Query(ctx, t, fmt.Sprintf(`INSERT INTO %s VALUES (1, 'one')`, tableName))
	tb.Query(ctx, t, fmt.Sprintf(`INSERT INTO %s VALUES (2, repeat('x', 431*1024*1024))`, tableName))
	tb.Query(ctx, t, fmt.Sprintf(`INSERT INTO %s VALUES (3, 'three')`, tableName))
	cs.Capture(ctx, t, nil)
	cupaloy.SnapshotT(t, cs.Summary())
}

// TestCaptureAsPartitions verifies that the "Capture Partitioned Tables as Partitions" advanced setting works as intended.
func TestCaptureAsPartitions(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = uniqueTableID(t)
	var rootName = tb.CreateTable(ctx, t, uniqueID, "(logdate DATE PRIMARY KEY, value TEXT) PARTITION BY RANGE (logdate)")

	var cleanup = func() {
		tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE IF EXISTS %s_2023q1;`, rootName))
		tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE IF EXISTS %s_2023q2;`, rootName))
		tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE IF EXISTS %s_2023q3;`, rootName))
		tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE IF EXISTS %s_2023q4;`, rootName))
	}
	cleanup()
	tb.Query(ctx, t, fmt.Sprintf(`CREATE TABLE %[1]s_2023q1 PARTITION OF %[1]s FOR VALUES FROM ('2023-01-01') TO ('2023-04-01');`, rootName))
	tb.Query(ctx, t, fmt.Sprintf(`CREATE TABLE %[1]s_2023q2 PARTITION OF %[1]s FOR VALUES FROM ('2023-04-01') TO ('2023-07-01');`, rootName))
	tb.Query(ctx, t, fmt.Sprintf(`CREATE TABLE %[1]s_2023q3 PARTITION OF %[1]s FOR VALUES FROM ('2023-07-01') TO ('2023-10-01');`, rootName))
	tb.Query(ctx, t, fmt.Sprintf(`CREATE TABLE %[1]s_2023q4 PARTITION OF %[1]s FOR VALUES FROM ('2023-10-01') TO ('2024-01-01');`, rootName))
	t.Cleanup(cleanup)

	// Recreate the publication without the `publish_via_partition_root` flag for this test only
	t.Cleanup(func() {
		tb.Query(ctx, t, `DROP PUBLICATION IF EXISTS flow_publication;`)
		tb.Query(ctx, t, `CREATE PUBLICATION flow_publication FOR ALL TABLES;`)
		tb.Query(ctx, t, `ALTER PUBLICATION flow_publication SET (publish_via_partition_root = true);`)
	})
	tb.Query(ctx, t, `DROP PUBLICATION IF EXISTS flow_publication;`)
	tb.Query(ctx, t, `CREATE PUBLICATION flow_publication FOR ALL TABLES;`)

	// Test discovery with CaptureAsPartitions enabled
	var cs = tb.CaptureSpec(ctx, t)
	cs.EndpointSpec.(*Config).Advanced.CaptureAsPartitions = true
	var bindings = cs.Discover(ctx, t, regexp.MustCompile(uniqueID))
	t.Run("Discover", func(t *testing.T) { cupaloy.SnapshotT(t, st.SummarizeBindings(t, bindings)) })

	// Test capture with CaptureAsPartitions enabled
	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterCaughtUp(t, true)
		cs.Bindings = tests.ConvertBindings(t, bindings)

		// Insert test data into the partitions for backfill
		tb.Insert(ctx, t, rootName, [][]any{
			{"2023-01-15", "Q1 data 1"},
			{"2023-02-20", "Q1 data 2"},
			{"2023-05-10", "Q2 data 1"},
			{"2023-06-15", "Q2 data 2"},
			{"2023-08-05", "Q3 data 1"},
			{"2023-09-25", "Q3 data 2"},
			{"2023-11-12", "Q4 data 1"},
			{"2023-12-28", "Q4 data 2"},
		})
		cs.Capture(ctx, t, nil)

		// Insert more test data into the partitions for replication
		tb.Insert(ctx, t, rootName, [][]any{
			{"2023-03-01", "Q1 replication"},
			{"2023-04-01", "Q2 replication"},
			{"2023-07-01", "Q3 replication"},
			{"2023-10-01", "Q4 replication"},
		})
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
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
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = uniqueTableID(t)
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data JSON)")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	setShutdownAfterCaughtUp(t, true)

	// Test values
	var vals = []string{
		`{"text": "normal"}`,
		`{"text": "\ud83d\u200b\ude14"}`,
		`{"text": "\uDeAd"}`,
		`{"\uDeAd": "\ud83d\udE14"}`,
		`{"text": "foo \uDEAD bar \uDEAD baz"}`,
		`[{"type":"text","text":"foo \"bar\\udfs\" /baz"}]`,
	}

	// Backfill
	for idx, val := range vals {
		tb.Insert(ctx, t, tableName, [][]any{{100 + idx, val}})
	}
	cs.Capture(ctx, t, nil)

	// Replication
	for idx, val := range vals {
		tb.Insert(ctx, t, tableName, [][]any{{200 + idx, val}})
	}
	cs.Capture(ctx, t, nil)
	cupaloy.SnapshotT(t, cs.Summary())
}

// TestSourceTag verifies the output of a capture with /advanced/source_tag set
func TestSourceTag(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = uniqueTableID(t)
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.EndpointSpec.(*Config).Advanced.SourceTag = "example_source_tag_1234"
	setShutdownAfterCaughtUp(t, true)
	tb.Insert(ctx, t, tableName, [][]any{{0, "zero"}, {1, "one"}})
	cs.Capture(ctx, t, nil)
	tb.Insert(ctx, t, tableName, [][]any{{2, "two"}, {3, "three"}})
	cs.Capture(ctx, t, nil)
	cupaloy.SnapshotT(t, cs.Summary())
}

// TestBackfillPriority checks that tables with higher priority values are
// backfilled completely before tables with lower priority values.
func TestBackfillPriority(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = uniqueTableID(t)

	// Create five tables with 20 rows each
	var tableHi1 = tb.CreateTable(ctx, t, uniqueID+"_hi1", "(id INTEGER PRIMARY KEY, data TEXT)")
	var tableHi2 = tb.CreateTable(ctx, t, uniqueID+"_hi2", "(id INTEGER PRIMARY KEY, data TEXT)")
	var tableDef = tb.CreateTable(ctx, t, uniqueID+"_def", "(id INTEGER PRIMARY KEY, data TEXT)")
	var tableLo1 = tb.CreateTable(ctx, t, uniqueID+"_lo1", "(id INTEGER PRIMARY KEY, data TEXT)")
	var tableLo2 = tb.CreateTable(ctx, t, uniqueID+"_lo2", "(id INTEGER PRIMARY KEY, data TEXT)")
	for _, table := range []string{tableHi1, tableHi2, tableDef, tableLo1, tableLo2} {
		var rows [][]any
		for i := range 20 {
			rows = append(rows, []any{i, fmt.Sprintf("Row %d in %s", i, table)})
		}
		tb.Insert(ctx, t, table, rows)
	}

	// Create capture spec with small backfill chunk size
	var cs = tb.CaptureSpec(ctx, t)
	cs.EndpointSpec.(*Config).Advanced.BackfillChunkSize = 5

	// Discover bindings for all five tables
	var discoveredBindings = cs.Discover(ctx, t, regexp.MustCompile(uniqueID))
	var bindings = tests.ConvertBindings(t, discoveredBindings)
	require.Len(t, bindings, 5)

	// Assign priorities to the bindings:
	// - Two tables with priority 10 (high priority)
	// - One table with default priority (0)
	// - Two tables with priority -10 (low priority)
	for _, b := range bindings {
		var resource = sqlcapture.Resource{}
		require.NoError(t, json.Unmarshal(b.ResourceConfigJson, &resource))

		if strings.Contains(b.ResourcePath[1], "_hi") {
			resource.Priority = 10
		} else if strings.Contains(b.ResourcePath[1], "_lo") {
			resource.Priority = -10
		}

		var resourceJSON, err = json.Marshal(resource)
		require.NoError(t, err)
		b.ResourceConfigJson = resourceJSON
	}
	cs.Bindings = bindings

	setShutdownAfterCaughtUp(t, true)
	cs.Capture(ctx, t, nil)
	cupaloy.SnapshotT(t, cs.Summary())
}
