package tests

import (
	"context"
	"testing"
	"time"

	"github.com/estuary/connectors/sqlcapture"
)

// Run executes the generic SQL capture test suite
func Run(ctx context.Context, t *testing.T, tb TestBackend) {
	t.Helper()
	t.Run("SimpleDiscovery", func(t *testing.T) { testSimpleDiscovery(ctx, t, tb) })
	t.Run("SimpleCapture", func(t *testing.T) { testSimpleCapture(ctx, t, tb) })
	t.Run("Tailing", func(t *testing.T) { testTailing(ctx, t, tb) })
	t.Run("ReplicationInserts", func(t *testing.T) { testReplicationInserts(ctx, t, tb) })
	t.Run("ReplicationUpdates", func(t *testing.T) { testReplicationUpdates(ctx, t, tb) })
	t.Run("ReplicationDeletes", func(t *testing.T) { testReplicationDeletes(ctx, t, tb) })
	t.Run("EmptyTable", func(t *testing.T) { testEmptyTable(ctx, t, tb) })
	t.Run("IgnoredStreams", func(t *testing.T) { testIgnoredStreams(ctx, t, tb) })
	t.Run("MultipleStreams", func(t *testing.T) { testMultipleStreams(ctx, t, tb) })
	t.Run("ComplexDataset", func(t *testing.T) { testComplexDataset(ctx, t, tb) })
	t.Run("CatalogPrimaryKey", func(t *testing.T) { testCatalogPrimaryKey(ctx, t, tb) })
	t.Run("CatalogPrimaryKeyOverride", func(t *testing.T) { testCatalogPrimaryKeyOverride(ctx, t, tb) })
}

// testSimpleDiscovery creates a new table in the database, performs stream discovery,
// and then verifies both that a stream with the expected name exists, and that it
// matches a golden snapshot.
func testSimpleDiscovery(ctx context.Context, t *testing.T, tb TestBackend) {
	var tableName = tb.CreateTable(ctx, t, "", "(a INTEGER PRIMARY KEY, b TEXT, c REAL NOT NULL, d VARCHAR(255))")

	// Create the table (with deferred cleanup), perform discovery, and verify
	// both that a stream with the expected name is discovered, and that it matches
	// the golden snapshot.
	var catalog, err = sqlcapture.DiscoverCatalog(ctx, tb.GetDatabase())
	if err != nil {
		t.Fatal(err)
	}
	VerifyStream(t, "", catalog, tableName)
}

// testSimpleCapture initializes a DB table with a few rows, then runs a capture
// which should emit all rows during table-scanning, start replication, and then
// shut down due to a lack of further events.
func testSimpleCapture(ctx context.Context, t *testing.T, tb TestBackend) {
	var tableName = tb.CreateTable(ctx, t, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	var catalog, state = ConfiguredCatalog(ctx, t, tb, tableName), sqlcapture.PersistentState{}
	tb.Insert(ctx, t, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}, {3, "Four"}, {4, "5"}})
	VerifiedCapture(ctx, t, tb, &catalog, &state, "")
}

// TestTailing performs a capture in 'tail' mode, which is how it actually runs
// under Flow outside of development tests. This means that after the initial
// backfilling completes, the capture will run indefinitely without writing
// any more watermarks.
func testTailing(ctx context.Context, t *testing.T, tb TestBackend) {
	var tableName = tb.CreateTable(ctx, t, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	var catalog, state = ConfiguredCatalog(ctx, t, tb, tableName), sqlcapture.PersistentState{}
	catalog.Tail = true

	// Initial data which must be backfilled
	tb.Insert(ctx, t, tableName, [][]interface{}{{0, "A"}, {10, "bbb"}, {20, "CDEFGHIJKLMNOP"}, {30, "Four"}, {40, "5"}})

	// Run the capture
	var captureCtx, cancelCapture = context.WithCancel(ctx)
	go VerifiedCapture(captureCtx, t, tb, &catalog, &state, "")
	time.Sleep(1 * time.Second)

	// Some more changes occurring after the backfill completes
	tb.Insert(ctx, t, tableName, [][]interface{}{{5, "asdf"}, {100, "lots"}})
	tb.Delete(ctx, t, tableName, "id", 20)
	tb.Update(ctx, t, tableName, "id", 30, "data", "updated")

	// Let the capture catch up and then terminate it
	time.Sleep(1 * time.Second)
	cancelCapture()
}

// testReplicationInserts runs two captures, where the first will perform the
// initial table scan and the second capture will use replication to receive
// additional inserts performed after the first capture.
func testReplicationInserts(ctx context.Context, t *testing.T, tb TestBackend) {
	var tableName = tb.CreateTable(ctx, t, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	var catalog, state = ConfiguredCatalog(ctx, t, tb, tableName), sqlcapture.PersistentState{}

	tb.Insert(ctx, t, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}, {3, "Four"}, {4, "5"}})
	VerifiedCapture(ctx, t, tb, &catalog, &state, "init")
	tb.Insert(ctx, t, tableName, [][]interface{}{{1002, "some"}, {1000, "more"}, {1001, "rows"}})
	VerifiedCapture(ctx, t, tb, &catalog, &state, "")
}

// testReplicationDeletes runs two captures, where the first will perform the
// initial table scan and the second capture will use replication to receive
// additional inserts and deletions performed after the first capture.
func testReplicationDeletes(ctx context.Context, t *testing.T, tb TestBackend) {
	var tableName = tb.CreateTable(ctx, t, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	var catalog, state = ConfiguredCatalog(ctx, t, tb, tableName), sqlcapture.PersistentState{}

	tb.Insert(ctx, t, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}, {3, "Four"}, {4, "5"}})
	VerifiedCapture(ctx, t, tb, &catalog, &state, "init")
	tb.Insert(ctx, t, tableName, [][]interface{}{{1002, "some"}, {1000, "more"}, {1001, "rows"}})
	tb.Delete(ctx, t, tableName, "id", 1)
	tb.Delete(ctx, t, tableName, "id", 1002)
	VerifiedCapture(ctx, t, tb, &catalog, &state, "")
}

// testReplicationUpdates runs two captures, where the first will perform the
// initial table scan and the second capture will use replication to receive
// additional inserts and row updates performed after the first capture.
func testReplicationUpdates(ctx context.Context, t *testing.T, tb TestBackend) {
	var tableName = tb.CreateTable(ctx, t, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	var catalog, state = ConfiguredCatalog(ctx, t, tb, tableName), sqlcapture.PersistentState{}

	tb.Insert(ctx, t, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}})
	VerifiedCapture(ctx, t, tb, &catalog, &state, "init")
	tb.Insert(ctx, t, tableName, [][]interface{}{{1002, "some"}, {1000, "more"}, {1001, "rows"}})
	tb.Update(ctx, t, tableName, "id", 1, "data", "updated")
	tb.Update(ctx, t, tableName, "id", 1002, "data", "updated")
	VerifiedCapture(ctx, t, tb, &catalog, &state, "")
}

// testEmptyTable leaves the table empty during the initial table backfill
// and only adds data after replication has begun.
func testEmptyTable(ctx context.Context, t *testing.T, tb TestBackend) {
	var tableName = tb.CreateTable(ctx, t, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	var catalog, state = ConfiguredCatalog(ctx, t, tb, tableName), sqlcapture.PersistentState{}

	VerifiedCapture(ctx, t, tb, &catalog, &state, "init")
	tb.Insert(ctx, t, tableName, [][]interface{}{{1002, "some"}, {1000, "more"}, {1001, "rows"}})
	VerifiedCapture(ctx, t, tb, &catalog, &state, "")
}

// testIgnoredStreams checks that replicated changes are only reported
// for tables which are configured in the catalog.
func testIgnoredStreams(ctx context.Context, t *testing.T, tb TestBackend) {
	var state = sqlcapture.PersistentState{}
	var table1 = tb.CreateTable(ctx, t, "one", "(id INTEGER PRIMARY KEY, data TEXT)")
	var table2 = tb.CreateTable(ctx, t, "two", "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, table1, [][]interface{}{{0, "zero"}, {1, "one"}, {2, "two"}})
	tb.Insert(ctx, t, table2, [][]interface{}{{3, "three"}, {4, "four"}, {5, "five"}})
	var catalog = ConfiguredCatalog(ctx, t, tb, table1)
	VerifiedCapture(ctx, t, tb, &catalog, &state, "capture1") // Scan table1
	tb.Insert(ctx, t, table1, [][]interface{}{{6, "six"}})
	tb.Insert(ctx, t, table2, [][]interface{}{{7, "seven"}})
	tb.Insert(ctx, t, table1, [][]interface{}{{8, "eight"}})
	tb.Insert(ctx, t, table2, [][]interface{}{{9, "nine"}})
	VerifiedCapture(ctx, t, tb, &catalog, &state, "capture2") // Replicate table1 events, ignoring table2
}

// testMultipleStreams exercises captures with multiple stream configured, as
// well as adding/removing/re-adding a stream.
func testMultipleStreams(ctx context.Context, t *testing.T, tb TestBackend) {
	var state = sqlcapture.PersistentState{}
	var table1 = tb.CreateTable(ctx, t, "one", "(id INTEGER PRIMARY KEY, data TEXT)")
	var table2 = tb.CreateTable(ctx, t, "two", "(id INTEGER PRIMARY KEY, data TEXT)")
	var table3 = tb.CreateTable(ctx, t, "three", "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, table1, [][]interface{}{{0, "zero"}, {1, "one"}, {2, "two"}})
	tb.Insert(ctx, t, table2, [][]interface{}{{3, "three"}, {4, "four"}, {5, "five"}})
	tb.Insert(ctx, t, table3, [][]interface{}{{6, "six"}, {7, "seven"}, {8, "eight"}})
	var catalog1 = ConfiguredCatalog(ctx, t, tb, table1)
	var catalog123 = ConfiguredCatalog(ctx, t, tb, table1, table2, table3)
	var catalog13 = ConfiguredCatalog(ctx, t, tb, table1, table3)
	VerifiedCapture(ctx, t, tb, &catalog1, &state, "capture1")   // Scan table1
	VerifiedCapture(ctx, t, tb, &catalog123, &state, "capture2") // Add table2 and table3
	VerifiedCapture(ctx, t, tb, &catalog13, &state, "capture3")  // Forget about table2
	tb.Insert(ctx, t, table1, [][]interface{}{{9, "nine"}})
	tb.Insert(ctx, t, table2, [][]interface{}{{10, "ten"}})
	tb.Insert(ctx, t, table3, [][]interface{}{{11, "eleven"}})
	VerifiedCapture(ctx, t, tb, &catalog13, &state, "capture4")  // Replicate changes from table1 and table3 only
	VerifiedCapture(ctx, t, tb, &catalog123, &state, "capture5") // Re-scan table2 including the new row
}

// testComplexDataset tries to throw together a bunch of different bits of complexity
// to synthesize something vaguely "realistic". It features a multiple-column primary
// key, a dataset large enough that the initial table scan gets divided across many
// "chunks", two connector restarts at different points in the initial table scan, and
// some concurrent modifications to row ranges already-scanned and not-yet-scanned.
func testComplexDataset(ctx context.Context, t *testing.T, tb TestBackend) {
	var tableName = tb.CreateTable(ctx, t, "", "(year INTEGER, state VARCHAR(2), fullname VARCHAR(64), population INTEGER, PRIMARY KEY (year, state))")
	var catalog, state = ConfiguredCatalog(ctx, t, tb, tableName), sqlcapture.PersistentState{}

	LoadCSV(ctx, t, tb, tableName, "statepop.csv", 0)
	var states = VerifiedCapture(ctx, t, tb, &catalog, &state, "init")
	state = states[21] // Restart in between (1960, 'IA') and (1960, 'ID')

	tb.Insert(ctx, t, tableName, [][]interface{}{
		{1930, "XX", "No Such State", 1234},   // An insert in the already-scanned portion, which will be reported before backfilling resumes.
		{1970, "XX", "No Such State", 12345},  // An insert after the already-scanned portion, which will be reported when backfilling reaches this row.
		{1990, "XX", "No Such State", 123456}, // An insert after the already-scanned portion, which will be reported when backfilling reaches this row.
	})
	states = VerifiedCapture(ctx, t, tb, &catalog, &state, "restart1")
	state = states[10] // Restart in between (1980, 'SC') and (1980, 'SD')

	// All three "XX" states are deleted, but at the restart point only (1930, 'XX')
	// and (1970, 'XX') should be considered "already backfilled", and so there should
	// be two deletion events during the initial catchup streaming. The same is true of
	// the subsequent reinsertion.
	tb.Delete(ctx, t, tableName, "state", "XX")
	tb.Insert(ctx, t, tableName, [][]interface{}{
		{1930, "XX", "No Such State", 1234},
		{1970, "XX", "No Such State", 12345},
		{1990, "XX", "No Such State", 123456},
	})
	VerifiedCapture(ctx, t, tb, &catalog, &state, "restart2")
}

// testCatalogPrimaryKey sets up a table with no primary key in the database
// and instead specifies one in the catalog configuration.
func testCatalogPrimaryKey(ctx context.Context, t *testing.T, tb TestBackend) {
	var state = sqlcapture.PersistentState{}
	var table = tb.CreateTable(ctx, t, "", "(year INTEGER, state VARCHAR(2), fullname VARCHAR(64), population INTEGER)")
	LoadCSV(ctx, t, tb, table, "statepop.csv", 100)
	var catalog = ConfiguredCatalog(ctx, t, tb, table)
	catalog.Streams[0].PrimaryKey = [][]string{{"fullname"}, {"year"}}
	VerifiedCapture(ctx, t, tb, &catalog, &state, "capture1")
	tb.Insert(ctx, t, table, [][]interface{}{
		{1930, "XX", "No Such State", 1234},
		{1970, "XX", "No Such State", 12345},
		{1990, "XX", "No Such State", 123456},
	})
	VerifiedCapture(ctx, t, tb, &catalog, &state, "capture2")
}

// testCatalogPrimaryKeyOverride sets up a table with a primary key, but
// then overrides that via the catalog configuration.
func testCatalogPrimaryKeyOverride(ctx context.Context, t *testing.T, tb TestBackend) {
	var state = sqlcapture.PersistentState{}
	var table = tb.CreateTable(ctx, t, "", "(year INTEGER, state VARCHAR(2), fullname VARCHAR(64), population INTEGER, PRIMARY KEY (year, state))")
	LoadCSV(ctx, t, tb, table, "statepop.csv", 100)
	var catalog = ConfiguredCatalog(ctx, t, tb, table)
	catalog.Streams[0].PrimaryKey = [][]string{{"fullname"}, {"year"}}
	VerifiedCapture(ctx, t, tb, &catalog, &state, "capture1")
	tb.Insert(ctx, t, table, [][]interface{}{
		{1930, "XX", "No Such State", 1234},
		{1970, "XX", "No Such State", 12345},
		{1990, "XX", "No Such State", 123456},
	})
	VerifiedCapture(ctx, t, tb, &catalog, &state, "capture2")
}
