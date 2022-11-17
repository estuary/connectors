package tests

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/sqlcapture"
	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

// Run executes the generic SQL capture test suite
func Run(ctx context.Context, t *testing.T, tb TestBackend) {
	t.Helper()
	t.Run("SpecResponse", func(t *testing.T) { testConfigSchema(ctx, t, tb) })
	t.Run("SimpleDiscovery", func(t *testing.T) { testSimpleDiscovery(ctx, t, tb) })
	t.Run("SimpleCapture", func(t *testing.T) { testSimpleCapture(ctx, t, tb) })
	t.Run("ReplicationInserts", func(t *testing.T) { testReplicationInserts(ctx, t, tb) })
	t.Run("ReplicationUpdates", func(t *testing.T) { testReplicationUpdates(ctx, t, tb) })
	t.Run("ReplicationDeletes", func(t *testing.T) { testReplicationDeletes(ctx, t, tb) })
	t.Run("EmptyTable", func(t *testing.T) { testEmptyTable(ctx, t, tb) })
	t.Run("IgnoredStreams", func(t *testing.T) { testIgnoredStreams(ctx, t, tb) })
	t.Run("MultipleStreams", func(t *testing.T) { testMultipleStreams(ctx, t, tb) })
	t.Run("CatalogPrimaryKey", func(t *testing.T) { testCatalogPrimaryKey(ctx, t, tb) })
	t.Run("CatalogPrimaryKeyOverride", func(t *testing.T) { testCatalogPrimaryKeyOverride(ctx, t, tb) })
	//t.Run("ComplexDataset", func(t *testing.T) { testComplexDataset(ctx, t, tb) })
}

// testConfigSchema serializes the response to a SpecRequest RPC and verifies it
// against a snapshot.
func testConfigSchema(ctx context.Context, t *testing.T, tb TestBackend) {
	response, err := tb.CaptureSpec(t).Driver.Spec(ctx, &pc.SpecRequest{})
	require.NoError(t, err)
	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

// testSimpleDiscovery creates a new table in the database, performs stream discovery,
// and then verifies both that a stream with the expected name exists, and that it
// matches a golden snapshot
func testSimpleDiscovery(ctx context.Context, t *testing.T, tb TestBackend) {
	const uniqueString = "magnanimous_outshine"
	tb.CreateTable(ctx, t, uniqueString, "(a INTEGER PRIMARY KEY, b TEXT, c REAL NOT NULL, d VARCHAR(255))")
	tb.CaptureSpec(t).VerifyDiscover(ctx, t, regexp.MustCompile(regexp.QuoteMeta(uniqueString)))
}

// testSimpleCapture initializes a DB table with a few rows, then runs a capture
// which should emit all rows during table-scanning, start replication, and then
// shut down due to a lack of further events.
func testSimpleCapture(ctx context.Context, t *testing.T, tb TestBackend) {
	var tableName = tb.CreateTable(ctx, t, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}, {3, "Four"}, {4, "5"}})
	VerifiedCapture(ctx, t, tb.CaptureSpec(t, tableName))
}

// testReplicationInserts runs two captures, where the first will perform the
// initial table scan and the second capture will use replication to receive
// additional inserts performed after the first capture.
func testReplicationInserts(ctx context.Context, t *testing.T, tb TestBackend) {
	var tableName = tb.CreateTable(ctx, t, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	var cs = tb.CaptureSpec(t, tableName)

	tb.Insert(ctx, t, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}, {3, "Four"}, {4, "5"}})
	t.Run("init", func(t *testing.T) { VerifiedCapture(ctx, t, cs) })
	tb.Insert(ctx, t, tableName, [][]interface{}{{1002, "some"}, {1000, "more"}, {1001, "rows"}})
	t.Run("main", func(t *testing.T) { VerifiedCapture(ctx, t, cs) })
}

// testReplicationUpdates runs two captures, where the first will perform the
// initial table scan and the second capture will use replication to receive
// additional inserts and row updates performed after the first capture.
func testReplicationUpdates(ctx context.Context, t *testing.T, tb TestBackend) {
	var tableName = tb.CreateTable(ctx, t, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	var cs = tb.CaptureSpec(t, tableName)

	tb.Insert(ctx, t, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}})
	t.Run("init", func(t *testing.T) { VerifiedCapture(ctx, t, cs) })
	tb.Insert(ctx, t, tableName, [][]interface{}{{1002, "some"}, {1000, "more"}, {1001, "rows"}})
	tb.Update(ctx, t, tableName, "id", 1, "data", "updated")
	tb.Update(ctx, t, tableName, "id", 1002, "data", "updated")
	t.Run("main", func(t *testing.T) { VerifiedCapture(ctx, t, cs) })
}

// testReplicationDeletes runs two captures, where the first will perform the
// initial table scan and the second capture will use replication to receive
// additional inserts and deletions performed after the first capture.
func testReplicationDeletes(ctx context.Context, t *testing.T, tb TestBackend) {
	var tableName = tb.CreateTable(ctx, t, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	var cs = tb.CaptureSpec(t, tableName)

	tb.Insert(ctx, t, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}, {3, "Four"}, {4, "5"}})
	t.Run("init", func(t *testing.T) { VerifiedCapture(ctx, t, cs) })
	tb.Insert(ctx, t, tableName, [][]interface{}{{1002, "some"}, {1000, "more"}, {1001, "rows"}})
	tb.Delete(ctx, t, tableName, "id", 1)
	tb.Delete(ctx, t, tableName, "id", 1002)
	t.Run("main", func(t *testing.T) { VerifiedCapture(ctx, t, cs) })
}

// testEmptyTable leaves the table empty during the initial table backfill
// and only adds data after replication has begun.
func testEmptyTable(ctx context.Context, t *testing.T, tb TestBackend) {
	var tableName = tb.CreateTable(ctx, t, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	var cs = tb.CaptureSpec(t, tableName)

	t.Run("init", func(t *testing.T) { VerifiedCapture(ctx, t, cs) })
	tb.Insert(ctx, t, tableName, [][]interface{}{{1002, "some"}, {1000, "more"}, {1001, "rows"}})
	t.Run("main", func(t *testing.T) { VerifiedCapture(ctx, t, cs) })
}

// testIgnoredStreams checks that replicated changes are only reported
// for tables which are configured in the catalog.
func testIgnoredStreams(ctx context.Context, t *testing.T, tb TestBackend) {
	var table1 = tb.CreateTable(ctx, t, "one", "(id INTEGER PRIMARY KEY, data TEXT)")
	var table2 = tb.CreateTable(ctx, t, "two", "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, table1, [][]interface{}{{0, "zero"}, {1, "one"}, {2, "two"}})
	tb.Insert(ctx, t, table2, [][]interface{}{{3, "three"}, {4, "four"}, {5, "five"}})

	var cs = tb.CaptureSpec(t, table1)
	t.Run("capture1", func(t *testing.T) { VerifiedCapture(ctx, t, cs) })
	tb.Insert(ctx, t, table1, [][]interface{}{{6, "six"}})
	tb.Insert(ctx, t, table2, [][]interface{}{{7, "seven"}})
	tb.Insert(ctx, t, table1, [][]interface{}{{8, "eight"}})
	tb.Insert(ctx, t, table2, [][]interface{}{{9, "nine"}})
	t.Run("capture2", func(t *testing.T) { VerifiedCapture(ctx, t, cs) })
}

// testMultipleStreams exercises captures with multiple stream configured, as
// well as adding/removing/re-adding a stream.
func testMultipleStreams(ctx context.Context, t *testing.T, tb TestBackend) {
	var table1 = tb.CreateTable(ctx, t, "one", "(id INTEGER PRIMARY KEY, data TEXT)")
	var table2 = tb.CreateTable(ctx, t, "two", "(id INTEGER PRIMARY KEY, data TEXT)")
	var table3 = tb.CreateTable(ctx, t, "three", "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, table1, [][]interface{}{{0, "zero"}, {1, "one"}, {2, "two"}})
	tb.Insert(ctx, t, table2, [][]interface{}{{3, "three"}, {4, "four"}, {5, "five"}})
	tb.Insert(ctx, t, table3, [][]interface{}{{6, "six"}, {7, "seven"}, {8, "eight"}})

	var cs = tb.CaptureSpec(t, table1)
	t.Run("capture1", func(t *testing.T) { VerifiedCapture(ctx, t, cs) }) // Scan table1
	cs.Bindings = ResourceBindings(t, table1, table2, table3)
	t.Run("capture2", func(t *testing.T) { VerifiedCapture(ctx, t, cs) }) // Add table2 and table3
	cs.Bindings = ResourceBindings(t, table1, table3)
	t.Run("capture3", func(t *testing.T) { VerifiedCapture(ctx, t, cs) }) // Forget about table2

	tb.Insert(ctx, t, table1, [][]interface{}{{9, "nine"}})
	tb.Insert(ctx, t, table2, [][]interface{}{{10, "ten"}})
	tb.Insert(ctx, t, table3, [][]interface{}{{11, "eleven"}})
	t.Run("capture4", func(t *testing.T) { VerifiedCapture(ctx, t, cs) }) // Replicate changes from table1 and table3 only

	cs.Bindings = ResourceBindings(t, table1, table2, table3)
	t.Run("capture5", func(t *testing.T) { VerifiedCapture(ctx, t, cs) }) // Re-scan table2 including the new row
}

// testCatalogPrimaryKey sets up a table with no primary key in the database
// and instead specifies one in the catalog configuration.
func testCatalogPrimaryKey(ctx context.Context, t *testing.T, tb TestBackend) {
	var tableName = tb.CreateTable(ctx, t, "", "(year INTEGER, state VARCHAR(2), fullname VARCHAR(64), population INTEGER)")
	LoadCSV(ctx, t, tb, tableName, "statepop.csv", 100)
	var cs = tb.CaptureSpec(t)

	var nameParts = strings.SplitN(tableName, ".", 2)
	var specJSON, err = json.Marshal(sqlcapture.Resource{
		Namespace:  nameParts[0],
		Stream:     nameParts[1],
		PrimaryKey: []string{"fullname", "year"},
	})
	require.NoError(t, err)
	cs.Bindings = append(cs.Bindings, &flow.CaptureSpec_Binding{
		ResourceSpecJson: specJSON,
		ResourcePath:     []string{tableName},
	})

	t.Run("capture1", func(t *testing.T) { VerifiedCapture(ctx, t, cs) })
	tb.Insert(ctx, t, tableName, [][]interface{}{
		{1930, "XX", "No Such State", 1234},
		{1970, "XX", "No Such State", 12345},
		{1990, "XX", "No Such State", 123456},
	})
	t.Run("capture2", func(t *testing.T) { VerifiedCapture(ctx, t, cs) })
}

// testCatalogPrimaryKeyOverride sets up a table with a primary key, but
// then overrides that via the catalog configuration.
func testCatalogPrimaryKeyOverride(ctx context.Context, t *testing.T, tb TestBackend) {
	var tableName = tb.CreateTable(ctx, t, "", "(year INTEGER, state VARCHAR(2), fullname VARCHAR(64), population INTEGER, PRIMARY KEY (year, state))")
	LoadCSV(ctx, t, tb, tableName, "statepop.csv", 100)
	var cs = tb.CaptureSpec(t)

	var nameParts = strings.SplitN(tableName, ".", 2)
	var specJSON, err = json.Marshal(sqlcapture.Resource{
		Namespace:  nameParts[0],
		Stream:     nameParts[1],
		PrimaryKey: []string{"fullname", "year"},
	})
	require.NoError(t, err)
	cs.Bindings = append(cs.Bindings, &flow.CaptureSpec_Binding{
		ResourceSpecJson: specJSON,
		ResourcePath:     []string{tableName},
	})

	t.Run("capture1", func(t *testing.T) { VerifiedCapture(ctx, t, cs) })
	tb.Insert(ctx, t, tableName, [][]interface{}{
		{1930, "XX", "No Such State", 1234},
		{1970, "XX", "No Such State", 12345},
		{1990, "XX", "No Such State", 123456},
	})
	t.Run("capture2", func(t *testing.T) { VerifiedCapture(ctx, t, cs) })
}

// TODO(wgd): Rewrite testComplexDataset using some trickery after figuring out a
// more principled way of exercising "shut down partway through a backfill" behavior.
//
//// testComplexDataset tries to throw together a bunch of different bits of complexity
//// to synthesize something vaguely "realistic". It features a multiple-column primary
//// key, a dataset large enough that the initial table scan gets divided across many
//// "chunks", two connector restarts at different points in the initial table scan, and
//// some concurrent modifications to row ranges already-scanned and not-yet-scanned.
//func testComplexDataset(ctx context.Context, t *testing.T, tb TestBackend) {
//	var tableName = tb.CreateTable(ctx, t, "", "(year INTEGER, state VARCHAR(2), fullname VARCHAR(64), population INTEGER, PRIMARY KEY (year, state))")
//	var catalog, state = ConfiguredCatalog(ctx, t, tb, tableName), sqlcapture.PersistentState{}
//
//	LoadCSV(ctx, t, tb, tableName, "statepop.csv", 0)
//	var states = VerifiedCapture(ctx, t, tb, &catalog, &state, "init")
//	state = states[22] // Restart in between (1960, 'IA') and (1960, 'ID')
//
//	tb.Insert(ctx, t, tableName, [][]interface{}{
//		{1930, "XX", "No Such State", 1234},   // An insert in the already-scanned portion, which will be reported before backfilling resumes.
//		{1970, "XX", "No Such State", 12345},  // An insert after the already-scanned portion, which will be reported when backfilling reaches this row.
//		{1990, "XX", "No Such State", 123456}, // An insert after the already-scanned portion, which will be reported when backfilling reaches this row.
//	})
//	states = VerifiedCapture(ctx, t, tb, &catalog, &state, "restart1")
//	state = states[10] // Restart in between (1980, 'SC') and (1980, 'SD')
//
//	// All three "XX" states are deleted, but at the restart point only (1930, 'XX')
//	// and (1970, 'XX') should be considered "already backfilled", and so there should
//	// be two deletion events during the initial catchup streaming. The same is true of
//	// the subsequent reinsertion.
//	tb.Delete(ctx, t, tableName, "state", "XX")
//	tb.Insert(ctx, t, tableName, [][]interface{}{
//		{1930, "XX", "No Such State", 1234},
//		{1970, "XX", "No Such State", 12345},
//		{1990, "XX", "No Such State", 123456},
//	})
//	VerifiedCapture(ctx, t, tb, &catalog, &state, "restart2")
//}
