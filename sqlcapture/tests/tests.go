package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/connectors/sqlcapture"
	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
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
	t.Run("MissingTable", func(t *testing.T) { testMissingTable(ctx, t, tb) })
	t.Run("StressCorrectness", func(t *testing.T) { testStressCorrectness(ctx, t, tb) })
	t.Run("DuplicatedScanKey", func(t *testing.T) { testDuplicatedScanKey(ctx, t, tb) })
	t.Run("KeylessDiscovery", func(t *testing.T) { testKeylessDiscovery(ctx, t, tb) })
	t.Run("KeylessCapture", func(t *testing.T) { testKeylessCapture(ctx, t, tb) })
	t.Run("ReplicationOnly", func(t *testing.T) { testReplicationOnly(ctx, t, tb) })
	//t.Run("ComplexDataset", func(t *testing.T) { testComplexDataset(ctx, t, tb) })
}

// testConfigSchema serializes the response to a SpecRequest RPC and verifies it
// against a snapshot.
func testConfigSchema(ctx context.Context, t *testing.T, tb TestBackend) {
	response, err := tb.CaptureSpec(ctx, t).Driver.Spec(ctx, &pc.Request_Spec{})
	require.NoError(t, err)
	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

// testSimpleDiscovery creates a new table in the database, performs stream discovery,
// and then verifies both that a stream with the expected name exists, and that it
// matches a golden snapshot
func testSimpleDiscovery(ctx context.Context, t *testing.T, tb TestBackend) {
	var uniqueID = "49210954"
	tb.CreateTable(ctx, t, uniqueID, "(a INTEGER PRIMARY KEY, b TEXT, c REAL NOT NULL, d VARCHAR(255))")
	tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
}

// testSimpleCapture initializes a DB table with a few rows, then runs a capture
// which should emit all rows during table-scanning, start replication, and then
// shut down due to a lack of further events.
func testSimpleCapture(ctx context.Context, t *testing.T, tb TestBackend) {
	var uniqueID = "24869578"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}, {3, "Four"}, {4, "5"}})
	VerifiedCapture(ctx, t, tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID)))
}

// testReplicationInserts runs two captures, where the first will perform the
// initial table scan and the second capture will use replication to receive
// additional inserts performed after the first capture.
func testReplicationInserts(ctx context.Context, t *testing.T, tb TestBackend) {
	var uniqueID = "58418982"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

	tb.Insert(ctx, t, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}, {3, "Four"}, {4, "5"}})
	t.Run("init", func(t *testing.T) { VerifiedCapture(ctx, t, cs) })
	tb.Insert(ctx, t, tableName, [][]interface{}{{1002, "some"}, {1000, "more"}, {1001, "rows"}})
	t.Run("main", func(t *testing.T) { VerifiedCapture(ctx, t, cs) })
}

// testReplicationUpdates runs two captures, where the first will perform the
// initial table scan and the second capture will use replication to receive
// additional inserts and row updates performed after the first capture.
func testReplicationUpdates(ctx context.Context, t *testing.T, tb TestBackend) {
	var uniqueID = "79710599"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

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
	var uniqueID = "65713151"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

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
	var uniqueID = "91431165"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

	t.Run("init", func(t *testing.T) { VerifiedCapture(ctx, t, cs) })
	tb.Insert(ctx, t, tableName, [][]interface{}{{1002, "some"}, {1000, "more"}, {1001, "rows"}})
	t.Run("main", func(t *testing.T) { VerifiedCapture(ctx, t, cs) })
}

// testIgnoredStreams checks that replicated changes are only reported
// for tables which are configured in the catalog.
func testIgnoredStreams(ctx context.Context, t *testing.T, tb TestBackend) {
	var unique1, unique2 = "17789375", "24120951"
	var table1 = tb.CreateTable(ctx, t, unique1, "(id INTEGER PRIMARY KEY, data TEXT)")
	var table2 = tb.CreateTable(ctx, t, unique2, "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, table1, [][]interface{}{{0, "zero"}, {1, "one"}, {2, "two"}})
	tb.Insert(ctx, t, table2, [][]interface{}{{3, "three"}, {4, "four"}, {5, "five"}})

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(unique1))
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
	var unique1, unique2, unique3 = "14930049", "26016615", "30084965"
	var table1 = tb.CreateTable(ctx, t, unique1, "(id INTEGER PRIMARY KEY, data TEXT)")
	var table2 = tb.CreateTable(ctx, t, unique2, "(id INTEGER PRIMARY KEY, data TEXT)")
	var table3 = tb.CreateTable(ctx, t, unique3, "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, table1, [][]interface{}{{0, "zero"}, {1, "one"}, {2, "two"}})
	tb.Insert(ctx, t, table2, [][]interface{}{{3, "three"}, {4, "four"}, {5, "five"}})
	tb.Insert(ctx, t, table3, [][]interface{}{{6, "six"}, {7, "seven"}, {8, "eight"}})

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(unique1))
	t.Run("capture1", func(t *testing.T) { VerifiedCapture(ctx, t, cs) }) // Scan table1
	cs.Bindings = DiscoverBindings(ctx, t, tb, regexp.MustCompile(unique1), regexp.MustCompile(unique2), regexp.MustCompile(unique3))
	t.Run("capture2", func(t *testing.T) { VerifiedCapture(ctx, t, cs) }) // Add table2 and table3
	cs.Bindings = DiscoverBindings(ctx, t, tb, regexp.MustCompile(unique1), regexp.MustCompile(unique3))
	t.Run("capture3", func(t *testing.T) { VerifiedCapture(ctx, t, cs) }) // Forget about table2

	tb.Insert(ctx, t, table1, [][]interface{}{{9, "nine"}})
	tb.Insert(ctx, t, table2, [][]interface{}{{10, "ten"}})
	tb.Insert(ctx, t, table3, [][]interface{}{{11, "eleven"}})
	t.Run("capture4", func(t *testing.T) { VerifiedCapture(ctx, t, cs) }) // Replicate changes from table1 and table3 only

	cs.Bindings = DiscoverBindings(ctx, t, tb, regexp.MustCompile(unique1), regexp.MustCompile(unique2), regexp.MustCompile(unique3))
	t.Run("capture5", func(t *testing.T) { VerifiedCapture(ctx, t, cs) }) // Re-scan table2 including the new row
}

// testCatalogPrimaryKey sets up a table with no primary key in the database
// and instead specifies one in the catalog configuration.
func testCatalogPrimaryKey(ctx context.Context, t *testing.T, tb TestBackend) {
	var uniqueID = "46055452"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(year INTEGER, state VARCHAR(2), fullname VARCHAR(64), population INTEGER)")
	LoadCSV(ctx, t, tb, tableName, "statepop.csv", 100)
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

	// Override capture mode to 'Normal' and primary key to [fullname year]
	var res sqlcapture.Resource
	require.NoError(t, json.Unmarshal(cs.Bindings[0].ResourceConfigJson, &res))
	res.Mode = sqlcapture.BackfillModeNormal
	res.PrimaryKey = []string{"fullname", "year"}
	resourceJSON, err := json.Marshal(res)
	require.NoError(t, err)
	cs.Bindings[0].ResourceConfigJson = resourceJSON
	cs.Bindings[0].Collection.Key = res.PrimaryKey

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
	var uniqueID = "83617880"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(year INTEGER, state VARCHAR(2), fullname VARCHAR(64), population INTEGER, PRIMARY KEY (year, state))")
	LoadCSV(ctx, t, tb, tableName, "statepop.csv", 100)
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

	// Override capture mode to 'Normal' and primary key to [fullname year]
	var res sqlcapture.Resource
	require.NoError(t, json.Unmarshal(cs.Bindings[0].ResourceConfigJson, &res))
	res.Mode = sqlcapture.BackfillModeNormal
	res.PrimaryKey = []string{"fullname", "year"}
	resourceJSON, err := json.Marshal(res)
	require.NoError(t, err)
	cs.Bindings[0].ResourceConfigJson = resourceJSON
	cs.Bindings[0].Collection.Key = res.PrimaryKey

	t.Run("capture1", func(t *testing.T) { VerifiedCapture(ctx, t, cs) })
	tb.Insert(ctx, t, tableName, [][]interface{}{
		{1930, "XX", "No Such State", 1234},
		{1970, "XX", "No Such State", 12345},
		{1990, "XX", "No Such State", 123456},
	})
	t.Run("capture2", func(t *testing.T) { VerifiedCapture(ctx, t, cs) })
}

// testMissingTable verifies that things fail cleanly if a capture
// binding doesn't actually exist.
func testMissingTable(ctx context.Context, t *testing.T, tb TestBackend) {
	var unique1, unique2 = "18865235", "27607177"
	tb.CreateTable(ctx, t, unique1, "(id INTEGER PRIMARY KEY, data TEXT)")
	var binding1 = DiscoverBindings(ctx, t, tb, regexp.MustCompile(unique1))[0]
	var binding2 = BindingReplace(binding1, unique1, unique2)
	var cs = tb.CaptureSpec(ctx, t)
	cs.Bindings = []*flow.CaptureSpec_Binding{binding1, binding2}
	VerifiedCapture(ctx, t, cs)
}

func testDuplicatedScanKey(ctx context.Context, t *testing.T, tb TestBackend) {
	var uniqueID = "92011048"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id VARCHAR(8), data TEXT)")
	tb.Insert(ctx, t, tableName, [][]any{{"AAA", "1"}, {"BBB", "2"}, {"BBB", "3"}, {"CCC", "4"}})
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Bindings[0].Collection.Key = []string{"id"}
	VerifiedCapture(ctx, t, cs)
}

// testStressCorrectness issues a lengthy stream of inserts, updates, and
// deletes to the database with certain invariants, and verifies that the
// capture result doesn't violate those invariants.
//
// The dataset for this test is a collection of (id, counter) tuples
// which will progress through the following states:
//
//		(id, 1) ->  (id, 2) ->  (id, 3) -> (id, 4) -> deleted
//	         ->  (id, 6) ->  (id, 7) -> (id, 8)
//
// The specific ordering of changes is randomly generated on every test run
// and the capture introduces more variation depending on the precise timing
// of the backfill queries and replication stream merging. However it is still
// possible to verify many correctness properties by observing the output event
// stream. For instance:
//
//   - When the capture completes, we know exactly how many IDs should exist
//     and that they should all be in the final state with counter=8. If this
//     is not the case then some data has been lost.
//
//   - We know that (modulo the deletion at id=5) counter values must always
//     increase by exactly one for every successive update. If this is not the
//     case then some changes have been skipped or duplicated.
func testStressCorrectness(ctx context.Context, t *testing.T, tb TestBackend) {
	const numTotalIDs = 2000
	const numActiveIDs = 100

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	var uniqueID = "51314288"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, counter INTEGER)")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = &correctnessInvariantsCaptureValidator{
		NumExpectedIDs: numTotalIDs,
	}
	cs.Sanitizers[`"backfilled":999`] = regexp.MustCompile(`"backfilled":[0-9]+`)
	cs.Validator.Reset()

	// Run the load generator for at most 60s
	loadgenCtx, cancelLoadgen := context.WithCancel(ctx)
	time.AfterFunc(60*time.Second, cancelLoadgen)
	defer cancelLoadgen()
	go func(ctx context.Context) {
		var nextID int
		var activeIDs = make(map[int]int) // Map from ID to counter value
		for {
			// There should always be N active IDs (until we reach the end)
			for nextID < numTotalIDs && len(activeIDs) < numActiveIDs {
				activeIDs[nextID] = 1
				nextID++
			}
			// Thus if we run out of active IDs we must be done
			if len(activeIDs) == 0 {
				log.Info("load generator complete")
				return
			}

			// Randomly select an entry from the active set
			var selected int
			var sampleIndex int
			for id := range activeIDs {
				if sampleIndex == 0 || rand.Intn(sampleIndex) == 0 {
					selected = id
				}
				sampleIndex++
			}
			var counter = activeIDs[selected]

			// Issue an update/insert/delete depending on the counter value
			switch counter {
			case 1, 6:
				tb.Insert(ctx, t, tableName, [][]any{{selected, counter}})
			case 5:
				tb.Delete(ctx, t, tableName, "id", selected)
			default:
				tb.Update(ctx, t, tableName, "id", selected, "counter", counter)
			}

			// Increment the counter state or delete the entry if we're done with it.
			if counter := activeIDs[selected]; counter >= 8 {
				delete(activeIDs, selected)
			} else {
				activeIDs[selected] = counter + 1
			}

			// Slow down database load generation to at most 500 QPS
			time.Sleep(2 * time.Millisecond)
		}
	}(loadgenCtx)

	// Start the capture in parallel with the ongoing database load
	VerifiedCapture(ctx, t, cs)
}

// We can see new IDs occurring out of order, so long as they make it to
// completion before the end.
type correctnessInvariantsCaptureValidator struct {
	NumExpectedIDs int

	currentTransaction map[int]string

	states     map[int]string
	violations *strings.Builder
}

var correctnessInvariantsStateTransitions = map[string]string{
	"New:Create(1)":     "1",        // A row may first be observed at any point
	"New:Create(2)":     "2",        // A row may first be observed at any point
	"New:Create(3)":     "3",        // A row may first be observed at any point
	"New:Create(4)":     "4",        // A row may first be observed at any point
	"New:Create(6)":     "6",        // A row may first be observed at any point
	"New:Create(7)":     "7",        // A row may first be observed at any point
	"New:Create(8)":     "Finished", // A row may first be observed at any point
	"New:Update(2)":     "2",        // This first observation may also be an update
	"New:Update(3)":     "3",        // This first observation may also be an update
	"New:Update(4)":     "4",        // This first observation may also be an update
	"New:Delete()":      "Deleted",  // This first observation could theoretically also be a deletion
	"New:Update(7)":     "7",        // This first observation may also be an update
	"New:Update(8)":     "Finished", // This first observation may also be an update
	"1:Update(2)":       "2",
	"2:Update(3)":       "3",
	"3:Update(4)":       "4",
	"4:Delete()":        "Deleted",
	"Deleted:Create(6)": "6",
	"6:Update(7)":       "7",
	"7:Update(8)":       "Finished",
}

func (v *correctnessInvariantsCaptureValidator) Output(collection string, data json.RawMessage) {
	var event struct {
		ID      int `json:"ID"`
		Counter int `json:"counter"`
		Meta    struct {
			Operation string `json:"op"`
		} `json:"_meta"`
	}
	if err := json.Unmarshal(data, &event); err != nil {
		fmt.Fprintf(v.violations, "error parsing change event: %v\n", err)
		return
	}

	// Compute a string representing the change event and store it as part of the
	// current pending transaction. Overwriting any previous change for this ID
	// emulates the default last-write-wins reduction semantics of /_meta/op and
	// the main document payload properties.
	var change string
	switch event.Meta.Operation {
	case "c":
		change = fmt.Sprintf("Create(%d)", event.Counter)
	case "u":
		change = fmt.Sprintf("Update(%d)", event.Counter)
	case "d":
		change = fmt.Sprintf("Delete()")
	default:
		change = fmt.Sprintf("UnknownOperation(%q)", event.Meta.Operation)
	}
	log.WithField("id", event.ID).WithField("change", change).Trace("change event")
	if v.currentTransaction == nil {
		v.currentTransaction = make(map[int]string)
	}
	v.currentTransaction[event.ID] = change
}

func (v *correctnessInvariantsCaptureValidator) Checkpoint(data json.RawMessage) {
	for id, change := range v.currentTransaction {
		// Get the previous state for this ID and put together a string describing
		// the state machine edge corresponding to this change.
		var prevState = v.states[id]
		if prevState == "" {
			prevState = "New"
		}
		var edge = prevState + ":" + change

		// This is the complete set of allowable state transitions in the dataset.
		if nextState, ok := correctnessInvariantsStateTransitions[edge]; ok {
			v.states[id] = nextState
		} else {
			fmt.Fprintf(v.violations, "id %d: invalid state transition %q\n", id, edge)
			v.states[id] = "Error"
		}
	}
	v.currentTransaction = nil
}

func (v *correctnessInvariantsCaptureValidator) Reset() {
	v.violations = new(strings.Builder)
	v.states = make(map[int]string)
}

func (v *correctnessInvariantsCaptureValidator) Summarize(w io.Writer) error {
	fmt.Fprintf(w, "# ================================\n")
	fmt.Fprintf(w, "# Invariant Violations\n")
	fmt.Fprintf(w, "# ================================\n")

	for id := 0; id < v.NumExpectedIDs; id++ {
		var state = v.states[id]
		if state != "Finished" {
			fmt.Fprintf(v.violations, "id %d in state %q (expected \"Finished\")\n", id, state)
		}
	}

	if str := v.violations.String(); len(str) == 0 {
		fmt.Fprintf(w, "no invariant violations observed\n\n")
	} else {
		fmt.Fprintf(w, "%s\n", str)
	}
	return nil
}

func testReplicationOnly(ctx context.Context, t *testing.T, tb TestBackend) {
	var uniqueID = "34322067"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER, data TEXT)")

	// Create a capture spec and replace the suggested "Without Key" backfill mode
	// with the "Only Changes" one. This mirrors the deliberate user action which
	// would be required in the UI to get this mode of operation.
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Bindings[0] = BindingReplace(cs.Bindings[0], string(sqlcapture.BackfillModeWithoutKey), string(sqlcapture.BackfillModeOnlyChanges))

	for i := 0; i < 8; i++ {
		var batch [][]any
		for j := 0; j < 256; j++ {
			batch = append(batch, []any{i*1000000 + j, fmt.Sprintf("Batch %d Value %d", i, j)})
		}
		tb.Insert(ctx, t, tableName, batch)
	}
	t.Run("backfill", func(t *testing.T) { VerifiedCapture(ctx, t, cs) })

	for i := 8; i < 16; i++ {
		var batch [][]any
		for j := 0; j < 256; j++ {
			batch = append(batch, []any{i*1000000 + j, fmt.Sprintf("Batch %d Value %d", i, j)})
		}
		tb.Insert(ctx, t, tableName, batch)
	}
	t.Run("replication", func(t *testing.T) { VerifiedCapture(ctx, t, cs) })

}

func testKeylessDiscovery(ctx context.Context, t *testing.T, tb TestBackend) {
	const uniqueString = "t32386"
	tb.CreateTable(ctx, t, uniqueString, "(a INTEGER, b TEXT, c REAL NOT NULL, d VARCHAR(255))")
	tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(regexp.QuoteMeta(uniqueString)))
}

func testKeylessCapture(ctx context.Context, t *testing.T, tb TestBackend) {
	var uniqueID = "91511186"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER, data TEXT)")
	var generate = func(n, m int) [][]any {
		var rows [][]any
		for i := n; i < m; i++ {
			rows = append(rows, []any{i, fmt.Sprintf("Row Number %d", i)})
		}
		return rows
	}

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = &st.OrderedCaptureValidator{}

	tb.Insert(ctx, t, tableName, generate(0, 500))
	t.Run("Backfill", func(t *testing.T) { VerifiedCapture(ctx, t, cs) })
	tb.Insert(ctx, t, tableName, generate(500, 1000))
	t.Run("Replication", func(t *testing.T) { VerifiedCapture(ctx, t, cs) })
}
