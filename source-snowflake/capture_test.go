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
	"github.com/stretchr/testify/require"
)

func verifiedCapture(ctx context.Context, t testing.TB, cs *st.CaptureSpec) {
	t.Helper()
	cs.Capture(ctx, t, nil)
	cupaloy.SnapshotT(t, cs.Summary())
	cs.Reset()
}

func TestSimpleCapture(t *testing.T) {
	var ctx, tb = context.Background(), snowflakeTestBackend(t)
	var uniqueID = "24869578"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

	tb.Insert(ctx, t, tableName, [][]any{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}, {3, "Four"}, {4, "5"}})
	t.Run("init", func(t *testing.T) { verifiedCapture(ctx, t, cs) })

	tb.Insert(ctx, t, tableName, [][]any{{6, "F"}, {7, "ggg"}, {8, "QRSTUVWXYZ"}, {9, "Ten"}, {10, "11"}})
	t.Run("main", func(t *testing.T) { verifiedCapture(ctx, t, cs) })
}

func TestLongTableNames(t *testing.T) {
	var ctx, tb = context.Background(), snowflakeTestBackend(t)
	var uniqueID = "24869578"

	// In some contexts the identifier length limit of 255 bytes is applied to the entire
	// fully-qualified identifier (<database_name>.<schema_name>.<object_name>), so the
	// padding constant 173 here was determined empirically as the longest name which still
	// works when qualified and quoted as `CONNECTOR_TESTING.PUBLIC."padded_table_name"`.
	var tableSuffix = uniqueID + "_" + strings.Repeat("x", 173) + "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	var tableName = tb.CreateTable(ctx, t, tableSuffix, "(id INTEGER PRIMARY KEY, data TEXT)")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

	tb.Insert(ctx, t, tableName, [][]any{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}, {3, "Four"}, {4, "5"}})
	t.Run("init", func(t *testing.T) { verifiedCapture(ctx, t, cs) })

	tb.Insert(ctx, t, tableName, [][]any{{6, "F"}, {7, "ggg"}, {8, "QRSTUVWXYZ"}, {9, "Ten"}, {10, "11"}})
	t.Run("main", func(t *testing.T) { verifiedCapture(ctx, t, cs) })
}

func TestBasicDatatypes(t *testing.T) {
	var ctx, tb = context.Background(), snowflakeTestBackend(t)
	var uniqueID = "77528227"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT, bdata VARBINARY, fdata FLOAT, bit BOOLEAN, x NUMBER(4, 0), y NUMBER(4, 2))")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

	t.Run("discover", func(t *testing.T) { cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID)) })

	tb.Insert(ctx, t, tableName, [][]any{
		{0, "abcd", []byte{0xFF, 0xA5, 0x00, 0x5A}, 1.32, true, 9999, 99.99},
		{1, "", []byte{0x00}, 0, true, -1, -0.01},
		{2, nil, nil, nil, nil, nil, nil},
	})
	t.Run("init", func(t *testing.T) { verifiedCapture(ctx, t, cs) })

	tb.Insert(ctx, t, tableName, [][]any{
		{3, "DCBA", []byte{0xEE, 0x5A, 0x11, 0xA5}, 2.64, false, 0, 0},
		{4, "    ", []byte{0xFF}, 0.00001, false, -9999, -0.99},
		{5, nil, nil, nil, nil, nil, nil},
	})
	t.Run("main", func(t *testing.T) { verifiedCapture(ctx, t, cs) })
}

func TestTimestampDatatypes(t *testing.T) {
	var ctx, tb = context.Background(), snowflakeTestBackend(t)
	var uniqueID = "38028141"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(ymd DATE, hms TIME, tstz TIMESTAMP_TZ, tsntz TIMESTAMP_NTZ)")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

	t.Run("discover", func(t *testing.T) { cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID)) })

	var x = "1991-08-31T09:25:27Z"
	var y = "1991-11-25T06:13:44Z"
	tb.Insert(ctx, t, tableName, [][]any{{"1991-08-31", "09:25:27", x, x}})
	tb.Insert(ctx, t, tableName, [][]any{{"1991-11-25", "06:13:44", y, y}})
	t.Run("capture", func(t *testing.T) { verifiedCapture(ctx, t, cs) })
}

func TestVariantDatatypes(t *testing.T) {
	var ctx, tb = context.Background(), snowflakeTestBackend(t)
	var uniqueID = "13308929"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(a VARIANT, b OBJECT, c ARRAY)")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

	t.Run("discover", func(t *testing.T) { cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID)) })

	tb.Query(ctx, t, fmt.Sprintf(`INSERT INTO %s SELECT 'hello'::VARIANT, object_construct('foo', 123), array_construct(1, 2, 3)`, tableName))
	tb.Query(ctx, t, fmt.Sprintf(`INSERT INTO %s SELECT 321, object_construct(), array_construct()`, tableName))
	tb.Query(ctx, t, fmt.Sprintf(`INSERT INTO %s SELECT NULL, NULL, NULL`, tableName))
	t.Run("capture", func(t *testing.T) { verifiedCapture(ctx, t, cs) })
}

func TestLargeCapture(t *testing.T) {
	var ctx, tb = context.Background(), snowflakeTestBackend(t)
	var uniqueID = "63855638"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

	var summary = new(strings.Builder)

	// Helper operations to make the sequence of test manipulations cleaner
	var insertRows = func(ctx context.Context, lo, hi int) {
		var rows [][]any
		for i := lo; i < hi; i++ {
			rows = append(rows, []any{i, fmt.Sprintf("Row #%d", i)})
		}
		tb.Insert(ctx, t, tableName, rows)
	}
	var rewindCheckpoint = func(cs *st.CaptureSpec, seq, off int) error {
		var checkpoint captureState
		if err := json.Unmarshal(cs.Checkpoint, &checkpoint); err != nil {
			return err
		}
		for _, streamState := range checkpoint.Streams {
			streamState.SequenceNumber = seq
			streamState.TableOffset = off
		}
		var err error
		cs.Checkpoint, err = json.Marshal(checkpoint)
		return err
	}
	var sanitize = func(data json.RawMessage) json.RawMessage {
		var bs = []byte(data)
		for replacement, matcher := range cs.Sanitizers {
			bs = matcher.ReplaceAll(bs, []byte(replacement))
		}
		return json.RawMessage(bs)
	}
	var summarize = func(data json.RawMessage) {
		fmt.Fprintf(summary, "%s\n", string(sanitize(data)))
	}

	// Insert 100 rows and then run the first capture
	insertRows(ctx, 0, 100)
	fmt.Fprintf(summary, "// =================================\n")
	fmt.Fprintf(summary, "// Initial Capture\n")
	fmt.Fprintf(summary, "// =================================\n")
	cs.Capture(ctx, t, summarize)

	// Manually reset the state checkpoint back to (0, 70) which is 70% of the way
	// through the initial backfill, then run another capture. This simulates the
	// capture getting killed and restarted partway through the initial backfill.
	require.NoError(t, rewindCheckpoint(cs, 0, 70))
	fmt.Fprintf(summary, "\n")
	fmt.Fprintf(summary, "// =================================\n")
	fmt.Fprintf(summary, "// Backfill Capture Rewound to %s\n", string(sanitize(cs.Checkpoint)))
	fmt.Fprintf(summary, "// =================================\n")
	cs.Capture(ctx, t, summarize)

	// Insert another 100 rows and run a subsequent capture
	insertRows(ctx, 100, 200)
	fmt.Fprintf(summary, "\n")
	fmt.Fprintf(summary, "// =================================\n")
	fmt.Fprintf(summary, "// Subsequent Capture Resuming at %s\n", string(sanitize(cs.Checkpoint)))
	fmt.Fprintf(summary, "// =================================\n")
	cs.Capture(ctx, t, summarize)

	// Reset the subsequent capture back to (3, 60) to simulate killing and restarting
	// the capture partway through a large chunk of staged stream changes.
	require.NoError(t, rewindCheckpoint(cs, 3, 60))
	fmt.Fprintf(summary, "\n")
	fmt.Fprintf(summary, "// =================================\n")
	fmt.Fprintf(summary, "// Subsequent Capture Rewound to %s\n", string(sanitize(cs.Checkpoint)))
	fmt.Fprintf(summary, "// =================================\n")
	cs.Capture(ctx, t, summarize)

	// Run another capture which shouldn't observe any changes
	fmt.Fprintf(summary, "\n")
	fmt.Fprintf(summary, "// =================================\n")
	fmt.Fprintf(summary, "// Subsequent Capture Resuming at %s\n", string(sanitize(cs.Checkpoint)))
	fmt.Fprintf(summary, "// =================================\n")
	cs.Capture(ctx, t, summarize)

	// Do it again to observe that the sequence numbers are ticking over nicely
	fmt.Fprintf(summary, "\n")
	fmt.Fprintf(summary, "// =================================\n")
	fmt.Fprintf(summary, "// Subsequent Capture Resuming at %s\n", string(sanitize(cs.Checkpoint)))
	fmt.Fprintf(summary, "// =================================\n")
	cs.Capture(ctx, t, summarize)

	// Insert another 51 rows and observe those the next time the capture runs
	insertRows(ctx, 200, 251)
	fmt.Fprintf(summary, "\n")
	fmt.Fprintf(summary, "// =================================\n")
	fmt.Fprintf(summary, "// Subsequent Capture Resuming at %s\n", string(sanitize(cs.Checkpoint)))
	fmt.Fprintf(summary, "// =================================\n")
	cs.Capture(ctx, t, summarize)

	cupaloy.SnapshotT(t, summary.String())
}

func TestAddingAndRemovingBindings(t *testing.T) {
	var ctx, tb = context.Background(), snowflakeTestBackend(t)
	var uniqueA, uniqueB, uniqueC = "66311983", "21819672", "47715271"
	var tableA = tb.CreateTable(ctx, t, uniqueA, "(id INTEGER PRIMARY KEY, data TEXT)")
	var tableB = tb.CreateTable(ctx, t, uniqueB, "(id INTEGER PRIMARY KEY, data TEXT)")
	var tableC = tb.CreateTable(ctx, t, uniqueC, "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, tableA, [][]any{{0, "Zero"}, {1, "One"}, {2, "Two"}})
	tb.Insert(ctx, t, tableB, [][]any{{3, "Three"}, {4, "Four"}, {5, "Five"}})
	tb.Insert(ctx, t, tableC, [][]any{{6, "Six"}, {7, "Seven"}, {8, "Eight"}})

	var cs = tb.CaptureSpec(ctx, t)

	var summary = new(strings.Builder)
	var summarize = func(data json.RawMessage) {
		var bs = []byte(data)
		for replacement, matcher := range cs.Sanitizers {
			bs = matcher.ReplaceAll(bs, []byte(replacement))
		}
		fmt.Fprintf(summary, "%s\n", string(bs))
	}

	fmt.Fprintf(summary, "// Just A\n")
	cs.Bindings = discoverBindings(ctx, t, tb, regexp.MustCompile(uniqueA))
	cs.Capture(ctx, t, summarize)

	fmt.Fprintf(summary, "\n// Just B\n")
	cs.Bindings = discoverBindings(ctx, t, tb, regexp.MustCompile(uniqueB))
	cs.Capture(ctx, t, summarize)

	fmt.Fprintf(summary, "\n// Both A and C\n")
	cs.Bindings = discoverBindings(ctx, t, tb, regexp.MustCompile(uniqueA), regexp.MustCompile(uniqueC))
	cs.Capture(ctx, t, summarize)

	fmt.Fprintf(summary, "\n// Both B and C\n")
	cs.Bindings = discoverBindings(ctx, t, tb, regexp.MustCompile(uniqueB), regexp.MustCompile(uniqueC))
	cs.Capture(ctx, t, summarize)

	cupaloy.SnapshotT(t, summary.String())
}

func TestDynamicTable(t *testing.T) {
	var ctx, tb = context.Background(), snowflakeTestBackend(t)
	var uniqueA, uniqueB = "81804963", "99720399"

	// Create the base table and put a few rows in
	var baseTable = tb.CreateTable(ctx, t, uniqueA, "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, baseTable, [][]any{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}, {3, "Four"}, {4, "5"}})

	// Create a dynamic table from the base table
	var dynamicTable = snowflakeObject{*dbName, *dbSchema, "test_" + strings.TrimPrefix(t.Name(), "Test") + "_" + uniqueB}.QuotedName()
	tb.Query(ctx, t, fmt.Sprintf("CREATE OR REPLACE DYNAMIC TABLE %s TARGET_LAG = '60 seconds' WAREHOUSE = '%s' REFRESH_MODE = INCREMENTAL AS SELECT id, data FROM %s;", dynamicTable, *dbWarehouse, baseTable))
	t.Cleanup(func() { tb.Query(ctx, t, fmt.Sprintf("DROP DYNAMIC TABLE IF EXISTS %s;", dynamicTable)) })

	t.Run("discovery", func(t *testing.T) { tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueB)) })

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueB))
	t.Run("initial", func(t *testing.T) { verifiedCapture(ctx, t, cs) })

	tb.Insert(ctx, t, baseTable, [][]any{{6, "F"}, {7, "ggg"}, {8, "QRSTUVWXYZ"}, {9, "Ten"}, {10, "11"}})
	time.Sleep(61 * time.Second) // Wait for the dynamic table to refresh
	t.Run("subsequent", func(t *testing.T) { verifiedCapture(ctx, t, cs) })
}
