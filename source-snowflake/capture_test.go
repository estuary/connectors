package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/connectors/sqlcapture/tests"
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

	tb.Insert(ctx, t, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}, {3, "Four"}, {4, "5"}})
	t.Run("init", func(t *testing.T) { verifiedCapture(ctx, t, cs) })

	tb.Insert(ctx, t, tableName, [][]interface{}{{6, "F"}, {7, "ggg"}, {8, "QRSTUVWXYZ"}, {9, "Ten"}, {10, "11"}})
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

	tb.Insert(ctx, t, tableName, [][]interface{}{{0, "A"}, {1, "bbb"}, {2, "CDEFGHIJKLMNOP"}, {3, "Four"}, {4, "5"}})
	t.Run("init", func(t *testing.T) { verifiedCapture(ctx, t, cs) })

	tb.Insert(ctx, t, tableName, [][]interface{}{{6, "F"}, {7, "ggg"}, {8, "QRSTUVWXYZ"}, {9, "Ten"}, {10, "11"}})
	t.Run("main", func(t *testing.T) { verifiedCapture(ctx, t, cs) })
}

func TestBasicDatatypes(t *testing.T) {
	var ctx, tb = context.Background(), snowflakeTestBackend(t)
	var uniqueID = "77528227"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER, data TEXT, bdata VARBINARY, fdata FLOAT, bit BOOLEAN, ymd DATE, hms TIME, tstz TIMESTAMP_TZ, tsntz TIMESTAMP_NTZ)")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

	t.Run("discover", func(t *testing.T) { cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID)) })

	var x = "1991-08-31T09:25:27Z"
	tb.Insert(ctx, t, tableName, [][]interface{}{{0, "abcd", []byte{0xFF, 0xA5, 0x00, 0x5A}, 1.32, true, "1991-08-31", "09:25:27", x, x}})
	t.Run("init", func(t *testing.T) { verifiedCapture(ctx, t, cs) })

	var y = "1991-11-25T06:13:44Z"
	tb.Insert(ctx, t, tableName, [][]interface{}{{1, "DCBA", []byte{0xEE, 0x5A, 0x11, 0xA5}, 2.64, false, "1991-11-25", "06:13:44", y, y}})
	t.Run("main", func(t *testing.T) { verifiedCapture(ctx, t, cs) })
}

func TestLargeCapture(t *testing.T) {
	var ctx, tb = context.Background(), snowflakeTestBackend(t)
	var uniqueID = "63855638"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

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

	var summary = new(strings.Builder)

	// Insert 100 rows and then run the first capture
	insertRows(ctx, 0, 100)
	fmt.Fprintf(summary, "// =================================\n")
	fmt.Fprintf(summary, "// Initial Capture\n")
	fmt.Fprintf(summary, "// =================================\n")
	cs.Capture(ctx, t, func(data json.RawMessage) { fmt.Fprintf(summary, "%s\n", string(data)) })

	// Manually reset the state checkpoint back to (0, 70) which is 70% of the way
	// through the initial backfill, then run another capture. This simulates the
	// capture getting killed and restarted partway through the initial backfill.
	require.NoError(t, rewindCheckpoint(cs, 0, 70))
	fmt.Fprintf(summary, "\n")
	fmt.Fprintf(summary, "// =================================\n")
	fmt.Fprintf(summary, "// Backfill Capture Rewound to %s\n", string(cs.Checkpoint))
	fmt.Fprintf(summary, "// =================================\n")
	cs.Capture(ctx, t, func(data json.RawMessage) { fmt.Fprintf(summary, "%s\n", string(data)) })

	// Insert another 100 rows and run a subsequent capture
	insertRows(ctx, 100, 200)
	fmt.Fprintf(summary, "\n")
	fmt.Fprintf(summary, "// =================================\n")
	fmt.Fprintf(summary, "// Subsequent Capture Resuming at %s\n", string(cs.Checkpoint))
	fmt.Fprintf(summary, "// =================================\n")
	cs.Capture(ctx, t, func(data json.RawMessage) { fmt.Fprintf(summary, "%s\n", string(data)) })

	// Reset the subsequent capture back to (3, 60) to simulate killing and restarting
	// the capture partway through a large chunk of staged stream changes.
	require.NoError(t, rewindCheckpoint(cs, 3, 60))
	fmt.Fprintf(summary, "\n")
	fmt.Fprintf(summary, "// =================================\n")
	fmt.Fprintf(summary, "// Subsequent Capture Rewound to %s\n", string(cs.Checkpoint))
	fmt.Fprintf(summary, "// =================================\n")
	cs.Capture(ctx, t, func(data json.RawMessage) { fmt.Fprintf(summary, "%s\n", string(data)) })

	// Run another capture which shouldn't observe any changes
	fmt.Fprintf(summary, "\n")
	fmt.Fprintf(summary, "// =================================\n")
	fmt.Fprintf(summary, "// Subsequent Capture Resuming at %s\n", string(cs.Checkpoint))
	fmt.Fprintf(summary, "// =================================\n")
	cs.Capture(ctx, t, func(data json.RawMessage) { fmt.Fprintf(summary, "%s\n", string(data)) })

	// Do it again to observe that the sequence numbers are ticking over nicely
	fmt.Fprintf(summary, "\n")
	fmt.Fprintf(summary, "// =================================\n")
	fmt.Fprintf(summary, "// Subsequent Capture Resuming at %s\n", string(cs.Checkpoint))
	fmt.Fprintf(summary, "// =================================\n")
	cs.Capture(ctx, t, func(data json.RawMessage) { fmt.Fprintf(summary, "%s\n", string(data)) })

	// Insert another 51 rows and observe those the next time the capture runs
	insertRows(ctx, 200, 251)
	fmt.Fprintf(summary, "\n")
	fmt.Fprintf(summary, "// =================================\n")
	fmt.Fprintf(summary, "// Subsequent Capture Resuming at %s\n", string(cs.Checkpoint))
	fmt.Fprintf(summary, "// =================================\n")
	cs.Capture(ctx, t, func(data json.RawMessage) { fmt.Fprintf(summary, "%s\n", string(data)) })

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

	var summary = new(strings.Builder)

	fmt.Fprintf(summary, "// Just A\n")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueA))
	cs.Capture(ctx, t, func(data json.RawMessage) { fmt.Fprintf(summary, "%s\n", string(data)) })

	fmt.Fprintf(summary, "\n// Just B\n")
	cs.Bindings = tests.DiscoverBindings(ctx, t, tb, regexp.MustCompile(uniqueB))
	cs.Capture(ctx, t, func(data json.RawMessage) { fmt.Fprintf(summary, "%s\n", string(data)) })

	fmt.Fprintf(summary, "\n// Both A and C\n")
	cs.Bindings = tests.DiscoverBindings(ctx, t, tb, regexp.MustCompile(uniqueA), regexp.MustCompile(uniqueC))
	cs.Capture(ctx, t, func(data json.RawMessage) { fmt.Fprintf(summary, "%s\n", string(data)) })

	fmt.Fprintf(summary, "\n// Both B and C\n")
	cs.Bindings = tests.DiscoverBindings(ctx, t, tb, regexp.MustCompile(uniqueB), regexp.MustCompile(uniqueC))
	cs.Capture(ctx, t, func(data json.RawMessage) { fmt.Fprintf(summary, "%s\n", string(data)) })

	cupaloy.SnapshotT(t, summary.String())
}
