package main

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
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
