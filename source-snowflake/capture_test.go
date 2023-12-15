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
