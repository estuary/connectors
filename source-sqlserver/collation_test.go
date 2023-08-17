package main

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/connectors/sqlcapture/tests"
	"github.com/stretchr/testify/require"
)

func TestVarcharKeyDiscovery(t *testing.T) {
	var tb, ctx = sqlserverTestBackend(t), context.Background()
	var uniqueID = "42325208"
	tb.CreateTable(ctx, t, uniqueID, fmt.Sprintf("(id VARCHAR(32) PRIMARY KEY, data TEXT)"))
	tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
}

// TestCollatedCapture runs a complete capture from a table with a `VARCHAR
// COLLATE SQL_Latin1_General_CP1_CI_AS` primary key, with key values which
// sort differently in SQL_Latin1_General_CP1_CI_AS compared to the Unicode
// lexicographic sort order.
//
// The capture output will be in Unicode sorted order since capture output
// is generally sorted for stability. However by running a full capture we
// ensure that the internal key-ordering checks will also be run, so given
// that the test passes we know the backfill key ordering was correct.
func TestCollatedCapture(t *testing.T) {
	var tb, ctx = sqlserverTestBackend(t), context.Background()
	var uniqueID = "00913034"
	var tableName = tb.CreateTable(ctx, t, uniqueID, fmt.Sprintf("(id VARCHAR(32) COLLATE SQL_Latin1_General_CP1_CI_AS PRIMARY KEY, data TEXT)"))
	tb.Insert(ctx, t, tableName, [][]any{
		{"a?a", "Row A01"}, {"a?b", "Row B01"}, // U+003F
		{"aaa", "Row A02"}, {"aab", "Row B02"}, // U+0061
		{"aba", "Row A03"}, {"abb", "Row B03"}, // U+0062
		{"aca", "Row A04"}, {"acb", "Row B04"}, // U+0063
		{"ada", "Row A05"}, {"adb", "Row B05"}, // U+0064
		{"a°a", "Row A06"}, {"a°b", "Row B06"}, // U+00B0
		{"a·a", "Row A07"}, {"a·b", "Row B07"}, // U+00B7
		{"aƒa", "Row A08"}, {"aƒb", "Row B08"}, // U+0192
		{"aµa", "Row A09"}, {"aµb", "Row B09"}, // U+03BC
		{"a†a", "Row A10"}, {"a†b", "Row B10"}, // U+2020
		{"a…a", "Row A11"}, {"a…b", "Row B11"}, // U+2026
		{"a€a", "Row A12"}, {"a€b", "Row B12"}, // U+20AC
	})
	tests.VerifiedCapture(ctx, t, tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID)))
}

// TestColumnCollations verifies that `encodeKeyFDB` produces serialized keys
// for text columns whose bytewise lexicographic ordering matches the database
// collation ordering. Since we want to rapidly test a whole bunch of tables
// this test skips running an actual capture and just directly queries the
// table and calls `sqlcapture.EncodeRowKey` on the result rows.
func TestColumnCollations(t *testing.T) {
	var tb, ctx = sqlserverTestBackend(t), context.Background()

	// Generate a set of unique test strings which we expect to exhibit
	// interesting sort behavior. Many of these runes have been selected
	// deliberately to exercise code points which various SQL Server code
	// pages sort differently from Unicode code point order. This list
	// should only ever be added to.
	var testRunes = "abc?@-_,·°€丠両░▒▓█αßσµ☆★"
	var testStrings []string
	for idx, r := range testRunes {
		testStrings = append(testStrings, fmt.Sprintf("%s %06d", string([]rune{'a', r, 'a'}), idx*3+0))
		testStrings = append(testStrings, fmt.Sprintf("%s %06d", string([]rune{'b', r, 'b'}), idx*3+1))
		testStrings = append(testStrings, fmt.Sprintf("%s %06d", string([]rune{'c', r, 'c'}), idx*3+2))
	}

	for idx, tc := range []struct {
		ColumnType    string
		CollationName string
	}{
		{"VARCHAR", "SQL_Latin1_General_CP1_CI_AI"},
		{"VARCHAR", "SQL_Latin1_General_CP1_CI_AS"}, // SQL Server Default
		{"VARCHAR", "SQL_Latin1_General_CP1_CS_AS"},
		{"VARCHAR", "SQL_Latin1_General_CP437_BIN"},
		{"VARCHAR", "SQL_Latin1_General_CP437_BIN2"},
		{"VARCHAR", "SQL_Latin1_General_CP437_CI_AI"},
		{"VARCHAR", "SQL_Latin1_General_CP437_CI_AS"},
		{"VARCHAR", "SQL_Latin1_General_CP437_CS_AS"},
		{"VARCHAR", "Latin1_General_100_CI_AS_SC"},
		{"VARCHAR", "Latin1_General_100_CS_AS_WS_SC_UTF8"},
		{"VARCHAR", "Latin1_General_100_BIN2_UTF8"},
		{"VARCHAR", "Chinese_PRC_BIN2"},
		{"NVARCHAR", "SQL_Latin1_General_CP1_CI_AI"},
		{"NVARCHAR", "SQL_Latin1_General_CP1_CI_AS"}, // SQL Server Default
		{"NVARCHAR", "SQL_Latin1_General_CP1_CS_AS"},
		{"NVARCHAR", "SQL_Latin1_General_CP437_BIN"},
		{"NVARCHAR", "SQL_Latin1_General_CP437_BIN2"},
		{"NVARCHAR", "SQL_Latin1_General_CP437_CI_AI"},
		{"NVARCHAR", "SQL_Latin1_General_CP437_CI_AS"},
		{"NVARCHAR", "SQL_Latin1_General_CP437_CS_AS"},
		{"NVARCHAR", "Latin1_General_100_CI_AS_SC"},
		{"NVARCHAR", "Latin1_General_100_CS_AS_WS_SC_UTF8"},
		{"NVARCHAR", "Latin1_General_100_BIN2_UTF8"},
		{"NVARCHAR", "Chinese_PRC_BIN2"},
	} {
		var subtestName = fmt.Sprintf("%s_%s", tc.ColumnType, tc.CollationName)
		t.Run(subtestName, func(t *testing.T) {
			var uniqueID = fmt.Sprintf("2553_%04d", idx)
			var tableName = tb.CreateTable(ctx, t, uniqueID, fmt.Sprintf("(id %s(32) COLLATE %s PRIMARY KEY, data TEXT)", tc.ColumnType, tc.CollationName))

			var insertRows [][]any
			for idx, str := range testStrings {
				insertRows = append(insertRows, []any{str, fmt.Sprintf("This is test row %d with contents %q", idx, str)})
			}
			tb.Insert(ctx, t, tableName, insertRows)

			var resultRows, err = tb.control.QueryContext(ctx, fmt.Sprintf(`SELECT id, data FROM %s ORDER BY id`, tableName))
			require.NoError(t, err)
			defer resultRows.Close()

			// Simulate what we'd end up with after all the discovery->backfill plumbing.
			var keyColumns = []string{"id"}
			var columnTypes = map[string]any{
				"id":   &sqlserverTextColumnType{strings.ToLower(tc.ColumnType), tc.CollationName},
				"data": &sqlserverTextColumnType{"text", "SQL_Latin1_General_CP1_CI_AS"},
			}

			var prevID string
			var prevKey []byte
			for resultRows.Next() {
				var id, data string
				require.NoError(t, resultRows.Scan(&id, &data))
				var rowKey, err = sqlcapture.EncodeRowKey(keyColumns, map[string]any{"id": id, "data": data}, columnTypes, encodeKeyFDB)
				require.NoError(t, err)

				if bytes.Compare(prevKey, rowKey) >= 0 {
					t.Fatalf("key ordering failure: we think %q >= %q (in `%s(32) COLLATE %s` column) but the server disagrees", prevID, id, tc.ColumnType, tc.CollationName)
				}
				prevID, prevKey = id, rowKey
			}
		})
	}
}
