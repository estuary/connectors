package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/estuary/connectors/go/sqlserver/datatypes"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/stretchr/testify/require"
)

// TestColumnCollations verifies that our encoding of text column keys can
// be round-tripped through FDB serialization/deserialization without loss,
// and that for collations which are considered "predictable" the serialized
// key ordering matches the server result ordering.
func TestColumnCollations(t *testing.T) {
	// Generate a set of unique test strings which we expect to exhibit
	// interesting sort behavior. Many of these runes have been selected
	// deliberately to exercise code points which various SQL Server code
	// pages sort differently from Unicode code point order.
	var testRunes = "abc?@-_,·°€丠両░▒▓█αßσµ☆★"
	var testStrings []string
	for idx, r := range testRunes {
		testStrings = append(testStrings, fmt.Sprintf("%s %06d", string([]rune{'a', r, 'a'}), idx*3+0))
		testStrings = append(testStrings, fmt.Sprintf("%s %06d", string([]rune{'b', r, 'b'}), idx*3+1))
		testStrings = append(testStrings, fmt.Sprintf("%s %06d", string([]rune{'c', r, 'c'}), idx*3+2))
	}

	for _, tc := range []struct {
		ColumnType    string
		CollationName string
	}{
		{"VARCHAR", "SQL_Latin1_General_CP1_CI_AS"}, // SQL Server Default
		{"VARCHAR", "SQL_Latin1_General_CP437_BIN"},
		{"VARCHAR", "Latin1_General_100_BIN2_UTF8"},
		{"VARCHAR", "Chinese_PRC_BIN2"},
		{"NVARCHAR", "SQL_Latin1_General_CP1_CI_AS"}, // SQL Server Default
		{"NVARCHAR", "SQL_Latin1_General_CP437_BIN"},
		{"NVARCHAR", "Latin1_General_100_BIN2_UTF8"},
		{"NVARCHAR", "Chinese_PRC_BIN2"},
	} {
		var subtestName = fmt.Sprintf("%s_%s", tc.ColumnType, tc.CollationName)
		t.Run(subtestName, func(t *testing.T) {
			var db, _ = blackboxTestSetup(t)
			var ctx = context.Background()
			db.CreateTable(t, `<NAME>`, fmt.Sprintf(`(id %s(32) COLLATE %s PRIMARY KEY, data TEXT)`, tc.ColumnType, tc.CollationName))

			// Insert test strings
			for idx, str := range testStrings {
				// Escape single quotes for SQL
				var escaped = strings.ReplaceAll(str, "'", "''")
				db.Exec(t, fmt.Sprintf(`INSERT INTO <NAME> VALUES ('%s', 'This is test row %d with contents %q')`, escaped, idx, str))
			}

			var resultRows, err = db.conn.QueryContext(ctx, fmt.Sprintf(`SELECT id, data FROM %s ORDER BY id`, db.Expand(`<NAME>`)))
			require.NoError(t, err)
			defer resultRows.Close()

			// Simulate what we'd end up with after all the discovery->backfill plumbing.
			var keyColumns = []string{"id"}
			var idCollation = &datatypes.TextColumnType{Type: strings.ToLower(tc.ColumnType), Collation: tc.CollationName, FullType: tc.ColumnType + "(32)", MaxLength: 32}
			var columnTypes = map[string]any{
				"id":   idCollation,
				"data": &datatypes.TextColumnType{Type: "text", Collation: "SQL_Latin1_General_CP1_CI_AS", FullType: "TEXT", MaxLength: 0},
			}

			var prevID string
			var prevKey []byte
			for resultRows.Next() {
				var id, data string
				require.NoError(t, resultRows.Scan(&id, &data))
				var rowKey, err = sqlcapture.EncodeRowKey(keyColumns, map[string]any{"id": id, "data": data}, columnTypes, datatypes.EncodeKeyFDB)
				require.NoError(t, err)

				roundTrippedKey, err := sqlcapture.UnpackTuple(rowKey, datatypes.DecodeKeyFDB)
				require.NoError(t, err)
				if len(roundTrippedKey) != 1 || id != roundTrippedKey[0] {
					t.Fatalf("key round-trip failure: originally %#v but now %#v", id, roundTrippedKey)
				}

				if datatypes.PredictableCollation(idCollation) && bytes.Compare(prevKey, rowKey) >= 0 {
					t.Fatalf("key ordering failure: we think %q >= %q (in `%s(32) COLLATE %s` column) but the server disagrees", prevID, id, tc.ColumnType, tc.CollationName)
				}
				prevID, prevKey = id, rowKey
			}
		})
	}
}
