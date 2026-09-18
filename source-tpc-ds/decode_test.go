package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var testTable = &tableDef{
	Name: "t",
	Columns: []column{
		{Name: "id", Kind: kindInteger, NotNull: true},
		{Name: "amount", Kind: kindDecimal},
		{Name: "day", Kind: kindDate},
		{Name: "label", Kind: kindString},
	},
	Key: []string{"id"},
}

func TestDecodeLine(t *testing.T) {
	for _, tc := range []struct {
		name, line, want string
	}{
		{"all kinds", "7|-12.50|1998-12-31|hello|", `{"id":7,"amount":"-12.50","day":"1998-12-31","label":"hello"}`},
		{"no trailing delimiter", "7|1.00|2000-01-01|x", `{"id":7,"amount":"1.00","day":"2000-01-01","label":"x"}`},
		{"nulls omitted", "-3||||", `{"id":-3}`},
		{"integral decimal", "1|-5|||", `{"id":1,"amount":"-5"}`},
		{"string escaping", `1|||say "hi" \ there|`, `{"id":1,"label":"say \"hi\" \\ there"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc, err := decodeLine(testTable, []byte(tc.line))
			require.NoError(t, err)
			require.Equal(t, tc.want, string(doc))
		})
	}
}

func TestDecodeLineRejects(t *testing.T) {
	for _, tc := range []struct{ name, line, want string }{
		{"too few fields", "1|2|3|", "expected 4 fields, got 3"},
		{"too many fields", "1|2|3|4|5|", "expected 4 fields, got 5"},
		{"empty not-null", "|1.00|2000-01-01|x|", "NOT NULL column id is empty"},
		{"bad integer", "1x||||", "not an integer"},
		{"bad decimal", "1|1.2.3|||", "not a decimal"},
		{"bad date", "1||19981231||", "not a YYYY-MM-DD date"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeLine(testTable, []byte(tc.line))
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestRouteLine(t *testing.T) {
	var parent = tableByName("store_sales")
	require.Len(t, parent.Columns, 23)
	require.Len(t, tableByName("store_returns").Columns, 20)

	var line = func(n int) []byte { return []byte(strings.Repeat("1|", n)) }
	got, err := routeLine(parent, line(23))
	require.NoError(t, err)
	require.Equal(t, "store_sales", got.Name)

	got, err = routeLine(parent, line(20))
	require.NoError(t, err)
	require.Equal(t, "store_returns", got.Name)

	_, err = routeLine(parent, line(21))
	require.ErrorContains(t, err, "21 fields matches no table in the store_sales stream")
}

func TestTables(t *testing.T) {
	require.Len(t, tables, 24)
	var parents = map[string]int{}
	for _, tbl := range tables {
		require.NotEmpty(t, tbl.Key, tbl.Name)
		for _, k := range tbl.Key {
			var found bool
			for _, c := range tbl.Columns {
				if c.Name == k {
					found = true
					require.True(t, c.NotNull, "%s key %s must be NOT NULL", tbl.Name, k)
				}
			}
			require.True(t, found, "%s key %s is not a column", tbl.Name, k)
		}
		if tbl.Parent != "" {
			parents[tbl.Parent]++
			require.NotNil(t, tableByName(tbl.Parent))
		}
	}
	require.Equal(t, map[string]int{"store_sales": 1, "catalog_sales": 1, "web_sales": 1}, parents)
	// Routing by field count needs distinct counts within a stream.
	for _, tbl := range tables {
		for _, c := range children(tbl) {
			require.NotEqual(t, len(tbl.Columns), len(c.Columns), "%s and %s have the same field count", tbl.Name, c.Name)
		}
	}
}

func TestPlanChunks(t *testing.T) {
	require.Equal(t, 1, planChunks(1))
	require.Equal(t, 1, planChunks(chunkTargetRows))
	require.Equal(t, 2, planChunks(chunkTargetRows+1))
	require.Equal(t, 3, planChunks(2_500_000))
	require.Equal(t, 240, planChunks(240_000_000))
}
