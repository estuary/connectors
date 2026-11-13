package connector

import (
	"math"
	"strings"
	"testing"
	"text/template"
	"unicode/utf8"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

func TestStagePath(t *testing.T) {
	require.Equal(t, []string{"flow_stage_orders"}, stagePath([]string{"orders"}))
	require.Equal(t, []string{"schema", "flow_stage_My Table"}, stagePath([]string{"schema", "My Table"}))

	// A name that would be too long keeps a prefix of the table's name, cut at
	// a character boundary, and a hash of all of it.
	for _, table := range []string{strings.Repeat("a", 128), strings.Repeat("é", 64), strings.Repeat("a", 117) + "é"} {
		var name = stagePath([]string{table})[0]
		require.LessOrEqual(t, len(name), maxIdentifierBytes, table)
		require.True(t, utf8.ValidString(name), table)
		require.True(t, strings.HasPrefix(name, stagePrefix), table)
		require.NotEqual(t, name, stagePath([]string{table + "x"})[0], table)
	}
}

var testDialect = newOracleDialect("FLOW")

func TestSQLGeneration(t *testing.T) {
	snap, tables := sql.RunSqlGenTests(
		t,
		testDialect,
		func(table string) []string { return []string{table} },
		sql.TestTemplates{
			TableTemplates: []*template.Template{renderTemplates(testDialect).createTargetTable},
		},
	)

	for _, tbl := range tables {
		var stage = testDialect.Identifier(stagePath(tbl.Path)...)
		for _, tc := range []struct {
			name string
			sql  string
		}{
			{"create stage", createStageSQL(&tbl, stage)},
			{"stage insert", stageInsertSQL(&tbl, stage)},
			{"apply update", applyDeleteSQL(&tbl, stage, opUpdate, 2)},
			{"apply insert", applyInsertSQL(&tbl, stage, 2)},
			{"clear stage", clearStageSQL(stage, 2)},
			{"prune stage", pruneStageSQL(stage, 1)},
		} {
			snap.WriteString("--- Begin " + tbl.Identifier + " " + tc.name + " ---\n")
			snap.WriteString(tc.sql)
			snap.WriteString("\n--- End " + tbl.Identifier + " " + tc.name + " ---\n\n")
		}
		if tbl.Document != nil {
			snap.WriteString("--- Begin " + tbl.Identifier + " load ---\n")
			snap.WriteString(loadSQL(&tbl))
			snap.WriteString("\n--- End " + tbl.Identifier + " load ---\n\n")
		}
	}

	cupaloy.SnapshotT(t, snap.String())
}

func TestIdentifiers(t *testing.T) {
	for _, tc := range []struct {
		in, quoted, stored string
	}{
		{"simple", "simple", "SIMPLE"},
		{"MixedCase", "MixedCase", "MIXEDCASE"},
		{"with space", `"with space"`, "with space"},
		{"_leading", `"_leading"`, "_leading"},
		{"123", `"123"`, "123"},
		{"date", `"date"`, "date"},
		{`a"b`, `"a_b"`, "a_b"},
		{"dollar$", `"dollar$"`, "dollar$"},
	} {
		require.Equal(t, tc.quoted, testDialect.Identifier(tc.in), tc.in)
		require.Equal(t, tc.stored, testDialect.ColumnLocator(tc.in), tc.in)
	}
}

func TestConverters(t *testing.T) {
	for _, tc := range []struct {
		conv sql.ElementConverter
		in   any
		want any
	}{
		{numberConverter, int64(-12), "-12"},
		{numberConverter, uint64(math.MaxUint64), "18446744073709551615"},
		{numberConverter, "123.000", "123"},
		{numberConverter, nil, nil},
		{floatConverter, 1.5, "1.5"},
		{floatConverter, 1e300, "1e+300"},
		{floatConverter, "NaN", "NaN"},
		{floatConverter, "Infinity", "Inf"},
		{floatConverter, "-Infinity", "-Inf"},
		{floatConverter, int64(3), "3"},
		{timestampConverter, "2024-01-02T03:04:05.1+01:00", "2024-01-02T02:04:05.100000000+00:00"},
		{timestampConverter, "2024-01-02T03:04:05z", "2024-01-02T03:04:05.000000000+00:00"},
		{timestampConverter, "0000-01-01T00:00:00Z", "0001-01-01T00:00:00.000000000+00:00"},
	} {
		got, err := tc.conv(tc.in)
		require.NoError(t, err)
		require.Equal(t, tc.want, got, "%v", tc.in)
	}

	_, err := floatConverter("abc")
	require.Error(t, err)
}

func TestVarcharLength(t *testing.T) {
	_, _, conv := varcharMapping(3, nil)

	got, err := conv("ab𐀀")
	require.NoError(t, err)
	require.Equal(t, "ab𐀀", got)

	got, err = conv(nil)
	require.NoError(t, err)
	require.Nil(t, got)

	_, err = conv("abcd")
	require.EqualError(t, err, "a value of 4 characters is longer than the column's VARCHAR2(3 CHAR)")

	_, _, conv = varcharMapping(2, sql.ToStr)
	_, err = conv(int64(123))
	require.Error(t, err)
}
