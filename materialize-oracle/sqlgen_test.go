package connector

import (
	"math"
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

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
		for _, tc := range []struct {
			name string
			sql  string
		}{
			{"insert", insertSQL(&tbl)},
			{"delete", deleteSQL(&tbl)},
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

	snap.WriteString("--- Begin Fence Update ---\n")
	snap.WriteString(updateFenceSQL(testDialect, sql.Fence{
		TablePath:       sql.TablePath{"path", "to", "checkpoints"},
		Fence:           123,
		Materialization: pf.Materialization("some/Materialization"),
		KeyBegin:        0x00112233,
		KeyEnd:          0xffeeddcc,
	}))
	snap.WriteString("\n--- End Fence Update ---\n")

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
