package connector

import (
	"math"
	"strings"
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

var testDialect = newOracleDialect("FLOW", 4000)
var testTemplates = renderTemplates(testDialect)

func TestSQLGeneration(t *testing.T) {
	snap, _ := sql.RunSqlGenTests(
		t,
		testDialect,
		func(table string) []string { return []string{table} },
		sql.TestTemplates{
			TableTemplates: []*template.Template{
				testTemplates.createTargetTable,
				testTemplates.createLoadTable,
				testTemplates.loadInsert,
				testTemplates.loadQuery,
				testTemplates.storeInsert,
				testTemplates.storeUpdate,
				testTemplates.deleteQuery,
			},
			TplAddColumns:    testTemplates.alterTableColumns,
			TplDropNotNulls:  testTemplates.alterTableColumns,
			TplCombinedAlter: testTemplates.alterTableColumns,
			TplUpdateFence:   testTemplates.updateFence,
		},
	)

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
		{"except", `"except"`, "except"},
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
		{timestampConverter, "0001-01-01T00:30:00+01:00", "0001-01-01T00:00:00.000000000+00:00"},
		{timestampConverter, "9999-12-31T23:30:00-01:00", "9999-12-31T23:59:59.999999999+00:00"},
	} {
		got, err := tc.conv(tc.in)
		require.NoError(t, err)
		require.Equal(t, tc.want, got, "%v", tc.in)
	}
}

func TestStringAndBinaryColumns(t *testing.T) {
	var mapType = func(inference pf.Inference) sql.MappedType {
		inference.Exists = pf.Inference_MUST
		return testDialect.MapType(&sql.Projection{Projection: pf.Projection{Inference: inference}}, sql.FieldConfig{})
	}

	// Oracle stores an empty BLOB as NULL, so even a required one is nullable.
	var binary = mapType(pf.Inference{Types: []string{"string"}, String_: &pf.Inference_String{ContentEncoding: "base64"}})
	require.Equal(t, "BLOB", binary.DDL)

	// Only a key's VARCHAR2 is sized by its maxLength.
	var short = pf.Inference{Types: []string{"string"}, String_: &pf.Inference_String{MaxLength: 10}}
	require.Equal(t, "VARCHAR2(4000 CHAR)", mapType(short).DDL)
	var key = testDialect.MapType(&sql.Projection{Projection: pf.Projection{Inference: short, IsPrimaryKey: true}}, sql.FieldConfig{})
	require.Equal(t, "VARCHAR2(10 CHAR)", key.BareDDL)

	// A string's VARCHAR2 also accepts an existing CLOB.
	var unbounded = mapType(pf.Inference{Types: []string{"string"}, String_: &pf.Inference_String{}})
	require.Equal(t, "VARCHAR2(4000 CHAR)", unbounded.DDL)
	require.True(t, unbounded.Compatible(boilerplate.ExistingField{Type: "CLOB"}))
	require.False(t, unbounded.Compatible(boilerplate.ExistingField{Type: "VARCHAR2", CharacterMaxLength: 100}))
}

func TestOverflowingColumns(t *testing.T) {
	// The first column isn't a VARCHAR2, the second holds 10 characters and
	// the third 4000, which is also the most bytes a VARCHAR2 holds.
	var b = &binding{varcharChars: []int{0, 10, 4000}}
	for _, tc := range []struct {
		row  []any
		want []int
	}{
		{[]any{strings.Repeat("x", 5000), strings.Repeat("é", 10), strings.Repeat("x", 4000)}, nil},
		{[]any{nil, strings.Repeat("x", 11), strings.Repeat("x", 4001)}, []int{1, 2}},
		{[]any{nil, nil, strings.Repeat("é", 2001)}, []int{2}},
		{[]any{int64(1), "", ""}, nil},
	} {
		require.Equal(t, tc.want, b.overflowingColumns(tc.row, 4000))
	}
}

func TestIntegerCompatibility(t *testing.T) {
	var integer = testDialect.MapType(&sql.Projection{Projection: pf.Projection{
		Inference: pf.Inference{Types: []string{"integer"}, Exists: pf.Inference_MUST},
	}}, sql.FieldConfig{})
	require.Equal(t, "NUMBER(38,0)", integer.BareDDL)

	for existing, want := range map[string]bool{
		"NUMBER(38,0)": true,
		"NUMBER(*,0)":  true,
		"NUMBER(10,0)": false,
		"NUMBER":       false,
	} {
		require.Equal(t, want, integer.Compatible(boilerplate.ExistingField{Type: existing}), existing)
	}
}
