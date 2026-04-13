package main

import (
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

func TestSQLGeneration(t *testing.T) {

	snap, tables := sql.RunSqlGenTests(
		t,
		testDialect,
		func(table string) []string {
			return []string{"a-schema", table}
		},
		sql.TestTemplates{
			TableTemplates: []*template.Template{
				testTemplates.createTargetTable,
			},
			TplAddColumns:    testTemplates.alterTableColumns,
			TplDropNotNulls:  testTemplates.alterTableColumns,
			TplCombinedAlter: testTemplates.alterTableColumns,
		},
	)

	for _, tpl := range []*template.Template{
		testTemplates.loadQuery,
		testTemplates.loadQueryNoFlowDocument,
		testTemplates.mergeInto,
	} {
		tbl := tables[0]
		require.False(t, tbl.DeltaUpdates)
		var testcase = tbl.Identifier + " " + tpl.Name()

		bounds := []sql.MergeBound{
			{
				Column:       tbl.Keys[0],
				LiteralLower: testDialect.Literal(int64(10)),
				LiteralUpper: testDialect.Literal(int64(100)),
			},
			{
				Column: tbl.Keys[1],
				// No bounds - as would be the case for a boolean key, which
				// would be a very weird key, but technically allowed.
			},
			{
				Column:       tbl.Keys[2],
				LiteralLower: testDialect.Literal("aGVsbG8K"),
				LiteralUpper: testDialect.Literal("Z29vZGJ5ZQo="),
			},
		}

		tf := boundedQueryInput{
			Table:  tbl,
			File:   "test-file",
			Bounds: bounds,
		}

		snap.WriteString("--- Begin " + testcase + " ---")
		require.NoError(t, tpl.Execute(snap, &tf))
		snap.WriteString("--- End " + testcase + " ---\n\n")
	}

	// Re-run the bound-aware templates with bounds that include observed null
	// keys, to cover the IsNull paths in the templates.
	for _, tpl := range []*template.Template{
		testTemplates.loadQuery,
		testTemplates.loadQueryNoFlowDocument,
		testTemplates.mergeInto,
	} {
		tbl := tables[0]
		var testcase = tbl.Identifier + " " + tpl.Name() + " (with null bounds)"

		bounds := []sql.MergeBound{
			{
				// Range bound and null observed: must produce the
				// parenthesized "(range OR IS NULL)" predicate.
				Column:       tbl.Keys[0],
				LiteralLower: testDialect.Literal(int64(10)),
				LiteralUpper: testDialect.Literal(int64(100)),
				IsNull:       true,
			},
			{
				// Boolean key: the builder always emits an empty MergeBound
				// for booleans (no literals, IsNull never set), regardless
				// of observed values. Mirrored here so the snapshot reflects
				// production-realistic input.
				Column: tbl.Keys[1],
			},
			{
				// Null observed but no literal bounds (e.g. all observed
				// values for the column were null): emit a bare IS NULL.
				Column: tbl.Keys[2],
				IsNull: true,
			},
		}

		tf := boundedQueryInput{
			Table:  tbl,
			File:   "test-file",
			Bounds: bounds,
		}

		snap.WriteString("--- Begin " + testcase + " ---")
		require.NoError(t, tpl.Execute(snap, &tf))
		snap.WriteString("--- End " + testcase + " ---\n\n")
	}

	for _, tpl := range []*template.Template{
		testTemplates.copyInto,
	} {
		tbl := tables[0]
		require.False(t, tbl.DeltaUpdates)
		var testcase = tbl.Identifier + " " + tpl.Name()

		var tf = tableAndFile{
			Table: tbl,
			File:  "test-file",
		}

		snap.WriteString("--- Begin " + testcase + " ---")
		require.NoError(t, tpl.Execute(snap, &tf))
		snap.WriteString("--- End " + testcase + " ---\n\n")
	}

	for _, tpl := range []*template.Template{
		testTemplates.copyInto,
	} {
		tbl := tables[1]
		require.True(t, tbl.DeltaUpdates)
		require.Nil(t, tbl.Document)
		var testcase = tbl.Identifier + " " + tpl.Name()

		var tf = tableAndFile{
			Table: tbl,
			File:  "test-file",
		}

		snap.WriteString("--- Begin " + testcase + " ---")
		require.NoError(t, tpl.Execute(snap, &tf))
		snap.WriteString("--- End " + testcase + " ---\n\n")
	}

	for _, tpl := range []*template.Template{
		testTemplates.createPipe,
	} {
		tbl := tables[0]
		require.False(t, tbl.DeltaUpdates)
		var testcase = tbl.Identifier + " " + tpl.Name()

		var tf = tablePipe{
			Table:    tbl,
			PipeName: "db.schema.flow_pipe_0_tableName_00000000_0",
		}

		snap.WriteString("--- Begin " + testcase + " ---")
		require.NoError(t, tpl.Execute(snap, &tf))
		snap.WriteString("--- End " + testcase + " ---\n\n")
	}

	cupaloy.SnapshotT(t, snap.String())
}
