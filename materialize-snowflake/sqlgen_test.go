package main

import (
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

var testDialect = snowflakeDialect("public", timestampLTZ)

func TestSQLGeneration(t *testing.T) {
	var templates = renderTemplates(testDialect)

	snap, tables := sql.RunSqlGenTests(
		t,
		testDialect,
		func(table string, delta bool) sql.Resource {
			return tableConfig{
				Table:  table,
				Schema: "a-schema",
				Delta:  delta,
			}
		},
		sql.TestTemplates{
			TableTemplates: []*template.Template{
				templates.createTargetTable,
			},
			TplAddColumns:    templates.alterTableColumns,
			TplDropNotNulls:  templates.alterTableColumns,
			TplCombinedAlter: templates.alterTableColumns,
		},
	)

	{
		tpl := templates.mergeInto
		tbl := tables[0]
		require.False(t, tbl.DeltaUpdates)
		var testcase = tbl.Identifier + " " + tpl.Name()

		bounds := []sql.MergeBound{
			{
				Identifier:   tbl.Keys[0].Identifier,
				LiteralLower: testDialect.Literal(int64(10)),
				LiteralUpper: testDialect.Literal(int64(100)),
			},
			{
				Identifier: tbl.Keys[1].Identifier,
				// No bounds - as would be the case for a boolean key, which
				// would be a very weird key, but technically allowed.
			},
			{
				Identifier:   tbl.Keys[2].Identifier,
				LiteralLower: testDialect.Literal("aGVsbG8K"),
				LiteralUpper: testDialect.Literal("Z29vZGJ5ZQo="),
			},
		}

		tf := mergeQueryInput{
			Table:  tbl,
			File:   "test-file",
			Bounds: bounds,
		}

		snap.WriteString("--- Begin " + testcase + " ---")
		require.NoError(t, tpl.Execute(snap, &tf))
		snap.WriteString("--- End " + testcase + " ---\n\n")
	}

	for _, tpl := range []*template.Template{
		templates.loadQuery,
		templates.copyInto,
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
		templates.copyInto,
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

	cupaloy.SnapshotT(t, snap.String())
}
