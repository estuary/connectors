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

	for _, tbl := range tables {
		for _, tpl := range []*template.Template{
			templates.loadQuery,
			templates.copyInto,
			templates.mergeInto,
		} {
			var testcase = tbl.Identifier + " " + tpl.Name()

			var tf = tableAndFile{
				Table: tbl,
				File:  "test-file",
			}

			snap.WriteString("--- Begin " + testcase + " ---")
			require.NoError(t, tpl.Execute(snap, &tf))
			snap.WriteString("--- End " + testcase + " ---\n\n")
		}
	}

	cupaloy.SnapshotT(t, snap.String())
}
