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
		databricksDialect,
		func(table string, delta bool) sql.Resource {
			return tableConfig{
				Table:  table,
				Schema: "a-schema",
				Delta:  delta,
			}
		},
		sql.TestTemplates{
			TableTemplates: []*template.Template{
				tplCreateTargetTable,
			},
			TplAddColumns: tplAlterTableColumns,
		},
	)

	for _, tpl := range []*template.Template{
		tplLoadQuery,
		tplMergeInto,
		tplCopyIntoDirect,
	} {
		tbl := tables[0]
		require.False(t, tbl.DeltaUpdates)

		var testcase = tbl.Identifier + " " + tpl.Name()

		var tplData = tableWithFiles{Table: &tbl, StagingPath: "test-staging-path", Files: []string{"file1", "file2"}}
		snap.WriteString("--- Begin " + testcase + " ---")
		require.NoError(t, tpl.Execute(snap, &tplData))
		snap.WriteString("--- End " + testcase + " ---\n\n")
	}

	for _, tpl := range []*template.Template{
		tplCopyIntoDirect,
	} {
		tbl := tables[1]
		require.True(t, tbl.DeltaUpdates)
		require.Nil(t, tbl.Document)

		var testcase = tbl.Identifier + " " + tpl.Name()

		var tplData = tableWithFiles{Table: &tbl, StagingPath: "test-staging-path", Files: []string{"file1", "file2"}}
		snap.WriteString("--- Begin " + testcase + " ---")
		require.NoError(t, tpl.Execute(snap, &tplData))
		snap.WriteString("--- End " + testcase + " ---\n\n")
	}

	cupaloy.SnapshotT(t, snap.String())
}
