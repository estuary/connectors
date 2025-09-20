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
		func(table string) []string {
			return []string{"a-schema", table}
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
		tplLoadQueryNoFlowDocument,
		tplMergeInto,
	} {
		tbl := tables[0]
		require.False(t, tbl.DeltaUpdates)
		var testcase = tbl.Identifier + " " + tpl.Name()

		bounds := []sql.MergeBound{
			{
				Column:       tbl.Keys[0],
				LiteralLower: databricksDialect.Literal(int64(10)),
				LiteralUpper: databricksDialect.Literal(int64(100)),
			},
			{
				Column: tbl.Keys[1],
				// No bounds - as would be the case for a boolean key, which
				// would be a very weird key, but technically allowed.
			},
			{
				Column:       tbl.Keys[2],
				LiteralLower: databricksDialect.Literal("aGVsbG8K"),
				LiteralUpper: databricksDialect.Literal("Z29vZGJ5ZQo="),
			},
		}

		tf := tableWithFiles{
			Table:       &tbl,
			StagingPath: "test-staging-path",
			Files:       []string{"file1", "file2"},
			Bounds:      bounds,
		}

		snap.WriteString("--- Begin " + testcase + " ---")
		require.NoError(t, tpl.Execute(snap, &tf))
		snap.WriteString("--- End " + testcase + " ---\n\n")
	}

	for _, tpl := range []*template.Template{
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
