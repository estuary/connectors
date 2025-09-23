package main

import (
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

var testDialect = createDatabricksDialect(featureFlagDefaults)
var testTemplates = renderTemplates(testDialect)

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
			TplAddColumns: testTemplates.alterTableColumns,
		},
	)

	for _, tpl := range []*template.Template{
		testTemplates.loadQuery,
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
		testTemplates.copyIntoDirect,
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
		testTemplates.copyIntoDirect,
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
