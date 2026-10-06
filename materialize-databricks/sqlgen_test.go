package connector

import (
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/go/common"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

var testDialect = createDatabricksDialect(common.ResolveFlagDefaults(featureFlagDefaults, common.CreatedAt{}))
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

		var schema = stagedSchemaDDL(tbl.Columns(), true)
		if tpl != testTemplates.mergeInto {
			schema = stagedSchemaDDL(tbl.KeyPtrs(), false)
		}
		for _, tc := range []struct {
			name        string
			dirs, files []string
		}{
			{"directory", []string{"test-staging-path/txn-1"}, nil},
			{"directories", []string{"test-staging-path/txn-1", "test-staging-path/txn-2"}, nil},
			{"root files", nil, []string{"test-staging-path/file1.json.gz", "test-staging-path/file2.json.gz"}},
			{"directory and root files", []string{"test-staging-path/txn-1"}, []string{"test-staging-path/file1.json.gz"}},
		} {
			var rendered, err = RenderTableWithStaged(tbl, tc.dirs, tc.files, schema, tpl, bounds)
			require.NoError(t, err)
			snap.WriteString("--- Begin " + testcase + " " + tc.name + " ---")
			snap.WriteString(rendered)
			snap.WriteString("--- End " + testcase + " ---\n\n")
		}
	}

	for _, tpl := range []*template.Template{
		testTemplates.copyIntoDirect,
	} {
		tbl := tables[0]
		require.False(t, tbl.DeltaUpdates)

		var testcase = tbl.Identifier + " " + tpl.Name()

		for _, tc := range []struct {
			name  string
			files []string
			path  string
		}{
			{"directory", nil, "test-staging-path/txn-1"},
			{"root files", []string{"file1.json.gz", "file2.json.gz"}, "test-staging-path"},
		} {
			var rendered, err = RenderTableWithFiles(tbl, tc.files, tc.path, tpl, nil)
			require.NoError(t, err)
			snap.WriteString("--- Begin " + testcase + " " + tc.name + " ---")
			snap.WriteString(rendered)
			snap.WriteString("--- End " + testcase + " ---\n\n")
		}
	}

	for _, tpl := range []*template.Template{
		testTemplates.copyIntoDirect,
	} {
		tbl := tables[1]
		require.True(t, tbl.DeltaUpdates)
		require.Nil(t, tbl.Document)

		var testcase = tbl.Identifier + " " + tpl.Name()

		for _, tc := range []struct {
			name  string
			files []string
			path  string
		}{
			{"directory", nil, "test-staging-path/txn-1"},
			{"root files", []string{"file1.json.gz", "file2.json.gz"}, "test-staging-path"},
		} {
			var rendered, err = RenderTableWithFiles(tbl, tc.files, tc.path, tpl, nil)
			require.NoError(t, err)
			snap.WriteString("--- Begin " + testcase + " " + tc.name + " ---")
			snap.WriteString(rendered)
			snap.WriteString("--- End " + testcase + " ---\n\n")
		}
	}

	cupaloy.SnapshotT(t, snap.String())
}
