package connector

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
			return []string{"projectID", "dataset", table}
		},
		sql.TestTemplates{
			TableTemplates: []*template.Template{
				testTemplates.createTargetTable,
				testTemplates.storeInsert,
			},
			TplAddColumns:    testTemplates.alterTableColumns,
			TplDropNotNulls:  testTemplates.alterTableColumns,
			TplCombinedAlter: testTemplates.alterTableColumns,
			TplInstallFence:  testTemplates.installFence,
			TplUpdateFence:   testTemplates.updateFence,
		},
	)

	for _, tpl := range []*template.Template{
		testTemplates.storeUpdate,
		testTemplates.loadQuery,
		testTemplates.loadQueryNoFlowDocument,
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

		tf := queryParams{
			Table:  tbl,
			Bounds: bounds,
		}

		snap.WriteString("--- Begin " + testcase + " ---\n")
		require.NoError(t, tpl.Execute(snap, &tf))
		snap.WriteString("--- End " + testcase + " ---\n\n")
	}

	// Re-run the bound-aware templates with bounds that include observed null
	// keys, to cover the IsNull paths in the templates.
	for _, tpl := range []*template.Template{
		testTemplates.storeUpdate,
		testTemplates.loadQuery,
		testTemplates.loadQueryNoFlowDocument,
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
				// Boolean key: builder always emits an empty MergeBound for
				// booleans, regardless of observed values.
				Column: tbl.Keys[1],
			},
			{
				// Null observed but no literal bounds: emit a bare IS NULL.
				Column: tbl.Keys[2],
				IsNull: true,
			},
		}

		tf := queryParams{
			Table:  tbl,
			Bounds: bounds,
		}

		snap.WriteString("--- Begin " + testcase + " ---\n")
		require.NoError(t, tpl.Execute(snap, &tf))
		snap.WriteString("--- End " + testcase + " ---\n\n")
	}

	cupaloy.SnapshotT(t, snap.String())
}
