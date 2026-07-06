package main

import (
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

var testDialect = bqDialect(featureFlagDefaults, false)
var testTemplates = renderTemplates(testDialect, false)

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

	cupaloy.SnapshotT(t, snap.String())
}

// TestSQLGenerationEmulatorGoccy snapshots the goccy-emulator dialect and
// template variants: INT64 integer columns, STRING columns for
// object/array/MULTIPLE fields, and one ALTER command per statement. The
// production TestSQLGeneration snapshot above must remain byte-identical to
// main; only this emulator snapshot may reflect goccy workarounds.
func TestSQLGenerationEmulatorGoccy(t *testing.T) {
	var emulatorDialect = bqDialect(featureFlagDefaults, true)
	var emulatorTemplates = renderTemplates(emulatorDialect, true)

	snap, tables := sql.RunSqlGenTests(
		t,
		emulatorDialect,
		func(table string) []string {
			return []string{"projectID", "dataset", table}
		},
		sql.TestTemplates{
			TableTemplates: []*template.Template{
				emulatorTemplates.createTargetTable,
				emulatorTemplates.storeInsert,
			},
			TplAddColumns:    emulatorTemplates.alterTableColumns,
			TplDropNotNulls:  emulatorTemplates.alterTableColumns,
			TplCombinedAlter: emulatorTemplates.alterTableColumns,
			TplInstallFence:  emulatorTemplates.installFence,
			TplUpdateFence:   emulatorTemplates.updateFence,
		},
	)

	for _, tpl := range []*template.Template{
		emulatorTemplates.storeUpdate,
		emulatorTemplates.loadQuery,
		emulatorTemplates.loadQueryNoFlowDocument,
	} {
		tbl := tables[0]
		require.False(t, tbl.DeltaUpdates)
		var testcase = tbl.Identifier + " " + tpl.Name()

		bounds := []sql.MergeBound{
			{
				Column:       tbl.Keys[0],
				LiteralLower: emulatorDialect.Literal(int64(10)),
				LiteralUpper: emulatorDialect.Literal(int64(100)),
			},
			{
				Column: tbl.Keys[1],
			},
			{
				Column:       tbl.Keys[2],
				LiteralLower: emulatorDialect.Literal("aGVsbG8K"),
				LiteralUpper: emulatorDialect.Literal("Z29vZGJ5ZQo="),
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
