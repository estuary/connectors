package main

import (
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
)

var testDialect = clickHouseDialect("test_db")
var testTemplates = renderTemplates(testDialect)

func TestSQLGeneration(t *testing.T) {
	// Load/store templates require a RangeKey field that RunSqlGenTests does not
	// provide, so only the target template goes through the standard test harness.
	// The load/store templates are rendered separately with renderTableAndRangeKey.
	rangeKeyTemplates := []*template.Template{
		testTemplates.createLoadTable,
		testTemplates.queryLoadTable,
		testTemplates.insertLoadTable,
		testTemplates.dropLoadTable,
		testTemplates.createStoreTable,
		testTemplates.insertStoreTable,
		testTemplates.queryStoreParts,
		testTemplates.moveStorePartition,
		testTemplates.existsStoreTable,
		testTemplates.dropStoreTable,
	}

	snap, tables := sql.RunSqlGenTests(
		t,
		testDialect,
		func(table string) []string {
			return []string{table}
		},
		sql.TestTemplates{
			TableTemplates: []*template.Template{
				testTemplates.createTargetTable,
			},
			TplAddColumns:    testTemplates.alterTargetColumns,
			TplDropNotNulls:  testTemplates.alterTargetColumns,
			TplCombinedAlter: testTemplates.alterTargetColumns,
		},
	)

	for _, tpl := range rangeKeyTemplates {
		for _, tbl := range tables {
			var testcase = tbl.Identifier + " " + tpl.Name()
			snap.WriteString("--- Begin " + testcase + " ---\n")
			rendered, err := renderTableAndRangeKey(tbl, 0, tpl)
			if err != nil {
				t.Fatalf("rendering %s: %v", testcase, err)
			}
			snap.WriteString(rendered)
			snap.WriteString("--- End " + testcase + " ---\n\n")
		}
	}

	cupaloy.SnapshotT(t, snap.String())
}
