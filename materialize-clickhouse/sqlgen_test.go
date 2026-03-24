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
	snap, _ := sql.RunSqlGenTests(
		t,
		testDialect,
		func(table string) []string {
			return []string{table}
		},
		sql.TestTemplates{
			TableTemplates: []*template.Template{
				testTemplates.targetCreateTable,
				testTemplates.loadCreateTable,
				testTemplates.loadQuery,
				testTemplates.loadInsert,
				testTemplates.loadTruncateTable,
				testTemplates.storeInsert,
			},
			TplAddColumns:    testTemplates.targetAlterColumns,
			TplDropNotNulls:  testTemplates.targetAlterColumns,
			TplCombinedAlter: testTemplates.targetAlterColumns,
		},
	)

	cupaloy.SnapshotT(t, snap.String())
}
