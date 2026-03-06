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
				testTemplates.createTargetTable,
				testTemplates.loadQuery,
				testTemplates.storeInsert,
			},
			TplAddColumns:    testTemplates.alterTableColumns,
			TplDropNotNulls:  testTemplates.alterTableColumns,
			TplCombinedAlter: testTemplates.alterTableColumns,
		},
	)

	cupaloy.SnapshotT(t, snap.String())
}
