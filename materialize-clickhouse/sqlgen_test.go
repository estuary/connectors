package main

import (
	"fmt"
	"strings"
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
)

var testDialect = clickHouseDialect("test_db")

func TestSQLGeneration(t *testing.T) {
	var testTemplates = renderTemplates(testDialect, true)

	// Load/store templates require a RangeKey field that RunSqlGenTests does not
	// provide, so only the target template goes through the standard test harness.
	// The load/store templates are rendered separately with renderTableAndRangeKey.
	rangeKeyTemplates := []*template.Template{
		testTemplates.createLoadTable,
		testTemplates.queryLoadTable,
		testTemplates.queryLoadTableNoFlowDocument,
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

func TestSQLGenerationNoHardDelete(t *testing.T) {
	var noDeleteTemplates = renderTemplates(testDialect, false)

	_, tables := sql.RunSqlGenTests(
		t,
		testDialect,
		func(table string) []string {
			return []string{table}
		},
		sql.TestTemplates{
			TableTemplates: []*template.Template{
				noDeleteTemplates.createTargetTable,
			},
			TplAddColumns:    noDeleteTemplates.alterTargetColumns,
			TplDropNotNulls:  noDeleteTemplates.alterTargetColumns,
			TplCombinedAlter: noDeleteTemplates.alterTargetColumns,
		},
	)

	var snap strings.Builder
	for _, tbl := range tables {
		snap.WriteString("--- Begin " + tbl.Identifier + " createTargetTable ---\n")
		rendered, err := sql.RenderTableTemplate(tbl, noDeleteTemplates.createTargetTable)
		if err != nil {
			t.Fatalf("rendering createTargetTable: %v", err)
		}
		snap.WriteString(rendered)
		snap.WriteString("--- End " + tbl.Identifier + " createTargetTable ---\n\n")

		snap.WriteString("--- Begin " + tbl.Identifier + " insertStoreTable ---\n")
		rendered, err = renderTableAndRangeKey(tbl, 0, noDeleteTemplates.insertStoreTable)
		if err != nil {
			t.Fatalf("rendering insertStoreTable: %v", err)
		}
		snap.WriteString(rendered)
		snap.WriteString("--- End " + tbl.Identifier + " insertStoreTable ---\n\n")
	}

	cupaloy.SnapshotT(t, snap.String())
}

func TestSQLGenerationQuotedTableNames(t *testing.T) {
	var testTemplates = renderTemplates(testDialect, true)

	rangeKeyTemplates := []*template.Template{
		testTemplates.createLoadTable,
		testTemplates.queryLoadTable,
		testTemplates.queryLoadTableNoFlowDocument,
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
			return []string{fmt.Sprintf("%s-@你好-`-\"especiál", table)}
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
