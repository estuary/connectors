package main

import (
	"fmt"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

var testDialect = clickHouseDialect("test_db")

func TestSQLGeneration(t *testing.T) {
	var testTemplates = renderTemplates(testDialect, true)

	// Load/store templates require a RangeKey field that RunSqlGenTests does not
	// provide, so only the target template goes through the standard test harness.
	// The load/store templates are rendered separately with renderTableAndRangeKey.
	rangeKeyTemplates := []*template.Template{
		testTemplates.createLoadTable,
		testTemplates.truncateLoadTable,
		testTemplates.countLoadKeys,
		testTemplates.queryLoadTable,
		testTemplates.queryLoadTableNoFlowDocument,
		testTemplates.insertLoadTable,
		testTemplates.dropLoadTable,
		testTemplates.createStoreTable,
		testTemplates.truncateStoreTable,
		testTemplates.insertStoreTable,
		testTemplates.queryStoreParts,
		testTemplates.moveStorePartition,
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
		testTemplates.truncateLoadTable,
		testTemplates.countLoadKeys,
		testTemplates.queryLoadTable,
		testTemplates.queryLoadTableNoFlowDocument,
		testTemplates.insertLoadTable,
		testTemplates.dropLoadTable,
		testTemplates.createStoreTable,
		testTemplates.truncateStoreTable,
		testTemplates.insertStoreTable,
		testTemplates.queryStoreParts,
		testTemplates.moveStorePartition,
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

func TestClickHouseClampDatetime(t *testing.T) {
	for _, tt := range []struct {
		input   string
		want    time.Time
		wantErr bool
	}{
		{
			input: "2023-08-29T16:17:18Z",
			want:  time.Date(2023, 8, 29, 16, 17, 18, 0, time.UTC),
		},
		{
			input: "2025-11-29 01:05:28+00:00",
			want:  time.Date(2025, 11, 29, 1, 5, 28, 0, time.UTC),
		},
		{
			input: "2026-06-23 00:15:23.918411+00:00",
			want:  time.Date(2026, 6, 23, 0, 15, 23, 918411000, time.UTC),
		},
		{
			input: "2025-11-29 01:05:28",
			want:  time.Date(2025, 11, 29, 1, 5, 28, 0, time.UTC),
		},
		{
			input: "1900-01-01 00:00:00Z",
			want:  time.Date(1925, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			input:   "not a timestamp",
			wantErr: true,
		},
	} {
		t.Run(tt.input, func(t *testing.T) {
			got, err := clickHouseClampDatetime(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.True(t, tt.want.Equal(got.(time.Time)))
		})
	}
}

func TestClickHouseClampDate(t *testing.T) {
	for _, tt := range []struct {
		input   string
		want    time.Time
		wantErr bool
	}{
		{
			input: "2023-08-29",
			want:  time.Date(2023, 8, 29, 0, 0, 0, 0, time.UTC),
		},
		{
			input: "1899-12-31",
			want:  time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			input:   "not a date",
			wantErr: true,
		},
		{
			input:   "2025-11-29 01:05:28+00:00",
			wantErr: true,
		},
	} {
		t.Run(tt.input, func(t *testing.T) {
			got, err := clickHouseClampDate(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.True(t, tt.want.Equal(got.(time.Time)))
		})
	}
}
