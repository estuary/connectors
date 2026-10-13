package connector

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
	var testTemplates = renderTemplates(testDialect, true, clusterDDL{})

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
	var noDeleteTemplates = renderTemplates(testDialect, false, clusterDDL{})

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
	var testTemplates = renderTemplates(testDialect, true, clusterDDL{})

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
			input: "1900-01-01 00:00:00Z",
			want:  time.Date(1925, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			input:   "not a timestamp",
			wantErr: true,
		},
		{
			input:   "2025-11-29 01:05:28",
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

// TestSQLGenerationDistributed pins the DDL rendered for a cluster: a
// Replicated local table, a Distributed proxy over it, and ON CLUSTER on
// every temp-table statement. The single-server snapshots above must not
// move when this one does.
func TestSQLGenerationDistributed(t *testing.T) {
	var cluster = clusterDDL{Cluster: "flow_cluster", Database: "test_db", LocalSuffix: "_local"}
	var tpls = renderTemplates(testDialect, true, cluster)

	var snap strings.Builder
	for _, tc := range []struct {
		name        string
		shardingKey []string
		delta       bool
	}{
		{"all keys", nil, false},
		{"subset of keys", []string{"id"}, false},
		{"delta updates", nil, true},
	} {
		var tbl = buildTestTable(t, testDialect, "target_table")
		tbl.DeltaUpdates = tc.delta
		tbl.ResourceConfigJson = mustMarshal(t, tableConfig{Table: "target_table", ShardingKey: tc.shardingKey, Delta: tc.delta})

		for _, tpl := range []*template.Template{
			tpls.createTargetTable,
			tpls.createLoadTable, tpls.truncateLoadTable, tpls.dropLoadTable,
			tpls.createStoreTable, tpls.truncateStoreTable, tpls.moveStorePartition, tpls.dropStoreTable,
		} {
			var testcase = tc.name + " " + tpl.Name()
			snap.WriteString("--- Begin " + testcase + " ---\n")
			// The transactor renders its temp-table templates against the
			// local table, as newTransactor re-addresses each binding.
			var in = tbl
			if tpl != tpls.createTargetTable {
				in = localTable(tbl, testDialect, cluster)
			}
			rendered, err := renderTableAndRangeKey(in, 0, tpl)
			require.NoError(t, err, testcase)
			snap.WriteString(rendered)
			snap.WriteString("--- End " + testcase + " ---\n\n")
		}
	}

	// A sharding key naming a non-key field is caught at render time.
	var bad = buildTestTable(t, testDialect, "target_table")
	bad.ResourceConfigJson = mustMarshal(t, tableConfig{Table: "target_table", ShardingKey: []string{"value"}})
	_, err := renderTableAndRangeKey(bad, 0, tpls.createTargetTable)
	require.ErrorContains(t, err, `sharding_key field "value" is not a key`)

	cupaloy.SnapshotT(t, snap.String())
}
