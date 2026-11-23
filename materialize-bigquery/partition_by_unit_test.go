package connector

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestPartitionExpr(t *testing.T) {
	for _, tt := range []struct {
		name    string
		raw     string
		want    string
		wantErr bool
	}{
		{name: "nil config", raw: "", want: ""},
		{name: "empty object", raw: "{}", want: ""},
		{name: "absent field", raw: `{"table":"t"}`, want: ""},
		{name: "whitespace only", raw: `{"table":"t","partition_by":"  \t"}`, want: ""},
		{name: "expression", raw: `{"table":"t","partition_by":"DATE(created_at)"}`, want: "DATE(created_at)"},
		{name: "trimmed", raw: `{"table":"t","partition_by":"  DATE(created_at)\t"}`, want: "DATE(created_at)"},
		{name: "invalid json", raw: `{`, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := partitionExpr(json.RawMessage(tt.raw))
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestValidatePartitionByGrammar(t *testing.T) {
	for _, expr := range []string{
		"created_at",
		"`created-at`",
		"_PARTITIONDATE",
		"DATE(_PARTITIONTIME)",
		"DATE(created_at)",
		"date ( `order` )",
		"DATE_TRUNC(day, MONTH)",
		"DATETIME_TRUNC(created_at, DAY)",
		"TIMESTAMP_TRUNC(updated_at, MONTH)",
		"timestamp_trunc(_PARTITIONTIME,\n\thour)",
		"RANGE_BUCKET(id, GENERATE_ARRAY(0, 1000))",
		"RANGE_BUCKET(id, GENERATE_ARRAY(-100, 1000, 10))",
	} {
		require.NoError(t, validatePartitionBy(expr), expr)
	}

	for _, expr := range []string{
		"",
		"DATE(created_at);",
		"DATE(created_at); SELECT 1",
		"DATE(created_at) -- note",
		"DATE(created_at) # note",
		"DATE(created_at) /* note */",
		"DATE(created_at) OPTIONS(description='x')",
		"DATE(created_at) CLUSTER BY id",
		"DATE(created_at) AS SELECT 1",
		"DATE(created_at), id",
		"(DATE(created_at))",
		"DATE(created_at))",
		"DATE((SELECT 1))",
		"DATE(created_at, 'UTC')",
		"DATE(t.created_at)",
		"DATE(`created at`)",
		"DATE(`created_at\\``)",
		"DATE(`created_at",
		"TIMESTAMP_TRUNC(updated_at, WEEK)",
		"TIMESTAMP_TRUNC(updated_at, `MONTH`)",
		"CURRENT_DATE()",
		"RANGE_BUCKET(id, GENERATE_ARRAY(0, 10 * 100, 10))",
		"RANGE_BUCKET(id, GENERATE_ARRAY(0, (SELECT 1000), 10))",
		"RANGE_BUCKET(id, [0, 10, 20])",
		"DATE(created_at)\nDATE(created_at)",
	} {
		require.Error(t, validatePartitionBy(expr), expr)
	}
}

// A partition_by that is not a partition expression never renders into DDL.
func TestSQLGenerationPartitionByRejected(t *testing.T) {
	_, tables := sql.RunSqlGenTests(t, testDialect, func(table string) []string {
		return []string{"projectID", "dataset", table}
	}, sql.TestTemplates{})

	var table = tables[0]
	table.ResourceConfigJson = json.RawMessage(`{"table":"target_table","partition_by":"DATE(flow_published_at); SELECT 1"}`)
	rendered, err := sql.RenderTableTemplate(table, testTemplates.createTargetTable)
	require.ErrorContains(t, err, "partition_by")
	require.Empty(t, rendered)
}

func TestMustRecreateResourcePartitionBy(t *testing.T) {
	var c = &client{}
	var binding = func(raw string) *pf.MaterializationSpec_Binding {
		return &pf.MaterializationSpec_Binding{ResourceConfigJson: json.RawMessage(raw)}
	}

	for _, tt := range []struct {
		name       string
		last, next *pf.MaterializationSpec_Binding
		want       bool
	}{
		{name: "nil last", last: nil, next: binding(`{"table":"t"}`), want: false},
		{name: "nil next", last: binding(`{"table":"t"}`), next: nil, want: false},
		{name: "both absent", last: binding(`{"table":"t"}`), next: binding(`{"table":"t"}`), want: false},
		{name: "absent to empty", last: binding(`{"table":"t"}`), next: binding(`{"table":"t","partition_by":""}`), want: false},
		{name: "whitespace only difference", last: binding(`{"table":"t","partition_by":"DATE(ts)"}`), next: binding(`{"table":"t","partition_by":" DATE(ts) "}`), want: false},
		{name: "absent to expression", last: binding(`{"table":"t"}`), next: binding(`{"table":"t","partition_by":"DATE(ts)"}`), want: true},
		{name: "changed expression", last: binding(`{"table":"t","partition_by":"DATE(ts)"}`), next: binding(`{"table":"t","partition_by":"TIMESTAMP_TRUNC(ts, MONTH)"}`), want: true},
		{name: "expression removed", last: binding(`{"table":"t","partition_by":"DATE(ts)"}`), next: binding(`{"table":"t"}`), want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.MustRecreateResource(nil, tt.last, tt.next)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestSQLGenerationPartitionBy(t *testing.T) {
	// The standard (index 0) and delta-updates (index 1) tables of the shared
	// SQL generation fixture, with a partition_by attached to their resource
	// config.
	_, tables := sql.RunSqlGenTests(t, testDialect, func(table string) []string {
		return []string{"projectID", "dataset", table}
	}, sql.TestTemplates{})

	var snap strings.Builder
	for _, tc := range []struct {
		name        string
		partitionBy string
		table       sql.Table
	}{
		{name: "standard date partition", partitionBy: "DATE(flow_published_at)", table: tables[0]},
		{name: "delta month partition", partitionBy: "TIMESTAMP_TRUNC(flow_published_at, MONTH)", table: tables[1]},
		{name: "ingestion time", partitionBy: "_PARTITIONDATE", table: tables[0]},
		{name: "whitespace only renders no clause", partitionBy: "   ", table: tables[0]},
	} {
		var table = tc.table
		table.ResourceConfigJson = json.RawMessage(`{"table":"target_table","partition_by":"` + tc.partitionBy + `"}`)
		rendered, err := sql.RenderTableTemplate(table, testTemplates.createTargetTable)
		require.NoError(t, err)
		snap.WriteString("--- Begin " + tc.name + " ---\n")
		snap.WriteString(rendered)
		snap.WriteString("--- End " + tc.name + " ---\n\n")
	}
	cupaloy.SnapshotT(t, snap.String())
}
