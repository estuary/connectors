package connector

import (
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/go/common"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
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

	{
		tbl := tables[0]
		var staging = "delta.`test-staging-path/txn-1_delta`"
		var staged = &stagingCopy{Target: &tbl, Identifier: staging, Directory: "test-staging-path/txn-1"}
		for _, tc := range []struct {
			name string
			tpl  *template.Template
			data any
		}{
			{"createStagingTable", testTemplates.createStagingTable, staged},
			{"copyIntoStaging", testTemplates.copyIntoStaging, staged},
			{"mergeInto staging tables and directory", testTemplates.mergeInto, &tableWithFiles{
				Table:       &tbl,
				Tables:      []string{staging, "delta.`test-staging-path/txn-2_delta`"},
				Directories: []string{"test-staging-path/txn-3"},
				Schema:      stagedSchemaDDL(tbl.Columns(), true),
			}},
		} {
			var rendered, err = renderTemplate(tc.tpl, tc.data)
			require.NoError(t, err)
			snap.WriteString("--- Begin " + tbl.Identifier + " " + tc.name + " ---")
			snap.WriteString(rendered)
			snap.WriteString("--- End " + tbl.Identifier + " ---\n\n")
		}
	}

	cupaloy.SnapshotT(t, snap.String())
}

// TestDatetimeKeyBounds resolves a table through the real dialect, so the
// key's column type is what a task gets, and renders the load query's bounds
// for a date-time key given in several RFC3339 forms.
func TestDatetimeKeyBounds(t *testing.T) {
	var projection = func(field string, inference pf.Inference) sql.Projection {
		return sql.Projection{Projection: pf.Projection{Field: field, Ptr: "/" + field, Inference: inference}}
	}
	var shape = sql.TableShape{
		Path: sql.TablePath{"schema", "events"},
		Keys: []sql.Projection{
			projection("created_at", pf.Inference{Types: []string{"string"}, String_: &pf.Inference_String{Format: "date-time"}, Exists: pf.Inference_MUST}),
			projection("id", pf.Inference{Types: []string{"string"}, Exists: pf.Inference_MUST}),
		},
		Values: []sql.Projection{projection("val", pf.Inference{Types: []string{"integer"}})},
		Document: func() *sql.Projection {
			p := projection("flow_document", pf.Inference{Types: []string{"object"}})
			return &p
		}(),
	}
	// Each key is a valid RFC3339 form; text order and instant order disagree.
	var keys = []tuple.Tuple{
		{"2025-05-05T02:00:00+02:00", "b"},
		{"2025-05-05T00:00:01.123456789Z", "c"},
		{"2025-05-04T22:59:59-01:00", "a"},
		{"2025-05-05T00:00:00.000Z", "d"},
	}

	for _, tc := range []struct {
		name          string
		flags         map[string]bool
		wantDDL       string
		wantBoundKeys bool
	}{
		{"TIMESTAMP key gets bounds", map[string]bool{"datetime_keys_as_string": false}, "TIMESTAMP", true},
		{"STRING key gets no bounds", map[string]bool{"datetime_keys_as_string": true}, "STRING", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var dialect = createDatabricksDialect(tc.flags)
			table, err := sql.ResolveTable(shape, dialect)
			require.NoError(t, err)
			require.Equal(t, tc.wantDDL, table.Keys[0].BareDDL)

			var builder = sql.NewMergeBoundsBuilder(table.Keys, dialect.Literal, sql.WithDatetimeBounds(isTimestampColumn))
			for _, k := range keys {
				converted, err := table.ConvertKey(k)
				require.NoError(t, err)
				builder.NextKey(converted)
			}
			var bounds = builder.Build()

			query, err := RenderTableWithStaged(table, []string{"dir"}, nil, stagedSchemaDDL(table.KeyPtrs(), false), renderTemplates(dialect).loadQuery, bounds)
			require.NoError(t, err)
			require.Contains(t, query, "schema.events.created_at = r.created_at")
			require.Contains(t, query, "schema.events.id = r.id AND schema.events.id >= 'a' AND schema.events.id <= 'd'")
			if tc.wantBoundKeys {
				require.Contains(t, query, "schema.events.created_at >= '2025-05-04T22:59:59-01:00' AND schema.events.created_at <= '2025-05-05T00:00:01.123456789Z'")
			} else {
				require.NotContains(t, query, "created_at >=")
			}
		})
	}
}
