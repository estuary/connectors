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

var testDialect = bqDialect(common.ResolveFlagDefaults(featureFlagDefaults, common.CreatedAt{}))
var testTemplates = renderTemplates(testDialect)

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

	{
		tbl := tables[0]
		var bounds = []sql.MergeBound{
			{
				Column:       tbl.Keys[0],
				LiteralLower: testDialect.Literal(int64(10)),
				LiteralUpper: testDialect.Literal(int64(100)),
				Ranges: []sql.MergeRange{
					{LiteralLower: testDialect.Literal(int64(10)), LiteralUpper: testDialect.Literal(int64(10))},
					{LiteralLower: testDialect.Literal(int64(90)), LiteralUpper: testDialect.Literal(int64(100))},
				},
			},
			{Column: tbl.Keys[1]},
			{
				Column:       tbl.Keys[2],
				LiteralLower: testDialect.Literal("aGVsbG8K"),
				LiteralUpper: testDialect.Literal("Z29vZGJ5ZQo="),
			},
		}
		for _, tpl := range []*template.Template{
			testTemplates.storeUpdate,
			testTemplates.loadQuery,
			testTemplates.loadQueryNoFlowDocument,
		} {
			snap.WriteString("--- Begin " + tbl.Identifier + " " + tpl.Name() + " ranges ---\n")
			require.NoError(t, tpl.Execute(snap, &queryParams{Table: tbl, Bounds: bounds}))
			snap.WriteString("--- End " + tbl.Identifier + " ---\n\n")
		}
	}

	cupaloy.SnapshotT(t, snap.String())
}

// TestDatetimeKeyRanges renders the load query's bound of a date-time key
// whose values split into a band and an outlier, given with sub-microsecond
// precision and a non-UTC offset.
func TestDatetimeKeyRanges(t *testing.T) {
	var projection = func(field string, inference pf.Inference) sql.Projection {
		return sql.Projection{Projection: pf.Projection{Field: field, Ptr: "/" + field, Inference: inference}}
	}
	var shape = sql.TableShape{
		Path: sql.TablePath{"project", "dataset", "events"},
		Keys: []sql.Projection{
			projection("created_at", pf.Inference{Types: []string{"string"}, String_: &pf.Inference_String{Format: "date-time"}, Exists: pf.Inference_MUST}),
			projection("id", pf.Inference{Types: []string{"string"}, Exists: pf.Inference_MUST}),
		},
		Document: func() *sql.Projection {
			p := projection("flow_document", pf.Inference{Types: []string{"object"}})
			return &p
		}(),
	}
	var keys = []tuple.Tuple{
		{"2026-10-26T12:00:00.123456789Z", "a"},
		{"2026-03-01T02:00:00+02:00", "b"},
		{"2026-10-26T12:15:00.9999999Z", "c"},
		{"2026-10-26T12:05:00Z", "d"},
	}

	for _, tc := range []struct {
		name       string
		flags      map[string]bool
		wantBounds string
	}{
		{"TIMESTAMP key splits at the gap, floored to microseconds", map[string]bool{"datetime_keys_as_string": false},
			"l.created_at = r.c0 AND (" +
				"(l.created_at >= '2026-03-01T00:00:00.000000Z' AND l.created_at <= '2026-03-01T00:00:00.000000Z') OR " +
				"(l.created_at >= '2026-10-26T12:00:00.123456Z' AND l.created_at <= '2026-10-26T12:15:00.999999Z'))"},
		{"STRING key keeps one date range", map[string]bool{"datetime_keys_as_string": true},
			"l.created_at = r.c0 AND l.created_at >= '2026-02-28' AND l.created_at <= '2026-10-28'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var dialect = bqDialect(tc.flags)
			table, err := sql.ResolveTable(shape, dialect)
			require.NoError(t, err)

			var b = &binding{
				target:          table,
				loadMergeBounds: newMergeBoundsBuilder(table, dialect.Literal),
				timestampKeys:   timestampKeys(table),
			}
			for _, k := range keys {
				converted, err := table.ConvertKey(k)
				require.NoError(t, err)
				b.loadMergeBounds.NextKey(b.boundsKey(converted))
			}

			query, err := renderQueryTemplate(table, renderTemplates(dialect).loadQuery, b.loadMergeBounds.Build(), false)
			require.NoError(t, err)
			require.Contains(t, query, tc.wantBounds)
			require.Contains(t, query, "l.id = r.c1 AND l.id >= 'a' AND l.id <= 'd'")
		})
	}
}
