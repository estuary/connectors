package connector

import (
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"
	"text/template"
	"time"

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
		})
	}
}

// TestStringKeyRanges renders the load query's bound of a time-ordered string
// id whose alphabet runs 0-9, a-z, A-Z, as Klaviyo's do, for a round holding
// both recent ids just past a "7z" to "7A" step and ids from a week earlier.
func TestStringKeyRanges(t *testing.T) {
	var shape = sql.TableShape{
		Path:     sql.TablePath{"project", "dataset", "events"},
		Keys:     []sql.Projection{{Projection: pf.Projection{Field: "id", Ptr: "/id", Inference: pf.Inference{Types: []string{"string"}, Exists: pf.Inference_MUST}}}},
		Document: &sql.Projection{Projection: pf.Projection{Field: "flow_document", Ptr: "", Inference: pf.Inference{Types: []string{"object"}}}},
	}
	table, err := sql.ResolveTable(shape, testDialect)
	require.NoError(t, err)
	require.Equal(t, "STRING", table.Keys[0].BareDDL)

	// An id's leading 8 digits count time, with its second digit stepping
	// every 2.5 days, followed by 18 random digits.
	const alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	var rng = rand.New(rand.NewSource(1))
	var step = 2.5 * 24 * time.Hour
	var at = time.Duration(7*62+36) * step // The "7A" step.
	var id = func(ts time.Duration) string {
		var b []byte
		var v = float64(ts) / float64(step) * 62 * 62 * 62 * 62 * 62 * 62
		for i := 0; i < 8; i++ {
			var w = 1.0
			for j := i; j < 7; j++ {
				w *= 62
			}
			var d = int(v / w)
			b = append(b, alphabet[d%62])
		}
		for len(b) < 26 {
			b = append(b, alphabet[rng.Intn(62)])
		}
		return string(b)
	}

	var b = newMergeBoundsBuilder(table, testDialect.Literal)
	var recent, weekOld []string
	for i := 0; i < 500; i++ {
		recent = append(recent, id(at+time.Duration(rng.Int63n(int64(time.Hour)))))
	}
	for i := 0; i < 200; i++ {
		weekOld = append(weekOld, id(at-7*24*time.Hour+time.Duration(rng.Int63n(int64(time.Hour)))))
	}
	for _, k := range append(append([]string{}, recent...), weekOld...) {
		b.NextKey([]any{k})
	}
	require.True(t, strings.HasPrefix(slices.Min(recent), "7A") && strings.HasPrefix(slices.Min(weekOld), "7x"))

	// No range spans the step, and every id lies within one.
	var bounds = b.Build()
	var ranges = bounds[0].Ranges
	require.NotEmpty(t, ranges)
	for _, r := range ranges {
		require.Equal(t, r.LiteralLower[:3], r.LiteralUpper[:3], "range %v spans the step", r)
	}
	for _, k := range append(recent, weekOld...) {
		require.True(t, slices.ContainsFunc(ranges, func(r sql.MergeRange) bool {
			return testDialect.Literal(k) >= r.LiteralLower && testDialect.Literal(k) <= r.LiteralUpper
		}), "%s is outside every range", k)
	}

	query, err := renderQueryTemplate(table, testTemplates.loadQuery, bounds, false)
	require.NoError(t, err)
	require.Contains(t, query, fmt.Sprintf("l.id = r.c0 AND ((l.id >= '%s' AND l.id <= '%s') OR ", slices.Min(recent), slices.Max(recent)))
}
