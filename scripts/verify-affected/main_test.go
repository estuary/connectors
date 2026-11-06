package main

import (
	"testing"

	"github.com/bradleyjkemp/cupaloy"
)

// TestRenderQuery pins the fixed query skeleton. The whole point of this tool is
// that the join/scope logic never drifts: any change to the reads_from join, the
// spec_type filters, or the parameter binding must be reviewed as a snapshot
// diff.
func TestRenderQuery(t *testing.T) {
	cases := []struct {
		name      string
		predicate string
		column    string
	}{
		{"uses-enum-built", `{{COL}}::text LIKE '%"enum":%'`, "built_spec"},
		{"uses-enum-spec", `{{COL}}::text LIKE '%"enum":%'`, "spec"},
		{"adhoc-explicit-column", `c.spec->'schema' ? 'enum'`, "built_spec"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cupaloy.SnapshotT(t, renderQuery(tc.predicate, tc.column))
		})
	}
}

// TestReportRender pins the audit-report format using a fixed fixture so format
// changes are reviewed deliberately.
func TestReportRender(t *testing.T) {
	r := reportData{
		Timestamp:  "2026-06-02T00:00:00Z",
		Connector:  "materialize-postgres",
		Images:     []string{"ghcr.io/estuary/materialize-postgres"},
		AIModel:    "claude-opus-4-8",
		Overridden: false,
		Status:     "APPROVED",
		Approver:   "Mahdi Dibaiee",
		Check: resolvedCheck{
			Name:        "uses-enum",
			Description: "Collections whose schema constrains a field with an enum.",
			Predicate:   `{{COL}}::text LIKE '%"enum":%'`,
			Column:      "built_spec",
		},
		SQL: renderQuery(`{{COL}}::text LIKE '%"enum":%'`, "built_spec"),
		AI:  aiVerdict{Verdict: "PASS", Notes: []string{"Join uses reads_from; scope looks correct."}, Raw: "PASS"},
		Summary: summarize([]affectedRow{
			{Task: "acmeCo/prod/to-pg", Image: "ghcr.io/estuary/materialize-postgres", Collection: "acmeCo/orders"},
			{Task: "acmeCo/prod/to-pg", Image: "ghcr.io/estuary/materialize-postgres", Collection: "acmeCo/users"},
			{Task: "beta/sink", Image: "ghcr.io/estuary/materialize-postgres", Collection: "beta/events"},
		}),
	}
	cupaloy.SnapshotT(t, renderReport(r, 0))
}

// TestReportRenderTruncated pins the terminal-sampled rendering: the table is
// capped and a "showing N of M" note is appended, while the file rendering
// (rowLimit 0) stays complete.
func TestReportRenderTruncated(t *testing.T) {
	var rows []affectedRow
	for _, n := range []string{"a", "b", "c", "d", "e"} {
		rows = append(rows, affectedRow{
			Task:       "tenant/" + n + "/to-pg",
			Image:      "ghcr.io/estuary/materialize-postgres",
			Collection: "tenant/" + n,
		})
	}
	r := reportData{
		Timestamp: "2026-06-02T00:00:00Z",
		Connector: "materialize-postgres",
		Images:    []string{"ghcr.io/estuary/materialize-postgres"},
		AIModel:   "claude-opus-4-8",
		Status:    "APPROVED",
		Approver:  "Mahdi Dibaiee",
		Check:     resolvedCheck{Name: "uses-enum", Description: "enum collections", Predicate: `{{COL}}::text LIKE '%"enum":%'`, Column: "built_spec"},
		SQL:       renderQuery(`{{COL}}::text LIKE '%"enum":%'`, "built_spec"),
		AI:        aiVerdict{Verdict: "PASS", Raw: "PASS"},
		Summary:   summarize(rows),
	}
	cupaloy.SnapshotT(t, renderReport(r, 2))
}
