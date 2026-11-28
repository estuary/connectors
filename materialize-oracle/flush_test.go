package connector

import (
	"strings"
	"testing"
	"time"

	"github.com/estuary/connectors/go/common"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestFlushFeatureFlags(t *testing.T) {
	var boundary = time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		flags string
		want  map[int]time.Time
	}{
		{flags: "", want: map[int]time.Time{0: boundary}},
		{flags: "no_truncate_after_backfill", want: map[int]time.Time{}},
		{flags: "retain_existing_data_on_backfill", want: map[int]time.Time{}},
	} {
		t.Run(tc.flags, func(t *testing.T) {
			var flags = common.ResolveFlags(tc.flags, featureFlagDefaults, common.CreatedAt{})
			var d = &transactor{
				bindings:              []*binding{{target: sql.Table{}, truncateSQL: "DELETE"}},
				truncateAfterBackfill: sql.TruncateAfterBackfill(flags),
			}
			require.NoError(t, d.Flush(t.Context(), map[int]time.Time{0: boundary}))
			require.Equal(t, tc.want, d.truncations)
		})
	}
}

func TestAddTruncation(t *testing.T) {
	var column = func(fc sql.FieldConfig) *sql.Column {
		var p = sql.Projection{Projection: pf.Projection{Inference: pf.Inference{
			Types:   []string{"string"},
			String_: &pf.Inference_String{Format: "date-time"},
			Exists:  pf.Inference_MUST,
		}}}
		return &sql.Column{Identifier: "flow_published_at", MappedType: testDialect.MapType(&p, fc)}
	}

	var op = sql.Column{Identifier: `"_meta/op"`, Projection: sql.Projection{Projection: pf.Projection{Ptr: "/_meta/op"}}}
	var withOp = sql.Table{Identifier: "the_table", Values: []sql.Column{op}}

	var b = &binding{target: withOp}
	require.NoError(t, addTruncation(b, column(sql.FieldConfig{}), true))
	require.Equal(t, `DELETE FROM the_table WHERE flow_published_at < TO_TIMESTAMP_TZ(:1, 'YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM')`, b.truncateSQL)

	// Soft deletes mark the rows deleted instead.
	b = &binding{target: withOp}
	require.NoError(t, addTruncation(b, column(sql.FieldConfig{}), false))
	require.Equal(t, `UPDATE the_table SET "_meta/op" = 'd' WHERE flow_published_at < TO_TIMESTAMP_TZ(:1, 'YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM')`, b.truncateSQL)

	b = &binding{target: sql.Table{Identifier: "the_table"}}
	require.EqualError(t, addTruncation(b, column(sql.FieldConfig{}), false), "the binding excludes the _meta/op field, which soft deletes need to mark them deleted")
	require.Empty(t, b.truncateSQL)

	b = &binding{target: withOp}
	require.EqualError(t, addTruncation(b, column(sql.FieldConfig{CastToString_: true}), true), "column flow_published_at is not a TIMESTAMP")
	require.Empty(t, b.truncateSQL)
}

func TestLongColumns(t *testing.T) {
	var row = []any{"1", strings.Repeat("x", 4001), nil, []byte("small"), strings.Repeat("y", 4000)}
	require.Equal(t, ".L...", longColumns(row, 4000))
	require.Equal(t, ".....", longColumns(row, 32767))
}
