package connector

import (
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
			var b = &binding{target: sql.Table{}}
			b.truncate.deleteSQL = "DELETE"
			var d = &transactor{
				bindings:              []*binding{b},
				truncateAfterBackfill: sql.TruncateAfterBackfill(flags),
			}
			require.NoError(t, d.Flush(t.Context(), map[int]time.Time{0: boundary}))
			require.Equal(t, tc.want, d.truncations)
		})
	}
}

func TestAddBindingTruncateSQL(t *testing.T) {
	var cfg = testConfig()
	var dialect = clickHouseDialect(cfg.Database)
	var id = sql.Projection{Projection: pf.Projection{
		Field:     "id",
		Inference: pf.Inference{Types: []string{"integer"}, Exists: pf.Inference_MUST},
	}}
	var publishedAt = sql.Projection{Projection: pf.Projection{
		Field: "flow_published_at",
		Ptr:   "/_meta/uuid",
		Inference: pf.Inference{
			Types:   []string{"string"},
			Exists:  pf.Inference_MUST,
			String_: &pf.Inference_String{Format: "date-time", ContentEncoding: "uuid"},
		},
	}}

	table, err := sql.ResolveTable(sql.TableShape{
		Path:   sql.TablePath{"the_table"},
		Keys:   []sql.Projection{id},
		Values: []sql.Projection{publishedAt},
	}, dialect)
	require.NoError(t, err)

	var tr = &transactor{dialect: dialect, templates: renderTemplates(dialect, cfg.HardDelete), cfg: cfg, _range: &pf.RangeSpec{}}
	require.NoError(t, tr.addBinding(t.Context(), table))
	require.Equal(t, "SELECT count() FROM the_table WHERE flow_published_at < toDateTime64(?, 6, 'UTC') SETTINGS select_sequential_consistency = 1;", tr.bindings[0].truncate.countSQL)
	require.Equal(t, "DELETE FROM the_table WHERE flow_published_at < toDateTime64(?, 6, 'UTC');", tr.bindings[0].truncate.deleteSQL)
}
