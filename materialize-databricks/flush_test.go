package connector

import (
	"testing"
	"time"

	"github.com/estuary/connectors/go/common"
	sql "github.com/estuary/connectors/materialize-sql"
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
				primary:               true,
				truncateAfterBackfill: sql.TruncateAfterBackfill(flags),
			}
			require.NoError(t, d.Flush(t.Context(), map[int]time.Time{0: boundary}))
			require.Equal(t, tc.want, d.truncations)
		})
	}
}

func TestFlushNonPrimary(t *testing.T) {
	var d = &transactor{
		bindings:              []*binding{{target: sql.Table{}, truncateSQL: "DELETE"}},
		primary:               false,
		truncateAfterBackfill: true,
	}
	require.NoError(t, d.Flush(t.Context(), map[int]time.Time{0: time.Now()}))
	require.Equal(t, map[int]time.Time{}, d.truncations)
}

func TestFlushDeltaUpdatesExempt(t *testing.T) {
	var d = &transactor{
		bindings:              []*binding{{target: sql.Table{TableShape: sql.TableShape{DeltaUpdates: true}}, truncateSQL: "DELETE"}},
		primary:               true,
		truncateAfterBackfill: true,
	}
	require.NoError(t, d.Flush(t.Context(), map[int]time.Time{0: time.Now()}))
	require.Equal(t, map[int]time.Time{}, d.truncations)
}

func TestFlushNoPublishedAtColumn(t *testing.T) {
	var d = &transactor{
		bindings:              []*binding{{target: sql.Table{}, truncateSQL: ""}},
		primary:               true,
		truncateAfterBackfill: true,
	}
	require.NoError(t, d.Flush(t.Context(), map[int]time.Time{0: time.Now()}))
	require.Equal(t, map[int]time.Time{}, d.truncations)
}
