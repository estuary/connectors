package connector

import (
	"testing"
	"time"

	"github.com/estuary/connectors/go/common"
	"github.com/estuary/connectors/go/schedule"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
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
	var identity = func(s string) string { return s }
	var col = &sql.Column{
		Identifier: "flow_published_at",
		Projection: sql.Projection{Projection: pf.Projection{Field: "flow_published_at"}},
	}

	for _, tc := range []struct {
		name          string
		field         *boilerplate.ExistingField
		noTable       bool
		wantPrecision time.Duration
		wantReason    string
	}{
		{
			name:          "microseconds",
			field:         &boilerplate.ExistingField{Name: "flow_published_at", Meta: datetimeFieldMeta{precision: 6}},
			wantPrecision: time.Microsecond,
		},
		{
			name:          "seconds",
			field:         &boilerplate.ExistingField{Name: "flow_published_at", Meta: datetimeFieldMeta{precision: 0}},
			wantPrecision: time.Second,
		},
		{
			name:       "not a DATETIME",
			field:      &boilerplate.ExistingField{Name: "flow_published_at"},
			wantReason: "column flow_published_at is not a DATETIME",
		},
		{
			name:       "missing column",
			wantReason: "column flow_published_at was not found in the table",
		},
		{
			name:       "missing table",
			noTable:    true,
			wantReason: "table the_table was not found in the database",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var is = boilerplate.NewInfoSchema(func(p []string) []string { return p }, identity, identity, false, false)
			if !tc.noTable {
				var res = is.PushResource("the_table")
				if tc.field != nil {
					res.PushField(*tc.field)
				}
			}
			var b = &binding{target: sql.Table{Identifier: "the_table", TableShape: sql.TableShape{Path: []string{"the_table"}}}}

			var reason = addTruncation(b, col, is)
			if tc.wantReason != "" {
				require.EqualError(t, reason, tc.wantReason)
				require.Empty(t, b.truncateSQL)
				return
			}
			require.NoError(t, reason)
			require.Equal(t, "DELETE FROM the_table WHERE flow_published_at < ?;", b.truncateSQL)
			require.Equal(t, tc.wantPrecision, b.truncatePrecision)
		})
	}
}

func TestTruncateBefore(t *testing.T) {
	var newYork, err = schedule.ParseTimezone("America/New_York")
	require.NoError(t, err)
	fixed, err := schedule.ParseTimezone("+05:00")
	require.NoError(t, err)

	for _, tc := range []struct {
		name      string
		boundary  time.Time
		loc       *time.Location
		precision time.Duration
		want      string
	}{
		{
			name:      "microseconds floor the boundary",
			boundary:  time.Date(2026, 9, 29, 12, 0, 0, 1_500, time.UTC),
			loc:       time.UTC,
			precision: time.Microsecond,
			want:      "2026-09-29 12:00:00.000001",
		},
		{
			name:      "seconds floor the boundary",
			boundary:  time.Date(2026, 9, 29, 12, 0, 0, 700_000_000, time.UTC),
			loc:       time.UTC,
			precision: time.Second,
			want:      "2026-09-29 12:00:00",
		},
		{
			name:      "fixed offset",
			boundary:  time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC),
			loc:       fixed,
			precision: time.Microsecond,
			want:      "2026-09-29 17:00:00",
		},
		{
			name:      "named zone",
			boundary:  time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC),
			loc:       newYork,
			precision: time.Microsecond,
			want:      "2026-09-29 08:00:00",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, truncateBefore(tc.boundary, tc.loc, tc.precision))
		})
	}
}
