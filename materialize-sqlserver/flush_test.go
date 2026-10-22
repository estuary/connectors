package connector

import (
	"testing"
	"time"

	"github.com/estuary/connectors/go/common"
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
		name       string
		field      *boilerplate.ExistingField
		noTable    bool
		wantSQL    string
		wantReason string
	}{
		{
			name:    "DATETIME2",
			field:   &boilerplate.ExistingField{Name: "flow_published_at", Meta: datetimeFieldMeta{ddl: "DATETIME2(7)"}},
			wantSQL: "DELETE FROM the_table WHERE flow_published_at < CAST(CAST(@p1 AS DATETIME2(7)) AS DATETIME2(7));",
		},
		{
			name:    "DATETIME2 at seconds",
			field:   &boilerplate.ExistingField{Name: "flow_published_at", Meta: datetimeFieldMeta{ddl: "DATETIME2(0)"}},
			wantSQL: "DELETE FROM the_table WHERE flow_published_at < CAST(CAST(@p1 AS DATETIME2(7)) AS DATETIME2(0));",
		},
		{
			name:    "DATETIME",
			field:   &boilerplate.ExistingField{Name: "flow_published_at", Meta: datetimeFieldMeta{ddl: "DATETIME"}},
			wantSQL: "DELETE FROM the_table WHERE flow_published_at < CAST(CAST(@p1 AS DATETIME2(7)) AS DATETIME);",
		},
		{
			name:    "SMALLDATETIME",
			field:   &boilerplate.ExistingField{Name: "flow_published_at", Meta: datetimeFieldMeta{ddl: "SMALLDATETIME"}},
			wantSQL: "DELETE FROM the_table WHERE flow_published_at < CAST(CAST(@p1 AS DATETIME2(7)) AS SMALLDATETIME);",
		},
		{
			name:       "not a datetime",
			field:      &boilerplate.ExistingField{Name: "flow_published_at", Type: "nvarchar"},
			wantReason: "column flow_published_at is not a DATETIME, DATETIME2 or SMALLDATETIME",
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
			require.Equal(t, tc.wantSQL, b.truncateSQL)
		})
	}
}
