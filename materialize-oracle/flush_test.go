package connector

import (
	"testing"
	"time"

	"github.com/estuary/connectors/go/common"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

func TestFlushFeatureFlags(t *testing.T) {
	var boundary = time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		flags string
		delta bool
		sql   string
		want  map[int]time.Time
	}{
		{flags: "", sql: "DELETE", want: map[int]time.Time{0: boundary}},
		{flags: "no_truncate_after_backfill", sql: "DELETE", want: map[int]time.Time{}},
		{flags: "retain_existing_data_on_backfill", sql: "DELETE", want: map[int]time.Time{}},
		{flags: "", delta: true, sql: "DELETE", want: map[int]time.Time{}},
		{flags: "", want: map[int]time.Time{}},
	} {
		t.Run(tc.flags, func(t *testing.T) {
			var flags = common.ResolveFlags(tc.flags, featureFlagDefaults, common.CreatedAt{})
			var d = &transactor{
				bindings:              []*binding{{target: sql.Table{TableShape: sql.TableShape{DeltaUpdates: tc.delta}}, truncateSQL: tc.sql}},
				truncateAfterBackfill: sql.TruncateAfterBackfill(flags),
			}
			require.NoError(t, d.Flush(t.Context(), map[int]time.Time{0: boundary}))
			require.Equal(t, tc.want, d.truncations)
		})
	}
}

func TestAddTruncation(t *testing.T) {
	transactor, _ := newStoreTest(t)
	target := transactor.bindings[0].target
	col, err := target.PublishedAtColumn()
	require.Error(t, err, "the test table projects /_meta/uuid without uuid encoding")

	shape := target.TableShape
	shape.Values[0].Inference.String_.ContentEncoding = "uuid"
	target, err = sql.ResolveTable(shape, testDialect)
	require.NoError(t, err)
	col, err = target.PublishedAtColumn()
	require.NoError(t, err)

	for _, tc := range []struct {
		name       string
		is         *boilerplate.InfoSchema
		wantSQL    string
		wantReason string
	}{
		{
			name:    "timestamp with time zone",
			is:      testInfoSchema(boilerplate.ExistingField{Name: "FLOW_PUBLISHED_AT", Type: "TIMESTAMP WITH TIME ZONE"}),
			wantSQL: "DELETE FROM TEST.ORDERS WHERE FLOW_PUBLISHED_AT < :1",
		},
		{
			name:       "not a timestamp with time zone",
			is:         testInfoSchema(boilerplate.ExistingField{Name: "FLOW_PUBLISHED_AT", Type: "VARCHAR2"}),
			wantReason: "column FLOW_PUBLISHED_AT is not a TIMESTAMP WITH TIME ZONE",
		},
		{
			name:       "missing column",
			is:         testInfoSchema(boilerplate.ExistingField{Name: "ID", Type: "NUMBER"}),
			wantReason: "column FLOW_PUBLISHED_AT was not found in the table",
		},
		{
			name:       "missing table",
			is:         testInfoSchema(),
			wantReason: "table TEST.ORDERS was not found in the database",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var b = &binding{target: target}
			var reason = addTruncation(b, col, tc.is)
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
