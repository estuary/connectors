package connector

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"math"
	"strings"
	"testing"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestRenderDropNotNulls(t *testing.T) {
	var table = sql.Table{Identifier: "A_TABLE"}
	var newResource = func(fields ...boilerplate.ExistingField) *boilerplate.ExistingResource {
		var r = boilerplate.NewInfoSchema(nil, nil, func(f string) string { return f }, false, false).PushResource("PUBLIC", "A_TABLE")
		for _, f := range fields {
			r.PushField(f)
		}
		return r
	}

	t.Run("drops every NOT NULL column", func(t *testing.T) {
		stmt, err := renderDropNotNulls(testTemplates.alterTableColumns, table, newResource(
			boilerplate.ExistingField{Name: "KEY"},
			boilerplate.ExistingField{Name: "OPTIONAL", Nullable: true},
			boilerplate.ExistingField{Name: "a required value"},
		))
		require.NoError(t, err)
		require.Equal(t,
			"ALTER TABLE A_TABLE ALTER COLUMN\n\tKEY DROP NOT NULL,\n\t\"a required value\" DROP NOT NULL;",
			strings.TrimSpace(stmt),
		)
	})

	t.Run("no statement when every column is nullable", func(t *testing.T) {
		stmt, err := renderDropNotNulls(testTemplates.alterTableColumns, table, newResource(
			boilerplate.ExistingField{Name: "KEY", Nullable: true},
		))
		require.NoError(t, err)
		require.Empty(t, stmt)
	})
}

// TestStreamV2DropNotNullsLive opens a snowpipe_streaming_v2 binding on a table
// that still has NOT NULL columns and whose pipe has already ingested rows.
func TestStreamV2DropNotNullsLive(t *testing.T) {
	t.Parallel()

	var ctx = context.Background()
	var cfg = mustGetCfg(t)

	dsn, err := cfg.toURI(true, "")
	require.NoError(t, err)
	db, err := stdsql.Open("snowflake", dsn)
	require.NoError(t, err)
	defer db.Close()

	var accountName string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT CURRENT_ACCOUNT()").Scan(&accountName))

	var tableName = "STREAMV2_NOT_NULL_TEST_" + testRunNonce
	var cleanup = func() {
		db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", tableName))
	}
	cleanup()
	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (KEY TEXT NOT NULL, VAL TIMESTAMP_TZ NOT NULL, OTHER TEXT);", tableName))
	require.NoError(t, err)
	defer cleanup()

	var dialect = snowflakeDialect(cfg.Schema, timestampTypeLTZ, map[string]bool{flagSnowpipeStreaming: true, flagSnowpipeStreamingV2: true})
	var target = sql.Table{
		TableShape: sql.TableShape{Binding: 0, Path: []string{tableName}, DeltaUpdates: true},
		Identifier: tableName,
		Keys:       []sql.Column{{Identifier: `KEY`}},
		Values:     []sql.Column{{Identifier: `VAL`}, {Identifier: `OTHER`}},
		StateKey:   noncedStateKey("dropNotNulls"),
	}

	var task = "test/streamV2DropNotNulls-" + testRunNonce
	sm, err := newStreamManager(&cfg, task, accountName, 0)
	require.NoError(t, err)
	var newManager = func(prior streamV2Checkpoint) *streamV2Manager {
		m, err := newStreamV2Manager(&cfg, db, dialect, sm, task, accountName, &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
		require.NoError(t, err)
		singleChannelLayout(m)
		m.addBinding(cfg.Database, cfg.Schema, tableName, target, prior)
		return m
	}
	var writeRows = func(m *streamV2Manager, prefix string, rows int) streamV2Checkpoint {
		for i := range rows {
			require.NoError(t, testWriteRow(ctx, m, 0, []any{fmt.Sprintf("%s-%d", prefix, i), "2026-01-02T03:04:05Z", "x"}))
		}
		entries, err := m.flush(ctx)
		require.NoError(t, err)
		return entries[0]
	}

	c, err := newClient(ctx, task, &sql.Endpoint[config]{Config: cfg, Dialect: dialect})
	require.NoError(t, err)
	defer c.Close()
	var notNullColumns = func() (*boilerplate.InfoSchema, []string) {
		var is = boilerplate.NewInfoSchema(sql.ToLocatePathFn(dialect.TableLocator), dialect.SchemaLocator, dialect.ColumnLocator, false, false)
		require.NoError(t, c.PopulateInfoSchema(ctx, is, [][]string{target.Path}))
		var existing = is.GetResource(target.Path)
		require.NotNil(t, existing)
		var names []string
		for _, f := range existing.AllFields() {
			if !f.Nullable {
				names = append(names, f.Name)
			}
		}
		return is, names
	}

	// The pipe ingests rows while the columns are still NOT NULL.
	var prior = writeRows(newManager(nil), "before", 50)

	is, names := notNullColumns()
	require.ElementsMatch(t, []string{"KEY", "VAL"}, names)

	var d = &transactor{
		db:        db,
		templates: renderTemplates(dialect),
		bindings:  []*binding{{target: target, streamingV2: true}},
	}
	require.NoError(t, d.dropStreamV2NotNulls(ctx, is))
	// Another shard that read the same columns drops the same constraints.
	require.NoError(t, d.dropStreamV2NotNulls(ctx, is))

	// The rows that follow the drop without delay are not rejected.
	writeRows(newManager(prior), "after", 50)

	_, names = notNullColumns()
	require.Empty(t, names)

	var count int
	require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s;", tableName)).Scan(&count))
	require.Equal(t, 100, count)
}
