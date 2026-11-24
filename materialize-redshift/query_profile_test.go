package connector

import (
	"context"
	"fmt"
	"path"
	"testing"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestQueryHistoryFetch(t *testing.T) {
	cfg := mustGetCfg(t)
	ctx := context.Background()
	flags := testFlags(t, cfg)

	ep, err := NewDriver().NewEndpoint(ctx, cfg, flags)
	require.NoError(t, err)
	client, err := cfg.toS3Client(ctx, flags)
	require.NoError(t, err)
	store := newS3Store(client, cfg.Bucket)

	conn, err := pgx.Connect(ctx, cfg.toURI())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close(ctx) })

	table := "query_profile_" + uuid.NewString()[:8]
	_, err = conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id BIGINT, val BIGINT)", table))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := conn.Exec(ctx, "DROP TABLE "+table)
		require.NoError(t, err)
	})

	staged := stageRows(t, cfg, []string{"id", "val"}, [][]any{{1, 10}, {2, 20}, {3, 30}})
	manifest := path.Join(cfg.effectiveBucketPath(), uuid.NewString(), manifestFile)
	require.NoError(t, store.putManifest(ctx, manifest, staged.StoreFiles))
	t.Cleanup(func() { store.deleteObjects(ctx, append(staged.StoreFiles, manifest)) })

	d := &transactor{templates: renderTemplates(ep.Dialect), cfg: cfg, store: store}
	d.profiler = boilerplate.NewQueryProfiler(nil, profileInterval, nil, nil, nil)
	defer d.profiler.Stop()
	copySQL, err := d.copyFromS3(table, manifest, true)
	require.NoError(t, err)

	txn, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer txn.Rollback(ctx)

	// Each query that runs on the compute nodes has its own ID.
	var run = func(query string) string {
		var _, trace = d.profiler.Trace(ctx, 0, boilerplate.StatementKind(query))
		_, err := txn.Exec(ctx, query)
		require.NoError(t, err)
		var id string
		require.NoError(t, txn.QueryRow(ctx, "SELECT pg_last_query_id()::varchar").Scan(&id))
		trace.Finish(nil)
		return id
	}
	copied := run(copySQL)
	run(fmt.Sprintf("CREATE TEMP TABLE query_profile_source AS SELECT id + 1 AS id, val FROM %s", table))
	merged := run(fmt.Sprintf("MERGE INTO %[1]s USING query_profile_source AS r ON %[1]s.id = r.id "+
		"WHEN MATCHED THEN UPDATE SET val = r.val WHEN NOT MATCHED THEN INSERT (id, val) VALUES (r.id, r.val)", table))
	joined := run(fmt.Sprintf("CREATE TEMP TABLE query_profile_joined AS SELECT a.id FROM %[1]s a JOIN %[1]s b ON a.id = b.id", table))
	require.NoError(t, txn.Commit(ctx))
	require.Len(t, map[string]bool{copied: true, merged: true, joined: true}, 3)

	history, err := (&queryHistory{uri: cfg.toURI()}).Fetch(ctx, []string{copied, merged, joined})
	require.NoError(t, err)

	require.Equal(t, "success", history[copied]["status"])
	require.Equal(t, int64(3), history[copied]["loadedRows"])
	require.Equal(t, int64(1), history[copied]["sourceFiles"])
	require.Positive(t, history[copied]["totalMs"])

	require.Equal(t, "success", history[merged]["status"])
	require.NotContains(t, history[merged], "loadedRows")
	require.Positive(t, history[joined]["rowsScanned"])
	require.Contains(t, history[joined], "blocksRead")

	fields, err := newTableContext(cfg.toURI(), []sql.InfoTableLocation{ep.TableLocator([]string{table})})(ctx, 0)
	require.NoError(t, err)
	// The merge's updates leave the rows they replaced to be vacuumed.
	require.GreaterOrEqual(t, fields["table"].(log.Fields)["rows"], int64(4))
	require.Contains(t, fields["table"].(log.Fields), "visibleRows")
	require.NotEmpty(t, fields["table"].(log.Fields)["distStyle"])
	require.NotEmpty(t, fields["warehouse"].(log.Fields)["version"])
}
