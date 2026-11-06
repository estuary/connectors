package connector

import (
	"context"
	stdsql "database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueryHistoryFetch(t *testing.T) {
	cfg := mustGetCfg(t)
	dsn, err := cfg.toURI(true, "query-profile-test")
	require.NoError(t, err)
	db, err := stdsql.Open("snowflake", dsn)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	// The driver reports each query's ID.
	var run = func(query string) (string, error) {
		var id string
		queryCtx, collect := reportQueryID(ctx, func(reported string) { id = reported })
		rows, err := conn.QueryContext(queryCtx, query)
		if err == nil {
			rows.Close()
		}
		collect()
		return id, err
	}

	_, err = conn.ExecContext(ctx, "CREATE TEMPORARY TABLE query_profile_scan AS SELECT SEQ4() AS id FROM TABLE(GENERATOR(ROWCOUNT => 1000000))")
	require.NoError(t, err)
	scanned, err := run("SELECT COUNT(*) FROM query_profile_scan WHERE id > 500000")
	require.NoError(t, err)
	failed, err := run("SELECT * FROM query_profile_no_such_table")
	require.Error(t, err)

	history, err := (&queryHistory{db: db}).Fetch(ctx, []string{scanned, failed})
	require.NoError(t, err)

	require.Equal(t, "SUCCESS", history[scanned]["status"])
	require.NotEmpty(t, history[scanned]["warehouseSize"])
	require.Positive(t, history[scanned]["totalMs"])
	require.Positive(t, history[scanned]["partitionsTotal"])
	require.Contains(t, history[scanned], "partitionsScanned")
	require.Equal(t, int64(1), history[scanned]["rowsProduced"])

	require.Equal(t, "FAILED_WITH_ERROR", history[failed]["status"])
	require.NotContains(t, history[failed], "partitionsTotal")
}
