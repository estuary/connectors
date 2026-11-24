package connector

import (
	"context"
	stdsql "database/sql"
	"testing"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/google/uuid"
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

func TestTableContext(t *testing.T) {
	cfg := mustGetCfg(t)
	dsn, err := cfg.toURI(true, "query-profile-test")
	require.NoError(t, err)
	db, err := stdsql.Open("snowflake", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	ctx := context.Background()
	var dialect = snowflakeDialect(cfg.Schema, cfg.TimestampType, nil)
	var name = "query_profile_" + uuid.NewString()[:8]
	var identifier = dialect.Identifier(name)
	_, err = db.ExecContext(ctx, "CREATE TABLE "+identifier+" AS SELECT SEQ4() AS id FROM TABLE(GENERATOR(ROWCOUNT => 1000))")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := db.ExecContext(ctx, "DROP TABLE "+identifier)
		require.NoError(t, err)
	})

	var b = &binding{target: sql.Table{
		Identifier:   identifier,
		InfoLocation: dialect.TableLocator([]string{name}),
		Keys:         []sql.Column{{Identifier: "id"}},
	}}
	fields, err := newTableContext(db, []*binding{b})(ctx, 0)
	require.NoError(t, err)

	var table, warehouse = fields["table"].(map[string]any), fields["warehouse"].(map[string]any)
	require.Equal(t, int64(1000), table["rows"])
	require.Positive(t, table["bytes"])
	require.Equal(t, int64(1), table["partitions"])
	require.Contains(t, table, "keyAverageDepth")
	require.NotEmpty(t, warehouse["version"])
	require.NotEmpty(t, warehouse["size"])
}
