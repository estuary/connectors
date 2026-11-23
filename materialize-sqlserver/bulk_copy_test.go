package connector

import (
	"context"
	stdsql "database/sql"
	"testing"

	mssqldb "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/require"
)

// TestBulkCopyColumnNameWithBracket verifies that a bulk copy accepts a
// column whose name contains "]". Load and Store pass field names to the
// driver unquoted and rely on it to escape them.
func TestBulkCopyColumnNameWithBracket(t *testing.T) {
	startDatabase(t)

	var ctx = context.Background()
	var cfg = testConfig()
	connector, err := cfg.ToSQLConnector(ctx)
	require.NoError(t, err)
	var db = stdsql.OpenDB(connector)
	defer db.Close()

	// The temporary table belongs to one connection.
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecContext(ctx, `CREATE TABLE #flow_temp_bulk_copy_test (id BIGINT NOT NULL, "a]b" VARCHAR(MAX));`)
	require.NoError(t, err)

	txn, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer txn.Rollback()

	batch, err := txn.PrepareContext(ctx, mssqldb.CopyIn("#flow_temp_bulk_copy_test", mssqldb.BulkOptions{}, "id", "a]b"))
	require.NoError(t, err)
	_, err = batch.ExecContext(ctx, int64(1), "value")
	require.NoError(t, err)
	_, err = batch.ExecContext(ctx)
	require.NoError(t, err)

	var id int64
	var value string
	require.NoError(t, txn.QueryRowContext(ctx, `SELECT id, "a]b" FROM #flow_temp_bulk_copy_test;`).Scan(&id, &value))
	require.Equal(t, int64(1), id)
	require.Equal(t, "value", value)
	require.NoError(t, txn.Commit())
}
