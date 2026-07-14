package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"testing"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

// TestSdkStreamDatatypes streams one row of each column type that the
// connector maps and verifies the server-side parse of the NDJSON values.
func TestSdkStreamDatatypes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	ctx := context.Background()
	cfg := mustGetCfg(t)

	dsn, err := cfg.toURI(true, "")
	require.NoError(t, err)
	db, err := stdsql.Open("snowflake", dsn)
	require.NoError(t, err)
	defer db.Close()

	var accountName string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT CURRENT_ACCOUNT()").Scan(&accountName))

	cleanup := func() {
		db.ExecContext(ctx, "DROP TABLE IF EXISTS SDK_TYPES_TEST;")
	}
	defer cleanup()
	cleanup()

	_, err = db.ExecContext(ctx, `CREATE TABLE SDK_TYPES_TEST (
		I INTEGER, F FLOAT, B BOOLEAN, T TEXT, V VARIANT, D DATE, TS TIMESTAMP_LTZ, BIN BINARY
	);`)
	require.NoError(t, err)

	table := sql.Table{TableShape: sql.TableShape{Binding: 0}, Identifier: `SDK_TYPES_TEST`}
	columnNames := []string{"I", "F", "B", "T", "V", "D", "TS", "BIN"}

	sm, err := newSdkStreamManager(&cfg, "testing-sdk-types", accountName, 0, db)
	require.NoError(t, err)
	require.NoError(t, sm.addBinding(ctx, cfg.Schema, "SDK_TYPES_TEST", columnNames, table))

	require.NoError(t, sm.writeRow(ctx, 0, []any{
		int64(42),
		3.14,
		true,
		"hello",
		json.RawMessage(`{"a": [1, 2], "b": "c"}`),
		"2024-01-02",
		"2024-01-02T03:04:05Z",
		"aGVsbG8=", // "hello" in base64, as the runtime delivers binary values
	}))
	chunks, err := sm.flush(ctx, "types-token")
	require.NoError(t, err)
	require.NoError(t, sm.write(ctx, cfg.Schema, defaultPipeName("SDK_TYPES_TEST"), chunks[0]))

	var i int64
	var f float64
	var b bool
	var text, variant, date, timestamp, binary string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT
		I, F, B, T, TO_JSON(V), TO_VARCHAR(D, 'YYYY-MM-DD'),
		TO_VARCHAR(CONVERT_TIMEZONE('UTC', TS), 'YYYY-MM-DD"T"HH24:MI:SSTZH:TZM'),
		TO_VARCHAR(BIN, 'UTF-8')
		FROM SDK_TYPES_TEST`).Scan(&i, &f, &b, &text, &variant, &date, &timestamp, &binary))

	require.Equal(t, int64(42), i)
	require.Equal(t, 3.14, f)
	require.Equal(t, true, b)
	require.Equal(t, "hello", text)
	require.Equal(t, `{"a":[1,2],"b":"c"}`, variant)
	require.Equal(t, "2024-01-02", date)
	require.Equal(t, "2024-01-02T03:04:05Z", timestamp)
	require.Equal(t, "hello", binary)
}
