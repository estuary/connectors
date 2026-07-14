package main

import (
	"context"
	stdsql "database/sql"
	"os"
	"testing"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

func TestSdkStreamManager(t *testing.T) {
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

	verify := func(t *testing.T, query string, want [][]any) {
		t.Helper()

		rows, err := db.Query(query)
		require.NoError(t, err)
		defer rows.Close()

		cols, err := rows.Columns()
		require.NoError(t, err)

		var got [][]any
		for rows.Next() {
			var data = make([]any, len(cols))
			var ptrs = make([]any, len(cols))
			for i := range data {
				ptrs[i] = &data[i]
			}
			require.NoError(t, rows.Scan(ptrs...))
			got = append(got, data)
		}

		require.Equal(t, want, got)
	}

	table := sql.Table{
		TableShape: sql.TableShape{
			Binding: 0,
		},
		Identifier: `SDK_STREAM_TEST`,
	}
	columnNames := []string{"KEY", "FIRSTCOL", "SECONDCOL"}

	cleanup := func(t *testing.T) {
		t.Helper()
		_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS SDK_STREAM_TEST;")
	}
	defer cleanup(t)

	cleanup(t)
	_, err = db.ExecContext(ctx, `CREATE TABLE SDK_STREAM_TEST (key TEXT, firstcol TEXT, secondcol TEXT);`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, createStageSQL)
	require.NoError(t, err)

	pipe := defaultPipeName("SDK_STREAM_TEST")

	sm, err := newSdkStreamManager(&cfg, "testing-sdk", accountName, 0, db)
	require.NoError(t, err)
	require.NoError(t, sm.addBinding(ctx, cfg.Schema, "SDK_STREAM_TEST", columnNames, table))

	require.NoError(t, sm.writeRow(ctx, 0, []any{"key1", "hello1", "world1"}))
	require.NoError(t, sm.writeRow(ctx, 0, []any{"key2", "hello2", "world2"}))
	require.NoError(t, sm.writeRow(ctx, 0, []any{"key3", "hello3", "world3"}))
	chunks, err := sm.flush(ctx, "the-token-1")
	require.NoError(t, err)
	require.Equal(t, 1, len(chunks))
	require.Equal(t, 1, len(chunks[0]))

	require.NoError(t, sm.write(ctx, cfg.Schema, pipe, chunks[0]))

	verify(t, "SELECT * FROM SDK_STREAM_TEST ORDER BY key", [][]any{
		{"key1", "hello1", "world1"},
		{"key2", "hello2", "world2"},
		{"key3", "hello3", "world3"},
	})

	// Writing the same chunks again is a no-op: their tokens are already
	// committed by the channel.
	require.NoError(t, sm.write(ctx, cfg.Schema, pipe, chunks[0]))

	verify(t, "SELECT * FROM SDK_STREAM_TEST ORDER BY key", [][]any{
		{"key1", "hello1", "world1"},
		{"key2", "hello2", "world2"},
		{"key3", "hello3", "world3"},
	})

	// Simulate a commit replay after a restart: a second transaction's chunks
	// are made durable, but the connector restarts before appending them. The
	// local chunk files are gone, so the replay retrieves the durable copies
	// from the internal stage.
	require.NoError(t, sm.writeRow(ctx, 0, []any{"key4", "hello4", "world4"}))
	require.NoError(t, sm.writeRow(ctx, 0, []any{"key5", "hello5", "world5"}))
	chunks, err = sm.flush(ctx, "the-token-2")
	require.NoError(t, err)
	require.Equal(t, 1, len(chunks[0]))
	for _, chunk := range chunks[0] {
		require.NoError(t, os.Remove(chunk.LocalPath))
	}

	sm, err = newSdkStreamManager(&cfg, "testing-sdk", accountName, 0, db)
	require.NoError(t, err)
	require.NoError(t, sm.addBinding(ctx, cfg.Schema, "SDK_STREAM_TEST", columnNames, table))

	require.NoError(t, sm.write(ctx, cfg.Schema, pipe, chunks[0]))

	verify(t, "SELECT * FROM SDK_STREAM_TEST ORDER BY key", [][]any{
		{"key1", "hello1", "world1"},
		{"key2", "hello2", "world2"},
		{"key3", "hello3", "world3"},
		{"key4", "hello4", "world4"},
		{"key5", "hello5", "world5"},
	})

	// Replaying through the restarted manager is also a no-op.
	require.NoError(t, sm.write(ctx, cfg.Schema, pipe, chunks[0]))

	verify(t, "SELECT * FROM SDK_STREAM_TEST ORDER BY key", [][]any{
		{"key1", "hello1", "world1"},
		{"key2", "hello2", "world2"},
		{"key3", "hello3", "world3"},
		{"key4", "hello4", "world4"},
		{"key5", "hello5", "world5"},
	})
}
