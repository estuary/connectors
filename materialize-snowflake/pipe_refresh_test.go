package connector

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"math"
	"testing"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

const pipeRefreshIssue = "https://github.com/estuary/connectors/issues/5809"

// TestStreamV2StalePipeAfterDropNotNullLive asserts that a Snowflake bug still
// exists: a streaming pipe that has ingested rows rejects a null in a column
// whose NOT NULL constraint was just dropped.
func TestStreamV2StalePipeAfterDropNotNullLive(t *testing.T) {
	t.Parallel()

	var err = writeNullAfterDropNotNull(t, "STALE", false)
	if err == nil {
		t.Fatalf("Snowflake accepted a null row written right after DROP NOT NULL, with no pipe refresh. "+
			"Snowflake appears to have fixed the stale streaming pipe bug that pipeRefreshStatements works around. "+
			"Remove pipeRefreshStatements, its use in client.AlterTable, and the tests in this file. See %s.", pipeRefreshIssue)
	}
	require.ErrorContains(t, err, "rejected and discarded by Snowflake")
}

// TestStreamV2PipeRefreshAfterDropNotNullLive asserts that the statements of
// pipeRefreshStatements make a streaming pipe accept a null in a column whose
// NOT NULL constraint was just dropped.
func TestStreamV2PipeRefreshAfterDropNotNullLive(t *testing.T) {
	t.Parallel()

	var err = writeNullAfterDropNotNull(t, "REFRESH", true)
	if err != nil {
		t.Fatalf("a null row written right after DROP NOT NULL and a pipe refresh failed, so the "+
			"workaround of pipeRefreshStatements has stopped working. %s describes the fallback, "+
			"which is to recreate the pipe. The error was: %s", pipeRefreshIssue, err)
	}
}

// writeNullAfterDropNotNull has a streaming pipe ingest rows into the NOT NULL
// column VAL, drops that constraint, and returns the outcome of then writing a
// null into the column. With refresh the constraint is dropped by
// client.AlterTable for a streaming v2 table, and without it by the bare
// statement.
func writeNullAfterDropNotNull(t *testing.T, name string, refresh bool) error {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	var ctx = context.Background()
	var cfg = mustGetCfg(t)

	var dsn, err = cfg.toURI(true, "")
	require.NoError(t, err)
	db, err := stdsql.Open("snowflake", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	var accountName string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT CURRENT_ACCOUNT()").Scan(&accountName))

	var materialization = fmt.Sprintf("test/streamV2PipeRefresh-%s-%s", name, testRunNonce)
	var tbl = sql.Table{
		TableShape: sql.TableShape{Binding: 0, DeltaUpdates: true},
		Identifier: fmt.Sprintf("STREAMV2_PIPE_REFRESH_%s_%s", name, testRunNonce),
		Keys:       []sql.Column{{Identifier: `KEY`}},
		Values:     []sql.Column{{Identifier: `VAL`}, {Identifier: `OTHER`}},
	}
	var exec = func(query string) {
		var _, err = db.ExecContext(ctx, query)
		require.NoError(t, err)
	}
	exec(fmt.Sprintf("CREATE OR REPLACE TABLE %s (KEY TEXT NOT NULL, VAL TIMESTAMP_TZ NOT NULL, OTHER TEXT);", tbl.Identifier))
	t.Cleanup(func() { db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", tbl.Identifier)) })

	// write stores rows through a new manager on a channel of its own, and
	// returns the first failure of the writes or of the flush.
	var write = func(stateKey string, rows int, val any) error {
		sm, err := newStreamManager(&cfg, materialization, accountName, 0)
		require.NoError(t, err)
		m, err := newStreamV2Manager(&cfg, db, testDialect, sm, materialization, accountName,
			&pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
		require.NoError(t, err)
		singleChannelLayout(m)

		var target = tbl
		target.StateKey = noncedStateKey(stateKey)
		m.addBinding(cfg.Database, cfg.Schema, tbl.Identifier, target, nil)

		for i := range rows {
			if err := testWriteRow(ctx, m, 0, []any{fmt.Sprintf("%s-%d", stateKey, i), val, nil}); err != nil {
				return err
			}
		}
		_, err = m.flush(ctx)
		return err
	}

	// A pipe that has never ingested a row is not stale.
	require.NoError(t, write("pipe-refresh-"+name+"-before", 200, "2026-01-02T03:04:05Z"))

	if refresh {
		var c = &client{db: db, cfg: cfg, ep: &sql.Endpoint[config]{Dialect: testDialect}}
		var _, apply, err = c.AlterTable(ctx, sql.TableAlter{Table: tbl, DropNotNulls: []boilerplate.ExistingField{{Name: "VAL"}}})
		require.NoError(t, err)
		require.NoError(t, apply(ctx))
	} else {
		exec(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN VAL DROP NOT NULL;", tbl.Identifier))
	}

	return write("pipe-refresh-"+name+"-after", 1, nil)
}
