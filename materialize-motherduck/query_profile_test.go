package connector

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	testutil "github.com/estuary/connectors/materialize-boilerplate/testutil"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestHistoryFields(t *testing.T) {
	var n = func(v int64) stdsql.NullInt64 { return stdsql.NullInt64{Int64: v, Valid: true} }
	var s = func(v string) stdsql.NullString { return stdsql.NullString{String: v, Valid: true} }

	require.Equal(t, log.Fields{
		"totalMs": int64(9000), "executionMs": int64(7000), "waitMs": int64(2000),
		"uploadedBytes": int64(10), "downloadedBytes": int64(20), "spillToDiskBytes": int64(30),
		"errorType": "OUT_OF_MEMORY", "instanceType": "standard",
	}, historyRow{
		total: n(9000), execution: n(7000), wait: n(2000), uploaded: n(10), downloaded: n(20), spilled: n(30),
		errorType: s("OUT_OF_MEMORY"), instanceType: s("standard"),
	}.fields())

	require.Equal(t, log.Fields{
		"totalMs": int64(500), "executionMs": int64(500),
		"uploadedBytes": int64(0), "downloadedBytes": int64(0), "spillToDiskBytes": int64(0),
	}, historyRow{total: n(500), execution: n(500), wait: n(0)}.fields())
}

func TestHistoryErr(t *testing.T) {
	var denied = errors.New("Invalid Error: MDExternalException: Missing permission for query history. Ask an organization admin to grant you access.")
	require.ErrorIs(t, historyErr(denied), boilerplate.ErrQueryHistoryDenied)
	require.NotErrorIs(t, historyErr(errors.New("Invalid Error: MDExternalException: internal error")), boilerplate.ErrQueryHistoryDenied)
}

func TestQueryProfileLive(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	var ctx = context.Background()

	for _, tc := range []struct{ config, databaseType string }{
		{"testdata/config.s3.yaml", "DEFAULT"},
		{"testdata/config.ducklake.s3.yaml", "DUCKLAKE"},
	} {
		t.Run(tc.databaseType, func(t *testing.T) {
			raw, err := testutil.ReadConfigFile(tc.config)
			require.NoError(t, err)
			var cfg config
			require.NoError(t, json.Unmarshal(raw, &cfg))

			db, err := cfg.bareOpen()
			require.NoError(t, err)
			defer db.Close()
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()

			var dialect = createDuckDialect(nil, tc.databaseType == "DUCKLAKE")
			var table = "query_profile_" + uuid.NewString()[:8]
			var identifier = dialect.Identifier(cfg.Database, cfg.Schema, table)
			_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id BIGINT, val VARCHAR)", identifier))
			require.NoError(t, err)
			defer func() {
				_, err := conn.ExecContext(ctx, "DROP TABLE "+identifier)
				require.NoError(t, err)
			}()

			// Each query of a connection has its own ID, in or out of a
			// transaction.
			_, err = conn.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", identifier))
			require.NoError(t, err)
			inserted, err := lastQueryID(ctx, conn)
			require.NoError(t, err)
			txn, err := conn.BeginTx(ctx, nil)
			require.NoError(t, err)
			_, err = txn.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = 3", identifier))
			require.NoError(t, err)
			deleted, err := lastQueryID(ctx, txn)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
			require.NotEqual(t, inserted, deleted)
			for _, id := range []string{inserted, deleted} {
				_, err := uuid.Parse(id)
				require.NoError(t, err)
			}

			// The history query binds against the views whether or not the
			// user may read them, which the test user may not.
			rows, err := db.QueryContext(ctx, "DESCRIBE "+fmt.Sprintf(historyQuery, "'"+inserted+"'::UUID"))
			require.NoError(t, err)
			require.NoError(t, rows.Close())
			history, err := (&queryHistory{db: db}).Fetch(ctx, []string{inserted, deleted})
			if !errors.Is(err, boilerplate.ErrQueryHistoryDenied) {
				require.NoError(t, err)
				for _, fields := range history {
					require.Contains(t, fields, "totalMs")
				}
			}

			var location = sql.InfoTableLocation{TableSchema: cfg.Schema, TableName: table}
			fields, err := newTableContext(db, dialect.Literal(cfg.Database), []sql.InfoTableLocation{location})(ctx, 0)
			require.NoError(t, err)

			var warehouse = fields["warehouse"].(log.Fields)
			require.Equal(t, tc.databaseType, warehouse["databaseType"])
			require.NotEmpty(t, warehouse["ducklingSize"])
			require.NotEmpty(t, warehouse["duckdbVersion"])
			require.NotEmpty(t, warehouse["extensionVersion"])

			// The estimate catches up with new rows only minutes later.
			var described = fields["table"].(log.Fields)
			require.Contains(t, described, "estimatedRows")
			require.Equal(t, int64(2), described["columns"])
			if tc.databaseType == "DUCKLAKE" {
				require.Contains(t, described, "files")
				require.Contains(t, described, "deleteFiles")
			} else {
				require.NotContains(t, described, "files")
			}
		})
	}
}
