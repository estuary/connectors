package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/estuary/connectors/go/blob"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/connectors/materialize-boilerplate/testutil"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// Query insights records a statement minutes after it finishes, so the history
// this checks is that of statements run before it, such as the integration
// test's.
func TestQueryHistoryFetch(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	raw, err := testutil.ReadConfigFile("testdata/config.yaml")
	require.NoError(t, err)
	var cfg config
	require.NoError(t, json.Unmarshal(raw, &cfg))
	ctx := context.Background()

	db, err := cfg.db()
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	bucket, err := blob.NewAzureBlobBucket(ctx, cfg.ContainerName, cfg.StorageAccountName, blob.WithAzureStorageAccountKey(cfg.StorageAccountKey))
	require.NoError(t, err)
	files := boilerplate.NewStagedFiles(stagedFileClient{}, bucket, fileSizeLimit, cfg.Directory, false, false)
	files.AddBinding(0, []string{"id", "val"})
	for id := range 3 {
		require.NoError(t, files.WriteRow(ctx, 0, []any{id, id * 10}))
	}
	uris, err := files.Flush(0)
	require.NoError(t, err)
	t.Cleanup(func() { files.CleanupCurrentTransaction(ctx) })
	for i, uri := range uris {
		uris[i] = testDialect.Literal(uri)
	}

	name := "query_profile_" + uuid.NewString()[:8]
	table := testDialect.Identifier(cfg.Warehouse, cfg.Schema, name)
	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id BIGINT, val BIGINT)", table))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := db.ExecContext(ctx, "DROP TABLE "+table)
		require.NoError(t, err)
	})
	copyInto := func(target string) string {
		return fmt.Sprintf("COPY INTO %s (id, val) FROM %s WITH (FILE_TYPE = 'CSV', COMPRESSION = 'Gzip', FIELDQUOTE = '`', "+
			"CREDENTIAL = (IDENTITY='Storage Account Key', SECRET=%s));", target, strings.Join(uris, ", "), testDialect.Literal(cfg.StorageAccountKey))
	}

	profiler := boilerplate.NewQueryProfiler(nil, profileInterval, nil, nil, nil)
	defer profiler.Stop()
	txn, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer txn.Rollback()

	// Every kind of statement the connector labels must accept a label.
	var labels []string
	for _, query := range []string{
		copyInto(table),
		"CREATE TABLE query_profile_staged (id BIGINT, val BIGINT);\n\n" + copyInto("query_profile_staged"),
		fmt.Sprintf("DELETE r FROM %s AS r INNER JOIN query_profile_staged AS l ON l.id = r.id AND r.id >= 0 AND r.id <= 1;", table),
		fmt.Sprintf("INSERT INTO %s (id, val) SELECT id, val FROM query_profile_staged WHERE id < 2;", table),
		fmt.Sprintf("DELETE FROM %s WHERE id < 0;", table),
		fmt.Sprintf("SELECT 0, r.val FROM query_profile_staged AS l JOIN %[1]s AS r ON l.id = r.id\nUNION ALL\nSELECT 1, r.val FROM %[1]s AS r\n", table),
	} {
		var _, trace = profiler.Trace(ctx, 0, boilerplate.StatementKind(query))
		labelled := labelQuery(trace, query)
		_, err := txn.ExecContext(ctx, labelled)
		require.NoError(t, err, query)
		labels = append(labels, regexp.MustCompile(`LABEL = '(.+)'`).FindStringSubmatch(labelled)[1])
	}
	_, err = txn.ExecContext(ctx, "DROP TABLE query_profile_staged;")
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// These statements aren't recorded yet, so the history checked is that of
	// the labelled statements recorded most recently.
	history := &queryHistory{cfg: cfg}
	_, err = history.Fetch(ctx, labels)
	require.NoError(t, err)

	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
SELECT TOP 5 label FROM queryinsights.exec_requests_history
WHERE label <> '' AND end_time >= DATEADD(second, -%d, SYSUTCDATETIME())
GROUP BY label ORDER BY MAX(end_time) DESC`, int(historyWindow.Seconds()/2)))
	require.NoError(t, err)
	var recorded []string
	for rows.Next() {
		var label string
		require.NoError(t, rows.Scan(&label))
		recorded = append(recorded, label)
	}
	require.NoError(t, rows.Err())
	if len(recorded) == 0 {
		t.Log("query insights recorded no labelled statement recently; its history went unchecked")
	} else {
		fetched, err := history.Fetch(ctx, recorded)
		require.NoError(t, err)
		require.Len(t, fetched, len(recorded))
		for _, fields := range fetched {
			require.Len(t, fields["statementId"], 36)
			require.NotEmpty(t, fields["status"])
			require.NotEmpty(t, fields["sqlPool"])
			require.Contains(t, fields, "totalMs")
			require.Contains(t, fields, "cpuMs")
			require.Contains(t, fields, "scannedRemoteMB")
		}
	}

	fields, err := newTableContext(cfg, []sql.InfoTableLocation{{TableSchema: cfg.Schema, TableName: name}})(ctx, 0)
	require.NoError(t, err)
	require.Contains(t, fields["table"], "statistics")
	require.NotEmpty(t, fields["warehouse"].(log.Fields)["version"])
	require.NotEmpty(t, fields["warehouse"].(log.Fields)["capacity"])
	require.Contains(t, fields["warehouse"], "vOrder")
}

func TestLabelQuery(t *testing.T) {
	var profiler = boilerplate.NewQueryProfiler(nil, time.Hour, nil, nil, nil)
	defer profiler.Stop()

	for name, query := range map[string]string{
		"a statement":             "DELETE FROM t WHERE id < 0;\n",
		"a batch":                 "CREATE TABLE s (id BIGINT);\n\nCOPY INTO s (id) FROM 'u' WITH (FILE_TYPE = 'CSV');\n",
		"a query with no ending":  "SELECT 0, d FROM a\nUNION ALL\nSELECT 1, d FROM b\n",
		"a statement, no newline": "INSERT INTO t SELECT * FROM s;",
	} {
		t.Run(name, func(t *testing.T) {
			var _, first = profiler.Trace(t.Context(), 0, "delete")
			var _, second = profiler.Trace(t.Context(), 0, "delete")
			var labelled = labelQuery(first, query)

			require.Regexp(t, `^[^;]*(;[^;]+)?\nOPTION \(LABEL = '[0-9a-f-]{36}'\);$`, labelled)
			require.NotEqual(t, labelled, labelQuery(second, query))
		})
	}
}

func TestVersionFields(t *testing.T) {
	require.Equal(t, log.Fields{"version": "12.0.2000.8", "build": "Nov 10 2026 19:38:16"},
		versionFields("Microsoft Azure SQL Data Warehouse (RTM) - 12.0.2000.8 \n\tNov 10 2026 19:38:16 \n\tCopyright (C) 2026 Microsoft Corporation\n"))
	require.Empty(t, versionFields("unrecognized"))
}
