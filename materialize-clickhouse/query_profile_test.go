package connector

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// testProfiler is the profiler of a transactor a test builds itself.
func testProfiler() *boilerplate.QueryProfiler {
	return boilerplate.NewQueryProfiler(nil, profileInterval, nil, nil, nil)
}

func TestQueryHistoryFetch(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	ctx := context.Background()
	conn, err := clickhouse.Open(testConfig().newClickhouseOptions())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	const table = "query_profile_test"
	require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS "+table+" SYNC"))
	require.NoError(t, conn.Exec(ctx, "CREATE TABLE "+table+" (id UInt64, val String) ENGINE = ReplacingMergeTree PARTITION BY id % 2 ORDER BY id"))
	t.Cleanup(func() { require.NoError(t, conn.Exec(ctx, "DROP TABLE "+table+" SYNC")) })

	// The server logs each query under the ID the connector gave it.
	var run = func(do func(context.Context) error) (string, error) {
		var id string
		queryCtx, _ := reportQueryID(ctx, func(reported string) { id = reported })
		return id, do(queryCtx)
	}

	inserted, err := run(func(ctx context.Context) error {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table)
		if err != nil {
			return err
		}
		for id := range 1000 {
			if err := batch.Append(uint64(id), "value"); err != nil {
				return err
			}
		}
		return batch.Send()
	})
	require.NoError(t, err)
	scanned, err := run(func(ctx context.Context) error {
		var count uint64
		return conn.QueryRow(ctx, "SELECT count() FROM "+table+" FINAL WHERE id IN (2, 4, 6)").Scan(&count)
	})
	require.NoError(t, err)
	failed, err := run(func(ctx context.Context) error {
		return conn.Exec(ctx, "SELECT * FROM query_profile_no_such_table")
	})
	require.Error(t, err)
	require.NoError(t, conn.Exec(ctx, "SYSTEM FLUSH LOGS"))

	// The log is read the same from every replica of the cluster and, where
	// there is no such cluster, from the one replica.
	for _, cluster := range []string{historyCluster, "query_profile_no_such_cluster"} {
		var h = &queryHistory{conn: conn, cluster: cluster}
		history, err := h.Fetch(ctx, []string{inserted, scanned, failed})
		require.NoError(t, err)
		require.Equal(t, cluster == historyCluster, h.cluster != "")
		require.Len(t, history, 3)

		require.Equal(t, "QueryFinish", history[inserted]["status"])
		require.Equal(t, uint64(1000), history[inserted]["rowsWritten"])
		require.Positive(t, history[inserted]["bytesWritten"])

		require.Equal(t, "QueryFinish", history[scanned]["status"])
		require.Equal(t, uint64(1), history[scanned]["rowsProduced"])
		require.Positive(t, history[scanned]["rowsRead"])
		require.Equal(t, uint64(1), history[scanned]["partsScanned"])
		require.Equal(t, uint64(2), history[scanned]["partsTotal"])
		require.Positive(t, history[scanned]["marksTotal"])
		require.NotContains(t, history[scanned], "exceptionCode")

		require.Equal(t, "ExceptionBeforeStart", history[failed]["status"])
		require.Equal(t, int32(chproto.ErrUnknownTable), history[failed]["exceptionCode"])
	}

	fields, err := newTableContext(conn, []string{table})(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, log.Fields{
		"engine": "ReplacingMergeTree", "sortingKey": "id", "partitionKey": "id % 2",
		"rows": uint64(1000), "partitions": uint64(2), "parts": uint64(2), "maxPartitionParts": uint64(1),
	}, withoutKeys(fields["table"].(log.Fields), "bytes", "uncompressedBytes"))
	require.Positive(t, fields["table"].(log.Fields)["bytes"])
	require.Positive(t, fields["table"].(log.Fields)["uncompressedBytes"])
	require.NotEmpty(t, fields["warehouse"].(log.Fields)["version"])
	require.Positive(t, fields["warehouse"].(log.Fields)["memoryBytes"])
	require.Equal(t, uint64(1), fields["warehouse"].(log.Fields)["replicas"])
}

func withoutKeys(fields log.Fields, keys ...string) log.Fields {
	var out = make(log.Fields)
	for k, v := range fields {
		out[k] = v
	}
	for _, k := range keys {
		delete(out, k)
	}
	return out
}

// historyConn fails a read of the query log of every replica with one error,
// and of one replica's with another.
type historyConn struct {
	chdriver.Conn
	cluster, local error
}

func (c *historyConn) Query(_ context.Context, query string, _ ...any) (chdriver.Rows, error) {
	if strings.Contains(query, "clusterAllReplicas") {
		return nil, c.cluster
	}
	return nil, c.local
}

func TestQueryHistoryFetchErrors(t *testing.T) {
	var exception = func(code chproto.Error) error {
		return &clickhouseproto.Exception{Code: int32(code)}
	}

	for _, tc := range []struct {
		name           string
		cluster, local error
		denied         bool
		keepsCluster   bool
	}{
		{name: "no such cluster, and no grant on the log", cluster: exception(chproto.ErrClusterDoesntExist), local: exception(chproto.ErrAccessDenied), denied: true},
		{name: "no grant to read other replicas, and no log", cluster: exception(chproto.ErrAccessDenied), local: exception(chproto.ErrUnknownTable), denied: true},
		{name: "one replica's log fails otherwise", cluster: exception(chproto.ErrBadGet), local: exception(chproto.ErrMemoryLimitExceeded)},
		{name: "the cluster's logs fail otherwise", cluster: exception(chproto.ErrMemoryLimitExceeded), keepsCluster: true},
		{name: "the connection drops", cluster: io.EOF, keepsCluster: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var h = &queryHistory{conn: &historyConn{cluster: tc.cluster, local: tc.local}, cluster: historyCluster}
			_, err := h.Fetch(t.Context(), []string{"q1"})
			require.Error(t, err)
			require.Equal(t, tc.denied, errors.Is(err, boilerplate.ErrQueryHistoryDenied))
			require.Equal(t, tc.keepsCluster, h.cluster != "")
		})
	}
}
