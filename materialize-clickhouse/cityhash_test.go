package connector

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"
)

func bigFromString(s string) *big.Int {
	var n, ok = new(big.Int).SetString(s, 10)
	if !ok {
		panic(s)
	}
	return n
}

func execCluster(t *testing.T, conn chdriver.Conn, q string) {
	t.Helper()
	require.NoError(t, conn.Exec(context.Background(), q), q)
}

// TestCityHashMatchesClickHouse is the guard against silent misplacement:
// the connector's cityHash64 and weight walk must agree with ClickHouse's
// own, hash for hash on typed literals and row for row through a Distributed
// table, on both an equal-weight and a 1:3 weighted cluster.
func TestCityHashMatchesClickHouse(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureClusterUp(t)
	var ctx = context.Background()
	var conn = clusterConn(t, 9101)

	var ts = time.Date(2020, 1, 1, 0, 0, 0, 123456000, time.UTC)
	var long = strings.Repeat("abcdefghij", 13) // 130 bytes, past every short-input branch

	for _, tc := range []struct {
		sql  string
		args []any
	}{
		{"cityHash64(toInt64(1))", []any{int64(1)}},
		{"cityHash64(toInt64(0))", []any{int64(0)}},
		{"cityHash64(toInt64(-1))", []any{int64(-1)}},
		{"cityHash64(toInt64(9223372036854775807))", []any{int64(math.MaxInt64)}},
		{"cityHash64(toInt64(-9223372036854775808))", []any{int64(math.MinInt64)}},
		{"cityHash64(true)", []any{true}},
		{"cityHash64(false)", []any{false}},
		{"cityHash64(toFloat64(1.5))", []any{1.5}},
		{"cityHash64(toFloat64(-0.0))", []any{math.Copysign(0, -1)}},
		{"cityHash64('')", []any{""}},
		{"cityHash64('a')", []any{"a"}},
		{"cityHash64('tenant-00017')", []any{"tenant-00017"}},
		{"cityHash64('" + long + "')", []any{long}},
		{"cityHash64(toDateTime64('2020-01-01 00:00:00.123456', 6, 'UTC'))", []any{ts}},
		{"cityHash64(toDate32('2020-01-01'))", []any{date32{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)}}},
		{"cityHash64(toDate32('1960-01-01'))", []any{date32{time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)}}},
		{"cityHash64(toInt128(1))", []any{int128{big.NewInt(1)}}},
		{"cityHash64(toInt128(-1))", []any{int128{big.NewInt(-1)}}},
		{"cityHash64(toInt128('170141183460469231731687303715884105727'))", []any{int128{bigFromString("170141183460469231731687303715884105727")}}},
		{"cityHash64(toInt256(1))", []any{int256{big.NewInt(1)}}},
		{"cityHash64(toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968'))", []any{int256{bigFromString("-57896044618658097711785492504343953926634992332820282019728792003956564819968")}}},
		{"cityHash64(toInt128(7), 'x', toDate32('2020-01-01'))", []any{int128{big.NewInt(7)}, "x", date32{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)}}},
		{"cityHash64('a', 'b')", []any{"a", "b"}},
		{"cityHash64(toInt64(1), 'b')", []any{int64(1), "b"}},
		{"cityHash64('b', toInt64(1))", []any{"b", int64(1)}},
		{"cityHash64(toInt64(1), toInt64(2), toInt64(3))", []any{int64(1), int64(2), int64(3)}},
		{"cityHash64('t', toInt64(7), true, toFloat64(2.5), toDateTime64('2020-01-01 00:00:00.123456', 6, 'UTC'))", []any{"t", int64(7), true, 2.5, ts}},
	} {
		var want uint64
		require.NoError(t, conn.QueryRow(ctx, "SELECT "+tc.sql).Scan(&want), tc.sql)
		require.Equal(t, want, cityHash64(tc.args...), tc.sql)
	}

	// Insert through a Distributed table with a multi-column sharding key and
	// read back which shard took each row.
	for _, cl := range []struct {
		cluster string
		weights []uint32
	}{
		{"flow_cluster", []uint32{1, 1}},
		{"flow_cluster_weighted", []uint32{1, 3}},
	} {
		var local = "cityhash_" + cl.cluster + "_local"
		var dist = "cityhash_" + cl.cluster
		execCluster(t, conn, "DROP TABLE IF EXISTS flow."+dist+" ON CLUSTER flow_cluster SYNC")
		execCluster(t, conn, "DROP TABLE IF EXISTS flow."+local+" ON CLUSTER flow_cluster SYNC")
		execCluster(t, conn, `CREATE TABLE flow.`+local+` ON CLUSTER flow_cluster
			(tenant String, n Int64, ok Bool, f Float64, ts DateTime64(6, 'UTC'))
			ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/`+local+`', '{replica}') ORDER BY (tenant, n)`)
		execCluster(t, conn, `CREATE TABLE flow.`+dist+` ON CLUSTER flow_cluster AS flow.`+local+`
			ENGINE = Distributed(`+cl.cluster+`, flow, `+local+`, cityHash64(tenant, n, ok, f, ts))`)

		var rows = make([][]any, 0, 200)
		for i := 0; i < 200; i++ {
			rows = append(rows, []any{
				fmt.Sprintf("tenant-%05d", i%37), int64(i*7919 - 100), i%3 == 0,
				float64(i) / 8, ts.Add(time.Duration(i) * time.Microsecond),
			})
		}
		batch, err := conn.PrepareBatch(clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
			"distributed_foreground_insert": 1,
		})), "INSERT INTO flow."+dist)
		require.NoError(t, err)
		for _, r := range rows {
			require.NoError(t, batch.Append(r...))
		}
		require.NoError(t, batch.Send())
		// The read below may land on either replica of a shard, so wait for
		// the insert to replicate.
		execCluster(t, conn, "SYSTEM SYNC REPLICA ON CLUSTER flow_cluster flow."+local)

		res, err := conn.Query(ctx, "SELECT tenant, n, ok, f, ts, _shard_num FROM flow."+dist)
		require.NoError(t, err)
		var seen int
		var perShard = map[uint32]int{}
		for res.Next() {
			var tenant string
			var n int64
			var ok bool
			var f float64
			var got time.Time
			var shard uint32
			require.NoError(t, res.Scan(&tenant, &n, &ok, &f, &got, &shard))
			var want = shardFor(cityHash64(tenant, n, ok, f, got), cl.weights)
			require.Equal(t, want, int(shard), "row %q %d placed on shard %d, Go says %d", tenant, n, shard, want)
			perShard[shard]++
			seen++
		}
		require.Equal(t, len(rows), seen)
		require.Len(t, perShard, 2, "both shards received rows: %v", perShard)
		t.Logf("%s: rows per shard %v", cl.cluster, perShard)
	}
}
