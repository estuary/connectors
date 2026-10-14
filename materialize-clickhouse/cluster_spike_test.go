package connector

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-faster/city"
	"github.com/stretchr/testify/require"
)

// chIntHash64 is ClickHouse's IntHash64Impl (a salted intHash64), which
// cityHash64 applies to every plain integer, float, Bool, Date and DateTime
// argument in place of hashing the value's bytes. Narrow types are
// zero-extended to 64 bits first.
func chIntHash64(v uint64) uint64 {
	v ^= 0x4CF2D2BAAE6DA887
	v ^= v >> 33
	v *= 0xff51afd7ed558ccd
	v ^= v >> 33
	v *= 0xc4ceb9fe1a85ec53
	v ^= v >> 33
	return v
}

// chHash128to64 is CityHash's Hash128to64, which cityHash64 uses to fold each
// further argument's hash into the running hash.
func chHash128to64(lo, hi uint64) uint64 {
	const mul = uint64(0x9ddfea08eb382d69)
	a := (lo ^ hi) * mul
	a ^= a >> 47
	b := (hi ^ a) * mul
	b ^= b >> 47
	b *= mul
	return b
}

// chInt128, chInt256 and chDate32 mark a value with the column type it is
// stored in, because the hash depends on the column's width and not on the
// Go value alone.
type chInt128 struct{ *big.Int }
type chInt256 struct{ *big.Int }
type chDate32 struct{ time.Time }

// twosComplementLE renders v as a little-endian two's complement integer of
// the given byte width.
func twosComplementLE(v *big.Int, width int) []byte {
	var n = new(big.Int).Set(v)
	if n.Sign() < 0 {
		n.Add(n, new(big.Int).Lsh(big.NewInt(1), uint(width*8)))
	}
	var out = make([]byte, width)
	var be = n.Bytes()
	for i, b := range be {
		out[len(be)-1-i] = b
	}
	return out
}

// chCityHash64 computes cityHash64(args...) as ClickHouse does for the column
// types this connector materializes keys into: Int64, Int128, Int256, Bool,
// Float64, String, Date32 and DateTime64(6, 'UTC').
func chCityHash64(args ...any) uint64 {
	var one = func(a any) uint64 {
		switch v := a.(type) {
		case int64:
			return chIntHash64(uint64(v))
		case chInt128:
			// Wide integers are not plain PODs to ClickHouse, so they hash
			// their little-endian bytes like a string would.
			return city.CH64(twosComplementLE(v.Int, 16))
		case chInt256:
			return city.CH64(twosComplementLE(v.Int, 32))
		case chDate32:
			// Date32 is an Int32 day count, zero-extended to 64 bits.
			var days = int32(v.Unix() / 86400)
			return chIntHash64(uint64(uint32(days)))
		case bool:
			if v {
				return chIntHash64(1)
			}
			return chIntHash64(0)
		case float64:
			return chIntHash64(math.Float64bits(v))
		case string:
			return city.CH64([]byte(v))
		case time.Time:
			// DateTime64 is a decimal type, hashed as the little-endian bytes
			// of its Int64 tick count rather than through intHash64.
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], uint64(v.UnixMicro()))
			return city.CH64(buf[:])
		default:
			panic(fmt.Sprintf("unsupported key type %T", a))
		}
	}
	var h = one(args[0])
	for _, a := range args[1:] {
		h = chHash128to64(h, one(a))
	}
	return h
}

// chShardFor walks cumulative shard weights the way a Distributed table does:
// the hash modulo the total weight selects a slot, and slots are dealt to
// shards in order, `weight` apiece. Returns the 1-based shard number.
func chShardFor(hash uint64, weights []uint32) int {
	var total uint64
	for _, w := range weights {
		total += uint64(w)
	}
	if total == 0 {
		panic("every shard has weight 0")
	}
	var slot = hash % total
	for i, w := range weights {
		if slot < uint64(w) {
			return i + 1
		}
		slot -= uint64(w)
	}
	panic("unreachable")
}

func bigFromString(s string) *big.Int {
	var n, ok = new(big.Int).SetString(s, 10)
	if !ok {
		panic(s)
	}
	return n
}

func clusterConn(t *testing.T, port int) chdriver.Conn {
	t.Helper()
	var cfg = testClusterConfig()
	cfg.Address = fmt.Sprintf("127.0.0.1:%d", port)
	conn, err := clickhouse.Open(cfg.newClickhouseOptions())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

func execCluster(t *testing.T, conn chdriver.Conn, q string) {
	t.Helper()
	require.NoError(t, conn.Exec(context.Background(), q), q)
}

// Spike 2 from issue #5077: Go placement must match ClickHouse placement.
func TestClusterSpikePlacement(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureClusterUp(t)
	var ctx = context.Background()
	var conn = clusterConn(t, 9101)

	var ts = time.Date(2020, 1, 1, 0, 0, 0, 123456000, time.UTC)
	var long = strings.Repeat("abcdefghij", 13) // 130 bytes, past every short-input branch

	// Per-type hash agreement, computed by ClickHouse on typed literals.
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
		{"cityHash64(toDate32('2020-01-01'))", []any{chDate32{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)}}},
		{"cityHash64(toDate32('1960-01-01'))", []any{chDate32{time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)}}},
		{"cityHash64(toInt128(1))", []any{chInt128{big.NewInt(1)}}},
		{"cityHash64(toInt128(-1))", []any{chInt128{big.NewInt(-1)}}},
		{"cityHash64(toInt128('170141183460469231731687303715884105727'))", []any{chInt128{bigFromString("170141183460469231731687303715884105727")}}},
		{"cityHash64(toInt256(1))", []any{chInt256{big.NewInt(1)}}},
		{"cityHash64(toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968'))", []any{chInt256{bigFromString("-57896044618658097711785492504343953926634992332820282019728792003956564819968")}}},
		{"cityHash64(toInt128(7), 'x', toDate32('2020-01-01'))", []any{chInt128{big.NewInt(7)}, "x", chDate32{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)}}},
		{"cityHash64('a', 'b')", []any{"a", "b"}},
		{"cityHash64(toInt64(1), 'b')", []any{int64(1), "b"}},
		{"cityHash64('b', toInt64(1))", []any{"b", int64(1)}},
		{"cityHash64(toInt64(1), toInt64(2), toInt64(3))", []any{int64(1), int64(2), int64(3)}},
		{"cityHash64('t', toInt64(7), true, toFloat64(2.5), toDateTime64('2020-01-01 00:00:00.123456', 6, 'UTC'))", []any{"t", int64(7), true, 2.5, ts}},
	} {
		var want uint64
		require.NoError(t, conn.QueryRow(ctx, "SELECT "+tc.sql).Scan(&want), tc.sql)
		require.Equal(t, want, chCityHash64(tc.args...), tc.sql)
	}

	// Placement agreement: insert through a Distributed table with a
	// multi-column sharding key and read back which shard took each row.
	for _, cl := range []struct {
		cluster string
		weights []uint32
	}{
		{"flow_cluster", []uint32{1, 1}},
		{"flow_cluster_weighted", []uint32{1, 3}},
	} {
		var local = "spk_place_" + cl.cluster + "_local"
		var dist = "spk_place_" + cl.cluster
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
			var want = chShardFor(chCityHash64(tenant, n, ok, f, got), cl.weights)
			require.Equal(t, want, int(shard), "row %q %d placed on shard %d, Go says %d", tenant, n, shard, want)
			perShard[shard]++
			seen++
		}
		require.Equal(t, len(rows), seen)
		require.Len(t, perShard, 2, "both shards received rows: %v", perShard)
		t.Logf("%s: rows per shard %v", cl.cluster, perShard)
	}
}

// Spike 1 from issue #5077: MOVE PARTITION ... TO TABLE between two
// ReplicatedMergeTree tables is replicated to the other replica of the shard,
// and the moved rows are visible there without any sequential-consistency
// setting after SYSTEM SYNC REPLICA.
func TestClusterSpikeMovePartitionReplicates(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureClusterUp(t)
	var ctx = context.Background()
	var r1 = clusterConn(t, 9101)
	var r2 = clusterConn(t, 9102)

	for _, tbl := range []string{"spk_mv_src", "spk_mv_dst"} {
		execCluster(t, r1, "DROP TABLE IF EXISTS flow."+tbl+" ON CLUSTER flow_cluster SYNC")
		execCluster(t, r1, `CREATE TABLE flow.`+tbl+` ON CLUSTER flow_cluster (k Int64, v String)
			ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/`+tbl+`', '{replica}')
			PARTITION BY k % 2 ORDER BY k`)
	}
	// Rows inserted on r2, then moved from r1, so the move operates on a part
	// r1 only has by replication.
	execCluster(t, r2, "INSERT INTO flow.spk_mv_src VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')")
	execCluster(t, r1, "SYSTEM SYNC REPLICA flow.spk_mv_src")
	execCluster(t, r1, "ALTER TABLE flow.spk_mv_src MOVE PARTITION 1 TO TABLE flow.spk_mv_dst")
	execCluster(t, r2, "SYSTEM SYNC REPLICA flow.spk_mv_src")
	execCluster(t, r2, "SYSTEM SYNC REPLICA flow.spk_mv_dst")

	var count = func(c chdriver.Conn, tbl string) uint64 {
		var n uint64
		require.NoError(t, c.QueryRow(ctx, "SELECT count() FROM flow."+tbl).Scan(&n))
		return n
	}
	require.Equal(t, uint64(2), count(r1, "spk_mv_src"))
	require.Equal(t, uint64(2), count(r2, "spk_mv_src"))
	require.Equal(t, uint64(2), count(r1, "spk_mv_dst"))
	require.Equal(t, uint64(2), count(r2, "spk_mv_dst"))

	// A second move of the same (now empty) partition is a no-op, not an error.
	execCluster(t, r1, "ALTER TABLE flow.spk_mv_src MOVE PARTITION 1 TO TABLE flow.spk_mv_dst")
	require.Equal(t, uint64(2), count(r2, "spk_mv_dst"))

	// Cross-family moves are rejected, so store tables must be Replicated*.
	execCluster(t, r1, "DROP TABLE IF EXISTS flow.spk_mv_plain SYNC")
	execCluster(t, r1, "CREATE TABLE flow.spk_mv_plain (k Int64, v String) ENGINE = MergeTree PARTITION BY k % 2 ORDER BY k")
	execCluster(t, r1, "INSERT INTO flow.spk_mv_plain VALUES (5,'e')")
	var err = r1.Exec(ctx, "ALTER TABLE flow.spk_mv_plain MOVE PARTITION 1 TO TABLE flow.spk_mv_dst")
	require.Error(t, err)
	t.Logf("cross-family move error: %v", err)
}

// Spike 3 from issue #5077: insert_quorum = 'auto' with
// insert_quorum_parallel = 0 makes a write on one replica visible to a
// select_sequential_consistency = 1 read on the other replica immediately.
func TestClusterSpikeQuorumSequentialConsistency(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureClusterUp(t)
	var ctx = context.Background()
	var r1 = clusterConn(t, 9101)
	var r2 = clusterConn(t, 9102)

	execCluster(t, r1, "DROP TABLE IF EXISTS flow.spk_q ON CLUSTER flow_cluster SYNC")
	execCluster(t, r1, `CREATE TABLE flow.spk_q ON CLUSTER flow_cluster (k Int64)
		ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/spk_q', '{replica}') ORDER BY k`)

	var quorum = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"insert_quorum":          "auto",
		"insert_quorum_parallel": 0,
		"insert_quorum_timeout":  10000,
	}))
	var seqRead = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"select_sequential_consistency": 1,
	}))
	var countOn = func(c chdriver.Conn, c2 context.Context) uint64 {
		var n uint64
		require.NoError(t, c.QueryRow(c2, "SELECT count() FROM flow.spk_q").Scan(&n))
		return n
	}

	// Stop r2 fetching so a non-quorum insert is provably invisible there, and a
	// quorum insert blocks until r2 catches up.
	execCluster(t, r2, "SYSTEM STOP FETCHES flow.spk_q")
	execCluster(t, r1, "INSERT INTO flow.spk_q VALUES (1)")
	require.Equal(t, uint64(0), countOn(r2, ctx), "plain insert acks before r2 has the part")

	// Quorum insert while r2 cannot fetch: must time out rather than ack.
	var err = r1.Exec(quorum, "INSERT INTO flow.spk_q VALUES (2)")
	require.Error(t, err)
	t.Logf("quorum insert with a replica stalled: %v", err)

	// A sequential-consistency read sees that a quorum is pending/failed and
	// refuses to serve stale data rather than returning it.
	_, err = r2.Query(seqRead, "SELECT count() FROM flow.spk_q")
	t.Logf("sequential read on stalled replica: err=%v", err)

	execCluster(t, r2, "SYSTEM START FETCHES flow.spk_q")
	execCluster(t, r2, "SYSTEM SYNC REPLICA flow.spk_q")

	// With both replicas healthy, a quorum insert on r1 is immediately visible to
	// a sequential-consistency read on r2.
	for i := int64(10); i < 20; i++ {
		require.NoError(t, r1.Exec(quorum, fmt.Sprintf("INSERT INTO flow.spk_q VALUES (%d)", i)))
		var n uint64
		require.NoError(t, r2.QueryRow(seqRead, "SELECT count() FROM flow.spk_q WHERE k = ?", i).Scan(&n))
		require.Equal(t, uint64(1), n, "quorum insert of %d not visible on r2", i)
	}
	t.Logf("final count r1=%d r2=%d", countOn(r1, ctx), countOn(r2, seqRead))
}
