package connector

import (
	"context"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
)

// testClusterConfig addresses the first replica of the first shard of the
// test cluster. Every other node is reachable at the host:port pairs the
// cluster reports in system.clusters.
func testClusterConfig() config {
	var cfg = testConfig()
	cfg.Address = "localhost:9101"
	return cfg
}

// TestClusterTopology pins the shape of the docker-compose cluster that the
// distributed-table tests depend on: two shards of two replicas each, every
// member reachable from the test process at the address the cluster reports.
func TestClusterTopology(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureClusterUp(t)
	var ctx = context.Background()

	conn, err := clickhouse.Open(testClusterConfig().newClickhouseOptions())
	require.NoError(t, err)
	defer conn.Close()

	rows, err := conn.Query(ctx, `
		SELECT shard_num, replica_num, host_address, port
		FROM system.clusters WHERE cluster = 'flow_cluster'
		ORDER BY shard_num, replica_num`)
	require.NoError(t, err)

	type member struct {
		shard, replica uint32
		host           string
		port           uint16
	}
	var members []member
	for rows.Next() {
		var m member
		require.NoError(t, rows.Scan(&m.shard, &m.replica, &m.host, &m.port))
		members = append(members, m)
	}
	require.Equal(t, []member{
		{1, 1, "127.0.0.1", 9101},
		{1, 2, "127.0.0.1", 9102},
		{2, 1, "127.0.0.1", 9103},
		{2, 2, "127.0.0.1", 9104},
	}, members)

	for _, m := range members {
		var cfg = testClusterConfig()
		cfg.Address = fmt.Sprintf("%s:%d", m.host, m.port)
		node, err := clickhouse.Open(cfg.newClickhouseOptions())
		require.NoError(t, err)

		var shard, replica string
		require.NoError(t, node.QueryRow(ctx, "SELECT getMacro('shard'), getMacro('replica')").Scan(&shard, &replica))
		require.Equal(t, fmt.Sprint(m.shard), shard)
		require.Equal(t, fmt.Sprintf("r%d", m.replica), replica)
		node.Close()
	}
}
