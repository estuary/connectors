package connector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTopologyPure(t *testing.T) {
	var topo = &topology{Cluster: "c", Replicas: []replica{
		{Shard: 1, Replica: 1, Weight: 1, Host: "a", Port: 1, Errors: 3},
		{Shard: 1, Replica: 2, Weight: 1, Host: "b", Port: 1},
		{Shard: 2, Replica: 1, Weight: 3, Host: "c", Port: 1},
		{Shard: 2, Replica: 2, Weight: 3, Host: "d", Port: 1},
	}}
	require.Equal(t, 2, topo.shardCount())
	require.Equal(t, []uint32{1, 3}, topo.weights())

	var picked = topo.pickReplicas()
	require.Len(t, picked, 2)
	require.Equal(t, "b:1", picked[0].address(), "a replica with errors is passed over")
	require.Equal(t, "c:1", picked[1].address())

	var same = *topo
	same.Replicas = append([]replica(nil), topo.Replicas...)
	same.Replicas[0].Errors = 0
	require.True(t, topo.equal(&same), "error counts do not change placement")

	var reweighted = *topo
	reweighted.Replicas = append([]replica(nil), topo.Replicas...)
	reweighted.Replicas[2].Weight = 1
	reweighted.Replicas[3].Weight = 1
	require.False(t, topo.equal(&reweighted))

	var grown = *topo
	grown.Replicas = append(append([]replica(nil), topo.Replicas...), replica{Shard: 3, Replica: 1, Weight: 1, Host: "e", Port: 1})
	require.False(t, topo.equal(&grown))
	require.Equal(t, 3, grown.shardCount())

	var moved = *topo
	moved.Replicas = append([]replica(nil), topo.Replicas...)
	moved.Replicas[1].Host = "b2"
	moved.Replicas[1].Port = 2
	require.True(t, topo.equal(&moved), "a replica's address does not change placement")
}

func TestTopologyCluster(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureClusterUp(t)
	var ctx = t.Context()
	var cfg = testClusterConfig()
	cfg.Cluster = "flow_cluster"
	var conn = clusterConn(t, 9101)

	topo, err := readTopology(ctx, conn, "flow_cluster")
	require.NoError(t, err)
	require.Equal(t, 2, topo.shardCount())
	require.Equal(t, []uint32{1, 1}, topo.weights())
	require.Len(t, topo.Replicas, 4)

	weighted, err := readTopology(ctx, conn, "flow_cluster_weighted")
	require.NoError(t, err)
	require.Equal(t, []uint32{1, 3}, weighted.weights())
	require.False(t, topo.equal(weighted))

	_, err = readTopology(ctx, conn, "no_such_cluster")
	require.ErrorContains(t, err, "not defined")

	t.Run("opens one verified connection pair per shard", func(t *testing.T) {
		shards, err := openShardConns(ctx, cfg, topo)
		require.NoError(t, err)
		defer func() {
			for _, s := range shards {
				s.close()
			}
		}()
		require.Len(t, shards, 2)
		require.Equal(t, "127.0.0.1:9101", shards[0].replica.address())
		require.Equal(t, "127.0.0.1:9103", shards[1].replica.address())

		// The store pool carries the quorum settings on every query.
		var quorum, parallel, timeout string
		require.NoError(t, shards[0].store.QueryRow(ctx,
			"SELECT (SELECT value FROM system.settings WHERE name = 'insert_quorum'), (SELECT value FROM system.settings WHERE name = 'insert_quorum_parallel'), (SELECT value FROM system.settings WHERE name = 'insert_quorum_timeout')",
		).Scan(&quorum, &parallel, &timeout))
		require.Equal(t, "auto", quorum)
		require.Equal(t, "0", parallel)
		require.Equal(t, "600000", timeout)

		// The load pool does not.
		require.NoError(t, shards[0].load.QueryRow(ctx, "SELECT value FROM system.settings WHERE name = 'insert_quorum'").Scan(&quorum))
		require.Equal(t, "0", quorum)
	})

	t.Run("a transposed address fails at open", func(t *testing.T) {
		var wrong = *topo
		wrong.Replicas = append([]replica(nil), topo.Replicas...)
		// Claim shard 1's first replica lives where shard 2's does.
		wrong.Replicas[0].Port = 9103
		_, err := openShardConns(ctx, cfg, &wrong)
		require.ErrorContains(t, err, "does not identify as shard 1 replica 1")
		require.ErrorContains(t, err, "s2/r1")
	})

	t.Run("refresh keeps a stable layout and fails a changed one", func(t *testing.T) {
		var tr = &transactor{cfg: cfg}
		tr.store.conn = conn
		tr.cfg.Advanced.TopologyRefreshInterval = "1ms"

		// No topology means single-node mode: nothing to refresh.
		require.NoError(t, tr.refreshTopology(ctx))

		stale, err := readTopology(ctx, conn, "flow_cluster")
		require.NoError(t, err)
		stale.ReadAt = time.Now().Add(-time.Hour)
		tr.topology = stale
		require.NoError(t, tr.refreshTopology(ctx))
		require.WithinDuration(t, time.Now(), tr.topology.ReadAt, time.Minute, "an unchanged layout refreshes the read time")

		// Not yet due: no query, no change.
		tr.cfg.Advanced.TopologyRefreshInterval = "1h"
		tr.topology.Replicas[0].Weight = 99
		tr.topology.Replicas[1].Weight = 99
		require.NoError(t, tr.refreshTopology(ctx))

		// Due, and the stored layout disagrees with the server.
		tr.topology.ReadAt = time.Now().Add(-2 * time.Hour)
		err = tr.refreshTopology(ctx)
		require.ErrorContains(t, err, "topology changed")
		require.ErrorContains(t, err, "[99 1] to [1 1]")
	})
}
