package connector

import (
	"context"
	"fmt"
	"testing"

	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

// clusterTableEngines reads the engine of the named tables on every node of
// the test cluster, keyed by "port/table".
func clusterTableEngines(t *testing.T, ctx context.Context, names ...string) map[string]string {
	t.Helper()
	var out = map[string]string{}
	for _, port := range []int{9101, 9102, 9103, 9104} {
		var conn = clusterConn(t, port)
		for _, name := range names {
			var engine string
			var err = conn.QueryRow(ctx, "SELECT engine FROM system.tables WHERE database = 'flow' AND name = ?", name).Scan(&engine)
			if err == nil {
				out[fmt.Sprintf("%d/%s", port, name)] = engine
			}
		}
	}
	return out
}

func dropClusterTables(t *testing.T, names ...string) {
	t.Helper()
	var conn = clusterConn(t, 9101)
	for _, name := range names {
		require.NoError(t, conn.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS flow.`%s` ON CLUSTER flow_cluster SYNC", name)))
	}
}

func TestClusterApply(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureClusterUp(t)
	var ctx = t.Context()
	var cfg = testClusterConfig()
	cfg.Cluster = "flow_cluster"
	var table = "test_cluster_apply"
	var local = table + "_local"

	dropClusterTables(t, table, local)
	t.Cleanup(func() { dropClusterTables(t, table, local) })

	var engineFull = func(name string) string {
		var s string
		require.NoError(t, clusterConn(t, 9101).QueryRow(ctx, "SELECT engine_full FROM system.tables WHERE database = 'flow' AND name = ?", name).Scan(&s))
		return s
	}

	var specA = specWith(t, cfg, tableConfig{Table: table}, 0)
	_, err := NewDriver().Apply(ctx, &pm.Request_Apply{Materialization: specA, Version: "test"})
	require.NoError(t, err)

	var engines = clusterTableEngines(t, ctx, table, local)
	require.Len(t, engines, 8, "both tables exist on all four nodes: %v", engines)
	for k, e := range engines {
		if k[len(k)-len("_local"):] == "_local" {
			require.Equal(t, "ReplicatedReplacingMergeTree", e, k)
		} else {
			require.Equal(t, "Distributed", e, k)
		}
	}
	require.Contains(t, engineFull(table), "cityHash64(key)")
	require.Contains(t, engineFull(local), "'/clickhouse/tables/{uuid}/{shard}', '{replica}'")

	// Both tables are usable end to end: a row written through the proxy
	// lands on exactly one shard's local table.
	require.NoError(t, clusterConn(t, 9101).Exec(ctx, fmt.Sprintf(
		"INSERT INTO flow.%s (key, flow_published_at, flow_document) SETTINGS distributed_foreground_insert = 1 VALUES ('k1', now64(6), '{}')", table)))
	// A read through the proxy may land on either replica of a shard.
	require.NoError(t, clusterConn(t, 9101).Exec(ctx, fmt.Sprintf("SYSTEM SYNC REPLICA ON CLUSTER flow_cluster flow.%s", local)))
	var total uint64
	require.NoError(t, clusterConn(t, 9101).QueryRow(ctx, fmt.Sprintf("SELECT count() FROM flow.%s", table)).Scan(&total))
	require.Equal(t, uint64(1), total)

	// A partition_by change with a backfill drops and re-creates both tables
	// on every node.
	var specB = specWith(t, cfg, tableConfig{Table: table, PartitionBy: "toYYYYMM(flow_published_at)"}, 1)
	_, err = NewDriver().Apply(ctx, &pm.Request_Apply{Materialization: specB, Version: "test", LastMaterialization: specA, LastVersion: "test"})
	require.NoError(t, err)
	require.Len(t, clusterTableEngines(t, ctx, table, local), 8)
	require.Contains(t, engineFull(local), "PARTITION BY toYYYYMM(flow_published_at)")
	require.NoError(t, clusterConn(t, 9101).QueryRow(ctx, fmt.Sprintf("SELECT count() FROM flow.%s", table)).Scan(&total))
	require.Equal(t, uint64(0), total, "re-created tables are empty")

	// A backfill with no DDL-affecting change truncates the local table on
	// every node and leaves the proxy alone.
	require.NoError(t, clusterConn(t, 9103).Exec(ctx, fmt.Sprintf(
		"INSERT INTO flow.%s (key, flow_published_at, flow_document) VALUES ('k2', now64(6), '{}')", local)))
	var specC = specWith(t, cfg, tableConfig{Table: table, PartitionBy: "toYYYYMM(flow_published_at)"}, 2)
	_, err = NewDriver().Apply(ctx, &pm.Request_Apply{Materialization: specC, Version: "test", LastMaterialization: specB, LastVersion: "test"})
	require.NoError(t, err)
	require.NoError(t, clusterConn(t, 9103).QueryRow(ctx, fmt.Sprintf("SELECT count() FROM flow.%s", local)).Scan(&total))
	require.Equal(t, uint64(0), total)
}

func TestClusterValidateRejectsForeignTables(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureClusterUp(t)
	var ctx = t.Context()
	var cfg = testClusterConfig()
	cfg.Cluster = "flow_cluster"
	var table = "test_cluster_foreign"
	var local = table + "_local"
	dropClusterTables(t, table, local)
	t.Cleanup(func() { dropClusterTables(t, table, local) })

	var conn = clusterConn(t, 9101)
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf("CREATE TABLE flow.%s ON CLUSTER flow_cluster (key String) ENGINE = MergeTree ORDER BY key", table)))
	_, err := NewDriver().Validate(ctx, validateReq(t, cfg, tableConfig{Table: table}, 0, nil))
	require.ErrorContains(t, err, "already exists with engine MergeTree")
	require.ErrorContains(t, err, "Distributed table")
	dropClusterTables(t, table)

	require.NoError(t, conn.Exec(ctx, fmt.Sprintf("CREATE TABLE flow.%s ON CLUSTER flow_cluster (key String) ENGINE = MergeTree ORDER BY key", local)))
	_, err = NewDriver().Validate(ctx, validateReq(t, cfg, tableConfig{Table: table}, 0, nil))
	require.ErrorContains(t, err, "already exists with engine MergeTree")
	require.ErrorContains(t, err, "Replicated table")
}

func TestMustRecreateResourceCluster(t *testing.T) {
	var ep = &sql.Endpoint[config]{Dialect: testDialect}
	var c = &client{ep: ep}
	var bind = func(rc tableConfig) *pf.MaterializationSpec_Binding {
		return &pf.MaterializationSpec_Binding{ResourceConfigJson: mustMarshal(t, rc)}
	}
	var req = func(lastCfg config) *pm.Request_Apply {
		return &pm.Request_Apply{LastMaterialization: &pf.MaterializationSpec{ConfigJson: mustMarshal(t, lastCfg)}}
	}

	var single = testConfig()
	var clustered = single
	clustered.Cluster = "flow_cluster"
	ep.Config = clustered

	must, err := c.MustRecreateResource(req(clustered), bind(tableConfig{Table: "t"}), bind(tableConfig{Table: "t", ShardingKey: []string{"key"}}))
	require.NoError(t, err)
	require.True(t, must, "a changed sharding key re-creates")

	must, err = c.MustRecreateResource(req(clustered), bind(tableConfig{Table: "t", ShardingKey: []string{"a", "b"}}), bind(tableConfig{Table: "t", ShardingKey: []string{"b", "a"}}))
	require.NoError(t, err)
	require.True(t, must, "sharding key order is part of the hash")

	must, err = c.MustRecreateResource(req(single), bind(tableConfig{Table: "t"}), bind(tableConfig{Table: "t"}))
	require.NoError(t, err)
	require.True(t, must, "moving onto a cluster re-creates")

	must, err = c.MustRecreateResource(req(clustered), bind(tableConfig{Table: "t", ShardingKey: []string{"key"}}), bind(tableConfig{Table: "t", ShardingKey: []string{"key"}}))
	require.NoError(t, err)
	require.False(t, must)
}
