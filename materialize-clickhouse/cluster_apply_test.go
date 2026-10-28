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

	// A store table created AS the local table gets its own Keeper path from
	// the {uuid} macro, and a partition moved from it lands in the local
	// table on both replicas of the shard.
	var storeTable = "flow_temp_store_0_" + local
	dropClusterTables(t, storeTable)
	t.Cleanup(func() { dropClusterTables(t, storeTable) })
	var tpls = renderTemplates(clickHouseDialect(cfg.Database), cfg.HardDelete, cfg.clusterDDL())
	var bindingTable = localTable(buildTestTableFromSpec(t, specA, cfg), clickHouseDialect(cfg.Database), cfg.clusterDDL())
	createStore, err := renderTableAndRangeKey(bindingTable, 0, tpls.createStoreTable)
	require.NoError(t, err)
	require.NoError(t, clusterConn(t, 9103).Exec(ctx, createStore))
	var zkPath = func(port int, name string) string {
		var p string
		require.NoError(t, clusterConn(t, port).QueryRow(ctx, "SELECT zookeeper_path FROM system.replicas WHERE database = 'flow' AND table = ?", name).Scan(&p))
		return p
	}
	require.NotEqual(t, zkPath(9103, local), zkPath(9103, storeTable), "store table has its own replication path")
	require.Equal(t, zkPath(9103, storeTable), zkPath(9104, storeTable), "both replicas of a shard share the store table's path")
	require.NoError(t, clusterConn(t, 9103).Exec(ctx, fmt.Sprintf(
		"INSERT INTO flow.%s (key, flow_published_at, flow_document) VALUES ('k-store', now64(6), '{}')", storeTable)))
	movePartition, err := renderTableAndRangeKey(bindingTable, 0, tpls.moveStorePartition)
	require.NoError(t, err)
	require.NoError(t, clusterConn(t, 9103).Exec(ctx, movePartition, "all"))
	require.NoError(t, clusterConn(t, 9104).Exec(ctx, fmt.Sprintf("SYSTEM SYNC REPLICA flow.%s", local)))
	require.NoError(t, clusterConn(t, 9104).QueryRow(ctx, fmt.Sprintf("SELECT count() FROM flow.%s WHERE key = 'k-store'", local)).Scan(&total))
	require.Equal(t, uint64(1), total, "moved partition is visible on the other replica")

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
	var clustered = testConfig()
	clustered.Cluster = "flow_cluster"
	ep.Config = clustered

	must, err := c.MustRecreateResource(nil, bind(tableConfig{Table: "t"}), bind(tableConfig{Table: "t", ShardingKey: []string{"key"}}))
	require.NoError(t, err)
	require.True(t, must, "a changed sharding key re-creates")

	must, err = c.MustRecreateResource(nil, bind(tableConfig{Table: "t", ShardingKey: []string{"a", "b"}}), bind(tableConfig{Table: "t", ShardingKey: []string{"b", "a"}}))
	require.NoError(t, err)
	require.True(t, must, "sharding key order is part of the hash")

	must, err = c.MustRecreateResource(nil, bind(tableConfig{Table: "t", ShardingKey: []string{"key"}}), bind(tableConfig{Table: "t", ShardingKey: []string{"key"}}))
	require.NoError(t, err)
	require.False(t, must)
}

func TestClusterDropAndTruncateStatements(t *testing.T) {
	var single = testConfig()
	var clustered = single
	clustered.Cluster = "flow_cluster"

	var c = &client{ep: &sql.Endpoint[config]{Dialect: testDialect, Config: single}}
	require.Equal(t, []string{"DROP TABLE IF EXISTS t;"}, c.dropStatements([]string{"t"}))
	require.Equal(t, []string{"TRUNCATE TABLE t SYNC;"}, c.truncateStatements([]string{"t"}))

	c.ep.Config = clustered
	require.Equal(t, []string{
		"DROP TABLE IF EXISTS t ON CLUSTER flow_cluster SYNC;",
		"DROP TABLE IF EXISTS t_local ON CLUSTER flow_cluster SYNC;",
	}, c.dropStatements([]string{"t"}))
	require.Equal(t, []string{"TRUNCATE TABLE t_local ON CLUSTER flow_cluster SYNC;"}, c.truncateStatements([]string{"t"}))
}

// buildTestTableFromSpec resolves the first binding of a spec into the table
// the transactor would be handed for it.
func buildTestTableFromSpec(t *testing.T, spec *pf.MaterializationSpec, cfg config) sql.Table {
	t.Helper()
	var b = spec.Bindings[0]
	var shape = sql.BuildTableShape(spec.Name.String(), b, 0, b.ResourcePath, b.DeltaUpdates)
	table, err := sql.ResolveTable(shape, clickHouseDialect(cfg.Database))
	require.NoError(t, err)
	return table
}
