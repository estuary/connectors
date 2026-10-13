package connector

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestClickHouseConfig(t *testing.T) {
	validConfig := config{
		Address: "clickhouse.example.com:9000",
		Credentials: credentialConfig{
			AuthType:         UserPass,
			usernamePassword: usernamePassword{Username: "default", Password: "password"},
		},
		Database: "test_db",
	}
	require.NoError(t, validConfig.Validate())

	noAddress := validConfig
	noAddress.Address = ""
	require.Error(t, noAddress.Validate())

	noUser := validConfig
	noUser.Credentials.Username = ""
	require.Error(t, noUser.Validate())

	noPass := validConfig
	noPass.Credentials.Password = ""
	require.Error(t, noPass.Validate())

	noDatabase := validConfig
	noDatabase.Database = ""
	require.Error(t, noDatabase.Validate())

	clustered := validConfig
	clustered.Cluster = "flow_cluster"
	require.NoError(t, clustered.Validate())
	require.True(t, clustered.distributed())
	require.False(t, validConfig.distributed())

	badRefresh := clustered
	badRefresh.Advanced.TopologyRefreshInterval = "soon"
	require.ErrorContains(t, badRefresh.Validate(), "invalid topology_refresh_interval")

	zeroQuorum := clustered
	zeroQuorum.Advanced.InsertQuorumTimeout = "0s"
	require.ErrorContains(t, zeroQuorum.Validate(), "must be positive")
}

func TestClusterConfigDefaults(t *testing.T) {
	var cfg = config{Cluster: "flow_cluster"}
	require.Equal(t, "_local", cfg.localTableSuffix())
	require.Equal(t, 5*time.Minute, cfg.topologyRefreshInterval())
	require.Equal(t, 10*time.Minute, cfg.insertQuorumTimeout())

	cfg.Advanced.LocalTableSuffix = "_shard"
	cfg.Advanced.TopologyRefreshInterval = "90s"
	cfg.Advanced.InsertQuorumTimeout = "1h"
	require.Equal(t, "_shard", cfg.localTableSuffix())
	require.Equal(t, 90*time.Second, cfg.topologyRefreshInterval())
	require.Equal(t, time.Hour, cfg.insertQuorumTimeout())
}

func TestDefaultNamespace(t *testing.T) {
	var cfg config
	require.Equal(t, "", cfg.DefaultNamespace())
}

func TestCredentialValidation(t *testing.T) {
	var invalid = credentialConfig{AuthType: "bogus"}
	require.ErrorContains(t, invalid.Validate(), "invalid credentials auth type")
}

func TestTableConfigValidation(t *testing.T) {
	var empty tableConfig
	require.ErrorContains(t, empty.Validate(), "missing table")

	var valid = tableConfig{Table: "my_table"}
	require.NoError(t, valid.Validate())

	valid = tableConfig{Table: "my_table_@你好-`-\"especiál"}
	require.NoError(t, valid.Validate())

	valid = tableConfig{Table: "my_table", ShardingKey: []string{"tenant_id", "region"}}
	require.NoError(t, valid.Validate())

	var dup = tableConfig{Table: "my_table", ShardingKey: []string{"tenant_id", "tenant_id"}}
	require.ErrorContains(t, dup.Validate(), "more than once")

	var blank = tableConfig{Table: "my_table", ShardingKey: []string{""}}
	require.ErrorContains(t, blank.Validate(), "empty field name")
}

func TestResolvedAddress(t *testing.T) {
	// With port — unchanged.
	var cfg = config{Address: "host:1234"}
	require.Equal(t, "host:1234", cfg.resolvedAddress())

	// Without port — appends :9000.
	cfg.Address = "host"
	require.Equal(t, "host:9440", cfg.resolvedAddress())

	// Without port, without TLS — appends :9440.
	cfg.Advanced.SSLMode = "disable"
	require.Equal(t, "host:9000", cfg.resolvedAddress())
}

func TestAcknowledge(t *testing.T) {
	tr := transactor{ensured: make(chan struct{})}
	state, err := tr.Acknowledge(t.Context(), nil, nil)
	require.NoError(t, err)
	require.Nil(t, state)
}

func TestSpecification(t *testing.T) {
	resp, err := NewDriver().
		Spec(t.Context(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}
