package main

import (
	"encoding/json"
	"testing"

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
}

func TestResolvedAddress(t *testing.T) {
	// With port — unchanged.
	var cfg = config{Address: "host:1234"}
	require.Equal(t, "host:1234", cfg.resolvedAddress())

	// Without port — appends :9000.
	cfg.Address = "host"
	require.Equal(t, "host:9000", cfg.resolvedAddress())
}

func TestAcknowledge(t *testing.T) {
	var tr transactor
	state, err := tr.Acknowledge(t.Context())
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(state.UpdatedJson))
	require.True(t, state.MergePatch)
}

func TestSpecification(t *testing.T) {
	resp, err := newClickHouseDriver().
		Spec(t.Context(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}
