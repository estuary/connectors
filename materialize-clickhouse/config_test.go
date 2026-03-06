package main

import (
	"context"
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
			AuthType: PASSWORD_AUTH_TYPE,
			User:     "default",
			Password: "password",
		},
		Database: "test_db",
	}
	require.NoError(t, validConfig.Validate())

	noAddress := validConfig
	noAddress.Address = ""
	require.Error(t, noAddress.Validate())

	noUser := validConfig
	noUser.Credentials.User = ""
	require.Error(t, noUser.Validate())

	noPass := validConfig
	noPass.Credentials.Password = ""
	require.Error(t, noPass.Validate())

	noDatabase := validConfig
	noDatabase.Database = ""
	require.Error(t, noDatabase.Validate())
}

func TestSpecification(t *testing.T) {
	resp, err := newClickHouseDriver().
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}
