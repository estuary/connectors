package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestDatabricksConfig(t *testing.T) {
	var validConfig = config{
		Address:  "db-something.cloud.databricks.com:400",
		CatalogName: "mycatalog",
		HTTPPath: "/sql/1.0/warehouses/someid",
		Credentials: credentialConfig{
			AuthType: "PAT",
			PersonalAccessToken: "secret",
		},
	}
	require.NoError(t, validConfig.Validate())
	var uri = validConfig.ToURI()
	require.Equal(t, "token:secret@db-something.cloud.databricks.com:400/sql/1.0/warehouses/someid?catalog=mycatalog&userAgentEntry=Estuary+Technologies+Flow", uri)

	var noPort = validConfig
	noPort.Address = "db-something.cloud.databricks.com"
	require.NoError(t, noPort.Validate())
	uri = noPort.ToURI()
	require.Equal(t, "token:secret@db-something.cloud.databricks.com:443/sql/1.0/warehouses/someid?catalog=mycatalog&userAgentEntry=Estuary+Technologies+Flow", uri)

	var noAddress = validConfig
	noAddress.Address = ""
	require.Error(t, noAddress.Validate(), "expected validation error")

	var noCatalog = validConfig
	noCatalog.CatalogName = ""
	require.Error(t, noCatalog.Validate(), "expected validation error")

	var noPath = validConfig
	noPath.HTTPPath = ""
	require.Error(t, noPath.Validate(), "expected validation error")

	var noPAT = validConfig
	noPAT.Credentials.PersonalAccessToken = ""
	require.Error(t, noPAT.Validate(), "expected validation error")
}

func TestSpecification(t *testing.T) {
	var resp, err = newDatabricksDriver().
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}
