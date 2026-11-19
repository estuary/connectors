package connector

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	dbConfig "github.com/databricks/databricks-sdk-go/config"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestDatabricksConfig(t *testing.T) {
	var validConfig = config{
		Address:     "db-something.cloud.databricks.com:400",
		CatalogName: "mycatalog",
		HTTPPath:    "/sql/1.0/warehouses/someid",
		Credentials: credentialConfig{
			AuthType:            "PAT",
			PersonalAccessToken: "secret",
		},
		SchemaName: "default",
	}
	require.NoError(t, validConfig.Validate())
	var uri = validConfig.ToURI("my-materialization")
	require.Equal(t, "token:secret@db-something.cloud.databricks.com:400/sql/1.0/warehouses/someid?catalog=mycatalog&maxDownloadThreads=2&query_tags=materialization_name%3Amy-materialization&schema=default&userAgentEntry=Estuary+Technologies+Flow%2F3.0.0+materialize-databricks", uri)

	var noPort = validConfig
	noPort.Address = "db-something.cloud.databricks.com"
	require.NoError(t, noPort.Validate())
	uri = noPort.ToURI("")
	require.Equal(t, "token:secret@db-something.cloud.databricks.com:443/sql/1.0/warehouses/someid?catalog=mycatalog&maxDownloadThreads=2&schema=default&userAgentEntry=Estuary+Technologies+Flow%2F3.0.0+materialize-databricks", uri)

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

	var noSchema = validConfig
	noSchema.SchemaName = ""
	require.Error(t, noSchema.Validate(), "expected validation error")

	// OAuth2 M2M tests
	var validOAuthConfig = config{
		Address:     "db-something.cloud.databricks.com:400",
		CatalogName: "mycatalog",
		HTTPPath:    "/sql/1.0/warehouses/someid",
		Credentials: credentialConfig{
			AuthType:     "OAuth2 M2M",
			ClientID:     "my-client-id",
			ClientSecret: "my-client-secret",
		},
		SchemaName: "default",
	}
	require.NoError(t, validOAuthConfig.Validate())
	var oauthUri = validOAuthConfig.ToURI("")
	require.Equal(t, "db-something.cloud.databricks.com:400/sql/1.0/warehouses/someid?authType=OauthM2M&catalog=mycatalog&clientID=my-client-id&clientSecret=my-client-secret&maxDownloadThreads=2&schema=default&userAgentEntry=Estuary+Technologies+Flow%2F3.0.0+materialize-databricks", oauthUri)

	var noClientID = validOAuthConfig
	noClientID.Credentials.ClientID = ""
	require.Error(t, noClientID.Validate(), "expected validation error for missing client_id")

	var noClientSecret = validOAuthConfig
	noClientSecret.Credentials.ClientSecret = ""
	require.Error(t, noClientSecret.Validate(), "expected validation error for missing client_secret")

	patWs := validConfig.workspaceConfig()
	require.Equal(t, "db-something.cloud.databricks.com:400//sql/1.0/warehouses/someid", patWs.Host)
	require.Equal(t, "secret", patWs.Token)
	require.Empty(t, patWs.ClientID)
	require.Empty(t, patWs.ClientSecret)
	require.IsType(t, dbConfig.PatCredentials{}, patWs.Credentials)

	oauthWs := validOAuthConfig.workspaceConfig()
	require.Equal(t, "db-something.cloud.databricks.com:400//sql/1.0/warehouses/someid", oauthWs.Host)
	require.Equal(t, "my-client-id", oauthWs.ClientID)
	require.Equal(t, "my-client-secret", oauthWs.ClientSecret)
	require.Empty(t, oauthWs.Token)
	require.Nil(t, oauthWs.Credentials)
}

// The driver fails to parse these with an error that quotes the whole
// connection string, so they must not reach it.
func TestValidateRejectsUnparseableAddress(t *testing.T) {
	var pat = credentialConfig{AuthType: PAT_AUTH_TYPE, PersonalAccessToken: "secret1234"}
	var oauth = credentialConfig{AuthType: OAUTH_M2M_AUTH_TYPE, ClientID: "my-client-id", ClientSecret: "secret1234"}

	type testCase struct {
		creds    credentialConfig
		address  string
		httpPath string
	}
	var cases []testCase
	for _, creds := range []credentialConfig{pat, oauth} {
		for _, address := range []string{"example.com:prod", "will@example.com", "example.com/db", "example .com:443"} {
			cases = append(cases, testCase{creds, address, "/sql/1.0/warehouses/someid"})
		}
	}
	// These connection strings start with a scheme, so the driver doesn't add one.
	cases = append(cases,
		testCase{oauth, "https:", "//example.com:prod/sql"},
		testCase{oauth, "http:", "//example.com:prod/sql"},
	)

	for _, tc := range cases {
		var cfg = config{
			Address:     tc.address,
			CatalogName: "mycatalog",
			HTTPPath:    tc.httpPath,
			Credentials: tc.creds,
			SchemaName:  "default",
		}
		_, driverErr := stdsql.Open("databricks", cfg.ToURI(""))
		require.ErrorContains(t, driverErr, "secret1234", tc.address)

		var err = cfg.Validate()
		require.ErrorContains(t, err, "invalid 'address'", tc.address)
		require.NotContains(t, err.Error(), "secret1234", tc.address)
	}
}

func TestSpecification(t *testing.T) {
	var resp, err = NewDriver().
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}
