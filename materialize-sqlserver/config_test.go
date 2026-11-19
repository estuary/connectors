package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	var validConfig = config{
		Address:  "post.toast:1234",
		User:     "youser",
		Password: "shmassword",
		Database: "namegame",
	}
	require.NoError(t, validConfig.Validate())
	uri, err := validConfig.ToURI()
	require.NoError(t, err)
	require.Equal(t, "sqlserver://youser:shmassword@post.toast:1234?TrustServerCertificate=true&app+name=Flow+Materialization+Connector&database=namegame&encrypt=true", uri.String())

	var noPort = validConfig
	noPort.Address = "post.toast"
	require.NoError(t, noPort.Validate())
	uri, err = noPort.ToURI()
	require.NoError(t, err)
	require.Equal(t, "sqlserver://youser:shmassword@post.toast:1433?TrustServerCertificate=true&app+name=Flow+Materialization+Connector&database=namegame&encrypt=true", uri.String())

	var noAddress = validConfig
	noAddress.Address = ""
	require.Error(t, noAddress.Validate(), "expected validation error")

	var noUser = validConfig
	noUser.User = ""
	require.Error(t, noUser.Validate(), "expected validation error")

	var noPass = validConfig
	noPass.Password = ""
	require.Error(t, noPass.Validate(), "expected validation error")

	var noDatabase = validConfig
	noDatabase.Database = ""
	require.Error(t, noDatabase.Validate(), "expected validation error")
}

// The driver fails to parse these with an error that quotes the whole
// connection string, so they must not reach it.
func TestValidateRejectsUnparseableAddress(t *testing.T) {
	for _, address := range []string{"example.com:prod", "will@example.com", "example.com/db", "example .com:1433"} {
		var cfg = config{
			Address:  address,
			User:     "will",
			Password: "secret1234",
			Database: "namegame",
		}
		_, connectorErr := cfg.ToSQLConnector(context.Background())
		for _, err := range []error{connectorErr, cfg.Validate()} {
			require.ErrorContains(t, err, "invalid 'address'", address)
			require.NotContains(t, err.Error(), "secret1234", address)
		}
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

func TestConfigURI(t *testing.T) {
	for name, cfg := range map[string]config{
		"Basic": {
			Address:  "example.com",
			User:     "will",
			Password: "secret1234",
			Database: "somedb",
		},
	} {
		t.Run(name, func(t *testing.T) {
			var valid = "config valid"
			if err := cfg.Validate(); err != nil {
				valid = err.Error()
			}
			uri, err := cfg.ToURI()
			require.NoError(t, err)
			cupaloy.SnapshotT(t, fmt.Sprintf("%s\n%s", uri, valid))
		})
	}
}
