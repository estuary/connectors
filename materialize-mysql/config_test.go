package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestMySQLConfig(t *testing.T) {
	var validConfig = config{
		Address:  "post.toast:1234",
		User:     "youser",
		Password: "shmassword",
		Database: "namegame",
	}
	require.NoError(t, validConfig.Validate())
	var uri = validConfig.ToURI()
	require.Equal(t, "youser:shmassword@tcp(post.toast:1234)/namegame", uri)

	var noPort = validConfig
	noPort.Address = "post.toast"
	require.NoError(t, noPort.Validate())
	uri = noPort.ToURI()
	require.Equal(t, "youser:shmassword@tcp(post.toast:3306)/namegame", uri)

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

	var sslVerifyCA = validConfig
	sslVerifyCA.Advanced.SSLMode = "verify_ca"
	require.Error(t, sslVerifyCA.Validate(), "expected validation error")
	sslVerifyCA.Advanced.SSLServerCA = "some_value"
	require.NoError(t, sslVerifyCA.Validate())

	var sslVerifyIdentity = validConfig
	sslVerifyIdentity.Advanced.SSLMode = "verify_identity"
	require.Error(t, sslVerifyIdentity.Validate(), "expected validation error")
	sslVerifyIdentity.Advanced.SSLServerCA = "some_value"
	require.NoError(t, sslVerifyIdentity.Validate())
}

func TestSpecification(t *testing.T) {
	var resp, err = newMysqlDriver().
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
		"RequireSSL": {
			Address:  "example.com",
			User:     "will",
			Password: "secret1234",
			Database: "somedb",
			Advanced: advancedConfig{
				SSLMode: "preferred",
			},
		},
		"IncorrectSSL": {
			Address:  "example.com",
			User:     "will",
			Password: "secret1234",
			Database: "somedb",
			Advanced: advancedConfig{
				SSLMode: "whoops-this-isnt-right",
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			var valid = "config valid"
			if err := cfg.Validate(); err != nil {
				valid = err.Error()
			}
			var uri = cfg.ToURI()
			cupaloy.SnapshotT(t, fmt.Sprintf("%s\n%s", uri, valid))
		})
	}
}
