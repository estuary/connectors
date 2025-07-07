package main

import (
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	snowflake_auth "github.com/estuary/connectors/go/auth/snowflake"
	"github.com/stretchr/testify/require"
)

func TestConfigURI(t *testing.T) {
	for name, cfg := range map[string]config{
		"User & Password Authentication": {
			Host:     "orgname-accountname.snowflakecomputing.com",
			Database: "mydb",
			Credentials: &snowflake_auth.CredentialConfig{
				AuthType:   snowflake_auth.UserPass,
				User:       "will",
				Password:   "some+complex/password",
				PrivateKey: "non-existant-jwt",
			},
		},
		"Optional Parameters": {
			Host:      "orgname-accountname.snowflakecomputing.com",
			Database:  "mydb",
			Warehouse: "mywarehouse",
			Role:      "myrole",
			Credentials: &snowflake_auth.CredentialConfig{
				AuthType:   snowflake_auth.UserPass,
				User:       "alex",
				Password:   "some+complex/password",
				PrivateKey: "non-existant-jwt",
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, cfg.Validate())
			uri, err := cfg.ToURI()
			require.NoError(t, err)
			cupaloy.SnapshotT(t, uri)
		})
	}
}
