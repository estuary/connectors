package main

import (
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

func TestConfigURI(t *testing.T) {
	for name, cfg := range map[string]config{
		"v1 User & Password Authentication": {
			Host: "orgname-accountname.snowflakecomputing.com",
			User: "alex",
			Password: "mysecret",
			Database: "mydb",
			Schema: "myschema",
		},
		"Optional Parameters": {
			Host: "orgname-accountname.snowflakecomputing.com",
			User: "alex",
			Password: "mysecret",
			Database: "mydb",
			Schema: "myschema",
			Warehouse: "mywarehouse",
			Role: "myrole",
			Account: "myaccount",
		},
		"v2 User & Password Authentication": {
			Host: "orgname-accountname.snowflakecomputing.com",
			Database: "mydb",
			Schema: "myschema",
			Credentials: credentialConfig{
				UserPass,
				"will",
				"password",
				"non-existant-jwt",
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, cfg.Validate())
			uri, err := cfg.toURI("mytenant")
			require.NoError(t, err)
			cupaloy.SnapshotT(t, uri)
		})
	}
}