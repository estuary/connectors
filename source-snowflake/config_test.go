package main

import (
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

func TestConfigURI(t *testing.T) {
	for name, cfg := range map[string]config{
		"User & Password Authentication": {
			Host:     "orgname-accountname.snowflakecomputing.com",
			Database: "mydb",
			User:     "will",
			Account:  "myaccount",
			Password: "some+complex/password",
		},
		"Optional Parameters": {
			Host:      "orgname-accountname.snowflakecomputing.com",
			Database:  "mydb",
			User:      "alex",
			Password:  "some+complex/password",
			Warehouse: "mywarehouse",
			Account:   "myaccount",
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, cfg.Validate())
			uri := cfg.ToURI()
			cupaloy.SnapshotT(t, uri)
		})
	}
}
