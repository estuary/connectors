package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	protocol "github.com/estuary/connectors/go/protocol"
	"github.com/stretchr/testify/require"
)

func TestPostgresConfig(t *testing.T) {
	var validConfig = config{
		Address:  "post.toast:1234",
		User:     "youser",
		Password: "shmassword",
		Database: "namegame",
	}
	require.NoError(t, validConfig.Validate())
	var uri = validConfig.ToURI()
	require.Equal(t, "postgres://youser:shmassword@post.toast:1234/namegame?statement_cache_mode=describe", uri)

	var minimal = validConfig
	minimal.Database = ""
	require.NoError(t, minimal.Validate())
	uri = minimal.ToURI()
	require.Equal(t, "postgres://youser:shmassword@post.toast:1234?statement_cache_mode=describe", uri)

	var noAddress = validConfig
	noAddress.Address = ""
	require.Error(t, noAddress.Validate(), "expected validation error")

	var noUser = validConfig
	noUser.User = ""
	require.Error(t, noUser.Validate(), "expected validation error")

	var noPass = validConfig
	noPass.Password = ""
	require.Error(t, noPass.Validate(), "expected validation error")
}

func TestSpecification(t *testing.T) {
	var resp, err = newPostgresDriver().
		Spec(context.Background(), protocol.SpecRequest{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}
