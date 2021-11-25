package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pf "github.com/estuary/protocols/flow"
	pm "github.com/estuary/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	var validConfig = config{
		Endpoint: "testEndpoint",
		Username: "testUsername",
		Password: "testPassword",
	}

	require.NoError(t, validConfig.Validate())

	var missingEndpoint = validConfig
	missingEndpoint.Endpoint = ""
	require.Error(t, missingEndpoint.Validate(), "expected validation error")
}

func TestResource(t *testing.T) {
	var validResourceA resource
	pf.UnmarshalStrict(json.RawMessage(`{
		"index":        "testIndex",
		"delta_updates": true,
		"field_overides": [
			{
				"pointer": "/test_pointer",
				"esType":  {"field_type": "test_field_type"}
			}
		]
	}`), &validResourceA)
	require.NoError(t, validResourceA.Validate())
	require.Equal(t, 0, validResourceA.GetNumOfReplicas())
	require.Equal(t, 1, validResourceA.GetNumOfShards())

	var missingIndex = validResourceA
	missingIndex.Index = ""
	require.Error(t, missingIndex.Validate(), "expected validation error")

	var validResourceB resource
	pf.UnmarshalStrict(json.RawMessage(`{
		"index":        "testIndex",
		"delta_updates": true,
		"number_of_shards": 3,
		"number_of_replicas": 4
	}`), &validResourceB)
	require.NoError(t, validResourceB.Validate())
	require.Equal(t, 3, validResourceB.GetNumOfShards())
	require.Equal(t, 4, validResourceB.GetNumOfReplicas())
}

func TestDriverSpec(t *testing.T) {
	var drv = driver{}
	var resp, err1 = drv.Spec(context.Background(), &pm.SpecRequest{EndpointType: pf.EndpointType_FLOW_SINK})
	require.NoError(t, err1)
	var formatted, err2 = json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err2)
	cupaloy.SnapshotT(t, formatted)
}
