package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	sb "github.com/estuary/connectors/materialize-elasticsearch/schemabuilder"
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
	var validResource = resource{
		Index:        "testIndex",
		DeltaUpdates: true,
		FieldOverides: []sb.FieldOverride{
			{
				Pointer: "/test_pointer",
				EsType:  sb.ElasticFieldType{FieldType: "test_field_type"},
			},
		},
	}
	require.NoError(t, validResource.Validate())

	var missingIndex = validResource
	missingIndex.Index = ""
	require.Error(t, missingIndex.Validate(), "expected validation error")
}

func TestDriverSpec(t *testing.T) {
	var drv = driver{}
	var resp, err1 = drv.Spec(context.Background(), &pm.SpecRequest{EndpointType: pf.EndpointType_FLOW_SINK})
	require.NoError(t, err1)
	var formatted, err2 = json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err2)
	cupaloy.SnapshotT(t, formatted)
}
