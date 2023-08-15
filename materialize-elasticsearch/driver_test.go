package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate_tests "github.com/estuary/connectors/materialize-boilerplate/testing"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

var (
	username = flag.String("username", "", "Username to use with the elasticsearch API")
	password = flag.String("password", "", "Password for the user")
	apiKey   = flag.String("apiKey", "", "API key for authenticating with the elasticsearch API")
	endpoint = flag.String("endpoint", "http://localhost:9200", "Endpoint host or URL. If using Elastic Cloud this follows the format https://CLUSTER_ID.REGION.CLOUD_PLATFORM.DOMAIN:PORT")
)

func TestApply(t *testing.T) {
	flag.Parse()

	cfg := config{
		Endpoint: *endpoint,
		Credentials: credentials{
			Username: *username,
			Password: *password,
			ApiKey:   *apiKey,
		},
	}

	configJson, err := json.Marshal(cfg)
	require.NoError(t, err)

	testIndexName := "some-index"

	baseResource := resource{
		Index:         testIndexName,
		DeltaUpdates:  false,
		NumOfShards:   1,
		NumOfReplicas: 0,
	}
	resourceJson, err := json.Marshal(baseResource)
	require.NoError(t, err)

	es, err := newElasticsearchClient(cfg)
	require.NoError(t, err)

	boilerplate_tests.RunApplyTestCases(
		t,
		driver{},
		configJson,
		resourceJson,
		[]string{testIndexName},
		func(t *testing.T) string {
			t.Helper()

			resp, err := es.client.Indices.Get([]string{testIndexName})
			require.NoError(t, err)
			defer resp.Body.Close()

			bb, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			return gjson.GetBytes(bb, fmt.Sprintf("%s.mappings.properties", testIndexName)).String()
		},
		func(t *testing.T) {
			t.Helper()

			_, err := es.client.Indices.Delete([]string{defaultFlowMaterializations, testIndexName})
			require.NoError(t, err)
		},
	)
}

func TestDriverSpec(t *testing.T) {
	driver := driver{}
	response, err := driver.Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}
