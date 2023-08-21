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
	username = flag.String("username", "elastic", "Username to use with the elasticsearch API")
	password = flag.String("password", "elastic", "Password for the user")
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
		Index:        testIndexName,
		DeltaUpdates: false,
	}
	resourceJson, err := json.Marshal(baseResource)
	require.NoError(t, err)

	client, err := cfg.toClient()
	require.NoError(t, err)

	boilerplate_tests.RunApplyTestCases(
		t,
		driver{},
		configJson,
		resourceJson,
		[]string{testIndexName},
		func(t *testing.T) string {
			t.Helper()

			resp, err := client.es.Indices.Get([]string{testIndexName})
			require.NoError(t, err)
			defer resp.Body.Close()

			bb, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			return gjson.GetBytes(bb, fmt.Sprintf("%s.mappings.properties", testIndexName)).String()
		},
		func(t *testing.T) {
			t.Helper()

			_, err := client.es.Indices.Delete([]string{defaultFlowMaterializations, testIndexName})
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

func TestNormalizeIndexName(t *testing.T) {
	for _, tt := range []struct {
		name       string
		byteLength int
		input      string
		want       string
	}{
		{
			name:       "not normalized",
			byteLength: maxByteLength,
			input:      "some_collection",
			want:       "some_collection",
		},
		{
			name:       "dots",
			byteLength: maxByteLength,
			input:      ".some..collection",
			want:       "some__collection",
		},
		{
			name:       "capitalized",
			byteLength: maxByteLength,
			input:      "SomeCollection",
			want:       "somecollection",
		},
		{
			name:       "strip prefixes",
			byteLength: maxByteLength,
			input:      "-_.collection",
			want:       "collection",
		},
		{
			name:       "truncate Ascii",
			byteLength: 3,
			input:      "__aaaa",
			want:       "aaa",
		},
		{
			name:       "truncate UTF-8",
			byteLength: 10,
			input:      "__中文内码",
			want:       "中文内",
		},
		{
			name:       "empty input",
			byteLength: maxByteLength,
			input:      "",
			want:       "",
		},
		{
			name:       "no valid characters",
			byteLength: maxByteLength,
			input:      "._-",
			want:       "",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, normalizeIndexName(tt.input, tt.byteLength))
		})
	}
}
