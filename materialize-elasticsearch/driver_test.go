package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	bp_test "github.com/estuary/connectors/materialize-boilerplate/testing"
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

	firstIndex := "first-index"
	secondIndex := "second-index"

	firstResource := resource{
		Index:        firstIndex,
		DeltaUpdates: false,
	}
	firstResourceJson, err := json.Marshal(firstResource)
	require.NoError(t, err)

	secondResource := resource{
		Index:        secondIndex,
		DeltaUpdates: false,
	}
	secondResourceJson, err := json.Marshal(secondResource)
	require.NoError(t, err)

	client, err := cfg.toClient()
	require.NoError(t, err)

	bp_test.RunApplyTestCases(
		t,
		driver{},
		configJson,
		[2]json.RawMessage{firstResourceJson, secondResourceJson},
		[2][]string{{firstIndex}, {secondIndex}},
		func(t *testing.T) []string {
			resp, err := client.es.Cat.Indices(client.es.Cat.Indices.WithFormat("json"))
			require.NoError(t, err)
			defer resp.Body.Close()

			bb, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			var got []map[string]string
			require.NoError(t, json.Unmarshal(bb, &got))

			indices := []string{}
			for _, i := range got {
				indices = append(indices, i["index"])
			}

			return indices
		},
		func(t *testing.T, resourcePath []string) string {
			t.Helper()

			resp, err := client.es.Indices.Get([]string{resourcePath[0]})
			require.NoError(t, err)
			defer resp.Body.Close()

			bb, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			return gjson.GetBytes(bb, fmt.Sprintf("%s.mappings.properties", resourcePath[0])).String()
		},
		func(t *testing.T) {
			t.Helper()

			_, err := client.es.Indices.Delete([]string{defaultFlowMaterializations, firstIndex, secondIndex})
			require.NoError(t, err)
		},
	)
}

func TestValidate(t *testing.T) {
	bp_test.RunValidateTestCases(t, elasticValidator)
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
