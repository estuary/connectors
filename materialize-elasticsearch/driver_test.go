package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func testConfig() *config {
	return &config{
		Endpoint: "http://localhost:9200",
		Credentials: credentials{
			Username: "elastic",
			Password: "elastic",
		},
	}
}

func TestValidateAndApply(t *testing.T) {
	flag.Parse()

	cfg := testConfig()

	resourceConfig := resource{
		Index: "target",
	}

	client, err := cfg.toClient(false)
	require.NoError(t, err)

	boilerplate.RunValidateAndApplyTestCases(
		t,
		driver{},
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()

			resp, err := client.es.Indices.Get([]string{resourceConfig.Index})
			require.NoError(t, err)
			defer resp.Body.Close()

			bb, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			props := gjson.GetBytes(bb, fmt.Sprintf("%s.mappings.properties", resourceConfig.Index)).Map()

			type row struct {
				Field string
				Type  string
			}
			rows := make([]row, 0, len(props))
			for f, p := range props {
				rows = append(rows, row{
					Field: f,
					Type:  p.Get("type").Str,
				})
			}

			slices.SortFunc(rows, func(i, j row) int {
				return strings.Compare(i.Field, j.Field)
			})

			var out strings.Builder
			enc := json.NewEncoder(&out)
			for _, r := range rows {
				require.NoError(t, enc.Encode(r))
			}

			return out.String()
		},
		func(t *testing.T, materialization pf.Materialization) {
			t.Helper()

			_, err := client.es.Indices.Delete([]string{defaultFlowMaterializations, resourceConfig.Index})
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
