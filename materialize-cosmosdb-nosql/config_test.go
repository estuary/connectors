package connector

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func testConfig() config {
	return config{
		Endpoint: "https://example.documents.azure.com:443/",
		Key:      base64.StdEncoding.EncodeToString([]byte("not-a-real-account-key")),
		Database: "test-database",
	}
}

func TestSpec(t *testing.T) {
	response, err := NewDriver().Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, formatted)

	var endpoint struct {
		Required   []string                  `json:"required"`
		Properties map[string]map[string]any `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(response.ConfigSchemaJson, &endpoint))
	require.ElementsMatch(t, []string{"endpoint", "key", "database"}, endpoint.Required)
	require.Len(t, endpoint.Properties, 5)
	require.Equal(t, true, endpoint.Properties["key"]["secret"])
	require.Equal(t, false, endpoint.Properties["hardDelete"]["default"])
	require.Equal(t, true, endpoint.Properties["advanced"]["advanced"])

	var res struct {
		Required   []string                  `json:"required"`
		Properties map[string]map[string]any `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(response.ResourceConfigSchemaJson, &res))
	require.Equal(t, []string{"container"}, res.Required)
	require.Len(t, res.Properties, 3)
	require.Equal(t, true, res.Properties["container"]["x-collection-name"])
	require.Equal(t, "string", res.Properties["partition_key"]["type"])
	require.Equal(t, true, res.Properties["delta_updates"]["x-delta-updates"])
	require.Equal(t, false, res.Properties["delta_updates"]["default"])
}

func TestConfig(t *testing.T) {
	for _, tt := range []struct {
		name   string
		update func(*config)
		want   string
	}{
		{"valid", func(c *config) {}, ""},
		{"http", func(c *config) { c.Endpoint = "http://cosmos.test:8081/" }, ""},
		{"no trailing slash", func(c *config) { c.Endpoint = "https://cosmos.test" }, ""},
		{"missing endpoint", func(c *config) { c.Endpoint = "" }, "missing 'endpoint'"},
		{"missing key", func(c *config) { c.Key = "" }, "missing 'key'"},
		{"missing database", func(c *config) { c.Database = "" }, "missing 'database'"},
		{"relative endpoint", func(c *config) { c.Endpoint = "cosmos.test" }, "absolute http or https"},
		{"invalid scheme", func(c *config) { c.Endpoint = "ftp://cosmos.test/" }, "absolute http or https"},
		{"missing host", func(c *config) { c.Endpoint = "https:///" }, "absolute http or https"},
		{"invalid port", func(c *config) { c.Endpoint = "https://cosmos.test:invalid/" }, "absolute http or https"},
		{"credentials", func(c *config) { c.Endpoint = "https://fake-user:fake-password@cosmos.test/" }, "without credentials"},
		{"query", func(c *config) { c.Endpoint = "https://cosmos.test/?key=fake-key" }, "without credentials"},
		{"fragment", func(c *config) { c.Endpoint = "https://cosmos.test/#fragment" }, "without credentials"},
		{"resource URI", func(c *config) { c.Endpoint = "https://cosmos.test/dbs/example" }, "without credentials"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			tt.update(&cfg)
			err := cfg.Validate()
			if tt.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.want)
				if cfg.Key != "" {
					require.NotContains(t, err.Error(), cfg.Key)
				}
				require.NotContains(t, err.Error(), "fake-password")
			}
		})
	}
}

func TestUnmarshalConfig(t *testing.T) {
	var cfg config
	raw, err := json.Marshal(testConfig())
	require.NoError(t, err)
	require.NoError(t, boilerplate.UnmarshalStrict(raw, &cfg))
	require.False(t, cfg.HardDelete)
	require.Equal(t, "test-database", cfg.DefaultNamespace())

	var bad config
	require.ErrorContains(t, boilerplate.UnmarshalStrict([]byte(`{"unknown":true}`), &bad), `unknown field "unknown"`)
}

func TestFeatureFlags(t *testing.T) {
	cfg := testConfig()
	flags, err := boilerplate.ResolveFlags(cfg, nil)
	require.NoError(t, err)
	require.False(t, flags["allow_existing_tables_for_new_bindings"])
	require.False(t, flags["retain_existing_data_on_backfill"])

	cfg.Advanced.FeatureFlags = "allow_existing_tables_for_new_bindings"
	flags, err = boilerplate.ResolveFlags(cfg, nil)
	require.NoError(t, err)
	require.True(t, flags["allow_existing_tables_for_new_bindings"])
	require.False(t, flags["retain_existing_data_on_backfill"])
}

func TestResource(t *testing.T) {
	var res resource
	require.NoError(t, boilerplate.UnmarshalStrict([]byte(`{"container":"events"}`), &res))
	res = res.WithDefaults(testConfig())
	path, delta, err := res.Parameters()
	require.NoError(t, err)
	require.Equal(t, []string{"test-database", "events"}, path)
	require.False(t, delta)

	require.NoError(t, boilerplate.UnmarshalStrict([]byte(`{"container":"events","partition_key":"/seq","delta_updates":true}`), &res))
	_, delta, err = res.Parameters()
	require.NoError(t, err)
	require.True(t, delta)

	require.ErrorContains(t, (resource{}).Validate(), "missing 'container'")
	require.ErrorContains(t, (resource{Container: "events", PartitionKey: "seq"}).Validate(), "JSON pointer")
	require.ErrorContains(t, boilerplate.UnmarshalStrict([]byte(`{"container":"events","unknown":true}`), &res), `unknown field "unknown"`)
}

func TestPartitionKey(t *testing.T) {
	for _, tt := range []struct {
		name string
		keys []string
		ptr  string
		want string
		err  string
	}{
		{"first key", []string{"/id"}, "", "/id", ""},
		{"compound key default", []string{"/region", "/seq"}, "", "/region", ""},
		{"explicit key", []string{"/region", "/seq"}, "/seq", "/seq", ""},
		{"nested key", []string{"/region/code", "/seq"}, "/region/code", "/region/code", ""},
		{"escaped pointer", []string{"/a~1b~0c"}, "/a~1b~0c", "/a~1b~0c", ""},
		{"nested id default", []string{"/id/number"}, "", "", "replaces id with an encoded string"},
		{"nested id explicit", []string{"/id/tenant"}, "/id/tenant", "", "replaces id with an encoded string"},
		{"root pointer", []string{""}, "", "", "JSON pointer"},
		{"invalid escape", []string{"/a~2b"}, "", "", "JSON pointer"},
		{"trailing escape", []string{"/a~"}, "", "", "JSON pointer"},
		{"empty segment", []string{"/region/"}, "", "", "unsupported partition key"},
		{"quoted segment", []string{`/a"b`}, "", "", "unsupported partition key"},
		{"backslash segment", []string{`/a\b`}, "", "", "unsupported partition key"},
		{"control character", []string{"/a\nb"}, "", "", "unsupported partition key"},
		{"not a key", []string{"/id"}, "/region", "", "must be one of the collection key pointers"},
		{"no keys", nil, "", "", "collection must have at least one key"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			res := resource{Container: "test-container", PartitionKey: tt.ptr}
			got, err := res.partitionKey(tt.keys)
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}
