package connector

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/sijms/go-ora/v2/configurations"
	"github.com/stretchr/testify/require"
)

func testConfig() config {
	return config{
		Address: "oracle.example:1522", ServiceName: "FREEPDB1", User: "test", Password: "factorytest",
		// Tests reuse tables that earlier test runs created.
		Advanced: advancedConfig{FeatureFlags: "allow_existing_tables_for_new_bindings"},
	}
}

func TestConfig(t *testing.T) {
	var cfg = testConfig()
	require.NoError(t, cfg.Validate())
	require.Equal(t, "test", cfg.DefaultNamespace())
	require.False(t, cfg.HardDelete)

	for _, field := range []string{"address", "service_name", "user", "password"} {
		t.Run("missing "+field, func(t *testing.T) {
			encoded, err := json.Marshal(cfg)
			require.NoError(t, err)
			var fields map[string]any
			require.NoError(t, json.Unmarshal(encoded, &fields))
			delete(fields, field)
			encoded, err = json.Marshal(fields)
			require.NoError(t, err)
			var missing config
			require.NoError(t, json.Unmarshal(encoded, &missing))
			require.EqualError(t, missing.Validate(), "missing '"+field+"'")
		})
	}
	cfg.Schema = "reporting"
	require.Equal(t, "reporting", cfg.DefaultNamespace())
}

func TestConfigURI(t *testing.T) {
	for _, tc := range []struct {
		address string
		host    string
	}{
		{"oracle.example", "oracle.example:1521"},
		{"oracle.example:1522", "oracle.example:1522"},
		{"127.0.0.1", "127.0.0.1:1521"},
		{"[::1]", "[::1]:1521"},
		{"::1", "[::1]:1521"},
		{"[::1]:1522", "[::1]:1522"},
	} {
		t.Run(tc.address, func(t *testing.T) {
			cfg := testConfig()
			cfg.Address = tc.address
			cfg.User = "test@example"
			cfg.Password = "made/up:p@ss?word#%+&"
			require.NoError(t, cfg.Validate())
			uri, err := cfg.ToURI()
			require.NoError(t, err)
			require.Equal(t, tc.host, uri.Host)

			// Check the driver's parser as well as URL escaping without connecting.
			parsed, err := configurations.ParseConfig(uri.String())
			require.NoError(t, err)
			require.Equal(t, cfg.User, parsed.UserID)
			require.Equal(t, cfg.Password, parsed.Password)
			require.Equal(t, cfg.ServiceName, parsed.ServiceName)
		})
	}
	for _, address := range []string{"", ":1521", "oracle.example:", "oracle.example:bad", "oracle.example:0", "oracle.example:65536", "oracle://example", "example/path", "example name"} {
		t.Run("invalid "+address, func(t *testing.T) {
			cfg := testConfig()
			cfg.Address = address
			require.Error(t, cfg.Validate())
		})
	}
}

func TestTableConfig(t *testing.T) {
	var cfg = testConfig()
	var table = tableConfig{Table: "orders"}.WithDefaults(cfg)
	require.NoError(t, table.Validate())
	path, delta, err := table.Parameters()
	require.NoError(t, err)
	require.Equal(t, []string{"test", "orders"}, path)
	require.False(t, delta)

	cfg.Schema = "reporting"
	require.Equal(t, "reporting", (tableConfig{Table: "orders"}).WithDefaults(cfg).Schema)
	table = tableConfig{Table: "orders", Schema: "override", Delta: true}.WithDefaults(cfg)
	path, delta, err = table.Parameters()
	require.NoError(t, err)
	require.Equal(t, []string{"override", "orders"}, path)
	require.True(t, delta)
	require.Error(t, tableConfig{}.Validate())
	require.Error(t, tableConfig{Table: strings.Repeat("a", 129)}.Validate())
	require.Error(t, tableConfig{Table: "nul\x00byte"}.Validate())
	require.Error(t, tableConfig{Table: `a"b`}.Validate())
	require.Error(t, tableConfig{Table: "orders", Schema: `a"b`}.Validate())
	require.NoError(t, tableConfig{Table: "Order Items"}.Validate())
}

func TestSpecification(t *testing.T) {
	t.Setenv("DOCS_URL", "")
	resp, err := NewDriver().Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)
	require.Equal(t, "https://go.estuary.dev/materialize-oracle", resp.DocumentationUrl)

	var endpoint, resource struct {
		Type       string                    `json:"type"`
		Required   []string                  `json:"required"`
		Properties map[string]map[string]any `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(resp.ConfigSchemaJson, &endpoint))
	require.NoError(t, json.Unmarshal(resp.ResourceConfigSchemaJson, &resource))
	require.Equal(t, "object", endpoint.Type)
	require.ElementsMatch(t, []string{"address", "service_name", "user", "password"}, endpoint.Required)
	require.Equal(t, true, endpoint.Properties["password"]["secret"])
	require.Equal(t, false, endpoint.Properties["hardDelete"]["default"])
	require.Equal(t, true, endpoint.Properties["advanced"]["advanced"])
	require.Equal(t, "object", resource.Type)
	require.Equal(t, []string{"table"}, resource.Required)
	require.Equal(t, true, resource.Properties["table"]["x-collection-name"])
	require.Equal(t, true, resource.Properties["schema"]["x-schema-name"])
	require.Equal(t, false, resource.Properties["delta_updates"]["default"])

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, formatted)
}
