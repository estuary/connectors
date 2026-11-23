package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/estuary/connectors/sqlcapture"
	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/stretchr/testify/require"
)

var (
	dbCaptureAddress = flag.String("db_capture_addr", "localhost:50000", "The database server address to use for integration tests")
	dbCaptureUser    = flag.String("db_capture_user", "db2inst1", "The database user to use for integration tests")
	dbName           = flag.String("db_name", "TESTDB", "Connect to the named database for tests")
)

func TestMain(m *testing.M) {
	if os.Getenv("DB2CODEPAGE") == "1208" {
		os.Exit(m.Run())
	}
	// Match the image's UTF-8 client encoding. The CLI caches its code page during
	// driver initialization, before TestMain, so changing the environment needs a restart.
	var cmd = exec.Command(os.Args[0], os.Args[1:]...)
	cmd.Env = append(os.Environ(), "DB2CODEPAGE=1208")
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			os.Exit(exit.ExitCode())
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func TestSpec(t *testing.T) {
	var response, err = db2Driver.Spec(context.Background(), &pc.Request_Spec{})
	require.NoError(t, err)
	require.EqualValues(t, 3032023, response.Protocol)
	require.Equal(t, "https://go.estuary.dev/source-db2", response.DocumentationUrl)
	require.Equal(t, []string{"/namespace", "/stream"}, response.ResourcePathPointers)

	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
		Required   []string                   `json:"required"`
	}
	require.NoError(t, json.Unmarshal(response.ConfigSchemaJson, &schema))
	require.ElementsMatch(t, []string{"address", "user", "password", "database", "historyMode", "advanced", "networkTunnel"}, mapKeys(schema.Properties))
	require.ElementsMatch(t, []string{"address", "password", "database"}, schema.Required)
	var password map[string]any
	require.NoError(t, json.Unmarshal(schema.Properties["password"], &password))
	require.Equal(t, true, password["secret"])
	var advanced struct {
		Properties map[string]json.RawMessage `json:"properties"`
		Advanced   bool                       `json:"advanced"`
	}
	require.NoError(t, json.Unmarshal(schema.Properties["advanced"], &advanced))
	require.True(t, advanced.Advanced)
	require.ElementsMatch(t, []string{"capture_schema", "watermarksTable", "backfill_chunk_size", "polling_interval", "discover_schemas", "timezone", "source_tag", "feature_flags"}, mapKeys(advanced.Properties))
	var resource map[string]any
	require.NoError(t, json.Unmarshal(response.ResourceConfigSchemaJson, &resource))
	require.ElementsMatch(t, []any{"namespace", "stream"}, resource["required"])
}

func mapKeys[V any](values map[string]V) []string {
	var keys []string
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}

// TestDiscoveryIntegration is read-only and expects the SQL Replication fixture.
func TestDiscoveryIntegration(t *testing.T) {
	var cfg = integrationConfig(t)
	var configJSON, err = json.Marshal(cfg)
	require.NoError(t, err)
	var response, discoverErr = db2Driver.Discover(context.Background(), &pc.Request_Discover{ConfigJson: configJSON})
	require.NoError(t, discoverErr)
	var keys = make(map[string][]string)
	for _, binding := range response.Bindings {
		var resource sqlcapture.Resource
		require.NoError(t, json.Unmarshal(binding.ResourceConfigJson, &resource))
		require.NotEqual(t, "ASN", resource.Namespace)
		require.NotContains(t, []string{"FLOW_WATERMARKS", "CDORDERS", "CDLINES", "CDFLOW_WATERMARKS"}, resource.Stream)
		keys[resource.Namespace+"."+resource.Stream] = binding.Key
	}
	require.Equal(t, []string{"/ID"}, keys["DB2INST1.ORDERS"])
	require.Equal(t, []string{"/ORDER_ID", "/LINE_NO"}, keys["DB2INST1.LINES"])
}
