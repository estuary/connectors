package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os/exec"
	"regexp"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate/testutil"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestSpec(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	var resp, err = driver{}.
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	// Start the Iceberg REST catalog (+ PostgreSQL).
	require.NoError(t, exec.Command("docker", "compose", "-f", "docker-compose.yaml", "up", "--wait").Run())
	t.Cleanup(func() {
		exec.Command("docker", "compose", "-f", "docker-compose.yaml", "down", "-v").Run()
	})

	// Decrypt the config to get S3 credentials for warehouse creation.
	cfg := decryptTestConfig(t)

	// Create the test warehouse in the REST catalog.
	createTestWarehouse(t, cfg)

	d := true
	makeResourceFn := func(table string, delta bool) resource {
		return resource{Table: table, Delta: &d}
	}

	// Sanitize S3 file paths and table hashes that contain random UUIDs.
	sanitizers := []func(string) string{
		func(s string) string {
			// Replace UUIDs in S3 parquet file paths.
			re := regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.parquet`)
			return re.ReplaceAllString(s, "<uuid>.parquet")
		},
		func(s string) string {
			// Replace table name hashes (16 hex chars).
			re := regexp.MustCompile(`_([\dA-F]{16})/data/`)
			return re.ReplaceAllString(s, "_<hash>/data/")
		},
	}

	t.Run("materialize", func(t *testing.T) {
		boilerplate.RunMaterializationTest(t, newMaterialization, "testdata/materialize.flow.yaml", makeResourceFn, sanitizers)
	})

	t.Run("apply", func(t *testing.T) {
		boilerplate.RunApplyTest(t, &driver{}, newMaterialization, "testdata/apply.flow.yaml", makeResourceFn)
	})

	// Migration test is skipped because Iceberg does not support the type
	// migrations exercised by the test (e.g. long→string). pyiceberg rejects
	// schema mismatches when appending files with migrated types.
	//t.Run("migrate", func(t *testing.T) {
	//	boilerplate.RunMigrationTest(t, newMaterialization, "testdata/migrate.flow.yaml", makeResourceFn, nil)
	//})
}

func decryptTestConfig(t *testing.T) config {
	t.Helper()

	bundled := boilerplate.RunFlowctl(t, "raw", "bundle", "--source", "testdata/materialize.flow.yaml")
	taskName := "acmeCo/tests/materialize-s3-iceberg"

	raw := json.RawMessage(gjson.GetBytes(bundled, "materializations."+taskName+".endpoint.local.config").Raw)
	require.NotEmpty(t, raw, "could not find config in bundled spec")

	if gjson.GetBytes(raw, "sops").Exists() {
		sopsCmd := exec.Command("sops", "--decrypt", "--input-type", "json", "--output-type", "json", "/dev/stdin")
		jqCmd := exec.Command("jq", `walk( if type == "object" then with_entries(.key |= rtrimstr("_sops")) else . end)`)
		sopsCmd.Stdin = bytes.NewReader(raw)
		var err error
		jqCmd.Stdin, err = sopsCmd.StdoutPipe()
		require.NoError(t, err)
		require.NoError(t, sopsCmd.Start())
		raw, err = jqCmd.Output()
		require.NoError(t, err)
		require.NoError(t, sopsCmd.Wait())
	}

	var cfg config
	require.NoError(t, pf.UnmarshalStrict(raw, &cfg))
	return cfg
}

func createTestWarehouse(t *testing.T, cfg config) {
	t.Helper()

	warehouse := map[string]any{
		"warehouse-name": cfg.Catalog.Warehouse,
		"project-id":     "00000000-0000-0000-0000-000000000000",
		"storage-profile": map[string]any{
			"type":        "s3",
			"bucket":      cfg.Bucket,
			"region":      cfg.Region,
			"sts-enabled": false,
		},
		"storage-credential": map[string]any{
			"type":                  "s3",
			"credential-type":      "access-key",
			"aws-access-key-id":     cfg.AWSAccessKeyID,
			"aws-secret-access-key": cfg.AWSSecretAccessKey,
		},
	}

	body, err := json.Marshal(warehouse)
	require.NoError(t, err)

	var resp *http.Response
	for i := 0; i < 30; i++ {
		resp, err = http.Post(
			"http://localhost:8090/management/v1/warehouse",
			"application/json",
			bytes.NewReader(body),
		)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	require.NoError(t, err)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		var respBody bytes.Buffer
		respBody.ReadFrom(resp.Body)
		t.Fatalf("creating warehouse failed with status %d: %s", resp.StatusCode, respBody.String())
	}
}
