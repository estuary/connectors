package main

import (
	"bytes"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"testing"
	"time"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

func restartEmulator(t *testing.T) {
	t.Log("Restarting BigQuery emulator...")
	_ = exec.Command("docker", "compose", "-f", "docker-compose.yaml", "restart", "bigquery").Run()
	
	// Wait for BigQuery emulator to be ready
	var resp *http.Response
	var err error
	for i := 0; i < 30; i++ {
		resp, err = http.Get("http://localhost:9050/bigquery/v2/projects/test-project/datasets")
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		time.Sleep(1 * time.Second)
	}
	require.NoError(t, err, "failed to reach BigQuery emulator after restart")
	require.True(t, resp != nil && resp.StatusCode == http.StatusOK, "BigQuery emulator returned status %d after restart", resp.StatusCode)
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	all := os.Getenv("BIGQUERY_TEST_ALL") != ""

	materializeSpec := "testdata/materialize-local.flow.yaml"
	applySpec := "testdata/apply-local.flow.yaml"
	migrateSpec := "testdata/migrate-local.flow.yaml"
	if all {
		materializeSpec = "testdata/materialize.flow.yaml"
		applySpec = "testdata/apply.flow.yaml"
		migrateSpec = "testdata/migrate.flow.yaml"
	}

	// Opt-in for real cloud tests
	if !all {
		// Start docker-compose and set up emulators
		t.Log("Starting docker-compose for emulators...")
		cmd := exec.Command("docker", "compose", "-f", "docker-compose.yaml", "up", "-d")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		require.NoError(t, cmd.Run())
		// t.Cleanup(func() {
		// 	t.Log("Tearing down docker-compose...")
		// 	downCmd := exec.Command("docker", "compose", "-f", "docker-compose.yaml", "down", "-v")
		// 	downCmd.Stdout = os.Stdout
		// 	downCmd.Stderr = os.Stderr
		// 	_ = downCmd.Run()
		// })

		// Set the storage emulator host for GCS client
		os.Setenv("STORAGE_EMULATOR_HOST", "http://localhost:4443")
		t.Cleanup(func() { os.Unsetenv("STORAGE_EMULATOR_HOST") })

		// Wait for GCS emulator to be ready and create the bucket
		t.Log("Creating test-bucket in GCS emulator...")
		var resp *http.Response
		var err error
		for i := 0; i < 30; i++ {
			body := []byte(`{"name": "test-bucket"}`)
			resp, err = http.Post("http://localhost:4443/storage/v1/b", "application/json", bytes.NewBuffer(body))
			if err == nil {
				defer resp.Body.Close()
				if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusConflict {
					break
				}
			}
			time.Sleep(1 * time.Second)
		}
		require.NoError(t, err, "failed to reach GCS emulator")
		require.True(t, resp != nil && (resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusConflict), "failed to create bucket: status %d", resp.StatusCode)

		// Wait for BigQuery emulator to be ready
		t.Log("Waiting for BigQuery emulator...")
		for i := 0; i < 30; i++ {
			resp, err = http.Get("http://localhost:9050/bigquery/v2/projects/test-project/datasets")
			if err == nil {
				defer resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					break
				}
			}
			time.Sleep(1 * time.Second)
		}
		require.NoError(t, err, "failed to reach BigQuery emulator")
		require.True(t, resp != nil && resp.StatusCode == http.StatusOK, "BigQuery emulator returned status %d", resp.StatusCode)
	}

	makeResourceFn := func(table string, delta bool) tableConfig {
		return tableConfig{
			Table: table,
		}
	}

	actionDescSanitizers := []func(string) string{
		func(s string) string {
			return regexp.MustCompile(`"JobPrefix":\s*"[^"]*"`).ReplaceAllString(s, `"JobPrefix": "<uuid>"`)
		},
		func(s string) string {
			return regexp.MustCompile(`"gs://[^/]+/[^"]*"`).ReplaceAllString(s, `"gs://[bucket]/<uuid>"`)
		},
	}

	t.Run("materialize", func(t *testing.T) {
		sql.RunMaterializationTest(t, newBigQueryDriver(), materializeSpec, makeResourceFn, actionDescSanitizers)
	})

	t.Run("apply", func(t *testing.T) {
		if !all {
			restartEmulator(t)
		}
		sql.RunApplyTest(t, newBigQueryDriver(), applySpec, makeResourceFn)
	})

	t.Run("migrate", func(t *testing.T) {
		if !all {
			t.Skip("Skipping migrate test for emulator due to schema evolution bug in goccy/bigquery-emulator")
		}
		sql.RunMigrationTest(t, newBigQueryDriver(), migrateSpec, makeResourceFn, nil)
	})

	// Toggling objects_and_arrays_as_json migrates the object column and the
	// flow_document column between JSON and STRING in place. This exercises the
	// root document JSON<->text migration end-to-end, the scenario that requires
	// the root document to be migratable (rather than needing a backfill).
	t.Run("flow_document-migration", func(t *testing.T) {
		sql.RunFeatureFlagMigrationTest(t, newBigQueryDriver(), "testdata/migrate-doc.flow.yaml", makeResourceFn, []sql.FeatureFlagMigrationPhase{
			{FeatureFlags: "objects_and_arrays_as_json", Fixture: "testdata/fixture.doc-migrate.json"},    // materialize as JSON
			{FeatureFlags: "no_objects_and_arrays_as_json", Fixture: "testdata/fixture.doc-migrate.json"}, // migrate JSON -> STRING
			{FeatureFlags: "objects_and_arrays_as_json", Fixture: "testdata/fixture.doc-migrate.json"},    // migrate STRING -> JSON
		}, actionDescSanitizers)
	})
}

