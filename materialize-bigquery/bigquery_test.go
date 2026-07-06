package main

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

// The two integration suites are gated by boolean environment variables:
//
//   - BIGQUERY_TEST_SAAS gates TestIntegration, which runs against real SaaS
//     BigQuery (project estuary-theatre), needs GCP credentials, and takes
//     ~10 minutes. Disabled unless BIGQUERY_TEST_SAAS=1.
//   - BIGQUERY_TEST_LOCAL_EMULATOR_GOCCY gates TestIntegrationLocalEmulatorGoccy,
//     which runs against the docker-compose goccy/bigquery-emulator +
//     fake-gcs-server stack and needs no GCP access. Enabled unless
//     BIGQUERY_TEST_LOCAL_EMULATOR_GOCCY=0.
//
// suiteEnabled reports whether the suite gated by the named variable should
// run: unset falls back to the suite's default, anything else must parse as a
// boolean ("1"/"0"/"true"/"false").
func suiteEnabled(t *testing.T, name string, enabledByDefault bool) bool {
	t.Helper()

	var v = os.Getenv(name)
	if v == "" {
		return enabledByDefault
	}
	var enabled, err = strconv.ParseBool(v)
	if err != nil {
		t.Fatalf("invalid %s=%q: want a boolean like 1 or 0", name, v)
	}
	return enabled
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	if !suiteEnabled(t, "BIGQUERY_TEST_SAAS", false) {
		t.Skip("SaaS suite disabled: set BIGQUERY_TEST_SAAS=1 to run against real BigQuery")
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
		sql.RunMaterializationTest(t, newBigQueryDriver(), "testdata/materialize.flow.yaml", makeResourceFn, actionDescSanitizers)
	})

	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, newBigQueryDriver(), "testdata/apply.flow.yaml", makeResourceFn)
	})

	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, newBigQueryDriver(), "testdata/migrate.flow.yaml", makeResourceFn, nil)
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

func TestIntegrationLocalEmulatorGoccy(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	if !suiteEnabled(t, "BIGQUERY_TEST_LOCAL_EMULATOR_GOCCY", true) {
		t.Skip("local-emulator-goccy suite disabled by BIGQUERY_TEST_LOCAL_EMULATOR_GOCCY=0")
	}

	require.NoError(t, exec.Command("docker", "compose", "-f", "docker-compose.yaml", "up", "--wait").Run())
	t.Cleanup(func() {
		exec.Command("docker", "compose", "-f", "docker-compose.yaml", "down", "-v").Run()
	})

	// The connector stages files in fake-gcs-server rather than real GCS. The
	// Go storage client honors STORAGE_EMULATOR_HOST, both in this test process
	// and in the flowctl-spawned `go run .` connector, which inherits the test
	// environment.
	t.Setenv("STORAGE_EMULATOR_HOST", "http://localhost:4443")
	createFakeGCSBucket(t, "test-bucket")

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

	t.Run("materialize-local-emulator-goccy", func(t *testing.T) {
		sql.RunMaterializationTest(t, newBigQueryDriver(), "testdata/materialize-local-emulator-goccy.flow.yaml", makeResourceFn, actionDescSanitizers)
	})

	t.Run("apply-local-emulator-goccy", func(t *testing.T) {
		sql.RunApplyTest(t, newBigQueryDriver(), "testdata/apply-local-emulator-goccy.flow.yaml", makeResourceFn)
	})

	t.Run("migrate-local-emulator-goccy", func(t *testing.T) {
		sql.RunMigrationTest(t, newBigQueryDriver(), "testdata/migrate-local-emulator-goccy.flow.yaml", makeResourceFn, nil)
	})
}

// createFakeGCSBucket creates the staging bucket in fake-gcs-server. The
// compose stack starts empty every run (`down -v` removes volumes), but 409
// Conflict is tolerated so a re-run against a still-live stack also works.
func createFakeGCSBucket(t *testing.T, bucket string) {
	t.Helper()

	resp, err := http.Post(
		"http://localhost:4443/storage/v1/b?project=test-project",
		"application/json",
		strings.NewReader(fmt.Sprintf(`{"name":%q}`, bucket)),
	)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Contains(t, []int{http.StatusOK, http.StatusConflict}, resp.StatusCode)
}
