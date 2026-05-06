package main

import (
	"flag"
	"os"
	"os/exec"
	"testing"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate/testutil"
	"github.com/stretchr/testify/require"
)

// testAll controls whether integration tests run against a real Cloud
// Bigtable instance in addition to the local emulator. The real-instance
// path requires the encrypted `config.gcp.yaml` to decrypt and a live
// instance pre-provisioned per `testdata/README.md`, so by default we
// only exercise the emulator. Opt in to the full matrix by setting the
// `BIGTABLE_TEST_ALL` environment variable or passing
// `-bigtable.test-all` to `go test`.
var testAll = flag.Bool("bigtable.test-all", false, "run integration tests against a real Cloud Bigtable instance in addition to the local emulator")

func testConfig() *config {
	return &config{
		ProjectID:  "test-project",
		InstanceID: "test-instance",
		Advanced: advancedConfig{
			Endpoint: "localhost:8086",
		},
	}
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	makeResourceFn := func(table string, delta bool) resource {
		return resource{Table: table}
	}

	require.NoError(t, exec.Command("docker", "compose", "-f", "docker-compose.yaml", "up", "--wait").Run())
	t.Cleanup(func() {
		exec.Command("docker", "compose", "-f", "docker-compose.yaml", "down", "-v").Run()
	})

	all := *testAll || os.Getenv("BIGTABLE_TEST_ALL") != ""

	materializeSpec := "testdata/materialize-local.flow.yaml"
	applySpec := "testdata/apply-local.flow.yaml"
	if all {
		materializeSpec = "testdata/materialize.flow.yaml"
		applySpec = "testdata/apply.flow.yaml"
	}

	t.Run("materialize", func(t *testing.T) {
		boilerplate.RunMaterializationTest(t, newMaterialization, materializeSpec, makeResourceFn, nil)
	})

	t.Run("apply", func(t *testing.T) {
		boilerplate.RunApplyTest(t, &driver{}, newMaterialization, applySpec, makeResourceFn)
	})
}
