package connector

import (
	"flag"
	"os"
	"testing"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate/testutil"
)

// testAll controls whether integration tests run against every catalog
// configuration (REST, Glue, S3 Tables, and their deprecated variants) or only
// against the REST catalog. Running the full matrix is slow and requires
// credentials for multiple cloud backends, so by default we only exercise the
// REST catalog. Opt in to the full matrix by setting the `ICEBERG_TEST_ALL`
// environment variable or passing `-iceberg.test-all` to `go test`.
var testAll = flag.Bool("iceberg.test-all", false, "run integration tests against all catalog configurations (REST, Glue, S3 Tables) instead of REST only")

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	makeResourceFn := func(table string, delta bool) resource {
		return resource{Table: table}
	}

	all := *testAll || os.Getenv("ICEBERG_TEST_ALL") != ""

	materializeSpec := "testdata/materialize-rest.flow.yaml"
	applySpec := "testdata/apply-rest.flow.yaml"
	migrateSpec := "testdata/migrate-rest.flow.yaml"
	if all {
		materializeSpec = "testdata/materialize.flow.yaml"
		applySpec = "testdata/apply.flow.yaml"
		migrateSpec = "testdata/migrate.flow.yaml"
	}

	t.Run("materialize", func(t *testing.T) {
		boilerplate.RunMaterializationTestParallel(t, newMaterialization, materializeSpec, makeResourceFn, nil)
	})

	t.Run("apply", func(t *testing.T) {
		boilerplate.RunApplyTestParallel(t, &Driver{}, newMaterialization, applySpec, makeResourceFn)
	})

	t.Run("migrate", func(t *testing.T) {
		boilerplate.RunMigrationTestParallel(t, newMaterialization, migrateSpec, makeResourceFn, nil)
	})
}
