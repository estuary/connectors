package connector

import (
	"testing"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate/testutil"
)

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	makeResourceFn := func(table string, delta bool) resource {
		return resource{Table: table}
	}

	t.Run("materialize", func(t *testing.T) {
		boilerplate.RunMaterializationTest(t, newMaterialization, "testdata/materialize.flow.yaml", makeResourceFn, nil)
	})

	t.Run("apply", func(t *testing.T) {
		boilerplate.RunApplyTest(t, &Driver{}, newMaterialization, "testdata/apply.flow.yaml", makeResourceFn)
	})

	t.Run("migrate", func(t *testing.T) {
		boilerplate.RunMigrationTest(t, newMaterialization, "testdata/migrate.flow.yaml", makeResourceFn, nil)
	})
}
