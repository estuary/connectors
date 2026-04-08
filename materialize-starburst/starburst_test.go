package main

import (
	"testing"

	sql "github.com/estuary/connectors/materialize-sql"
)

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	makeResourceFn := func(table string, delta bool) tableConfig {
		return tableConfig{
			Table: table,
		}
	}

	t.Run("materialize", func(t *testing.T) {
		sql.RunMaterializationTest(t, newStarburstDriver(), "testdata/materialize.flow.yaml", makeResourceFn, nil)
	})
	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, newStarburstDriver(), "testdata/apply.flow.yaml", makeResourceFn)
	})
	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, newStarburstDriver(), "testdata/migrate.flow.yaml", makeResourceFn, nil)
	})
}

