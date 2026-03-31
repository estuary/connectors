//go:build !nodb

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
			Delta: delta,
		}
	}

	t.Run("materialize", func(t *testing.T) {
		sql.RunMaterializationTest(t, newDatabricksDriver(), "testdata/materialize.flow.yaml", makeResourceFn, nil)
	})
	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, newDatabricksDriver(), "testdata/apply.flow.yaml", makeResourceFn)
	})
	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, newDatabricksDriver(), "testdata/migrate.flow.yaml", makeResourceFn, nil)
	})
}
