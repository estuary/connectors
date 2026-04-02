//go:build !nodb

package main

import (
	"regexp"
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

	sanitizers := []func(string) string{
		func(s string) string {
			return regexp.MustCompile(`[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}`).
				ReplaceAllString(s, "<uuid>")
		},
	}

	t.Run("materialize", func(t *testing.T) {
		sql.RunMaterializationTest(t, newDatabricksDriver(), "testdata/materialize.flow.yaml", makeResourceFn, sanitizers)
	})
	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, newDatabricksDriver(), "testdata/apply.flow.yaml", makeResourceFn)
	})
	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, newDatabricksDriver(), "testdata/migrate.flow.yaml", makeResourceFn, sanitizers)
	})
}
