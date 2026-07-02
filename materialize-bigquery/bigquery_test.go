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

