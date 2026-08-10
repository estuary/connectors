package connector

import (
	"context"
	"fmt"
	"strings"
	"testing"

	sql "github.com/estuary/connectors/materialize-sql"

	_ "github.com/duckdb/duckdb-go/v2"
)

// integrationVariant is one destination database format the connector is
// expected to work against. MotherDuck serves both classic databases and
// DuckLake databases behind the same `md:` endpoint, and their DDL surfaces
// differ: DuckLake never rewrites data files on ALTER, so it accepts only
// lossless widening type promotions. That difference is not visible from the
// endpoint config, so each format needs its own coverage.
type integrationVariant struct {
	// name is the subtest namespace and the infix of this variant's snapshots.
	name string
	// The flow specs for the variant differ from each other only in the
	// endpoint config they reference.
	materializeSpec string
	applySpec       string
	migrateSpec     string
	fenceSpec       string
}

var (
	// motherduckVariant targets a classic MotherDuck database, whose ALTER
	// TABLE support is that of plain DuckDB.
	motherduckVariant = integrationVariant{
		name:            "motherduck",
		materializeSpec: "testdata/materialize.flow.yaml",
		applySpec:       "testdata/apply.flow.yaml",
		migrateSpec:     "testdata/migrate.flow.yaml",
		fenceSpec:       "testdata/fence.flow.yaml",
	}

	// ducklakeVariant targets a MotherDuck-hosted DuckLake database.
	ducklakeVariant = integrationVariant{
		name:            "ducklake",
		materializeSpec: "testdata/materialize.ducklake.flow.yaml",
		applySpec:       "testdata/apply.ducklake.flow.yaml",
		migrateSpec:     "testdata/migrate.ducklake.flow.yaml",
		fenceSpec:       "testdata/fence.ducklake.flow.yaml",
	}
)

// TestIntegration runs the integration suite against each destination format.
// The variants target separate databases and run concurrently. Select one with
// `-run TestIntegration/ducklake` or `-run TestIntegration/motherduck`.
func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	for _, variant := range []integrationVariant{motherduckVariant, ducklakeVariant} {
		t.Run(variant.name, func(t *testing.T) {
			t.Parallel()
			runIntegrationSuite(t, variant)
		})
	}

	// The fencing snapshots live in materialize-sql/.snapshots, a set shared by
	// every SQL materialization connector and keyed by test name. Fencing
	// exercises checkpoint SQL whose output is identical across destination
	// formats, so it runs once against the classic variant rather than per
	// variant, which would fork connector-specific copies of those shared
	// snapshots. DuckLake still covers flow_checkpoints_v1 creation and updates
	// by way of its materialize subtest.
	t.Run("fence", func(t *testing.T) {
		runFenceSuite(t, motherduckVariant)
	})
}

func makeTestResource(table string, delta bool) tableConfig {
	return tableConfig{
		Table: table,
		Delta: delta,
	}
}

func runIntegrationSuite(t *testing.T, variant integrationVariant) {
	t.Run("materialize", func(t *testing.T) {
		sql.RunMaterializationTest(t, NewDriver(), variant.materializeSpec, makeTestResource, nil)
	})

	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, NewDriver(), variant.applySpec, makeTestResource)
	})

	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, NewDriver(), variant.migrateSpec, makeTestResource, nil)
	})
}

func runFenceSuite(t *testing.T, variant integrationVariant) {
	sql.RunFencingTest(
		t,
		NewDriver(),
		variant.fenceSpec,
		makeTestResource,
		testTemplates.createTargetTable,
		func(ctx context.Context, client sql.Client, fence sql.Fence) error {
			var fenceUpdate strings.Builder
			if err := testTemplates.updateFence.Execute(&fenceUpdate, fence); err != nil {
				return fmt.Errorf("evaluating fence template: %w", err)
			}
			return client.ExecStatements(ctx, []string{fenceUpdate.String()})
		},
	)
}
