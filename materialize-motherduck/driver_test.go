package connector

import (
	"context"
	"fmt"
	"strings"
	"testing"

	m "github.com/estuary/connectors/go/materialize"
	testutil "github.com/estuary/connectors/materialize-boilerplate/testutil"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"

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
	// duckLake is the format the variant's database is expected to have, asserted
	// before the suite runs.
	duckLake bool
	// The flow specs for the variant differ from each other only in the
	// endpoint config they reference. fenceSpec is set only for the variant that
	// runs the fencing suite.
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
		duckLake:        false,
		materializeSpec: "testdata/materialize.flow.yaml",
		applySpec:       "testdata/apply.flow.yaml",
		migrateSpec:     "testdata/migrate.flow.yaml",
		fenceSpec:       "testdata/fence.flow.yaml",
	}

	// ducklakeVariant targets a MotherDuck-hosted DuckLake database.
	ducklakeVariant = integrationVariant{
		name:            "ducklake",
		duckLake:        true,
		materializeSpec: "testdata/materialize.ducklake.flow.yaml",
		applySpec:       "testdata/apply.ducklake.flow.yaml",
		migrateSpec:     "testdata/migrate.ducklake.flow.yaml",
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
	requireDatabaseFormat(t, variant)

	t.Run("materialize", func(t *testing.T) {
		sql.RunMaterializationTest(t, NewDriver(), variant.materializeSpec, makeTestResource, nil,
			sql.RuntimeConfig{Shards: 1, Fidelity: m.FidelityTotal})
	})

	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, NewDriver(), variant.applySpec, makeTestResource)
	})

	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, NewDriver(), variant.migrateSpec, makeTestResource, nil)
	})
}

// requireDatabaseFormat fails the variant before it runs anything if its
// destination is not the format it is meant to cover. Both test databases are
// provisioned by hand and a DuckLake database is indistinguishable from a classic
// one in the endpoint configuration, so a database recreated without
// `TYPE ducklake` would leave these subtests passing while quietly exercising the
// classic format twice.
func requireDatabaseFormat(t *testing.T, variant integrationVariant) {
	t.Helper()

	testutil.RunTestAllTasks(t, variant.materializeSpec, func(t *testing.T, _ []byte, taskName string, cfg config) {
		isDuckLake, err := cfg.isDuckLake(context.Background())
		require.NoError(t, err)
		require.Equalf(t, variant.duckLake, isDuckLake,
			"database %q of task %q does not have the format the %q variant covers",
			cfg.Database, taskName, variant.name)
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
