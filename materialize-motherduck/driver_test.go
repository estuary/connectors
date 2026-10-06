package connector

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

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
	keyChangeSpec   string
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
		keyChangeSpec:   "testdata/key-change.flow.yaml",
		fenceSpec:       "testdata/fence.flow.yaml",
	}

	// ducklakeVariant targets a MotherDuck-hosted DuckLake database.
	ducklakeVariant = integrationVariant{
		name:            "ducklake",
		duckLake:        true,
		materializeSpec: "testdata/materialize.ducklake.flow.yaml",
		applySpec:       "testdata/apply.ducklake.flow.yaml",
		migrateSpec:     "testdata/migrate.ducklake.flow.yaml",
		keyChangeSpec:   "testdata/key-change.ducklake.flow.yaml",
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
	if variant.duckLake {
		maintainSnapshots(t, variant)
	}

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
	t.Run("key-change-migrate", func(t *testing.T) {
		sql.RunKeyChangeMigrationTest(t, NewDriver(), variant.keyChangeSpec, makeTestResource, nil)
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

const (
	// snapshotRetention is how old a DuckLake snapshot must be before it is
	// expired, as a DuckDB interval. Every test run finishes well within it, so
	// expiry never touches a snapshot a concurrent run still reads.
	snapshotRetention = "1 DAY"
	// snapshotExpiryBatch is the most snapshots one expiry call removes, because
	// MotherDuck fails calls that remove a few hundred at once.
	snapshotExpiryBatch = 100
	// snapshotExpiryCalls and snapshotExpiryBudget bound the work one run spends
	// on expiry. Each run removes a share of the backlog, so a run that stops
	// early leaves the rest to the next.
	snapshotExpiryCalls  = 10
	snapshotExpiryBudget = 15 * time.Second
	// snapshotBacklogLimit is the snapshot count above which the variant fails
	// before it runs. MotherDuck returns internal errors on ordinary statements
	// against a DuckLake database with a large enough backlog.
	snapshotBacklogLimit = 20_000
)

// maintainSnapshots fails the variant if its DuckLake database holds more
// snapshots than snapshotBacklogLimit, and expires old snapshots once the
// variant's subtests finish. A DuckLake database keeps every snapshot until one
// is expired explicitly, so a shared test database grows with every run.
func maintainSnapshots(t *testing.T, variant integrationVariant) {
	t.Helper()

	testutil.RunTestAllTasks(t, variant.materializeSpec, func(t *testing.T, _ []byte, _ string, cfg config) {
		var ctx = context.Background()
		var database = createDuckDialect(nil, true).Literal(cfg.Database)

		db, err := cfg.bareOpen()
		require.NoError(t, err)

		t.Cleanup(func() {
			defer db.Close()

			expired, err := expireSnapshots(ctx, db, database)
			if err != nil {
				t.Logf("expired %d snapshots of %s before stopping: %s", expired, cfg.Database, err)
			} else {
				t.Logf("expired %d snapshots of %s", expired, cfg.Database)
			}
		})

		var count int
		require.NoError(t, db.QueryRowContext(ctx,
			fmt.Sprintf("SELECT count(*) FROM ducklake_snapshots(%s)", database),
		).Scan(&count))
		require.LessOrEqualf(t, count, snapshotBacklogLimit,
			"database %q holds %d snapshots, so expiry is not keeping up with test runs", cfg.Database, count)
	})
}

// expireSnapshots expires snapshots older than snapshotRetention from the
// DuckLake database named by the SQL literal database. It makes at most
// snapshotExpiryCalls calls of up to snapshotExpiryBatch snapshots each within
// snapshotExpiryBudget, and returns the number of snapshots expired along with
// whatever stopped it early.
func expireSnapshots(ctx context.Context, db *stdsql.DB, database string) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, snapshotExpiryBudget)
	defer cancel()

	// The cutoff is held in a session variable, so every statement must run on
	// the same connection.
	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	// ducklake_expire_snapshots accepts no subquery as an argument, so the cutoff
	// is computed first.
	var setCutoff = fmt.Sprintf(`SET VARIABLE snapshot_expiry_cutoff = least(
		now() - INTERVAL %s,
		(SELECT snapshot_time FROM ducklake_snapshots(%s) ORDER BY snapshot_time LIMIT 1 OFFSET %d))`,
		snapshotRetention, database, snapshotExpiryBatch)
	var expire = fmt.Sprintf(
		"SELECT count(*) FROM ducklake_expire_snapshots(%s, older_than => getvariable('snapshot_expiry_cutoff'))",
		database)

	var total int
	for range snapshotExpiryCalls {
		if _, err := conn.ExecContext(ctx, setCutoff); err != nil {
			return total, fmt.Errorf("computing expiry cutoff: %w", err)
		}

		var expired int
		if err := conn.QueryRowContext(ctx, expire).Scan(&expired); err != nil {
			return total, fmt.Errorf("expiring snapshots: %w", err)
		} else if expired == 0 {
			break
		}
		total += expired
	}

	return total, nil
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
