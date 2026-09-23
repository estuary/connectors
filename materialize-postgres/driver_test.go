package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"

	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/connectors/materialize-boilerplate/testutil"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func testConfig() config {
	return config{
		Address:  *dbAddress,
		User:     "flow",
		Password: "flow",
		Database: "flow",
		Schema:   "public",
		Advanced: advancedConfig{
			NoFlowDocument: true,
			FeatureFlags:   "allow_existing_tables_for_new_bindings",
		},
	}
}

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

	// Enum type names embed the full table name, which includes the test-run-specific random
	// suffix. The test framework strips the full rndSuffix via strings.ReplaceAll, but when
	// the table name is truncated to 42 bytes inside the enum type name (hash mode), only a
	// prefix of rndSuffix survives. We strip that truncated prefix here too.
	// Additionally, the 8-hex hash in hash-mode type names is computed from the random table
	// name and therefore changes per run; normalize it to a stable placeholder.
	actionDescSanitizers := []func(string) string{
		func(s string) string {
			// Strip truncated test-suffix components (_<uuid8>_flow_test_<partial_ts>) that
			// remain after the full rndSuffix has already been removed by the framework.
			return regexp.MustCompile(`_[0-9a-f]{8}_flow_test_\d+`).ReplaceAllString(s, "")
		},
		func(s string) string {
			// Normalize the content-addressed hash in hash-mode enum type names.
			return regexp.MustCompile(`_[0-9a-f]{8}_flow_enum`).ReplaceAllString(s, "_<hash>_flow_enum")
		},
	}

	require.NoError(t, exec.Command("docker", "compose", "-f", "docker-compose.yaml", "up", "--wait").Run())
	t.Cleanup(func() {
		exec.Command("docker", "compose", "-f", "docker-compose.yaml", "down", "-v").Run()
	})

	t.Run("materialize", func(t *testing.T) {
		sql.RunMaterializationTest(t, NewDriver(), "testdata/materialize.flow.yaml", makeResourceFn, actionDescSanitizers,
			sql.RuntimeConfig{Shards: 1, Fidelity: m.FidelityTotal})
	})

	t.Run("truncate", func(t *testing.T) {
		var ctx = context.Background()
		var rawCfg, err = os.ReadFile("testdata/config.local.yaml")
		require.NoError(t, err)
		var cfgMap map[string]any
		require.NoError(t, yaml.Unmarshal(rawCfg, &cfgMap))
		cfgJSON, err := json.Marshal(cfgMap)
		require.NoError(t, err)
		var cfg config
		require.NoError(t, json.Unmarshal(cfgJSON, &cfg))
		uri, err := cfg.ToURI(ctx)
		require.NoError(t, err)

		conn, err := pgx.Connect(ctx, uri)
		require.NoError(t, err)
		defer conn.Close(ctx)

		_, err = conn.Exec(ctx, `DROP TABLE IF EXISTS truncate_standard, truncate_delta, truncate_no_published_at;`)
		require.NoError(t, err)
		_, err = conn.Exec(ctx, `DO $$ BEGIN
			IF to_regclass('flow_checkpoints_v1') IS NOT NULL THEN
				DELETE FROM flow_checkpoints_v1 WHERE materialization = 'acmeCo/tests/materialize-postgres-truncate';
			END IF;
		END $$;`)
		require.NoError(t, err)

		testutil.RunFlowctl(t, "raw", "preview-next",
			"--name", "acmeCo/tests/materialize-postgres-truncate",
			"--source", "testdata/truncate.flow.yaml",
			"--fixture", "testdata/truncate.fixture.json",
			"--shards", "1",
			"--timeout", "5m",
			"--network", "flow-test",
		)

		// The fixture stores ids 1-3, then re-stores only id 1 during a
		// backfill. Only the standard table with a flow_published_at column
		// loses the rows published before the backfill.
		for table, want := range map[string][]int64{
			"truncate_standard":        {1},
			"truncate_delta":           {1, 1, 2, 3},
			"truncate_no_published_at": {1, 2, 3},
		} {
			var rows, err = conn.Query(ctx, "SELECT id FROM "+table+" ORDER BY id;")
			require.NoError(t, err)
			ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
			require.NoError(t, err)
			require.Equal(t, want, ids, table)
		}
	})

	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, NewDriver(), "testdata/apply.flow.yaml", makeResourceFn)
	})

	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, NewDriver(), "testdata/migrate.flow.yaml", makeResourceFn, actionDescSanitizers)
	})

	t.Run("fence", func(t *testing.T) {
		sql.RunFencingTest(
			t,
			NewDriver(),
			"testdata/fence.flow.yaml",
			makeResourceFn,
			testTemplates.createTargetTable,
			func(ctx context.Context, c sql.Client, fence sql.Fence) error {
				var fenceUpdate strings.Builder
				if err := testTemplates.updateFence.Execute(&fenceUpdate, fence); err != nil {
					return fmt.Errorf("evaluating fence template: %w", err)
				}
				// The fence update is parameterized, so it must be executed with
				// bind arguments rather than as a plain statement, mirroring the
				// driver's own fenced-off detection via RowsAffected.
				res, err := c.(*client).db.ExecContext(ctx, fenceUpdate.String(), fenceUpdateArgs(fence)...)
				if err != nil {
					return fmt.Errorf("updating fence: %w", err)
				} else if rows, err := res.RowsAffected(); err != nil {
					return fmt.Errorf("fetching fence update rows: %w", err)
				} else if rows != 1 {
					return fmt.Errorf("this instance was fenced off by another")
				}
				return nil
			},
		)
	})
}

func TestPrereqs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	t.Skip("todo: fix pre-reqs tests")

	cfg := testConfig()

	tests := []struct {
		name string
		cfg  func(config) config
		want []error
	}{
		{
			name: "valid",
			cfg:  func(cfg config) config { return cfg },
			want: nil,
		},
		{
			name: "wrong username",
			cfg: func(cfg config) config {
				cfg.User = "wrong" + cfg.User
				return cfg
			},
			want: []error{fmt.Errorf("incorrect username or password")},
		},
		{
			name: "wrong password",
			cfg: func(cfg config) config {
				cfg.Password = "wrong" + cfg.Password
				return cfg
			},
			want: []error{fmt.Errorf("incorrect username or password")},
		},
		{
			name: "wrong database",
			cfg: func(cfg config) config {
				cfg.Database = "wrong" + cfg.Database
				return cfg
			},
			want: []error{fmt.Errorf("database %q does not exist", "wrong"+cfg.Database)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, preReqs(context.Background(), tt.cfg(cfg), testFlagDefaults()).Unwrap())
		})
	}
}
