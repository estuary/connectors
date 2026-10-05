package connector

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"github.com/estuary/connectors/go/common"
	"os/exec"
	"strings"
	"testing"

	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/connectors/materialize-boilerplate/testutil"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"

	_ "github.com/microsoft/go-mssqldb"
)

func testConfig() config {
	return config{
		Address:  "localhost:1433",
		User:     "sa",
		Password: "!Flow1234",
		Database: "master",
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

	require.NoError(t, exec.Command("docker", "compose", "-f", "docker-compose.yaml", "up", "--wait").Run())
	t.Cleanup(func() {
		exec.Command("docker", "compose", "-f", "docker-compose.yaml", "down", "-v").Run()
	})

	t.Run("materialize", func(t *testing.T) {
		sql.RunMaterializationTest(t, NewDriver(), "testdata/materialize.flow.yaml", makeResourceFn, nil,
			sql.RuntimeConfig{Shards: 1, Fidelity: m.FidelityTotal})
	})

	t.Run("truncate", func(t *testing.T) {
		if testutil.RuntimeV1() {
			t.Skip("backfill signals require runtime-next")
		}
		var ctx = context.Background()
		var cfg = testConfig()
		connector, err := cfg.ToSQLConnector(ctx)
		require.NoError(t, err)
		var db = stdsql.OpenDB(connector)
		defer db.Close()

		_, err = db.ExecContext(ctx, `DROP TABLE IF EXISTS truncate_standard, truncate_delta, truncate_no_published_at;`)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `IF OBJECT_ID('flow_checkpoints_v1') IS NOT NULL
			DELETE FROM flow_checkpoints_v1 WHERE materialization = 'acmeCo/tests/materialize-sqlserver-truncate';`)
		require.NoError(t, err)

		testutil.RunFlowctl(t, "raw", "preview-next",
			"--name", "acmeCo/tests/materialize-sqlserver-truncate",
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
			var rows, err = db.QueryContext(ctx, "SELECT id FROM "+table+" ORDER BY id;")
			require.NoError(t, err)
			var ids []int64
			for rows.Next() {
				var id int64
				require.NoError(t, rows.Scan(&id))
				ids = append(ids, id)
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			require.Equal(t, want, ids, table)
		}
	})

	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, NewDriver(), "testdata/apply.flow.yaml", makeResourceFn)
	})

	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, NewDriver(), "testdata/migrate.flow.yaml", makeResourceFn, nil)
	})

	t.Run("fence", func(t *testing.T) {
		var templates = renderTemplates(testDialect)
		sql.RunFencingTest(
			t,
			NewDriver(),
			"testdata/fence.flow.yaml",
			makeResourceFn,
			templates.createTargetTable,
			func(ctx context.Context, client sql.Client, fence sql.Fence) error {
				var fenceUpdate strings.Builder
				if err := templates.updateFence.Execute(&fenceUpdate, fence); err != nil {
					return fmt.Errorf("evaluating fence template: %w", err)
				}
				return client.ExecStatements(ctx, []string{fenceUpdate.String()})
			},
		)
	})
}

func TestPrereqs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	require.NoError(t, exec.Command("docker", "compose", "-f", "docker-compose.yaml", "up", "--wait").Run())
	t.Cleanup(func() {
		exec.Command("docker", "compose", "-f", "docker-compose.yaml", "down", "-v").Run()
	})

	cfg := testConfig()

	tests := []struct {
		name string
		cfg  func(config) config
		want []string
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
			want: []string{"Login failed for user 'wrongsa'"},
		},
		{
			name: "wrong password",
			cfg: func(cfg config) config {
				cfg.Password = "wrong" + cfg.Password
				return cfg
			},
			want: []string{"Login failed for user 'sa'"},
		},
		{
			name: "wrong database",
			cfg: func(cfg config) config {
				cfg.Database = "wrong" + cfg.Database
				return cfg
			},
			want: []string{"Cannot open database \"wrongmaster\" that was requested by the login."},
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var actual = preReqs(ctx, tt.cfg(cfg), common.ResolveFlagDefaults(featureFlagDefaults, common.CreatedAt{})).Unwrap()

			require.Equal(t, len(tt.want), len(actual))
			for i := 0; i < len(tt.want); i++ {
				require.ErrorContains(t, actual[i], tt.want[i])
			}
		})
	}
}
