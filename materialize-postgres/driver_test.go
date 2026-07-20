package main

import (
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"testing"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"

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
		sql.RunMaterializationTest(t, newPostgresDriver(), "testdata/materialize.flow.yaml", makeResourceFn, actionDescSanitizers)
	})

	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, newPostgresDriver(), "testdata/apply.flow.yaml", makeResourceFn)
	})

	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, newPostgresDriver(), "testdata/migrate.flow.yaml", makeResourceFn, actionDescSanitizers)
	})

	t.Run("fence", func(t *testing.T) {
		sql.RunFencingTest(
			t,
			newPostgresDriver(),
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
			require.Equal(t, tt.want, preReqs(context.Background(), tt.cfg(cfg)).Unwrap())
		})
	}
}
