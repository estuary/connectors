//go:build !nodb

package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func testConfig() config {
	return config{
		Address:  "localhost:5432",
		User:     "flow",
		Password: "flow",
		Database: "flow",
		Schema:   "public",
		Advanced: advancedConfig{
			FeatureFlags: "allow_existing_tables_for_new_bindings",
		},
	}
}

func TestIntegration(t *testing.T) {
	makeResourceFn := func(table string, delta bool) tableConfig {
		return tableConfig{
			Table: table,
			Delta: delta,
		}
	}

	t.Run("materialize", func(t *testing.T) {
		sql.RunMaterializationTest(t, newPostgresDriver(), "testdata/materialize.flow.yaml", makeResourceFn)
	})

	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, newPostgresDriver(), "testdata/apply.flow.yaml", makeResourceFn)
	})

	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, newPostgresDriver(), "testdata/migrate.flow.yaml", makeResourceFn)
	})
}

func TestFencingCases(t *testing.T) {
	var ctx = context.Background()

	c, err := newClient(ctx, &sql.Endpoint[config]{Config: testConfig()})
	require.NoError(t, err)
	defer c.Close()

	sql.RunFenceTestCases(t,
		c,
		[]string{"temp_test_fencing_checkpoints"},
		pgDialect,
		tplCreateTargetTable,
		func(table sql.Table, fence sql.Fence) error {
			var fenceUpdate strings.Builder
			if err := tplUpdateFence.Execute(&fenceUpdate, fence); err != nil {
				return fmt.Errorf("evaluating fence template: %w", err)
			}
			return c.ExecStatements(ctx, []string{fenceUpdate.String()})
		},
		func(table sql.Table) (out string, err error) {
			return sql.StdDumpTable(ctx, c.(*client).db, table)
		},
	)
}

func TestPrereqs(t *testing.T) {
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
			require.Equal(t, tt.want, preReqs(context.Background(), tt.cfg(cfg), "testing").Unwrap())
		})
	}
}
