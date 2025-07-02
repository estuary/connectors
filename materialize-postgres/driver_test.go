//go:build !nodb

package main

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"strings"
	"testing"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql-v2"
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

func TestValidateAndApply(t *testing.T) {
	ctx := context.Background()

	cfg := testConfig()

	resourceConfig := tableConfig{
		Table:  "target",
		Schema: "public",
	}

	db, err := stdsql.Open("pgx", cfg.ToURI())
	require.NoError(t, err)
	defer db.Close()

	boilerplate.RunValidateAndApplyTestCases(
		t,
		newPostgresDriver(),
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()

			sch, err := sql.StdGetSchema(ctx, db, cfg.Database, resourceConfig.Schema, resourceConfig.Table)
			require.NoError(t, err)

			return sch
		},
		func(t *testing.T) {
			t.Helper()
			_, _ = db.ExecContext(ctx, fmt.Sprintf("drop table %s;", pgDialect.Identifier(resourceConfig.Schema, resourceConfig.Table)))
		},
	)
}

func TestValidateAndApplyMigrations(t *testing.T) {
	ctx := context.Background()

	cfg := testConfig()

	resourceConfig := tableConfig{
		Table:  "target",
		Schema: "public",
	}

	db, err := stdsql.Open("pgx", cfg.ToURI()+"?default_query_exec_mode=exec")
	require.NoError(t, err)
	defer db.Close()

	sql.RunValidateAndApplyMigrationsTests(
		t,
		newPostgresDriver(),
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()

			sch, err := sql.StdGetSchema(ctx, db, cfg.Database, resourceConfig.Schema, resourceConfig.Table)
			require.NoError(t, err)

			return sch
		},
		func(t *testing.T, cols []string, values []string) {
			t.Helper()

			var keys = make([]string, len(cols))
			for i, col := range cols {
				keys[i] = pgDialect.Identifier(col)
			}
			keys = append(keys, pgDialect.Identifier("_meta/flow_truncated"))
			values = append(values, "FALSE")
			keys = append(keys, pgDialect.Identifier("flow_published_at"))
			values = append(values, "'2024-09-13 01:01:01'")
			keys = append(keys, pgDialect.Identifier("flow_document"))
			values = append(values, "'{}'")
			q := fmt.Sprintf("insert into %s (%s) VALUES (%s);", pgDialect.Identifier(resourceConfig.Table), strings.Join(keys, ","), strings.Join(values, ","))
			_, err = db.ExecContext(ctx, q)

			require.NoError(t, err)
		},
		func(t *testing.T) string {
			t.Helper()

			rows, err := sql.DumpTestTable(t, db, pgDialect.Identifier(resourceConfig.Schema, resourceConfig.Table))

			require.NoError(t, err)

			return rows
		},
		func(t *testing.T) {
			t.Helper()
			_, _ = db.ExecContext(ctx, fmt.Sprintf("drop table %s;", pgDialect.Identifier(resourceConfig.Schema, resourceConfig.Table)))
		},
	)
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
