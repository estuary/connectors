//go:build !nodb

package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/require"
)

func testConfig() *config {
	return &config{
		Address:  "localhost:1433",
		User:     "sa",
		Password: "!Flow1234",
		Database: "flow",
	}
}

func TestFencingCases(t *testing.T) {
	var ctx = context.Background()
	var dialect = testDialect
	var templates = renderTemplates(dialect)

	c, err := newClient(ctx, &sql.Endpoint{Config: testConfig(), Dialect: dialect})
	require.NoError(t, err)
	defer c.Close()

	sql.RunFenceTestCases(t,
		c,
		[]string{"temp_test_fencing_checkpoints"},
		dialect,
		templates["createTargetTable"],
		func(table sql.Table, fence sql.Fence) error {
			var fenceUpdate strings.Builder
			if err := templates["updateFence"].Execute(&fenceUpdate, fence); err != nil {
				return fmt.Errorf("evaluating fence template: %w", err)
			}
			return c.ExecStatements(ctx, []string{fenceUpdate.String()})
		},
		func(table sql.Table) (out string, err error) {
			out, err = sql.StdDumpTable(ctx, c.(*client).db, table)
			// SQLServer quotes "checkpoint" because it is a reserved word. The
			// snapshots for the test expect a checkpoint without quotes, so this is
			// a hack to allow this test to proceed
			return strings.Replace(out, "\"checkpoint\"", "checkpoint", 1), err
		},
	)
}

func TestApply(t *testing.T) {
	ctx := context.Background()

	cfg := testConfig()

	configJson, err := json.Marshal(cfg)
	require.NoError(t, err)

	firstTable := "first-table"
	secondTable := "second-table"

	firstResource := tableConfig{
		Table: firstTable,
	}
	firstResourceJson, err := json.Marshal(firstResource)
	require.NoError(t, err)

	secondResource := tableConfig{
		Table: secondTable,
	}
	secondResourceJson, err := json.Marshal(secondResource)
	require.NoError(t, err)

	boilerplate.RunApplyTestCases(
		t,
		newSqlServerDriver(),
		configJson,
		[2]json.RawMessage{firstResourceJson, secondResourceJson},
		[2][]string{firstResource.Path(), secondResource.Path()},
		func(t *testing.T) []string {
			t.Helper()

			db, err := stdsql.Open("sqlserver", cfg.ToURI())
			require.NoError(t, err)

			rows, err := sql.StdListTables(ctx, db, cfg.Database, "dbo")
			require.NoError(t, err)

			return rows
		},
		func(t *testing.T, resourcePath []string) string {
			t.Helper()

			db, err := stdsql.Open("sqlserver", cfg.ToURI())
			require.NoError(t, err)

			sch, err := sql.StdGetSchema(ctx, db, cfg.Database, "dbo", resourcePath[0])
			require.NoError(t, err)

			return sch
		},
		func(t *testing.T) {
			t.Helper()

			db, err := stdsql.Open("sqlserver", cfg.ToURI())
			require.NoError(t, err)

			for _, tbl := range []string{firstTable, secondTable} {
				_, _ = db.ExecContext(ctx, fmt.Sprintf(
					"drop table %s",
					testDialect.Identifier(tbl),
				))
			}

			_, _ = db.ExecContext(ctx, fmt.Sprintf(
				"delete from %s where materialization = 'test/sqlite'",
				testDialect.Identifier("flow_materializations_v2"),
			))
		},
	)
}

func TestPrereqs(t *testing.T) {
	cfg := testConfig()

	tests := []struct {
		name string
		cfg  func(config) *config
		want []string
	}{
		{
			name: "valid",
			cfg:  func(cfg config) *config { return &cfg },
			want: nil,
		},
		{
			name: "wrong username",
			cfg: func(cfg config) *config {
				cfg.User = "wrong" + cfg.User
				return &cfg
			},
			want: []string{"Login failed for user 'wrongsa'"},
		},
		{
			name: "wrong password",
			cfg: func(cfg config) *config {
				cfg.Password = "wrong" + cfg.Password
				return &cfg
			},
			want: []string{"Login failed for user 'sa'"},
		},
		{
			name: "wrong database",
			cfg: func(cfg config) *config {
				cfg.Database = "wrong" + cfg.Database
				return &cfg
			},
			want: []string{"Cannot open database \"wrongflow\" that was requested by the login."},
		},
		{
			name: "wrong address",
			cfg: func(cfg config) *config {
				cfg.Address = "wrong." + cfg.Address
				return &cfg
			},
			want: []string{fmt.Sprintf("host at address %q cannot be found", "wrong."+cfg.Address)},
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg(*cfg)

			c, err := newClient(ctx, &sql.Endpoint{Config: cfg})
			require.NoError(t, err)
			defer c.Close()

			var actual = c.PreReqs(ctx).Unwrap()

			require.Equal(t, len(tt.want), len(actual))
			for i := 0; i < len(tt.want); i++ {
				require.ErrorContains(t, actual[i], tt.want[i])
			}
		})
	}
}
