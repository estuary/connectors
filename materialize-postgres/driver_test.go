//go:build !nodb

package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	bp_test "github.com/estuary/connectors/materialize-boilerplate/testing"
	sql "github.com/estuary/connectors/materialize-sql"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/stretchr/testify/require"
)

func TestFencingCases(t *testing.T) {
	var ctx = context.Background()
	var client = client{uri: "postgresql://postgres:postgres@localhost:5432/postgres"}
	sql.RunFenceTestCases(t,
		client,
		[]string{"temp_test_fencing_checkpoints"},
		pgDialect,
		tplCreateTargetTable,
		func(table sql.Table, fence sql.Fence) error {
			var err = client.withDB(func(db *stdsql.DB) error {
				// Pick a path randomly to ensure both are exercised and correct.
				if time.Now().Unix()%2 == 0 {

					// Option 1: Update using template.
					var fenceUpdate strings.Builder
					if err := tplUpdateFence.Execute(&fenceUpdate, fence); err != nil {
						return fmt.Errorf("evaluating fence template: %w", err)
					}
					var _, err = db.Exec(fenceUpdate.String())
					return err
				}

				// Option 2: Update using StdUpdateFence.
				var tx, err = db.BeginTx(ctx, nil)
				if err != nil {
					return err
				} else if err = sql.StdUpdateFence(ctx, tx, table, fence); err != nil {
					return err
				}
				return tx.Commit()
			})
			return err
		},
		func(table sql.Table) (out string, err error) {
			err = client.withDB(func(db *stdsql.DB) error {
				out, err = sql.StdDumpTable(ctx, db, table)
				return err
			})
			return
		},
	)
}

func TestValidate(t *testing.T) {
	sql.RunValidateTestCases(t, pgDialect)
}

func TestApply(t *testing.T) {
	ctx := context.Background()

	cfg := config{
		Address:  "localhost:5432",
		User:     "postgres",
		Password: "postgres",
		Schema:   "public",
	}
	catalog := "postgres"

	configJson, err := json.Marshal(cfg)
	require.NoError(t, err)

	firstTable := "first-table"
	secondTable := "second-table"

	firstResource := tableConfig{
		Table:  firstTable,
		Schema: cfg.Schema,
	}
	firstResourceJson, err := json.Marshal(firstResource)
	require.NoError(t, err)

	secondResource := tableConfig{
		Table:  secondTable,
		Schema: cfg.Schema,
	}
	secondResourceJson, err := json.Marshal(secondResource)
	require.NoError(t, err)

	bp_test.RunApplyTestCases(
		t,
		newPostgresDriver(),
		configJson,
		[2]json.RawMessage{firstResourceJson, secondResourceJson},
		[2][]string{firstResource.Path(), secondResource.Path()},
		func(t *testing.T) []string {
			t.Helper()

			db, err := stdsql.Open("pgx", cfg.ToURI())
			require.NoError(t, err)

			rows, err := sql.StdListTables(ctx, db, catalog, cfg.Schema)
			require.NoError(t, err)

			return rows
		},
		func(t *testing.T, resourcePath []string) string {
			t.Helper()

			db, err := stdsql.Open("pgx", cfg.ToURI())
			require.NoError(t, err)

			sch, err := sql.StdGetSchema(ctx, db, catalog, resourcePath[0], resourcePath[1])
			require.NoError(t, err)

			return sch
		},
		func(t *testing.T) {
			t.Helper()

			db, err := stdsql.Open("pgx", cfg.ToURI())
			require.NoError(t, err)

			for _, tbl := range []string{firstTable, secondTable} {
				_, _ = db.ExecContext(ctx, fmt.Sprintf(
					"drop table %s",
					pgDialect.Identifier(cfg.Schema, tbl),
				))
			}

			_, _ = db.ExecContext(ctx, fmt.Sprintf(
				"delete from %s where materialization = 'test/sqlite'",
				pgDialect.Identifier(cfg.Schema, "flow_materializations_v2"),
			))
		},
	)
}

func TestPrereqs(t *testing.T) {
	cfg := config{
		Address:  "localhost:5432",
		User:     "postgres",
		Password: "postgres",
		Database: "postgres",
	}

	tests := []struct {
		name string
		cfg  func(config) *config
		want []error
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
			want: []error{fmt.Errorf("incorrect username or password")},
		},
		{
			name: "wrong password",
			cfg: func(cfg config) *config {
				cfg.Password = "wrong" + cfg.Password
				return &cfg
			},
			want: []error{fmt.Errorf("incorrect username or password")},
		},
		{
			name: "wrong database",
			cfg: func(cfg config) *config {
				cfg.Database = "wrong" + cfg.Database
				return &cfg
			},
			want: []error{fmt.Errorf("database %q does not exist", "wrong"+cfg.Database)},
		},
		{
			name: "wrong address",
			cfg: func(cfg config) *config {
				cfg.Address = "wrong." + cfg.Address
				return &cfg
			},
			want: []error{fmt.Errorf("host at address %q cannot be found", "wrong."+cfg.Address)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg(cfg)
			client := client{uri: cfg.ToURI()}
			require.Equal(t, tt.want, client.PreReqs(context.Background(), &sql.Endpoint{
				Config: cfg,
				Tenant: "tenant",
			}).Unwrap())
		})
	}
}
