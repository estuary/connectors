//go:build !nodb

package main

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

func TestFencingCases(t *testing.T) {
	var ctx = context.Background()
	var client = client{uri: "postgresql://postgres:postgres@localhost:5432/postgres"}
	sql.RunFenceTestCases(t,
		sql.FenceSnapshotPath,
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

func TestPrereqs(t *testing.T) {
	cfg := config{
		Address:  "localhost:5432",
		User:     "postgres",
		Password: "postgres",
		Database: "postgres",
	}

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
		{
			name: "wrong address",
			cfg: func(cfg config) config {
				cfg.Address = "wrong." + cfg.Address
				return cfg
			},
			want: []error{fmt.Errorf("host at address %q cannot be found", "wrong."+cfg.Address)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, prereqs(context.Background(), &sql.Endpoint{
				Config: tt.cfg(cfg),
				Tenant: "tenant",
			}).Unwrap())
		})
	}
}
