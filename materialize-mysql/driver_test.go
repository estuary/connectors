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
	var dialect = mysqlDialect(time.FixedZone("UTC", 0))
	var templates = renderTemplates(dialect)
	var client = client{uri: "flow:flow@tcp(localhost:3306)/flow"}
	sql.RunFenceTestCases(t,
		client,
		[]string{"temp_test_fencing_checkpoints"},
		dialect,
		templates["createTargetTable"],
		func(table sql.Table, fence sql.Fence) error {
			var err = client.withDB(func(db *stdsql.DB) error {
				// Option 1: Update using template.
				var fenceUpdate strings.Builder
				if err := templates["updateFence"].Execute(&fenceUpdate, fence); err != nil {
					return fmt.Errorf("evaluating fence template: %w", err)
				}
				var _, err = db.Exec(fenceUpdate.String())

				return err
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
	sql.RunValidateTestCases(t, mysqlDialect(time.FixedZone("UTC", 0)))
}

func TestPrereqs(t *testing.T) {
	cfg := config{
		Address:  "localhost:3306",
		User:     "flow",
		Password: "flow",
		Database: "flow",
	}

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
			want: []string{"incorrect username or password (1045): Access denied for user 'wrongflow'"},
		},
		{
			name: "wrong password",
			cfg: func(cfg config) *config {
				cfg.Password = "wrong" + cfg.Password
				return &cfg
			},
			want: []string{"incorrect username or password (1045): Access denied for user 'flow'"},
		},
		{
			name: "wrong database",
			cfg: func(cfg config) *config {
				cfg.Database = "wrong" + cfg.Database
				return &cfg
			},
			want: []string{"database \"wrongflow\" cannot be accessed, it might not exist or you do not have permission to access it ("},
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var actual = prereqs(context.Background(), &sql.Endpoint{
				Config: tt.cfg(cfg),
				Tenant: "tenant",
			}).Unwrap()

			require.Equal(t, len(tt.want), len(actual))
			for i := 0; i < len(tt.want); i++ {
				require.ErrorContains(t, actual[i], tt.want[i])
			}
		})
	}
}
