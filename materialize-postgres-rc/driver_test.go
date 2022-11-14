//go:build !nodb

package main

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	sql "github.com/estuary/connectors/materialize-sql"
)

func TestFencingCases(t *testing.T) {
	rand.Seed(time.Now().Unix())

	var ctx = context.Background()
	var client = client{uri: "postgresql://postgres:postgres@localhost:5432/postgres"}
	sql.RunFenceTestCases(t,
		client,
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
