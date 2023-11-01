//go:build !nodb

package main

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	sql "github.com/estuary/connectors/materialize-sql"
)

func mustGetCfg(t *testing.T) config {
	if os.Getenv("TESTDB") != "yes" {
		t.Skipf("skipping %q: ${TESTDB} != \"yes\"", t.Name())
		return config{}
	}

	out := config{}
	out.Credentials.AuthType = "PAT"

	for _, prop := range []struct {
		key  string
		dest *string
	}{
		{"DATABRICKS_HOST", &out.Address},
		{"DATABRICKS_HTTP_PATH", &out.HTTPPath},
		{"DATABRICKS_CATALOG", &out.CatalogName},
		{"DATABRICKS_PAT", &out.Credentials.PersonalAccessToken},
	} {
		*prop.dest = os.Getenv(prop.key)
	}

	if err := out.Validate(); err != nil {
		t.Fatal(err)
	}

	return out
}

func TestFencingCases(t *testing.T) {
	// Because of the number of round-trips required for this test to run it is not run normally.
	// Enable it via the TESTDB environment variable. It will take several minutes for this test to
	// complete (you should run it with a sufficient -timeout value).
	cfg := mustGetCfg(t)

	client := client{uri: cfg.ToURI()}

	ctx := context.Background()

	sql.RunFenceTestCases(t,
		client,
		[]string{"temp_test_fencing_checkpoints"},
		databricksDialect,
		tplCreateTargetTable,
		func(table sql.Table, fence sql.Fence) error {
			var err = client.withDB(func(db *stdsql.DB) error {
				var fenceUpdate strings.Builder
				if err := tplUpdateFence.Execute(&fenceUpdate, fence); err != nil {
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
	sql.RunValidateTestCases(t, databricksDialect)
}
