package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestFencingCases(t *testing.T) {
	// Because of the number of round-trips required for this test to run it is not run normally.
	// Enable it via the RUN_FENCE_TESTS environment variable. It will take several minutes for this
	// test to complete (you should run it with a sufficient -timeout value).
	if os.Getenv("RUN_FENCE_TESTS") != "yes" {
		t.Skipf("skipping %q: ${RUN_FENCE_TESTS} != \"yes\"", t.Name())
	}

	address, ok := os.LookupEnv("REDSHIFT_ADDRESS")
	if !ok {
		t.Fatal("missing REDSHIFT_ADDRESS environment variable")
	}

	user, ok := os.LookupEnv("REDSHIFT_USER")
	if !ok {
		t.Fatal("missing REDSHIFT_USER environment variable")
	}

	password, ok := os.LookupEnv("REDSHIFT_PASSWORD")
	if !ok {
		t.Fatal("missing REDSHIFT_PASSWORD environment variable")
	}

	database, ok := os.LookupEnv("REDSHIFT_DATABASE")
	if !ok {
		t.Fatal("missing REDSHIFT_DATABASE environment variable")
	}

	cfg := config{
		Address:  address,
		User:     user,
		Password: password,
		Database: database,
	}

	client := client{uri: cfg.toURI()}

	ctx := context.Background()

	sql.RunFenceTestCases(t,
		sql.FenceSnapshotPath,
		client,
		[]string{"temp_test_fencing_checkpoints"},
		rsDialect,
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

func TestSpecification(t *testing.T) {
	var resp, err = newRedshiftDriver().
		Spec(context.Background(), &pm.SpecRequest{EndpointType: pf.EndpointType_AIRBYTE_SOURCE})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}
