package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"

	_ "github.com/marcboeker/go-duckdb/v2"
)

func mustGetCfg(t *testing.T) config {
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
		return config{}
	}

	out := config{
		StagingBucket: stagingBucketConfig{
			StagingBucketType: stagingBucketTypeS3,
		},
	}
	out.Advanced.FeatureFlags = "allow_existing_tables_for_new_bindings"

	for _, prop := range []struct {
		key  string
		dest *string
	}{
		{"MOTHERDUCK_TOKEN", &out.Token},
		{"MOTHERDUCK_DATABASE", &out.Database},
		{"MOTHERDUCK_SCHEMA", &out.Schema},
		{"MOTHERDUCK_BUCKET", &out.StagingBucket.BucketS3},
		{"AWS_ACCESS_KEY_ID", &out.StagingBucket.AWSAccessKeyID},
		{"AWS_SECRET_ACCESS_KEY", &out.StagingBucket.AWSSecretAccessKey},
		{"AWS_REGION", &out.StagingBucket.Region},
	} {
		*prop.dest = os.Getenv(prop.key)
	}

	require.NoError(t, out.Validate())

	return out
}

func TestIntegration(t *testing.T) {
	makeResourceFn := func(table string, delta bool) tableConfig {
		return tableConfig{
			Table: table,
			Delta: delta,
		}
	}

	t.Run("materialize", func(t *testing.T) {
		sql.RunMaterializationTest(t, newDuckDriver(), "testdata/materialize.flow.yaml", makeResourceFn)
	})

	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, newDuckDriver(), "testdata/apply.flow.yaml", makeResourceFn)
	})

	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, newDuckDriver(), "testdata/migrate.flow.yaml", makeResourceFn)
	})
}

func TestFencingCases(t *testing.T) {
	var ctx = context.Background()

	var cfg = mustGetCfg(t)

	c, err := newClient(ctx, &sql.Endpoint[config]{Config: cfg})
	require.NoError(t, err)
	defer c.Close()

	sql.RunFencingTest(t,
		c,
		[]string{cfg.Database, cfg.Schema, "temp_test_fencing_checkpoints"},
		duckDialect,
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

func TestSpecification(t *testing.T) {
	var resp, err = newDuckDriver().
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}
