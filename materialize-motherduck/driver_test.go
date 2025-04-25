package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
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

func TestValidateAndApply(t *testing.T) {
	ctx := context.Background()

	cfg := mustGetCfg(t)

	resourceConfig := tableConfig{
		Table:    "target",
		Schema:   cfg.Schema,
		Delta:    true,
		database: cfg.Database,
	}

	boilerplate.RunValidateAndApplyTestCases(
		t,
		newDuckDriver(),
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()

			db, err := cfg.db(ctx)
			require.NoError(t, err)
			defer db.Close()

			sch, err := sql.StdGetSchema(ctx, db, cfg.Database, resourceConfig.Schema, resourceConfig.Table)
			require.NoError(t, err)

			return sch
		},
		func(t *testing.T) {
			t.Helper()

			db, err := cfg.db(ctx)
			require.NoError(t, err)
			defer db.Close()

			_, _ = db.ExecContext(ctx, fmt.Sprintf("drop table %s;", duckDialect.Identifier(cfg.Database, resourceConfig.Schema, resourceConfig.Table)))
		},
	)
}

func TestValidateAndApplyMigrations(t *testing.T) {
	ctx := context.Background()

	cfg := mustGetCfg(t)

	resourceConfig := tableConfig{
		Table:    "target",
		Schema:   cfg.Schema,
		Delta:    true,
		database: cfg.Database,
	}

	sql.RunValidateAndApplyMigrationsTests(
		t,
		newDuckDriver(),
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()

			db, err := cfg.db(ctx)
			require.NoError(t, err)
			defer db.Close()

			sch, err := sql.StdGetSchema(ctx, db, cfg.Database, resourceConfig.Schema, resourceConfig.Table)
			require.NoError(t, err)

			return sch
		},
		func(t *testing.T, cols []string, values []string) {
			t.Helper()
			db, err := cfg.db(ctx)
			require.NoError(t, err)

			var keys = make([]string, len(cols))
			for i, col := range cols {
				keys[i] = duckDialect.Identifier(col)
			}
			keys = append(keys, duckDialect.Identifier("_meta/flow_truncated"))
			values = append(values, "FALSE")
			keys = append(keys, duckDialect.Identifier("flow_published_at"))
			values = append(values, "'2024-09-13 01:01:01'")
			keys = append(keys, duckDialect.Identifier("flow_document"))
			values = append(values, "'{}'")
			keys = append(keys, duckDialect.Identifier("second_root"))
			values = append(values, "'{}'")
			q := fmt.Sprintf("insert into %s (%s) VALUES (%s);", duckDialect.Identifier(cfg.Database, resourceConfig.Schema, resourceConfig.Table), strings.Join(keys, ","), strings.Join(values, ","))
			_, err = db.ExecContext(ctx, q)

			require.NoError(t, err)
		},
		func(t *testing.T) string {
			t.Helper()

			db, err := cfg.db(ctx)
			require.NoError(t, err)

			rows, err := sql.DumpTestTable(t, db, duckDialect.Identifier(cfg.Database, resourceConfig.Schema, resourceConfig.Table))

			require.NoError(t, err)

			return rows
		},
		func(t *testing.T) {
			t.Helper()

			db, err := cfg.db(ctx)
			require.NoError(t, err)
			defer db.Close()

			_, _ = db.ExecContext(ctx, fmt.Sprintf("drop table %s;", duckDialect.Identifier(cfg.Database, resourceConfig.Schema, resourceConfig.Table)))
		},
	)
}

func TestFencingCases(t *testing.T) {
	var ctx = context.Background()

	var cfg = mustGetCfg(t)

	c, err := newClient(ctx, &sql.Endpoint{Config: &cfg})
	require.NoError(t, err)
	defer c.Close()

	sql.RunFenceTestCases(t,
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
