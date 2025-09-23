//go:build !nodb

package main

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"

	_ "github.com/databricks/databricks-sql-go"
)

func mustGetCfg(t *testing.T) config {
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
		return config{}
	}

	out := config{
		Advanced: advancedConfig{
			FeatureFlags: "allow_existing_tables_for_new_bindings",
		},
	}

	for _, prop := range []struct {
		key  string
		dest *string
	}{
		{"DATABRICKS_HOST_NAME", &out.Address},
		{"DATABRICKS_HTTP_PATH", &out.HTTPPath},
		{"DATABRICKS_CATALOG", &out.CatalogName},
		{"DATABRICKS_SCHEMA", &out.SchemaName},
		{"DATABRICKS_ACCESS_TOKEN", &out.Credentials.PersonalAccessToken},
	} {
		*prop.dest = os.Getenv(prop.key)
	}

	out.Credentials.AuthType = PAT_AUTH_TYPE

	if err := out.Validate(); err != nil {
		t.Fatal(err)
	}

	return out
}

func TestValidateAndApply(t *testing.T) {
	ctx := context.Background()

	cfg := mustGetCfg(t)

	resourceConfig := tableConfig{
		Table:  "target",
		Schema: cfg.SchemaName,
	}

	db, err := stdsql.Open("databricks", cfg.ToURI())
	require.NoError(t, err)
	defer db.Close()

	boilerplate.RunValidateAndApplyTestCases(
		t,
		newDatabricksDriver(),
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()

			sch, err := sql.StdGetSchema(ctx, db, cfg.CatalogName, resourceConfig.Schema, resourceConfig.Table)
			require.NoError(t, err)

			return sch
		},
		func(t *testing.T) {
			t.Helper()
			_, _ = db.ExecContext(ctx, fmt.Sprintf("drop table %s;", testDialect.Identifier(resourceConfig.Schema, resourceConfig.Table)))
		},
	)
}

func TestValidateAndApplyMigrations(t *testing.T) {
	ctx := context.Background()

	cfg := mustGetCfg(t)

	resourceConfig := tableConfig{
		Table:  "target",
		Schema: cfg.SchemaName,
	}

	db, err := stdsql.Open("databricks", cfg.ToURI())
	require.NoError(t, err)
	defer db.Close()

	sql.RunValidateAndApplyMigrationsTests(
		t,
		newDatabricksDriver(),
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()

			sch, err := sql.StdGetSchema(ctx, db, cfg.CatalogName, resourceConfig.Schema, resourceConfig.Table)
			require.NoError(t, err)

			return sch
		},
		func(t *testing.T, cols []string, values []string) {
			t.Helper()

			var keys = make([]string, len(cols))
			for i, col := range cols {
				keys[i] = testDialect.Identifier(col)
			}
			keys = append(keys, testDialect.Identifier("_meta/flow_truncated"))
			values = append(values, "0")
			keys = append(keys, testDialect.Identifier("flow_published_at"))
			values = append(values, "'2024-09-13 01:01:01'")
			keys = append(keys, testDialect.Identifier("flow_document"))
			values = append(values, "'{}'")
			q := fmt.Sprintf("insert into %s (%s) VALUES (%s);", testDialect.Identifier(resourceConfig.Schema, resourceConfig.Table), strings.Join(keys, ","), strings.Join(values, ","))
			_, err = db.ExecContext(ctx, q)

			require.NoError(t, err)
		},
		func(t *testing.T) string {
			t.Helper()

			rows, err := sql.DumpTestTable(t, db, testDialect.Identifier(resourceConfig.Schema, resourceConfig.Table))

			require.NoError(t, err)

			return rows
		},
		func(t *testing.T) {
			t.Helper()
			_, _ = db.ExecContext(ctx, fmt.Sprintf("drop table %s;", testDialect.Identifier(resourceConfig.Schema, resourceConfig.Table)))
		},
	)
}
