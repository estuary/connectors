//go:build !nodb

package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	bp_test "github.com/estuary/connectors/materialize-boilerplate/testing"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

func mustGetCfg(t *testing.T) config {
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
		return config{}
	}

	out := config{}

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

func TestValidate(t *testing.T) {
	sql.RunValidateTestCases(t, databricksDialect)
}

func TestApply(t *testing.T) {
	cfg := mustGetCfg(t)
	ctx := context.Background()

	configJson, err := json.Marshal(cfg)
	require.NoError(t, err)

	firstTable := "first-table"
	secondTable := "second-table"

	firstResource := tableConfig{
		Table:  firstTable,
		Schema: cfg.SchemaName,
	}
	firstResourceJson, err := json.Marshal(firstResource)
	require.NoError(t, err)

	secondResource := tableConfig{
		Table:  secondTable,
		Schema: cfg.SchemaName,
	}
	secondResourceJson, err := json.Marshal(secondResource)
	require.NoError(t, err)

	bp_test.RunApplyTestCases(
		t,
		newDatabricksDriver(),
		configJson,
		[2]json.RawMessage{firstResourceJson, secondResourceJson},
		[2][]string{firstResource.Path(), secondResource.Path()},
		func(t *testing.T) []string {
			t.Helper()

			db, err := stdsql.Open("databricks", cfg.ToURI())
			require.NoError(t, err)

			rows, err := sql.StdListTables(ctx, db, cfg.CatalogName, cfg.SchemaName)
			require.NoError(t, err)

			return rows
		},
		func(t *testing.T, resourcePath []string) string {
			t.Helper()

			db, err := stdsql.Open("databricks", cfg.ToURI())
			require.NoError(t, err)

			sch, err := sql.StdGetSchema(ctx, db, cfg.CatalogName, resourcePath[0], resourcePath[1])
			require.NoError(t, err)

			return sch
		},
		func(t *testing.T) {
			t.Helper()

			db, err := stdsql.Open("databricks", cfg.ToURI())
			require.NoError(t, err)

			for _, tbl := range []string{firstTable, secondTable} {
				_, _ = db.ExecContext(ctx, fmt.Sprintf(
					"drop table %s",
					databricksDialect.Identifier([]string{cfg.CatalogName, cfg.SchemaName, tbl}...),
				))
			}

			_, _ = db.ExecContext(ctx, fmt.Sprintf(
				"delete from %s where materialization = 'test/sqlite'",
				databricksDialect.Identifier([]string{cfg.CatalogName, cfg.SchemaName, "flow_materializations_v2"}...),
			))
		},
	)
}
