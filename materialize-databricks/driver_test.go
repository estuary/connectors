//go:build !nodb

package main

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"os"
	"testing"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"

	_ "github.com/databricks/databricks-sql-go"
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
		func(t *testing.T, materialization pf.Materialization) {
			t.Helper()

			_, _ = db.ExecContext(ctx, fmt.Sprintf("drop table %s;", databricksDialect.Identifier(resourceConfig.Schema, resourceConfig.Table)))

			_, _ = db.ExecContext(ctx, fmt.Sprintf(
				"delete from %s where materialization = %s",
				databricksDialect.Identifier(cfg.SchemaName, sql.DefaultFlowMaterializations),
				databricksDialect.Literal(materialization.String()),
			))
		},
	)
}
