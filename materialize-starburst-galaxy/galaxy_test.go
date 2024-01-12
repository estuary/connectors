//go:build !nodb

package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"os"
	"slices"
	"strings"
	"testing"

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
		{"GALAXY_HOST", &out.Host},
		{"GALAXY_CATALOG", &out.Catalog},
		{"GALAXY_SCHEMA", &out.Schema},
		{"GALAXY_ACCOUNT", &out.Account},
		{"GALAXY_PASSWORD", &out.Password},
		{"GALAXY_AWS_KEY_ID", &out.AWSAccessKeyID},
		{"GALAXY_AWS_SECRET_KEY", &out.AWSSecretAccessKey},
		{"GALAXY_REGION", &out.Region},
		{"GALAXY_BUCKET", &out.Bucket},
		{"GALAXY_DATA_DIR", &out.DataStoreDir},
	} {
		*prop.dest = os.Getenv(prop.key)
	}

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
		Schema: cfg.Schema,
	}

	db, err := stdsql.Open("trino", cfg.ToURI())
	require.NoError(t, err)
	defer db.Close()

	boilerplate.RunValidateAndApplyTestCases(
		t,
		newGalaxyDriver(),
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()

			sch, err := getSchema(ctx, db, cfg.Catalog, resourceConfig.Schema, resourceConfig.Table)
			require.NoError(t, err)

			return sch
		},
		func(t *testing.T, materialization pf.Materialization) {
			t.Helper()

			_, _ = db.ExecContext(ctx, fmt.Sprintf("drop table %s", testDialect.Identifier(resourceConfig.Schema, resourceConfig.Table)))

			_, _ = db.ExecContext(ctx, fmt.Sprintf(
				"delete from %s where materialization = %s",
				testDialect.Identifier(cfg.Schema, sql.DefaultFlowMaterializations),
				testDialect.Literal(materialization.String()),
			))
		},
	)
}

// sql.StdGetSchema with removed semicolon from query
func getSchema(ctx context.Context, db *stdsql.DB, catalog string, schema string, name string) (string, error) {
	q := fmt.Sprintf(`
	select column_name, is_nullable, data_type
	from information_schema.columns
	where 
		table_catalog = '%s' 
		and table_schema = '%s'
		and table_name = '%s'
`,
		catalog,
		schema,
		name,
	)

	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	type foundColumn struct {
		Name     string
		Nullable string // string "YES" or "NO"
		Type     string
	}

	cols := []foundColumn{}
	for rows.Next() {
		var c foundColumn
		if err := rows.Scan(&c.Name, &c.Nullable, &c.Type); err != nil {
			return "", err
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	slices.SortFunc(cols, func(a, b foundColumn) int {
		return strings.Compare(a.Name, b.Name)
	})

	var out strings.Builder
	enc := json.NewEncoder(&out)
	for _, c := range cols {
		if err := enc.Encode(c); err != nil {
			return "", err
		}
	}

	return out.String(), nil
}

func TestGalaxyConfig(t *testing.T) {
	var validConfig = config{
		Host:               "test.com:400",
		Catalog:            "mycatalog",
		Schema:             "test_schema",
		Account:            "test@galaxy.com/public",
		Password:           "pass",
		AWSAccessKeyID:     "AWSAccessKeyID",
		AWSSecretAccessKey: "AWSSecretAccessKey",
		Region:             "ue-east-1",
		Bucket:             "test-bucket",
		DataStoreDir:       "warehouse/test_dir",
	}
	require.NoError(t, validConfig.Validate())
	var uri = validConfig.ToURI()
	require.Equal(t, "https://test%40galaxy.com%2Fpublic:pass@test.com:400?catalog=mycatalog&schema=test_schema", uri)

	var noHost = validConfig
	noHost.Host = ""
	require.Error(t, noHost.Validate(), "expected validation error")

	var noCatalog = validConfig
	noCatalog.Catalog = ""
	require.Error(t, noCatalog.Validate(), "expected validation error")

	var noSchema = validConfig
	noSchema.Schema = ""
	require.Error(t, noSchema.Validate(), "expected validation error")

	var noAccount = validConfig
	noAccount.Account = ""
	require.Error(t, noAccount.Validate(), "expected validation error")

	var noPassword = validConfig
	noPassword.Password = ""
	require.Error(t, noPassword.Validate(), "expected validation error")

	var noAwsKey = validConfig
	noAwsKey.AWSAccessKeyID = ""
	require.Error(t, noAwsKey.Validate(), "expected validation error")

	var noAwsSecretKey = validConfig
	noAwsSecretKey.AWSSecretAccessKey = ""
	require.Error(t, noAwsSecretKey.Validate(), "expected validation error")

	var noRegion = validConfig
	noRegion.Region = ""
	require.Error(t, noRegion.Validate(), "expected validation error")

	var noBucket = validConfig
	noBucket.Bucket = ""
	require.Error(t, noBucket.Validate(), "expected validation error")

	var noDataStoreDir = validConfig
	noDataStoreDir.DataStoreDir = ""
	require.Error(t, noDataStoreDir.Validate(), "expected validation error")
}

func TestSpecification(t *testing.T) {
	var resp, err = newGalaxyDriver().
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}
