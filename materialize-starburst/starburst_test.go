//go:build !nodb

package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	makeResourceFn := func(table string, delta bool) tableConfig {
		return tableConfig{
			Table: table,
		}
	}

	t.Run("materialize", func(t *testing.T) {
		sql.RunMaterializationTest(t, newStarburstDriver(), "testdata/materialize.flow.yaml", makeResourceFn, nil)
	})
	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, newStarburstDriver(), "testdata/apply.flow.yaml", makeResourceFn)
	})
	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, newStarburstDriver(), "testdata/migrate.flow.yaml", makeResourceFn, nil)
	})
}

func TestStarburstConfig(t *testing.T) {
	var validConfig = config{
		Host:               "test.com:400",
		Catalog:            "mycatalog",
		Schema:             "test_schema",
		Account:            "test@acme.com/public",
		Password:           "pass",
		AWSAccessKeyID:     "AWSAccessKeyID",
		AWSSecretAccessKey: "AWSSecretAccessKey",
		Region:             "ue-east-1",
		Bucket:             "test-bucket",
		BucketPath:         "warehouse/test_dir",
	}
	require.NoError(t, validConfig.Validate())
	var uri = validConfig.ToURI()
	require.Equal(t, "https://test%40acme.com%2Fpublic:pass@test.com:400?catalog=mycatalog&schema=test_schema", uri)

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
	noDataStoreDir.BucketPath = ""
	require.Error(t, noDataStoreDir.Validate(), "expected validation error")
}

func TestSpecification(t *testing.T) {
	var resp, err = newStarburstDriver().
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}
