package main

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestSpec(t *testing.T) {
	var resp, err = (&driver{}).
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}

func testConfig(t *testing.T, ns string) config {
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	cfg := config{
		Namespace: ns,
		Catalog: catalogConfig{
			CatalogType: catalogTypeRest,
			restCatalogConfig: restCatalogConfig{
				URL:        os.Getenv("ICEBERG_CATALOG_URL"),
				Credential: os.Getenv("ICEBERG_CATALOG_CREDENTIAL"),
				Warehouse:  os.Getenv("ICEBERG_CATALOG_WAREHOUSE"),
				Scope:      os.Getenv("ICEBERG_CATALOG_SCOPE"),
			},
		},
		Compute: computeConfig{
			ComputeType: computeTypeEmrServerless,
			emrConfig: emrConfig{
				AWSAccessKeyID:     os.Getenv("ICEBERG_AWS_ACCESS_KEY_ID"),
				AWSSecretAccessKey: os.Getenv("ICEBERG_AWS_SECRET_ACCESS_KEY"),
				Region:             os.Getenv("ICEBERG_REGION_NAME"),
				ApplicationId:      "anything",
				ExecutionRoleArn:   "anything",
				Bucket:             os.Getenv("ICEBERG_BUCKET"),
			},
		},
	}

	return cfg
}

func TestValidateAndApply(t *testing.T) {
	ctx := context.Background()

	resourceConfig := resource{
		Namespace: "test_namespace",
		Table:     "test_table",
	}

	cfg := testConfig(t, resourceConfig.Namespace)

	catalog, err := cfg.toCatalog(ctx)
	require.NoError(t, err)

	boilerplate.RunValidateAndApplyTestCases(
		t,
		&driver{},
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			meta, err := catalog.tableMetadata(ctx, resourceConfig.Namespace, resourceConfig.Table)
			require.NoError(t, err)

			return meta.currentSchema().String()
		},
		func(t *testing.T) {
			t.Helper()
			catalog.deleteTable(ctx, resourceConfig.Namespace, resourceConfig.Table)
		},
	)
}
