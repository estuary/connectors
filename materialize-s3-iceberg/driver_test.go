package main

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestSpec(t *testing.T) {
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	var resp, err = driver{}.
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}

func testConfig(t *testing.T) config {
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	cfg := config{
		Bucket:             os.Getenv("ICEBERG_BUCKET"),
		AWSAccessKeyID:     os.Getenv("ICEBERG_AWS_ACCESS_KEY_ID"),
		AWSSecretAccessKey: os.Getenv("ICEBERG_AWS_SECRET_ACCESS_KEY"),
		Namespace:          "test_namespace",
		Region:             os.Getenv("ICEBERG_REGION_NAME"),
		UploadInterval:     "PT5M",
		Prefix:             os.Getenv("ICEBERG_PREFIX"),
		Catalog: catalogConfig{
			CatalogType: catalogTypeRest,
			URI:         "http://localhost:8090/catalog",
			Token:       "some_token",
			Warehouse:   "test_warehouse",
		},
		Advanced: advancedConfig{
			FeatureFlags: "allow_existing_tables_for_new_bindings",
		},
	}

	return cfg
}

func TestValidateAndApply(t *testing.T) {
	t.Skip("TODO: migrate to new test structure")
}
