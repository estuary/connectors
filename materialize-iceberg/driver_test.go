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
		URL:       os.Getenv("ICEBERG_CATALOG_URL"),
		Warehouse: os.Getenv("ICEBERG_WAREHOUSE"),
		Namespace: ns,
		Location:  os.Getenv("ICEBERG_CATALOG_LOCATION"),
		Compute: computeConfig{
			ComputeType: computeTypeEmrServerless,
			emrConfig: emrConfig{
				AWSAccessKeyID:     os.Getenv("ICEBERG_AWS_ACCESS_KEY_ID"),
				AWSSecretAccessKey: os.Getenv("ICEBERG_AWS_SECRET_ACCESS_KEY"),
				Region:             os.Getenv("ICEBERG_AWS_REGION_NAME"),
				ApplicationId:      "anything",
				ExecutionRoleArn:   "anything",
				Bucket:             os.Getenv("ICEBERG_BUCKET"),
			},
		},
	}

	switch os.Getenv("ICEBERG_CATALOG_AUTH_TYPE") {
	case "OAuth 2.0 Client Credentials":
		cfg.CatalogAuthentication.CatalogAuthType = catalogAuthTypeClientCredential
		cfg.CatalogAuthentication.catalogAuthClientCredentialConfig.Credential = os.Getenv("ICEBERG_CATALOG_CREDENTIAL")
		cfg.CatalogAuthentication.catalogAuthClientCredentialConfig.Scope = os.Getenv("ICEBERG_CATALOG_SCOPE")
		cfg.Compute.emrConfig.EmrCatalogAuthentication.EmrAuthType = emrAuthTypeClientCredential
		cfg.Compute.emrConfig.EmrCatalogAuthentication.emrAuthClientCredentialConfig.CredentialSecretName = os.Getenv("ICEBERG_CATALOG_CREDENTIAL_SECRET_NAME")
	case "AWS SigV4":
		cfg.CatalogAuthentication.CatalogAuthType = catalogAuthTypeSigV4
		cfg.CatalogAuthentication.catalogAuthSigV4Config.AWSAccessKeyID = os.Getenv("ICEBERG_AWS_ACCESS_KEY_ID")
		cfg.CatalogAuthentication.catalogAuthSigV4Config.AWSSecretAccessKey = os.Getenv("ICEBERG_AWS_SECRET_ACCESS_KEY")
		cfg.CatalogAuthentication.catalogAuthSigV4Config.Region = os.Getenv("ICEBERG_AWS_REGION_NAME")
		cfg.Compute.emrConfig.EmrCatalogAuthentication.EmrAuthType = emrAuthTypeSigV4
	}

	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
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
			meta, err := catalog.TableMetadata(ctx, resourceConfig.Namespace, resourceConfig.Table)
			require.NoError(t, err)

			return meta.CurrentSchema().String()
		},
		func(t *testing.T) {
			t.Helper()
			catalog.DeleteTable(ctx, resourceConfig.Namespace, resourceConfig.Table)
		},
	)
}
