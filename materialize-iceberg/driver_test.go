package connector

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
	var resp, err = (Driver{}).
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
		URL:          os.Getenv("ICEBERG_CATALOG_URL"),
		Warehouse:    os.Getenv("ICEBERG_WAREHOUSE"),
		Namespace:    ns,
		BaseLocation: os.Getenv("ICEBERG_CATALOG_LOCATION"),
		Compute: computeConfig{
			ComputeType: computeTypeEmrServerless,
			emrConfig: emrConfig{
				AWSAccessKeyID:       os.Getenv("ICEBERG_AWS_ACCESS_KEY_ID"),
				AWSSecretAccessKey:   os.Getenv("ICEBERG_AWS_SECRET_ACCESS_KEY"),
				Region:               os.Getenv("ICEBERG_AWS_REGION_NAME"),
				ApplicationId:        "anything",
				ExecutionRoleArn:     "anything",
				Bucket:               os.Getenv("ICEBERG_BUCKET"),
				SystemsManagerPrefix: os.Getenv("ICEBERG_SYSTEMS_MANAGER_PREFIX"),
			},
		},
	}

	switch os.Getenv("ICEBERG_CATALOG_AUTH_TYPE") {
	case "OAuth 2.0 Client Credentials":
		cfg.CatalogAuthentication.CatalogAuthType = oldCatalogAuthTypeClientCredential
		cfg.CatalogAuthentication.catalogAuthClientCredentialConfig.Oauth2ServerURI = os.Getenv("ICEBERG_CATALOG_OAUTH2_SERVER_URI")
		cfg.CatalogAuthentication.catalogAuthClientCredentialConfig.Credential = os.Getenv("ICEBERG_CATALOG_CREDENTIAL")
		cfg.CatalogAuthentication.catalogAuthClientCredentialConfig.Scope = os.Getenv("ICEBERG_CATALOG_SCOPE")
	case "AWS SigV4":
		cfg.CatalogAuthentication.CatalogAuthType = oldCatalogAuthTypeSigV4
		cfg.CatalogAuthentication.oldCatalogAuthSigV4Config.AWSAccessKeyID = os.Getenv("ICEBERG_AWS_ACCESS_KEY_ID")
		cfg.CatalogAuthentication.oldCatalogAuthSigV4Config.AWSSecretAccessKey = os.Getenv("ICEBERG_AWS_SECRET_ACCESS_KEY")
		cfg.CatalogAuthentication.oldCatalogAuthSigV4Config.Region = os.Getenv("ICEBERG_AWS_REGION_NAME")
		cfg.CatalogAuthentication.oldCatalogAuthSigV4Config.SigningName = os.Getenv("ICEBERG_AWS_SIGNING_NAME")
	}

	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	return cfg
}

func TestValidateAndApply(t *testing.T) {
	t.Skip("TODO: migrate to new test structure")
}

func TestValidateAndApplyLowercaseColumnNames(t *testing.T) {
	t.Skip("TODO: migrate to new test structure")
}
