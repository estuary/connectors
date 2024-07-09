package main

import (
	"context"
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
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
	}

	return cfg
}

func TestValidateAndApply(t *testing.T) {
	ctx := context.Background()

	cfg := testConfig(t)

	resourceConfig := resource{
		Namespace: "test_namespace",
		Table:     "test_table",
		Delta:     true,
	}

	catalog := glueCatalog{
		cfg: &cfg,
	}

	boilerplate.RunValidateAndApplyTestCases(
		t,
		driver{},
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			is, err := catalog.infoSchema()
			require.NoError(t, err)

			fields, err := is.FieldsForResource(resourceConfig.path())
			require.NoError(t, err)

			slices.SortFunc(fields, func(a, b boilerplate.EndpointField) int {
				return strings.Compare(a.Name, b.Name)
			})

			var out strings.Builder
			enc := json.NewEncoder(&out)
			for _, f := range fields {
				require.NoError(t, enc.Encode(f))
			}

			return out.String()
		},
		func(t *testing.T, materialization pf.Materialization) {
			t.Helper()

			_, do, _ := catalog.DeleteResource(ctx, resourceConfig.path())
			do(ctx)
		},
	)
}
