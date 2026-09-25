package connector

import (
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	testutil "github.com/estuary/connectors/materialize-boilerplate/testutil"
	"github.com/stretchr/testify/require"
)

// placeholderDataset is a standing fixture dataset in the test project that
// exists in a location other than the test configuration's region.
const placeholderDataset = "testing_placeholder"

func TestValidateRejectsEndpointDatasetInAnotherLocation(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	testutil.RunTestAllTasks(t, "testdata/apply.flow.yaml", func(t *testing.T, _ []byte, _ string, cfg config) {
		var ctx = t.Context()

		credOption, err := cfg.CredentialsClientOption()
		require.NoError(t, err)
		bq, err := bigquery.NewClient(ctx, cfg.ProjectID, credOption)
		require.NoError(t, err)
		t.Cleanup(func() { bq.Close() })

		md, err := bq.DatasetInProject(cfg.ProjectID, placeholderDataset).Metadata(ctx)
		require.NoError(t, err, "fixture dataset %s must exist", placeholderDataset)
		require.False(t, strings.EqualFold(md.Location, cfg.Region),
			"fixture dataset %s must be located outside %s", placeholderDataset, cfg.Region)

		// Every binding overrides the dataset, so the endpoint dataset is
		// only ever touched by the load results table.
		var rc = tableConfig{Table: "prereqs_test_validate", Dataset: cfg.Dataset}
		var placeholderCfg = cfg
		placeholderCfg.Dataset = placeholderDataset

		_, err = NewDriver().Validate(ctx, validateResourceReq(t, placeholderCfg, rc, 0, nil))
		require.ErrorContains(t, err, placeholderDataset)
		require.ErrorContains(t, err, md.Location)
		require.ErrorContains(t, err, cfg.Region)

		_, err = NewDriver().Validate(ctx, validateResourceReq(t, cfg, rc, 0, nil))
		require.NoError(t, err)
	})
}
