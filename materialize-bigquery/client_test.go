package connector

import (
	"sync"
	"testing"

	"cloud.google.com/go/bigquery"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	testutil "github.com/estuary/connectors/materialize-boilerplate/testutil"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// A table can be dropped between a dataset listing returning it and its
// metadata being fetched, as happens with another transaction's load results
// table. Such a table is absent from the information schema rather than an
// error.
func TestPushTableMetadataSkipsDroppedTable(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	testutil.RunTestAllTasks(t, "testdata/apply.flow.yaml", func(t *testing.T, _ []byte, _ string, cfg config) {
		credOption, err := cfg.CredentialsClientOption()
		require.NoError(t, err)
		bq, err := bigquery.NewClient(t.Context(), cfg.ProjectID, credOption)
		require.NoError(t, err)
		t.Cleanup(func() { bq.Close() })

		var is = boilerplate.NewInfoSchema(
			func(rp []string) []string { return rp },
			func(f string) string { return f },
			func(f string) string { return f },
			false, false,
		)
		var mu sync.Mutex
		var dropped = bq.DatasetInProject(cfg.ProjectID, cfg.Dataset).Table("flow_test_dropped_" + uuid.NewString()[:8])

		require.NoError(t, pushTableMetadata(t.Context(), is, &mu, dropped))
		require.Empty(t, is.Resources())
	})
}
