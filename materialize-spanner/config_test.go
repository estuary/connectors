package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestSpannerConfig(t *testing.T) {
	var validConfig = config{
		ProjectID:  "my-gcp-project",
		InstanceID: "my-spanner-instance",
		Database:   "my-database",
	}
	require.NoError(t, validConfig.Validate())

	var noProject = validConfig
	noProject.ProjectID = ""
	require.Error(t, noProject.Validate(), "expected validation error for missing project_id")

	var noInstance = validConfig
	noInstance.InstanceID = ""
	require.Error(t, noInstance.Validate(), "expected validation error for missing instance_id")

	var noDatabase = validConfig
	noDatabase.Database = ""
	require.Error(t, noDatabase.Validate(), "expected validation error for missing database")

	var invalidBatchSize = validConfig
	invalidBatchSize.Advanced.BatchSize = 100000
	require.Error(t, invalidBatchSize.Validate(), "expected validation error for batch_size too large")

	var validBatchSize = validConfig
	validBatchSize.Advanced.BatchSize = 1000
	require.NoError(t, validBatchSize.Validate())
}

func TestSpecification(t *testing.T) {
	var resp, err = newSpannerDriver().
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}

func TestTableConfig(t *testing.T) {
	var validTable = tableConfig{
		Table: "my_table",
	}
	require.NoError(t, validTable.Validate())

	path, delta, err := validTable.Parameters()
	require.NoError(t, err)
	require.Equal(t, []string{"my_table"}, path)
	require.False(t, delta)

	var deltaTable = tableConfig{
		Table: "my_delta_table",
		Delta: true,
	}
	require.NoError(t, deltaTable.Validate())

	path, delta, err = deltaTable.Parameters()
	require.NoError(t, err)
	require.Equal(t, []string{"my_delta_table"}, path)
	require.True(t, delta)

	var noTable = tableConfig{}
	require.Error(t, noTable.Validate(), "expected validation error for missing table")
}
