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
}

func TestSpecification(t *testing.T) {
	var resp, err = (&driver{}).
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
	require.False(t, delta, "delta updates should always be false for Spanner")

	var noTable = tableConfig{}
	require.Error(t, noTable.Validate(), "expected validation error for missing table")
}
