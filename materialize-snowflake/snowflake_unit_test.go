package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestAcknowledgeSubsetLeavesOtherKeysPending(t *testing.T) {
	d := &transactor{
		cp: checkpoint{
			"a_table.v1": {Table: "a_table", Query: "MERGE INTO a", StagedDir: "dir"},
		},
		bindings: []*binding{{target: sql.Table{StateKey: "a_table.v1"}}},
	}

	// The staged entry's state key is not requested: nothing may execute (the
	// nil database would panic if a query ran) and no state update is
	// returned, so the entry remains pending in the persisted state.
	state, err := d.Acknowledge(context.Background(), nil, []string{"other_table.v1"})
	require.NoError(t, err)
	require.Nil(t, state)
	require.NotNil(t, d.cp["a_table.v1"])
	require.True(t, d.didRecovery)
}

func TestSpecification(t *testing.T) {
	var resp, err = newSnowflakeDriver().
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}

func TestValidHost(t *testing.T) {
	for _, tt := range []struct {
		host string
		want error
	}{
		{"orgname-accountname.snowflakecomputing.com", nil},
		{"identifer.snowflakecomputing.com", nil},
		{"ORGNAME-accountname.snowFLAKEcomputing.coM", nil},
		{"orgname-accountname.aws.us-east-2.snowflakecomputing.com", nil},
		{"http://orgname-accountname.snowflakecomputing.com", fmt.Errorf("invalid host %q (must not include a protocol)", "http://orgname-accountname.snowflakecomputing.com")},
		{"https://orgname-accountname.snowflakecomputing.com", fmt.Errorf("invalid host %q (must not include a protocol)", "https://orgname-accountname.snowflakecomputing.com")},
		{"orgname-accountname.snowflakecomputin.com", fmt.Errorf("invalid host %q (must end in snowflakecomputing.com)", "orgname-accountname.snowflakecomputin.com")},
	} {
		t.Run(tt.host, func(t *testing.T) {
			require.Equal(t, tt.want, validHost(tt.host))
		})
	}
}
