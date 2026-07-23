package connector

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/connectors/materialize-iceberg/python"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestAcknowledgeSubsetLeavesOtherKeysPending(t *testing.T) {
	tr := &transactor{
		cp: map[string]*python.MergeBinding{
			"a_table.v1": {Binding: 0, Query: "MERGE INTO a"},
		},
		bindings: []binding{{Mapped: &boilerplate.MappedBinding[config, resource, mapped]{
			MaterializationSpec_Binding: pf.MaterializationSpec_Binding{StateKey: "a_table.v1"},
		}}},
	}

	// The staged entry's state key is not requested: no merge job may run
	// (the nil compute runner would panic otherwise) and no state update is
	// returned, so the entry remains pending in the persisted state.
	state, err := tr.Acknowledge(context.Background(), nil, []string{"other_table.v1"})
	require.NoError(t, err)
	require.Nil(t, state)
	require.NotNil(t, tr.cp["a_table.v1"])
}

func TestSpec(t *testing.T) {
	var resp, err = (Driver{}).
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}
