package main

import (
	"context"
	"testing"

	"cloud.google.com/go/bigquery"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestAcknowledgeSubsetLeavesOtherKeysPending(t *testing.T) {
	tr := &transactor{
		cp: checkpoint{
			"a_table.v1": {Query: "MERGE INTO a", JobPrefix: "jp"},
			"b_table.v1": {Query: "MERGE INTO b", JobPrefix: "jp"},
		},
		bindings: []*binding{
			{target: sql.Table{StateKey: "a_table.v1"}},
			{target: sql.Table{StateKey: "b_table.v1"}},
		},
	}

	// No staged entry's state key is requested: nothing may execute (the nil
	// client would panic if a query ran) and no state update is returned, so
	// the entries remain pending in the persisted state.
	state, err := tr.Acknowledge(context.Background(), nil, []string{"c_table.v1"})
	require.NoError(t, err)
	require.Nil(t, state)
	require.NotNil(t, tr.cp["a_table.v1"])
	require.NotNil(t, tr.cp["b_table.v1"])
}

func TestSchemaForColsStripsPolicyTags(t *testing.T) {
	makeCol := func(field string) *sql.Column {
		return &sql.Column{Projection: sql.Projection{Projection: pf.Projection{Field: field}}}
	}

	fieldSchemas := map[string]*bigquery.FieldSchema{
		"id": {Name: "id", Type: bigquery.StringFieldType, Required: true},
		"birthday": {
			Name: "birthday",
			Type: bigquery.DateFieldType,
			PolicyTags: &bigquery.PolicyTagList{
				Names: []string{"projects/p/locations/us/taxonomies/1/policyTags/2"},
			},
		},
	}

	got, err := schemaForCols([]*sql.Column{makeCol("id"), makeCol("birthday")}, fieldSchemas)
	require.NoError(t, err)

	require.Len(t, got, 2)
	require.Equal(t, "c0", got[0].Name)
	require.Equal(t, "c1", got[1].Name)
	require.Equal(t, bigquery.StringFieldType, got[0].Type)
	require.Equal(t, bigquery.DateFieldType, got[1].Type)

	// The temp/external table schema must never carry the destination
	// column's policy tags: BigQuery enforces column-level security on the
	// query-scoped external table itself, and Fine-Grained Reader grants do
	// not extend to it (https://github.com/estuary/connectors/issues/4833).
	for _, f := range got {
		require.Nil(t, f.PolicyTags, "field %s must not carry policy tags", f.Name)
	}

	// The destination table's field schemas must not be mutated: loadSchema
	// and storeSchema are both built from the same map, and the caller's view
	// of the destination schema should remain intact.
	require.Equal(t, "id", fieldSchemas["id"].Name)
	require.Equal(t, "birthday", fieldSchemas["birthday"].Name)
	require.NotNil(t, fieldSchemas["birthday"].PolicyTags)
}
