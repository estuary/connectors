package connector

import (
	"context"
	"encoding/json"
	"testing"

	"cloud.google.com/go/bigquery"
	m "github.com/estuary/connectors/go/materialize"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

const (
	fullRangeKey  = "00000000-ffffffff"
	lowerRangeKey = "00000000-7fffffff"
	upperRangeKey = "80000000-ffffffff"
)

func testTransactor(rangeKey string, stateKeys ...string) *transactor {
	var tr = &transactor{
		cp:                    make(checkpoint),
		peerShardsCheckpoints: make(rangeCheckpoints),
		primaryShard:          rangeKey == fullRangeKey || rangeKey == lowerRangeKey,
		rangeKey:              rangeKey,
		be:                    &m.BindingEvents{},
	}
	for _, sk := range stateKeys {
		tr.bindings = append(tr.bindings, &binding{target: sql.Table{StateKey: sk}})
	}
	return tr
}

func TestAcknowledgeSubsetLeavesOtherKeysPending(t *testing.T) {
	tr := testTransactor(fullRangeKey, "a_table.v1", "b_table.v1")
	tr.cp["a_table.v1"] = &checkpointItem{Query: "MERGE INTO a", JobPrefix: "jp"}
	tr.cp["b_table.v1"] = &checkpointItem{Query: "MERGE INTO b", JobPrefix: "jp"}

	// No staged entry's state key is requested: nothing may execute (the nil
	// client would panic if a query ran) and no state update is returned, so
	// the entries remain pending in the persisted state.
	state, err := tr.Acknowledge(context.Background(), nil, []string{"c_table.v1"})
	require.NoError(t, err)
	require.Nil(t, state)
	require.NotNil(t, tr.cp["a_table.v1"])
	require.NotNil(t, tr.cp["b_table.v1"])
}

func TestUnmarshalStateRouting(t *testing.T) {
	var mixedState = json.RawMessage(`{
		"00000000-7fffffff": {"a_table.v1": {"Query": "QL", "JobPrefix": "pl"}},
		"80000000-ffffffff": {"a_table.v1": {"Query": "QU", "JobPrefix": "pu"}},
		"a_table.v1": {"Query": "Q0", "JobPrefix": "p0"}
	}`)

	t.Run("legacy top-level entries route to the legacy bucket", func(t *testing.T) {
		var tr = testTransactor(fullRangeKey, "a_table.v1", "b_table.v1")
		require.NoError(t, tr.UnmarshalState(json.RawMessage(`{
			"a_table.v1": {"Query": "Q1", "JobPrefix": "p1"},
			"b_table.v1": {"Query": "Q2", "JobPrefix": "p2"}
		}`)))
		require.Empty(t, tr.cp)
		require.Len(t, tr.peerShardsCheckpoints[legacyRangeKey], 2)
		require.Equal(t, "Q1", tr.peerShardsCheckpoints[legacyRangeKey]["a_table.v1"].Query)
	})

	t.Run("single shard routes own range to cp, others to peers", func(t *testing.T) {
		var tr = testTransactor(fullRangeKey, "a_table.v1")
		require.NoError(t, tr.UnmarshalState(json.RawMessage(`{
			"00000000-ffffffff": {"a_table.v1": {"Query": "OWN", "JobPrefix": "po"}},
			"00000000-7fffffff": {"a_table.v1": {"Query": "QL", "JobPrefix": "pl"}},
			"a_table.v1": {"Query": "Q0", "JobPrefix": "p0"}
		}`)))
		require.Equal(t, "OWN", tr.cp["a_table.v1"].Query)
		require.Equal(t, "QL", tr.peerShardsCheckpoints[lowerRangeKey]["a_table.v1"].Query)
		require.Equal(t, "Q0", tr.peerShardsCheckpoints[legacyRangeKey]["a_table.v1"].Query)
	})

	t.Run("multi-shard primary routes own range to cp", func(t *testing.T) {
		var tr = testTransactor(lowerRangeKey, "a_table.v1")
		require.NoError(t, tr.UnmarshalState(mixedState))
		require.Equal(t, "QL", tr.cp["a_table.v1"].Query)
		require.Equal(t, "QU", tr.peerShardsCheckpoints[upperRangeKey]["a_table.v1"].Query)
		require.Equal(t, "Q0", tr.peerShardsCheckpoints[legacyRangeKey]["a_table.v1"].Query)
	})

	t.Run("non-primary discards everything", func(t *testing.T) {
		var tr = testTransactor(upperRangeKey, "a_table.v1")
		require.NoError(t, tr.UnmarshalState(mixedState))
		require.Empty(t, tr.cp)
		require.Empty(t, tr.peerShardsCheckpoints)
	})
}

func TestMergePeerStatePatches(t *testing.T) {
	var patches = func(ps ...string) []json.RawMessage {
		var out []json.RawMessage
		for _, p := range ps {
			out = append(out, json.RawMessage(p))
		}
		return out
	}

	t.Run("no-op when non-primary", func(t *testing.T) {
		var tr = testTransactor(upperRangeKey)
		require.NoError(t, tr.mergePeerStatePatches(patches(`{"00000000-7fffffff": {"a_table.v1": {"Query": "Q", "JobPrefix": "p"}}}`)))
		require.Empty(t, tr.peerShardsCheckpoints)
	})

	t.Run("own contribution is skipped, peers merged", func(t *testing.T) {
		var tr = testTransactor(lowerRangeKey, "a_table.v1")
		require.NoError(t, tr.mergePeerStatePatches(patches(
			`{"00000000-7fffffff": {"a_table.v1": {"Query": "OWN", "JobPrefix": "po"}}}`,
			`{"80000000-ffffffff": {"a_table.v1": {"Query": "PEER", "JobPrefix": "pp"}}}`,
		)))
		require.Empty(t, tr.cp) // own patch is not folded back
		require.Len(t, tr.peerShardsCheckpoints, 1)
		require.Equal(t, "PEER", tr.peerShardsCheckpoints[upperRangeKey]["a_table.v1"].Query)
	})

	t.Run("state reset patch errors", func(t *testing.T) {
		var tr = testTransactor(lowerRangeKey, "a_table.v1")
		require.Error(t, tr.mergePeerStatePatches(patches(`null`, `{"a": 1}`)))
	})
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
