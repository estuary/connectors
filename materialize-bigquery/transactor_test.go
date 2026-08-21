package connector

import (
	"context"
	"encoding/json"
	"testing"

	"cloud.google.com/go/bigquery"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestAcknowledgeSubsetLeavesOtherKeysPending(t *testing.T) {
	tr := &transactor{
		primaryShard: true,
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

func TestGroupCheckpointsByBinding(t *testing.T) {
	cp := checkpoint{
		NewCheckpointKey("a.v1", "00000000-7fffffff"): {Query: "MERGE INTO a"},
		NewCheckpointKey("a.v1", "80000000-ffffffff"): {Query: "MERGE INTO a"},
		NewCheckpointKey("b.v1", "00000000-7fffffff"): {Query: "MERGE INTO b"},
		NewCheckpointKey("b.v1", "80000000-ffffffff"): {Query: "MERGE INTO b"},
	}

	i := 0
	for group := range groupCheckpointsByBinding(cp) {
		switch i {
		case 0:
			require.Equal(t, group, []CheckpointKey{
				"a.v1:00000000-7fffffff",
				"a.v1:80000000-ffffffff",
			})
		case 1:
			require.Equal(t, group, []CheckpointKey{
				"b.v1:00000000-7fffffff",
				"b.v1:80000000-ffffffff",
			})
		default:
			t.FailNow()
		}
		i++
	}
}

func TestMergeStatePatches(t *testing.T) {
	tests := []struct {
		name        string
		target      checkpoint
		patches     []json.RawMessage
		expected    checkpoint
		expectedErr error
	}{
		{
			name:   "single shard two bindings",
			target: map[CheckpointKey]*checkpointItem{},
			patches: []json.RawMessage{
				json.RawMessage(`
					{
					  "a.v1:00000000-ffffffff": {
						"JobPrefix": "1",
						"Query": "query1",
						"SourceURIs": [
						  "gs://1"
						],
						"TempTableName": "flow_temp_table_0"
					  },
					  "b.v1:00000000-ffffffff": {
						"JobPrefix": "2",
						"Query": "query2",
						"SourceURIs": [
						  "gs://2"
						],
						"TempTableName": "flow_temp_table_1"
					  }
					}
				`),
			},
			expected: map[CheckpointKey]*checkpointItem{
				NewCheckpointKey("a.v1", "00000000-ffffffff"): &checkpointItem{
					Query:         "query1",
					SourceURIs:    []string{"gs://1"},
					JobPrefix:     "1",
					TempTableName: "flow_temp_table_0",
				},
				NewCheckpointKey("b.v1", "00000000-ffffffff"): &checkpointItem{
					Query:         "query2",
					SourceURIs:    []string{"gs://2"},
					JobPrefix:     "2",
					TempTableName: "flow_temp_table_1",
				},
			},
		},
		{
			name:   "split shard two bindings",
			target: map[CheckpointKey]*checkpointItem{},
			patches: []json.RawMessage{
				json.RawMessage(`
					{
					  "a.v1:00000000-7fffffff": {
						"JobPrefix": "1",
						"Query": "query1",
						"SourceURIs": [
						  "gs://1"
						],
						"TempTableName": "flow_temp_table_0"
					  },
					  "b.v1:00000000-7fffffff": {
						"JobPrefix": "2",
						"Query": "query2",
						"SourceURIs": [
						  "gs://2"
						],
						"TempTableName": "flow_temp_table_1"
					  }
					}
				`),
				json.RawMessage(`
					{
					  "a.v1:80000000-ffffffff": {
						"JobPrefix": "3",
						"Query": "query3",
						"SourceURIs": [
						  "gs://3"
						],
						"TempTableName": "flow_temp_table_0"
					  },
					  "b.v1:80000000-ffffffff": {
						"JobPrefix": "4",
						"Query": "query4",
						"SourceURIs": [
						  "gs://4"
						],
						"TempTableName": "flow_temp_table_1"
					  }
					}
				`),
			},
			expected: map[CheckpointKey]*checkpointItem{
				NewCheckpointKey("a.v1", "00000000-7fffffff"): &checkpointItem{
					Query:         "query1",
					SourceURIs:    []string{"gs://1"},
					JobPrefix:     "1",
					TempTableName: "flow_temp_table_0",
				},
				NewCheckpointKey("b.v1", "00000000-7fffffff"): &checkpointItem{
					Query:         "query2",
					SourceURIs:    []string{"gs://2"},
					JobPrefix:     "2",
					TempTableName: "flow_temp_table_1",
				},
				NewCheckpointKey("a.v1", "80000000-ffffffff"): &checkpointItem{
					Query:         "query3",
					SourceURIs:    []string{"gs://3"},
					JobPrefix:     "3",
					TempTableName: "flow_temp_table_0",
				},
				NewCheckpointKey("b.v1", "80000000-ffffffff"): &checkpointItem{
					Query:         "query4",
					SourceURIs:    []string{"gs://4"},
					JobPrefix:     "4",
					TempTableName: "flow_temp_table_1",
				},
			},
		},
		{
			name:   "split shard two bindings merge patch",
			target: map[CheckpointKey]*checkpointItem{},
			patches: []json.RawMessage{
				json.RawMessage(`null`),
				json.RawMessage(`
					{
					  "a.v1:00000000-7fffffff": {
						"JobPrefix": "1",
						"Query": "query1",
						"SourceURIs": [
						  "gs://1"
						],
						"TempTableName": "flow_temp_table_0"
					  },
					  "b.v1:00000000-7fffffff": {
						"JobPrefix": "2",
						"Query": "query2",
						"SourceURIs": [
						  "gs://2"
						],
						"TempTableName": "flow_temp_table_1"
					  }
					}
				`),
			},
			expected: map[CheckpointKey]*checkpointItem{
				NewCheckpointKey("a.v1", "00000000-7fffffff"): &checkpointItem{
					Query:         "query1",
					SourceURIs:    []string{"gs://1"},
					JobPrefix:     "1",
					TempTableName: "flow_temp_table_0",
				},
				NewCheckpointKey("b.v1", "00000000-7fffffff"): &checkpointItem{
					Query:         "query2",
					SourceURIs:    []string{"gs://2"},
					JobPrefix:     "2",
					TempTableName: "flow_temp_table_1",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := mergeStatePatches(tt.target, tt.patches)
			require.Equal(t, tt.expectedErr, err)
			require.Equal(t, tt.expected, actual)
		})
	}
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
