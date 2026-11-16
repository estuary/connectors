package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

const (
	fullRangeKey  = "00000000-ffffffff"
	lowerRangeKey = "00000000-7fffffff"
	upperRangeKey = "80000000-ffffffff"
)

func TestLoadResultsTableName(t *testing.T) {
	var name = loadResultsTableName("acmeCo/tests/materialize bigquery.v2", fullRangeKey)
	var prefix = "flow_load_results_acmeCo_tests_materialize_bigquery_v2_00000000-ffffffff_"
	require.True(t, strings.HasPrefix(name, prefix), name)
	require.Len(t, name, len(prefix)+36)
	require.Regexp(t, `^[-_0-9a-zA-Z]+$`, name)
}

func testTransactor(rangeKey string, stateKeys ...string) *transactor {
	var keyBegin, keyEnd uint32
	if _, err := fmt.Sscanf(rangeKey, "%08x-%08x", &keyBegin, &keyEnd); err != nil {
		panic(err)
	}
	var tr = &transactor{
		state: boilerplate.NewShardedState[*checkpointItem](keyBegin, keyEnd, stateKeys),
		be:    &m.BindingEvents{},
	}
	for _, sk := range stateKeys {
		tr.bindings = append(tr.bindings, &binding{target: sql.Table{StateKey: sk}})
	}
	return tr
}

func TestAcknowledgeSubsetLeavesOtherKeysPending(t *testing.T) {
	tr := testTransactor(fullRangeKey, "a_table.v1", "b_table.v1")
	_, err := tr.state.StartCommit(map[string]*checkpointItem{
		"a_table.v1": {Query: "MERGE INTO a", JobPrefix: "jp"},
		"b_table.v1": {Query: "MERGE INTO b", JobPrefix: "jp"},
	})
	require.NoError(t, err)

	// No staged entry's state key is requested: nothing may execute (the nil
	// client would panic if a query ran) and no state update is returned, so
	// the entries remain pending in the persisted state.
	state, err := tr.Acknowledge(context.Background(), nil, []string{"c_table.v1"})
	require.NoError(t, err)
	require.Nil(t, state)
	require.NotNil(t, tr.state.Pending()["a_table.v1"][fullRangeKey])
	require.NotNil(t, tr.state.Pending()["b_table.v1"][fullRangeKey])
}

func TestEntryWithoutTruncationOmitsIt(t *testing.T) {
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(mustMarshal(t, &checkpointItem{JobPrefix: "jp"}), &fields))
	require.NotContains(t, fields, "TruncateBefore")
}

func TestCombineBoundsUnionsRanges(t *testing.T) {
	var keys = []sql.Column{{Identifier: "id", MappedType: sql.MappedType{BareDDL: "INTEGER"}}, {Identifier: "ts", MappedType: sql.MappedType{BareDDL: "TIMESTAMP"}}}
	var bound = func(lower, upper string) mergeBoundLiterals { return mergeBoundLiterals{Lower: lower, Upper: upper} }
	var split = bound("'2026-03-01T00:00:00Z'", "'2026-10-26T12:15:00Z'")
	split.Ranges = []mergeRangeLiterals{
		{"'2026-03-01T00:00:00Z'", "'2026-03-01T00:00:00Z'"},
		{"'2026-10-26T12:00:00Z'", "'2026-10-26T12:15:00Z'"},
	}
	var items = []*checkpointItem{
		{Bounds: []mergeBoundLiterals{bound("1", "10"), split}},
		{Bounds: []mergeBoundLiterals{bound("5", "50"), bound("'2026-10-26T12:10:00Z'", "'2026-10-26T12:30:00Z'")}},
		{Bounds: []mergeBoundLiterals{bound("5", "50"), split}},
	}

	// A recovered entry's ranges survive the persisted state.
	var recovered checkpointItem
	require.NoError(t, json.Unmarshal(mustMarshal(t, items[2]), &recovered))
	items[2] = &recovered

	var out = combineBounds(keys, items)
	require.Nil(t, out[0].Ranges)
	require.Equal(t, "GREATEST(CAST('2026-10-26T12:15:00Z' AS TIMESTAMP), CAST('2026-10-26T12:30:00Z' AS TIMESTAMP))", out[1].LiteralUpper)
	require.Equal(t, []sql.MergeRange{
		{LiteralLower: "'2026-03-01T00:00:00Z'", LiteralUpper: "'2026-03-01T00:00:00Z'"},
		{LiteralLower: "'2026-10-26T12:00:00Z'", LiteralUpper: "'2026-10-26T12:15:00Z'"},
		{LiteralLower: "'2026-10-26T12:10:00Z'", LiteralUpper: "'2026-10-26T12:30:00Z'"},
	}, out[1].Ranges)
}

func TestTimestampLiteralFloorsToMicroseconds(t *testing.T) {
	var ts = time.Date(2026, 9, 29, 12, 34, 56, 999_999_999, time.FixedZone("x", 3600))
	require.Equal(t, "TIMESTAMP '2026-09-29 11:34:56.999999 UTC'", timestampLiteral(ts))
}

func TestUnmarshalState(t *testing.T) {
	var state = json.RawMessage(`{
		"a_table.v1": {
			"00000000-7fffffff": {"Query": "QL", "JobPrefix": "pl"},
			"80000000-ffffffff": {"Query": "QU", "JobPrefix": "pu"}
		}
	}`)

	t.Run("the primary keeps every shard's entries", func(t *testing.T) {
		var tr = testTransactor(lowerRangeKey, "a_table.v1")
		require.NoError(t, tr.UnmarshalState(state))
		require.Equal(t, "QL", tr.state.Pending()["a_table.v1"][lowerRangeKey].Query)
		require.Equal(t, "QU", tr.state.Pending()["a_table.v1"][upperRangeKey].Query)
	})

	t.Run("non-primary discards everything", func(t *testing.T) {
		var tr = testTransactor(upperRangeKey, "a_table.v1")
		require.NoError(t, tr.UnmarshalState(state))
		require.Zero(t, tr.state.Pending().Len())
	})

	t.Run("unknown fields error", func(t *testing.T) {
		var tr = testTransactor(fullRangeKey, "a_table.v1")
		require.Error(t, tr.UnmarshalState(json.RawMessage(`{"a_table.v1": {"00000000-ffffffff": {"Unknown": 1}}}`)))
	})
}

// The primary applies its own and its peers' recovered entries, and clears
// each of them.
func TestAcknowledgeClearsEveryShardsEntries(t *testing.T) {
	var tr = testTransactor(lowerRangeKey, "a_table.v1", "b_table.v1")
	tr.skipCleanup = true
	require.NoError(t, tr.UnmarshalState(json.RawMessage(`{
		"a_table.v1": {
			"00000000-7fffffff": {"JobPrefix": "a-lower"},
			"80000000-ffffffff": {"JobPrefix": "a-upper"}
		},
		"b_table.v1": {"80000000-ffffffff": {"JobPrefix": "b-upper"}}
	}`)))

	update, err := tr.Acknowledge(context.Background(), nil, nil)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"a_table.v1": {"00000000-7fffffff": null, "80000000-ffffffff": null},
		"b_table.v1": {"80000000-ffffffff": null}
	}`, string(update.UpdatedJson))
	require.Zero(t, tr.state.Pending().Len())
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

func TestSchemaForColsMatchesColumnCaseInsensitively(t *testing.T) {
	makeCol := func(field string) *sql.Column {
		return &sql.Column{Projection: sql.Projection{Projection: pf.Projection{Field: field}}}
	}

	fieldSchemas := fieldSchemasByName(bigquery.Schema{
		{Name: "a_b_Foo", Type: bigquery.StringFieldType},
		{Name: "a_b_bar", Type: bigquery.IntegerFieldType},
	})

	got, err := schemaForCols([]*sql.Column{makeCol("a/b/foo"), makeCol("a/b/Bar")}, fieldSchemas)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, bigquery.StringFieldType, got[0].Type)
	require.Equal(t, bigquery.IntegerFieldType, got[1].Type)
}
