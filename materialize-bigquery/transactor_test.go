package connector

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

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

func TestLoadResultsTableName(t *testing.T) {
	var name = loadResultsTableName("acmeCo/tests/materialize bigquery.v2", fullRangeKey)
	var prefix = "flow_load_results_acmeCo_tests_materialize_bigquery_v2_00000000-ffffffff_"
	require.True(t, strings.HasPrefix(name, prefix), name)
	require.Len(t, name, len(prefix)+36)
	require.Regexp(t, `^[-_0-9a-zA-Z]+$`, name)
}

func testTransactor(rangeKey string, stateKeys ...string) *transactor {
	var tr = &transactor{
		cp:           make(checkpoints),
		primaryShard: rangeKey == fullRangeKey || rangeKey == lowerRangeKey,
		rangeKey:     rangeKey,
		be:           &m.BindingEvents{},
	}
	for _, sk := range stateKeys {
		tr.bindings = append(tr.bindings, &binding{target: sql.Table{StateKey: sk}})
	}
	return tr
}

func TestAcknowledgeSubsetLeavesOtherKeysPending(t *testing.T) {
	tr := testTransactor(fullRangeKey, "a_table.v1", "b_table.v1")
	tr.cp.add("a_table.v1", fullRangeKey, &checkpointItem{Query: "MERGE INTO a", JobPrefix: "jp"})
	tr.cp.add("b_table.v1", fullRangeKey, &checkpointItem{Query: "MERGE INTO b", JobPrefix: "jp"})

	// No staged entry's state key is requested: nothing may execute (the nil
	// client would panic if a query ran) and no state update is returned, so
	// the entries remain pending in the persisted state.
	state, err := tr.Acknowledge(context.Background(), nil, []string{"c_table.v1"})
	require.NoError(t, err)
	require.Nil(t, state)
	require.NotNil(t, tr.cp["a_table.v1"][fullRangeKey])
	require.NotNil(t, tr.cp["b_table.v1"][fullRangeKey])
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
		require.Len(t, tr.cp, 2)
		require.Equal(t, "Q1", tr.cp["a_table.v1"][legacyRangeKey].Query)
		require.Equal(t, "Q2", tr.cp["b_table.v1"][legacyRangeKey].Query)
	})

	t.Run("single shard routes own range to cp, others to peers", func(t *testing.T) {
		var tr = testTransactor(fullRangeKey, "a_table.v1")
		require.NoError(t, tr.UnmarshalState(json.RawMessage(`{
			"00000000-ffffffff": {"a_table.v1": {"Query": "OWN", "JobPrefix": "po"}},
			"00000000-7fffffff": {"a_table.v1": {"Query": "QL", "JobPrefix": "pl"}},
			"a_table.v1": {"Query": "Q0", "JobPrefix": "p0"}
		}`)))
		require.Equal(t, "OWN", tr.cp["a_table.v1"][fullRangeKey].Query)
		require.Equal(t, "QL", tr.cp["a_table.v1"][lowerRangeKey].Query)
		require.Equal(t, "Q0", tr.cp["a_table.v1"][legacyRangeKey].Query)
	})

	t.Run("multi-shard primary routes own range to cp", func(t *testing.T) {
		var tr = testTransactor(lowerRangeKey, "a_table.v1")
		require.NoError(t, tr.UnmarshalState(mixedState))
		require.Equal(t, "QL", tr.cp["a_table.v1"][lowerRangeKey].Query)
		require.Equal(t, "QU", tr.cp["a_table.v1"][upperRangeKey].Query)
		require.Equal(t, "Q0", tr.cp["a_table.v1"][legacyRangeKey].Query)
	})

	t.Run("non-primary discards everything", func(t *testing.T) {
		var tr = testTransactor(upperRangeKey, "a_table.v1")
		require.NoError(t, tr.UnmarshalState(mixedState))
		require.Empty(t, tr.cp)
	})
}

// An upgrade from a version that wrote {rangeKey: {stateKey: entry}}: its
// entries are held like this version's own, and clear where they were found.
func TestRangeFirstEntriesConvertAndClear(t *testing.T) {
	var tr = testTransactor(lowerRangeKey, "a_table.v1", "b_table.v1")
	tr.skipCleanup = true
	require.NoError(t, tr.UnmarshalState(json.RawMessage(`{
		"00000000-7fffffff": {},
		"80000000-ffffffff": {"b_table.v1": {"JobPrefix": "b-upper"}},
		"a_table.v1": {
			"00000000-7fffffff": {"JobPrefix": "a-lower"},
			"80000000-ffffffff": {"JobPrefix": "a-upper"}
		},
		"b_table.v1": {}
	}`)))
	require.Equal(t, "a-lower", tr.cp["a_table.v1"][lowerRangeKey].JobPrefix)
	require.Equal(t, "a-upper", tr.cp["a_table.v1"][upperRangeKey].JobPrefix)
	require.Equal(t, "b-upper", tr.cp["b_table.v1"][upperRangeKey].JobPrefix)

	update, err := tr.Acknowledge(context.Background(), nil, nil)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"a_table.v1": {"00000000-7fffffff": null, "80000000-ffffffff": null},
		"80000000-ffffffff": {"b_table.v1": null}
	}`, string(update.UpdatedJson))
	require.Empty(t, tr.cp["a_table.v1"])
	require.Empty(t, tr.cp["b_table.v1"])
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
		require.NoError(t, tr.mergePeerStatePatches(patches(`{"a_table.v1": {"00000000-7fffffff": {"Query": "Q", "JobPrefix": "p"}}}`)))
		require.Empty(t, tr.cp)
	})

	t.Run("own contribution is skipped, peers merged", func(t *testing.T) {
		var tr = testTransactor(lowerRangeKey, "a_table.v1")
		require.NoError(t, tr.mergePeerStatePatches(patches(
			`{"a_table.v1": {"00000000-7fffffff": {"Query": "OWN", "JobPrefix": "po"}}}`,
			`{"a_table.v1": {"80000000-ffffffff": {"Query": "PEER", "JobPrefix": "pp"}}}`,
		)))
		require.Len(t, tr.cp["a_table.v1"], 1) // own patch is not folded back
		require.Equal(t, "PEER", tr.cp["a_table.v1"][upperRangeKey].Query)
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
