package connector

import (
	"context"
	"encoding/json"
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
	var tr = &transactor{
		rangeFirst:   make(checkpoints),
		stateFirst:   make(checkpoints),
		flat:         make(map[string]flatCheckpoint),
		primaryShard: rangeKey == fullRangeKey || rangeKey == lowerRangeKey,
		rangeKey:     rangeKey,
		be:           &m.BindingEvents{},
		skipCleanup:  true,
	}
	for _, sk := range stateKeys {
		tr.bindings = append(tr.bindings, &binding{target: sql.Table{StateKey: sk}})
	}
	return tr
}

// reduce applies a connector state update to a state document, as the runtime
// does.
func reduce(t *testing.T, state string, update *pf.ConnectorState) string {
	t.Helper()
	require.True(t, update.MergePatch)
	var doc, patch any
	require.NoError(t, json.Unmarshal([]byte(state), &doc))
	require.NoError(t, json.Unmarshal(update.UpdatedJson, &patch))
	return string(mustMarshal(t, boilerplate.ApplyMergePatch(doc, patch)))
}

func jobPrefixes(items []*checkpointItem) []string {
	var out []string
	for _, item := range items {
		out = append(out, item.JobPrefix)
	}
	return out
}

func TestAcknowledgeSubsetLeavesOtherKeysPending(t *testing.T) {
	tr := testTransactor(fullRangeKey, "a_table.v1", "b_table.v1")
	tr.stage("a_table.v1", &checkpointItem{Query: "MERGE INTO a", JobPrefix: "jp"})
	tr.stage("b_table.v1", &checkpointItem{Query: "MERGE INTO b", JobPrefix: "jp"})

	// No staged entry's state key is requested: nothing may execute (the nil
	// client would panic if a query ran) and no state update is returned, so
	// the entries remain pending in the persisted state.
	state, err := tr.Acknowledge(context.Background(), nil, []string{"c_table.v1"})
	require.NoError(t, err)
	require.Nil(t, state)
	require.Len(t, tr.pendingItems("a_table.v1"), 1)
	require.Len(t, tr.pendingItems("b_table.v1"), 1)
}

func TestEntryWithoutTruncationOmitsIt(t *testing.T) {
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(mustMarshal(t, &checkpointItem{JobPrefix: "jp"}), &fields))
	require.NotContains(t, fields, "TruncateBefore")
}

func TestTimestampLiteralFloorsToMicroseconds(t *testing.T) {
	var ts = time.Date(2026, 9, 29, 12, 34, 56, 999_999_999, time.FixedZone("x", 3600))
	require.Equal(t, "TIMESTAMP '2026-09-29 11:34:56.999999 UTC'", timestampLiteral(ts))
}

func TestStartCommitState(t *testing.T) {
	t.Run("an entry nests under its state key, then this shard's range", func(t *testing.T) {
		var tr = testTransactor(upperRangeKey, "a_table.v1")
		tr.stage("a_table.v1", &checkpointItem{NeedsMerge: true, SourceURIs: []string{"gs://b/f1"}, JobPrefix: "jp", round: 3})

		update, err := tr.startCommitState()
		require.NoError(t, err)
		require.True(t, update.MergePatch)
		require.Equal(t, `{"a_table.v1":{"80000000-ffffffff":{"NeedsMerge":true,"SourceURIs":["gs://b/f1"],"JobPrefix":"jp"}}}`, string(update.UpdatedJson))
	})

	t.Run("the primary emits only its own entries", func(t *testing.T) {
		var tr = testTransactor(lowerRangeKey, "a_table.v1", "b_table.v1")
		require.NoError(t, tr.UnmarshalState(json.RawMessage(`{
			"a_table.v1": {"80000000-ffffffff": {"JobPrefix": "peer"}, "JobPrefix": "flat"},
			"80000000-ffffffff": {"b_table.v1": {"JobPrefix": "range-first"}}
		}`)))
		tr.stage("b_table.v1", &checkpointItem{JobPrefix: "own"})

		update, err := tr.startCommitState()
		require.NoError(t, err)
		require.Equal(t, `{"b_table.v1":{"00000000-7fffffff":{"JobPrefix":"own"}}}`, string(update.UpdatedJson))
	})

	t.Run("nothing staged", func(t *testing.T) {
		update, err := testTransactor(fullRangeKey, "a_table.v1").startCommitState()
		require.NoError(t, err)
		require.Equal(t, `{}`, string(update.UpdatedJson))
	})
}

func TestUnmarshalState(t *testing.T) {
	t.Run("range-first layout", func(t *testing.T) {
		var tr = testTransactor(lowerRangeKey, "a_table.v1")
		require.NoError(t, tr.UnmarshalState(json.RawMessage(`{
			"80000000-ffffffff": {"a_table.v1": {"JobPrefix": "peer"}},
			"00000000-7fffffff": {"a_table.v1": {"JobPrefix": "own"}}
		}`)))
		require.Equal(t, []string{"own", "peer"}, jobPrefixes(tr.pendingItems("a_table.v1")))
		require.Equal(t, "own", tr.rangeFirst[lowerRangeKey]["a_table.v1"].JobPrefix)
		require.Empty(t, tr.stateFirst)
		require.Empty(t, tr.flat)
	})

	t.Run("state-key-first layout", func(t *testing.T) {
		var tr = testTransactor(lowerRangeKey, "a_table.v1")
		require.NoError(t, tr.UnmarshalState(json.RawMessage(`{"a_table.v1": {
			"80000000-ffffffff": {"JobPrefix": "peer"},
			"00000000-7fffffff": {"JobPrefix": "own"}
		}}`)))
		require.Equal(t, []string{"own", "peer"}, jobPrefixes(tr.pendingItems("a_table.v1")))
		require.Equal(t, "own", tr.stateFirst["a_table.v1"][lowerRangeKey].JobPrefix)
		require.Empty(t, tr.rangeFirst)
		require.Empty(t, tr.flat)
	})

	t.Run("flat layout", func(t *testing.T) {
		var tr = testTransactor(fullRangeKey, "a_table.v1")
		require.NoError(t, tr.UnmarshalState(json.RawMessage(`{"a_table.v1": {
			"Query": "MERGE INTO a", "SourceURIs": ["gs://b/f1"], "JobPrefix": "flat", "TempTableName": "tmp"
		}}`)))
		require.Equal(t,
			[]*checkpointItem{{Query: "MERGE INTO a", SourceURIs: []string{"gs://b/f1"}, JobPrefix: "flat", TempTableName: "tmp"}},
			tr.pendingItems("a_table.v1"))
		require.Equal(t, []string{"JobPrefix", "Query", "SourceURIs", "TempTableName"}, tr.flat["a_table.v1"].fields)
		require.Empty(t, tr.rangeFirst)
		require.Empty(t, tr.stateFirst)
	})

	// The three layouts for one state key, as merge patches of different
	// versions leave them.
	var mixed = json.RawMessage(`{
		"a_table.v1": {
			"80000000-ffffffff": {"JobPrefix": "state-first-upper"},
			"00000000-7fffffff": {"JobPrefix": "state-first-lower"},
			"JobPrefix": "flat"
		},
		"80000000-ffffffff": {"a_table.v1": {"JobPrefix": "range-first-upper"}},
		"00000000-7fffffff": {"a_table.v1": {"JobPrefix": "range-first-lower"}}
	}`)

	t.Run("entries apply by layout, then by range key", func(t *testing.T) {
		var tr = testTransactor(lowerRangeKey, "a_table.v1")
		require.NoError(t, tr.UnmarshalState(mixed))
		require.Equal(t,
			[]string{"state-first-lower", "state-first-upper", "flat", "range-first-lower", "range-first-upper"},
			jobPrefixes(tr.pendingItems("a_table.v1")))
	})

	t.Run("a non-primary shard recovers nothing", func(t *testing.T) {
		var tr = testTransactor(upperRangeKey, "a_table.v1")
		require.NoError(t, tr.UnmarshalState(mixed))
		require.Empty(t, tr.pendingItems("a_table.v1"))
	})

	t.Run("an unknown entry field is ignored", func(t *testing.T) {
		var tr = testTransactor(fullRangeKey, "a_table.v1")
		require.NoError(t, tr.UnmarshalState(json.RawMessage(`{
			"00000000-ffffffff": {"a_table.v1": {"JobPrefix": "range-first", "Later": 1}},
			"a_table.v1": {"00000000-ffffffff": {"JobPrefix": "state-first", "Later": 1}}
		}`)))
		require.Equal(t, []*checkpointItem{{JobPrefix: "state-first"}, {JobPrefix: "range-first"}}, tr.pendingItems("a_table.v1"))
	})

	t.Run("what clearing leaves behind holds no entry", func(t *testing.T) {
		var tr = testTransactor(fullRangeKey, "a_table.v1")
		require.NoError(t, tr.UnmarshalState(json.RawMessage(`{"a_table.v1": {}, "00000000-ffffffff": {}}`)))
		require.Empty(t, tr.pendingItems("a_table.v1"))
		require.Empty(t, tr.rangeFirst)
		require.Empty(t, tr.stateFirst)
		require.Empty(t, tr.flat)
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
		var tr = testTransactor(upperRangeKey, "a_table.v1")
		require.NoError(t, tr.mergePeerStatePatches(patches(`{"a_table.v1": {"00000000-7fffffff": {"JobPrefix": "p"}}}`)))
		require.Empty(t, tr.pendingItems("a_table.v1"))
	})

	t.Run("own contribution is skipped, peers merged", func(t *testing.T) {
		var tr = testTransactor(lowerRangeKey, "a_table.v1")
		tr.stage("a_table.v1", &checkpointItem{JobPrefix: "own", round: 2})
		require.NoError(t, tr.mergePeerStatePatches(patches(
			`{"a_table.v1": {"00000000-7fffffff": {"JobPrefix": "own"}}}`,
			`{"a_table.v1": {"80000000-ffffffff": {"JobPrefix": "peer"}}}`,
		)))
		require.Equal(t, []string{"own", "peer"}, jobPrefixes(tr.pendingItems("a_table.v1")))
		// The echo didn't replace the entry this session staged.
		require.Equal(t, 2, tr.staged("a_table.v1").round)
	})

	t.Run("a peer's range-first patch is merged", func(t *testing.T) {
		var tr = testTransactor(lowerRangeKey, "a_table.v1")
		tr.stage("a_table.v1", &checkpointItem{JobPrefix: "own"})
		require.NoError(t, tr.mergePeerStatePatches(patches(
			`{"a_table.v1": {"00000000-7fffffff": {"JobPrefix": "own"}}}`,
			`{"80000000-ffffffff": {"a_table.v1": {"JobPrefix": "peer"}}}`,
		)))
		require.Equal(t, []string{"own", "peer"}, jobPrefixes(tr.pendingItems("a_table.v1")))
		require.Equal(t, "peer", tr.rangeFirst[upperRangeKey]["a_table.v1"].JobPrefix)
	})

	t.Run("a flat patch is merged", func(t *testing.T) {
		var tr = testTransactor(lowerRangeKey, "a_table.v1")
		require.NoError(t, tr.mergePeerStatePatches(patches(`{"a_table.v1": {"JobPrefix": "flat"}}`)))
		require.Equal(t, []string{"flat"}, jobPrefixes(tr.pendingItems("a_table.v1")))
		require.Equal(t, []string{"JobPrefix"}, tr.flat["a_table.v1"].fields)
	})

	t.Run("state reset patch errors", func(t *testing.T) {
		var tr = testTransactor(lowerRangeKey, "a_table.v1")
		require.Error(t, tr.mergePeerStatePatches(patches(`null`, `{"a": {}}`)))
	})
}

// Entries apply here without running a query, because none has staged files.
func TestAcknowledgeClearsEachLayoutAtItsPath(t *testing.T) {
	for _, tc := range []struct {
		name, state, clear, after string
	}{
		{
			name:  "range-first",
			state: `{"00000000-7fffffff": {"a_table.v1": {"JobPrefix": "own"}}, "80000000-ffffffff": {"a_table.v1": {"JobPrefix": "peer"}}}`,
			clear: `{"00000000-7fffffff": {"a_table.v1": null}, "80000000-ffffffff": {"a_table.v1": null}}`,
			after: `{"00000000-7fffffff": {}, "80000000-ffffffff": {}}`,
		},
		{
			name:  "state-key-first",
			state: `{"a_table.v1": {"00000000-7fffffff": {"JobPrefix": "own"}, "80000000-ffffffff": {"JobPrefix": "peer"}}}`,
			clear: `{"a_table.v1": {"00000000-7fffffff": null, "80000000-ffffffff": null}}`,
			after: `{"a_table.v1": {}}`,
		},
		{
			name:  "flat",
			state: `{"a_table.v1": {"Query": "", "SourceURIs": null, "JobPrefix": "flat", "TempTableName": "tmp"}}`,
			clear: `{"a_table.v1": {"Query": null, "SourceURIs": null, "JobPrefix": null, "TempTableName": null}}`,
			after: `{"a_table.v1": {}}`,
		},
		{
			name: "all three for one state key",
			state: `{
				"a_table.v1": {"80000000-ffffffff": {"JobPrefix": "state-first"}, "JobPrefix": "flat", "TempTableName": "tmp"},
				"00000000-7fffffff": {"a_table.v1": {"JobPrefix": "range-first"}}
			}`,
			clear: `{
				"a_table.v1": {"80000000-ffffffff": null, "JobPrefix": null, "TempTableName": null},
				"00000000-7fffffff": {"a_table.v1": null}
			}`,
			after: `{"a_table.v1": {}, "00000000-7fffffff": {}}`,
		},
		{
			name: "entries of an unbound state key stay",
			state: `{
				"a_table.v1": {"00000000-7fffffff": {"JobPrefix": "state-first"}},
				"gone.v1": {"00000000-7fffffff": {"JobPrefix": "state-first"}, "JobPrefix": "flat"},
				"80000000-ffffffff": {"a_table.v1": {"JobPrefix": "range-first"}, "gone.v1": {"JobPrefix": "range-first"}}
			}`,
			clear: `{"a_table.v1": {"00000000-7fffffff": null}, "80000000-ffffffff": {"a_table.v1": null}}`,
			after: `{
				"a_table.v1": {},
				"gone.v1": {"00000000-7fffffff": {"JobPrefix": "state-first"}, "JobPrefix": "flat"},
				"80000000-ffffffff": {"gone.v1": {"JobPrefix": "range-first"}}
			}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var tr = testTransactor(lowerRangeKey, "a_table.v1")
			require.NoError(t, tr.UnmarshalState(json.RawMessage(tc.state)))

			update, err := tr.Acknowledge(t.Context(), nil, nil)
			require.NoError(t, err)
			require.JSONEq(t, tc.clear, string(update.UpdatedJson))
			require.JSONEq(t, tc.after, reduce(t, tc.state, update))
			require.Empty(t, tr.pendingItems("a_table.v1"))

			// Nothing is left to apply, in memory or after a restart.
			update, err = tr.Acknowledge(t.Context(), nil, nil)
			require.NoError(t, err)
			require.Nil(t, update)

			var restarted = testTransactor(lowerRangeKey, "a_table.v1")
			require.NoError(t, restarted.UnmarshalState(json.RawMessage(tc.after)))
			update, err = restarted.Acknowledge(t.Context(), nil, nil)
			require.NoError(t, err)
			require.Nil(t, update)
		})
	}
}

// A flat entry clears by its fields, so that a state-key-first entry merged
// onto the same state key meanwhile survives.
func TestFlatEntryClearKeepsStateFirstEntry(t *testing.T) {
	var tr = testTransactor(fullRangeKey, "a_table.v1")
	require.NoError(t, tr.UnmarshalState(json.RawMessage(`{"a_table.v1": {"JobPrefix": "flat", "TempTableName": "tmp"}}`)))

	update, err := tr.Acknowledge(t.Context(), nil, nil)
	require.NoError(t, err)
	require.JSONEq(t,
		`{"a_table.v1": {"00000000-ffffffff": {"JobPrefix": "later"}}}`,
		reduce(t, `{"a_table.v1": {"JobPrefix": "flat", "TempTableName": "tmp", "00000000-ffffffff": {"JobPrefix": "later"}}}`, update))
}

// Runtime v1 sends no statePatches: the shard applies what it staged itself.
func TestSingleShardRoundTripWithoutStatePatches(t *testing.T) {
	var tr = testTransactor(fullRangeKey, "a_table.v1")
	tr.stage("a_table.v1", &checkpointItem{JobPrefix: "own"})

	started, err := tr.startCommitState()
	require.NoError(t, err)
	var state = reduce(t, `{}`, started)
	require.JSONEq(t, `{"a_table.v1": {"00000000-ffffffff": {"JobPrefix": "own"}}}`, state)

	acked, err := tr.Acknowledge(t.Context(), nil, nil)
	require.NoError(t, err)
	require.JSONEq(t, `{"a_table.v1": {"00000000-ffffffff": null}}`, string(acked.UpdatedJson))
	require.JSONEq(t, `{"a_table.v1": {}}`, reduce(t, state, acked))
}

// Runtime v2 with two shards: both stage, the primary applies and clears
// both entries, and a restart before the clearing commits recovers both.
func TestTwoShardRoundTrip(t *testing.T) {
	var primary = testTransactor(lowerRangeKey, "a_table.v1", "b_table.v1")
	var secondary = testTransactor(upperRangeKey, "a_table.v1", "b_table.v1")
	primary.stage("a_table.v1", &checkpointItem{JobPrefix: "lower-a"})
	secondary.stage("a_table.v1", &checkpointItem{JobPrefix: "upper-a"})
	secondary.stage("b_table.v1", &checkpointItem{JobPrefix: "upper-b"})

	// The shards' patches merge in either order.
	var state = `{}`
	var statePatches []json.RawMessage
	for _, tr := range []*transactor{secondary, primary} {
		started, err := tr.startCommitState()
		require.NoError(t, err)
		state = reduce(t, state, started)
		statePatches = append(statePatches, started.UpdatedJson)
	}
	require.JSONEq(t, `{
		"a_table.v1": {"00000000-7fffffff": {"JobPrefix": "lower-a"}, "80000000-ffffffff": {"JobPrefix": "upper-a"}},
		"b_table.v1": {"80000000-ffffffff": {"JobPrefix": "upper-b"}}
	}`, state)

	// A restart at this point recovers every entry on the primary alone.
	var restarted = testTransactor(lowerRangeKey, "a_table.v1", "b_table.v1")
	require.NoError(t, restarted.UnmarshalState(json.RawMessage(state)))
	require.Equal(t, []string{"lower-a", "upper-a"}, jobPrefixes(restarted.pendingItems("a_table.v1")))
	require.Equal(t, []string{"upper-b"}, jobPrefixes(restarted.pendingItems("b_table.v1")))

	acked, err := secondary.Acknowledge(t.Context(), statePatches, nil)
	require.NoError(t, err)
	require.Nil(t, acked)
	require.Empty(t, secondary.pendingItems("a_table.v1"))
	require.Empty(t, secondary.pendingItems("b_table.v1"))

	// The primary's own entry comes first, so it names the coalesced job.
	require.NoError(t, primary.mergePeerStatePatches(statePatches))
	require.Equal(t, []string{"lower-a", "upper-a"}, jobPrefixes(primary.pendingItems("a_table.v1")))

	acked, err = primary.Acknowledge(t.Context(), statePatches, nil)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"a_table.v1": {"00000000-7fffffff": null, "80000000-ffffffff": null},
		"b_table.v1": {"80000000-ffffffff": null}
	}`, string(acked.UpdatedJson))
	require.JSONEq(t, `{"a_table.v1": {}, "b_table.v1": {}}`, reduce(t, state, acked))
	require.Empty(t, primary.pendingItems("a_table.v1"))
	require.Empty(t, primary.pendingItems("b_table.v1"))
}

// An upgrade from a version that wrote the range-first layout: the primary
// recovers both shards' entries, applies them in that version's order and
// clears them where they were written.
func TestUpgradeFromRangeFirstWriter(t *testing.T) {
	var state = `{
		"00000000-7fffffff": {"a_table.v1": {"JobPrefix": "lower"}},
		"80000000-ffffffff": {"a_table.v1": {"JobPrefix": "upper"}}
	}`
	var primary = testTransactor(lowerRangeKey, "a_table.v1")
	require.NoError(t, primary.UnmarshalState(json.RawMessage(state)))
	require.Equal(t, []string{"lower", "upper"}, jobPrefixes(primary.pendingItems("a_table.v1")))

	acked, err := primary.Acknowledge(t.Context(), nil, nil)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"00000000-7fffffff": {"a_table.v1": null},
		"80000000-ffffffff": {"a_table.v1": null}
	}`, string(acked.UpdatedJson))
	state = reduce(t, state, acked)
	require.JSONEq(t, `{"00000000-7fffffff": {}, "80000000-ffffffff": {}}`, state)

	// The next transaction stages in the layout this version writes.
	primary.stage("a_table.v1", &checkpointItem{JobPrefix: "next"})
	started, err := primary.startCommitState()
	require.NoError(t, err)
	require.JSONEq(t, `{
		"00000000-7fffffff": {},
		"80000000-ffffffff": {},
		"a_table.v1": {"00000000-7fffffff": {"JobPrefix": "next"}}
	}`, reduce(t, state, started))
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
