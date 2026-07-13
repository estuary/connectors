package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	m "github.com/estuary/connectors/go/materialize"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

const (
	fullRangeKey  = "00000000-ffffffff"
	lowerRangeKey = "00000000-7fffffff"
	upperRangeKey = "80000000-ffffffff"
)

func testTransactor(scaleOut bool, rangeKey string, stateKeys ...string) *transactor {
	var d = &transactor{
		cp:                    make(checkpoint),
		peerShardsCheckpoints: make(rangeCheckpoints),
		scaleOut:              scaleOut,
		primary:               rangeKey == fullRangeKey || rangeKey == lowerRangeKey,
		rangeKey:              rangeKey,
		be:                    &m.BindingEvents{},
	}
	d.deleteFiles = func(context.Context, []string) {}

	for _, sk := range stateKeys {
		d.bindings = append(d.bindings, &binding{target: sql.Table{
			TableShape: sql.TableShape{Path: sql.TablePath{"schema", sk}},
			StateKey:   sk,
		}})
	}
	return d
}

func item(query string, toDelete ...string) *checkpointItem {
	return &checkpointItem{Queries: []string{query}, ToDelete: toDelete}
}

func TestUnmarshalStateRouting(t *testing.T) {
	var legacyState = json.RawMessage(`{
		"a_table.v1": {"Queries": ["Q1"], "ToDelete": ["f1"]},
		"b_table.v1": {"Queries": ["Q2"], "ToDelete": []}
	}`)
	var mixedState = json.RawMessage(`{
		"00000000-7fffffff": {"a_table.v1": {"Queries": ["QL"], "ToDelete": []}},
		"80000000-ffffffff": {"a_table.v1": {"Queries": ["QU"], "ToDelete": []}},
		"a_table.v1": {"Queries": ["Q0"], "ToDelete": []}
	}`)

	t.Run("flag off legacy document", func(t *testing.T) {
		var d = testTransactor(false, fullRangeKey, "a_table.v1", "b_table.v1")
		require.NoError(t, d.UnmarshalState(legacyState))
		require.True(t, d.cpRecovery)
		require.Len(t, d.cp, 2)
		require.Equal(t, []string{"Q1"}, d.cp["a_table.v1"].Queries)
		require.Empty(t, d.peerShardsCheckpoints)
	})

	t.Run("flag off routes range buckets after downgrade", func(t *testing.T) {
		var d = testTransactor(false, fullRangeKey, "a_table.v1")
		require.NoError(t, d.UnmarshalState(mixedState))
		require.True(t, d.cpRecovery)
		require.Equal(t, []string{"Q0"}, d.cp["a_table.v1"].Queries)
		require.Len(t, d.peerShardsCheckpoints, 2)
		require.Equal(t, []string{"QL"}, d.peerShardsCheckpoints[lowerRangeKey]["a_table.v1"].Queries)
	})

	t.Run("flag on primary routes own range to cp", func(t *testing.T) {
		var d = testTransactor(true, lowerRangeKey, "a_table.v1")
		require.NoError(t, d.UnmarshalState(mixedState))
		require.True(t, d.cpRecovery)
		require.Equal(t, []string{"QL"}, d.cp["a_table.v1"].Queries)
		require.Equal(t, []string{"QU"}, d.peerShardsCheckpoints[upperRangeKey]["a_table.v1"].Queries)
		require.Equal(t, []string{"Q0"}, d.peerShardsCheckpoints[legacyRangeKey]["a_table.v1"].Queries)
	})

	t.Run("flag on non-primary discards everything", func(t *testing.T) {
		var d = testTransactor(true, upperRangeKey, "a_table.v1")
		require.NoError(t, d.UnmarshalState(mixedState))
		require.False(t, d.cpRecovery)
		require.Empty(t, d.cp)
		require.Empty(t, d.peerShardsCheckpoints)
	})

	t.Run("unknown fields error", func(t *testing.T) {
		var d = testTransactor(false, fullRangeKey, "a_table.v1")
		require.Error(t, d.UnmarshalState(json.RawMessage(`{"a_table.v1": {"Unknown": 1}}`)))
	})
}

func TestStartCommitState(t *testing.T) {
	t.Run("flag off is a full state replacement", func(t *testing.T) {
		var d = testTransactor(false, fullRangeKey, "a_table.v1")
		d.cp["a_table.v1"] = item("Q1", "f1")

		state, err := d.startCommitState()
		require.NoError(t, err)
		require.False(t, state.MergePatch)
		require.JSONEq(t, `{"a_table.v1": {"Queries": ["Q1"], "ToDelete": ["f1"]}}`, string(state.UpdatedJson))
	})

	t.Run("flag on is a range-scoped merge patch", func(t *testing.T) {
		var d = testTransactor(true, lowerRangeKey, "a_table.v1")
		d.cp["a_table.v1"] = item("Q1", "f1")

		state, err := d.startCommitState()
		require.NoError(t, err)
		require.True(t, state.MergePatch)
		require.JSONEq(t, `{"00000000-7fffffff": {"a_table.v1": {"Queries": ["Q1"], "ToDelete": ["f1"]}}}`, string(state.UpdatedJson))
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

	t.Run("no-op when flag off or non-primary", func(t *testing.T) {
		for _, d := range []*transactor{
			testTransactor(false, fullRangeKey),
			testTransactor(true, upperRangeKey),
		} {
			require.NoError(t, d.mergePeerStatePatches(patches(`{"80000000-ffffffff": {"a_table.v1": {"Queries": ["Q"], "ToDelete": []}}}`)))
			require.Empty(t, d.peerShardsCheckpoints)
		}
	})

	t.Run("own contribution is skipped, peers merged", func(t *testing.T) {
		var d = testTransactor(true, lowerRangeKey, "a_table.v1")
		require.NoError(t, d.mergePeerStatePatches(patches(
			`{"00000000-7fffffff": {"a_table.v1": {"Queries": ["OWN"], "ToDelete": []}}}`,
			`{"80000000-ffffffff": {"a_table.v1": {"Queries": ["PEER"], "ToDelete": ["pf1"]}}}`,
		)))
		require.Empty(t, d.cp) // own patch is not folded back
		require.Len(t, d.peerShardsCheckpoints, 1)
		require.Equal(t, []string{"PEER"}, d.peerShardsCheckpoints[upperRangeKey]["a_table.v1"].Queries)
	})

	t.Run("null entries delete", func(t *testing.T) {
		var d = testTransactor(true, lowerRangeKey, "a_table.v1")
		d.peerShardsCheckpoints[upperRangeKey] = checkpoint{
			"a_table.v1": item("PEER"),
			"b_table.v1": item("PEER-B"),
		}

		require.NoError(t, d.mergePeerStatePatches(patches(`{"80000000-ffffffff": {"a_table.v1": null}}`)))
		require.Len(t, d.peerShardsCheckpoints[upperRangeKey], 1)

		require.NoError(t, d.mergePeerStatePatches(patches(`{"80000000-ffffffff": null}`)))
		require.Empty(t, d.peerShardsCheckpoints)
	})

	t.Run("state reset patch errors", func(t *testing.T) {
		var d = testTransactor(true, lowerRangeKey, "a_table.v1")
		require.Error(t, d.mergePeerStatePatches(patches(`null`, `{"a": 1}`)))
	})

	t.Run("empty patches are a no-op", func(t *testing.T) {
		var d = testTransactor(true, lowerRangeKey, "a_table.v1")
		require.NoError(t, d.mergePeerStatePatches(nil))
	})
}

func TestAcknowledge(t *testing.T) {
	var recordingExec = func(queries *[]string, failWith error) func(context.Context, string) error {
		return func(_ context.Context, query string) error {
			if failWith != nil {
				return failWith
			}
			*queries = append(*queries, query)
			return nil
		}
	}

	t.Run("flag off clearing patch matches legacy shape", func(t *testing.T) {
		var d = testTransactor(false, fullRangeKey, "a_table.v1", "b_table.v1")
		d.cp["a_table.v1"] = item("Q1")

		var executed []string
		state, err := d.acknowledgeApply(context.Background(), recordingExec(&executed, nil))
		require.NoError(t, err)
		require.Equal(t, []string{"Q1"}, executed)
		require.True(t, state.MergePatch)
		require.Equal(t, `{"a_table.v1":null,"b_table.v1":null}`, string(state.UpdatedJson))
		require.Empty(t, d.cp)
	})

	t.Run("flag on primary executes own and peer entries", func(t *testing.T) {
		var d = testTransactor(true, lowerRangeKey, "a_table.v1")
		d.cp["a_table.v1"] = item("OWN")
		d.peerShardsCheckpoints[upperRangeKey] = checkpoint{"a_table.v1": item("PEER")}
		d.peerShardsCheckpoints[legacyRangeKey] = checkpoint{"a_table.v1": item("LEGACY")}

		var executed []string
		state, err := d.acknowledgeApply(context.Background(), recordingExec(&executed, nil))
		require.NoError(t, err)
		require.Equal(t, []string{"OWN", "LEGACY", "PEER"}, executed)
		require.True(t, state.MergePatch)
		require.JSONEq(t, `{
			"00000000-7fffffff": {"a_table.v1": null},
			"80000000-ffffffff": {"a_table.v1": null},
			"a_table.v1": null
		}`, string(state.UpdatedJson))
		require.Empty(t, d.cp)
		require.Empty(t, d.peerShardsCheckpoints)
	})

	t.Run("removed binding entries are retained and not cleared", func(t *testing.T) {
		var d = testTransactor(true, lowerRangeKey, "a_table.v1")
		d.peerShardsCheckpoints[upperRangeKey] = checkpoint{
			"a_table.v1":       item("PEER"),
			"removed_table.v1": item("REMOVED"),
		}

		var executed []string
		state, err := d.acknowledgeApply(context.Background(), recordingExec(&executed, nil))
		require.NoError(t, err)
		require.Equal(t, []string{"PEER"}, executed)
		require.JSONEq(t, `{
			"00000000-7fffffff": {"a_table.v1": null},
			"80000000-ffffffff": {"a_table.v1": null}
		}`, string(state.UpdatedJson))
		require.NotNil(t, d.peerShardsCheckpoints[upperRangeKey]["removed_table.v1"])
	})

	t.Run("flag on non-primary does no work", func(t *testing.T) {
		var d = testTransactor(true, upperRangeKey, "a_table.v1")
		d.cp["a_table.v1"] = item("SHOULD NOT RUN")
		d.cpRecovery = true

		// Acknowledge itself is callable here since the non-primary path
		// never opens a database connection.
		state, err := d.Acknowledge(context.Background(), nil)
		require.NoError(t, err)
		require.Nil(t, state)
		require.Empty(t, d.cp)
		require.False(t, d.cpRecovery)
	})

	t.Run("recovery tolerates deleted paths", func(t *testing.T) {
		var d = testTransactor(true, lowerRangeKey, "a_table.v1")
		d.cp["a_table.v1"] = item("Q1")
		d.cpRecovery = true

		var executed []string
		_, err := d.acknowledgeApply(context.Background(), recordingExec(&executed, fmt.Errorf("some PATH_NOT_FOUND error")))
		require.NoError(t, err)
		require.False(t, d.cpRecovery)

		d.cp["a_table.v1"] = item("Q1")
		_, err = d.acknowledgeApply(context.Background(), recordingExec(&executed, fmt.Errorf("some PATH_NOT_FOUND error")))
		require.Error(t, err) // no longer a recovery apply
	})
}
