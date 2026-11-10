package connector

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

const (
	fullRange  = "00000000-ffffffff"
	lowerRange = "00000000-7fffffff"
	upperRange = "80000000-ffffffff"
)

func entry(id string, files ...string) *stateItem {
	return &stateItem{ID: id, StoreFiles: files}
}

// testTransactor builds a transactor of the given range with bindings for
// each state key, without any warehouse or S3 client.
func testTransactor(rangeKey string, stateKeys ...string) *transactor {
	var keyBegin, keyEnd uint32
	if _, err := fmt.Sscanf(rangeKey, "%08x-%08x", &keyBegin, &keyEnd); err != nil {
		panic(err)
	}
	var d = &transactor{
		committedTokens: make(map[string]bool),
		state:           boilerplate.NewShardedState[*stateItem](keyBegin, keyEnd, stateKeys),
	}
	for _, sk := range stateKeys {
		d.bindings = append(d.bindings, &binding{target: sql.Table{
			TableShape: sql.TableShape{Path: sql.TablePath{"schema", sk}},
			StateKey:   sk,
		}})
	}
	return d
}

func TestParseState(t *testing.T) {
	var parse = func(raw string) (boilerplate.ShardedEntries[*stateItem], error) {
		var d = testTransactor(fullRange, "a_table.v1")
		err := d.UnmarshalState(json.RawMessage(raw))
		return d.state.Pending(), err
	}

	t.Run("entries are keyed by state key then range", func(t *testing.T) {
		pending, err := parse(`{
			"a_table.v1": {
				"00000000-ffffffff": {
					"id": "p/x/files.manifest",
					"storeFiles": ["p/x/f1"],
					"deleteFiles": [],
					"mustMerge": true,
					"widen": ["\"col\""]
				}
			}
		}`)
		require.NoError(t, err)
		require.Equal(t, boilerplate.ShardedEntries[*stateItem]{"a_table.v1": {fullRange: &stateItem{
			ID:          "p/x/files.manifest",
			StoreFiles:  []string{"p/x/f1"},
			DeleteFiles: []string{},
			MustMerge:   true,
			Widen:       []string{`"col"`},
		}}}, pending)
	})

	t.Run("an entry with unknown fields is rejected", func(t *testing.T) {
		_, err := parse(`{"a_table.v1": {"00000000-ffffffff": {"Queries": ["Q1"]}}}`)
		require.Error(t, err)
	})
}

// The StartCommit patch is byte-for-byte what earlier versions wrote, so
// they can still read the state after a downgrade.
func TestStartCommitPatch(t *testing.T) {
	var d = testTransactor(upperRange, "a_table.v1", "b_table.v1")
	update, err := d.state.StartCommit(map[string]*stateItem{
		"a_table.v1": {ID: "t1", StoreFiles: []string{"p/a/f1"}, MustMerge: true, Round: 2, Rows: 3},
		"b_table.v1": {ID: "t2", DeleteFiles: []string{"p/b/f1"}, Widen: []string{`"c"`}},
	})
	require.NoError(t, err)
	require.True(t, update.MergePatch)
	require.Equal(t, `{"a_table.v1":{"80000000-ffffffff":{"id":"t1","storeFiles":["p/a/f1"],"deleteFiles":null,"mustMerge":true,"widen":null,"round":2,"rows":3}},`+
		`"b_table.v1":{"80000000-ffffffff":{"id":"t2","storeFiles":null,"deleteFiles":["p/b/f1"],"mustMerge":false,"widen":["\"c\""],"round":0,"rows":0}}}`,
		string(update.UpdatedJson))
}

func TestStateItemObjects(t *testing.T) {
	e := &stateItem{ID: "t1", StoreFiles: []string{"p/s/f1", "p/s/f2"}, DeleteFiles: []string{"p/d/f1"}}
	require.Equal(t, []string{"p/s/f1", "p/s/f2", "p/d/f1"}, e.objects())
}

func TestDecodeCheckpointsColumns(t *testing.T) {
	var s = &checkpointsTable{}

	t.Run("the tokens column", func(t *testing.T) {
		tokens, err := s.decodeTokensColumn(nil)
		require.NoError(t, err)
		require.Nil(t, tokens)

		raw := hex.EncodeToString([]byte(`{"a_table.v1":{"00000000-ffffffff":"p/x/files.manifest"}}`))
		tokens, err = s.decodeTokensColumn(&raw)
		require.NoError(t, err)
		require.Equal(t, checkpointTokensMap{"a_table.v1": {fullRange: "p/x/files.manifest"}}, tokens)
	})

	t.Run("the legacy checkpoint column", func(t *testing.T) {
		legacy, err := s.decodeLegacyCheckpoint(nil)
		require.NoError(t, err)
		require.Nil(t, legacy)

		checkpoint := []byte("some-runtime-checkpoint")
		compressed, err := compressBytes(checkpoint)
		require.NoError(t, err)
		raw := hex.EncodeToString([]byte(base64.StdEncoding.EncodeToString(compressed)))
		legacy, err = s.decodeLegacyCheckpoint(&raw)
		require.NoError(t, err)
		require.Equal(t, checkpoint, legacy)
	})

	t.Run("the legacy checkpoint column, never compressed", func(t *testing.T) {
		checkpoint := []byte("some-runtime-checkpoint")
		raw := hex.EncodeToString([]byte(base64.StdEncoding.EncodeToString(checkpoint)))
		legacy, err := s.decodeLegacyCheckpoint(&raw)
		require.NoError(t, err)
		require.Equal(t, checkpoint, legacy)
	})
}

func TestEntryPatchReplacesPrevious(t *testing.T) {
	previous := &stateItem{
		ID:          "t1",
		StoreFiles:  []string{"p/1/f1"},
		DeleteFiles: []string{"p/1d/f1"},
		MustMerge:   true,
		Widen:       []string{`"col"`},
	}
	next := entry("p/2/files.manifest", "p/2/f1")

	var before, patch any
	require.NoError(t, json.Unmarshal(mustMarshal(t, map[string]map[string]*stateItem{"a_table.v1": {fullRange: previous}}), &before))
	require.NoError(t, json.Unmarshal(mustMarshal(t, map[string]map[string]*stateItem{"a_table.v1": {fullRange: next}}), &patch))

	reduced, err := json.Marshal(boilerplate.ApplyMergePatch(before, patch))
	require.NoError(t, err)

	var d = testTransactor(fullRange, "a_table.v1")
	require.NoError(t, d.UnmarshalState(reduced))
	require.Equal(t, next, d.state.Pending()["a_table.v1"][fullRange])
}

func TestEntryWithoutTruncationOmitsIt(t *testing.T) {
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(mustMarshal(t, entry("p/files.manifest", "p/f1")), &fields))
	require.NotContains(t, fields, "truncateBefore")
}

func TestStateRouting(t *testing.T) {
	var state = json.RawMessage(`{
		"a_table.v1": {
			"00000000-7fffffff": {"id": "l/files.manifest", "storeFiles": ["l/f"], "deleteFiles": [], "mustMerge": false, "widen": []},
			"80000000-ffffffff": {"id": "u/files.manifest", "storeFiles": ["u/f"], "deleteFiles": [], "mustMerge": true, "widen": []}
		}
	}`)

	t.Run("a non-primary shard recovers nothing and acknowledges nothing", func(t *testing.T) {
		d := testTransactor(upperRange, "a_table.v1")
		require.NoError(t, d.UnmarshalState(state))
		require.False(t, d.state.Primary())
		require.Empty(t, d.state.Pending())
		patch, err := d.Acknowledge(t.Context(), []json.RawMessage{state}, nil)
		require.NoError(t, err)
		require.Nil(t, patch)
	})

	t.Run("an entry with no live binding stays pending and gives Acknowledge nothing to do", func(t *testing.T) {
		d := testTransactor(fullRange, "a_table.v1")
		require.NoError(t, d.UnmarshalState(json.RawMessage(`{
			"gone_table.v1": {"00000000-ffffffff": {"id": "g/files.manifest", "storeFiles": ["g/f"], "deleteFiles": [], "mustMerge": false, "widen": []}}
		}`)))
		patch, err := d.Acknowledge(t.Context(), nil, nil)
		require.NoError(t, err)
		require.Nil(t, patch)
		require.NotNil(t, d.state.Pending()["gone_table.v1"][fullRange])
	})

	t.Run("acknowledge leaves unrequested state keys pending", func(t *testing.T) {
		d := testTransactor(lowerRange, "a_table.v1")
		require.NoError(t, d.UnmarshalState(state))

		patch, err := d.Acknowledge(t.Context(), nil, []string{"other.v1"})
		require.NoError(t, err)
		require.Nil(t, patch)
		require.Equal(t, 2, d.state.Pending().Len())
	})
}

func mustMarshal(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}
