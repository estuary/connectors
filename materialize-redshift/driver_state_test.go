package connector

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"

	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

const (
	fullRange  = "00000000-ffffffff"
	lowerRange = "00000000-7fffffff"
	upperRange = "80000000-ffffffff"
)

func entry(id string, files ...string) *checkpointItem {
	return &checkpointItem{ID: id, StoreFiles: files}
}

// testTransactor builds a transactor of the given range with bindings for
// each state key, without any warehouse or S3 client.
func testTransactor(rangeKey string, stateKeys ...string) *transactor {
	var keyBegin, keyEnd uint32
	if _, err := fmt.Sscanf(rangeKey, "%08x-%08x", &keyBegin, &keyEnd); err != nil {
		panic(err)
	}
	var d = &transactor{
		rangeKey:        rangeKey,
		primary:         keyBegin == 0,
		committedTokens: make(map[string]bool),
		pending:         make(pendingState),
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
	t.Run("a task crossing over opens with no state", func(t *testing.T) {
		for _, raw := range []json.RawMessage{nil, json.RawMessage(``), json.RawMessage(`{}`)} {
			pending, err := parseState(raw)
			require.NoError(t, err)
			require.Empty(t, pending)
		}
	})

	t.Run("entries are keyed by state key then range", func(t *testing.T) {
		pending, err := parseState(json.RawMessage(`{
			"a_table.v1": {
				"00000000-ffffffff": {
					"id": "p/x/files.manifest",
					"storeFiles": ["p/x/f1"],
					"deleteFiles": [],
					"mustMerge": true,
					"widen": ["\"col\""]
				}
			}
		}`))
		require.NoError(t, err)
		require.Equal(t, pendingState{"a_table.v1": {fullRange: &checkpointItem{
			ID:          "p/x/files.manifest",
			StoreFiles:  []string{"p/x/f1"},
			DeleteFiles: []string{},
			MustMerge:   true,
			Widen:       []string{`"col"`},
		}}}, pending)
	})

	t.Run("an entry with unknown fields is rejected", func(t *testing.T) {
		_, err := parseState(json.RawMessage(`{"a_table.v1": {"00000000-ffffffff": {"Queries": ["Q1"]}}}`))
		require.Error(t, err)
	})

	t.Run("an entry not nested under a range key is rejected", func(t *testing.T) {
		_, err := parseState(json.RawMessage(`{"a_table.v1": {"id": "m", "storeFiles": [], "deleteFiles": [], "mustMerge": false, "widen": []}}`))
		require.Error(t, err)
	})
}

func TestCheckpointItemObjects(t *testing.T) {
	e := &checkpointItem{ID: "t1", StoreFiles: []string{"p/s/f1", "p/s/f2"}, DeleteFiles: []string{"p/d/f1"}}
	require.Equal(t, []string{"p/s/f1", "p/s/f2", "p/d/f1"}, e.objects())
}

func TestParseCheckpointsRow(t *testing.T) {
	var s = &checkpointsTable{}

	t.Run("a token payload", func(t *testing.T) {
		tokens, legacy, err := s.parseCheckpointsRow(hex.EncodeToString([]byte(`{"a_table.v1":{"00000000-ffffffff":"p/x/files.manifest"}}`)))
		require.NoError(t, err)
		require.Nil(t, legacy)
		require.Equal(t, checkpoints{"a_table.v1": {fullRange: "p/x/files.manifest"}}, tokens)
	})

	t.Run("a row in the old format", func(t *testing.T) {
		checkpoint := []byte("some-runtime-checkpoint")
		compressed, err := compressBytes(checkpoint)
		require.NoError(t, err)

		tokens, legacy, err := s.parseCheckpointsRow(hex.EncodeToString([]byte(base64.StdEncoding.EncodeToString(compressed))))
		require.NoError(t, err)
		require.Nil(t, tokens)
		require.Equal(t, checkpoint, legacy)
	})

	t.Run("a row in the old format that was never compressed", func(t *testing.T) {
		checkpoint := []byte("some-runtime-checkpoint")
		tokens, legacy, err := s.parseCheckpointsRow(hex.EncodeToString([]byte(base64.StdEncoding.EncodeToString(checkpoint))))
		require.NoError(t, err)
		require.Nil(t, tokens)
		require.Equal(t, checkpoint, legacy)
	})
}

func TestEntryPatchReplacesPrevious(t *testing.T) {
	previous := &checkpointItem{
		ID:          "t1",
		StoreFiles:  []string{"p/1/f1"},
		DeleteFiles: []string{"p/1d/f1"},
		MustMerge:   true,
		Widen:       []string{`"col"`},
	}
	next := entry("p/2/files.manifest", "p/2/f1")

	var before, patch any
	require.NoError(t, json.Unmarshal(mustMarshal(t, pendingState{"a_table.v1": {fullRange: previous}}), &before))
	require.NoError(t, json.Unmarshal(mustMarshal(t, pendingState{"a_table.v1": {fullRange: next}}), &patch))

	reduced, err := json.Marshal(boilerplate.ApplyMergePatch(before, patch))
	require.NoError(t, err)

	pending, err := parseState(reduced)
	require.NoError(t, err)
	require.Equal(t, next, pending["a_table.v1"][fullRange])
}

func TestStateRouting(t *testing.T) {
	var state = json.RawMessage(`{
		"a_table.v1": {
			"00000000-7fffffff": {"id": "l/files.manifest", "storeFiles": ["l/f"], "deleteFiles": [], "mustMerge": false, "widen": []},
			"80000000-ffffffff": {"id": "u/files.manifest", "storeFiles": ["u/f"], "deleteFiles": [], "mustMerge": true, "widen": []}
		}
	}`)

	t.Run("the primary recovers every range", func(t *testing.T) {
		d := testTransactor(lowerRange, "a_table.v1")
		require.NoError(t, d.UnmarshalState(state))
		require.Len(t, d.pending, 1)
		require.Len(t, d.pending["a_table.v1"], 2)
		require.Equal(t, "l/files.manifest", d.pending["a_table.v1"][lowerRange].ID)
		require.Equal(t, "u/files.manifest", d.pending["a_table.v1"][upperRange].ID)
	})

	t.Run("a non-primary shard recovers nothing and acknowledges nothing", func(t *testing.T) {
		d := testTransactor(upperRange, "a_table.v1")
		require.NoError(t, d.UnmarshalState(state))
		require.Empty(t, d.pending)
		patch, err := d.Acknowledge(t.Context(), []json.RawMessage{state}, nil)
		require.NoError(t, err)
		require.Nil(t, patch)
	})

	t.Run("the primary folds peers' patches and skips its own echo", func(t *testing.T) {
		d := testTransactor(lowerRange, "a_table.v1")
		d.pending["a_table.v1"] = map[string]*checkpointItem{lowerRange: entry("own/files.manifest", "own/f")}
		require.NoError(t, d.mergePeerStatePatches([]json.RawMessage{state}))
		require.Equal(t, "own/files.manifest", d.pending["a_table.v1"][lowerRange].ID)
		require.Equal(t, "u/files.manifest", d.pending["a_table.v1"][upperRange].ID)

		require.Error(t, d.mergePeerStatePatches([]json.RawMessage{json.RawMessage(`null`)}))

		// The peer's previous transaction is still pending: its work would be lost.
		require.Error(t, d.mergePeerStatePatches([]json.RawMessage{state}))
	})

	t.Run("an entry with no live binding stays pending and gives Acknowledge nothing to do", func(t *testing.T) {
		d := testTransactor(fullRange, "a_table.v1")
		require.NoError(t, d.UnmarshalState(json.RawMessage(`{
			"gone_table.v1": {"00000000-ffffffff": {"id": "g/files.manifest", "storeFiles": ["g/f"], "deleteFiles": [], "mustMerge": false, "widen": []}}
		}`)))
		patch, err := d.Acknowledge(t.Context(), nil, nil)
		require.NoError(t, err)
		require.Nil(t, patch)
		require.NotNil(t, d.pending["gone_table.v1"][fullRange])
	})

	t.Run("staged work groups every range's entries per binding and leaves unknown bindings pending", func(t *testing.T) {
		d := testTransactor(lowerRange, "a_table.v1")
		require.NoError(t, d.UnmarshalState(state))
		d.pending["gone_table.v1"] = map[string]*checkpointItem{upperRange: entry("g/files.manifest", "g/f")}

		var seen []string
		for _, w := range d.stagedWork(m.StateKeyFilter(nil)) {
			for _, pe := range w.entries {
				seen = append(seen, w.binding.target.StateKey+"@"+pe.rangeKey)
			}
		}
		require.Equal(t, []string{"a_table.v1@" + lowerRange, "a_table.v1@" + upperRange}, seen)

		seen = nil
		for _, w := range d.stagedWork(m.StateKeyFilter([]string{"other.v1"})) {
			seen = append(seen, w.binding.target.StateKey)
		}
		require.Empty(t, seen)
	})
}

func mustMarshal(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

// compressBytes encodes a checkpoint the way rows in the old format hold it.
func compressBytes(b []byte) ([]byte, error) {
	var gzb bytes.Buffer
	w := gzip.NewWriter(&gzb)
	if _, err := w.Write(b); err != nil {
		return nil, err
	} else if err := w.Close(); err != nil {
		return nil, err
	}
	return gzb.Bytes(), nil
}
