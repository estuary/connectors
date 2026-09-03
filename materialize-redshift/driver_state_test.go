package connector

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
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

func entry(manifest string, files ...string) *stagedTransaction {
	e := newStagedTransaction()
	e.StoreManifest = manifest
	e.StoreFiles = files
	return e
}

// testTransactor builds a transactor of the given range with bindings for
// each state key, without any warehouse or S3 client.
func testTransactor(rangeKey string, stateKeys ...string) *transactor {
	var keyBegin, keyEnd uint32
	if _, err := fmt.Sscanf(rangeKey, "%08x-%08x", &keyBegin, &keyEnd); err != nil {
		panic(err)
	}
	var d = &transactor{
		rangeKey: rangeKey,
		primary:  keyBegin == 0,
		applied:  make(map[string]bool),
		pending:  make(pendingState),
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

	t.Run("entries are keyed by range then state key", func(t *testing.T) {
		pending, err := parseState(json.RawMessage(`{
			"00000000-ffffffff": {
				"a_table.v1": {
					"storeManifest": "p/x/files.manifest",
					"deleteManifest": "",
					"storeFiles": ["p/x/f1"],
					"deleteFiles": [],
					"mustMerge": true,
					"widen": ["\"col\""]
				}
			}
		}`))
		require.NoError(t, err)
		require.Equal(t, pendingState{fullRange: {"a_table.v1": &stagedTransaction{
			StoreManifest: "p/x/files.manifest",
			StoreFiles:    []string{"p/x/f1"},
			DeleteFiles:   []string{},
			MustMerge:     true,
			Widen:         []string{`"col"`},
		}}}, pending)
	})

	t.Run("cleared entries and emptied ranges are dropped", func(t *testing.T) {
		pending, err := parseState(json.RawMessage(`{"00000000-7fffffff": {"a_table.v1": null}}`))
		require.NoError(t, err)
		require.Empty(t, pending)
	})

	t.Run("an entry with unknown fields is rejected", func(t *testing.T) {
		_, err := parseState(json.RawMessage(`{"00000000-ffffffff": {"a_table.v1": {"Queries": ["Q1"]}}}`))
		require.Error(t, err)
	})

	t.Run("a top-level key that is not a range is rejected", func(t *testing.T) {
		_, err := parseState(json.RawMessage(`{"a_table.v1": {"storeManifest": "m", "deleteManifest": "", "storeFiles": [], "deleteFiles": [], "mustMerge": false, "widen": []}}`))
		require.Error(t, err)
	})
}

func TestStagedTransactionTokenAndObjects(t *testing.T) {
	e := &stagedTransaction{
		StoreManifest:  "p/s/files.manifest",
		DeleteManifest: "p/d/files.manifest",
		StoreFiles:     []string{"p/s/f1", "p/s/f2"},
		DeleteFiles:    []string{"p/d/f1"},
	}
	require.Equal(t, "p/s/files.manifest", e.token())
	require.ElementsMatch(t, []string{"p/s/files.manifest", "p/d/files.manifest", "p/s/f1", "p/s/f2", "p/d/f1"}, e.objects())

	require.Equal(t, "p/d/files.manifest", (&stagedTransaction{DeleteManifest: "p/d/files.manifest"}).token())
}

func TestParseCheckpointsRow(t *testing.T) {
	t.Run("a token payload", func(t *testing.T) {
		tokens, legacy, err := parseCheckpointsRow([]byte(`{"00000000-ffffffff":{"a_table.v1":"p/x/files.manifest"}}`))
		require.NoError(t, err)
		require.Nil(t, legacy)
		require.Equal(t, tokenPayload{fullRange: {"a_table.v1": "p/x/files.manifest"}}, tokens)
	})

	t.Run("a row in the old format", func(t *testing.T) {
		checkpoint := []byte("some-runtime-checkpoint")
		compressed, err := compressBytes(checkpoint)
		require.NoError(t, err)

		tokens, legacy, err := parseCheckpointsRow([]byte(base64.StdEncoding.EncodeToString(compressed)))
		require.NoError(t, err)
		require.Nil(t, tokens)
		require.Equal(t, checkpoint, legacy)
	})

	t.Run("a row in the old format that was never compressed", func(t *testing.T) {
		checkpoint := []byte("some-runtime-checkpoint")
		tokens, legacy, err := parseCheckpointsRow([]byte(base64.StdEncoding.EncodeToString(checkpoint)))
		require.NoError(t, err)
		require.Nil(t, tokens)
		require.Equal(t, checkpoint, legacy)
	})
}

func TestEntryPatchReplacesPrevious(t *testing.T) {
	previous := &stagedTransaction{
		StoreManifest:  "p/1/files.manifest",
		DeleteManifest: "p/1d/files.manifest",
		StoreFiles:     []string{"p/1/f1"},
		DeleteFiles:    []string{"p/1d/f1"},
		MustMerge:      true,
		Widen:          []string{`"col"`},
	}
	next := entry("p/2/files.manifest", "p/2/f1")

	var before, patch any
	require.NoError(t, json.Unmarshal(mustMarshal(t, pendingState{fullRange: {"a_table.v1": previous}}), &before))
	require.NoError(t, json.Unmarshal(mustMarshal(t, pendingState{fullRange: {"a_table.v1": next}}), &patch))

	reduced, err := json.Marshal(boilerplate.ApplyMergePatch(before, patch))
	require.NoError(t, err)

	pending, err := parseState(reduced)
	require.NoError(t, err)
	require.Equal(t, next, pending[fullRange]["a_table.v1"])
}

func TestStateRouting(t *testing.T) {
	var state = json.RawMessage(`{
		"00000000-7fffffff": {"a_table.v1": {"storeManifest": "l/files.manifest", "deleteManifest": "", "storeFiles": ["l/f"], "deleteFiles": [], "mustMerge": false, "widen": []}},
		"80000000-ffffffff": {"a_table.v1": {"storeManifest": "u/files.manifest", "deleteManifest": "", "storeFiles": ["u/f"], "deleteFiles": [], "mustMerge": true, "widen": []}}
	}`)

	t.Run("the primary recovers every range", func(t *testing.T) {
		d := testTransactor(lowerRange, "a_table.v1")
		require.NoError(t, d.UnmarshalState(state))
		require.Len(t, d.pending, 2)
		require.Equal(t, "l/files.manifest", d.pending[lowerRange]["a_table.v1"].StoreManifest)
		require.Equal(t, "u/files.manifest", d.pending[upperRange]["a_table.v1"].StoreManifest)
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
		d.pending[lowerRange] = map[string]*stagedTransaction{"a_table.v1": entry("own/files.manifest", "own/f")}
		require.NoError(t, d.mergePeerStatePatches([]json.RawMessage{state}))
		require.Equal(t, "own/files.manifest", d.pending[lowerRange]["a_table.v1"].StoreManifest)
		require.Equal(t, "u/files.manifest", d.pending[upperRange]["a_table.v1"].StoreManifest)

		require.Error(t, d.mergePeerStatePatches([]json.RawMessage{json.RawMessage(`null`)}))

		// The peer's previous transaction is still pending: its work would be lost.
		require.Error(t, d.mergePeerStatePatches([]json.RawMessage{state}))
	})

	t.Run("an entry with no live binding stays pending and gives Acknowledge nothing to do", func(t *testing.T) {
		d := testTransactor(fullRange, "a_table.v1")
		require.NoError(t, d.UnmarshalState(json.RawMessage(`{
			"00000000-ffffffff": {"gone_table.v1": {"storeManifest": "g/files.manifest", "deleteManifest": "", "storeFiles": ["g/f"], "deleteFiles": [], "mustMerge": false, "widen": []}}
		}`)))
		patch, err := d.Acknowledge(t.Context(), nil, nil)
		require.NoError(t, err)
		require.Nil(t, patch)
		require.NotNil(t, d.pending[fullRange]["gone_table.v1"])
	})

	t.Run("staged work groups every range's entries per binding and leaves unknown bindings pending", func(t *testing.T) {
		d := testTransactor(lowerRange, "a_table.v1")
		require.NoError(t, d.UnmarshalState(state))
		d.pending[upperRange]["gone_table.v1"] = entry("g/files.manifest", "g/f")

		var seen []string
		for b, entries := range d.stagedWork(m.StateKeyFilter(nil)) {
			for _, pe := range entries {
				seen = append(seen, b.target.StateKey+"@"+pe.rangeKey)
			}
		}
		require.Equal(t, []string{"a_table.v1@" + lowerRange, "a_table.v1@" + upperRange}, seen)

		seen = nil
		for b := range d.stagedWork(m.StateKeyFilter([]string{"other.v1"})) {
			seen = append(seen, b.target.StateKey)
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
