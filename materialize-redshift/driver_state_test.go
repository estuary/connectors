package connector

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"testing"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/stretchr/testify/require"
)

func TestParseState(t *testing.T) {
	t.Run("a task crossing over opens with no state", func(t *testing.T) {
		for _, raw := range []json.RawMessage{nil, json.RawMessage(``), json.RawMessage(`{}`)} {
			pending, err := parseState(raw)
			require.NoError(t, err)
			require.Empty(t, pending)
		}
	})

	t.Run("entries are keyed by state key", func(t *testing.T) {
		pending, err := parseState(json.RawMessage(`{
			"a_table.v1": {
				"storeManifest": "p/x/files.manifest",
				"deleteManifest": "",
				"objects": ["p/x/files.manifest", "p/x/f1"],
				"mustMerge": true,
				"widen": ["\"col\""]
			}
		}`))
		require.NoError(t, err)
		require.Len(t, pending, 1)
		require.Equal(t, &stagedTransaction{
			StoreManifest: "p/x/files.manifest",
			Objects:       []string{"p/x/files.manifest", "p/x/f1"},
			MustMerge:     true,
			Widen:         []string{`"col"`},
		}, pending["a_table.v1"])
	})

	t.Run("an entry with unknown fields is rejected", func(t *testing.T) {
		_, err := parseState(json.RawMessage(`{"a_table.v1": {"Queries": ["Q1"], "ToDelete": []}}`))
		require.Error(t, err)
	})
}

func TestStagedTransactionToken(t *testing.T) {
	require.Equal(t, "p/s/files.manifest", (&stagedTransaction{
		StoreManifest:  "p/s/files.manifest",
		DeleteManifest: "p/d/files.manifest",
	}).token())
	require.Equal(t, "p/d/files.manifest", (&stagedTransaction{
		DeleteManifest: "p/d/files.manifest",
	}).token())
}

func TestParseCheckpointsRow(t *testing.T) {
	t.Run("a token payload", func(t *testing.T) {
		tokens, legacy, err := parseCheckpointsRow([]byte(`{"00000000-ffffffff":{"a_table.v1":"p/x/files.manifest"}}`))
		require.NoError(t, err)
		require.Nil(t, legacy)
		require.Equal(t, tokenPayload{"00000000-ffffffff": {"a_table.v1": "p/x/files.manifest"}}, tokens)
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
		Objects:        []string{"p/1/files.manifest", "p/1d/files.manifest"},
		MustMerge:      true,
		Widen:          []string{`"col"`},
	}
	next := &stagedTransaction{
		StoreManifest: "p/2/files.manifest",
		Objects:       []string{"p/2/files.manifest"},
		Widen:         []string{},
	}

	var before, patch any
	require.NoError(t, json.Unmarshal(mustMarshal(t, map[string]*stagedTransaction{"a_table.v1": previous}), &before))
	require.NoError(t, json.Unmarshal(mustMarshal(t, map[string]*stagedTransaction{"a_table.v1": next}), &patch))

	reduced, err := json.Marshal(boilerplate.ApplyMergePatch(before, patch))
	require.NoError(t, err)

	pending, err := parseState(reduced)
	require.NoError(t, err)
	require.Equal(t, next, pending["a_table.v1"])
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
