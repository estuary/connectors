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

// compressBytes produces the gzipped form the fenced connector this one
// replaces wrote its runtime checkpoint in. Only the reading half of that
// format is still production code, so its counterpart lives here.
//
// TODO: remove after a deployment.
func compressBytes(t *testing.T, b []byte) []byte {
	t.Helper()

	var gzb bytes.Buffer
	w := gzip.NewWriter(&gzb)
	_, err := w.Write(b)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	return gzb.Bytes()
}

func TestParseConnectorState(t *testing.T) {
	for _, tt := range []struct {
		name  string
		input string
		want  connectorState
	}{
		{
			// The connector this one replaces emitted no state at all, so a
			// task crossing over opens with nothing staged.
			name:  "no state, as written by the fenced connector",
			input: "",
			want:  connectorState{},
		},
		{
			name:  "empty state document",
			input: `{}`,
			want:  connectorState{},
		},
		{
			name:  "store only",
			input: `{"sk1":{"StoreManifest":"data/m1.manifest","Files":["data/m1.manifest","data/f1"]}}`,
			want: connectorState{"sk1": {
				StoreManifest: "data/m1.manifest",
				Files:         []string{"data/m1.manifest", "data/f1"},
			}},
		},
		{
			name:  "stores, deletes and widening",
			input: `{"sk1":{"StoreManifest":"data/m1.manifest","DeleteManifest":"data/d1.manifest","Files":["data/m1.manifest","data/d1.manifest"],"MustMerge":true,"Widen":["\"col\""]}}`,
			want: connectorState{"sk1": {
				StoreManifest:  "data/m1.manifest",
				DeleteManifest: "data/d1.manifest",
				Files:          []string{"data/m1.manifest", "data/d1.manifest"},
				MustMerge:      true,
				Widen:          []string{`"col"`},
			}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseConnectorState(json.RawMessage(tt.input))
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseConnectorStateRejectsUnknownEntries(t *testing.T) {
	// An entry this connector cannot fully understand describes data it would
	// otherwise apply incorrectly, so it fails rather than dropping the field.
	_, err := parseConnectorState(json.RawMessage(`{"sk1":{"Query":"INSERT INTO t VALUES (1)"}}`))
	require.ErrorContains(t, err, `parsing staged transaction "sk1"`)
}

func TestStagedTransactionPatchReplacesEntry(t *testing.T) {
	// Entries travel as RFC 7396 merge patches, which merge field by field, so
	// a transaction that stores but does not delete must not inherit the
	// DeleteManifest of the transaction staged before it. A stale manifest of
	// hard-deleted keys would delete rows from the target table that this
	// transaction never asked to remove.
	var stale = &stagedTransaction{
		StoreManifest:  "data/old-store.manifest",
		DeleteManifest: "data/old-delete.manifest",
		Files:          []string{"data/old-store.manifest", "data/old-delete.manifest"},
		MustMerge:      true,
		Widen:          []string{`"col"`},
	}
	var fresh = &stagedTransaction{
		StoreManifest: "data/new-store.manifest",
		Files:         []string{"data/new-store.manifest"},
	}

	var decode = func(entry *stagedTransaction) any {
		raw, err := json.Marshal(connectorState{"sk1": entry})
		require.NoError(t, err)
		var out any
		require.NoError(t, json.Unmarshal(raw, &out))
		return out
	}

	merged, err := json.Marshal(boilerplate.ApplyMergePatch(decode(stale), decode(fresh)))
	require.NoError(t, err)

	got, err := parseConnectorState(merged)
	require.NoError(t, err)
	require.Equal(t, connectorState{"sk1": fresh}, got, "the fresh entry must replace the stale one outright")
}

func TestStagedTransactionToken(t *testing.T) {
	// Either manifest names the transaction uniquely, and both are written by
	// the same apply transaction.
	require.Equal(t, "data/m1.manifest", (&stagedTransaction{
		StoreManifest:  "data/m1.manifest",
		DeleteManifest: "data/d1.manifest",
	}).token())
	require.Equal(t, "data/d1.manifest", (&stagedTransaction{
		DeleteManifest: "data/d1.manifest",
	}).token())
}

func TestTokenStoreParseCheckpoint(t *testing.T) {
	t.Run("no row content", func(t *testing.T) {
		tokens, legacy, err := tokenStore{}.parseCheckpoint(nil)
		require.NoError(t, err)
		require.Empty(t, tokens)
		require.Nil(t, legacy)
	})

	t.Run("applied tokens", func(t *testing.T) {
		tokens, legacy, err := tokenStore{}.parseCheckpoint([]byte(`{"00000000-ffffffff":{"sk1":"data/a.manifest"}}`))
		require.NoError(t, err)
		require.Equal(t, appliedTokens{"00000000-ffffffff": {"sk1": "data/a.manifest"}}, tokens)
		require.Nil(t, legacy)
	})

	// TODO: remove the two fenced-format cases after a deployment.
	t.Run("a row still in the fenced format", func(t *testing.T) {
		// The fenced connector stored base64(gzip(checkpoint)), which cannot
		// begin with '{'.
		want := []byte("a runtime checkpoint")
		raw := []byte(base64.StdEncoding.EncodeToString(compressBytes(t, want)))
		require.NotEqual(t, byte('{'), raw[0])

		tokens, legacy, err := tokenStore{}.parseCheckpoint(raw)
		require.NoError(t, err)
		require.Empty(t, tokens)
		require.Equal(t, want, legacy)
	})

	t.Run("an uncompressed row in the fenced format", func(t *testing.T) {
		want := []byte("a runtime checkpoint")
		tokens, legacy, err := tokenStore{}.parseCheckpoint([]byte(base64.StdEncoding.EncodeToString(want)))
		require.NoError(t, err)
		require.Empty(t, tokens)
		require.Equal(t, want, legacy)
	})

	t.Run("an unparseable token payload fails loudly", func(t *testing.T) {
		_, _, err := tokenStore{}.parseCheckpoint([]byte(`{"00000000-ffffffff":`))
		require.ErrorContains(t, err, "parsing applied tokens")
	})
}
