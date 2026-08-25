package connector

import (
	"encoding/json"
	"testing"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/stretchr/testify/require"
)

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
