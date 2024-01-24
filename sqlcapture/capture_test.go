package sqlcapture

import (
	"encoding/json"
	"net/url"
	"strings"
	"testing"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestMigrateState(t *testing.T) {
	mustMarshal := func(t *testing.T, in any) json.RawMessage {
		t.Helper()
		bytes, err := json.Marshal(in)
		require.NoError(t, err)
		return bytes
	}

	res1 := Resource{
		Namespace: "first",
		Stream:    "table",
	}
	binding1 := &pf.CaptureSpec_Binding{
		ResourceConfigJson: mustMarshal(t, res1),
		ResourcePath:       []string{res1.Namespace, res1.Stream},
		StateKey:           url.QueryEscape(strings.Join([]string{res1.Namespace, res1.Stream}, "/")),
	}
	ts1 := &TableState{
		Mode:            "Backfill",
		KeyColumns:      []string{"key1", "key2"},
		Scanned:         []byte("something"),
		BackfilledCount: 10,
	}

	res2 := Resource{
		Namespace: "first",
		Stream:    "other",
	}
	binding2 := &pf.CaptureSpec_Binding{
		ResourceConfigJson: mustMarshal(t, res2),
		ResourcePath:       []string{res2.Namespace, res2.Stream},
		StateKey:           url.QueryEscape(strings.Join([]string{res2.Namespace, res2.Stream}, "/")),
	}
	ts2 := &TableState{
		Mode:            "Backfill",
		KeyColumns:      []string{},
		Scanned:         []byte("else"),
		BackfilledCount: 100,
	}

	res3 := Resource{
		Namespace: "another",
		Stream:    "table",
	}
	binding3 := &pf.CaptureSpec_Binding{
		ResourceConfigJson: mustMarshal(t, res3),
		ResourcePath:       []string{res3.Namespace, res3.Stream},
		StateKey:           url.QueryEscape(strings.Join([]string{res3.Namespace, res3.Stream}, "/")),
	}
	ts3 := &TableState{
		Mode:            "Active",
		KeyColumns:      []string{"aKey"},
		Scanned:         nil,
		BackfilledCount: 1,
	}

	originalCheckpoint := map[string]interface{}{
		"cursor": "someCursor",
		"streams": map[string]interface{}{
			"first.table":   ts1,
			"first.other":   ts2,
			"another.table": ts3,
		},
	}
	originalCheckpointJson := mustMarshal(t, originalCheckpoint)

	t.Run("basic migration", func(t *testing.T) {
		bindings := []*pf.CaptureSpec_Binding{binding1, binding2, binding3}

		var originalState PersistentState
		require.NoError(t, json.Unmarshal(originalCheckpointJson, &originalState))

		migrated, err := migrateState(&originalState, bindings)
		require.NoError(t, err)
		require.True(t, migrated)

		want := PersistentState{
			Cursor: "someCursor",
			Streams: map[boilerplate.StateKey]*TableState{
				boilerplate.StateKey("first%2Ftable"):   ts1,
				boilerplate.StateKey("first%2Fother"):   ts2,
				boilerplate.StateKey("another%2Ftable"): ts3,
			},
		}

		require.Equal(t, want, originalState)
	})

	t.Run("state for a stream but no binding for it", func(t *testing.T) {
		bindings := []*pf.CaptureSpec_Binding{binding1, binding3}

		var originalState PersistentState
		require.NoError(t, json.Unmarshal(originalCheckpointJson, &originalState))

		migrated, err := migrateState(&originalState, bindings)
		require.NoError(t, err)
		require.True(t, migrated)

		want := PersistentState{
			Cursor: "someCursor",
			Streams: map[boilerplate.StateKey]*TableState{
				boilerplate.StateKey("first%2Ftable"):   ts1,
				boilerplate.StateKey("another%2Ftable"): ts3,
			},
		}

		require.Equal(t, want, originalState)
	})

	t.Run("binding for a stream but no state for it", func(t *testing.T) {
		bindings := []*pf.CaptureSpec_Binding{binding1, binding2, binding3}

		var originalState PersistentState
		require.NoError(t, json.Unmarshal(originalCheckpointJson, &originalState))
		delete(originalState.OldStreams, "first.other")

		migrated, err := migrateState(&originalState, bindings)
		require.NoError(t, err)
		require.True(t, migrated)

		want := PersistentState{
			Cursor: "someCursor",
			Streams: map[boilerplate.StateKey]*TableState{
				boilerplate.StateKey("first%2Ftable"):   ts1,
				boilerplate.StateKey("another%2Ftable"): ts3,
			},
		}

		require.Equal(t, want, originalState)
	})

	t.Run("both streams and old streams non-nil is an error", func(t *testing.T) {
		s := &PersistentState{
			Cursor:     "",
			Streams:    map[boilerplate.StateKey]*TableState{},
			OldStreams: map[string]*TableState{},
		}

		migrated, err := migrateState(s, nil)
		require.False(t, migrated)
		require.Error(t, err)
	})

	t.Run("skip migration if it's already done", func(t *testing.T) {
		s := &PersistentState{
			Cursor:  "",
			Streams: map[boilerplate.StateKey]*TableState{},
		}

		migrated, err := migrateState(s, nil)
		require.False(t, migrated)
		require.NoError(t, err)
	})

	t.Run("skip migration if there is no old state", func(t *testing.T) {
		s := &PersistentState{
			Cursor: "",
		}

		migrated, err := migrateState(s, nil)
		require.False(t, migrated)
		require.NoError(t, err)
	})
}
