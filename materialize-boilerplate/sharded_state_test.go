package boilerplate

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

const (
	fullRange  = "00000000-ffffffff"
	lowerRange = "00000000-7fffffff"
	upperRange = "80000000-ffffffff"
)

type testEntry struct {
	Files []string `json:"files"`
}

func newTestState(rangeKey string, liveStateKeys ...string) *ShardedState[*testEntry] {
	var begin, end uint32
	if _, err := fmt.Sscanf(rangeKey, "%08x-%08x", &begin, &end); err != nil {
		panic(err)
	}
	return NewShardedState[*testEntry](begin, end, liveStateKeys)
}

func te(files ...string) *testEntry { return &testEntry{Files: files} }

// collect is an apply func recording what it was called with.
func collect(into *ShardedEntries[*testEntry]) func(ShardedEntries[*testEntry]) error {
	return func(batch ShardedEntries[*testEntry]) error {
		*into = batch
		return nil
	}
}

func noApply(t *testing.T) func(ShardedEntries[*testEntry]) error {
	return func(ShardedEntries[*testEntry]) error {
		t.Fatal("apply must not be called")
		return nil
	}
}

// reduce applies the state updates to doc, as the runtime does.
func reduce(t *testing.T, doc json.RawMessage, updates ...*pf.ConnectorState) json.RawMessage {
	t.Helper()
	var cur any = map[string]any{}
	if len(doc) > 0 {
		require.NoError(t, json.Unmarshal(doc, &cur))
	}
	for _, u := range updates {
		if u == nil {
			continue
		}
		require.True(t, u.MergePatch, "every update is a merge patch")
		var patch any
		require.NoError(t, json.Unmarshal(u.UpdatedJson, &patch))
		cur = ApplyMergePatch(cur, patch)
	}
	out, err := json.Marshal(cur)
	require.NoError(t, err)
	return out
}

func TestShardedStateRecover(t *testing.T) {
	var doc = json.RawMessage(`{
		"a.v1": {
			"00000000-7fffffff": {"files": ["l"]},
			"80000000-ffffffff": {"files": ["u"]}
		}
	}`)

	t.Run("no state", func(t *testing.T) {
		for _, raw := range []json.RawMessage{nil, json.RawMessage(` `), json.RawMessage(`{}`), json.RawMessage(`null`)} {
			var s = newTestState(fullRange, "a.v1")
			require.NoError(t, s.Recover(raw))
			require.Empty(t, s.Pending())
			require.False(t, s.OwnRecovered())
		}
	})

	t.Run("the primary keeps every range", func(t *testing.T) {
		var s = newTestState(lowerRange, "a.v1")
		require.Equal(t, lowerRange, s.RangeKey())
		require.NoError(t, s.Recover(doc))
		require.Equal(t, ShardedEntries[*testEntry]{"a.v1": {lowerRange: te("l"), upperRange: te("u")}}, s.Pending())
		require.True(t, s.OwnRecovered())
	})

	t.Run("a non-primary shard validates and discards it", func(t *testing.T) {
		var s = newTestState(upperRange, "a.v1")
		require.NoError(t, s.Recover(doc))
		require.Empty(t, s.Pending())
		require.True(t, s.OwnRecovered())

		require.Error(t, s.Recover(json.RawMessage(`{"a.v1": {"80000000-ffffffff": {"bogus": 1}}}`)))
	})

	t.Run("own range not recovered", func(t *testing.T) {
		var s = newTestState(fullRange, "a.v1")
		require.NoError(t, s.Recover(doc))
		require.False(t, s.OwnRecovered())
	})

	t.Run("recovery replaces the pending entries", func(t *testing.T) {
		var s = newTestState(lowerRange, "a.v1")
		require.NoError(t, s.Recover(doc))
		require.NoError(t, s.Recover(json.RawMessage(`{"a.v1": {"80000000-ffffffff": {"files": ["u"]}}}`)))
		require.Equal(t, ShardedEntries[*testEntry]{"a.v1": {upperRange: te("u")}}, s.Pending())
		require.False(t, s.OwnRecovered())
	})

	t.Run("rejected documents", func(t *testing.T) {
		for name, raw := range map[string]string{
			"not an object":          `[]`,
			"a bucket not an object": `{"a.v1": "x"}`,
			"an unknown entry field": `{"a.v1": {"00000000-ffffffff": {"files": [], "extra": 1}}}`,
			"a flat entry":           `{"a.v1": {"files": ["f"]}}`,
		} {
			require.Error(t, newTestState(fullRange, "a.v1").Recover(json.RawMessage(raw)), name)
		}
	})
}

func TestShardedStateStartCommit(t *testing.T) {
	t.Run("a merge patch of this transaction's entries under this range", func(t *testing.T) {
		var s = newTestState(upperRange, "a.v1", "b.v1")
		update, err := s.StartCommit(map[string]*testEntry{"a.v1": te("a1"), "b.v1": te("b1")})
		require.NoError(t, err)
		require.True(t, update.MergePatch)
		require.JSONEq(t, `{
			"a.v1": {"80000000-ffffffff": {"files": ["a1"]}},
			"b.v1": {"80000000-ffffffff": {"files": ["b1"]}}
		}`, string(update.UpdatedJson))
		// A non-primary shard keeps nothing.
		require.Empty(t, s.Pending())
	})

	t.Run("nothing staged", func(t *testing.T) {
		update, err := newTestState(fullRange, "a.v1").StartCommit(nil)
		require.NoError(t, err)
		require.Equal(t, &pf.ConnectorState{UpdatedJson: json.RawMessage(`{}`), MergePatch: true}, update)
	})

	t.Run("the primary records its own entries", func(t *testing.T) {
		var s = newTestState(lowerRange, "a.v1")
		_, err := s.StartCommit(map[string]*testEntry{"a.v1": te("a1")})
		require.NoError(t, err)
		require.Equal(t, ShardedEntries[*testEntry]{"a.v1": {lowerRange: te("a1")}}, s.Pending())
	})
}

func TestShardedStateAcknowledge(t *testing.T) {
	var peerPatch = json.RawMessage(`{"a.v1": {"80000000-ffffffff": {"files": ["u"]}}}`)

	t.Run("a non-primary shard applies nothing", func(t *testing.T) {
		var s = newTestState(upperRange, "a.v1")
		_, err := s.StartCommit(map[string]*testEntry{"a.v1": te("u")})
		require.NoError(t, err)
		update, err := s.Acknowledge([]json.RawMessage{peerPatch, json.RawMessage(`null`)}, nil, noApply(t))
		require.NoError(t, err)
		require.Nil(t, update)
	})

	t.Run("a state reset patch is rejected", func(t *testing.T) {
		var s = newTestState(lowerRange, "a.v1")
		_, err := s.Acknowledge([]json.RawMessage{json.RawMessage(" null\t")}, nil, noApply(t))
		require.ErrorContains(t, err, "state reset")
	})

	t.Run("a peer's entry colliding with its pending one is rejected", func(t *testing.T) {
		var s = newTestState(lowerRange, "b.v1")
		require.NoError(t, s.Recover(peerPatch))
		_, err := s.Acknowledge([]json.RawMessage{peerPatch}, nil, noApply(t))
		require.ErrorContains(t, err, "still pending")
	})

	t.Run("a malformed peer patch is rejected", func(t *testing.T) {
		var s = newTestState(lowerRange, "a.v1")
		_, err := s.Acknowledge([]json.RawMessage{json.RawMessage(`{"a.v1": {"80000000-ffffffff": {"bogus": 1}}}`)}, nil, noApply(t))
		require.Error(t, err)
	})

	t.Run("entries stay pending while apply runs, and after it fails", func(t *testing.T) {
		var s = newTestState(fullRange, "a.v1")
		require.NoError(t, s.Recover(json.RawMessage(`{"a.v1": {"00000000-ffffffff": {"files": ["f"]}}}`)))
		var boom = errors.New("boom")
		update, err := s.Acknowledge(nil, nil, func(batch ShardedEntries[*testEntry]) error {
			require.Equal(t, batch, s.Pending())
			return boom
		})
		require.ErrorIs(t, err, boom)
		require.Nil(t, update)
		require.Equal(t, 1, s.Pending().Len())
	})

	t.Run("nothing pending", func(t *testing.T) {
		var s = newTestState(fullRange, "a.v1")
		// What clearing a state key's last entry leaves behind.
		require.NoError(t, s.Recover(json.RawMessage(`{"a.v1": {}}`)))
		update, err := s.Acknowledge(nil, nil, noApply(t))
		require.NoError(t, err)
		require.Nil(t, update)
	})
}

func TestShardedStateStateKeys(t *testing.T) {
	var doc = json.RawMessage(`{
		"a.v1": {"00000000-7fffffff": {"files": ["a"]}, "80000000-ffffffff": {"files": ["a2"]}},
		"b.v1": {"80000000-ffffffff": {"files": ["b"]}},
		"gone.v1": {"80000000-ffffffff": {"files": ["g"]}}
	}`)
	var recovered = func(t *testing.T) *ShardedState[*testEntry] {
		var s = newTestState(lowerRange, "a.v1", "b.v1")
		require.NoError(t, s.Recover(doc))
		return s
	}

	t.Run("nil applies every live state key", func(t *testing.T) {
		var s = recovered(t)
		var applied ShardedEntries[*testEntry]
		update, err := s.Acknowledge(nil, nil, collect(&applied))
		require.NoError(t, err)
		require.Equal(t, ShardedEntries[*testEntry]{
			"a.v1": {lowerRange: te("a"), upperRange: te("a2")},
			"b.v1": {upperRange: te("b")},
		}, applied)
		require.JSONEq(t, `{
			"a.v1": {"00000000-7fffffff": null, "80000000-ffffffff": null},
			"b.v1": {"80000000-ffffffff": null}
		}`, string(update.UpdatedJson))
		// A state key without a binding stays pending.
		require.Equal(t, ShardedEntries[*testEntry]{"gone.v1": {upperRange: te("g")}}, s.Pending())
	})

	t.Run("a subset applies exactly those keys", func(t *testing.T) {
		var s = recovered(t)
		var applied ShardedEntries[*testEntry]
		update, err := s.Acknowledge(nil, []string{"b.v1", "gone.v1"}, collect(&applied))
		require.NoError(t, err)
		require.Equal(t, ShardedEntries[*testEntry]{"b.v1": {upperRange: te("b")}}, applied)
		require.JSONEq(t, `{"b.v1": {"80000000-ffffffff": null}}`, string(update.UpdatedJson))
		require.Equal(t, 3, s.Pending().Len())

		// Draining it again finds nothing.
		update, err = s.Acknowledge(nil, []string{"b.v1"}, noApply(t))
		require.NoError(t, err)
		require.Nil(t, update)
	})

	t.Run("an empty subset applies nothing", func(t *testing.T) {
		update, err := recovered(t).Acknowledge(nil, []string{}, noApply(t))
		require.NoError(t, err)
		require.Nil(t, update)
	})
}

// A runtime v1 session: no statePatches, and each update merges into the
// shard's own state. Every transaction's work must be applied, not just the
// last one's (#5157).
func TestShardedStateRuntimeV1Session(t *testing.T) {
	var s = newTestState(fullRange, "a.v1")
	var doc json.RawMessage
	var applied []string
	var apply = func(batch ShardedEntries[*testEntry]) error {
		for _, bucket := range batch {
			for _, e := range bucket {
				applied = append(applied, e.Files...)
			}
		}
		return nil
	}

	for _, files := range []string{"t1", "t2", "t3"} {
		update, err := s.StartCommit(map[string]*testEntry{"a.v1": te(files)})
		require.NoError(t, err)
		doc = reduce(t, doc, update)

		update, err = s.Acknowledge(nil, nil, apply)
		require.NoError(t, err)
		doc = reduce(t, doc, update)
	}
	require.Equal(t, []string{"t1", "t2", "t3"}, applied)
	require.JSONEq(t, `{"a.v1": {}}`, string(doc))
}

// Two shards on runtime v2: patches aggregate into one consolidated state,
// and every Acknowledge carries all of the transaction's StartedCommit
// patches. The primary applies both shards' work; after a crash, a new
// primary recovers what was committed but not acknowledged.
func TestShardedStateRuntimeV2Shards(t *testing.T) {
	var primary = newTestState(lowerRange, "a.v1", "b.v1")
	var peer = newTestState(upperRange, "a.v1", "b.v1")
	var doc json.RawMessage
	var applied []string
	var apply = func(batch ShardedEntries[*testEntry]) error {
		for _, sk := range []string{"a.v1", "b.v1"} {
			for _, rk := range []string{lowerRange, upperRange} {
				if e, ok := batch[sk][rk]; ok {
					applied = append(applied, e.Files...)
				}
			}
		}
		return nil
	}

	var txn = func(t *testing.T, primaryStaged, peerStaged map[string]*testEntry) []json.RawMessage {
		p1, err := primary.StartCommit(primaryStaged)
		require.NoError(t, err)
		p2, err := peer.StartCommit(peerStaged)
		require.NoError(t, err)
		doc = reduce(t, doc, p1, p2)
		return []json.RawMessage{p1.UpdatedJson, p2.UpdatedJson}
	}
	// ack returns the primary's clearing patch.
	var ack = func(t *testing.T, patches []json.RawMessage) string {
		u1, err := primary.Acknowledge(patches, nil, apply)
		require.NoError(t, err)
		u2, err := peer.Acknowledge(patches, nil, noApply(t))
		require.NoError(t, err)
		require.Nil(t, u2)
		doc = reduce(t, doc, u1)
		return string(u1.UpdatedJson)
	}

	var cleared = ack(t, txn(t,
		map[string]*testEntry{"a.v1": te("t1-l-a")},
		map[string]*testEntry{"a.v1": te("t1-u-a"), "b.v1": te("t1-u-b")}))
	require.Equal(t, []string{"t1-l-a", "t1-u-a", "t1-u-b"}, applied)
	require.JSONEq(t, `{
		"a.v1": {"00000000-7fffffff": null, "80000000-ffffffff": null},
		"b.v1": {"80000000-ffffffff": null}
	}`, cleared)
	require.JSONEq(t, `{"a.v1": {}, "b.v1": {}}`, string(doc))

	// Committed, then the session ends before its Acknowledge.
	txn(t,
		map[string]*testEntry{"b.v1": te("t2-l-b")},
		map[string]*testEntry{"a.v1": te("t2-u-a")})
	applied = nil

	// Every shard of the next session opens with the consolidated state, and
	// its first Acknowledge carries no patches.
	primary = newTestState(lowerRange, "a.v1", "b.v1")
	peer = newTestState(upperRange, "a.v1", "b.v1")
	require.NoError(t, primary.Recover(doc))
	require.NoError(t, peer.Recover(doc))
	ack(t, nil)
	require.Equal(t, []string{"t2-u-a", "t2-l-b"}, applied)
	require.JSONEq(t, `{"a.v1": {}, "b.v1": {}}`, string(doc))
}
