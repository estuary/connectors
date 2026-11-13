package boilerplate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"

	m "github.com/estuary/connectors/go/materialize"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// RangeKey names a shard's key range in ShardedState.
func RangeKey(keyBegin, keyEnd uint32) string {
	return fmt.Sprintf("%08x-%08x", keyBegin, keyEnd)
}

// ShardedEntries are entries by state key, then by the range key of the
// shard that staged them.
type ShardedEntries[E any] map[string]map[string]E

func (s ShardedEntries[E]) add(stateKey, rangeKey string, e E) {
	if s[stateKey] == nil {
		s[stateKey] = make(map[string]E)
	}
	s[stateKey][rangeKey] = e
}

// Len is the number of entries across every state key.
func (s ShardedEntries[E]) Len() int {
	var n int
	for _, bucket := range s {
		n += len(bucket)
	}
	return n
}

// ShardedState is the staged-but-unapplied work of a materialization that
// records it in the recovery log and applies it in Acknowledge, across every
// shard of the task. It persists as {stateKey: {rangeKey: entry}}, where
// rangeKey names the range of the shard that staged the entry, so shards'
// merge patches never overlap.
//
// Only the primary shard, whose range begins at 0, holds and applies
// pending work. It recovers the whole state, records its own entries at
// StartCommit, and learns its peers' from Acknowledge's statePatches. Other
// shards only stage.
//
// Entries decode strictly: a field E doesn't declare is an error, so older
// connector images can't read entries with new fields.
//
// It isn't safe for concurrent use.
type ShardedState[E any] struct {
	rangeKey     string
	primary      bool
	live         []string
	pending      ShardedEntries[E]
	ownRecovered bool
}

// NewShardedState returns the state of the shard covering [keyBegin,
// keyEnd], whose bindings have liveStateKeys. Pending entries of other state
// keys are never applied, and remain in the state.
func NewShardedState[E any](keyBegin, keyEnd uint32, liveStateKeys []string) *ShardedState[E] {
	return &ShardedState[E]{
		rangeKey: RangeKey(keyBegin, keyEnd),
		primary:  keyBegin == 0,
		live:     liveStateKeys,
		pending:  make(ShardedEntries[E]),
	}
}

func (s *ShardedState[E]) Primary() bool    { return s.primary }
func (s *ShardedState[E]) RangeKey() string { return s.rangeKey }

// Pending is every pending entry. It must not be modified.
func (s *ShardedState[E]) Pending() ShardedEntries[E] { return s.pending }

// OwnRecovered reports whether the last recovered state held entries staged
// under this shard's range.
func (s *ShardedState[E]) OwnRecovered() bool { return s.ownRecovered }

// Recover replaces the pending work with that of a recovered state document.
// Every shard validates it; only the primary keeps it.
func (s *ShardedState[E]) Recover(doc json.RawMessage) error {
	entries, err := parseShardedEntries[E](doc)
	if err != nil {
		return fmt.Errorf("parsing connector state: %w", err)
	}
	s.ownRecovered = false
	for _, bucket := range entries {
		if _, ok := bucket[s.rangeKey]; ok {
			s.ownRecovered = true
		}
	}

	s.pending = make(ShardedEntries[E])
	if s.primary {
		maps.Copy(s.pending, entries)
	}
	return nil
}

// StartCommit returns the merge patch recording this shard's entries of the
// committing transaction, by state key. The primary also records them as
// pending here: runtime v1 never echoes them in Acknowledge's statePatches.
func (s *ShardedState[E]) StartCommit(staged map[string]E) (*pf.ConnectorState, error) {
	var update = make(ShardedEntries[E])
	for sk, e := range staged {
		update.add(sk, s.rangeKey, e)
		if s.primary {
			s.pending.add(sk, s.rangeKey, e)
		}
	}
	return mergePatch(update)
}

// Acknowledge settles a committed transaction. The primary folds its peers'
// entries out of statePatches, calls apply with the pending entries of live
// state keys that stateKeys selects (m.StateKeyFilter), and once apply
// succeeds returns the patch clearing them. The entries are still pending
// while apply runs. Other shards return nil, as does the primary when there
// is nothing to apply.
//
// Clearing an entry leaves its state key's object in the state, `{}` once
// empty.
func (s *ShardedState[E]) Acknowledge(statePatches []json.RawMessage, stateKeys []string, apply func(ShardedEntries[E]) error) (*pf.ConnectorState, error) {
	if !s.primary {
		return nil, nil
	}
	for _, patch := range statePatches {
		if err := s.absorbPeer(patch); err != nil {
			return nil, err
		}
	}

	var shouldProcess = m.StateKeyFilter(stateKeys)
	var batch = make(ShardedEntries[E])
	var clear = make(ShardedEntries[any])
	for _, sk := range s.live {
		if !shouldProcess(sk) {
			continue
		}
		for rk, e := range s.pending[sk] {
			batch.add(sk, rk, e)
			clear.add(sk, rk, nil)
		}
	}

	if len(batch) == 0 {
		return nil, nil
	} else if err := apply(batch); err != nil {
		return nil, err
	}
	for sk := range batch {
		delete(s.pending, sk)
	}
	return mergePatch(clear)
}

// absorbPeer folds one shard's StartedCommit patch into the pending entries,
// skipping this shard's own, which the runtime echoes back.
func (s *ShardedState[E]) absorbPeer(patch json.RawMessage) error {
	if bytes.Equal(bytes.TrimSpace(patch), []byte("null")) {
		// The runtime's encoding of a full state replacement, which resets
		// every shard's entries. No shard here produces one.
		return fmt.Errorf("unexpected state reset patch in aggregated shard state")
	}
	entries, err := parseShardedEntries[E](patch)
	if err != nil {
		return fmt.Errorf("parsing aggregated state patch: %w", err)
	}
	for sk, bucket := range entries {
		for rk, e := range bucket {
			if rk == s.rangeKey {
				continue
			}
			// The previous transaction's entry is applied before the next
			// one's patch arrives, so a collision would lose staged work.
			if _, ok := s.pending[sk][rk]; ok {
				return fmt.Errorf("shard %s staged a transaction for %s while its previous one is still pending", rk, sk)
			}
			s.pending.add(sk, rk, e)
		}
	}
	return nil
}

func parseShardedEntries[E any](doc json.RawMessage) (ShardedEntries[E], error) {
	var entries ShardedEntries[E]
	if len(bytes.TrimSpace(doc)) == 0 {
		return nil, nil
	}
	var dec = json.NewDecoder(bytes.NewReader(doc))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&entries); err != nil {
		return nil, err
	}
	return entries, nil
}

func mergePatch(v any) (*pf.ConnectorState, error) {
	patch, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshalling state patch: %w", err)
	}
	return &pf.ConnectorState{UpdatedJson: patch, MergePatch: true}, nil
}
