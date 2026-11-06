package boilerplate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"

	m "github.com/estuary/connectors/go/materialize"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// RangeKey names a shard's key range in ShardedState.
func RangeKey(keyBegin, keyEnd uint32) string {
	return fmt.Sprintf("%08x-%08x", keyBegin, keyEnd)
}

var rangeKeyRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{8}$`)

// ShardedEntries are entries by state key, then by the range key of the
// shard that staged them. A flat pre-range entry has range key "".
type ShardedEntries[E any] map[string]map[string]E

func (s ShardedEntries[E]) add(stateKey, rangeKey string, e E) {
	if s[stateKey] == nil {
		s[stateKey] = make(map[string]E)
	}
	s[stateKey][rangeKey] = e
}

func (s ShardedEntries[E]) has(stateKey, rangeKey string) bool {
	_, ok := s[stateKey][rangeKey]
	return ok
}

// Len is the number of entries across every state key.
func (s ShardedEntries[E]) Len() int {
	var n int
	for _, bucket := range s {
		n += len(bucket)
	}
	return n
}

type ShardedStateOptions[E any] struct {
	// Decode parses an entry; json.Unmarshal when nil.
	Decode func(json.RawMessage) (E, error)
	// DecodeLegacy parses an entry of a layout older than
	// {stateKey: {rangeKey: entry}}: range-key outer {rangeKey: {stateKey:
	// entry}}, or flat {stateKey: entry}, whose fields may share the state
	// key's object with current entries. Those layouts are errors when nil.
	DecodeLegacy func(json.RawMessage) (E, error)
}

// DecodeStrict decodes an entry, rejecting fields E doesn't declare. Older
// connector images that decode strictly can't read entries with new fields.
func DecodeStrict[E any](raw json.RawMessage) (E, error) {
	var e E
	var dec = json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	err := dec.Decode(&e)
	return e, err
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
// It isn't safe for concurrent use.
type ShardedState[E any] struct {
	rangeKey     string
	primary      bool
	live         map[string]bool
	opts         ShardedStateOptions[E]
	pending      ShardedEntries[E]
	legacy       map[slot]legacyOrigin
	empty        map[string]bool // keys recovered with an empty object
	ownRecovered bool
}

type slot struct{ stateKey, rangeKey string }

// legacyOrigin locates a legacy entry for its clearing patch: under
// {rangeKey: {stateKey}} when rangeOuter, else as the fields of a flat
// {stateKey} item.
type legacyOrigin struct {
	rangeOuter bool
	fields     []string
}

// NewShardedState returns the state of the shard covering [keyBegin,
// keyEnd], whose bindings have liveStateKeys. Pending entries of other state
// keys are never applied, and remain in the state.
func NewShardedState[E any](keyBegin, keyEnd uint32, liveStateKeys []string, opts ShardedStateOptions[E]) *ShardedState[E] {
	if opts.Decode == nil {
		opts.Decode = func(raw json.RawMessage) (E, error) {
			var e E
			err := json.Unmarshal(raw, &e)
			return e, err
		}
	}
	var live = make(map[string]bool, len(liveStateKeys))
	for _, sk := range liveStateKeys {
		live[sk] = true
	}
	return &ShardedState[E]{
		rangeKey: RangeKey(keyBegin, keyEnd),
		primary:  keyBegin == 0,
		live:     live,
		opts:     opts,
		pending:  make(ShardedEntries[E]),
		legacy:   make(map[slot]legacyOrigin),
		empty:    make(map[string]bool),
	}
}

func (s *ShardedState[E]) Primary() bool    { return s.primary }
func (s *ShardedState[E]) RangeKey() string { return s.rangeKey }

// Len is the number of pending entries.
func (s *ShardedState[E]) Len() int { return s.pending.Len() }

// Pending is every pending entry. It must not be modified.
func (s *ShardedState[E]) Pending() ShardedEntries[E] { return s.pending }

// OwnRecovered reports whether the last recovered state held entries staged
// under this shard's range.
func (s *ShardedState[E]) OwnRecovered() bool { return s.ownRecovered }

// Recover replaces the pending work with that of a recovered state document.
// Every shard validates it; only the primary keeps it.
func (s *ShardedState[E]) Recover(doc json.RawMessage) error {
	p, err := s.parse(doc, false)
	if err != nil {
		return fmt.Errorf("parsing connector state: %w", err)
	}
	s.ownRecovered = false
	for _, bucket := range p.entries {
		if _, ok := bucket[s.rangeKey]; ok {
			s.ownRecovered = true
		}
	}

	s.pending = make(ShardedEntries[E])
	s.legacy = make(map[slot]legacyOrigin)
	s.empty = make(map[string]bool)
	if s.primary {
		s.pending, s.legacy, s.empty = p.entries, p.legacy, p.empty
	}
	return nil
}

// StartCommit returns the merge patch recording this shard's entries of the
// committing transaction, by state key. The primary also records them as
// pending here: runtime v1 never echoes them in Acknowledge's statePatches.
func (s *ShardedState[E]) StartCommit(staged map[string]E) (*pf.ConnectorState, error) {
	var update = make(ShardedEntries[E])
	for sk, e := range staged {
		if s.primary && s.pending.has(sk, s.rangeKey) {
			return nil, fmt.Errorf("staged a transaction for %s while this shard's previous one is still pending", sk)
		}
		update.add(sk, s.rangeKey, e)
	}
	if s.primary {
		for sk, e := range staged {
			s.pending.add(sk, s.rangeKey, e)
		}
	}

	patch, err := json.Marshal(update)
	if err != nil {
		return nil, fmt.Errorf("marshalling staged entries: %w", err)
	}
	return &pf.ConnectorState{UpdatedJson: patch, MergePatch: true}, nil
}

// Acknowledge settles a committed transaction. The primary folds its peers'
// entries out of statePatches, calls apply with the pending entries of live
// state keys that stateKeys selects (m.StateKeyFilter), and once apply
// succeeds returns the patch clearing them. Other shards return nil. It
// returns nil when nothing was applied.
//
// Clearing an entry leaves its state key's object in place, `{}` once
// empty. A session Acknowledge (nil stateKeys) removes recovered empty
// objects under keys that aren't live state keys, which no shard stages
// under.
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
	for sk, bucket := range s.pending {
		if !s.live[sk] || !shouldProcess(sk) {
			continue
		}
		for rk, e := range bucket {
			batch.add(sk, rk, e)
		}
	}

	var prune []string
	if stateKeys == nil {
		for key := range s.empty {
			if !s.live[key] && len(s.pending[key]) == 0 {
				prune = append(prune, key)
			}
		}
	}

	if batch.Len() == 0 && len(prune) == 0 {
		return nil, nil
	} else if batch.Len() != 0 {
		if err := apply(batch); err != nil {
			return nil, err
		}
	}

	patch, err := json.Marshal(s.settle(batch, prune))
	if err != nil {
		return nil, fmt.Errorf("marshalling state clearing patch: %w", err)
	}
	return &pf.ConnectorState{UpdatedJson: patch, MergePatch: true}, nil
}

// absorbPeer folds one shard's StartedCommit patch into the pending entries,
// skipping this shard's own, which the runtime echoes back.
func (s *ShardedState[E]) absorbPeer(patch json.RawMessage) error {
	if isJSONNull(patch) {
		// The runtime's encoding of a full state replacement, which resets
		// every shard's entries. No shard here produces one.
		return fmt.Errorf("unexpected state reset patch in aggregated shard state")
	}
	p, err := s.parse(patch, true)
	if err != nil {
		return fmt.Errorf("parsing aggregated state patch: %w", err)
	}
	for sk, bucket := range p.entries {
		for rk, e := range bucket {
			// The previous transaction's entry is applied before the next
			// one's patch arrives, so a collision would lose staged work.
			if s.pending.has(sk, rk) {
				return fmt.Errorf("shard %s staged a transaction for %s while its previous one is still pending", rk, sk)
			}
			s.pending.add(sk, rk, e)
		}
	}
	for at, origin := range p.legacy {
		s.legacy[at] = origin
	}
	return nil
}

// settle drops the batch from the pending entries and returns the merge
// patch clearing it and the pruned keys.
func (s *ShardedState[E]) settle(batch ShardedEntries[E], prune []string) map[string]any {
	var patch = make(map[string]any)
	var nest = func(outer, inner string) {
		obj, _ := patch[outer].(map[string]any)
		if obj == nil {
			obj = make(map[string]any)
			patch[outer] = obj
		}
		obj[inner] = nil
	}

	var rangeOuter = make(map[string][]string) // settled legacy state keys by range key
	for sk, bucket := range batch {
		for rk := range bucket {
			delete(s.pending[sk], rk)
			var at = slot{sk, rk}
			origin, isLegacy := s.legacy[at]
			delete(s.legacy, at)

			if !isLegacy {
				nest(sk, rk)
			} else if origin.rangeOuter {
				rangeOuter[rk] = append(rangeOuter[rk], sk)
			} else {
				// A flat item's state key may hold current entries too.
				for _, field := range origin.fields {
					nest(sk, field)
				}
			}
		}
		if len(s.pending[sk]) == 0 {
			delete(s.pending, sk)
		}
	}

	// Nothing writes the range-key-outer layout anymore, so a bucket whose
	// every entry is settled goes entirely.
	for rk, settled := range rangeOuter {
		var remaining bool
		for at, origin := range s.legacy {
			remaining = remaining || (origin.rangeOuter && at.rangeKey == rk)
		}
		if !remaining {
			patch[rk] = nil
			continue
		}
		for _, sk := range settled {
			nest(rk, sk)
		}
	}

	for _, key := range prune {
		patch[key] = nil
		delete(s.empty, key)
	}
	return patch
}

type parsedState[E any] struct {
	entries ShardedEntries[E]
	legacy  map[slot]legacyOrigin
	empty   map[string]bool
}

// parse decodes a state document or a patch of one. skipOwn drops entries
// staged under this shard's range. Null members are absent, per RFC 7396.
func (s *ShardedState[E]) parse(doc json.RawMessage, skipOwn bool) (*parsedState[E], error) {
	var p = &parsedState[E]{
		entries: make(ShardedEntries[E]),
		legacy:  make(map[slot]legacyOrigin),
		empty:   make(map[string]bool),
	}
	if len(bytes.TrimSpace(doc)) == 0 {
		return p, nil
	}

	var top map[string]json.RawMessage
	if err := json.Unmarshal(doc, &top); err != nil {
		return nil, err
	}
	for key, val := range top {
		if isJSONNull(val) {
			continue
		}
		var members map[string]json.RawMessage
		if err := json.Unmarshal(val, &members); err != nil {
			return nil, fmt.Errorf("parsing %q: %w", key, err)
		} else if len(members) == 0 {
			p.empty[key] = true
			continue
		}

		if rangeKeyRe.MatchString(key) {
			if err := s.parseRangeOuter(p, key, members, skipOwn); err != nil {
				return nil, err
			}
			continue
		}

		var flat = make(map[string]json.RawMessage)
		for rk, raw := range members {
			if isJSONNull(raw) || (skipOwn && rk == s.rangeKey) {
				continue
			} else if !rangeKeyRe.MatchString(rk) {
				flat[rk] = raw
				continue
			}
			e, err := s.opts.Decode(raw)
			if err != nil {
				return nil, fmt.Errorf("parsing entry %q of %q: %w", rk, key, err)
			} else if p.entries.has(key, rk) {
				return nil, fmt.Errorf("entry %q of %q is also in the range-key-outer layout", rk, key)
			}
			p.entries.add(key, rk, e)
		}
		if len(flat) == 0 {
			continue
		} else if s.opts.DecodeLegacy == nil {
			return nil, fmt.Errorf("entry of %q is not nested under a range key", key)
		}
		var raw, _ = json.Marshal(flat)
		e, err := s.opts.DecodeLegacy(raw)
		if err != nil {
			return nil, fmt.Errorf("parsing flat entry %q: %w", key, err)
		}
		var fields = make([]string, 0, len(flat))
		for field := range flat {
			fields = append(fields, field)
		}
		slices.Sort(fields)
		p.entries.add(key, "", e)
		p.legacy[slot{key, ""}] = legacyOrigin{fields: fields}
	}
	return p, nil
}

func (s *ShardedState[E]) parseRangeOuter(p *parsedState[E], rk string, members map[string]json.RawMessage, skipOwn bool) error {
	if s.opts.DecodeLegacy == nil {
		return fmt.Errorf("%q holds entries keyed by range then state key", rk)
	}
	for sk, raw := range members {
		if isJSONNull(raw) || (skipOwn && rk == s.rangeKey) {
			continue
		}
		e, err := s.opts.DecodeLegacy(raw)
		if err != nil {
			return fmt.Errorf("parsing entry %q of %q: %w", sk, rk, err)
		} else if p.entries.has(sk, rk) {
			return fmt.Errorf("entry %q of %q is also in the range-key-outer layout", rk, sk)
		}
		p.entries.add(sk, rk, e)
		p.legacy[slot{sk, rk}] = legacyOrigin{rangeOuter: true}
	}
	return nil
}

func isJSONNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}
