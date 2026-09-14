package materialize

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/gogo/protobuf/types"
	log "github.com/sirupsen/logrus"
)

// truncationsStateKey is the reserved connector-state key under which
// shards coordinate the backfill-truncation election.
const truncationsStateKey = "__truncations"

// truncations tracks the shard-zero election that runs a Transactor's
// Truncate once every key range covering a binding's state key reports the
// same backfill boundary, coordinated through the "__truncations"
// connector-state subtree (range key -> binding state key -> RFC3339Nano
// boundary).
type truncations struct {
	ownRange string
	primary  bool
	bindings map[string]int
	tree     map[string]any
}

// newTruncations builds a truncations tracker for a transactions session,
// seeded from the connector state persisted at Open.
func newTruncations(open *pm.Request_Open) (*truncations, error) {
	var t = &truncations{
		ownRange: fmt.Sprintf("%08x-%08x", open.Range.KeyBegin, open.Range.KeyEnd),
		primary:  open.Range.KeyBegin == 0,
		bindings: make(map[string]int, len(open.Materialization.Bindings)),
	}
	for i, b := range open.Materialization.Bindings {
		t.bindings[b.StateKey] = i
	}

	if len(open.StateJson) == 0 {
		return t, nil
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(open.StateJson, &top); err != nil {
		return nil, fmt.Errorf("unmarshalling state: %w", err)
	}
	if raw, ok := top[truncationsStateKey]; ok {
		if err := json.Unmarshal(raw, &t.tree); err != nil {
			return nil, fmt.Errorf("unmarshalling %s: %w", truncationsStateKey, err)
		}
	}
	return t, nil
}

// recordCompletes sets this shard's boundary for every binding whose
// backfill completed in flush.
func (t *truncations) recordCompletes(spec *pf.MaterializationSpec, flush *pm.Request_Flush) error {
	var boundaries = make(map[string]any, len(flush.BackfillCompletes))
	for _, c := range flush.BackfillCompletes {
		if int(c.Binding) >= len(spec.Bindings) {
			continue
		}
		ts, err := types.TimestampFromProto(c.Timestamp)
		if err != nil {
			return fmt.Errorf("backfill complete timestamp: %w", err)
		}
		boundaries[spec.Bindings[c.Binding].StateKey] = ts.Format(time.RFC3339Nano)
	}
	t.mergeTree(map[string]any{t.ownRange: boundaries})
	return nil
}

// absorb applies each patch's "__truncations" value into the in-memory
// tree as an RFC 7396 merge patch.
func (t *truncations) absorb(patches []json.RawMessage) error {
	for _, patch := range patches {
		var top map[string]json.RawMessage
		if err := json.Unmarshal(patch, &top); err != nil {
			return fmt.Errorf("unmarshalling state patch: %w", err)
		}
		raw, ok := top[truncationsStateKey]
		if !ok {
			continue
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return fmt.Errorf("unmarshalling %s patch: %w", truncationsStateKey, err)
		}
		t.mergeTree(value)
	}
	return nil
}

// mergeTree applies patch to the in-memory tree as an RFC 7396 merge patch.
func (t *truncations) mergeTree(patch any) {
	if merged, ok := ApplyMergePatch(t.tree, patch).(map[string]any); ok {
		t.tree = merged
	} else {
		t.tree = nil
	}
}

// emit folds this shard's own current boundaries into state, following the
// same merge-patch or full-replacement shape state already carries.
func (t *truncations) emit(state *pf.ConnectorState) (*pf.ConnectorState, error) {
	return t.mergeInto(state, map[string]any{t.ownRange: t.tree[t.ownRange]})
}

// evaluate runs the shard-zero truncation election: for every binding state
// key whose reported ranges fully cover the key space at a single boundary,
// it calls transactor.Truncate and clears those ranges' entries. A state
// key matching no live binding is cleared without truncating. It is a
// no-op for a non-primary shard (KeyBegin != 0), and folds any clearing
// patch into state following the same merge-patch or full-replacement
// shape state already carries.
func (t *truncations) evaluate(ctx context.Context, transactor Transactor, state *pf.ConnectorState) (*pf.ConnectorState, error) {
	if !t.primary {
		return state, nil
	}

	var stateKeys = make(map[string]struct{})
	for _, entries := range t.tree {
		if obj, ok := entries.(map[string]any); ok {
			for sk := range obj {
				stateKeys[sk] = struct{}{}
			}
		}
	}

	var clear = make(map[string]any)
	for sk := range stateKeys {
		var entries = t.entriesFor(sk)

		binding, live := t.bindings[sk]
		if !live {
			for _, e := range entries {
				clearEntry(clear, e.rangeKey, sk)
			}
			continue
		}

		boundary, ok := fullCoverage(entries)
		if !ok {
			continue
		}
		before, err := time.Parse(time.RFC3339Nano, boundary)
		if err != nil {
			return nil, fmt.Errorf("parsing truncation boundary %q: %w", boundary, err)
		}
		deleted, err := transactor.Truncate(ctx, binding, before)
		if err != nil {
			return nil, fmt.Errorf("transactor.Truncate: %w", err)
		}
		log.WithFields(log.Fields{
			"stateKey": sk,
			"binding":  binding,
			"boundary": before,
			"deleted":  deleted,
		}).Info("truncated backfill")

		for _, e := range entries {
			clearEntry(clear, e.rangeKey, sk)
		}
	}

	if len(clear) == 0 {
		return state, nil
	}
	t.mergeTree(clear)
	return t.mergeInto(state, clear)
}

// mergeInto folds patch into state's "__truncations" key: a nil state
// starts a merge-patch update; an existing merge-patch update gets the key
// set to patch; a full-replacement update gets the key set to the entire
// tree so a narrower patch cannot lose sibling ranges' entries.
func (t *truncations) mergeInto(state *pf.ConnectorState, patch any) (*pf.ConnectorState, error) {
	if state != nil && !state.MergePatch {
		patch = t.tree
	}
	if state == nil {
		raw, err := json.Marshal(map[string]any{truncationsStateKey: patch})
		if err != nil {
			return nil, fmt.Errorf("marshalling %s update: %w", truncationsStateKey, err)
		}
		return &pf.ConnectorState{UpdatedJson: raw, MergePatch: true}, nil
	}

	var top map[string]any
	if err := json.Unmarshal(state.UpdatedJson, &top); err != nil {
		return nil, fmt.Errorf("connector state update must be a JSON object: %w", err)
	}
	top[truncationsStateKey] = patch
	raw, err := json.Marshal(top)
	if err != nil {
		return nil, fmt.Errorf("marshalling %s update: %w", truncationsStateKey, err)
	}
	return &pf.ConnectorState{UpdatedJson: raw, MergePatch: state.MergePatch}, nil
}

// truncationEntry is one range's reported boundary for a binding state key.
type truncationEntry struct {
	rangeKey   string
	begin, end uint32
	boundary   string
}

// entriesFor collects every range's reported boundary for stateKey.
func (t *truncations) entriesFor(stateKey string) []truncationEntry {
	var out []truncationEntry
	for key, v := range t.tree {
		obj, ok := v.(map[string]any)
		if !ok {
			continue
		}
		boundary, ok := obj[stateKey].(string)
		if !ok {
			continue
		}
		begin, end, err := parseRangeKey(key)
		if err != nil {
			continue
		}
		out = append(out, truncationEntry{rangeKey: key, begin: begin, end: end, boundary: boundary})
	}
	return out
}

// fullCoverage reports the agreed boundary if entries, sorted by range
// begin, tile the full key space [0, 0xffffffff] at a single boundary.
func fullCoverage(entries []truncationEntry) (string, bool) {
	if len(entries) == 0 {
		return "", false
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].begin < entries[j].begin })

	if entries[0].begin != 0 {
		return "", false
	}
	var boundary = entries[0].boundary
	for i, e := range entries {
		if e.boundary != boundary {
			return "", false
		}
		if i > 0 && e.begin != entries[i-1].end+1 {
			return "", false
		}
	}
	if entries[len(entries)-1].end != math.MaxUint32 {
		return "", false
	}
	return boundary, true
}

// parseRangeKey parses a "%08x-%08x" shard key range.
func parseRangeKey(key string) (uint32, uint32, error) {
	beginStr, endStr, ok := strings.Cut(key, "-")
	if !ok {
		return 0, 0, fmt.Errorf("invalid truncation range key %q", key)
	}
	begin, err := strconv.ParseUint(beginStr, 16, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid truncation range key %q: %w", key, err)
	}
	end, err := strconv.ParseUint(endStr, 16, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid truncation range key %q: %w", key, err)
	}
	return uint32(begin), uint32(end), nil
}

// clearEntry sets a null entry for stateKey under rangeKey in clear.
func clearEntry(clear map[string]any, rangeKey, stateKey string) {
	obj, ok := clear[rangeKey].(map[string]any)
	if !ok {
		obj = make(map[string]any, 1)
		clear[rangeKey] = obj
	}
	obj[stateKey] = nil
}
