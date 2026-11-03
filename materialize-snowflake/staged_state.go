package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"time"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
)

// stagedCheckpoint holds staged entries by binding state key, then by the
// range key ("%08x-%08x" of key_begin-key_end) of the shard that staged them.
// Range keys are disjoint across shards, so the shards' merge patches of their
// own entries commute.
type stagedCheckpoint map[string]map[string]*checkpointItem

func (s stagedCheckpoint) add(stateKey, rangeKey string, item *checkpointItem) {
	if s[stateKey] == nil {
		s[stateKey] = make(map[string]*checkpointItem)
	}
	s[stateKey][rangeKey] = item
}

// mergeBoundLiterals is the serialized form of a key column's sql.MergeBound.
// Empty literals mean the column carries no bound.
type mergeBoundLiterals struct {
	Lower string `json:",omitempty"`
	Upper string `json:",omitempty"`
}

func boundsLiterals(bounds []sql.MergeBound) []mergeBoundLiterals {
	var out = make([]mergeBoundLiterals, len(bounds))
	for i, b := range bounds {
		out[i] = mergeBoundLiterals{Lower: b.LiteralLower, Upper: b.LiteralUpper}
	}
	return out
}

var rangeKeyRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{8}$`)

func unmarshalStrict(data json.RawMessage, v any) error {
	var dec = json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func parseCheckpointItem(data json.RawMessage) (*checkpointItem, error) {
	var item checkpointItem
	if err := unmarshalStrict(data, &item); err != nil {
		return nil, err
	}
	return &item, nil
}

// flatItemClear nulls every field of a flat checkpoint item. It clears a flat
// item while keeping the range-keyed entries that share its state key.
var flatItemClear = func() map[string]any {
	var out = make(map[string]any)
	var t = reflect.TypeFor[checkpointItem]()
	for i := range t.NumField() {
		out[t.Field(i).Name] = nil
	}
	return out
}()

// legacyItemClear nulls every field of a flat checkpoint item except its
// streaming v2 channel state. It clears a legacy entry while keeping the
// range-keyed entries and channel state that share its state key.
// TODO(#5176): remove in about January 2027, after checkpoints have all migrated
var legacyItemClear = func() map[string]any {
	var out = maps.Clone(flatItemClear)
	delete(out, "StreamV2")
	return out
}()

func isMergeQuery(query string) bool {
	return strings.HasPrefix(query, "\nMERGE INTO")
}

func (item *checkpointItem) hasWork() bool {
	return len(item.Query) > 0 || len(item.StreamBlobs) > 0 || len(item.PipeFiles) > 0 || len(item.StreamV2) > 0
}

// absorbState folds a state document into the transactor. Range-keyed entries
// go to d.staged when this shard is the primary, which applies every shard's
// staged work. A flat item's staged fields are a legacy entry, likewise kept
// only by the primary, and its remaining fields stay in d.cp. With
// peerPatch set, the document is an aggregated patch of every shard's
// StartedCommit state, so its flat fields and this shard's own entries are
// already held. A field that is neither a range key nor a field of
// checkpointItem is an error, so that a layout this build does not know fails
// instead of being dropped.
func (d *transactor) absorbState(doc json.RawMessage, peerPatch bool) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(doc, &raw); err != nil {
		return err
	}
	if d.cp == nil {
		d.cp = make(checkpoint)
	}
	if d.staged == nil {
		d.staged = make(stagedCheckpoint)
	}
	if d.legacyStaged == nil {
		d.legacyStaged = make(checkpoint)
	}

	for stateKey, val := range raw {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(val, &fields); err != nil {
			return fmt.Errorf("parsing checkpoint entry %q: %w", stateKey, err)
		}

		var flatFields = make(map[string]json.RawMessage)
		for field, itemRaw := range fields {
			if !rangeKeyRe.MatchString(field) {
				flatFields[field] = itemRaw
				continue
			} else if bytes.Equal(bytes.TrimSpace(itemRaw), []byte("null")) {
				continue
			}
			var item, err = parseCheckpointItem(itemRaw)
			if err != nil {
				return fmt.Errorf("parsing checkpoint entry %q of %q: %w", field, stateKey, err)
			} else if d.primary && !(peerPatch && field == d.rangeKey) {
				d.staged.add(stateKey, field, item)
			}
		}

		var flatRaw, err = json.Marshal(flatFields)
		if err != nil {
			return fmt.Errorf("marshalling checkpoint entry %q: %w", stateKey, err)
		}
		var flat checkpointItem
		if err := unmarshalStrict(flatRaw, &flat); err != nil {
			return fmt.Errorf("parsing checkpoint entry %q: %w", stateKey, err)
		} else if peerPatch {
			continue
		}

		// TODO(#5176): remove in about January 2027, after checkpoints have all migrated
		if len(flat.Query) > 0 || flat.TruncateBefore != nil {
			if d.primary {
				var legacy = flat
				legacy.StreamV2 = nil
				d.legacyStaged[stateKey] = &legacy
			}
			flat = checkpointItem{Table: flat.Table, StreamV2: flat.StreamV2}
		}
		if flat.hasWork() && !(flat.streamV2Only() && slices.Contains(d.sweptStreamV2, stateKey)) {
			d.cp[stateKey] = &flat
		}
	}

	return nil
}

// mergePeerStatePatches folds the aggregated StartedCommit state patches of
// every shard into the primary's staged entries.
func (d *transactor) mergePeerStatePatches(patches []json.RawMessage) error {
	if !d.primary {
		return nil
	}
	for _, patch := range patches {
		if bytes.Equal(bytes.TrimSpace(patch), []byte("null")) {
			return fmt.Errorf("unexpected state reset patch in aggregated shard state")
		} else if err := d.absorbState(patch, true); err != nil {
			return fmt.Errorf("parsing aggregated state patch: %w", err)
		}
	}
	return nil
}

// forgetStreamV2 drops a state key's streaming v2 channel state, whose
// channels are gone, so that the next started commit clears it.
func (d *transactor) forgetStreamV2(stateKey string) {
	if item := d.cp[stateKey]; item != nil && item.streamV2Only() {
		delete(d.cp, stateKey)
		d.sweptStreamV2 = append(d.sweptStreamV2, stateKey)
	}
}

// startCommitState is this shard's StartedCommit merge patch: its flat items,
// and its own range-keyed entries nested within their state keys. Each state
// key swept of streaming v2 channels has its flat item nulled. A flat item
// alone at its state key is emitted as it marshals.
func (d *transactor) startCommitState() (json.RawMessage, error) {
	var nested = make(map[string]map[string]any)
	var at = func(stateKey string) map[string]any {
		if nested[stateKey] == nil {
			nested[stateKey] = make(map[string]any)
		}
		return nested[stateKey]
	}

	for _, stateKey := range d.sweptStreamV2 {
		maps.Copy(at(stateKey), flatItemClear)
	}
	d.sweptStreamV2 = nil

	for stateKey, entries := range d.staged {
		if item := entries[d.rangeKey]; item != nil {
			at(stateKey)[d.rangeKey] = item
		}
	}

	var out = make(map[string]json.RawMessage)
	for stateKey, item := range d.cp {
		var raw, err = json.Marshal(item)
		if err != nil {
			return nil, fmt.Errorf("marshalling checkpoint entry %q: %w", stateKey, err)
		}
		if nested[stateKey] == nil {
			out[stateKey] = raw
			continue
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return nil, fmt.Errorf("unmarshalling checkpoint entry %q: %w", stateKey, err)
		}
		for field, value := range fields {
			nested[stateKey][field] = value
		}
	}
	for stateKey, fields := range nested {
		var raw, err = json.Marshal(fields)
		if err != nil {
			return nil, fmt.Errorf("marshalling checkpoint entry %q: %w", stateKey, err)
		}
		out[stateKey] = raw
	}

	var checkpointJSON, err = json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("creating checkpoint json: %w", err)
	}
	return checkpointJSON, nil
}

// commitStaged applies a binding's staged entries and deletes their staged
// files. legacy is the binding's entry without a range key, or nil, and runs
// first. Entries of MERGE INTO that the binding's current spec staged
// coalesce into one MERGE INTO over all of their files, because each MERGE
// INTO scans the target table. Every other entry runs its own pre-rendered
// query, since only that query matches the columns its files were staged with.
//
// A recovered entry may already have been applied before its clearing was
// committed. Re-applying it is safe, because its MERGE INTO converges on the
// same rows, and COPY INTO skips files it has already loaded.
func (d *transactor) commitStaged(ctx context.Context, b *binding, legacy *checkpointItem, entries map[string]*checkpointItem) error {
	var path = b.target.Path

	var items, coalesce []*checkpointItem
	if legacy != nil {
		items = append(items, legacy)
	}
	for _, rk := range slices.Sorted(maps.Keys(entries)) {
		var item = entries[rk]
		items = append(items, item)
		if isMergeQuery(item.Query) && item.Version == d.version && len(item.Bounds) == len(b.target.Keys) {
			coalesce = append(coalesce, item)
		}
	}
	if len(coalesce) < 2 {
		coalesce = nil
	}

	d.be.StartedResourceCommit(path)
	for _, item := range items {
		if len(item.Query) == 0 || slices.Contains(coalesce, item) {
			continue
		}
		d.commitProfile.SetBounds(b.target.Binding, profileBounds(b.target.Keys, item))
		if err := d.runStoreQuery(ctx, b, item.Query, item.Round, item.Rows); err != nil {
			return err
		}
	}
	if len(coalesce) > 0 {
		var dirs []string
		var rows int64
		for _, item := range coalesce {
			dirs = append(dirs, item.StagedDir)
			rows += item.Rows
		}
		var bounds = combineBounds(b.target.Keys, coalesce)
		query, err := renderMergeQueryTemplate(d.templates.mergeInto, b.target, dirs, bounds)
		if err != nil {
			return fmt.Errorf("mergeInto template: %w", err)
		}
		d.commitProfile.SetBounds(b.target.Binding, sql.MergeBounds(bounds))
		if err := d.runStoreQuery(ctx, b, query, coalesce[0].Round, rows); err != nil {
			return err
		}
	}

	// The truncation runs once, after every entry's stores, so that it keeps
	// the rows they just wrote, which were published after the boundary.
	var truncateBefore *time.Time
	var truncateRound int
	for _, item := range items {
		if tb := item.TruncateBefore; tb != nil && (truncateBefore == nil || tb.After(*truncateBefore)) {
			truncateBefore, truncateRound = tb, item.Round
		}
	}
	if truncateBefore != nil {
		if err := d.runTruncation(ctx, b, *truncateBefore, truncateRound); err != nil {
			return err
		}
	}
	d.be.FinishedResourceCommit(path)

	var dirs []string
	for _, item := range items {
		if dir := item.StagedDir; dir != "" {
			dirs = append(dirs, dir)
		}
	}
	if err := d.deleteFiles(ctx, dirs); err != nil {
		return fmt.Errorf("cleaning up files: %w", err)
	}
	return nil
}

// combineBounds bounds each key column by the extremes of the items' bounds.
// A column is unbounded if any item leaves it unbounded.
func combineBounds(keys []sql.Column, items []*checkpointItem) []sql.MergeBound {
	var out = make([]sql.MergeBound, len(keys))
	for i, key := range keys {
		out[i] = sql.MergeBound{Column: key}

		var lowers, uppers []string
		var bounded = true
		for _, item := range items {
			var lit = item.Bounds[i]
			if lit.Lower == "" || lit.Upper == "" {
				bounded = false
				break
			}
			if !slices.Contains(lowers, lit.Lower) {
				lowers = append(lowers, lit.Lower)
			}
			if !slices.Contains(uppers, lit.Upper) {
				uppers = append(uppers, lit.Upper)
			}
		}
		if bounded {
			out[i].LiteralLower = extremumExpr("LEAST", lowers, key)
			out[i].LiteralUpper = extremumExpr("GREATEST", uppers, key)
		}
	}
	return out
}

// profileBounds are the bounds an entry's own query is restricted to, or nil
// for an entry that doesn't record them.
func profileBounds(keys []sql.Column, item *checkpointItem) boilerplate.ProfileBounds {
	if len(item.Bounds) != len(keys) {
		return nil
	}
	return sql.MergeBounds(combineBounds(keys, []*checkpointItem{item}))
}

// extremumExpr selects the least or greatest of literals. Each literal is cast
// to the column's type, so that they compare as the column's values do.
func extremumExpr(fn string, literals []string, key sql.Column) string {
	if len(literals) == 1 {
		return literals[0]
	}
	var cast = make([]string, len(literals))
	for i, l := range literals {
		cast[i] = fmt.Sprintf("%s::%s", l, key.BareDDL)
	}
	return fmt.Sprintf("%s(%s)", fn, strings.Join(cast, ", "))
}
