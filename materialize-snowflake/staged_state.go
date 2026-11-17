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

// legacyRangeKey keys a staged entry persisted as the flat fields of its state
// key, without a range key. It sorts before every range key.
// TODO(#5176): remove in about January 2027, after checkpoints have all migrated
const legacyRangeKey = ""

var rangeKeyRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{8}$`)

func unmarshalStrict(data json.RawMessage, v any) error {
	var dec = json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
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
// streaming v2 channel state.
// TODO(#5176): remove in about January 2027, after checkpoints have all migrated
var legacyItemClear = func() map[string]any {
	var out = maps.Clone(flatItemClear)
	delete(out, "StreamV2")
	return out
}()

func isMergeQuery(query string) bool {
	return strings.HasPrefix(query, "\nMERGE INTO")
}

// absorbState folds a state document into the transactor. The primary keeps
// every range-keyed entry in d.staged, along with the staged fields of a flat
// item as its legacy entry. A flat item's remaining fields go to d.cp. With
// peerPatch set, the document is a patch whose flat fields and whose entry of
// this shard are already held. A field that is neither a range key nor a
// field of checkpointItem is an error.
func (d *transactor) absorbState(doc json.RawMessage, peerPatch bool) error {
	var raw map[string]map[string]json.RawMessage
	if err := json.Unmarshal(doc, &raw); err != nil {
		return err
	}
	if d.cp == nil {
		d.cp = make(checkpoint)
	}
	if d.staged == nil {
		d.staged = make(stagedCheckpoint)
	}

	for stateKey, fields := range raw {
		for field, itemRaw := range fields {
			if !rangeKeyRe.MatchString(field) {
				continue
			}
			delete(fields, field)

			var item *checkpointItem
			if err := unmarshalStrict(itemRaw, &item); err != nil {
				return fmt.Errorf("parsing checkpoint entry %q of %q: %w", field, stateKey, err)
			} else if item != nil && d.primary && !(peerPatch && field == d.rangeKey) {
				d.staged.add(stateKey, field, item)
			}
		}

		var flat checkpointItem
		if flatRaw, err := json.Marshal(fields); err != nil {
			return fmt.Errorf("marshalling checkpoint entry %q: %w", stateKey, err)
		} else if err := unmarshalStrict(flatRaw, &flat); err != nil {
			return fmt.Errorf("parsing checkpoint entry %q: %w", stateKey, err)
		} else if peerPatch {
			continue
		}

		// TODO(#5176): remove in about January 2027, after checkpoints have all migrated
		if len(flat.Query) > 0 || flat.TruncateBefore != nil {
			if d.primary {
				var legacy = flat
				legacy.StreamV2 = nil
				d.staged.add(stateKey, legacyRangeKey, &legacy)
			}
			flat = checkpointItem{Table: flat.Table, StreamV2: flat.StreamV2}
		}
		var hasWork = len(flat.StreamBlobs) > 0 || len(flat.PipeFiles) > 0 || len(flat.StreamV2) > 0
		if hasWork && !(flat.streamV2Only() && slices.Contains(d.sweptStreamV2, stateKey)) {
			d.cp[stateKey] = &flat
		}
	}

	return nil
}

// mergePeerStatePatches folds the StartedCommit state patches of every shard
// into the primary's staged entries.
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
	for _, stateKey := range d.sweptStreamV2 {
		nested[stateKey] = maps.Clone(flatItemClear)
	}
	d.sweptStreamV2 = nil

	for stateKey, entries := range d.staged {
		if item := entries[d.rangeKey]; item == nil {
			continue
		} else if nested[stateKey] == nil {
			nested[stateKey] = map[string]any{d.rangeKey: item}
		} else {
			nested[stateKey][d.rangeKey] = item
		}
	}

	var out = make(map[string]any, len(d.cp)+len(nested))
	for stateKey, item := range d.cp {
		out[stateKey] = item
	}
	for stateKey, fields := range nested {
		if flat := d.cp[stateKey]; flat != nil {
			var flatFields map[string]json.RawMessage
			if raw, err := json.Marshal(flat); err != nil {
				return nil, fmt.Errorf("marshalling checkpoint entry %q: %w", stateKey, err)
			} else if err := json.Unmarshal(raw, &flatFields); err != nil {
				return nil, fmt.Errorf("unmarshalling checkpoint entry %q: %w", stateKey, err)
			}
			for field, value := range flatFields {
				fields[field] = value
			}
		}
		out[stateKey] = fields
	}

	var checkpointJSON, err = json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("creating checkpoint json: %w", err)
	}
	return checkpointJSON, nil
}

// commitStaged applies the staged entries of binding idx in range-key order
// and deletes their staged files. Entries of MERGE INTO that the binding's
// current spec staged coalesce into one MERGE INTO over all of their files,
// because each MERGE INTO scans the target table. The COPY INTO of new rows
// that the current spec staged coalesce likewise, because Snowflake loads
// each staged file on one thread. Every other query is the entry's own
// pre-rendered one, since only that query matches the columns its files were
// staged with.
//
// A recovered entry may already have been applied before its clearing was
// committed. Re-applying it is safe, because its MERGE INTO converges on the
// same rows, and COPY INTO skips files it has already loaded.
func (d *transactor) commitStaged(ctx context.Context, idx int, entries map[string]*checkpointItem) error {
	var b = d.bindings[idx]
	var path = b.target.Path

	var items, merges, copies []*checkpointItem
	for _, rk := range slices.Sorted(maps.Keys(entries)) {
		var item = entries[rk]
		items = append(items, item)
		if rk == legacyRangeKey || item.Version != d.version {
			continue
		}
		if isMergeQuery(item.Query) && len(item.Bounds) == len(b.target.Keys) {
			merges = append(merges, item)
		}
		if len(item.CopyQuery) > 0 || (len(item.Query) > 0 && !isMergeQuery(item.Query)) {
			copies = append(copies, item)
		}
	}
	if len(merges) < 2 {
		merges = nil
	}
	if len(copies) < 2 {
		copies = nil
	}

	d.be.StartedResourceCommit(path)
	for _, item := range items {
		if len(item.Query) == 0 || slices.Contains(merges, item) || (slices.Contains(copies, item) && !isMergeQuery(item.Query)) {
			continue
		}
		d.commitProfile.SetBounds(idx, profileBounds(b.target.Keys, item))
		if err := d.runStoreQuery(ctx, item, item.Query, item.Rows, idx, path); err != nil {
			return err
		}
	}
	if len(merges) > 0 {
		var dirs []string
		var rows int64
		for _, item := range merges {
			dirs, rows = append(dirs, item.StagedDir), rows+item.Rows
		}
		var bounds = combineBounds(b.target.Keys, merges)
		var files = fmt.Sprintf("@flow_v1 (PATTERN => '%s')", stagedDirsPattern(dirs))
		query, err := renderBoundedQueryTemplate(d.templates.mergeInto, b.target, files, bounds)
		if err != nil {
			return fmt.Errorf("mergeInto template: %w", err)
		}
		d.commitProfile.SetBounds(idx, sql.MergeBounds(bounds))
		if err := d.runStoreQuery(ctx, merges[0], query, rows, idx, path); err != nil {
			return err
		}
	}
	// The COPYs run after the MERGEs so that a MERGE doesn't scan the rows a
	// COPY appends.
	for _, item := range items {
		if len(item.CopyQuery) == 0 || slices.Contains(copies, item) {
			continue
		} else if err := d.runStoreQuery(ctx, item, item.CopyQuery, 0, idx, path); err != nil {
			return err
		}
	}
	if len(copies) > 0 {
		var dirs []string
		var rows int64
		for _, item := range copies {
			if len(item.CopyQuery) > 0 {
				dirs = append(dirs, item.CopyStagedDir)
			} else {
				dirs, rows = append(dirs, item.StagedDir), rows+item.Rows
			}
		}
		var query strings.Builder
		if err := d.templates.copyInto.Execute(&query, &tableAndFile{Table: b.target, File: "@flow_v1", Pattern: stagedDirsPattern(dirs)}); err != nil {
			return fmt.Errorf("copyInto template: %w", err)
		} else if err := d.runStoreQuery(ctx, copies[0], query.String(), rows, idx, path); err != nil {
			return err
		}
	}

	// The truncation runs once, after every entry's stores, so that it keeps
	// the rows they just wrote, which were published after the boundary.
	var truncation *checkpointItem
	var dirs []string
	for _, item := range items {
		if tb := item.TruncateBefore; tb != nil && (truncation == nil || tb.After(*truncation.TruncateBefore)) {
			truncation = item
		}
		for _, dir := range []string{item.StagedDir, item.CopyStagedDir} {
			if dir != "" {
				dirs = append(dirs, dir)
			}
		}
	}
	if truncation != nil {
		if err := d.runTruncation(ctx, truncation, idx, path); err != nil {
			return err
		}
	}
	d.be.FinishedResourceCommit(path)

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
