package connector

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/estuary/connectors/go/materialize"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	pc "go.gazette.dev/core/consumer/protocol"
	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

const (
	// A binding that skips more than rejectLimit records and rejectRatio of those it received
	// in one transaction stops the task, since its data or mapping is likely wrong throughout.
	rejectLimit = 10
	rejectRatio = 0.1

	// A link whose record isn't in Twenty yet waits in the connector's state, which Flow saves
	// with each transaction, and is tried again at most every linkRetryEvery, until linkWaitFor
	// has passed. A binding keeps at most maxWaitingLinks, dropping the oldest, and tries at most
	// maxLinkRetries of them every linkRetryEvery, however often linkLater runs, so that retries
	// can't take a workspace's whole rate limit.
	linkRetryEvery  = 5 * time.Minute
	linkWaitFor     = 7 * 24 * time.Hour
	maxWaitingLinks = 10_000
	maxLinkRetries  = 10 * maxBatch
)

// giveBackTimeout bounds giving back the records adopted for a chunk the task couldn't write.
var giveBackTimeout = 30 * time.Second

type transactor struct {
	mu       sync.Mutex // Store and retryWaiting take turns
	stop     context.CancelFunc
	client   *client
	bindings []*binding
	be       *materialize.BindingEvents
	now      func() time.Time
	changed  bool // a binding's waiting links changed since the last saved state
	// The found links of a background retry whose write Twenty rejected, under rejected_records
	// fail. The next transaction tries them again and fails if they're still rejected.
	rejected map[*binding][]linkSlot
	members  *memberIndex // the workspace members, read once per round of writes when a link needs them
}

// connectorState is what the connector keeps in Flow between transactions and restarts.
type connectorState struct {
	// WaitingLinks are the links whose records weren't in Twenty yet, by binding path.
	WaitingLinks map[string][]savedLink `json:"waitingLinks,omitempty"`
}

type savedLink struct {
	Key    string    `json:"key"`    // the linking record's key
	Column string    `json:"column"` // its join column, like "companyId"
	Target string    `json:"target"` // the key of the record the link points at
	Lookup string    `json:"lookup"` // where that key is found, like "companies.externalId"
	Since  time.Time `json:"since"`
	Tried  time.Time `json:"tried"` // as of the last saved state
	Found  bool      `json:"found,omitempty"`
}

// retryBudget counts the waiting links a binding tried in the last linkRetryEvery.
type retryBudget []retryCount

type retryCount struct {
	at time.Time
	n  int
}

// take returns how many of n due links may be tried now, and counts them.
func (r *retryBudget) take(n int, now time.Time) int {
	*r = slices.DeleteFunc(*r, func(c retryCount) bool { return now.Sub(c.at) >= linkRetryEvery })
	used := 0
	for _, c := range *r {
		used += c.n
	}
	n = max(0, min(n, maxLinkRetries-used))
	if n > 0 {
		*r = append(*r, retryCount{now, n})
	}
	return n
}

// waitingLink is a link whose record isn't in Twenty yet.
type waitingLink struct {
	target string
	since  time.Time // when it started waiting
	tried  time.Time // when linkLater last looked for its record
	found  bool      // its record was found, but writing the link failed
}

func statePath(path []string) string { return strings.Join(path, "/") }

var _ materialize.Transactor = (*transactor)(nil)

func (t *transactor) RecoverCheckpoint(context.Context, pf.MaterializationSpec, pf.RangeSpec) (materialize.RuntimeCheckpoint, error) {
	return nil, nil
}

func (l link) lookup() string { return l.target + "." + l.idField }

// UnmarshalState restores the waiting links of the bindings and relations the task still has.
// The others are dropped from the next saved state, so they can't come back if a binding or
// relation is added again later.
func (t *transactor) UnmarshalState(raw json.RawMessage) error {
	var st connectorState
	if err := json.Unmarshal(raw, &st); err != nil {
		return fmt.Errorf("parsing the connector state: %w", err)
	}
	// The retry goroutine is already running.
	t.mu.Lock()
	defer t.mu.Unlock()
	restored, now := 0, t.now()
	for _, b := range t.bindings {
		for _, w := range st.WaitingLinks[statePath(b.path)] {
			// A relation that now finds its records by another field would read the key wrong.
			if !slices.ContainsFunc(b.links, func(l link) bool { return l.joinColumn == w.Column && l.lookup() == w.Lookup }) {
				continue
			}
			restored++
			if b.waiting == nil {
				b.waiting = make(map[linkSlot]waitingLink)
			}
			// A last try in the future, after the clock went back, would hold the link back.
			tried := w.Tried
			if tried.After(now) {
				tried = now
			}
			b.waiting[linkSlot{w.Key, w.Column}] = waitingLink{target: w.Target, since: w.Since, tried: tried, found: w.Found}
			// Retries before a restart count against the budget too; a link's first try doesn't.
			if tried.After(w.Since) && now.Sub(tried) < linkRetryEvery {
				b.retries = append(b.retries, retryCount{tried, 1})
			}
		}
	}
	saved := 0
	for _, links := range st.WaitingLinks {
		saved += len(links)
	}
	if saved > restored {
		t.changed = true
		log.WithFields(log.Fields{"dropped": saved - restored}).
			Warn("dropped waiting links of bindings or relations that were removed or changed")
	}
	return nil
}

// state returns the connector state to save: every binding's waiting links.
func (t *transactor) state() (json.RawMessage, error) {
	st := connectorState{WaitingLinks: make(map[string][]savedLink)}
	for _, b := range t.bindings {
		for _, slot := range slices.SortedFunc(maps.Keys(b.waiting), compareSlots) {
			w := b.waiting[slot]
			path := statePath(b.path)
			st.WaitingLinks[path] = append(st.WaitingLinks[path], savedLink{Key: slot.key, Column: slot.column, Target: w.target, Lookup: b.linkLookup(slot.column), Since: w.since, Tried: w.tried, Found: w.found})
		}
	}
	return json.Marshal(st)
}

func (t *transactor) Flush(context.Context, map[int]time.Time) error { return nil }

func (t *transactor) Acknowledge(context.Context, []json.RawMessage, []string) (*pf.ConnectorState, error) {
	return nil, nil
}

func (t *transactor) Destroy() {
	if t.stop != nil {
		t.stop()
		t.mu.Lock() // waits for a retry under way to stop
		t.mu.Unlock()
	}
}

// retryLinks tries the waiting links again every linkRetryEvery, also while no documents arrive,
// until ctx ends. The links it writes are saved as written with the next transaction's state, so
// after a crash before then they are only written again.
func (t *transactor) retryLinks(ctx context.Context) {
	// Often, so that a link is tried soon after it's due.
	ticker := time.NewTicker(linkRetryEvery / 5)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.retryWaiting(ctx)
		}
	}
}

func (t *transactor) retryWaiting(ctx context.Context) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.members = nil
	if len(t.rejected) > 0 {
		return // the next transaction tries them; retrying would only rewrite the same links
	}
	for _, b := range t.bindings {
		if len(b.waiting) == 0 {
			continue
		}
		var s stats
		if err := t.linkLater(ctx, b, &s); err != nil {
			// A link Twenty rejects under rejected_records fail stops the task if the next
			// transaction's try is rejected too. Other errors leave the links to the next round.
			if errors.As(err, new(*linkRejectedError)) {
				t.rejected = put(t.rejected, b, s.unwritten)
			}
			if ctx.Err() == nil {
				log.WithError(err).WithField("object", b.object).Warn("retrying waiting links failed; the next try is in a few minutes")
			}
			continue
		}
		if s.linkedLater > 0 || len(s.skipped) > 0 {
			log.WithFields(log.Fields{"object": b.object, "linkedLater": s.linkedLater, "skipped": s.skipped,
				"stillWaiting": len(b.waiting)}).Info("retried waiting links")
		}
	}
}

func (t *transactor) Load(it *materialize.LoadIterator, _ func(int, json.RawMessage) error) error {
	for it.Next() {
		panic("the connector only does delta updates")
	}
	return nil
}

// pending is a stored document on its way to Twenty.
type pending struct {
	key  string
	rec  record // nil for a deletion
	id   string // the Twenty record's id, if it has one
	held record // the Twenty record, if it has one
}

// stats counts what happened to one binding's documents in a transaction.
type stats struct {
	received, superseded, created, updated, flagged, deleted, rejected int
	adopted                                                            int                    // existing records the binding took, by adopt_by
	linkedLater                                                        int                    // links written after every binding's documents
	skipped                                                            map[string]int         // by reason
	invalid                                                            map[string]int         // values left out, by field
	lastRejected                                                       string                 // the key and reason of the last rejection
	rejectedKeys                                                       map[string]bool        // the keys rejected in the chunk being written
	waiting                                                            map[linkSlot]string    // links whose records weren't found yet, to their keys
	unwritten                                                          []linkSlot             // saved links found, whose write linkLater failed
	replaced                                                           map[linkSlot]priorLink // the links the chunk's documents set, as they were before
	linking                                                            bool                   // the writes are linkLater's, of records written already
}

// linkSlot is a record's join column, by the record's key.
type linkSlot struct{ key, column string }

// priorLink is what waited in a link slot before a document set the link, so a rejected document
// leaves it as it was.
type priorLink struct {
	waiting       string // in stats.waiting
	inTransaction bool
	saved         waitingLink // in binding.waiting
	inState       bool
}

func (s *stats) skip(reason string) {
	s.skipped = addCount(s.skipped, reason, 1)
}

// Store writes each binding's documents to Twenty, up to maxBatch documents at a time. Stores
// arrive grouped by binding and ordered by key, so the documents of a key are adjacent.
func (t *transactor) Store(it *materialize.StoreIterator) (materialize.StartCommitFunc, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.members = nil
	ctx := it.Context()
	all := make([]stats, len(t.bindings))
	// Tried first thing by linkLater, unless the transaction's documents set them anew.
	for i, b := range t.bindings {
		for _, slot := range t.rejected[b] {
			if w, ok := b.waiting[slot]; ok {
				all[i].waiting = put(all[i].waiting, slot, w.target)
			}
		}
	}
	t.rejected = nil
	var chunk []pending
	current := -1
	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		err := t.storeChunk(ctx, t.bindings[current], chunk, &all[current])
		chunk = nil
		return err
	}

	// Next(false), because in delta updates no document "exists" in the destination, so
	// Next(true) would drop every deletion.
	for it.Next(false) {
		if it.Binding != current || len(chunk) == maxBatch {
			if err := flush(); err != nil {
				return nil, err
			}
			current = it.Binding
		}
		b, s := t.bindings[it.Binding], &all[it.Binding]
		s.received++
		key, err := keyString(it.Key)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", b.object, err)
		}
		p := pending{key: key}
		deleted, err := isDeletion(it.Delete, it.RawJSON)
		if err != nil {
			return nil, fmt.Errorf("%s: reading the document: %w", b.object, err)
		}
		if deleted && b.res.deleteMode() == deleteIgnore {
			// Before the chunk, so an earlier document of the key in it is still written.
			s.skip("deletion_ignored")
			continue
		}
		if !deleted {
			if p.rec, err = b.record(key, it.Values, it.RawJSON); err != nil {
				// An earlier document of the key stays in the chunk, as if each were written in turn.
				if err := t.reject(b, s, key, err.Error()); err != nil {
					return nil, err
				}
				continue
			}
		}
		// The last document of a key in a chunk replaces the ones before it, keeping the fields
		// only an earlier one set, as writing each in turn would have.
		i := slices.IndexFunc(chunk, func(q pending) bool { return q.key == key })
		if i >= 0 && (p.rec == nil) != (chunk[i].rec == nil) && (p.rec == nil || b.res.deleteMode() != deleteFlag) {
			// A deletion after a write of the key: write first, then delete or flag the record.
			// A write after a deletion that removes the record goes after it, so it finds the
			// record trashed, or gone and to create anew. A write after a flag unsets it, so
			// that pair needs no flush.
			if err := flush(); err != nil {
				return nil, err
			}
		} else if i >= 0 {
			if earlier := chunk[i].rec; p.rec != nil && earlier != nil {
				b.mergeUnder(p.rec, earlier)
				// Again, since a subfield the earlier document set can clash with this one's,
				// like a phone's country code with a number that starts with "+".
				b.normalize(p.rec)
			}
			chunk = slices.Delete(chunk, i, i+1)
			s.superseded++
		}
		chunk = append(chunk, p)
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	if err := flush(); err != nil {
		return nil, err
	}
	for i, b := range t.bindings {
		if err := t.linkLater(ctx, b, &all[i]); err != nil {
			return nil, err
		}
	}

	for i, b := range t.bindings {
		s := all[i]
		// A binding with no documents may still have retried its waiting links.
		if s.received == 0 && s.linkedLater == 0 && len(s.skipped) == 0 {
			continue
		}
		if s.rejected > rejectLimit && float64(s.rejected) > rejectRatio*float64(s.received) {
			return nil, fmt.Errorf("%s: skipped %d of %d records, more than %d and %.0f%%; the last, %s",
				b.object, s.rejected, s.received, rejectLimit, rejectRatio*100, s.lastRejected)
		}
		log.WithFields(log.Fields{
			"object": b.object, "received": s.received, "superseded": s.superseded, "created": s.created,
			"updated": s.updated, "flaggedDeleted": s.flagged, "deleted": s.deleted, "skipped": s.skipped, "rejected": s.rejected,
			"invalidValuesLeftOut": s.invalid, "linkedLater": s.linkedLater, "adopted": s.adopted,
		}).Info("stored documents")
		// The total counts the documents applied as the binding's settings say, including those
		// it leaves alone on purpose, like ignored deletions. It falls short of the documents
		// received only when records were rejected and skipped, so the health check shows that.
		if s.received > 0 {
			t.be.ReportRowStats(it.Round, b.path, materialize.TotalRowStats(int64(s.received-s.rejected)))
		}
	}
	if !t.changed {
		return nil, nil
	}
	state, err := t.state()
	if err != nil {
		return nil, err
	}
	t.changed = false
	return func(context.Context, *pc.Checkpoint) (*pf.ConnectorState, materialize.OpFuture) {
		return &pf.ConnectorState{UpdatedJson: state}, nil
	}, nil
}

// storeChunk writes a chunk of one binding's documents, each with a different key.
func (t *transactor) storeChunk(ctx context.Context, b *binding, chunk []pending, s *stats) error {
	clear(s.rejectedKeys)
	var deletes, upserts []pending
	for _, p := range chunk {
		if !filterable(p.key) {
			// A lookup can't find this key's record, so a write could duplicate it, or restore
			// it after someone deleted it in Twenty.
			if err := t.reject(b, s, p.key, "the key can't go in a Twenty filter: it is empty, too long, or has a quote, backslash or control character"); err != nil {
				return err
			}
			continue
		}
		if p.rec == nil {
			deletes = append(deletes, p)
		} else {
			upserts = append(upserts, p)
		}
	}
	if err := t.applyDeletes(ctx, b, deletes, s); err != nil {
		return err
	}
	return t.upsert(ctx, b, upserts, s)
}

// lookup returns the binding's records for keys by their ID field, soft-deleted ones included,
// since Twenty's writes match those too.
func (t *transactor) lookup(ctx context.Context, b *binding, items []pending) (map[string]record, error) {
	keys := make([]string, len(items))
	for i, p := range items {
		keys[i] = p.key
	}
	rows, err := t.client.find(ctx, b.object, b.res.IDField, keys, true)
	if err != nil {
		return nil, err
	}
	out := make(map[string]record, len(rows))
	for _, r := range rows {
		if k, ok := r[b.res.IDField].(string); ok {
			out[k] = r
		}
	}
	return out, nil
}

// applyDeletes flags, trashes or deletes the records of deleted documents, as the binding's
// delete mode says.
func (t *transactor) applyDeletes(ctx context.Context, b *binding, deletes []pending, s *stats) error {
	if len(deletes) == 0 {
		return nil
	}
	mode := b.res.deleteMode()
	if err := t.deleteRecords(ctx, b, deletes, s, mode); err != nil {
		return err
	}
	if mode != deleteFlag {
		// A deleted record has nothing to link. One Twenty didn't delete keeps its waiting links.
		for _, p := range deletes {
			if s.rejectedKeys[p.key] {
				continue
			}
			for _, l := range b.links {
				slot := linkSlot{p.key, l.joinColumn}
				_, waiting := s.waiting[slot]
				_, saved := b.waiting[slot]
				if waiting || saved {
					s.skip("link_dropped_record_deleted")
				}
				delete(s.waiting, slot)
				if saved {
					delete(b.waiting, slot)
					t.changed = true
				}
			}
		}
	}
	return nil
}

// deleteRecords flags, trashes or deletes the records of deleted documents.
func (t *transactor) deleteRecords(ctx context.Context, b *binding, deletes []pending, s *stats, mode string) error {
	existing, err := t.lookup(ctx, b, deletes)
	if err != nil {
		return err
	}
	targets := make(map[string]string) // Twenty id -> key
	for _, p := range deletes {
		r, ok := existing[p.key]
		switch {
		case !ok:
			s.skip("deletion_not_in_twenty")
		case r["deletedAt"] != nil:
			s.skip("deletion_deleted_in_twenty")
		case mode == deleteFlag && r[b.res.DeletedField] == true:
			s.skip("deletion_already_flagged")
		default:
			id, _ := r["id"].(string)
			targets[id] = p.key
		}
	}
	if len(targets) == 0 {
		return nil
	}

	values := record{b.res.DeletedField: true}
	if b.res.DeletedAtField != "" {
		values[b.res.DeletedAtField] = t.now().UTC().Format(time.RFC3339)
	}
	// Each id goes in the URL with its key.
	ids := slices.Sorted(maps.Keys(targets))
	// Each id goes in the URL twice: in the id list that keeps the request to these records, and
	// in its pair.
	size := func(id string) int {
		pair, err := pairFilter(b.res.IDField, []string{id}, targets)
		if err != nil {
			return maxFilterBytes // pairFilter fails again below, with the reason
		}
		return len(escape(pair+",")) + inFilterBytes(id)
	}
	for _, group := range sizedBatches(ids, size) {
		where, err := pairFilter(b.res.IDField, group, targets)
		if err != nil {
			return err
		}
		// Filtered by each record's id and key together: a record deleted in Twenty since the
		// lookup isn't restored or recreated, and one whose key changed since, even to another
		// key being deleted, isn't the binding's to flag or delete.
		var done []record
		if mode == deleteFlag {
			done, err = t.client.update(ctx, b.object, group, values, where)
		} else {
			done, err = t.client.remove(ctx, b.object, group, where, mode == deleteTrash)
		}
		if err != nil {
			return err
		}
		applied := make(map[string]bool, len(done))
		for _, r := range done {
			if id, ok := r["id"].(string); ok {
				applied[id] = true
			}
		}
		var missed []string
		for _, id := range group {
			switch {
			case !applied[id]:
				missed = append(missed, id)
			case mode == deleteFlag:
				s.flagged++
			default:
				s.deleted++
			}
		}
		if err := t.checkMissed(ctx, b, s, mode, missed, targets); err != nil {
			return err
		}
	}
	return nil
}

// checkMissed handles the records a delete didn't apply to. One deleted, trashed or given another
// key in Twenty since the lookup isn't the binding's to delete, and is skipped, as is one a lost
// answer's retry no longer finds. One a trash or delete left live with the same key was skipped by
// Twenty itself, like Twenty v2.30 to v2.34 when it doesn't say its version for the guard in
// version.go, so it is rejected rather than counted as deleted.
func (t *transactor) checkMissed(ctx context.Context, b *binding, s *stats, mode string, missed []string, targets map[string]string) error {
	live := make(map[string]record)
	if mode != deleteFlag && len(missed) > 0 {
		found, err := t.client.find(ctx, b.object, "id", missed, false)
		if err != nil {
			return err
		}
		for _, r := range found {
			if id, ok := r["id"].(string); ok {
				live[id] = r
			}
		}
	}
	for _, id := range missed {
		key := targets[id]
		if r, ok := live[id]; ok && r[b.res.IDField] == key && r["deletedAt"] == nil {
			if err := t.reject(b, s, key, "Twenty answered the delete without an error, but the record is still live with the same key"); err != nil {
				return err
			}
			continue
		}
		s.skip("deletion_changed_in_twenty")
	}
	return nil
}

// pairFilter matches the records among ids whose field still holds the key keys gives each.
func pairFilter(field string, ids []string, keys map[string]string) (string, error) {
	pairs := make([]string, len(ids))
	for i, id := range ids {
		idEq, err := filterEq("id", id)
		if err != nil {
			return "", err
		}
		keyEq, err := filterEq(field, keys[id])
		if err != nil {
			return "", err
		}
		pairs[i] = "and(" + idEq + "," + keyEq + ")"
	}
	if len(pairs) == 1 {
		return pairs[0], nil
	}
	return "or(" + strings.Join(pairs, ",") + ")", nil
}

// upsert creates the records that are new and updates the others by their Twenty id.
func (t *transactor) upsert(ctx context.Context, b *binding, items []pending, s *stats) (err error) {
	if len(items) == 0 {
		return nil
	}
	existing, err := t.lookup(ctx, b, items)
	if err != nil {
		return err
	}
	var kept []pending
	for _, p := range items {
		if r, ok := existing[p.key]; ok {
			if r["deletedAt"] != nil && !b.res.RestoreDeleted {
				s.skip("deleted_in_twenty")
				continue
			}
			p.id, _ = r["id"].(string)
			p.held = r
		}
		kept = append(kept, p)
	}
	adopted, err := t.adopt(ctx, b, kept, s)
	defer func() {
		// The task stops before it writes the chunk's documents, so the records adopted for them
		// go back. A retry adopts them again. Twenty is often down when this happens, so the
		// connector tries for a short while and stops at the first failure.
		if err == nil {
			return
		}
		gbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), giveBackTimeout)
		defer cancel()
		for _, key := range slices.Sorted(maps.Keys(adopted)) {
			if gbErr := t.giveBack(gbCtx, b, key, adopted[key]); gbErr != nil {
				log.WithFields(log.Fields{"object": b.object, "key": key, "error": gbErr}).Warn("couldn't give back the adopted records")
				return
			}
		}
	}()
	if err != nil {
		return err
	}
	if err := t.resolveLinks(ctx, b, kept, adopted, s); err != nil {
		return err
	}
	var deferred []pending
	if kept, deferred, err = t.guardUnique(ctx, b, kept, false, s); err != nil {
		return err
	}

	var creates, updates []pending
	for _, p := range kept {
		if p.id == "" {
			creates = append(creates, p)
		} else {
			p.rec["id"] = p.id
			// After guardUnique, so a unique value it left out keeps the value Twenty holds.
			b.keepSubfields(p.rec, p.held)
			updates = append(updates, p)
		}
	}
	// New records go through Twenty's plain create, which refuses a unique value that another
	// record took since the lookup. An update names its record's id, and Twenty refuses it if a
	// unique value matches a second record. So no write can take another record over.
	if err := t.writeBatch(ctx, b, creates, false, s); err != nil {
		return err
	}
	if err := t.writeBatch(ctx, b, updates, true, s); err != nil {
		return err
	}
	if err := t.writeDeferred(ctx, b, deferred, s); err != nil {
		return err
	}
	if err := t.release(ctx, b, adopted, s); err != nil {
		return err
	}
	// A record the chunk didn't write keeps the waiting links it had, and gets none of its document's.
	for slot, prior := range s.replaced {
		if !s.rejectedKeys[slot.key] {
			continue
		}
		if prior.inTransaction {
			s.waiting[slot] = prior.waiting
		} else {
			delete(s.waiting, slot)
		}
		if prior.inState {
			b.waiting[slot] = prior.saved
		}
	}
	clear(s.replaced)
	return nil
}

// adopt gives a document with no record the live record that holds its adopt_by value and has
// an empty ID field, like one a person made, by writing the document's key to that ID field. The
// write checks both again, so a record that someone else took or changed since the lookup stays
// as it is, and the document goes on as a new one, whose value guardUnique then finds held.
// Before guardUnique, which would otherwise leave the value out as another record's.
func (t *transactor) adopt(ctx context.Context, b *binding, items []pending, s *stats) (map[string]adoption, error) {
	if b.adopt == nil {
		return nil, nil
	}
	f := *b.adopt
	first := make(map[string]int) // value -> the first document with no record that writes it
	for i, p := range items {
		v := uniqueValue(p.rec, f)
		if _, seen := first[v]; p.id == "" && filterable(v) && !seen {
			first[v] = i
		}
	}
	if len(first) == 0 {
		return nil, nil
	}
	// Live records only: one in the trash is never adopted.
	rows, err := t.client.find(ctx, b.object, uniquePath(f), slices.Sorted(maps.Keys(first)), false)
	if err != nil {
		return nil, err
	}
	// On an error, what was taken so far comes back too, for upsert to give back.
	adopted := make(map[string]adoption)
	for _, r := range rows {
		v := uniqueValue(r, f)
		id, _ := r["id"].(string)
		i, ok := first[v]
		if held, _ := r[b.res.IDField].(string); !ok || held != "" || id == "" {
			continue
		}
		p := &items[i]
		value, err := quoted(v)
		if err != nil {
			return adopted, err
		}
		where := fmt.Sprintf(`or(%[1]s[is]:NULL,%[1]s[eq]:""),%s[eq]:%s`, b.res.IDField, uniquePath(f), value)
		got, err := t.client.update(ctx, b.object, []string{id}, record{b.res.IDField: p.key}, where)
		switch {
		case err != nil:
			// The lookup found no record with the key, and no other binding writes the object,
			// so Twenty shouldn't refuse this. Going on would create a duplicate. The update may
			// have been applied, so the record is given back, which only matches if it was.
			adopted[p.key] = adoption{id: id, was: r[b.res.IDField]}
			return adopted, fmt.Errorf("%s: adopting the record with the same %s for the document with key %q: %w", b.object, f.Name, p.key, err)
		case len(got) == 0:
			// A retry of an update whose answer was lost finds the key already written.
			again, err := t.client.find(ctx, b.object, "id", []string{id}, false)
			if err != nil {
				adopted[p.key] = adoption{id: id, was: r[b.res.IDField]}
				return adopted, err
			}
			if len(again) == 1 && again[0][b.res.IDField] == p.key {
				break
			}
			log.WithFields(log.Fields{"object": b.object, "key": p.key, "field": f.Name}).
				Info("a record to adopt changed since the lookup, so the document is written as a new record")
			continue
		}
		adopted[p.key] = adoption{id: id, was: r[b.res.IDField]}
		r[b.res.IDField] = p.key
		p.id, p.held = id, r
		s.adopted++
	}
	return adopted, nil
}

// adoption is a record adopt took for a document: its id, and what its ID field held before.
type adoption struct {
	id  string
	was any
}

// release gives back the records adopted for documents whose writes were rejected and skipped,
// emptying their ID fields again, so a record a person made doesn't become the binding's with
// none of the document's values.
func (t *transactor) release(ctx context.Context, b *binding, adopted map[string]adoption, s *stats) error {
	for _, key := range slices.Sorted(maps.Keys(adopted)) {
		if !s.rejectedKeys[key] {
			continue
		}
		if err := t.giveBack(ctx, b, key, adopted[key]); err != nil {
			return err
		}
		s.adopted--
	}
	return nil
}

// giveBack empties again the ID field of a record adopted for the document with key, if it
// still holds the key.
func (t *transactor) giveBack(ctx context.Context, b *binding, key string, a adoption) error {
	where, err := filterEq(b.res.IDField, key)
	if err != nil {
		return err
	}
	if _, err := t.client.update(ctx, b.object, []string{a.id}, record{b.res.IDField: a.was}, where); err != nil {
		return fmt.Errorf("%s: giving back the record adopted for the document with key %q: %w", b.object, key, err)
	}
	return nil
}

// resolveLinks puts into each record the Twenty ids of the records its links point at, found by
// key, soft-deleted ones included. A link whose record isn't in Twenty is left out, so the
// record keeps the link it has, and waits in s.waiting for linkLater.
func (t *transactor) resolveLinks(ctx context.Context, b *binding, items []pending, adopted map[string]adoption, s *stats) error {
	// A link to a record adopted in this chunk waits until the record's write is through: if it
	// is rejected, the record goes back.
	adoptedIDs := make(map[string]bool)
	for _, a := range adopted {
		adoptedIDs[a.id] = true
	}
	for _, l := range b.links {
		var keys []string
		for _, p := range items {
			v, set := p.rec[l.joinColumn]
			if !set {
				continue
			}
			// The record's last document decides its link, even if an earlier one's waits.
			slot := linkSlot{p.key, l.joinColumn}
			if _, seen := s.replaced[slot]; !seen {
				if s.replaced == nil {
					s.replaced = make(map[linkSlot]priorLink)
				}
				var prior priorLink
				prior.waiting, prior.inTransaction = s.waiting[slot]
				prior.saved, prior.inState = b.waiting[slot]
				s.replaced[slot] = prior
			}
			delete(s.waiting, slot)
			if _, saved := b.waiting[slot]; saved {
				delete(b.waiting, slot)
				t.changed = true
			}
			if ref, ok := v.(linkRef); ok {
				keys = append(keys, ref.key)
			}
		}
		if len(keys) == 0 {
			continue
		}
		ids, unusable, err := t.targets(ctx, l, keys)
		if err != nil {
			return err
		}
		for _, p := range items {
			ref, ok := p.rec[l.joinColumn].(linkRef)
			if !ok {
				continue
			}
			if id, found := ids[ref.key]; found && !(l.target == b.object && adoptedIDs[id]) {
				p.rec[l.joinColumn] = id
				continue
			}
			delete(p.rec, l.joinColumn)
			if why, ok := unusable[ref.key]; ok {
				s.skip(l.field + why)
				log.WithFields(log.Fields{"object": b.object, "key": p.key, "field": l.field, "reason": l.field + why}).
					Info("left out a link whose key can't name one record")
				continue
			}
			s.waiting = put(s.waiting, linkSlot{p.key, l.joinColumn}, ref.key)
		}
	}
	return nil
}

// targets returns the Twenty ids of a link's records, by their keys, and the keys that can't
// name a record however long they wait, each with the reason it's skipped.
func (t *transactor) targets(ctx context.Context, l link, keys []string) (map[string]string, map[string]string, error) {
	ids, unusable := make(map[string]string), make(map[string]string)
	switch l.match {
	case matchMember:
		members, err := t.workspaceMembers(ctx)
		if err != nil {
			return nil, nil, err
		}
		index := members.byEmail
		if l.idField == "name" {
			index = members.byName
		}
		for _, k := range keys {
			switch nk := normalizeMemberKey(k, l.idField); {
			case nk == "":
				unusable[k] = "_target_key_unusable"
			case len(index[nk]) == 1:
				ids[k] = index[nk][0]
			case len(index[nk]) > 1:
				unusable[k] = "_link_ambiguous"
			}
		}
		return ids, unusable, nil
	}
	// Each key names the value Twenty holds for the record it links: an id, a unique field's
	// value in the form Twenty keeps it, or the key itself in a unique Text field.
	path, value, unusableAs := l.idField, func(r record) string { v, _ := r[l.idField].(string); return v }, "_target_key_unusable"
	norm := func(k string) (string, bool) { return k, filterable(k) }
	switch l.match {
	case matchID:
		path, value, unusableAs = "id", func(r record) string { v, _ := r["id"].(string); return v }, "_link_not_a_twenty_id"
		norm = func(k string) (string, bool) {
			// Twenty takes RFC 4122 ids of versions 1 to 5 in a filter and answers 400 to others,
			// which would fail the whole lookup.
			u, err := uuid.Parse(k)
			return u.String(), err == nil && u.Variant() == uuid.RFC4122 && u.Version() >= 1 && u.Version() <= 5
		}
	case matchUnique:
		path, value = uniquePath(l.idMeta), func(r record) string { return uniqueValue(r, l.idMeta) }
		norm = func(k string) (string, bool) {
			v, ok := l.targetValue(k)
			return v, ok && filterable(v)
		}
	}
	byValue := make(map[string][]string) // the value Twenty holds -> the keys that name it
	var values []string                  // in the keys' order, which find batches by size
	for _, k := range keys {
		if v, ok := norm(k); !ok {
			unusable[k] = unusableAs
		} else if byValue[v] = append(byValue[v], k); len(byValue[v]) == 1 {
			values = append(values, v)
		}
	}
	if l.match == matchID || l.match == matchUnique {
		slices.Sort(values)
	}
	if len(values) > 0 {
		rows, err := t.client.find(ctx, l.target, path, values, true)
		if err != nil {
			return nil, nil, err
		}
		for _, r := range rows {
			if id, _ := r["id"].(string); id != "" {
				for _, k := range byValue[value(r)] {
					ids[k] = id
				}
			}
		}
	}
	if l.match == matchID {
		// Twenty makes the ids, so one that isn't there won't arrive later.
		for _, k := range keys {
			if _, ok := ids[k]; !ok && unusable[k] == "" {
				unusable[k] = "_link_id_not_in_twenty"
			}
		}
	}
	return ids, unusable, nil
}

// logKey is how a link's target key appears in logs: a workspace member's email or name, or
// another email, is left out.
func logKey(l link, key string) string {
	if l.match == matchMember || l.match == matchUnique && l.idMeta.Type == "EMAILS" {
		return "<" + l.idField + ">"
	}
	return key
}

// memberIndex finds workspace members by their normalized email or name.
type memberIndex struct{ byEmail, byName map[string][]string }

// workspaceMembers reads the workspace's active members once per round of writes, for links
// that match them in memory. Workspaces have few members, and the connector can't add one, so
// the list doesn't change within the round as far as the connector is concerned.
func (t *transactor) workspaceMembers(ctx context.Context) (*memberIndex, error) {
	if t.members != nil {
		return t.members, nil
	}
	m := &memberIndex{byEmail: make(map[string][]string), byName: make(map[string][]string)}
	add := func(index map[string][]string, key, id string) {
		if key != "" && !slices.Contains(index[key], id) {
			index[key] = append(index[key], id)
		}
	}
	err := t.client.pages(ctx, "workspaceMembers", "deletedAt[is]:NULL", func(rows []record) error {
		for _, r := range rows {
			id, _ := r["id"].(string)
			if id == "" {
				continue
			}
			email, _ := r["userEmail"].(string)
			add(m.byEmail, normalizeMemberKey(email, "userEmail"), id)
			name, _ := r["name"].(map[string]any)
			first, _ := name["firstName"].(string)
			last, _ := name["lastName"].(string)
			add(m.byName, normalizeMemberKey(first+" "+last, "name"), id)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("reading the workspace members: %w", err)
	}
	t.members = m
	return m, nil
}

// normalizeMemberKey is the form in which a workspace member's email or name ("First Last")
// matches: Unicode-normalized and, for emails, lowercased, so "ß" stays apart from "ss"; names are
// case-folded, with runs of spaces made one.
func normalizeMemberKey(key, field string) string {
	if field == "name" {
		// Folding can undo NFC, so normalize again after it.
		key = norm.NFC.String(cases.Fold().String(norm.NFC.String(key)))
		return strings.Join(strings.Fields(key), " ")
	}
	return strings.TrimSpace(strings.ToLower(norm.NFC.String(key)))
}

// linkLater writes the links that resolveLinks left out, once every binding's documents are
// written, since a record may point at one that a later binding or chunk created. It also tries
// again the links that earlier transactions left waiting, each at most every linkRetryEvery. A link
// whose record still isn't in Twenty waits in the connector state until linkWaitFor has passed.
func (t *transactor) linkLater(ctx context.Context, b *binding, s *stats) error {
	now := t.now()
	try := maps.Clone(s.waiting) // slot -> the key of the record the link points at
	if try == nil {
		try = make(map[linkSlot]string)
	}
	var due []linkSlot
	for slot, w := range b.waiting {
		if _, ok := try[slot]; !ok && now.Sub(w.tried) >= linkRetryEvery {
			due = append(due, slot)
		}
	}
	// The longest untried first, so that every waiting link gets its turn.
	slices.SortFunc(due, func(x, y linkSlot) int {
		return cmp.Or(b.waiting[x].tried.Compare(b.waiting[y].tried), compareSlots(x, y))
	})
	for _, slot := range due[:b.retries.take(len(due), now)] {
		w := b.waiting[slot]
		try[slot] = w.target
		w.tried = now
		b.waiting[slot] = w
	}
	clear(s.waiting)
	slots := slices.SortedFunc(maps.Keys(try), compareSlots)
	recs := make(map[string]record) // record key -> the links to write
	var found []linkSlot            // saved links whose records are found, left out once written
	for _, l := range b.links {
		var keys []string
		for _, slot := range slots {
			if slot.column == l.joinColumn {
				keys = append(keys, try[slot])
			}
		}
		if len(keys) == 0 {
			continue
		}
		ids, unusable, err := t.targets(ctx, l, keys)
		if err != nil {
			return err
		}
		var missing []string
		for _, slot := range slots {
			if slot.column != l.joinColumn {
				continue
			}
			if why, ok := unusable[try[slot]]; ok {
				// Like a member's name that a second member now has: waiting won't help.
				s.skip(l.field + why)
				if _, saved := b.waiting[slot]; saved {
					delete(b.waiting, slot)
					t.changed = true
				}
				continue
			}
			id, ok := ids[try[slot]]
			if w, saved := b.waiting[slot]; saved {
				w.found, w.tried = ok, now
				b.waiting[slot] = w
			}
			if !ok {
				if _, saved := b.waiting[slot]; !saved {
					s.skip(l.field + "_link_waiting")
					missing = append(missing, slot.key+" -> "+logKey(l, try[slot]))
					b.wait(slot, try[slot], now)
					t.changed = true
				}
				continue
			}
			if recs[slot.key] == nil {
				recs[slot.key] = record{}
			}
			recs[slot.key][l.joinColumn] = id
			if _, saved := b.waiting[slot]; saved {
				found = append(found, slot)
			}
		}
		if len(missing) > 0 {
			log.WithFields(log.Fields{"object": b.object, "field": l.field, "target": l.target, "count": len(missing),
				"examples": missing[:min(5, len(missing))]}).Info("links to records that aren't in Twenty yet wait for them")
		}
	}
	// After an error, the found links stay to be written in a later round or, after a failed
	// transaction, from the saved state.
	done := func() {
		for _, slot := range found {
			delete(b.waiting, slot)
			t.changed = true
		}
		t.expireLinks(b, s, now)
	}
	if len(recs) == 0 {
		done()
		return nil
	}

	items := make([]pending, 0, len(recs))
	for _, key := range slices.Sorted(maps.Keys(recs)) {
		items = append(items, pending{key: key, rec: recs[key]})
	}
	existing, err := t.lookup(ctx, b, items)
	if err != nil {
		return err
	}
	var write []pending
	for _, p := range items {
		r, ok := existing[p.key]
		if !ok || (r["deletedAt"] != nil && !b.res.RestoreDeleted) {
			s.skip("link_record_not_in_twenty")
			continue
		}
		p.id, _ = r["id"].(string)
		p.rec["id"] = p.id
		write = append(write, p)
	}
	// The records are written already, so their links aren't more records written, and one
	// Twenty rejects is a link left out, not a record skipped.
	later := stats{linking: true}
	if err := t.writeBatch(ctx, b, write, true, &later); err != nil {
		s.unwritten = found
		return err
	}
	s.linkedLater += later.updated
	for range later.rejected {
		s.skip("link_rejected")
	}
	// Written, or left out for good since their records are gone or rejected.
	done()
	return nil
}

// expireLinks drops the links that waited longer than linkWaitFor once they were looked for one
// last time, and the oldest beyond maxWaitingLinks, keeping those whose records were found.
func (t *transactor) expireLinks(b *binding, s *stats, now time.Time) {
	var expired, dropped int
	for slot, w := range b.waiting {
		if now.Sub(w.since) > linkWaitFor && w.tried.Equal(now) {
			delete(b.waiting, slot)
			expired++
		}
	}
	if over := len(b.waiting) - maxWaitingLinks; over > 0 {
		oldest := slices.SortedFunc(maps.Keys(b.waiting), func(x, y linkSlot) int {
			wx, wy := b.waiting[x], b.waiting[y]
			return cmp.Or(cmpBool(wx.found, wy.found), wx.since.Compare(wy.since), compareSlots(x, y))
		})
		for _, slot := range oldest[:over] {
			delete(b.waiting, slot)
		}
		dropped = over
	}
	if expired+dropped == 0 {
		return
	}
	t.changed = true
	if expired > 0 {
		s.skipped = addCount(s.skipped, "link_expired", expired)
	}
	if dropped > 0 {
		s.skipped = addCount(s.skipped, "link_waiting_dropped", dropped)
	}
	log.WithFields(log.Fields{"object": b.object, "expired": expired, "dropped": dropped, "stillWaiting": len(b.waiting)}).
		Warn("left out links whose records never arrived in Twenty")
}

// put sets m[k] to v, making m first if it is nil, and returns m.
func put[K comparable, V any](m map[K]V, k K, v V) map[K]V {
	if m == nil {
		m = make(map[K]V)
	}
	m[k] = v
	return m
}

func addCount(m map[string]int, k string, n int) map[string]int {
	if m == nil {
		m = make(map[string]int)
	}
	m[k] += n
	return m
}

func cmpBool(x, y bool) int {
	switch {
	case x == y:
		return 0
	case x:
		return 1
	}
	return -1
}

func compareSlots(x, y linkSlot) int {
	return cmp.Or(strings.Compare(x.key, y.key), strings.Compare(x.column, y.column))
}

// wait keeps a link whose record isn't in Twenty yet for later transactions.
func (b *binding) wait(slot linkSlot, target string, now time.Time) {
	b.waiting = put(b.waiting, slot, waitingLink{target: target, since: now, tried: now})
}

// writeDeferred writes the unique values guardUnique held back because other records of the
// chunk were giving them up, now that those are written.
func (t *transactor) writeDeferred(ctx context.Context, b *binding, deferred []pending, s *stats) error {
	// A record the chunk didn't write, because it was rejected or deleted in Twenty, stays so.
	deferred = slices.DeleteFunc(deferred, func(p pending) bool { return s.rejectedKeys[p.key] })
	if len(deferred) == 0 {
		return nil
	}
	existing, err := t.lookup(ctx, b, deferred)
	if err != nil {
		return err
	}
	var items []pending
	for _, p := range deferred {
		if r, ok := existing[p.key]; ok && (r["deletedAt"] == nil || b.res.RestoreDeleted) {
			p.id, _ = r["id"].(string)
			p.held = r
			items = append(items, p)
		}
	}
	// A value still held now is a real conflict. A record left with only its key has nothing
	// to write.
	if items, _, err = t.guardUnique(ctx, b, items, true, s); err != nil {
		return err
	}
	items = slices.DeleteFunc(items, func(p pending) bool { return len(p.rec) <= 1 })
	for _, p := range items {
		p.rec["id"] = p.id
		b.keepSubfields(p.rec, p.held)
	}

	// The records are written already, so their values that moved aren't more records written,
	// and one Twenty rejects is a value left out, not a record skipped.
	var moved stats
	err = t.writeBatch(ctx, b, items, true, &moved)
	for f, n := range moved.invalid {
		s.invalid = addCount(s.invalid, f, n)
	}
	for range moved.rejected {
		s.skip("moved_value_rejected")
	}
	return err
}

// guardUnique keeps a unique value, like a domain or an email, out of a record when another
// record holds it. Twenty's upsert would otherwise match that record, and its create refuses
// the whole batch. A value held only by records whose documents in this chunk change it moves:
// unless final, it is left out now and returned in deferred, to write after those records.
func (t *transactor) guardUnique(ctx context.Context, b *binding, items []pending, final bool, s *stats) (kept, deferred []pending, _ error) {
	byKey := make(map[string]record, len(items))
	for _, p := range items {
		byKey[p.key] = p.rec
	}
	for _, f := range b.unique {
		var values []string
		for _, p := range items {
			if v := uniqueValue(p.rec, f); v != "" {
				values = append(values, v)
			}
		}
		if len(values) == 0 {
			continue
		}
		rows, err := t.client.find(ctx, b.object, uniquePath(f), values, true)
		if err != nil {
			return nil, nil, err
		}
		holders := make(map[string][]string) // value -> keys of the records that hold it ("" if none)
		for _, r := range rows {
			k, _ := r[b.res.IDField].(string)
			holders[uniqueValue(r, f)] = append(holders[uniqueValue(r, f)], k)
		}

		claimed := make(map[string]string) // value -> key of the document in this chunk that writes it
		for _, p := range items {
			v := uniqueValue(p.rec, f)
			others := slices.DeleteFunc(slices.Clone(holders[v]), func(k string) bool { return k == p.key })
			var conflict string
			switch {
			case v == "":
				continue
			case !filterable(v):
				conflict = "can't be checked against other records"
			case claimed[v] != "" && claimed[v] != p.key:
				conflict = "is also in another document with key " + claimed[v]
			case !final && len(others) > 0 && !slices.ContainsFunc(others, func(k string) bool { return !givesUp(byKey[k], f, v) }):
				claimed[v] = p.key
				// One write per record, even if more than one of its values moves.
				if i := slices.IndexFunc(deferred, func(d pending) bool { return d.key == p.key }); i >= 0 {
					maps.Copy(deferred[i].rec, uniqueOnly(b, p.key, f, v))
				} else {
					deferred = append(deferred, pending{key: p.key, rec: uniqueOnly(b, p.key, f, v)})
				}
				dropUnique(p.rec, f)
				continue
			case len(others) > 0:
				conflict = "belongs to another record"
			default:
				claimed[v] = p.key
				continue
			}
			if b.res.uniqueConflict() == conflictFail {
				return nil, nil, fmt.Errorf("%s: the %s value of the document with key %q %s", b.object, f.Name, p.key, conflict)
			}
			dropUnique(p.rec, f)
			s.skip(f.Name + "_held_by_another_record")
			log.WithFields(log.Fields{"object": b.object, "key": p.key, "field": f.Name, "conflict": conflict}).
				Info("left out a unique value")
		}
	}
	return items, deferred, nil
}

// givesUp reports whether a document's record sets a unique field to a value other than v. A
// missing record, like a deletion's, or one that leaves the field out keeps the value.
func givesUp(rec record, f fieldMeta, v string) bool {
	if rec == nil {
		return false
	}
	val, set := rec[f.Name]
	if obj, ok := val.(map[string]any); ok && f.Type != "TEXT" {
		_, set = obj[primarySubfield[f.Type]]
	}
	return set && uniqueValue(rec, f) != v
}

// uniqueOnly is a record that writes only a unique field's value to a key's record.
func uniqueOnly(b *binding, key string, f fieldMeta, v string) record {
	if f.Type == "TEXT" {
		return record{b.res.IDField: key, f.Name: v}
	}
	return record{b.res.IDField: key, f.Name: map[string]any{primarySubfield[f.Type]: v}}
}

// mergeUnder copies into rec the fields and composite subfields that only earlier set. Rich
// text subfields don't merge: Twenty replaces a rich text field's whole value on any write.
func (b *binding) mergeUnder(rec, earlier record) {
	for k, old := range earlier {
		cur, ok := rec[k]
		if !ok {
			rec[k] = old
			continue
		}
		curObj, ok1 := cur.(map[string]any)
		oldObj, ok2 := old.(map[string]any)
		whole := slices.ContainsFunc(b.fields, func(m mappedField) bool { return m.target.field == k && m.meta.Type == "RICH_TEXT" })
		if ok1 && ok2 && !whole {
			for sk, sv := range oldObj {
				if _, set := curObj[sk]; !set {
					curObj[sk] = sv
				}
			}
		}
	}
}

// recheck looks up the keys of refused creates again. A key that now has a record, written by
// someone else or by a retry of this create whose answer was lost, moves to the updates.
func (t *transactor) recheck(ctx context.Context, b *binding, items []pending, s *stats) (moved, rest []pending, _ error) {
	existing, err := t.lookup(ctx, b, items)
	if err != nil {
		return nil, nil, err
	}
	for _, p := range items {
		r, ok := existing[p.key]
		switch {
		case !ok:
			rest = append(rest, p)
		case r["deletedAt"] != nil && !b.res.RestoreDeleted:
			s.skip("deleted_in_twenty")
		default:
			p.id, _ = r["id"].(string)
			p.held = r
			p.rec["id"] = p.id
			b.keepSubfields(p.rec, r)
			moved = append(moved, p)
		}
	}
	return moved, rest, nil
}

// writeBatch writes records in batches. Twenty rejects a whole batch for one bad record, so a
// rejected batch is split until each bad record is alone.
func (t *transactor) writeBatch(ctx context.Context, b *binding, items []pending, update bool, s *stats) error {
	if len(items) > maxBatch {
		for start := 0; start < len(items); start += maxBatch {
			if err := t.writeBatch(ctx, b, items[start:min(start+maxBatch, len(items))], update, s); err != nil {
				return err
			}
		}
		return nil
	}
	if len(items) == 0 {
		return nil
	}

	recs := make([]record, len(items))
	for i, p := range items {
		recs[i] = p.rec
	}
	err := t.send(ctx, b, recs, update, s)
	var rej *rejectedError
	switch {
	case err == nil:
		return nil
	case !errors.As(err, &rej):
		return err
	}
	if !update {
		moved, rest, err := t.recheck(ctx, b, items, s)
		if err != nil {
			return err
		}
		if len(rest) < len(items) {
			if err := t.writeBatch(ctx, b, moved, true, s); err != nil {
				return err
			}
			return t.writeBatch(ctx, b, rest, false, s)
		}
	}
	// Twenty names an invalid phone number, so with rejected_records "skip" it is left out of
	// every record that has it and the batch is sent again, rather than split to find them.
	// Only for creates: an update may lean on country codes Twenty already holds.
	if !update && len(items) > 1 && b.res.rejectedRecords() == rejectedSkip && leaveOutPhone(b, rej, items, s) {
		return t.writeBatch(ctx, b, items, update, s)
	}
	if len(items) > 1 {
		mid := len(items) / 2
		if err := t.writeBatch(ctx, b, items[:mid], update, s); err != nil {
			return err
		}
		return t.writeBatch(ctx, b, items[mid:], update, s)
	}

	// One record, rejected. With rejected_records "skip", leave out the email, phone and link
	// values Twenty calls invalid and try again. Each try leaves out at least one, so it ends.
	p := items[0]
	if bad := rejectedFields(rej, p.rec); b.res.rejectedRecords() == rejectedSkip && len(bad) > 0 {
		for _, f := range bad {
			leaveOut(b, p, f, s)
		}
		return t.writeBatch(ctx, b, items, update, s)
	}
	return t.reject(b, s, p.key, redact(rej.Error(), p.rec))
}

// leaveOut drops a field Twenty calls invalid from a record.
func leaveOut(b *binding, p pending, field string, s *stats) {
	delete(p.rec, field)
	s.invalid = addCount(s.invalid, field, 1)
	log.WithFields(log.Fields{"object": b.object, "key": p.key, "field": field}).
		Info("left out a value that Twenty calls invalid")
}

// leaveOutPhone drops the phone field holding the number a rejection names from each record that
// has it, and reports whether it did. The message names only the number, so it does nothing when
// the records holding it differ in country or calling code, or one holds it as an additional
// phone, as one of them may be valid.
func leaveOutPhone(b *binding, rej *rejectedError, items []pending, s *stats) bool {
	if len(rej.raw) != 1 || !strings.HasPrefix(rej.raw[0], badPhone) {
		return false
	}
	number := strings.TrimPrefix(rej.raw[0], badPhone)
	type holder struct {
		p    pending
		name string
	}
	var holders []holder
	codes := make(map[string]bool)
	for _, p := range items {
		for name, v := range p.rec {
			obj, ok := v.(map[string]any)
			// Additional phones come as decoded JSON or, mapped on their own, raw. Any that mention
			// the number turn this off: Twenty may mean that one, and the other copies be valid.
			if extra, has := obj["additionalPhones"]; ok && has && mentions(extra, number) {
				return false
			}
			n := obj["primaryPhoneNumber"]
			if _, isText := n.(string); ok && n != nil && !isText {
				// A number written as JSON gets no default country, and may be spelled another way,
				// like 91234567.0, so it may be the one Twenty means.
				return false
			}
			if ok && n != nil && n == number {
				holders = append(holders, holder{p, name})
				codes[fmt.Sprint(obj["primaryPhoneCountryCode"], "|", obj["primaryPhoneCallingCode"])] = true
			}
		}
	}
	if len(holders) == 0 || len(codes) > 1 {
		return false
	}
	for _, h := range holders {
		leaveOut(b, h.p, h.name, s)
	}
	return true
}

// mentions reports whether a JSON value, decoded or raw, holds the text anywhere. Raw JSON that
// doesn't decode, and any JSON number, which may spell the text another way, count as holding it.
func mentions(v any, text string) bool {
	switch v := v.(type) {
	case nil:
		return false
	case json.RawMessage:
		var decoded any
		if err := decode(v, &decoded); err != nil {
			return true
		}
		return mentions(decoded, text)
	case []any:
		return slices.ContainsFunc(v, func(e any) bool { return mentions(e, text) })
	case map[string]any:
		for _, e := range v {
			if mentions(e, text) {
				return true
			}
		}
		return false
	case string:
		return v == text
	}
	return true
}

func (t *transactor) send(ctx context.Context, b *binding, recs []record, update bool, s *stats) error {
	if update {
		if _, err := t.client.upsert(ctx, b.object, recs); err != nil {
			return err
		}
		s.updated += len(recs)
		return nil
	}
	if _, err := t.client.create(ctx, b.object, recs); err != nil {
		return err
	}
	s.created += len(recs)
	return nil
}

// linkRejectedError is a link Twenty rejected under rejected_records fail.
type linkRejectedError struct{ error }

// reject handles a record that can't be written. It stops the task, unless the binding's
// rejected_records is "skip": then it logs the key and reason and goes on. Store stops the task
// after the transaction if it skipped too many.
func (t *transactor) reject(b *binding, s *stats, key, reason string) error {
	if b.res.rejectedRecords() == rejectedFail && s.linking {
		return &linkRejectedError{fmt.Errorf("%s: can't link the record with key %q: %s", b.object, key, reason)}
	} else if b.res.rejectedRecords() == rejectedFail {
		return fmt.Errorf("%s: can't write the document with key %q: %s", b.object, key, reason)
	}
	s.rejected++
	s.lastRejected = fmt.Sprintf("with key %q: %s", key, reason)
	if s.rejectedKeys == nil {
		s.rejectedKeys = make(map[string]bool)
	}
	s.rejectedKeys[key] = true
	if s.linking {
		log.WithFields(log.Fields{"object": b.object, "key": key, "reason": reason}).Warn("left out the links of a record")
	} else {
		log.WithFields(log.Fields{"object": b.object, "key": key, "reason": reason}).Warn("skipped a record")
	}
	return nil
}

const (
	badPhone = "Provided phone number is invalid "
	badURL   = "The URL of the link is not valid"
)

var (
	fieldAtEnd = regexp.MustCompile(`field "(\w+)\.\w+"\z`)
	email      = regexp.MustCompile(`[^\s@"']+@[^\s@"']+`)
)

// rejectedFields returns the email, phone and link fields of a record that Twenty's rejection
// names (v2.52 wording). The messages repeat the rejected value, so only Twenty's own text is
// read: the field that ends an email or domain message, or the number that ends a phone one.
func rejectedFields(rej *rejectedError, rec record) []string {
	if len(rej.raw) != 1 {
		return nil
	}
	msg := rej.raw[0]
	has := func(name, sub string) bool {
		obj, ok := rec[name].(map[string]any)
		if !ok {
			return false
		}
		_, ok = obj[sub]
		return ok
	}

	var named []string
	switch {
	case strings.HasPrefix(msg, badPhone):
		// The message names the number, not the field.
		number := strings.TrimPrefix(msg, badPhone)
		var phones []string
		for name, v := range rec {
			if has(name, "primaryPhoneNumber") {
				phones = append(phones, name)
				if fmt.Sprint(v.(map[string]any)["primaryPhoneNumber"]) == number {
					named = append(named, name)
				}
			}
		}
		if len(named) == 0 {
			named = phones
		}
	case msg == badURL:
		// The message names neither, so every link goes.
		for name := range rec {
			if has(name, "primaryLinkUrl") {
				named = append(named, name)
			}
		}
	default:
		if m := fieldAtEnd.FindStringSubmatch(msg); m != nil &&
			(has(m[1], "primaryEmail") || has(m[1], "primaryPhoneNumber") || has(m[1], "primaryLinkUrl")) {
			named = []string{m[1]}
		}
	}
	slices.Sort(named)
	return named
}

// redact takes phone numbers and emails out of a rejection before it is logged, since Twenty's
// messages repeat the values they reject.
func redact(reason string, rec record) string {
	mask := func(n any) {
		// A shorter value isn't a phone number, and could match anything.
		if number := strings.TrimSpace(fmt.Sprint(n)); n != nil && len(number) >= 5 {
			reason = strings.ReplaceAll(reason, number, "<phone>")
		}
	}
	for _, v := range rec {
		obj, _ := v.(map[string]any)
		if n, ok := obj["primaryPhoneNumber"]; ok {
			mask(n)
		}
		more, _ := obj["additionalPhones"].([]any)
		for _, p := range more {
			if p, ok := p.(map[string]any); ok {
				mask(p["number"])
			}
		}
	}
	reason = email.ReplaceAllString(reason, "<email>")
	if len(reason) > 300 {
		reason = strings.ToValidUTF8(reason[:300], "")
	}
	return reason
}

// isDeletion reports whether a stored document deletes its key. In delta updates the runtime
// never sets the Delete flag, which implies the document existed (flowctl preview, Oct 2026), so
// a deletion shows only as the document's _meta/op "d".
func isDeletion(flag bool, doc json.RawMessage) (bool, error) {
	if flag {
		return true, nil
	}
	var d struct {
		Meta struct {
			Op string `json:"op"`
		} `json:"_meta"`
	}
	if err := json.Unmarshal(doc, &d); err != nil {
		return false, err
	}
	return d.Meta.Op == "d", nil
}
