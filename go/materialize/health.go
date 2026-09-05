package materialize

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

// healthFlushInterval bounds how often healthy and unchecked rounds are logged:
// a judged round is logged at once when nothing has been for this long, and
// otherwise rolls up into the next line.
var healthFlushInterval = 5 * time.Minute

// Fidelity describes how much of a binding's commit result a connector was
// able to report, and so how strong a check the boilerplate can make.
type Fidelity string

const (
	// FidelityNone means the connector reported no usable result. The round
	// is logged as unchecked.
	FidelityNone Fidelity = "none"
	// FidelityTotal means only the total number of affected rows is known.
	// It is checked against expected inserts + updates + deletes.
	FidelityTotal Fidelity = "total"
	// FidelityExact means inserted, updated and deleted counts are each
	// known and are checked individually.
	FidelityExact Fidelity = "exact"
)

func (f Fidelity) rank() int {
	switch f {
	case FidelityTotal:
		return 1
	case FidelityExact:
		return 2
	default:
		return 0
	}
}

func (f Fidelity) orNone() Fidelity {
	if f == "" {
		return FidelityNone
	}
	return f
}

func minFidelity(a, b Fidelity) Fidelity {
	if a == "" {
		return b
	} else if b == "" {
		return a
	} else if b.rank() < a.rank() {
		return b
	}
	return a
}

// RowStats is what the destination reports having done for one binding's
// stored documents of one transaction round. Construct it with ExactRowStats
// or TotalRowStats; a zero RowStats reports FidelityNone.
type RowStats struct {
	Fidelity Fidelity
	// Inserted, Updated and Deleted are the destination's own breakdown.
	// Only meaningful at FidelityExact.
	Inserted, Updated, Deleted int64
	// Total is the number of rows affected. At FidelityExact it is the
	// destination's independently reported total when it has one, and
	// otherwise the sum of the breakdown.
	Total int64
	// Staged is the number of rows the connector wrote to its staging area,
	// when it stages. Checked against the number of documents stored.
	Staged *int64
	// Loaded is the number of staged rows the destination loaded, when the
	// connector can observe it. Checked against Staged.
	Loaded *int64
}

// ExactRowStats reports a full breakdown of affected rows.
func ExactRowStats(inserted, updated, deleted int64) RowStats {
	return RowStats{
		Fidelity: FidelityExact,
		Inserted: inserted,
		Updated:  updated,
		Deleted:  deleted,
		Total:    inserted + updated + deleted,
	}
}

// TotalRowStats reports only the total number of affected rows.
func TotalRowStats(total int64) RowStats {
	return RowStats{Fidelity: FidelityTotal, Total: total}
}

// WithTotal sets an independently reported total, which is cross-checked
// against the breakdown at FidelityExact.
func (s RowStats) WithTotal(total int64) RowStats {
	s.Total = total
	return s
}

// WithStaged sets the number of rows the connector staged.
func (s RowStats) WithStaged(n int64) RowStats {
	s.Staged = &n
	return s
}

// WithLoaded sets the number of staged rows the destination loaded.
func (s RowStats) WithLoaded(n int64) RowStats {
	s.Loaded = &n
	return s
}

func (s RowStats) add(o RowStats) RowStats {
	s.Fidelity = minFidelity(s.Fidelity.orNone(), o.Fidelity.orNone())
	s.Inserted += o.Inserted
	s.Updated += o.Updated
	s.Deleted += o.Deleted
	s.Total += o.Total
	s.Staged = addOptional(s.Staged, o.Staged)
	s.Loaded = addOptional(s.Loaded, o.Loaded)
	return s
}

func addOptional(a, b *int64) *int64 {
	if a == nil {
		return b
	} else if b == nil {
		return a
	}
	sum := *a + *b
	return &sum
}

// expectedCounts classifies the documents the runtime asked a binding to
// store in one round by the effect they should have on the destination.
type expectedCounts struct {
	insert, update, delete int64
	// softDeleted counts deletions that fold into insert or update because
	// hard deletes are off. It is a visibility bucket only.
	softDeleted int64
	// skipped counts hard deletes of documents that don't exist, which the
	// StoreIterator never hands to the connector.
	skipped int64
}

// stores is the number of documents actually handed to the connector.
func (e expectedCounts) stores() int64 { return e.insert + e.update + e.delete }

func (e expectedCounts) add(o expectedCounts) expectedCounts {
	e.insert += o.insert
	e.update += o.update
	e.delete += o.delete
	e.softDeleted += o.softDeleted
	e.skipped += o.skipped
	return e
}

func (e *expectedCounts) observe(exists, deleted, hardDelete bool) {
	switch {
	case !deleted && !exists:
		e.insert++
	case !deleted && exists:
		e.update++
	case exists && hardDelete:
		e.delete++
	case exists:
		e.update++
		e.softDeleted++
	case hardDelete:
		e.skipped++
	default:
		e.insert++
		e.softDeleted++
	}
}

type healthMismatch struct {
	Check        string   `json:"check"`
	ResourcePath []string `json:"resourcePath,omitempty"`
	Expected     int64    `json:"expected"`
	Actual       int64    `json:"actual"`
}

// healthRound is one transaction round awaiting evaluation.
type healthRound struct {
	round            int
	expectedRecorded bool
	acknowledged     bool
	loadRequests     int64
	loaded           int64
	expected         map[string]expectedCounts
	actual           map[string]RowStats
}

func newHealthRound(round int) *healthRound {
	return &healthRound{
		round:    round,
		expected: make(map[string]expectedCounts),
		actual:   make(map[string]RowStats),
	}
}

// complete is true once the expected side is recorded and every binding
// that stored documents has reported.
func (r *healthRound) complete() bool {
	if !r.expectedRecorded {
		return false
	}
	for key, e := range r.expected {
		if _, ok := r.actual[key]; e.stores() > 0 && !ok {
			return false
		}
	}
	return true
}

// healthWindow accumulates evaluated rounds sharing a verdict.
type healthWindow struct {
	rounds                int
	seen                  map[int]struct{} // Rounds counted so far.
	firstRound, lastRound int
	bindings              map[string]struct{}
	loadRequests, loaded  int64
	expected              expectedCounts
	actual                RowStats
	pending               int
	mismatches            []healthMismatch
	fidelity              Fidelity
}

func newHealthWindow() *healthWindow {
	return &healthWindow{bindings: make(map[string]struct{}), seen: make(map[int]struct{})}
}

func (w *healthWindow) addRound(round int) {
	if _, ok := w.seen[round]; ok {
		return
	}
	w.seen[round] = struct{}{}
	if w.rounds == 0 || round < w.firstRound {
		w.firstRound = round
	}
	if w.rounds == 0 || round > w.lastRound {
		w.lastRound = round
	}
	w.rounds++
}

func (w *healthWindow) addExpected(key string, e expectedCounts) {
	w.bindings[key] = struct{}{}
	w.expected = w.expected.add(e)
}

func (w *healthWindow) addActual(key string, a RowStats) {
	w.bindings[key] = struct{}{}
	w.fidelity = minFidelity(w.fidelity, a.Fidelity.orNone())
	w.actual = w.actual.add(a)
}

const (
	verdictOK        = "ok"
	verdictMismatch  = "mismatch"
	verdictUnchecked = "unchecked"
)

// healthTracker reconciles what the runtime asked the connector to store
// against what the destination reports it did, per binding and round, and
// logs the result. It is observation only: nothing here can fail a
// transaction.
type healthTracker struct {
	log      func(log.Fields, string)
	taskName string
	keys     []string            // Binding index => binding key.
	paths    map[string][]string // Binding key => resource path.
	sharded  bool

	mu       sync.Mutex
	closed   bool
	recovery bool
	// rounds are open rounds awaiting evaluation, by round number.
	rounds map[int]*healthRound
	// recoveryWindow gathers actuals reported during the recovery commit.
	recoveryWindow *healthWindow
	// shardedWindow gathers both sides under a multi-shard range, where
	// they cannot be paired.
	shardedWindow *healthWindow
	// okWindow and uncheckedWindow roll up evaluated rounds by verdict.
	okWindow, uncheckedWindow *healthWindow
	// lastEmit is when a line was last logged, gating the rollup.
	lastEmit time.Time
	// maxAcked is the highest acknowledged round; a report for a round at or
	// below it that is no longer open arrived after the round was judged.
	maxAcked int

	stopFlusher func(func())
}

func bindingKey(path []string) string { return strings.Join(path, "\x00") }

func newHealthTracker(open *pm.Request_Open) *healthTracker {
	h := &healthTracker{
		log: func(fields log.Fields, msg string) {
			log.WithFields(fields).Info(msg)
		},
		paths:           make(map[string][]string),
		recovery:        true,
		maxAcked:        -1,
		rounds:          make(map[int]*healthRound),
		recoveryWindow:  newHealthWindow(),
		shardedWindow:   newHealthWindow(),
		okWindow:        newHealthWindow(),
		uncheckedWindow: newHealthWindow(),
	}
	if open.Materialization != nil {
		h.taskName = open.Materialization.Name.String()
		for _, b := range open.Materialization.Bindings {
			key := bindingKey(b.ResourcePath)
			h.keys = append(h.keys, key)
			h.paths[key] = b.ResourcePath
		}
	}
	if r := open.Range; r != nil && (r.KeyBegin != 0 || r.KeyEnd != math.MaxUint32) {
		h.sharded = true
	}
	return h
}

// start begins the periodic window flush.
func (h *healthTracker) start() {
	h.stopFlusher = repeatAsync(func() {
		h.mu.Lock()
		due := h.intervalElapsed()
		h.mu.Unlock()
		if due {
			h.flush(false)
		}
	}, healthFlushInterval)
}

// intervalElapsed is whether enough time has passed since the last logged
// line for another to be logged. Callers hold h.mu.
func (h *healthTracker) intervalElapsed() bool {
	return time.Since(h.lastEmit) >= healthFlushInterval
}

// maybeFlushWindows logs the accumulated windows right away when they hold
// something worth logging and the interval since the last line has elapsed.
// Callers hold h.mu.
func (h *healthTracker) maybeFlushWindows() {
	if !h.intervalElapsed() {
		return
	}
	for _, w := range []*healthWindow{h.okWindow, h.uncheckedWindow, h.shardedWindow} {
		if len(w.bindings) > 0 {
			h.flushWindows()
			return
		}
	}
}

// close stops the periodic flush and flushes whatever can be judged. Rounds
// whose commit was never acknowledged in this session are discarded rather
// than reported as pending: recovery will re-apply them.
func (h *healthTracker) close() {
	defer recoverHealthPanic()
	if h.stopFlusher != nil {
		h.stopFlusher(func() {})
	}
	h.flush(true)
	h.mu.Lock()
	h.closed = true
	h.mu.Unlock()
}

func recoverHealthPanic() {
	if r := recover(); r != nil {
		log.WithField("panic", r).Warn("transaction health check failed")
	}
}

// observeStore classifies one Store request as the StoreIterator reads it.
func (h *healthTracker) observeStore(round, binding int, exists, deleted, hardDelete bool) {
	defer recoverHealthPanic()
	h.mu.Lock()
	defer h.mu.Unlock()

	r := h.openRound(round)
	key := fmt.Sprintf("binding %d", binding)
	if binding >= 0 && binding < len(h.keys) {
		key = h.keys[binding]
	}
	e := r.expected[key]
	e.observe(exists, deleted, hardDelete)
	r.expected[key] = e
}

// recordExpected closes the expected side of a round, once Store has read
// every document.
func (h *healthTracker) recordExpected(round int, loadRequests, loaded int64) {
	defer recoverHealthPanic()
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.sharded {
		r := h.openRound(round)
		delete(h.rounds, round)
		w := h.shardedWindow
		w.addRound(round)
		w.loadRequests += loadRequests
		w.loaded += loaded
		for key, e := range r.expected {
			w.addExpected(key, e)
		}
		h.maybeFlushWindows()
		return
	}

	r := h.openRound(round)
	r.expectedRecorded = true
	r.loadRequests = loadRequests
	r.loaded = loaded
}

// report records the destination's result for one binding of one round.
// Repeated reports for the same binding and round are summed.
func (h *healthTracker) report(round int, path []string, stats RowStats) {
	defer recoverHealthPanic()
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return
	}
	key := bindingKey(path)
	if _, known := h.paths[key]; !known {
		h.paths[key] = path
	}

	if h.recovery {
		w := h.recoveryWindow
		w.addRound(round)
		w.addActual(key, stats)
		return
	} else if h.sharded {
		h.shardedWindow.addActual(key, stats)
		h.maybeFlushWindows()
		return
	}

	r, open := h.rounds[round]
	if !open && round <= h.maxAcked {
		// The round was already judged (and logged as pending); the late
		// result is still worth logging, as unchecked.
		w := newHealthWindow()
		w.addRound(round)
		w.addActual(key, stats)
		h.mergeWindow(h.uncheckedWindow, w)
		h.maybeFlushWindows()
		return
	} else if !open {
		r = h.openRound(round)
	}
	if prior, ok := r.actual[key]; ok {
		stats = prior.add(stats)
	} else {
		stats.Fidelity = stats.Fidelity.orNone()
	}
	r.actual[key] = stats

	if r.acknowledged && r.complete() {
		h.evaluate(r)
	}
}

// endRecovery marks the recovery commit as acknowledged, emitting an
// actual-only line for anything reported during it.
func (h *healthTracker) endRecovery() {
	defer recoverHealthPanic()
	h.mu.Lock()
	defer h.mu.Unlock()

	h.recovery = false
	if h.recoveryWindow.rounds > 0 {
		h.emit(h.recoveryWindow, verdictUnchecked, log.Fields{"recovery": true})
		h.recoveryWindow = newHealthWindow()
	}
}

// acknowledged marks a round's commit as complete. A round is judged no
// earlier than this, so that every report made during the commit has landed.
func (h *healthTracker) acknowledged(round int) {
	defer recoverHealthPanic()
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.sharded {
		return
	}
	h.maxAcked = max(h.maxAcked, round)
	r := h.openRound(round)
	r.acknowledged = true
	if r.complete() {
		h.evaluate(r)
	}
}

func (h *healthTracker) openRound(round int) *healthRound {
	r, ok := h.rounds[round]
	if !ok {
		r = newHealthRound(round)
		h.rounds[round] = r
	}
	return r
}

// evaluate judges a round, removes it from the open set, and either
// accumulates it into a window or, on mismatch, logs it immediately.
// Callers hold h.mu.
func (h *healthTracker) evaluate(r *healthRound) {
	delete(h.rounds, r.round)

	w := newHealthWindow()
	w.addRound(r.round)
	w.loadRequests, w.loaded = r.loadRequests, r.loaded

	keys := make(map[string]struct{})
	for key := range r.expected {
		keys[key] = struct{}{}
	}
	for key := range r.actual {
		keys[key] = struct{}{}
	}
	sorted := make([]string, 0, len(keys))
	for key := range keys {
		sorted = append(sorted, key)
	}
	sort.Strings(sorted)

	for _, key := range sorted {
		e := r.expected[key]
		a, reported := r.actual[key]
		path := h.paths[key]

		if e.stores() > 0 || e.skipped > 0 {
			w.addExpected(key, e)
		}
		if !reported {
			if e.stores() > 0 {
				w.pending++
			}
			continue
		}
		w.addActual(key, a)

		check := func(name string, expected, actual int64) {
			if expected != actual {
				w.mismatches = append(w.mismatches, healthMismatch{
					Check: name, ResourcePath: path, Expected: expected, Actual: actual,
				})
			}
		}

		switch a.Fidelity {
		case FidelityExact:
			check("insert", e.insert, a.Inserted)
			check("update", e.update, a.Updated)
			check("delete", e.delete, a.Deleted)
			check("totalSum", a.Inserted+a.Updated+a.Deleted, a.Total)
		case FidelityTotal:
			check("total", e.stores(), a.Total)
		}
		if a.Staged != nil {
			check("staged", e.stores(), *a.Staged)
		}
		if a.Loaded != nil {
			if a.Staged != nil {
				check("loadedRows", *a.Staged, *a.Loaded)
			} else {
				check("loadedRows", e.stores(), *a.Loaded)
			}
		}
	}

	if r.loaded > r.loadRequests {
		w.mismatches = append(w.mismatches, healthMismatch{
			Check: "loadResponses", Expected: r.loadRequests, Actual: r.loaded,
		})
	}

	switch {
	case len(w.mismatches) > 0:
		h.flushWindows()
		h.emit(w, verdictMismatch, nil)
	case w.pending > 0 || w.fidelity == FidelityNone:
		h.mergeWindow(h.uncheckedWindow, w)
		h.maybeFlushWindows()
	default:
		h.mergeWindow(h.okWindow, w)
		h.maybeFlushWindows()
	}
}

func (h *healthTracker) mergeWindow(into, w *healthWindow) {
	for round := range w.seen {
		into.addRound(round)
	}
	for key := range w.bindings {
		into.bindings[key] = struct{}{}
	}
	into.loadRequests += w.loadRequests
	into.loaded += w.loaded
	into.expected = into.expected.add(w.expected)
	into.actual = into.actual.add(w.actual)
	into.pending += w.pending
	into.fidelity = minFidelity(into.fidelity, w.fidelity)
}

// flush judges every acknowledged round still awaiting reports as
// unchecked, then logs the accumulated windows. With final set, rounds that
// were never acknowledged are dropped.
func (h *healthTracker) flush(final bool) {
	defer recoverHealthPanic()
	h.mu.Lock()
	defer h.mu.Unlock()

	rounds := make([]int, 0, len(h.rounds))
	for round := range h.rounds {
		rounds = append(rounds, round)
	}
	sort.Ints(rounds)
	for _, round := range rounds {
		r := h.rounds[round]
		if r.acknowledged {
			h.evaluate(r)
		} else if final {
			delete(h.rounds, round)
		}
	}
	h.flushWindows()
}

// flushWindows logs and resets the accumulated windows. Callers hold h.mu.
func (h *healthTracker) flushWindows() {
	if h.okWindow.rounds > 0 {
		h.emit(h.okWindow, verdictOK, nil)
		h.okWindow = newHealthWindow()
	}
	if h.uncheckedWindow.rounds > 0 {
		h.emit(h.uncheckedWindow, verdictUnchecked, nil)
		h.uncheckedWindow = newHealthWindow()
	}
	if w := h.shardedWindow; w.rounds > 0 || len(w.bindings) > 0 {
		h.emit(w, verdictUnchecked, log.Fields{"sharded": true})
		h.shardedWindow = newHealthWindow()
	}
}

func (h *healthTracker) emit(w *healthWindow, verdict string, extra log.Fields) {
	actual := log.Fields{
		"inserted": w.actual.Inserted,
		"updated":  w.actual.Updated,
		"deleted":  w.actual.Deleted,
		"total":    w.actual.Total,
	}
	if w.actual.Staged != nil {
		actual["staged"] = *w.actual.Staged
	}
	if w.actual.Loaded != nil {
		actual["loaded"] = *w.actual.Loaded
	}

	fields := log.Fields{
		// observable promotes the line into the data-plane's Grafana stream,
		// where operators alert on commit-path regressions; see the ops
		// protocol README. The line is low-volume and high-signal by design.
		"observable":        true,
		"catalog_task_name": h.taskName,
		"verdict":           verdict,
		"fidelity":          w.fidelity.orNone(),
		"rounds":            w.rounds,
		"firstRound":        w.firstRound,
		"lastRound":         w.lastRound,
		"bindings":          len(w.bindings),
		"loadRequests":      w.loadRequests,
		"loaded":            w.loaded,
		"expected": log.Fields{
			"insert":      w.expected.insert,
			"update":      w.expected.update,
			"delete":      w.expected.delete,
			"softDeleted": w.expected.softDeleted,
			"skipped":     w.expected.skipped,
		},
		"actual": actual,
	}
	if w.pending > 0 {
		fields["pending"] = w.pending
	}
	if len(w.mismatches) > 0 {
		fields["mismatches"] = w.mismatches
	}
	if h.sharded {
		fields["sharded"] = true
	}
	for k, v := range extra {
		fields[k] = v
	}

	if _, recovery := extra["recovery"]; !recovery {
		h.lastEmit = time.Now()
	}
	h.log(fields, "transaction health")
}
