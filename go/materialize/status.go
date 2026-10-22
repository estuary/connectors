package materialize

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

const (
	// A stage is reported once it has been the current stage this long, so a
	// fast task doesn't report every stage of every transaction.
	statusDwell = time.Minute
	// A reported stage is reported again this often, with its updated counts
	// and elapsed time.
	statusRefresh = time.Minute
	// A task whose stages are all shorter than statusDwell reports a rollup of
	// the transactions it completed over this window instead.
	statusRollupWindow = 5 * time.Minute
	statusTick         = time.Second
)

// statusStage is a step of transaction processing reported as the connector
// status.
type statusStage int

const (
	stageIdle statusStage = iota
	stageStarting
	stageRecovery
	stageWaitingForData
	stageReading
	stageLoading
	stageStaging
	stageCommitting
	stageSyncWait
	stageRollup
)

// statusKey identifies a reported status: a stage of a round.
type statusKey struct {
	stage statusStage
	round int
}

// statusCounts counts the documents of a stage and the collections they are
// from.
type statusCounts struct {
	docs        int
	seen        []bool
	collections int
}

type statusCompletion struct {
	at   time.Time
	docs int
}

// statusTracker keeps the connector status current with the transaction stage
// the materialization is in. Two stages can be in progress at once: the commit
// of a round overlaps the reading of the next one, and the commit is reported
// while it runs. Warnings from BindingEvents.Warn replace progress statuses
// for the rest of the session.
type statusTracker struct {
	now  func() time.Time
	emit func(lvl log.Level, msg string)
	// Serializes emits, so a progress status rendered before a warning is
	// never emitted after it.
	emitMu sync.Mutex

	mu sync.Mutex
	// Only one shard reports progress, since all shards' statuses overwrite
	// the same one.
	progress     bool
	collectionOf []int
	nCollections int

	round        int
	recovery     bool
	stagingDone  bool
	backlog      bool
	load         statusKey
	commit       statusKey
	loads        statusCounts
	stores       statusCounts
	committing   statusCounts
	waitingSince time.Time
	syncAt       time.Time
	syncEvery    time.Duration
	completions  []statusCompletion

	shown      statusKey
	shownSince time.Time
	emitted    statusKey
	lastMsg    string
	lastEmit   time.Time

	warningKeys map[string]struct{}
	warnings    []string

	stop func(func())
}

func newStatusTracker(open *pm.Request_Open) *statusTracker {
	t := &statusTracker{
		now: time.Now,
		emit: func(lvl log.Level, msg string) {
			log.WithField("eventType", "connectorStatus").Log(lvl, msg)
		},
		progress:    open.Range == nil || (open.Range.KeyBegin == 0 && open.Range.RClockBegin == 0),
		recovery:    true,
		warningKeys: make(map[string]struct{}),
	}
	if open.Materialization != nil {
		index := make(map[string]int)
		for _, b := range open.Materialization.Bindings {
			name := b.Collection.Name.String()
			if _, ok := index[name]; !ok {
				index[name] = len(index)
			}
			t.collectionOf = append(t.collectionOf, index[name])
		}
		t.nCollections = len(index)
	}
	t.commit = statusKey{stage: stageStarting}
	t.load = statusKey{stage: stageIdle}
	t.shown = t.commit
	t.shownSince = t.now()
	t.emitted = statusKey{stage: stageIdle, round: -1}
	return t
}

// start reports the status right away and then keeps it current until the
// returned function is called.
func (t *statusTracker) start() (stop func()) {
	t.tick()
	stopRepeat := repeatAsync(t.tick, statusTick)
	return func() { stopRepeat(func() {}) }
}

func (c *statusCounts) reset(n int) {
	*c = statusCounts{seen: make([]bool, n)}
}

func (t *statusTracker) count(c *statusCounts, binding int) {
	c.docs++
	if binding < 0 || binding >= len(t.collectionOf) {
		return
	}
	if col := t.collectionOf[binding]; !c.seen[col] {
		c.seen[col] = true
		c.collections++
	}
}

// event updates the stages for a transactions event. `binding` is the binding
// of a Load or Store request.
func (t *statusTracker) event(ev transactionsEvent, binding int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	switch ev {
	case readLoad:
		if t.load.stage != stageReading {
			t.load = statusKey{stageReading, t.round}
			t.loads.reset(t.nCollections)
		}
		t.count(&t.loads, binding)
	case readFlush:
		if t.load.stage == stageReading {
			t.load = statusKey{stageLoading, t.round}
		} else {
			t.load = statusKey{stageStaging, t.round}
			t.stores.reset(t.nCollections)
		}
	case sentFlushed:
		if t.load.stage != stageStaging {
			t.load = statusKey{stageStaging, t.round}
			t.stores.reset(t.nCollections)
		}
	case readStore:
		t.count(&t.stores, binding)
	case readStartCommit:
		t.stagingDone = true
	case sentStartedCommit:
		t.committing = t.stores
		t.commit = statusKey{stageCommitting, t.round}
		t.load = statusKey{stageIdle, t.round + 1}
		t.stagingDone = false
		t.round++
	case readAcknowledge:
		if t.recovery {
			t.commit = statusKey{stage: stageRecovery}
		}
	case sentAcknowledged:
		// Completions are trimmed only while progress is reported.
		if !t.recovery && t.progress && len(t.warnings) == 0 {
			t.completions = append(t.completions, statusCompletion{at: t.now(), docs: t.committing.docs})
		}
		t.recovery = false
		t.commit = statusKey{stage: stageIdle}
		if t.load.stage == stageIdle {
			t.load = statusKey{stageWaitingForData, t.round}
			t.waitingSince = t.now()
		}
	}
	t.updateShown()
}

// syncWait reports the wait for the next scheduled sync. `every` is zero when
// the schedule has no fixed interval.
func (t *statusTracker) syncWait(at time.Time, every time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.commit = statusKey{stageSyncWait, t.round - 1}
	t.syncAt, t.syncEvery = at, every
	t.updateShown()
}

// setBacklog sets whether recent transactions were large enough to skip the
// sync delay.
func (t *statusTracker) setBacklog(backlog bool) {
	t.mu.Lock()
	t.backlog = backlog
	t.mu.Unlock()
}

func (t *statusTracker) updateShown() {
	shown := t.commit
	if shown.stage == stageIdle {
		shown = t.load
	}
	if shown != t.shown {
		t.shown = shown
		t.shownSince = t.now()
	}
}

// warn adds a warning to the status, which then reports every warning instead
// of progress. Warnings are deduplicated by key.
func (t *statusTracker) warn(key, msg string) {
	t.emitMu.Lock()
	defer t.emitMu.Unlock()

	t.mu.Lock()
	if _, ok := t.warningKeys[key]; ok {
		t.mu.Unlock()
		return
	}
	t.warningKeys[key] = struct{}{}
	t.warnings = append(t.warnings, msg)
	t.completions = nil
	msg = strings.Join(t.warnings, " ")
	t.lastMsg, t.lastEmit = msg, t.now()
	t.mu.Unlock()

	t.emit(log.WarnLevel, msg)
}

func (t *statusTracker) tick() {
	t.emitMu.Lock()
	defer t.emitMu.Unlock()

	t.mu.Lock()
	msg := t.next(t.now())
	t.mu.Unlock()

	if msg != "" {
		t.emit(log.InfoLevel, msg)
	}
}

// next is the status to report now, or "" if the reported one is current.
func (t *statusTracker) next(now time.Time) string {
	if len(t.warnings) > 0 || !t.progress {
		return ""
	}

	for len(t.completions) > 0 && now.Sub(t.completions[0].at) > statusRollupWindow {
		t.completions = t.completions[1:]
	}

	var key statusKey
	var msg string
	switch immediate := t.shown.stage == stageStarting || t.shown.stage == stageRecovery || t.shown.stage == stageSyncWait; {
	case immediate || now.Sub(t.shownSince) >= statusDwell:
		if t.shown == t.emitted && now.Sub(t.lastEmit) < statusRefresh {
			return ""
		}
		key, msg = t.shown, t.render(now)
	case now.Sub(t.lastEmit) >= statusRollupWindow && len(t.completions) > 0:
		var docs int
		for _, c := range t.completions {
			docs += c.docs
		}
		key = statusKey{stage: stageRollup}
		msg = fmt.Sprintf("Streaming changes (%s, %s in the last %d minutes)",
			plural(docs, "document"), plural(len(t.completions), "transaction"), int(statusRollupWindow/time.Minute))
	default:
		return ""
	}

	if msg == t.lastMsg {
		return ""
	}
	t.emitted, t.lastMsg, t.lastEmit = key, msg, now
	return msg
}

func (t *statusTracker) render(now time.Time) string {
	var msg string
	switch r := t.shown.round; t.shown.stage {
	case stageStarting:
		return "Starting up: connecting to the destination"
	case stageRecovery:
		msg = "Recovery commit: finishing the transaction that was in progress when the task last stopped"
	case stageWaitingForData:
		return "Waiting, no new collection data since " + formatTime(t.waitingSince, now)
	case stageSyncWait:
		msg = "Waiting, next scheduled sync at " + formatTime(t.syncAt, now)
		if t.syncEvery > 0 {
			msg += " (every " + formatDuration(t.syncEvery) + ")"
		}
		return msg
	case stageReading:
		msg = fmt.Sprintf("Reading new data (round %d)", r)
	case stageLoading:
		msg = fmt.Sprintf("Loading existing rows (%s, round %d)", t.describe(t.loads, ""), r)
	case stageStaging:
		var soFar = " so far"
		if t.stagingDone {
			soFar = ""
		}
		msg = fmt.Sprintf("Staging changes at destination (%s, round %d)", t.describe(t.stores, soFar), r)
	case stageCommitting:
		msg = fmt.Sprintf("Committing changes (%s, round %d)", t.describe(t.committing, ""), r)
	default:
		return ""
	}

	if t.backlog && t.shown.stage != stageRecovery {
		msg = "Backlog catchup: " + msg
	}
	if d := now.Sub(t.shownSince); d >= statusRefresh {
		msg += ", running for " + formatDuration(d.Truncate(time.Minute))
	}
	return msg
}

func (t *statusTracker) describe(c statusCounts, soFar string) string {
	return fmt.Sprintf("%s%s from %s", plural(c.docs, "document"), soFar, plural(c.collections, "collection"))
}

func plural(n int, noun string) string {
	if n != 1 {
		noun += "s"
	}
	return formatCount(n) + " " + noun
}

// formatCount formats n like "120,000" or, from a million, "1.2M".
func formatCount(n int) string {
	switch {
	case n >= 1_000_000_000:
		return trimZero(float64(n)/1e9) + "B"
	case n >= 1_000_000:
		return trimZero(float64(n)/1e6) + "M"
	}
	s := strconv.Itoa(n)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

func trimZero(f float64) string {
	return strings.TrimSuffix(strconv.FormatFloat(f, 'f', 1, 64), ".0")
}

// formatDuration formats d like "2m30s" or "1h", without zero units.
func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h, m, s := int(d/time.Hour), int(d%time.Hour/time.Minute), int(d%time.Minute/time.Second)
	var out string
	if h > 0 {
		out += strconv.Itoa(h) + "h"
	}
	if m > 0 {
		out += strconv.Itoa(m) + "m"
	}
	if s > 0 || out == "" {
		out += strconv.Itoa(s) + "s"
	}
	return out
}

// formatTime formats ts in UTC, with its date when that isn't the date of now.
func formatTime(ts, now time.Time) string {
	ts, now = ts.UTC(), now.UTC()
	if ts.YearDay() == now.YearDay() && ts.Year() == now.Year() {
		return ts.Format("15:04:05 UTC")
	}
	return ts.Format("2006-01-02 15:04:05 UTC")
}
