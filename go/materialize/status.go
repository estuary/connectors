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
	statusMinActivePeriod = time.Minute
	// A stage is reported again with its updated counts and elapsed time
	// this often
	statusRefresh = time.Minute
	// A task whose stages are all shorter than statusMinActivePeriod reports a
	// rollup of the transactions it completed over this window instead.
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

// statusCounts counts the documents of a stage and the distinct collections
// they are from.
type statusCounts struct {
	docs        int
	seen        []bool // Indexed by collection.
	collections int
}

func (c *statusCounts) reset(numCollections int) {
	*c = statusCounts{seen: make([]bool, numCollections)}
}

func (c *statusCounts) add(collection int) {
	c.docs++
	if !c.seen[collection] {
		c.seen[collection] = true
		c.collections++
	}
}

type statusCommit struct {
	at   time.Time
	docs int
}

// statusTracker keeps the connector status current with the transaction stage
// the materialization is in, and reports warnings from BindingEvents.Warn in
// place of progress for the rest of the session.
//
// It is safe for concurrent use. RunTransactions' load and acknowledge
// goroutines report events, the transactor reports warnings, and a ticker
// goroutine decides what to report. One mutex guards all state, and is held
// while a status is logged, so statuses are logged in the order they're
// decided and a progress status never follows a warning.
type statusTracker struct {
	mu   sync.Mutex
	now  func() time.Time
	emit func(lvl log.Level, msg string)

	// Fixed at Open. Only one shard reports progress, since every shard's
	// status overwrites the same one; other shards report only warnings.
	reportsProgress   bool
	bindingCollection []int // Collection index of each binding.
	numCollections    int

	// Transaction progress. A round's commit overlaps the reading of the next
	// round, so two stages can be in progress at once, and the commit stage
	// is reported while it runs.
	round          int
	inRecovery     bool
	commitStage    statusKey // Starting, recovery, committing or sync wait.
	readStage      statusKey // Waiting for data, reading, loading or staging.
	loadCounts     statusCounts
	storeCounts    statusCounts
	commitCounts   statusCounts
	storesComplete bool // All of the staging round's Store requests are read.
	backlog        bool
	waitingSince   time.Time
	syncAt         time.Time
	syncEvery      time.Duration
	recentCommits  []statusCommit // Within statusRollupWindow.

	// The stage that would be reported now, and what was last reported.
	current      statusKey
	currentSince time.Time
	reported     statusKey
	reportedMsg  string
	reportedAt   time.Time

	warningKeys map[string]struct{}
	warnings    []string
}

func newStatusTracker(open *pm.Request_Open) *statusTracker {
	var t = &statusTracker{
		now: time.Now,
		emit: func(lvl log.Level, msg string) {
			log.WithField("eventType", "connectorStatus").Log(lvl, msg)
		},
		reportsProgress: open.Range == nil || (open.Range.KeyBegin == 0 && open.Range.RClockBegin == 0),
		inRecovery:      true,
		commitStage:     statusKey{stage: stageStarting},
		readStage:       statusKey{stage: stageIdle},
		reported:        statusKey{stage: stageIdle, round: -1},
		warningKeys:     make(map[string]struct{}),
	}
	if open.Materialization != nil {
		var index = make(map[string]int)
		for _, b := range open.Materialization.Bindings {
			var name = b.Collection.Name.String()
			if _, ok := index[name]; !ok {
				index[name] = len(index)
			}
			t.bindingCollection = append(t.bindingCollection, index[name])
		}
		t.numCollections = len(index)
	}
	t.current, t.currentSince = t.commitStage, t.now()
	return t
}

// start reports the status right away and then keeps it current until the
// returned function is called.
func (t *statusTracker) start() (stop func()) {
	t.tick()
	var stopRepeat = repeatAsync(t.tick, statusTick)
	return func() { stopRepeat(func() {}) }
}

// event updates the stages for a transactions event. `binding` is the binding
// of a Load or Store request.
func (t *statusTracker) event(ev transactionsEvent, binding int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	switch ev {
	case readLoad:
		if t.readStage.stage != stageReading {
			t.readStage = statusKey{stageReading, t.round}
			t.loadCounts.reset(t.numCollections)
		}
		t.loadCounts.add(t.collectionOf(binding))
	case readFlush:
		if t.readStage.stage == stageReading {
			t.readStage = statusKey{stageLoading, t.round}
		} else {
			t.readStage = statusKey{stageStaging, t.round}
			t.storeCounts.reset(t.numCollections)
		}
	case sentFlushed:
		if t.readStage.stage != stageStaging {
			t.readStage = statusKey{stageStaging, t.round}
			t.storeCounts.reset(t.numCollections)
		}
	case readStore:
		t.storeCounts.add(t.collectionOf(binding))
	case readStartCommit:
		t.storesComplete = true
	case sentStartedCommit:
		t.commitCounts = t.storeCounts
		t.commitStage = statusKey{stageCommitting, t.round}
		t.readStage = statusKey{stageIdle, t.round + 1}
		t.storesComplete = false
		t.round++
	case readAcknowledge:
		if t.inRecovery {
			t.commitStage = statusKey{stage: stageRecovery}
		}
	case sentAcknowledged:
		// Commits are trimmed only while progress is reported.
		if !t.inRecovery && t.reportsProgress && len(t.warnings) == 0 {
			t.recentCommits = append(t.recentCommits, statusCommit{at: t.now(), docs: t.commitCounts.docs})
		}
		t.inRecovery = false
		t.commitStage = statusKey{stage: stageIdle}
		if t.readStage.stage == stageIdle {
			t.readStage = statusKey{stageWaitingForData, t.round}
			t.waitingSince = t.now()
		}
	}
	t.updateCurrent()
}

// syncWait reports the wait for the next scheduled sync. `every` is zero when
// the schedule has no fixed interval.
func (t *statusTracker) syncWait(at time.Time, every time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.commitStage = statusKey{stageSyncWait, t.round - 1}
	t.syncAt, t.syncEvery = at, every
	t.updateCurrent()
}

// setBacklog sets whether recent transactions were large enough to skip the
// sync delay.
func (t *statusTracker) setBacklog(backlog bool) {
	t.mu.Lock()
	t.backlog = backlog
	t.mu.Unlock()
}

// warn adds a warning to the status, which then reports every warning instead
// of progress. Warnings are deduplicated by key.
func (t *statusTracker) warn(key, msg string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.warningKeys[key]; ok {
		return
	}
	t.warningKeys[key] = struct{}{}
	t.warnings = append(t.warnings, msg)
	t.recentCommits = nil
	t.reportedMsg, t.reportedAt = strings.Join(t.warnings, " "), t.now()
	t.emit(log.WarnLevel, t.reportedMsg)
}

func (t *statusTracker) tick() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if msg := t.next(t.now()); msg != "" {
		t.emit(log.InfoLevel, msg)
	}
}

// collectionOf is the collection index of a binding. The runtime only sends
// bindings of the materialization, so any other is a bug.
func (t *statusTracker) collectionOf(binding int) int {
	if binding < 0 || binding >= len(t.bindingCollection) {
		panic(fmt.Sprintf("request for binding %d, but the materialization has %d bindings", binding, len(t.bindingCollection)))
	}
	return t.bindingCollection[binding]
}

func (t *statusTracker) updateCurrent() {
	var current = t.commitStage
	if current.stage == stageIdle {
		current = t.readStage
	}
	if current != t.current {
		t.current, t.currentSince = current, t.now()
	}
}

// next is the status to report now, or "" if the reported one is current.
func (t *statusTracker) next(now time.Time) string {
	if len(t.warnings) > 0 || !t.reportsProgress {
		return ""
	}

	for len(t.recentCommits) > 0 && now.Sub(t.recentCommits[0].at) > statusRollupWindow {
		t.recentCommits = t.recentCommits[1:]
	}

	var key statusKey
	var msg string
	switch immediate := t.current.stage == stageStarting || t.current.stage == stageRecovery || t.current.stage == stageSyncWait; {
	case immediate || now.Sub(t.currentSince) >= statusMinActivePeriod:
		if t.current == t.reported && now.Sub(t.reportedAt) < statusRefresh {
			return ""
		}
		key, msg = t.current, t.render(now)
	case now.Sub(t.reportedAt) >= statusRollupWindow && len(t.recentCommits) > 0:
		var docs int
		for _, c := range t.recentCommits {
			docs += c.docs
		}
		key = statusKey{stage: stageRollup}
		msg = fmt.Sprintf("Streaming changes (%s, %s in the last %d minutes)",
			plural(docs, "document"), plural(len(t.recentCommits), "transaction"), int(statusRollupWindow/time.Minute))
	default:
		return ""
	}

	if msg == t.reportedMsg {
		return ""
	}
	t.reported, t.reportedMsg, t.reportedAt = key, msg, now
	return msg
}

func (t *statusTracker) render(now time.Time) string {
	var msg string
	switch r := t.current.round; t.current.stage {
	case stageStarting:
		return "Starting up: connecting to the destination"
	case stageRecovery:
		msg = "Recovery commit: finishing the transaction that was in progress when the task last stopped"
	case stageWaitingForData:
		return "Waiting, no new collection data since " + formatTime(t.waitingSince)
	case stageSyncWait:
		msg = "Waiting, next scheduled sync at " + formatTime(t.syncAt)
		if t.syncEvery > 0 {
			msg += " (every " + formatDuration(t.syncEvery) + ")"
		}
		return msg
	case stageReading:
		msg = fmt.Sprintf("Reading new data (round %d)", r)
	case stageLoading:
		msg = fmt.Sprintf("Loading existing rows (%s, round %d)", describe(t.loadCounts, ""), r)
	case stageStaging:
		var soFar = " so far"
		if t.storesComplete {
			soFar = ""
		}
		msg = fmt.Sprintf("Staging changes at destination (%s, round %d)", describe(t.storeCounts, soFar), r)
	case stageCommitting:
		msg = fmt.Sprintf("Committing changes (%s, round %d)", describe(t.commitCounts, ""), r)
	default:
		return ""
	}

	if t.backlog && t.current.stage != stageRecovery {
		msg = "Backlog catchup: " + msg
	}
	if d := now.Sub(t.currentSince); d >= statusRefresh {
		msg += ", running for " + formatDuration(d.Truncate(time.Minute))
	}
	return msg
}

func describe(c statusCounts, soFar string) string {
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
	var s = strconv.Itoa(n)
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
	var h, m, s = int(d / time.Hour), int(d % time.Hour / time.Minute), int(d % time.Minute / time.Second)
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

// formatTime formats ts in UTC with its date, so a status that stays current
// for days still reads unambiguously.
func formatTime(ts time.Time) string {
	return ts.UTC().Format("2006-01-02 15:04:05 UTC")
}
