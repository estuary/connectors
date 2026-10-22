package materialize

import (
	"testing"
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

type statusHarness struct {
	t   *testing.T
	st  *statusTracker
	now time.Time
	got []string
}

// newStatusHarness tracks a materialization of three bindings, the first two
// sourced from the same collection.
func newStatusHarness(t *testing.T, rng *pf.RangeSpec) *statusHarness {
	open := openRequest([]testBinding{
		{path: []string{"schema", "alpha"}},
		{path: []string{"schema", "beta"}},
		{path: []string{"schema", "gamma"}},
	}, rng)
	open.Materialization.Bindings[0].Collection.Name = "acme/one"
	open.Materialization.Bindings[1].Collection.Name = "acme/one"
	open.Materialization.Bindings[2].Collection.Name = "acme/two"

	h := &statusHarness{t: t, now: time.Date(2026, 10, 22, 11, 0, 0, 0, time.UTC)}
	h.st = newStatusTracker(open)
	h.st.now = func() time.Time { return h.now }
	h.st.currentSince = h.now
	h.st.emit = func(lvl log.Level, msg string) { h.got = append(h.got, lvl.String()+": "+msg) }
	h.st.tick()
	return h
}

// advance moves the clock forward, ticking as the tracker's ticker would.
func (h *statusHarness) advance(d time.Duration) {
	for end := h.now.Add(d); h.now.Before(end); {
		h.now = h.now.Add(statusTick)
		h.st.tick()
	}
}

func (h *statusHarness) events(evs ...transactionsEvent) {
	for _, ev := range evs {
		h.st.event(ev, -1)
	}
}

func (h *statusHarness) loads(bindings ...int) {
	for _, b := range bindings {
		h.st.event(readLoad, b)
	}
}

func (h *statusHarness) stores(bindings ...int) {
	for _, b := range bindings {
		h.st.event(readStore, b)
	}
}

func (h *statusHarness) expect(want ...string) {
	h.t.Helper()
	require.Equal(h.t, want, h.got)
	h.got = nil
}

// recover runs the recovery commit at the given speed.
func (h *statusHarness) recover(took time.Duration) {
	h.events(readAcknowledge)
	h.advance(took)
	h.events(sentAcknowledged)
}

// round runs one transaction whose stages each take `stage`.
func (h *statusHarness) round(stage time.Duration) {
	h.loads(0, 1)
	h.advance(stage)
	h.events(readFlush)
	h.advance(stage)
	h.events(sentFlushed)
	h.stores(0, 1, 2)
	h.advance(stage)
	h.events(readStartCommit, sentStartedCommit, readAcknowledge)
	h.advance(stage)
	h.events(sentAcknowledged)
}

func TestStatusSlowStages(t *testing.T) {
	h := newStatusHarness(t, nil)
	h.expect("info: Starting up: connecting to the destination")

	h.recover(2 * time.Second)
	h.expect("info: Recovery commit: finishing the transaction that was in progress when the task last stopped")

	// Waiting for data is reported once, after it has lasted statusMinActivePeriod.
	h.advance(5 * time.Minute)
	h.expect("info: Waiting, no new collection data since 2026-10-22 11:00:02 UTC")

	h.loads(0, 1, 2)
	h.advance(30 * time.Second)
	h.events(readFlush)
	h.advance(90 * time.Second)
	h.expect("info: Loading existing rows (3 documents from 2 collections, round 0), running for 1m")

	// A staging count grows until StartCommit, and is refreshed each minute.
	h.events(sentFlushed)
	h.stores(0)
	h.advance(time.Minute)
	h.stores(1, 2)
	h.advance(time.Minute)
	h.expect(
		"info: Staging changes at destination (1 document so far from 1 collection, round 0), running for 1m",
		"info: Staging changes at destination (3 documents so far from 2 collections, round 0), running for 2m",
	)

	h.events(readStartCommit, sentStartedCommit, readAcknowledge)
	// The next round's loads don't replace the commit in progress.
	h.loads(0)
	h.advance(150 * time.Second)
	h.expect(
		"info: Committing changes (3 documents from 2 collections, round 0), running for 1m",
		"info: Committing changes (3 documents from 2 collections, round 0), running for 2m",
	)

	// Reading resumes being reported, timed from when the commit finished.
	h.events(sentAcknowledged)
	h.advance(59 * time.Second)
	h.expect()
	h.advance(time.Second)
	h.expect("info: Reading new data (round 1), running for 1m")
}

func TestStatusFastTask(t *testing.T) {
	h := newStatusHarness(t, nil)
	h.recover(time.Second)
	h.got = nil

	// Stages shorter than statusMinActivePeriod are never reported. A rollup is, once
	// statusRollupWindow has passed since the last status.
	// The rollup is due on the tick before the 75th round is acknowledged.
	for range 75 {
		h.round(time.Second)
	}
	h.expect("info: Streaming changes (222 documents, 74 transactions in the last 5 minutes)")

	// A long stage after short ones is reported after statusMinActivePeriod, not held
	// back until the next rollup.
	h.round(time.Second)
	h.loads(2)
	h.events(readFlush)
	h.advance(time.Minute)
	h.expect("info: Loading existing rows (1 document from 1 collection, round 76), running for 1m")
}

func TestStatusSyncSchedule(t *testing.T) {
	h := newStatusHarness(t, nil)
	h.recover(time.Second)
	h.got = nil

	h.st.setBacklog(true)
	h.loads(0)
	h.events(readFlush, sentFlushed)
	h.stores(0)
	h.events(readStartCommit, sentStartedCommit, readAcknowledge)
	h.advance(time.Minute)
	h.expect("info: Backlog catchup: Committing changes (1 document from 1 collection, round 0), running for 1m")

	// The wait for a scheduled sync is reported right away.
	h.st.setBacklog(false)
	h.st.syncWait(time.Date(2026, 10, 22, 11, 30, 0, 0, time.UTC), 30*time.Minute)
	h.advance(statusTick)
	h.loads(1)
	h.advance(10 * time.Minute)
	h.expect("info: Waiting, next scheduled sync at 2026-10-22 11:30:00 UTC (every 30m)")

	h.st.syncWait(time.Date(2026, 10, 23, 1, 0, 0, 0, time.UTC), 0)
	h.advance(statusTick)
	h.expect("info: Waiting, next scheduled sync at 2026-10-23 01:00:00 UTC")
}

func TestStatusWarnings(t *testing.T) {
	h := newStatusHarness(t, nil)
	h.recover(time.Second)
	h.got = nil

	h.st.warn("alpha", "Rows of table alpha were not deleted because A.")
	h.st.warn("alpha", "Rows of table alpha were not deleted because A.")
	h.st.warn("beta", "Rows of table beta were not deleted because B.")
	h.expect(
		"warning: Rows of table alpha were not deleted because A.",
		"warning: Rows of table alpha were not deleted because A. Rows of table beta were not deleted because B.",
	)

	// Warnings replace progress for the rest of the session.
	h.round(2 * time.Minute)
	h.advance(10 * time.Minute)
	h.expect()
	require.Empty(t, h.st.recentCommits)
}

func TestStatusOtherShards(t *testing.T) {
	h := newStatusHarness(t, &pf.RangeSpec{KeyBegin: 1 << 31, KeyEnd: 1<<32 - 1, RClockEnd: 1<<32 - 1})
	h.recover(time.Minute)
	h.round(2 * time.Minute)
	h.advance(10 * time.Minute)
	h.expect()
	require.Empty(t, h.st.recentCommits)

	h.st.warn("alpha", "Rows of table alpha were not deleted because A.")
	h.expect("warning: Rows of table alpha were not deleted because A.")
}

func TestStatusFormatting(t *testing.T) {
	for n, want := range map[int]string{0: "0", 999: "999", 1000: "1,000", 120000: "120,000", 999999: "999,999", 1000000: "1M", 1_234_567: "1.2M", 25_000_000: "25M", 3_400_000_000: "3.4B"} {
		require.Equal(t, want, formatCount(n), n)
	}
	for d, want := range map[time.Duration]string{0: "0s", 30 * time.Second: "30s", 150 * time.Second: "2m30s", 30 * time.Minute: "30m", 4 * time.Hour: "4h", 65 * time.Minute: "1h5m"} {
		require.Equal(t, want, formatDuration(d), d)
	}
	require.Equal(t, "1 collection", plural(1, "collection"))
	require.Equal(t, "2 collections", plural(2, "collection"))
}
