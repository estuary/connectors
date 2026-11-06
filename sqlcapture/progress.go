package sqlcapture

import (
	"context"
	"fmt"
	"math"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	backfillRateHalfLife      = 10 * time.Minute     // Older throughput measurements halve in influence over this period.
	backfillRateWarmup        = 2 * time.Minute      // No time estimate until throughput has been measured this long.
	backfillRateUpdateMinimum = 5 * time.Second      // Shorter measurement intervals are too noisy.
	backfillEstimateMaximum   = 365 * 24 * time.Hour // No time estimate is given beyond this.
)

// backfillRun is the progress of a backfill run, which lasts from when tables start
// backfilling until none remain. It's kept in memory, so after a restart it covers
// only the tables still backfilling.
type backfillRun struct {
	tablesFinished int // Tables which finished backfilling earlier in the run.
	rowsFinished   int // Rows backfilled from those tables.
	rate           backfillRate
}

// backfillRate measures backfill throughput as an exponentially weighted average,
// so that it follows changes in throughput as the backfill moves between tables.
// Until enough has been measured it's a plain average, so that the first noisy
// interval doesn't dominate.
type backfillRate struct {
	rows            int // Rows backfilled since the last update.
	rowsPerSecond   float64
	measuredSeconds float64
	lastUpdate      time.Time
}

// update folds the throughput since the previous update into the rate.
func (r *backfillRate) update(now time.Time) {
	if r.lastUpdate.IsZero() {
		r.lastUpdate, r.rows = now, 0
		return
	}
	var elapsed = now.Sub(r.lastUpdate)
	if elapsed < backfillRateUpdateMinimum {
		return
	}
	var seconds = elapsed.Seconds()
	var observed = float64(r.rows) / seconds
	var weight = max(1-math.Exp2(-seconds/backfillRateHalfLife.Seconds()), seconds/(r.measuredSeconds+seconds))
	r.rowsPerSecond += weight * (observed - r.rowsPerSecond)
	r.measuredSeconds += seconds
	r.lastUpdate, r.rows = now, 0
}

// estimate extrapolates the time to backfill the remaining rows, or reports false
// if the rate hasn't been measured for long enough or the estimate is implausibly long.
func (r *backfillRate) estimate(rowsRemaining int) (time.Duration, bool) {
	if r.measuredSeconds < backfillRateWarmup.Seconds() || r.rowsPerSecond <= 0 {
		return 0, false
	}
	var seconds = float64(rowsRemaining) / r.rowsPerSecond
	if seconds > backfillEstimateMaximum.Seconds() {
		return 0, false
	}
	return time.Duration(seconds * float64(time.Second)), true
}

// backfillProgress describes how far along the current backfill run is, for the
// connector status.
func (c *Capture) backfillProgress() string {
	var backfilling = c.bindingsCurrentlyBackfilling()
	var run = &c.backfillRun
	var tables = fmt.Sprintf("%d of %d tables backfilled", run.tablesFinished, run.tablesFinished+len(backfilling))

	var tableIDs = make([]TableID, 0, len(backfilling))
	for _, binding := range backfilling {
		tableIDs = append(tableIDs, TableID{binding.Resource.Namespace, binding.Resource.Stream})
	}
	estimatedCounts, err := c.Database.EstimatedRowCounts(context.Background(), tableIDs)
	if err != nil {
		log.WithError(err).Warn("error querying estimated row counts for backfill progress")
		return tables
	}

	var rowsDone, rowsRemaining = run.rowsFinished, 0
	var remainingKnown = true
	for i, binding := range backfilling {
		var estimated, ok = estimatedCounts[tableIDs[i]]
		if !ok || estimated <= 0 {
			return tables // An unknown table size would make the percentage and time estimate misleading.
		}
		var backfilled = c.State.Streams[binding.StateKey].BackfilledCount
		rowsDone += backfilled
		rowsRemaining += max(estimated-backfilled, 0)
		// Once a table outgrows its estimate, how much of it remains is unknown.
		remainingKnown = remainingKnown && backfilled < estimated
	}

	// Clamped to 99% since we're still backfilling.
	var progress = fmt.Sprintf("%.0f%% complete, %s", min(float64(rowsDone)/float64(rowsDone+rowsRemaining)*100, 99), tables)
	if remaining, ok := run.rate.estimate(rowsRemaining); ok && remainingKnown {
		progress += ", " + formatTimeRemaining(remaining)
	}
	return progress
}

// formatTimeRemaining renders a backfill time estimate coarsely, since it's only
// approximate and mainly tells whether a backfill will take hours or days.
func formatTimeRemaining(d time.Duration) string {
	switch {
	case d < 5*time.Minute:
		return "less than 5 minutes remaining"
	case d < 55*time.Minute:
		return fmt.Sprintf("about %d minutes remaining", int(math.Round(d.Minutes()/5))*5)
	case d < 90*time.Minute:
		return "about 1 hour remaining"
	case d < 48*time.Hour:
		return fmt.Sprintf("about %d hours remaining", int(math.Round(d.Hours())))
	default:
		return fmt.Sprintf("about %d days remaining", int(math.Round(d.Hours()/24)))
	}
}
