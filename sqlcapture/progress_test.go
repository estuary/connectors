package sqlcapture

import (
	"context"
	"fmt"
	"testing"
	"time"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/segmentio/encoding/json"
	"github.com/stretchr/testify/require"
)

// newProgressTestCapture makes a Capture whose tables are all backfilling, where
// each table holds the given number of rows, every chunk scans up to 10 of them,
// and the estimated row counts are exactly the table sizes.
func newProgressTestCapture(t *testing.T, tableRows map[string]int) *Capture {
	t.Helper()

	var bindings = make(map[StreamID]*Binding)
	var streams = make(map[boilerplate.StateKey]*TableState)
	var discovery = make(map[StreamID]*DiscoveryInfo)
	var index uint32
	for table := range tableRows {
		var streamID = JoinStreamID("public", table)
		var stateKey = boilerplate.StateKey("public%2F" + table)
		bindings[streamID] = &Binding{
			Index:    index,
			StreamID: streamID,
			StateKey: stateKey,
			Resource: Resource{Namespace: "public", Stream: table},
		}
		streams[stateKey] = &TableState{Mode: TableStateUnfilteredBackfill, KeyColumns: []string{"id"}}
		discovery[streamID] = &DiscoveryInfo{Name: table, Schema: "public", BaseTable: true}
		index++
	}

	var db = &mockDatabase{
		ScanTableChunkFunc: func(ctx context.Context, info *DiscoveryInfo, state *TableState, backfillFilter string, callback func(event ChangeEvent) error) (bool, []byte, error) {
			var remaining = tableRows[info.Name] - state.BackfilledCount
			var n = min(remaining, 10)
			for range n {
				if err := callback(&mockChangeEvent{ID: JoinStreamID("public", info.Name), Doc: json.RawMessage(`{}`)}); err != nil {
					return false, nil, err
				}
			}
			return n == remaining, []byte{0x01}, nil
		},
	}
	setEstimates(db, func(table TableID) int { return tableRows[table.Table] })

	var c = &Capture{
		Name:     "test/capture",
		Bindings: bindings,
		State:    &PersistentState{Streams: streams},
		Output:   newMockPullOutput(),
		Database: db,
	}
	startMockCapture(c, discovery)
	return c
}

// setEstimates makes the mock database report row count estimates from fn.
func setEstimates(db *mockDatabase, fn func(TableID) int) {
	db.EstimatedRowCountsFunc = func(ctx context.Context, tables []TableID) (map[TableID]int, error) {
		var result = make(map[TableID]int)
		for _, table := range tables {
			result[table] = fn(table)
		}
		return result, nil
	}
}

func TestBackfillProgressIsRunWide(t *testing.T) {
	var ctx = context.Background()
	var c = newProgressTestCapture(t, map[string]int{"small": 10, "medium": 40, "large": 150})

	for c.AnyBindingsCurrentlyBackfilling() {
		// With exact estimates the percentage is the fraction of all rows read, and
		// finished tables stay in the count.
		var rowsDone, tablesDone int
		for _, state := range c.State.Streams {
			rowsDone += state.BackfilledCount
			if state.Mode == TableStateActive {
				tablesDone++
			}
		}
		var want = fmt.Sprintf("%d%% complete, %d of 3 tables backfilled", min(rowsDone*100/200, 99), tablesDone)
		require.Equal(t, want, c.backfillProgress())

		var _, err = c.mainLoopIteration(ctx)
		require.NoError(t, err)
	}

	// The run ended with the last table, and the next run starts from scratch.
	require.Equal(t, backfillRun{}, c.backfillRun)
}

func TestBackfillRateMeasuredByMainLoop(t *testing.T) {
	var c = newProgressTestCapture(t, map[string]int{"a": 1000})
	var _, err = c.mainLoopIteration(context.Background())
	require.NoError(t, err)
	require.False(t, c.backfillRun.rate.lastUpdate.IsZero(), "the iteration takes the rate's baseline")
	require.Equal(t, 10, c.backfillRun.rate.rows, "the chunk's rows count towards the rate")
}

func TestBackfillRunEndsWhenLastTableRemoved(t *testing.T) {
	var c = newProgressTestCapture(t, map[string]int{"a": 10})
	c.backfillRun.tablesFinished = 3
	c.setStreamState("public%2Fa", &TableState{Mode: TableStateMissing})
	require.Equal(t, backfillRun{}, c.backfillRun)
}

func TestBackfillProgressUnknownEstimate(t *testing.T) {
	var c = newProgressTestCapture(t, map[string]int{"a": 10, "b": 10})
	// As postgres reports for a table which has never been analyzed.
	setEstimates(c.Database.(*mockDatabase), func(TableID) int { return -1 })
	require.Equal(t, "0 of 2 tables backfilled", c.backfillProgress())
}

func TestBackfillProgressOverEstimate(t *testing.T) {
	var c = newProgressTestCapture(t, map[string]int{"a": 1000, "b": 1000})
	setEstimates(c.Database.(*mockDatabase), func(table TableID) int {
		return map[string]int{"a": 800, "b": 1000}[table.Table] // Table a's estimate is low.
	})
	c.backfillRun.rate = backfillRate{rowsPerSecond: 1, measuredSeconds: 600}

	c.State.Streams["public%2Fa"].BackfilledCount = 700
	require.Equal(t, "39% complete, 0 of 2 tables backfilled, about 20 minutes remaining", c.backfillProgress())

	// Once table a passes its estimate the time remaining is unknown, but the
	// percentage still reports how far along the rest of the backfill is.
	c.State.Streams["public%2Fa"].BackfilledCount = 900
	require.Equal(t, "47% complete, 0 of 2 tables backfilled", c.backfillProgress())
}

// runBackfillRate returns a function which advances a backfillRate by the given
// duration, updating it every 10 seconds as the main loop would, with rows backfilled
// at the given rate.
func runBackfillRate(rate *backfillRate) func(d time.Duration, rowsPerSecond int) {
	var now = time.Now()
	rate.update(now) // Takes the baseline, as the first backfill iteration does.
	return func(d time.Duration, rowsPerSecond int) {
		for end := now.Add(d); now.Before(end); {
			now = now.Add(10 * time.Second)
			rate.rows += rowsPerSecond * 10
			rate.update(now)
		}
	}
}

func TestBackfillRateWarmup(t *testing.T) {
	var rate backfillRate
	var run = runBackfillRate(&rate)
	run(110*time.Second, 100)
	var _, ok = rate.estimate(360_000)
	require.False(t, ok, "no estimate during warmup")

	run(10*time.Second, 100)
	var remaining time.Duration
	remaining, ok = rate.estimate(360_000)
	require.True(t, ok)
	require.Equal(t, time.Hour, remaining)
}

func TestBackfillRateIgnoresNoisyStart(t *testing.T) {
	// A burst in the first interval. Seeding a weighted average with it would leave
	// the rate around 150 after half an hour.
	var rate backfillRate
	var run = runBackfillRate(&rate)
	run(10*time.Second, 500)
	run(30*time.Minute, 100)
	require.InDelta(t, 100, rate.rowsPerSecond, 2)
}

func TestBackfillRateTracksSlowdown(t *testing.T) {
	var rate backfillRate
	var run = runBackfillRate(&rate)
	run(time.Hour, 100)
	require.InDelta(t, 100, rate.rowsPerSecond, 0.01)

	// Throughput halves. After one half-life the measurement is halfway there.
	run(backfillRateHalfLife, 50)
	require.InDelta(t, 75, rate.rowsPerSecond, 1)
	run(time.Hour, 50) // Six more half-lives.
	require.InDelta(t, 50, rate.rowsPerSecond, 0.5)
}

func TestBackfillRateNoImplausibleEstimate(t *testing.T) {
	// Billions of rows at a crawl would overflow a time.Duration.
	var rate = backfillRate{rowsPerSecond: 0.1, measuredSeconds: 600}
	var _, ok = rate.estimate(2_000_000_000)
	require.False(t, ok)
}

func TestFormatTimeRemaining(t *testing.T) {
	for _, tc := range []struct {
		d    time.Duration
		want string
	}{
		{time.Minute, "less than 5 minutes remaining"},
		{6 * time.Minute, "about 5 minutes remaining"},
		{33 * time.Minute, "about 35 minutes remaining"},
		{54 * time.Minute, "about 55 minutes remaining"},
		{80 * time.Minute, "about 1 hour remaining"},
		{100 * time.Minute, "about 2 hours remaining"},
		{47 * time.Hour, "about 47 hours remaining"},
		{60 * time.Hour, "about 3 days remaining"},
	} {
		t.Run(fmt.Sprint(tc.d), func(t *testing.T) {
			require.Equal(t, tc.want, formatTimeRemaining(tc.d))
		})
	}
}
