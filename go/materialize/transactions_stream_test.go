package materialize

import (
	"context"
	"testing"
	"time"

	"github.com/estuary/connectors/go/schedule"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

func TestAckDelayLogs(t *testing.T) {
	var recent = time.Now().Add(-time.Minute)
	var future = time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)

	for _, tt := range []struct {
		name          string
		storedHistory []int
		lastAckTime   time.Time
		want          string
	}{
		{
			"backlog",
			[]int{1, storeBackfillThreshold, 1, 1, 1},
			recent,
			"Not waiting for the sync schedule while catching up on a backlog. The schedule applies again once the last 5 transactions have each stored fewer than 1M documents.",
		},
		{
			"started recently",
			[]int{1, 1},
			recent,
			"Not waiting for the sync schedule because the task started recently. The schedule applies once 5 transactions have stored data since it started (2 so far).",
		},
		{
			"sync time passed",
			[]int{1, 1, 1, 1, 1},
			time.Date(2026, 10, 31, 14, 0, 0, 0, time.UTC),
			"Not waiting for the sync schedule because the scheduled sync time 2026-10-31 14:30:00 UTC has already passed.",
		},
		{
			"waiting",
			[]int{1, 1, 1, 1, 1},
			future,
			"Waiting for the next scheduled sync at 2100-01-01 00:30:00 UTC (every 30m). Disable and re-enable the task to sync and apply config changes now.",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var hook = logtest.NewGlobal()
			defer hook.Reset()

			// Cancelled, so a delay returns right away.
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			var be = NewBindingEvents()
			be.status = newStatusTracker(&pm.Request_Open{})
			var s = &transactionsStream{
				ctx:           ctx,
				be:            be,
				handler:       func(transactionsEvent) {},
				ackSchedule:   schedule.NewPeriodicSchedule(30 * time.Minute),
				lastAckTime:   tt.lastAckTime,
				storedHistory: tt.storedHistory,
			}
			_ = s.maybeDelayAcknowledgement()

			var entry = hook.LastEntry()
			require.Equal(t, tt.want, entry.Message)
			require.Equal(t, tt.lastAckTime.Add(30*time.Minute).UTC().Truncate(time.Second).String(), entry.Data["nextScheduledAck"])
		})
	}
}

func TestAckDelayExtendedLog(t *testing.T) {
	var hook = logtest.NewGlobal()
	defer hook.Reset()

	var be = NewBindingEvents()
	be.status = newStatusTracker(&pm.Request_Open{})
	be.status.syncWait(time.Date(2026, 10, 31, 14, 30, 0, 0, time.UTC), 30*time.Minute)

	newExtendedLogger(loggerAtLevel{lvl: log.InfoLevel}, be).waitingForAckDelayLogFn(3)()

	var entry = hook.LastEntry()
	require.Equal(t, "Waiting for the next scheduled sync at 2026-10-31 14:30:00 UTC (every 30m). Disable and re-enable the task to sync and apply config changes now.", entry.Message)
	require.Equal(t, log.Fields{"round": 3}, entry.Data)
}
