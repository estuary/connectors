package boilerplate

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/estuary/connectors/go/dbt"
	m "github.com/estuary/connectors/go/protocols/materialize"
	"github.com/estuary/connectors/go/schedule"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

const (
	// storeBackfillThreshold is a somewhat crude indication that a transaction was
	// likely part of a backfill vs. a smaller incremental streaming
	// transaction. If a materialization is configured with a commit delay, it
	// should only apply that delay to transactions that occur after it has
	// fully backfilled from the collection. The idea is that if a transaction
	// stored a lot of documents it's probably part of a backfill.
	storeBackfillThreshold = 1_000_000

	// The number of transactions we will look back at for determining if an
	// acknowledgement delay should be applied. The number of documents stored
	// for all of these transactions must be below storeBackfillThreshold for
	// the delay to apply. This is used to estimate if we are in a "streaming"
	// mode or not, based on a consistent number of documents per transactions
	// being below the threshold.
	storedHistorySize = 5
)

// transactionsEvent represents one of the many interesting things that can
// happen while running materialization transactions.
type transactionsEvent int

const (
	readLoad transactionsEvent = iota
	readFlush
	readStore
	readStartCommit
	readAcknowledge
	sentLoaded
	sentFlushed
	sentStartedCommit
	startedAckDelay
	sentAcknowledged
)

type transactionsStream struct {
	ctx     context.Context
	stream  m.MaterializeStream
	handler func(transactionsEvent)

	// Variables used for handling acknowledgement delays when configured.
	ackSchedule   schedule.Schedule
	lastAckTime   time.Time
	storedHistory []int

	dbtJobTriggerConfig *dbt.JobConfig
	dbtJobTriggerCh     chan bool
}

// newTransactionsStream wraps a base stream MaterializeStream with extra
// capabilities via specific handling for events.
func newTransactionsStream(
	ctx context.Context,
	stream m.MaterializeStream,
	lvl log.Level,
	options MaterializeOptions,
	be *BindingEvents,
) (*transactionsStream, error) {
	s := &transactionsStream{stream: stream, ctx: ctx, dbtJobTriggerCh: make(chan bool)}

	var loggingHandler func(transactionsEvent)
	if options.ExtendedLogging {
		l := newExtendedLogger(loggerAtLevel{lvl: log.InfoLevel}, be)
		loggingHandler = l.handler()
	} else if lvl >= log.DebugLevel {
		l := newExtendedLogger(loggerAtLevel{lvl: log.DebugLevel}, be)
		loggingHandler = l.handler()
	} else {
		l := newBasicLogger()
		loggingHandler = l.handler()
	}

	if options.AckSchedule != nil {
		sched, err := createSchedule(options.AckSchedule.Config, options.AckSchedule.Jitter)
		if err != nil {
			return nil, fmt.Errorf("creating ack schedule: %w", err)
		}
		s.ackSchedule = sched
	}

	if options.DBTJobTrigger != nil && options.DBTJobTrigger.Enabled() {
		s.dbtJobTriggerConfig = options.DBTJobTrigger
	}

	// ackSchedule may be `nil` even if options.AckSchedule was not if the
	// configuration calls for an explicit 0 syncFrequency.
	if s.ackSchedule != nil {
		s.handler = s.storedHistoryHandler(loggingHandler)
	} else {
		s.handler = loggingHandler
	}

	// NOTE: This delay is set based on the default value of `maxTxnDuration` being 5 minutes, which means that we are guaranteed
	// to receive a new transaction from the runtime if data exists.
	// If a task has `maxTxnDuration` higher than 5 minutes, and there has been no delay in the last transaction, we will
	// trigger a dbt job 5 minutes after the last transaction. To avoid this users can set a different SafetyTriggerDelay to a value
	// higher than the maxTxnDuration
	delay := time.Duration(5*time.Minute + 15*time.Second)
	if s.dbtJobTriggerConfig != nil && s.dbtJobTriggerConfig.SafetyTriggerDelay != "" {
		var err error
		delay, err = time.ParseDuration(s.dbtJobTriggerConfig.SafetyTriggerDelay)
		if err != nil {
			return nil, fmt.Errorf("parsing dbt safety trigger delay: %w", err)
		}
	}
	go s.dbtJobTriggerHandler(ctx, delay)

	return s, nil
}

func (l *transactionsStream) dbtJobTriggerHandler(ctx context.Context, delay time.Duration) {
	timer := time.NewTimer(delay)
	timer.Stop()

	for {
		select {
		case trigger := <-l.dbtJobTriggerCh:
			// If we haven't processed anything yet, don't schedule a job
			if len(l.storedHistory) == 0 {
				continue
			}

			// A new transaction has happened, reset the timer
			if !timer.Stop() {
				// Drain the timer channel to prevent leaks.
				select {
				case <-timer.C:
				default:
				}
			}

			if trigger {
				timer.Reset(delay)
				log.Debug("resetting background dbt job trigger timer")
			}
			continue
		case <-timer.C:
			log.Debug("triggering background dbt job")
			if err := l.triggerDBTJob(); err != nil {
				log.WithField("err", err).Error("background dbt job trigger failed")
			}
			continue
		case <-ctx.Done():
			return
		}
	}
}

// storedHistoryHandler updates the stored history accounting for calculating an
// acknowledgement delay if an ackSchedule is configured.
func (l *transactionsStream) storedHistoryHandler(delegate func(transactionsEvent)) func(transactionsEvent) {
	var count int

	return func(event transactionsEvent) {
		switch event {
		case readStore:
			count++
		case readStartCommit:
			// It is possible to have commits with 0 Store requests, and those
			// will be ignored for this calculation. The only time I've seen
			// this happen is when a connector first starts up and has a
			// `notBefore` that causes it to skip reading a large amount of
			// journal data. It would not be appropriate to delay further
			// commits in that case.
			if count > 0 {
				l.storedHistory = append(l.storedHistory, count)
				if len(l.storedHistory) > storedHistorySize {
					l.storedHistory = l.storedHistory[1:]
				}
				count = 0
			}
		}
		delegate(event)
	}
}

func (l *transactionsStream) Send(m *pm.Response) error {
	// Every time there is some activity, we send a signal to
	// the dbt job trigger channel to postpone the trigger another 5 minutes
	// we only want to trigger the job after minutes of inactivity by the connector
	l.dbtJobTriggerCh <- true

	if m.Loaded != nil {
		l.handler(sentLoaded)
	} else if m.Flushed != nil {
		l.handler(sentFlushed)
	} else if m.StartedCommit != nil {
		l.handler(sentStartedCommit)
	} else if m.Acknowledged != nil {
		if err := l.maybeDelayAcknowledgement(); err != nil {
			return err
		}
		l.handler(sentAcknowledged)
	}

	return l.stream.Send(m)
}

func (l *transactionsStream) RecvMsg(m *pm.Request) error {
	if err := l.stream.RecvMsg(m); err != nil {
		return err
	}

	if m.Load != nil {
		l.handler(readLoad)
	} else if m.Flush != nil {
		l.handler(readFlush)
	} else if m.Store != nil {
		l.handler(readStore)
	} else if m.StartCommit != nil {
		l.handler(readStartCommit)
	} else if m.Acknowledge != nil {
		l.handler(readAcknowledge)
	}

	return nil
}

func (l *transactionsStream) triggerDBTJob() error {
	if l.dbtJobTriggerConfig != nil {
		if err := dbt.JobTrigger(*l.dbtJobTriggerConfig); err != nil {
			return fmt.Errorf("triggering dbt job: %w", err)
		}
	}
	return nil
}

func (l *transactionsStream) maybeDelayAcknowledgement() error {
	// lastAckTime at a zero value means this is the recovery commit, and we
	// never delay on the recovery commit.
	if l.ackSchedule != nil && !l.lastAckTime.IsZero() {
		nextAckAt := l.ackSchedule.Next(l.lastAckTime)
		d := time.Until(nextAckAt)

		ll := log.WithFields(log.Fields{
			"lastAckTime":      l.lastAckTime.UTC().Truncate(time.Second).String(),
			"nextScheduledAck": nextAckAt.UTC().Truncate(time.Second).String(),
			"storedHistory":    l.storedHistory,
		})

		// If there have been at least `storedHistorySize` transactions
		// completed and none of the transactions were large enough to suggest
		// we are still in the midst of a backfill, the acknowledgement delay
		// may be applicable.
		delayBasedOnStoredHistory := len(l.storedHistory) >= storedHistorySize && !slices.ContainsFunc(l.storedHistory, func(n int) bool {
			return n >= storeBackfillThreshold
		})

		if !delayBasedOnStoredHistory {
			ll.Info("not delaying commit acknowledgement based on stored history")
		} else if d <= 0 {
			ll.Info("not delaying commit acknowledgement since current time is after next scheduled acknowledgement")
		} else {
			l.handler(startedAckDelay)
			ll.WithField("delay", d.String()).Info("delaying before acknowledging commit")

			// Cancel the timer since we are triggering a job directly here
			l.dbtJobTriggerCh <- false
			if err := l.triggerDBTJob(); err != nil {
				return err
			}

			select {
			case <-l.ctx.Done():
				return l.ctx.Err()
			case <-time.After(d):
			}
		}
	} else if !l.lastAckTime.IsZero() && l.dbtJobTriggerConfig != nil {
		// Cancel the timer since we are triggering a job directly here
		l.dbtJobTriggerCh <- false
		if err := l.triggerDBTJob(); err != nil {
			return err
		}
	}

	l.lastAckTime = time.Now()
	return nil
}
