package materialize

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	// How often the various async loggers will report their status. When
	// "extended" logging is enabled, transitions of interest will always be
	// logged immediately, and only after loggingFrequency will status updates
	// be logged if nothing else has happened. When extended logging is not
	// enabled, there will be a more generic status update logged out at this
	// frequency.
	loggingFrequency = 5 * time.Minute

	// When the "basic" logger is being used, how long a commit must be in
	// progress or how long the connector must be waiting for documents from the
	// runtime before it logs something about that.
	slowOperationThreshold = 15 * time.Minute
)

// loggerAtLevel wraps a logrus logger to always log at the configured level.
type loggerAtLevel struct {
	lvl log.Level
}

func (l loggerAtLevel) log(fields log.Fields, msg string) {
	log.WithFields(fields).Log(l.lvl, msg)
}

type basicLogger struct {
	mu                  sync.Mutex
	round               int
	readLoads           int
	sentLoaded          int
	readStores          int
	waitingForDocsStart time.Time
	commitStart         time.Time
	ackDelayActive      bool
}

// newBasicLogger creates a logger that periodically logs some basic information
// about the progress of the materialization at the INFO level.
func newBasicLogger() *basicLogger {
	var l = new(basicLogger)
	repeatAsync(l.doLog, loggingFrequency)

	return l
}

func (l *basicLogger) doLog() {
	l.mu.Lock()
	defer l.mu.Unlock()

	ll := log.WithFields(log.Fields{
		"readLoads":   l.readLoads,
		"sentLoadeds": l.sentLoaded,
		"readStores":  l.readStores,
		"round":       l.round,
	})

	if !l.waitingForDocsStart.IsZero() && time.Since(l.waitingForDocsStart) > slowOperationThreshold {
		ll.Info("waiting for documents")
	} else if !l.ackDelayActive && !l.commitStart.IsZero() && time.Since(l.commitStart) > slowOperationThreshold {
		ll.WithField("started", l.commitStart.UTC().Truncate(time.Second).String()).Info("materialization commit has been running for a long time")
	} else {
		ll.Info("materialization progress")
	}
}

func (l *basicLogger) handler() func(transactionsEvent) {
	var loadPhaseStarted bool
	var recovery = true

	return func(event transactionsEvent) {
		l.mu.Lock()
		defer l.mu.Unlock()

		// Increment counters.
		switch event {
		case readLoad:
			l.readLoads++
		case sentLoaded:
			l.sentLoaded++
		case readStore:
			l.readStores++
		}

		// Increment the "round" counter and update state.
		switch event {
		case sentStartedCommit:
			loadPhaseStarted = false
			l.commitStart = time.Now()
			l.round++
		case readAcknowledge:
			if recovery {
				l.commitStart = time.Now()
			}
		case startedAckDelay:
			l.ackDelayActive = true
		case sentAcknowledged:
			if !loadPhaseStarted {
				l.waitingForDocsStart = time.Now()
			}
			l.commitStart = time.Time{}
			recovery = false
			l.ackDelayActive = false
		case readLoad, readFlush:
			l.waitingForDocsStart = time.Time{}
			loadPhaseStarted = true
		}
	}
}

type extendedLogger struct {
	be  *BindingEvents
	log func(log.Fields, string)

	readLoads           atomic.Int32
	sentLoaded          atomic.Int32
	readStores          atomic.Int32
	readingLoadsStart   time.Time
	sendingLoadedsStart time.Time
	readingStoresStart  time.Time
	commitStart         time.Time
	waitingForDocsStart time.Time
	ackDelayStart       time.Time
}

// newExtendedLogger returns a logger than logs detailed information about the
// materialization as it progresses through transaction steps.
func newExtendedLogger(ll loggerAtLevel, be *BindingEvents) *extendedLogger {
	be.activate(ll)
	return &extendedLogger{be: be, log: ll.log}
}

// logAsync starts an async logger and returns a function that can be used to
// stop it. The asyncLog logging callback is run periodically and while holding
// the mutex for extendedLogger. The doneLog callback does not acquire the lock.
func (l *extendedLogger) logAsync(asyncLog func()) (stopLogger func(doneLog func())) {
	return repeatAsync(asyncLog, loggingFrequency)
}

func (l *extendedLogger) handler() func(transactionsEvent) {
	var stopReadingLoadsLogger func(func())
	var stopProcessingLoadsLogger func(func())
	var stopStoreLogger func(func())
	var stopWaitingForDocsLogger func(func())
	var waitingForDocsMu sync.Mutex
	var loadPhaseStarted bool
	var ackDelayActive bool
	var recovery = true
	var round int

	return func(event transactionsEvent) {
		// Increment the "round" counter and establish logging for the case of
		// "waiting for documents".
		switch event {
		case sentStartedCommit:
			loadPhaseStarted = false
			round++
			l.be.round++
		case sentAcknowledged:
			waitingForDocsMu.Lock()
			if !loadPhaseStarted {
				stopWaitingForDocsLogger = l.logAsync(l.waitingForDocsLogFn(round))
			}
			waitingForDocsMu.Unlock()
		case readLoad, readFlush:
			waitingForDocsMu.Lock()
			if !l.waitingForDocsStart.IsZero() {
				stopWaitingForDocsLogger(l.finishedWaitingForDocsLogFn(round))
			}
			l.waitingForDocsStart = time.Time{}
			loadPhaseStarted = true
			waitingForDocsMu.Unlock()
		}

		// Start and stop other loggers, resetting counters as needed.
		switch event {
		case readLoad:
			if n := l.readLoads.Add(1); n == 1 {
				stopReadingLoadsLogger = l.logAsync(l.readingLoadsLogFn(round))
			}
		case readFlush:
			if total := l.readLoads.Swap(0); total != 0 {
				stopReadingLoadsLogger(l.finishedReadingLoadsLogFn(round, total))
			}
		case sentLoaded:
			if n := l.sentLoaded.Add(1); n == 1 {
				stopProcessingLoadsLogger = l.logAsync(l.processingLoadedsLogFn(round))
			}
		case sentFlushed:
			if total := l.sentLoaded.Swap(0); total != 0 {
				stopProcessingLoadsLogger(l.finishedProcessingLoadedsLogFn(round, total))
			}
		case readStore:
			if n := l.readStores.Add(1); n == 1 {
				stopStoreLogger = l.logAsync(l.readingStoresLogFn(round))
			}
		case readStartCommit:
			if total := l.readStores.Swap(0); total != 0 {
				stopStoreLogger(l.finishedReadingStoresLogFn(round, total))
			}
		case sentStartedCommit:
			// NB: The "round" is incremented in on sentStartedCommit prior to
			// the handling here and below by stopStoreLogger. This means that
			// the round for the commit is actually one less than currently
			// recorded.
			stopStoreLogger = l.logAsync(l.runningCommitLogFn(round - 1))
		case readAcknowledge:
			if recovery {
				stopStoreLogger = l.logAsync(l.runningRecoveryCommitLogFn())
			}
		case startedAckDelay:
			// NB: Ack delay is never used for the recovery commit.
			stopStoreLogger(l.finishedCommitLogFn(round - 1))
			stopStoreLogger = l.logAsync(l.waitingForAckDelayLogFn(round - 1))
			ackDelayActive = true
		case sentAcknowledged:
			if recovery {
				stopStoreLogger(l.finishedRecoveryCommitLogFn())
				recovery = false
			} else if ackDelayActive {
				stopStoreLogger(l.finishedAckDelayLogFn(round - 1))
				ackDelayActive = false
			} else {
				stopStoreLogger(l.finishedCommitLogFn(round - 1))
			}
		}
	}
}

func (l *extendedLogger) readingLoadsLogFn(round int) func() {
	l.readingLoadsStart = time.Now()
	l.log(log.Fields{"round": round}, "started reading load requests")
	var lastLoadCount int32

	return func() {
		thisLoadCount := l.readLoads.Load()
		if thisLoadCount == lastLoadCount {
			l.log(log.Fields{"round": round, "count": thisLoadCount}, "waiting for more load requests")
		} else {
			l.log(log.Fields{"round": round, "count": thisLoadCount}, "reading load requests")
		}
		lastLoadCount = thisLoadCount
	}
}

func (l *extendedLogger) finishedReadingLoadsLogFn(round int, total int32) func() {
	return func() {
		l.log(log.Fields{
			"round": round,
			"took":  time.Since(l.readingLoadsStart).String(),
			"count": total,
		}, "finished reading load requests")
	}
}

func (l *extendedLogger) processingLoadedsLogFn(round int) func() {
	l.sendingLoadedsStart = time.Now()
	l.log(log.Fields{"round": round}, "started processing loaded documents")
	return func() {
		l.log(log.Fields{"round": round, "count": l.sentLoaded.Load()}, "processing loaded documents")
	}
}

func (l *extendedLogger) finishedProcessingLoadedsLogFn(round int, total int32) func() {
	return func() {
		l.log(log.Fields{
			"round": round,
			"took":  time.Since(l.sendingLoadedsStart).String(),
			"count": total,
		}, "finished processing loaded documents")
	}
}

func (l *extendedLogger) readingStoresLogFn(round int) func() {
	l.readingStoresStart = time.Now()
	l.log(log.Fields{"round": round}, "started reading store requests")
	return func() {
		l.log(log.Fields{"round": round, "count": l.readStores.Load()}, "reading store requests")
	}
}

func (l *extendedLogger) finishedReadingStoresLogFn(round int, total int32) func() {
	return func() {
		l.log(log.Fields{
			"round": round,
			"took":  time.Since(l.readingStoresStart).String(),
			"count": total,
		}, "finished reading store requests")
	}
}

func (l *extendedLogger) runningCommitLogFn(round int) func() {
	l.commitStart = time.Now()
	l.log(log.Fields{"round": round}, "started materialization commit")
	return func() {
		l.log(log.Fields{"round": round}, "materialization commit in progress")
	}
}

func (l *extendedLogger) finishedCommitLogFn(round int) func() {
	return func() {
		l.log(log.Fields{"round": round, "took": time.Since(l.commitStart).String()}, "finished materialization commit")
	}
}

func (l *extendedLogger) waitingForAckDelayLogFn(round int) func() {
	l.ackDelayStart = time.Now()
	return func() {
		l.log(log.Fields{"round": round}, "waiting to acknowledge materialization commit")
	}
}

func (l *extendedLogger) finishedAckDelayLogFn(round int) func() {
	return func() {
		l.log(log.Fields{
			"round": round,
			"took":  time.Since(l.ackDelayStart).String(),
		}, "acknowledged materialization commit after configured delay")
	}
}

func (l *extendedLogger) runningRecoveryCommitLogFn() func() {
	l.commitStart = time.Now()
	l.log(log.Fields{"round": 0}, "started materialization recovery commit")
	return func() {
		l.log(log.Fields{"round": 0}, "materialization recovery commit in progress")
	}
}

func (l *extendedLogger) finishedRecoveryCommitLogFn() func() {
	return func() {
		l.log(log.Fields{"round": 0, "took": time.Since(l.commitStart).String()}, "finished materialization recovery commit")
	}
}

func (l *extendedLogger) waitingForDocsLogFn(round int) func() {
	l.waitingForDocsStart = time.Now()
	l.log(log.Fields{"round": round}, "started waiting for documents")
	return func() {
		l.log(log.Fields{"round": round}, "waiting for documents")
	}
}

func (l *extendedLogger) finishedWaitingForDocsLogFn(round int) func() {
	return func() {
		l.log(log.Fields{"round": round, "took": time.Since(l.waitingForDocsStart).String()}, "finished waiting for documents")
	}
}

// BindingEvents is used to log when certain events happen in the course of
// processing materialization transactions. Its usage is not necessary, but it
// may provide useful diagnostic information for materializations that support
// it these events.
//
// It is not activate unless the materialization is configured to use
// ExtendedLogging, or unless debug logging is enabled for the task.
type BindingEvents struct {
	enabled              bool
	log                  func(log.Fields, string)
	wg                   sync.WaitGroup
	stopLogger           chan struct{}
	round                int
	evaluatingLoadsStart time.Time

	mu           sync.Mutex
	activeStores map[string]time.Time
}

func newBindingEvents() *BindingEvents {
	return &BindingEvents{activeStores: make(map[string]time.Time)}
}

func (l *BindingEvents) activate(ll loggerAtLevel) {
	l.enabled = true
	l.log = ll.log
}

func (l *BindingEvents) do(fn func()) {
	if !l.enabled {
		return
	}
	fn()
}

// StartedEvaluatingLoads should be called when a materialization that stages
// request load keys for later evaluation begins evaluating those loads. For
// example, this would be called when a load query is issued against a
// destination database.
func (l *BindingEvents) StartedEvaluatingLoads() {
	l.do(func() {
		l.log(log.Fields{"round": l.round}, "started evaluating loads")
		l.evaluatingLoadsStart = time.Now()

		l.stopLogger = make(chan struct{})
		l.wg.Add(1)
		go func() {
			ticker := time.NewTicker(loggingFrequency)
			defer func() {
				l.wg.Done()
				ticker.Stop()
			}()

			for {
				select {
				case <-ticker.C:
					l.log(log.Fields{"round": l.round}, "evaluting loads")
				case <-l.stopLogger:
					l.log(log.Fields{"round": l.round, "took": time.Since(l.evaluatingLoadsStart).String()}, "finished evaluating loads")
					return
				}
			}
		}()
	})
}

// FinishedEvaluatingLoads should be called when evaluation of loads is
// complete, but prior to starting to process the loaded document response.
func (l *BindingEvents) FinishedEvaluatingLoads() {
	l.do(func() {
		close(l.stopLogger)
		l.wg.Wait()
	})
}

// StartedResourceCommit reports when documents are being committed to a
// specific binding.
func (l *BindingEvents) StartedResourceCommit(path []string) {
	l.do(func() {
		l.mu.Lock()
		l.activeStores[strings.Join(path, ".")] = time.Now()
		l.mu.Unlock()

		l.log(log.Fields{"round": l.round - 1, "resourcePath": path}, "started commiting documents for resource")
	})
}

// FinishedResourceCommit reports when documents finish being committed to a
// specific binding.
func (l *BindingEvents) FinishedResourceCommit(path []string) {
	l.do(func() {
		l.mu.Lock()
		took := time.Since(l.activeStores[strings.Join(path, ".")])
		l.mu.Unlock()

		l.log(log.Fields{
			"round":        l.round - 1,
			"resourcePath": path,
			"took":         took.String(),
		}, "finished commiting documents for resource")
	})
}

func repeatAsync(repeat func(), every time.Duration) (stop func(onStop func())) {
	var wg sync.WaitGroup
	var doStop = make(chan struct{})

	wg.Add(1)
	go func() {
		ticker := time.NewTicker(every)
		defer func() {
			wg.Done()
			ticker.Stop()
		}()

		for {
			select {
			case <-doStop:
				return
			case <-ticker.C:
				repeat()
			}
		}
	}()

	return func(onStop func()) {
		close(doStop)
		wg.Wait()
		onStop()
	}
}
