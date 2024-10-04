package boilerplate

import (
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	// How often various the various async loggers will report their status.
	loggingFrequency = 5 * time.Second

	// When the "basic" logger is being used, how long a commit must be in
	// progress or how long the connector must be waiting for documents from the
	// runtime before it logs something about that.
	slowOperationThreshold = 15 * time.Second
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

	mu                  sync.Mutex
	round               int
	readLoads           int
	sentLoaded          int
	readStores          int
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
	return repeatAsync(func() {
		l.mu.Lock()
		asyncLog()
		l.mu.Unlock()
	}, loggingFrequency)
}

func (l *extendedLogger) handler() func(transactionsEvent) {
	var stopLoadLogger func(func())
	var stopStoreLogger func(func())
	var stopWaitingForDocsLogger func(func())
	var loadPhaseStarted bool
	var ackDelayActive bool
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

		// Increment the "round" counter and establish logging for the case of
		// "waiting for documents".
		switch event {
		case sentStartedCommit:
			loadPhaseStarted = false
			l.round++
			l.be.round++
		case sentAcknowledged:
			if !loadPhaseStarted {
				stopWaitingForDocsLogger = l.logAsync(l.waitingForDocsLogFn())
			}
		case readLoad, readFlush:
			if !l.waitingForDocsStart.IsZero() {
				stopWaitingForDocsLogger(l.finishedWaitingForDocsLogFn())
			}
			l.waitingForDocsStart = time.Time{}
			loadPhaseStarted = true
		}

		// Start and stop other loggers, resetting counters as needed.
		switch event {
		case readLoad:
			if l.readLoads == 1 {
				stopLoadLogger = l.logAsync(l.readingLoadsLogFn())
			}
		case readFlush:
			if l.readLoads != 0 {
				stopLoadLogger(l.finishedReadingLoadsLogFn())
				l.readLoads = 0
			}
		case sentLoaded:
			if l.sentLoaded == 1 {
				stopLoadLogger = l.logAsync(l.processingLoadedsLogFn())
			}
		case sentFlushed:
			if l.sentLoaded != 0 {
				stopLoadLogger(l.finishedProcessingLoadedsLogFn())
				l.sentLoaded = 0
			}
		case readStore:
			if l.readStores == 1 {
				stopStoreLogger = l.logAsync(l.readingStoresLogFn())
			}
		case readStartCommit:
			if l.readStores != 0 {
				stopStoreLogger(l.finishedReadingStoresLogFn())
				l.readStores = 0
			}
		case sentStartedCommit:
			// NB: The "round" is incremented in on sentStartedCommit prior to
			// the handling here and below by stopStoreLogger. This means that
			// the round for the commit is actually one less than currently
			// recorded.
			stopStoreLogger = l.logAsync(l.runningCommitLogFn(l.round - 1))
		case readAcknowledge:
			if recovery {
				stopStoreLogger = l.logAsync(l.runningRecoveryCommitLogFn())
			}
		case startedAckDelay:
			// NB: Ack delay is never used for the recovery commit.
			stopStoreLogger(l.finishedCommitLogFn(l.round - 1))
			stopStoreLogger = l.logAsync(l.waitingForAckDelayLogFn(l.round - 1))
			ackDelayActive = true
		case sentAcknowledged:
			if recovery {
				stopStoreLogger(l.finishedRecoveryCommitLogFn())
				recovery = false
			} else if ackDelayActive {
				stopStoreLogger(l.finishedAckDelayLogFn(l.round - 1))
				ackDelayActive = false
			} else {
				stopStoreLogger(l.finishedCommitLogFn(l.round - 1))
			}
		}
	}
}

func (l *extendedLogger) readingLoadsLogFn() func() {
	l.readingLoadsStart = time.Now()
	l.log(log.Fields{"round": l.round}, "started reading load requests")
	lastLoadCount := l.readLoads

	return func() {
		if l.readLoads == lastLoadCount {
			l.log(log.Fields{"round": l.round, "count": l.readLoads}, "waiting for more load requests")
		} else {
			l.log(log.Fields{"round": l.round, "count": l.readLoads}, "reading load requests")
		}
		lastLoadCount = l.readLoads
	}
}

func (l *extendedLogger) finishedReadingLoadsLogFn() func() {
	return func() {
		l.log(log.Fields{
			"round": l.round,
			"took":  time.Since(l.readingLoadsStart).String(),
			"count": l.readLoads,
		}, "finished reading load requests")
	}
}

func (l *extendedLogger) processingLoadedsLogFn() func() {
	l.sendingLoadedsStart = time.Now()
	l.log(log.Fields{"round": l.round}, "started processing loaded documents")
	return func() {
		l.log(log.Fields{"round": l.round, "count": l.sentLoaded}, "processing loaded documents")
	}
}

func (l *extendedLogger) finishedProcessingLoadedsLogFn() func() {
	return func() {
		l.log(log.Fields{
			"round": l.round,
			"took":  time.Since(l.sendingLoadedsStart).String(),
			"count": l.sentLoaded,
		}, "finished processing loaded documents")
	}
}

func (l *extendedLogger) readingStoresLogFn() func() {
	l.readingStoresStart = time.Now()
	l.log(log.Fields{"round": l.round}, "started reading store requests")
	return func() {
		l.log(log.Fields{"round": l.round, "count": l.readStores}, "reading store requests")
	}
}

func (l *extendedLogger) finishedReadingStoresLogFn() func() {
	return func() {
		l.log(log.Fields{
			"round": l.round,
			"took":  time.Since(l.readingStoresStart).String(),
			"count": l.readStores,
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

func (l *extendedLogger) waitingForDocsLogFn() func() {
	l.waitingForDocsStart = time.Now()
	l.log(log.Fields{"round": l.round}, "started waiting for documents")
	return func() {
		l.log(log.Fields{"round": l.round}, "waiting for documents")
	}
}

func (l *extendedLogger) finishedWaitingForDocsLogFn() func() {
	return func() {
		l.log(log.Fields{"round": l.round, "took": time.Since(l.waitingForDocsStart).String()}, "finished waiting for documents")
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

		l.log(log.Fields{"round": l.round, "resourcePath": path}, "started commiting documents for resource")
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
			"round":        l.round,
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
