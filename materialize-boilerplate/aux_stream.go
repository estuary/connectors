package boilerplate

import (
	"sync"
	"time"

	m "github.com/estuary/connectors/go/protocols/materialize"
	pm "github.com/estuary/flow/go/protocols/materialize"
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
	sentAcknowledged
)

type auxStream struct {
	stream  m.MaterializeStream
	handler func(transactionsEvent)
}

// newAuxStream wraps a base stream MaterializeStream with extra capabilities
// via specific handling for events.
func newAuxStream(stream m.MaterializeStream, lvl log.Level, options MaterializeOptions) *auxStream {
	s := &auxStream{stream: stream}

	if options.ExtendedLogging {
		l := newExtendedLogger(log.InfoLevel)
		s.handler = l.handler()
	} else if lvl > log.InfoLevel {
		// If debug logging (or a more verbose level) is enabled, log extended
		// events at the configured level.
		l := newExtendedLogger(lvl)
		s.handler = l.handler()
	} else {
		l := newBasicLogger()
		s.handler = l.handler()
	}

	return s
}

func (l *auxStream) Send(m *pm.Response) error {
	if m.Loaded != nil {
		l.handler(sentLoaded)
	} else if m.Flushed != nil {
		l.handler(sentFlushed)
	} else if m.StartedCommit != nil {
		l.handler(sentStartedCommit)
	} else if m.Acknowledged != nil {
		l.handler(sentAcknowledged)
	}

	return l.stream.Send(m)
}

func (l *auxStream) RecvMsg(m *pm.Request) error {
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

type basicLogger struct {
	mu                  sync.Mutex
	round               int
	readLoads           int
	sentLoaded          int
	readStores          int
	waitingForDocsStart time.Time
	commitStart         time.Time
}

// newBasicLogger creates a logger that periodically logs some basic information
// about the progress of the materialization at the INFO level.
func newBasicLogger() *basicLogger {
	var l = new(basicLogger)
	go l.runLogger()

	return l
}

func (l *basicLogger) runLogger() {
	ticker := time.NewTicker(loggingFrequency)
	defer ticker.Stop()

	for {
		<-ticker.C
		func() {
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
			} else if !l.commitStart.IsZero() && time.Since(l.commitStart) > slowOperationThreshold {
				ll.WithField("started", l.commitStart.UTC().String()).Info("materialization commit has been running for a long time")
			} else {
				ll.Info("materialization progress")
			}
		}()
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

		case sentAcknowledged:
			if !loadPhaseStarted {
				l.waitingForDocsStart = time.Now()
			}
			l.commitStart = time.Time{}
			recovery = false

		case readLoad, readFlush:
			l.waitingForDocsStart = time.Time{}
			loadPhaseStarted = true
		}
	}
}

type extendedLogger struct {
	lvl log.Level

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
}

// newExtendedLogger returns a logger than logs detailed information about the
// materialization as it progresses through transaction steps. It can log at
// INFO or DEBUG levels, which may be useful for materializations that always
// what detailed logs, or only if debug logging is enabled.
func newExtendedLogger(lvl log.Level) *extendedLogger {
	return &extendedLogger{lvl: lvl}
}

// logCb logs something when called. It's a bit frivolous as a type definition,
// but is a bit more readable than func(func()).
type logCb func()

// logAsync starts an async logger and returns a function that can be used to
// stop it. The asyncLog logging callback is run periodically and while holding
// the mutex for extendedLogger. The doneLog callback does not acquire the lock.
func (l *extendedLogger) logAsync(asyncLog logCb) (stopLogger func(doneLog logCb)) {
	var wg sync.WaitGroup
	var doStop = make(chan struct{})

	wg.Add(1)
	go func() {
		ticker := time.NewTicker(loggingFrequency)
		defer func() {
			wg.Done()
			ticker.Stop()
		}()

		for {
			select {
			case <-doStop:
				return
			case <-ticker.C:
				l.mu.Lock()
				asyncLog()
				l.mu.Unlock()
			}
		}
	}()

	return func(doneLog logCb) {
		close(doStop)
		wg.Wait()
		doneLog()
	}
}

func (l *extendedLogger) handler() func(transactionsEvent) {
	var stopLoadLogger func(logCb)
	var stopStoreLogger func(logCb)
	var stopWaitingForDocsLogger func(logCb)
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

		// Increment the "round" counter and establish logging for the case of
		// "waiting for documents".
		switch event {
		case sentStartedCommit:
			loadPhaseStarted = false
			l.round++

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
		case sentAcknowledged:
			if recovery {
				stopStoreLogger(l.finishedRecoveryCommitLogFn())
				recovery = false
			} else {
				stopStoreLogger(l.finishedCommitLogFn(l.round - 1))
			}
		}
	}
}

func (l *extendedLogger) readingLoadsLogFn() logCb {
	l.readingLoadsStart = time.Now()
	log.WithField("round", l.round).Log(l.lvl, "started reading load requests")
	lastLoadCount := l.readLoads

	return func() {
		if l.readLoads == lastLoadCount {
			log.WithFields(log.Fields{
				"round": l.round,
				"count": l.readLoads,
			}).Log(l.lvl, "waiting for more load requests")
		} else {
			log.WithFields(log.Fields{
				"round": l.round,
				"count": l.readLoads,
			}).Log(l.lvl, "reading load requests")
		}
		lastLoadCount = l.readLoads
	}
}

func (l *extendedLogger) finishedReadingLoadsLogFn() logCb {
	return func() {
		log.WithFields(log.Fields{
			"round": l.round,
			"took":  time.Since(l.readingLoadsStart).String(),
			"count": l.readLoads,
		}).Log(l.lvl, "finished reading load requests")
	}
}

func (l *extendedLogger) processingLoadedsLogFn() logCb {
	l.sendingLoadedsStart = time.Now()
	log.WithField("round", l.round).Log(l.lvl, "started processing loaded documents")
	return func() {
		log.WithFields(log.Fields{
			"round": l.round,
			"count": l.sentLoaded,
		}).Log(l.lvl, "processing loaded documents")
	}
}

func (l *extendedLogger) finishedProcessingLoadedsLogFn() logCb {
	return func() {
		log.WithFields(log.Fields{
			"round": l.round,
			"took":  time.Since(l.sendingLoadedsStart).String(),
			"count": l.sentLoaded,
		}).Log(l.lvl, "finished processing loaded documents")
	}
}

func (l *extendedLogger) readingStoresLogFn() logCb {
	l.readingStoresStart = time.Now()
	log.WithField("round", l.round).Log(l.lvl, "started reading store requests")
	return func() {
		log.WithFields(log.Fields{
			"round": l.round,
			"count": l.readStores,
		}).Log(l.lvl, "reading store requests")
	}
}

func (l *extendedLogger) finishedReadingStoresLogFn() logCb {
	return func() {
		log.WithFields(log.Fields{
			"round": l.round,
			"took":  time.Since(l.readingStoresStart).String(),
			"count": l.readStores,
		}).Log(l.lvl, "finished reading store requests")
	}
}

func (l *extendedLogger) runningCommitLogFn(round int) logCb {
	l.commitStart = time.Now()
	log.WithField("round", round).Log(l.lvl, "started materialization commit")
	return func() {
		log.WithFields(log.Fields{
			"round": round,
		}).Log(l.lvl, "materialization commit in progress")
	}
}

func (l *extendedLogger) finishedCommitLogFn(round int) logCb {
	return func() {
		log.WithFields(log.Fields{
			"round": round,
			"took":  time.Since(l.commitStart).String(),
		}).Log(l.lvl, "finished materialization commit")
	}
}

func (l *extendedLogger) runningRecoveryCommitLogFn() logCb {
	l.commitStart = time.Now()
	log.Info("started materialization recovery commit")
	return func() { log.Info("materialization recovery commit in progress") }
}

func (l *extendedLogger) finishedRecoveryCommitLogFn() logCb {
	return func() {
		log.WithFields(log.Fields{
			"took": time.Since(l.commitStart).String(),
		}).Info("finished materialization recovery commit")
	}
}

func (l *extendedLogger) waitingForDocsLogFn() logCb {
	l.waitingForDocsStart = time.Now()
	log.Info("started waiting for documents")
	return func() {
		log.WithFields(log.Fields{
			"round": l.round,
		}).Info("waiting for documents")
	}
}

func (l *extendedLogger) finishedWaitingForDocsLogFn() logCb {
	return func() {
		log.WithFields(log.Fields{
			"round": l.round,
			"took":  time.Since(l.waitingForDocsStart).String(),
		}).Info("finished waiting for documents")
	}
}
