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
func newAuxStream(stream m.MaterializeStream) *auxStream {
	s := &auxStream{stream: stream}
	l := newBasicLogger()
	s.handler = l.handler()

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
