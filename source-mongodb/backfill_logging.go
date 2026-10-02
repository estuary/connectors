package main

import (
	"context"
	"errors"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
)

const backfillLoggerInterval = time.Minute

// backfillProgress reports independently of the worker so blocked calls remain visible.
type backfillProgress struct {
	log          *log.Entry
	started      time.Time
	done         chan struct{}
	stopped      chan struct{}
	stopBackfill <-chan struct{}

	mu                sync.Mutex
	operation         string
	operationStarted  time.Time
	docsEmitted       int
	docsReceived      int
	cursorID          int64
	lastEmittedCursor any
}

func startBackfillProgress(ll *log.Entry, stopBackfill <-chan struct{}, interval time.Duration) *backfillProgress {
	now := time.Now()
	p := &backfillProgress{
		log:              ll,
		started:          now,
		done:             make(chan struct{}),
		stopped:          make(chan struct{}),
		stopBackfill:     stopBackfill,
		operation:        "preparing query",
		operationStarted: now,
	}
	go func() {
		defer close(p.stopped)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-p.done:
				return
			case <-stopBackfill:
				stopBackfill = nil
				p.entry().Info("backfill time budget expired")
			case <-ticker.C:
				p.entry().Info("backfill progress")
			}
		}
	}()
	return p
}

func (p *backfillProgress) setOperation(operation string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.operation = operation
	p.operationStarted = time.Now()
}

func (p *backfillProgress) emitted(count int, cursor bson.RawValue) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.docsEmitted += count
	p.lastEmittedCursor = cursor.String()
}

func (p *backfillProgress) received(count int, cursorID int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.docsReceived += count
	p.cursorID = cursorID
}

func (p *backfillProgress) entry() *log.Entry {
	var budgetExpired bool
	select {
	case <-p.stopBackfill:
		budgetExpired = true
	default:
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.log.WithFields(log.Fields{
		"elapsed":           time.Since(p.started).String(),
		"operation":         p.operation,
		"operationElapsed":  time.Since(p.operationStarted).String(),
		"docsEmitted":       p.docsEmitted,
		"docsReceived":      p.docsReceived,
		"cursorID":          p.cursorID,
		"lastEmittedCursor": p.lastEmittedCursor,
		"budgetExpired":     budgetExpired,
	})
}

func (p *backfillProgress) finish(outcome string, err error) {
	close(p.done)
	<-p.stopped
	ll := p.entry()
	if err != nil {
		outcome = "failed"
		if errors.Is(err, context.Canceled) {
			outcome = "cancelled"
		}
		ll = ll.WithError(err)
	}
	ll.WithField("outcome", outcome).Info("finished backfill for collection")
}
