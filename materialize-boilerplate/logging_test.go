package boilerplate

import (
	"sync"
	"testing"

	log "github.com/sirupsen/logrus"
)

func TestExtendedLoggerWaitingForDocsRace(t *testing.T) {
	// TODO(whb): If we ever start running our tests with the -race flag
	// enabled, this outer loop won't be necessary. As is, running the enclosed
	// sequence ~100 times or so will reliably produce a panic unless sufficient
	// synchronization is provided in the extended logger event handler.
	for idx := 0; idx < 100; idx++ {
		be := newBindingEvents()
		logger := newExtendedLogger(loggerAtLevel{lvl: log.InfoLevel}, be)
		handler := logger.handler()

		handler(sentStartedCommit)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			for idx := 0; idx < 10; idx++ {
				handler(readLoad)
			}
			wg.Done()
		}()

		go func() {
			handler(sentAcknowledged)
			wg.Done()
		}()

		wg.Wait()
	}
}

func TestLoads(t *testing.T) {
	for _, tt := range []struct {
		name string
		do   func(func(transactionsEvent))
	}{
		{
			"no load requests",
			func(handler func(transactionsEvent)) {
				handler(readFlush)
				handler(sentFlushed)
			},
		},
		{
			"no load results",
			func(handler func(transactionsEvent)) {
				handler(readLoad)
				handler(readFlush)
				handler(sentFlushed)
			},
		},
		{
			"staged with results",
			func(handler func(transactionsEvent)) {
				handler(readLoad)
				handler(readFlush)
				handler(sentLoaded)
				handler(sentFlushed)
			},
		},
		{
			"not staged with results",
			func(handler func(transactionsEvent)) {
				handler(readLoad)
				handler(sentLoaded)
				handler(readFlush)
				handler(sentFlushed)
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			be := newBindingEvents()
			logger := newExtendedLogger(loggerAtLevel{lvl: log.InfoLevel}, be)
			handler := logger.handler()
			tt.do(handler)
		})

	}
}
