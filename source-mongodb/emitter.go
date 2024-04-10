package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/segmentio/encoding/json"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"golang.org/x/exp/maps"
)

const (
	// When the stream logger has been started, log a progress report at this frequency. The
	// progress report is currently just the count of stream documents that have been processed,
	// which provides a nice indication in the logs of what the capture is doing when it is reading
	// change streams.
	streamLoggerInterval = 1 * time.Minute
)

// emitter provides synchronization for emitted documents and checkpoints, as well as updating the
// capture's internal state as it progresses through backfills and change streaming.
type emitter struct {
	events chan emitEvent
	errors chan error
	wg     sync.WaitGroup

	// processedStreamEvents is the number of events processed by the emitter. Not all of these will
	// result in a document output if they are for a collection or operation that we don't track.
	processedStreamEvents atomic.Uint64

	// emittedStreamDocs is the count of documents that have actually been emitted from reading the
	// change stream.
	emittedStreamDocs atomic.Uint64
}

// emitEvent is either a single document + checkpoint from a streamEvent, or potentially multiple
// documents + a checkpoint from a backfillEvent.
type emitEvent interface {
	isEvent()
}

type streamEvent struct {
	doc         map[string]any
	stateUpdate captureState
	binding     bindingInfo
}

func (streamEvent) isEvent() {}

type backfillEvent struct {
	docs        []primitive.M
	stateUpdate resourceState
	binding     bindingInfo
}

func (backfillEvent) isEvent() {}

// startEmitter is called once to initialize the emitter.
func (c *capture) startEmitter(ctx context.Context) {
	c.emitter.events = make(chan emitEvent)
	c.emitter.errors = make(chan error)

	c.emitter.wg.Add(1)
	go c.emitWorker(ctx)
}

// flushEmitter is called whenever the events channel should be completely emptied. This ensures
// that there will not be any concurrent writes to the capture's internal state until more events
// are sent to the emitter. A call to flushEmitter leaves the emitter reader to continue receiving
// emitted events.
func (c *capture) flushEmitter(ctx context.Context) error {
	close(c.emitter.events)
	c.emitter.wg.Wait()

	select {
	case err := <-c.emitter.errors:
		return err
	default:
		c.emitter.events = make(chan emitEvent)

		c.emitter.wg.Add(1)
		go c.emitWorker(ctx)

		return nil
	}
}

func (c *capture) emitWorker(ctx context.Context) {
	defer c.emitter.wg.Done()

	for {
		select {
		case event, ok := <-c.emitter.events:
			if !ok {
				// Channel is closed because of flushEmitter.
				return
			}

			switch ev := event.(type) {
			case streamEvent:
				c.emitter.processedStreamEvents.Add(1)
				if err := c.handleStreamEvent(ev); err != nil {
					c.emitter.errors <- err
					return
				}
			case backfillEvent:
				if err := c.handleBackfillEvent(ev); err != nil {
					c.emitter.errors <- err
					return
				}
			}
		case <-ctx.Done():
			c.emitter.errors <- ctx.Err()
			return
		}
	}
}

// emitEvent is a convenience method for sending events to the emitter events channel, while also
// checking for emitter errors or cancellation signals.
func (c *capture) emitEvent(ctx context.Context, e emitEvent) error {
	select {
	case err := <-c.emitter.errors:
		return err
	case c.emitter.events <- e:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *capture) handleStreamEvent(event streamEvent) error {
	if l := len(maps.Keys(event.stateUpdate.DatabaseResumeTokens)); l != 1 {
		return fmt.Errorf("application error: must set a single resume token for handleStreamEvent, got %d key/value pairs", l)
	}

	for db, tok := range event.stateUpdate.DatabaseResumeTokens {
		c.state.DatabaseResumeTokens[db] = tok
	}

	if event.doc != nil {
		// There may not be a document if this is a checkpoint-only for a post-batch resume token,
		// or if it is for an untracked operation type or collection.
		if docJson, err := json.Marshal(event.doc); err != nil {
			return fmt.Errorf("serializing stream document: %w", err)
		} else if err := c.output.Documents(event.binding.index, docJson); err != nil {
			return fmt.Errorf("outputting stream document: %w", err)
		}

		c.emitter.emittedStreamDocs.Add(1)
	}

	if checkpointJson, err := json.Marshal(event.stateUpdate); err != nil {
		return fmt.Errorf("encoding stream checkpoint: %w", err)
	} else if err := c.output.Checkpoint(checkpointJson, true); err != nil {
		return fmt.Errorf("outputting stream checkpoint: %w", err)
	}

	return nil
}

func (c *capture) handleBackfillEvent(event backfillEvent) error {
	var sk = event.binding.stateKey
	var docs = make([]json.RawMessage, 0, len(event.docs))

	checkpoint := captureState{
		Resources: map[boilerplate.StateKey]resourceState{
			sk: event.stateUpdate,
		},
	}

	state := c.state.Resources[sk]
	state.Backfill.LastId = event.stateUpdate.Backfill.LastId
	state.Backfill.Done = event.stateUpdate.Backfill.Done
	c.state.Resources[sk] = state

	for _, doc := range event.docs {
		raw, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("serializing backfill document: %w", err)
		}

		docs = append(docs, raw)
	}

	if len(docs) > 0 {
		// There may be no documents on the final "backfill complete" checkpoint.
		if err := c.output.Documents(event.binding.index, docs...); err != nil {
			return fmt.Errorf("outputting backfill documents: %w", err)
		}
	}

	if checkpointJson, err := json.Marshal(checkpoint); err != nil {
		return fmt.Errorf("encoding backfill checkpoint: %w", err)
	} else if err := c.output.Checkpoint(checkpointJson, true); err != nil {
		return fmt.Errorf("outputting backfill checkpoint: %w", err)
	}

	return nil
}

func (c *capture) startStreamLogger() {
	c.streamLoggerStop = make(chan struct{})
	c.streamLoggerActive.Add(1)

	go func() {
		defer func() { c.streamLoggerActive.Done() }()

		initialProcessed := c.emitter.processedStreamEvents.Load()
		initialEmitted := c.emitter.emittedStreamDocs.Load()

		for {
			select {
			case <-time.After(streamLoggerInterval):
				nextProcessed := c.emitter.processedStreamEvents.Load()
				nextEmitted := c.emitter.emittedStreamDocs.Load()

				if nextProcessed != initialProcessed {
					log.WithFields(log.Fields{
						"events": nextProcessed - initialProcessed,
						"docs":   nextEmitted - initialEmitted,
					}).Info("processed change stream events")
				} else {
					log.Info("change stream idle")
				}

				initialProcessed = nextProcessed
				initialEmitted = nextEmitted
			case <-c.streamLoggerStop:
				return
			}

		}
	}()
}

func (c *capture) stopStreamLogger() {
	close(c.streamLoggerStop)
	c.streamLoggerActive.Wait()
}
