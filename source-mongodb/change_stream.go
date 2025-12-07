package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"

	m "github.com/estuary/connectors/go/materialize"
	"github.com/segmentio/encoding/json"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
)

var (
	// changeStream associates a *mongo.ChangeStream with which database it is for.
	// Change stream checkpoints that are a post-batch-resume-token only with no
	// documents are throttled slightly, to minimize spamming checkpoints constantly
	// even for change streams with no new data.
	pbrtCheckpointInterval = 1 * time.Minute
)

type changeStream struct {
	ms                 *mongo.ChangeStream
	db                 string
	lastPbrtCheckpoint time.Time
}

// streamEvent represents a single raw event copied from the cursor.
// The raw bytes must be copied because the MongoDB cursor reuses its internal buffer.
type streamEvent struct {
	raw           bson.Raw // Copied raw BSON from cursor
	resumeToken   bson.Raw // Resume token after this event
	batchComplete bool     // True when this is the last event in a MongoDB cursor batch
	err           error    // Non-nil if producer encountered an error
}

// initializeStreams starts change streams for the capture. Streams are started from any prior
// resume tokens stored in the capture's state.
func (c *capture) initializeStreams(
	ctx context.Context,
	changeStreamBindings []bindingInfo,
	maxAwaitTime *time.Duration,
	requestPreImages bool,
	useStartAfter bool,
	exclusiveCollectionFilter bool,
	excludeCollections map[string][]string,
) ([]*changeStream, error) {
	var out []*changeStream

	// Used if exclusiveCollectionFilter is enabled.
	var collsPerDb = make(map[string][]string)
	for _, b := range changeStreamBindings {
		collsPerDb[b.resource.Database] = append(collsPerDb[b.resource.Database], b.resource.Collection)
	}

	for _, db := range databasesForBindings(changeStreamBindings) {
		logEntry := log.WithField("db", db)

		// Use a pipeline to project the fields we need from the change stream. Importantly, this
		// suppresses fields like `updateDescription`, which contain a list of fields in the document
		// that were updated. If a large field is updated, it will appear both in the `fullDocument` and
		// `updateDescription`, which can cause the change stream total BSON document size to exceed the
		// 16MB limit.
		pl := mongo.Pipeline{
			{{Key: "$project", Value: bson.M{
				"documentKey":              1,
				"operationType":            1,
				"fullDocument":             1,
				"ns":                       1,
				"fullDocumentBeforeChange": 1,
				"splitEvent":               1,
			}}},
		}

		if exclusiveCollectionFilter {
			// Build a filter that only matches documents in MongoDB collections for the enabled
			// bindings of this database.
			var collectionFilters bson.A

			for _, coll := range collsPerDb[db] {
				collectionFilters = append(collectionFilters, bson.D{{
					Key: "ns", Value: bson.D{
						{Key: "$eq", Value: bson.D{
							{Key: "db", Value: db},
							{Key: "coll", Value: coll},
						}},
					}}})
			}

			pl = append(pl, bson.D{{Key: "$match", Value: bson.D{{Key: "$or", Value: collectionFilters}}}})
		} else if exclude := excludeCollections[db]; len(exclude) > 0 {
			// Specifically exclude listed collections for this database.
			pl = append(pl, bson.D{{Key: "$match", Value: bson.D{{
				Key:   "ns.coll",
				Value: bson.D{{Key: "$nin", Value: exclude}},
			}}}})
		}

		opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
		if maxAwaitTime != nil {
			opts = opts.SetMaxAwaitTime(*maxAwaitTime)
		}
		if requestPreImages {
			opts = opts.SetFullDocumentBeforeChange(options.WhenAvailable)
			pl = append(pl, bson.D{{Key: "$changeStreamSplitLargeEvent", Value: bson.D{}}}) // must be the last stage in the pipeline
		}

		if t, ok := c.state.DatabaseResumeTokens[db]; ok {
			logEntry = logEntry.WithField("resumeToken", t)
			if useStartAfter {
				opts = opts.SetStartAfter(t)
			} else {
				opts = opts.SetResumeAfter(t)
			}
		}

		ms, err := c.client.Database(db).Watch(ctx, pl, opts)
		if err != nil {
			return nil, fmt.Errorf("initializing change stream on database %q: %w", db, err)
		}

		logEntry.Debug("intialized change stream")

		out = append(out, &changeStream{
			ms: ms,
			db: db,
		})
	}

	return out, nil
}

// streamChanges repeatedly calls `(*ChangeStream).TryNext` via (*capture).pullStream, emitting and
// checkpointing any documents that result. `TryNext` is used instead of the forever-blocking `Next`
// to allow for checkpoint of "post-batch resume tokens", even if the change stream did emit any
// change events. `TryNext` uses a default time window of 1 second in which events can arrive before
// it returns. Under the hood, `TryNext` issues a "getMore" on the change stream cursor, and this
// loop provides similar behavior as blocking on `Next` which repeatedly issues "getMore" queries as
// well. See
// https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.md#why-do-we-need-to-expose-the-postbatchresumetoken
// for more information about post-batch resume tokens.
func (c *capture) streamChanges(
	ctx context.Context,
	coordinator *streamBackfillCoordinator,
	streams []*changeStream,
	catchup bool,
) error {
	group, groupCtx := errgroup.WithContext(ctx)

	for _, s := range streams {
		s := s // Capture loop variable for goroutine

		group.Go(func() error {
			defer s.ms.Close(groupCtx)

			// Create channel for producer-consumer communication
			events := make(chan streamEvent, 32) // Buffer events for prefetching

			// Start producer in separate goroutine
			producerCtx, cancelProducer := context.WithCancel(groupCtx)
			defer cancelProducer()

			producerDone := make(chan struct{})
			go func() {
				defer close(producerDone)
				c.produceStreamEvents(producerCtx, s, events)
			}()

			// Consumer loop
			for ev := range events {
				if ev.err != nil {
					return fmt.Errorf("change stream for %q: %w", s.db, ev.err)
				}

				opTime, _, err := c.processEvent(groupCtx, s, ev)
				if err != nil {
					return fmt.Errorf("processing event for %q: %w", s.db, err)
				}

				if catchup && coordinator.gotCaughtUp(s.db, opTime) {
					cancelProducer() // Stop producer
					<-producerDone   // Wait for producer to finish
					return nil
				}
			}

			<-producerDone // Wait for producer
			return nil
		})
	}

	streamsDone := m.RunAsyncOperation(func() error { return group.Wait() })

	// Log progress while the change streams are running.
	initialProcessed, initialEmitted, _ := c.changeStreamProgress()
	logProgress := func() error {
		nextProcessed, nextEmitted, clusterTimes := c.changeStreamProgress()
		currentOpTime, err := getClusterOpTime(ctx, c.client)
		if err != nil {
			return fmt.Errorf("getting cluster op time: %w", err)
		}

		// "lag" is an estimate of how far behind we are on reading change
		// streams event for each database change stream.
		lag := make(map[string]string)
		for db, latestEventTs := range clusterTimes {
			lag[db] = (time.Duration(currentOpTime.T-latestEventTs.T) * time.Second).String()
		}

		if nextProcessed != initialProcessed {
			log.WithFields(log.Fields{
				"events":                nextProcessed - initialProcessed,
				"docs":                  nextEmitted - initialEmitted,
				"latestClusterOpTime":   currentOpTime,
				"lastEventClusterTimes": clusterTimes,
				"lag":                   lag,
			}).Info("processed change stream events")
		} else {
			log.Info("change stream idle")
		}

		initialProcessed = nextProcessed
		initialEmitted = nextEmitted
		return nil
	}

	for {
		select {
		case <-time.After(streamLoggerInterval):
			if err := logProgress(); err != nil {
				return err
			}
		case <-streamsDone.Done():
			if err := logProgress(); err != nil {
				// Avoid clobbering an error which is likely more causal if the
				// change streams completed prematurely with an error.
				log.WithError(err).Warn("error logging change stream progress")
			}
			return streamsDone.Err()
		}
	}
}

func getToken(s *changeStream) bson.Raw {
	tok := s.ms.ResumeToken()
	if tok == nil {
		// This should never happen. But if it did it would be very
		// difficult to debug.
		panic("change stream resume token is nil")
	}
	return tok
}

// produceStreamEvents fetches events from the change stream cursor and sends them through
// the channel to be processed by a consumer. This allows prefetching events from
// MongoDB while the current event is being processed.
func (c *capture) produceStreamEvents(ctx context.Context, s *changeStream, events chan<- streamEvent) {
	defer close(events)

	for {
		hasEvents := false

		for s.ms.TryNext(ctx) {
			hasEvents = true

			// IMPORTANT: Copy raw bytes - cursor reuses internal buffer
			rawCopy := make(bson.Raw, len(s.ms.Current))
			copy(rawCopy, s.ms.Current)

			tokenCopy := make(bson.Raw, len(getToken(s)))
			copy(tokenCopy, getToken(s))

			ev := streamEvent{
				raw:           rawCopy,
				resumeToken:   tokenCopy,
				batchComplete: s.ms.RemainingBatchLength() == 0,
			}

			select {
			case events <- ev:
			case <-ctx.Done():
				return
			}
		}

		// Check for cursor errors
		if err := s.ms.Err(); err != nil {
			select {
			case events <- streamEvent{err: fmt.Errorf("change stream error: %w", err)}:
			case <-ctx.Done():
			}
			return
		}

		// When idle (no events), send an empty batch-complete signal with the current resume token.
		// This allows consumers waiting for batch completion to proceed.
		if !hasEvents {
			tokenCopy := make(bson.Raw, len(getToken(s)))
			copy(tokenCopy, getToken(s))

			select {
			case events <- streamEvent{resumeToken: tokenCopy, batchComplete: true}:
			case <-ctx.Done():
				return
			}

			// Small sleep to avoid busy-waiting
			select {
			case <-time.After(10 * time.Millisecond):
			case <-ctx.Done():
				return
			}
		}
	}
}

// processEvent processes a single event from the producer, transcoding and emitting the document.
// It returns the latest cluster time of the event and whether a logical batch boundary was reached.
// A batch boundary is reached when either the MongoDB cursor batch is exhausted OR a split event
// was completed (all fragments merged).
func (c *capture) processEvent(
	ctx context.Context,
	s *changeStream,
	ev streamEvent,
) (primitive.Timestamp, bool, error) {
	// Empty event (idle batch-complete signal) - emit PBRT checkpoint if conditions are met
	if ev.raw == nil {
		if ev.resumeToken != nil && ev.batchComplete && time.Since(s.lastPbrtCheckpoint) > pbrtCheckpointInterval {
			if err := c.emitPbrtCheckpoint(s, ev.resumeToken); err != nil {
				return primitive.Timestamp{}, false, err
			}
		}
		return primitive.Timestamp{}, true, nil
	}

	// Transcode through the Rust subprocess
	resp, err := c.transcoder.Transcode(ev.raw)
	if err != nil {
		return primitive.Timestamp{}, false, fmt.Errorf("transcoding event: %w", err)
	}

	// A logical batch boundary is reached when either the MongoDB cursor batch is exhausted
	// OR a split event was completed (all fragments merged).
	batchComplete := ev.batchComplete || resp.CompletedSplitEvent

	c.mu.Lock()
	c.processedStreamEvents++
	c.mu.Unlock()

	if resp.IsSkip {
		log.WithFields(log.Fields{
			"database":   resp.Database,
			"collection": resp.Collection,
			"reason":     resp.Reason,
		}).Debug("transcoder skipped event")
		// If this is the last event in a batch, emit PBRT checkpoint if conditions are met
		if ev.batchComplete && time.Since(s.lastPbrtCheckpoint) > pbrtCheckpointInterval {
			if err := c.emitPbrtCheckpoint(s, ev.resumeToken); err != nil {
				return primitive.Timestamp{}, false, err
			}
		}
		// For skip events (e.g., intermediate split fragments), don't signal a batch
		// completion since no document was emitted.
		return primitive.Timestamp{}, false, nil
	}

	binding, tracked := c.trackedChangeStreamBindings[resourceId(resp.Database, resp.Collection)]
	if !tracked {
		// If this is the last event in a batch, emit PBRT checkpoint if conditions are met
		if ev.batchComplete && time.Since(s.lastPbrtCheckpoint) > pbrtCheckpointInterval {
			if err := c.emitPbrtCheckpoint(s, ev.resumeToken); err != nil {
				return primitive.Timestamp{}, false, err
			}
		}
		// For untracked bindings, don't signal a batch completion since no document was emitted.
		return primitive.Timestamp{}, false, nil
	}

	stateUpdate := captureState{
		DatabaseResumeTokens: map[string]bson.Raw{s.db: ev.resumeToken},
	}

	cpJson, err := json.Marshal(stateUpdate)
	if err != nil {
		return primitive.Timestamp{}, false, fmt.Errorf("serializing stream checkpoint: %w", err)
	}
	if err := c.output.DocumentsAndCheckpoint(cpJson, true, binding.index, resp.Document); err != nil {
		return primitive.Timestamp{}, false, fmt.Errorf("outputting document and checkpoint: %w", err)
	}

	c.mu.Lock()
	c.emittedStreamDocs++
	c.state.DatabaseResumeTokens[s.db] = ev.resumeToken
	c.mu.Unlock()

	lastOpTime, err := extractTimestamp(ev.resumeToken)
	if err != nil {
		return primitive.Timestamp{}, false, fmt.Errorf("extracting timestamp from resume token: %w", err)
	}

	c.mu.Lock()
	if !lastOpTime.IsZero() {
		c.lastEventClusterTime[s.db] = lastOpTime
	}
	c.mu.Unlock()

	return lastOpTime, batchComplete, nil
}

// emitPbrtCheckpoint emits a post-batch resume token checkpoint.
func (c *capture) emitPbrtCheckpoint(s *changeStream, token bson.Raw) error {
	stateUpdate := captureState{
		DatabaseResumeTokens: map[string]bson.Raw{s.db: token},
	}
	cpJson, err := json.Marshal(stateUpdate)
	if err != nil {
		return fmt.Errorf("serializing pbrt stream checkpoint: %w", err)
	}
	if err := c.output.Checkpoint(cpJson, true); err != nil {
		return fmt.Errorf("outputting pbrt stream checkpoint: %w", err)
	}
	s.lastPbrtCheckpoint = time.Now()

	c.mu.Lock()
	c.state.DatabaseResumeTokens[s.db] = token
	c.mu.Unlock()

	return nil
}

func (c *capture) changeStreamProgress() (int, int, map[string]primitive.Timestamp) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.processedStreamEvents, c.emittedStreamDocs, c.lastEventClusterTime
}

func resourceId(database string, collection string) string {
	return fmt.Sprintf("%s.%s", database, collection)
}

// extractTimestamp extracts the timestamp from a resume token. The hex-encoded
// bytes of the resume take contain a 4-byte timestamp and a 4-byte increment,
// which are used to create a primitive.Timestamp.
//
// When sorted, MongoDB resume tokens are in timestamp order, so this structure
// is expected to stay stable.
func extractTimestamp(token bson.Raw) (primitive.Timestamp, error) {
	dataValue, err := token.LookupErr("_data")
	if err != nil {
		return primitive.Timestamp{}, fmt.Errorf("_data field not found in resume token")
	}

	dataString, ok := dataValue.StringValueOK()
	if !ok {
		return primitive.Timestamp{}, fmt.Errorf("_data field of type %q was not a string", dataValue.Type.String())
	}

	payloadBytes, err := hex.DecodeString(dataString)
	if err != nil {
		return primitive.Timestamp{}, fmt.Errorf("failed to hex-decode token payload: %w", err)
	}

	if l := len(payloadBytes); l < 9 {
		return primitive.Timestamp{}, fmt.Errorf("token payload %d too short for timestamp extraction", l)
	}

	// Extract timestamp (bytes 1-4) and increment (bytes 5-8).
	timestampSeconds := binary.BigEndian.Uint32(payloadBytes[1:5])
	increment := binary.BigEndian.Uint32(payloadBytes[5:9])

	return primitive.Timestamp{T: timestampSeconds, I: increment}, nil
}
