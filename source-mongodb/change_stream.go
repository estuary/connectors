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

type streamEvent struct {
	raw         bson.Raw // Copied raw BSON from cursor
	resumeToken bson.Raw // Resume token after this event
}

// streamBatch represents a batch of events from a single MongoDB cursor batch.
type streamBatch struct {
	events      []streamEvent // Events in this batch (may be empty for idle signals)
	resumeToken bson.Raw      // Resume token at end of batch (for idle signals or PBRT checkpoints)
	err         error         // Non-nil if producer encountered an error
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
	fullDocRequired map[string]bool,
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

		fullDocOpt := options.UpdateLookup
		if fullDocRequired[db] {
			logEntry.Info("using fullDocument 'required' mode (changeStreamPreAndPostImages enabled on all collections)")
			fullDocOpt = options.Required
		}
		opts := options.ChangeStream().SetFullDocument(fullDocOpt)
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

// streamChanges processes change stream events using a producer-consumer pattern:
//
// Producer (produceStreamBatches):
//   - Reads events from MongoDB cursor using TryNext
//   - Groups events into batches (one per MongoDB cursor batch)
//   - Sends batches through a buffered channel (size 4) for prefetching
//
// Consumer (this function):
//   - Receives batches from channel
//   - Transcodes BSON to JSON via Rust subprocess
//   - Emits documents and checkpoints
//
// The buffered channel provides natural backpressure: when full, the producer
// blocks on send, throttling MongoDB reads.
//
// TryNext is used instead of the blocking Next to allow checkpointing "post-batch
// resume tokens" (PBRT) even when no events arrive. See:
// https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.md#why-do-we-need-to-expose-the-postbatchresumetoken
func (c *capture) streamChanges(
	ctx context.Context,
	coordinator *streamBackfillCoordinator,
	streams []*changeStream,
	catchup bool,
) error {
	group, groupCtx := errgroup.WithContext(ctx)

	for _, s := range streams {
		group.Go(func() error {
			defer s.ms.Close(groupCtx)

			// Channel for batches with buffer size 4. This allows prefetching up to 4
			// MongoDB batches while the consumer processes.
			batches := make(chan streamBatch, 4)

			producerCtx, cancelProducer := context.WithCancel(groupCtx)
			defer cancelProducer()

			producerDone := make(chan struct{})
			go func() {
				defer close(producerDone)
				c.produceStreamBatches(producerCtx, s, batches)
			}()

			for batch := range batches {
				if batch.err != nil {
					return fmt.Errorf("change stream for %q: %w", s.db, batch.err)
				}

				opTime, err := c.processBatch(groupCtx, s, batch)
				if err != nil {
					return fmt.Errorf("processing batch for %q: %w", s.db, err)
				}

				if coordinator.gotCaughtUp(s.db, opTime) && catchup {
					cancelProducer()
					<-producerDone
					return nil
				}
			}

			<-producerDone
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

		// Get and reset throughput metrics
		c.mu.Lock()
		mongoReadTime := c.totalMongoReadTime
		transcodeTime := c.totalTranscodeTime
		bytesRead := c.totalBytesRead
		c.totalMongoReadTime = 0
		c.totalTranscodeTime = 0
		c.totalBytesRead = 0
		c.mu.Unlock()

		// "lag" is an estimate of how far behind we are on reading change
		// streams event for each database change stream.
		lag := make(map[string]string)
		for db, latestEventTs := range clusterTimes {
			lag[db] = (time.Duration(currentOpTime.T-latestEventTs.T) * time.Second).String()
		}

		if nextProcessed != initialProcessed {
			mbRead := float64(bytesRead) / (1024 * 1024)
			throughputMBps := mbRead / streamLoggerInterval.Seconds()

			log.WithFields(log.Fields{
				"events":                nextProcessed - initialProcessed,
				"docs":                  nextEmitted - initialEmitted,
				"latestClusterOpTime":   currentOpTime,
				"lastEventClusterTimes": clusterTimes,
				"lag":                   lag,
				"mongoReadTime":         mongoReadTime.String(),
				"transcodeTime":         transcodeTime.String(),
				"bytesReadMB":           fmt.Sprintf("%.2f", mbRead),
				"throughputMBps":        fmt.Sprintf("%.2f", throughputMBps),
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

// produceStreamBatches fetches batches of events from the change stream cursor and sends
// them through the channel to be processed by a consumer.
//
// Backpressure is provided by the channel buffer (size 4). When the buffer is full,
// the producer blocks on channel send, throttling reads from MongoDB.
func (c *capture) produceStreamBatches(
	ctx context.Context,
	s *changeStream,
	batches chan<- streamBatch,
) {
	defer close(batches)

	for {
		var events []streamEvent
		var batchBytes int64

		// Collect all events from the current MongoDB batch. We break when
		// RemainingBatchLength() == 0 to ensure each streamBatch corresponds
		// to exactly one MongoDB cursor batch.
		readStart := time.Now()
		for s.ms.TryNext(ctx) {
			rawCopy := make(bson.Raw, len(s.ms.Current))
			copy(rawCopy, s.ms.Current)
			batchBytes += int64(len(rawCopy))

			tokenCopy := make(bson.Raw, len(getToken(s)))
			copy(tokenCopy, getToken(s))

			events = append(events, streamEvent{
				raw:         rawCopy,
				resumeToken: tokenCopy,
			})

			if s.ms.RemainingBatchLength() == 0 {
				break
			}
		}
		readDuration := time.Since(readStart)

		c.mu.Lock()
		c.totalMongoReadTime += readDuration
		c.totalBytesRead += batchBytes
		c.mu.Unlock()

		// Check for cursor errors
		if err := s.ms.Err(); err != nil {
			select {
			case batches <- streamBatch{err: fmt.Errorf("change stream error: %w", err)}:
			case <-ctx.Done():
			}
			return
		}

		// Get the current resume token for the batch
		tokenCopy := make(bson.Raw, len(getToken(s)))
		copy(tokenCopy, getToken(s))

		select {
		case batches <- streamBatch{events: events, resumeToken: tokenCopy}:
		case <-ctx.Done():
			return
		}

		// When idle (no events in this batch), add a small sleep to avoid busy-waiting
		if len(events) == 0 {
			select {
			case <-time.After(10 * time.Millisecond):
			case <-ctx.Done():
				return
			}
		}
	}
}

type processedEvent struct {
	bindingIndex int
	document     json.RawMessage
	opTime       primitive.Timestamp
}

// handleIdleBatch processes an empty batch (idle signal), emitting a PBRT checkpoint
// if enough time has passed. Returns the opTime extracted from the resume token.
func (c *capture) handleIdleBatch(s *changeStream, token bson.Raw) (primitive.Timestamp, error) {
	if time.Since(s.lastPbrtCheckpoint) > pbrtCheckpointInterval {
		if err := c.emitPbrtCheckpoint(s, token); err != nil {
			return primitive.Timestamp{}, err
		}
	}
	opTime, err := extractTimestamp(token)
	if err != nil {
		log.WithError(err).Debug("failed to extract timestamp from idle signal resume token")
		return primitive.Timestamp{}, nil
	}
	return opTime, nil
}

// processBatch processes all events in a batch, returning the latest opTime from the batch.
// Documents are accumulated by binding and emitted in bulk using Documents(), with a single
// Checkpoint() at the end of the batch to minimize overhead.
func (c *capture) processBatch(
	ctx context.Context,
	s *changeStream,
	batch streamBatch,
) (primitive.Timestamp, error) {
	if len(batch.events) == 0 {
		return c.handleIdleBatch(s, batch.resumeToken)
	}

	var lastOpTime primitive.Timestamp

	docsByBinding := make(map[int][]json.RawMessage)
	var lastResumeToken bson.Raw
	var emittedCount int

	rawDocs := make([]bson.Raw, len(batch.events))
	for i, ev := range batch.events {
		rawDocs[i] = ev.raw
	}

	transcodeStart := time.Now()
	responses, err := c.transcoder.TranscodeBatch(rawDocs)
	transcodeDuration := time.Since(transcodeStart)
	if err != nil {
		return primitive.Timestamp{}, fmt.Errorf("transcoding batch: %w", err)
	}

	c.mu.Lock()
	c.totalTranscodeTime += transcodeDuration
	c.mu.Unlock()

	for i, resp := range responses {
		ev := batch.events[i]

		c.mu.Lock()
		c.processedStreamEvents++
		c.mu.Unlock()

		if opTime, err := extractTimestamp(ev.resumeToken); err != nil {
			return primitive.Timestamp{}, fmt.Errorf("extracting timestamp from resume token: %w", err)
		} else {
			lastOpTime = opTime
		}

		if resp.IsSkip {
			continue
		}

		binding, tracked := c.trackedChangeStreamBindings[resourceId(resp.Database, resp.Collection)]
		if !tracked {
			continue
		}

		docsByBinding[binding.index] = append(docsByBinding[binding.index], resp.Document)
		lastResumeToken = ev.resumeToken
		emittedCount++
	}

	// Emit all documents for each binding, then a single checkpoint
	if emittedCount > 0 {
		for bindingIndex, docs := range docsByBinding {
			if err := c.output.Documents(bindingIndex, docs...); err != nil {
				return primitive.Timestamp{}, fmt.Errorf("outputting documents for binding %d: %w", bindingIndex, err)
			}
		}

		stateUpdate := captureState{
			DatabaseResumeTokens: map[string]bson.Raw{s.db: lastResumeToken},
		}
		cpJson, err := json.Marshal(stateUpdate)
		if err != nil {
			return primitive.Timestamp{}, fmt.Errorf("serializing batch checkpoint: %w", err)
		}
		if err := c.output.Checkpoint(cpJson, true); err != nil {
			return primitive.Timestamp{}, fmt.Errorf("outputting batch checkpoint: %w", err)
		}

		c.mu.Lock()
		c.emittedStreamDocs += emittedCount
		c.state.DatabaseResumeTokens[s.db] = lastResumeToken
		if !lastOpTime.IsZero() {
			c.lastEventClusterTime[s.db] = lastOpTime
		}
		c.mu.Unlock()
	} else if time.Since(s.lastPbrtCheckpoint) > pbrtCheckpointInterval {
		// No documents emitted, but we can still emit a PBRT checkpoint
		lastEvent := batch.events[len(batch.events)-1]
		if err := c.emitPbrtCheckpoint(s, lastEvent.resumeToken); err != nil {
			return primitive.Timestamp{}, err
		}
	}

	return lastOpTime, nil
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
