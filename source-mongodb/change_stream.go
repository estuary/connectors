package main

import (
	"bytes"
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

// streamEvent represents a single raw event from the change stream with its metadata.
type streamEvent struct {
	raw         bson.Raw // Copied from cursor (cursor reuses buffers!)
	resumeToken bson.Raw // Resume token after this event
	isSplit     bool     // Whether this is a split event fragment
}

// streamBatch represents a batch of events to be processed together.
type streamBatch struct {
	events     []streamEvent // Events in this batch
	finalToken bson.Raw      // Final resume token for the batch (for pbrt checkpointing)
	err        error         // Non-nil if producer encountered error
}

type docKey struct {
	Id interface{} `bson:"_id"`
}

type namespace struct {
	Database   string `bson:"db"`
	Collection string `bson:"coll"`
}

type bsonFields struct {
	DocumentKey              docKey    `bson:"documentKey"`
	OperationType            string    `bson:"operationType"`
	FullDocument             bson.M    `bson:"fullDocument"`
	FullDocumentBeforeChange bson.M    `bson:"fullDocumentBeforeChange"`
	Ns                       namespace `bson:"ns"`
}

type changeEvent struct {
	fields      bsonFields
	fragments   int
	binding     bindingInfo
	isDocument  bool
	resumeToken bson.Raw // Resume token for this event (for checkpointing)
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
			batches := make(chan *streamBatch, 2) // Buffer 2 batches

			// Start producer in separate goroutine
			producerCtx, cancelProducer := context.WithCancel(groupCtx)
			defer cancelProducer()

			producerDone := make(chan struct{})
			go func() {
				defer close(producerDone)
				c.produceStreamBatches(producerCtx, s, batches)
			}()

			// Consumer runs in this goroutine
			for batch := range batches {
				if batch.err != nil {
					return fmt.Errorf("change stream for %q: %w", s.db, batch.err)
				}

				opTime, err := c.processBatch(groupCtx, s, batch)
				if err != nil {
					return fmt.Errorf("processing batch for %q: %w", s.db, err)
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

// produceStreamBatches fetches events from the change stream cursor and sends them through the
// channel to be processed by a consumer. It handles split event fragment assembly by detecting
// split events and calling Next() to fetch all fragments before sending the batch.
func (c *capture) produceStreamBatches(ctx context.Context, s *changeStream, batches chan<- *streamBatch) {
	defer close(batches)

	for {
		batch := &streamBatch{events: make([]streamEvent, 0, 16)}

		for s.ms.TryNext(ctx) {
			// must copy as cursor reuses internal buffer
			rawCopy := make(bson.Raw, len(s.ms.Current))
			copy(rawCopy, s.ms.Current)

			tokenCopy := make(bson.Raw, len(getToken(s)))
			copy(tokenCopy, getToken(s))

			event := streamEvent{
				raw:         rawCopy,
				resumeToken: tokenCopy,
				isSplit:     !s.ms.Current.Lookup("splitEvent").IsZero(),
			}
			batch.events = append(batch.events, event)

			// If this is a split event, fetch ALL fragments now
			if event.isSplit {
				var se splitEvent
				if err := s.ms.Current.Lookup("splitEvent").Unmarshal(&se); err != nil {
					batch.err = fmt.Errorf("unmarshaling splitEvent: %w", err)
					select {
					case batches <- batch:
					case <-ctx.Done():
					}
					return
				}

				// Keep fetching fragments until we have all of them
				for se.Fragment != se.Of {
					if !s.ms.Next(ctx) {
						batch.err = fmt.Errorf("fetching split event fragment: %w", s.ms.Err())
						select {
						case batches <- batch:
						case <-ctx.Done():
						}
						return
					}

					// Add this fragment to the batch (copy the data)
					fragRawCopy := make(bson.Raw, len(s.ms.Current))
					copy(fragRawCopy, s.ms.Current)

					fragTokenCopy := make(bson.Raw, len(getToken(s)))
					copy(fragTokenCopy, getToken(s))

					fragment := streamEvent{
						raw:         fragRawCopy,
						resumeToken: fragTokenCopy,
						isSplit:     true,
					}
					batch.events = append(batch.events, fragment)

					// Check if this is the last fragment
					if err := s.ms.Current.Lookup("splitEvent").Unmarshal(&se); err != nil {
						batch.err = fmt.Errorf("unmarshaling splitEvent fragment: %w", err)
						select {
						case batches <- batch:
						case <-ctx.Done():
						}
						return
					}
				}
			}

			// End of batch from MongoDB
			if s.ms.RemainingBatchLength() == 0 {
				break
			}
		}

		// Check for cursor errors
		if err := s.ms.Err(); err != nil {
			batch.err = fmt.Errorf("change stream error: %w", err)
			select {
			case batches <- batch:
			case <-ctx.Done():
			}
			return
		}

		// Capture the final token for this batch (for pbrt checkpointing)
		token := getToken(s)
		finalTokenCopy := make(bson.Raw, len(token))
		copy(finalTokenCopy, token)
		batch.finalToken = finalTokenCopy

		// Send batch to consumer (respect context cancellation)
		select {
		case batches <- batch:
		case <-ctx.Done():
			return
		}

		// If we sent an error, exit
		if batch.err != nil {
			return
		}

		// Idle handling: small sleep when no events to avoid busy-waiting
		if len(batch.events) == 0 {
			select {
			case <-time.After(10 * time.Millisecond):
			case <-ctx.Done():
				return
			}
		}
	}
}

// processBatch processes a batch of events from the producer, emitting documents and checkpoints.
// It returns the latest cluster time of any observed event in the batch.
func (c *capture) processBatch(
	ctx context.Context,
	s *changeStream,
	batch *streamBatch,
) (primitive.Timestamp, error) {
	var lastToken bson.Raw
	events := 0
	docs := 0

	// Process each event in the batch
	for i := 0; i < len(batch.events); {
		ev := &batch.events[i]

		var changeEv changeEvent
		fragmentCount, err := c.processEvent(ev, batch.events[i+1:], s.db, &changeEv)
		if err != nil {
			return primitive.Timestamp{}, fmt.Errorf("processing event: %w", err)
		}

		i += fragmentCount
		events += fragmentCount

		if changeEv.isDocument {
			lastToken = changeEv.resumeToken
			docs += 1

			stateUpdate := captureState{
				DatabaseResumeTokens: map[string]bson.Raw{s.db: lastToken},
			}

			if doc, err := makeEventDocument(changeEv, s.db, changeEv.binding.resource.Collection); err != nil {
				return primitive.Timestamp{}, fmt.Errorf("making change event document: %w", err)
			} else if cpJson, err := json.Marshal(stateUpdate); err != nil {
				return primitive.Timestamp{}, fmt.Errorf("serializing stream checkpoint: %w", err)
			} else if err := c.output.DocumentsAndCheckpoint(cpJson, true, changeEv.binding.index, doc); err != nil {
				return primitive.Timestamp{}, fmt.Errorf("outputting document and checkpoint: %w", err)
			}
		}
	}

	// Post-batch resume token (pbrt) checkpoint
	finalToken := batch.finalToken
	if !bytes.Equal(finalToken, lastToken) && time.Since(s.lastPbrtCheckpoint) > pbrtCheckpointInterval {
		stateUpdate := captureState{
			DatabaseResumeTokens: map[string]bson.Raw{s.db: finalToken},
		}

		if cpJson, err := json.Marshal(stateUpdate); err != nil {
			return primitive.Timestamp{}, fmt.Errorf("serializing pbrt stream checkpoint: %w", err)
		} else if err := c.output.Checkpoint(cpJson, true); err != nil {
			return primitive.Timestamp{}, fmt.Errorf("outputting pbrt stream checkpoint: %w", err)
		}
		s.lastPbrtCheckpoint = time.Now()
	}

	lastOpTime, err := extractTimestamp(finalToken)
	if err != nil {
		return primitive.Timestamp{}, fmt.Errorf("extracting timestamp from resume token: %w", err)
	}

	// Update in-memory state
	c.mu.Lock()
	c.state.DatabaseResumeTokens[s.db] = finalToken
	c.processedStreamEvents += events
	c.emittedStreamDocs += docs
	if !lastOpTime.IsZero() {
		c.lastEventClusterTime[s.db] = lastOpTime
	}
	c.mu.Unlock()

	return lastOpTime, nil
}

// processEvent processes a single event (or split event sequence) from the batch.
// It returns the number of fragments consumed from the batch.
func (c *capture) processEvent(
	firstEvent *streamEvent,
	remainingEvents []streamEvent,
	db string,
	ev *changeEvent,
) (fragmentCount int, err error) {
	ev.resumeToken = firstEvent.resumeToken

	var binding bindingInfo
	var trackedBinding bool

	if firstEvent.isSplit {
		// Assemble from multiple events in the batch
		fragmentCount, err = c.assembleFragments(firstEvent, remainingEvents, ev)
		if err != nil {
			return 0, fmt.Errorf("assembling fragments: %w", err)
		}

		// Check if this collection is tracked
		if binding, trackedBinding = c.trackedChangeStreamBindings[resourceId(db, ev.fields.Ns.Collection)]; !trackedBinding {
			// Assembled event is not for a tracked collection. This is
			// difficult to check prior to reading all the fragments for the
			// event, since which fragment will contain the 'ns' field is
			// unknown.
			return fragmentCount, nil
		}
	} else {
		// Single event
		fragmentCount = 1

		// Quick check if collection is tracked
		if collRaw, lookupErr := firstEvent.raw.LookupErr("ns", "coll"); lookupErr == nil {
			// LookupErr will only error if the `ns.coll` field is missing,
			// which is only known to happen with dropDatabase and invalidate
			// events, which are handled specifically a little further down.
			if binding, trackedBinding = c.trackedChangeStreamBindings[resourceId(db, collRaw.StringValue())]; !trackedBinding {
				return 1, nil
			}
		}

		if err := bson.Unmarshal(firstEvent.raw, &ev.fields); err != nil {
			return 0, fmt.Errorf("unmarshaling event: %w", err)
		}
	}

	// Handle operation types
	switch ev.fields.OperationType {
	case "drop", "rename":
		log.WithFields(log.Fields{
			"database":   db,
			"collection": ev.fields.Ns.Collection,
			"opType":     ev.fields.OperationType,
		}).Warn("received collection-level event for tracked collection")
		return fragmentCount, nil
	case "dropDatabase", "invalidate": // An invalidate event will always follow a dropDatabase event.
		log.WithFields(log.Fields{
			"database": db,
			"opType":   ev.fields.OperationType,
		}).Warn("received database-level event for tracked database")
		return fragmentCount, nil
	case "insert", "update", "replace", "delete":
		if ev.fields.OperationType != "delete" && ev.fields.FullDocument == nil {
			// FullDocument can be "null" for non-deletion events if another
			// operation has deleted the document. This happens because update
			// change events do not hold a copy of the full document, but rather
			// it is at query time that the full document is looked up, and in
			// these cases, the FullDocument can end up being null (if deleted)
			// or different from the deltas in the update event. We ignore
			// events where FullDocument is null. Another change event of type
			// delete will eventually come and delete the document.
			log.WithFields(log.Fields{"database": db, "collection": ev.fields.Ns.Collection}).Debug("discarding event with no FullDocument")
			return fragmentCount, nil
		}
	default:
		log.WithFields(log.Fields{
			"database":      db,
			"collection":    ev.fields.Ns.Collection,
			"operationType": ev.fields.OperationType,
		}).Debug("discarding event with un-tracked operation")
		return fragmentCount, nil
	}

	ev.isDocument = true
	ev.binding = binding
	ev.fragments = fragmentCount
	return fragmentCount, nil
}

// assembleFragments assembles a split event from multiple streamEvents in the batch.
// Fragments are repeatedly unmarshalled into a single change event since no two fragments
// have overlapping top-level properties - MongoDB only splits on top-level document fields.
// Returns the number of fragments consumed.
func (c *capture) assembleFragments(
	firstEvent *streamEvent,
	remainingEvents []streamEvent,
	ev *changeEvent,
) (int, error) {
	lastFragment := 0
	fragmentIdx := 0

	for {
		if fragmentIdx > len(remainingEvents) {
			return 0, fmt.Errorf("ran out of fragments in batch (expected more fragments)")
		}

		var current *streamEvent
		if fragmentIdx == 0 {
			current = firstEvent
		} else {
			current = &remainingEvents[fragmentIdx-1]
		}

		var se splitEvent
		if splitRaw, err := current.raw.LookupErr("splitEvent"); err != nil {
			return 0, fmt.Errorf("finding splitEvent in document fragment: %w", err)
		} else if err := splitRaw.Unmarshal(&se); err != nil {
			return 0, fmt.Errorf("unmarshaling splitEvent: %w", err)
		} else if lastFragment += 1; lastFragment != se.Fragment { // cheap sanity check
			return 0, fmt.Errorf("expected fragment %d but got %d", lastFragment, se.Fragment)
		}

		// Decode this fragment into the changeEvent (fields accumulate)
		if err := bson.Unmarshal(current.raw, &ev.fields); err != nil {
			return 0, fmt.Errorf("decoding fragment: %w", err)
		}

		if se.Fragment == se.Of {
			// Done reading fragments
			break
		}

		fragmentIdx++
	}

	if ev.fields.Ns.Collection == "" {
		// This should never happen, since split events should always be
		// collection documents.
		return 0, fmt.Errorf("assembling fragments produced an event with no collection")
	}

	return fragmentIdx + 1, nil
}

// pullStream pulls a new batch of documents for the change stream and processes
// them, returning the latest cluster time of any observed event. A zero-valued
// returned timestamp indicates that no documents were available for the change
// stream.
type splitEvent struct {
	Fragment int `bson:"fragment"`
	Of       int `bson:"of"`
}

// makeEventDocument processes a change event and returns a document if the change event is for a
// tracked collection and operation type. The binding index is also returned if a document is
// available from the change event. If no document is applicable to the change event, the returned
// map will be `nil`.
func makeEventDocument(ev changeEvent, database string, collection string) (json.RawMessage, error) {
	var fields = ev.fields

	var doc map[string]any
	meta := map[string]any{
		sourceProperty: sourceMeta{
			DB:         database,
			Collection: collection,
		},
	}
	if fields.FullDocumentBeforeChange != nil {
		meta[beforeProperty] = fields.FullDocumentBeforeChange
	}

	switch fields.OperationType {
	case "insert", "update", "replace":
		if fields.OperationType == "update" || fields.OperationType == "replace" {
			meta[opProperty] = "u"
		} else if fields.OperationType == "insert" {
			meta[opProperty] = "c"
		}
		doc = fields.FullDocument
	case "delete":
		meta[opProperty] = "d"
		doc = map[string]any{idProperty: fields.DocumentKey.Id}
	default:
		return nil, fmt.Errorf("unknown operation: %q", fields.OperationType)
	}

	doc[metaProperty] = meta

	docBytes, err := json.Marshal(sanitizeDocument(doc))
	if err != nil {
		return nil, fmt.Errorf("serializing document: %w", err)
	}

	return docBytes, nil
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
