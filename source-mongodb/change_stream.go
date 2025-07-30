package main

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"time"

	m "github.com/estuary/connectors/go/protocols/materialize"
	"github.com/segmentio/encoding/json"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
)

// disableRetriesForTesting can be set `true` when running tests that require
// timely operation.
var disableRetriesForTesting bool

// changeStream associates a *mongo.ChangeStream with which database it is for.
type changeStream struct {
	ms *mongo.ChangeStream
	db string
}

type docKey struct {
	Id interface{} `bson:"_id"`
}

type namespace struct {
	Database   string `bson:"db"`
	Collection string `bson:"coll"`
}

type bsonFields struct {
	DocumentKey              docKey              `bson:"documentKey"`
	OperationType            string              `bson:"operationType"`
	FullDocument             bson.M              `bson:"fullDocument"`
	FullDocumentBeforeChange bson.M              `bson:"fullDocumentBeforeChange"`
	Ns                       namespace           `bson:"ns"`
	ClusterTime              primitive.Timestamp `bson:"clusterTime"`
}

type changeEvent struct {
	fields     bsonFields
	fragments  int
	binding    bindingInfo
	isDocument bool
}

// initializeStreams starts change streams for the capture. Streams are started from any prior
// resume tokens stored in the capture's state.
func (c *capture) initializeStreams(
	ctx context.Context,
	changeStreamBindings []bindingInfo,
	requestPreImages bool,
	exclusiveCollectionFilter bool,
	excludeCollections map[string][]string,
) ([]changeStream, error) {
	var out []changeStream

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
				"clusterTime":              1,
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
		if requestPreImages {
			opts = opts.SetFullDocumentBeforeChange(options.WhenAvailable)
			pl = append(pl, bson.D{{Key: "$changeStreamSplitLargeEvent", Value: bson.D{}}}) // must be the last stage in the pipeline
		}

		if t, ok := c.state.DatabaseResumeTokens[db]; ok {
			logEntry = logEntry.WithField("resumeToken", t)
			opts = opts.SetResumeAfter(t)
		}

		ms, err := c.client.Database(db).Watch(ctx, pl, opts)
		if err != nil {
			return nil, fmt.Errorf("initializing change stream on database %q: %w", db, err)
		}

		logEntry.Debug("intialized change stream")

		out = append(out, changeStream{
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
	streams []changeStream,
	catchup bool,
) error {
	group, groupCtx := errgroup.WithContext(ctx)

	for _, s := range streams {
		group.Go(func() error {
			defer s.ms.Close(groupCtx)

			for {
				var err error
				var opTime primitive.Timestamp
				for attempt := range 10 {
					// For catching up streams, require 10 consecutive attempts
					// yielding no documents before the stream is considered
					// caught-up, in the absence of a specific event timestamp
					// that can be used to positively confirm caught-up-ness.
					ll := log.WithFields(log.Fields{"db": s.db, "attempt": attempt})
					if opTime, err = c.pullStream(groupCtx, s); err != nil {
						return fmt.Errorf("change stream for %q: %w", s.db, err)
					} else if !opTime.IsZero() || !catchup || disableRetriesForTesting {
						if attempt > 0 {
							ll.Info("change stream catch-up was retried and fetched documents")
						}
						break
					}
					ll.Debug("change stream returned no documents during catch-up, retrying")
					time.Sleep(1 * time.Second)
				}

				if coordinator.gotCaughtUp(s.db, opTime) && catchup {
					return nil
				}
			}
		})
	}

	streamsDone := m.RunAsyncOperation(func() error { return group.Wait() })

	// Log progress while the change streams are running.
	initialProcessed, initialEmitted, _ := c.changeStreamProgress()
	for {
		select {
		case <-time.After(streamLoggerInterval):
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
		case <-streamsDone.Done():
			return streamsDone.Err()
		}
	}
}

func getToken(s changeStream) bson.Raw {
	tok := s.ms.ResumeToken()
	if tok == nil {
		// This should never happen. But if it did it would be very
		// difficult to debug.
		panic("change stream resume token is nil")
	}
	return tok
}

// pullStream pulls a new batch of documents for the change stream and processes
// them, returning the latest cluster time of any observed event. A zero-valued
// returned timestamp indicates that no documents were available for the change
// stream.
func (c *capture) pullStream(ctx context.Context, s changeStream) (primitive.Timestamp, error) {
	var lastToken bson.Raw
	var lastOpTime primitive.Timestamp
	events := 0
	docs := 0
	for s.ms.TryNext(ctx) {
		var ev changeEvent
		requestedAnotherBatch, err := c.readEvent(ctx, s, &ev)
		if err != nil {
			return primitive.Timestamp{}, fmt.Errorf("reading change stream event: %w", err)
		}
		lastOpTime = ev.fields.ClusterTime
		events += ev.fragments

		if ev.isDocument {
			lastToken = getToken(s)
			docs += 1

			stateUpdate := captureState{
				DatabaseResumeTokens: map[string]bson.Raw{s.db: lastToken},
			}

			if doc, err := makeEventDocument(ev, s.db, ev.binding.resource.Collection); err != nil {
				return primitive.Timestamp{}, fmt.Errorf("making change event document: %w", err)
			} else if cpJson, err := json.Marshal(stateUpdate); err != nil {
				return primitive.Timestamp{}, fmt.Errorf("serializing stream checkpoint: %w", err)
			} else if err := c.output.DocumentsAndCheckpoint(cpJson, true, ev.binding.index, doc); err != nil {
				return primitive.Timestamp{}, fmt.Errorf("outputting document and checkpoint: %w", err)
			}
		}

		if s.ms.RemainingBatchLength() == 0 || requestedAnotherBatch {
			// Yield control back to the caller once this batch of documents has
			// been fully read, or an additional batch of documents was needed
			// to complete a split event document.
			if s.ms.RemainingBatchLength() != 0 {
				log.WithFields(log.Fields{
					"remainingBatchLength": s.ms.RemainingBatchLength(),
				}).Debug("pullStream is returning with a partial batch requested from readFragments")
			}
			break
		}
	}
	if err := s.ms.Err(); err != nil {
		return primitive.Timestamp{}, fmt.Errorf("change stream ended with error: %w", err)
	}

	// Checkpoint the final resume token from the change stream, which may correspond to the last
	// event received, but will often also be a "post batch resume token", which does not correspond
	// to an event for this stream, but is rather a resumable token representing the oplog entry the
	// change stream has scanned up to on the server (not necessarily a matching change). This will
	// be present even if TryNext returned no documents and allows us to keep the position of the
	// change stream fresh even if we are watching a database that has infrequent events.
	finalToken := getToken(s)
	if !bytes.Equal(finalToken, lastToken) {
		stateUpdate := captureState{
			DatabaseResumeTokens: map[string]bson.Raw{s.db: finalToken},
		}

		if cpJson, err := json.Marshal(stateUpdate); err != nil {
			return primitive.Timestamp{}, fmt.Errorf("serializing pbrt stream checkpoint: %w", err)
		} else if err := c.output.Checkpoint(cpJson, true); err != nil {
			return primitive.Timestamp{}, fmt.Errorf("outputting pbrt stream checkpoint: %w", err)
		}
	}

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

type splitEvent struct {
	Fragment int `bson:"fragment"`
	Of       int `bson:"of"`
}

func (c *capture) readEvent(
	ctx context.Context,
	s changeStream,
	ev *changeEvent,
) (requestedAnotherBatch bool, err error) {
	var binding bindingInfo
	var trackedBinding bool

	splitRaw := s.ms.Current.Lookup("splitEvent")
	if !splitRaw.IsZero() {
		// This change event is split across multiple change stream event
		// "fragments". Each of the fragments in the sequence must be read
		// and combined. This will require reading at least 1 additional
		// event from the change stream, which may involve requesting a new
		// batch.
		ev.fragments, requestedAnotherBatch, err = readFragments(ctx, s, ev)
		if err != nil {
			return false, fmt.Errorf("assembling event from change stream event segments: %w", err)
		}

		if binding, trackedBinding = c.trackedChangeStreamBindings[resourceId(s.db, ev.fields.Ns.Collection)]; !trackedBinding {
			// Assembled event is not for a tracked collection. This is
			// difficult to check prior to reading all the fragments for the
			// event, since which fragment will contain the 'ns' field is
			// unknown.
			return
		}
	} else {
		// This change event is not split.
		ev.fragments = 1
		if collRaw, lookupErr := s.ms.Current.LookupErr("ns", "coll"); lookupErr != nil {
			return false, fmt.Errorf("looking up 'ns.coll' field: %w", lookupErr)
		} else if binding, trackedBinding = c.trackedChangeStreamBindings[resourceId(s.db, collRaw.StringValue())]; !trackedBinding {
			// Event is not for a tracked collection. A quick check can be
			// done prior to fully unmarshalling the change event BSON to
			// fast-track bailing out on the very common case of events that
			// are for collections that are not being captured.
			if clusterTimeRaw, err := s.ms.Current.LookupErr("clusterTime"); err != nil {
				return false, fmt.Errorf("looking up clusterTime: %w", err)
			} else if err := clusterTimeRaw.Unmarshal(&ev.fields.ClusterTime); err != nil {
				return false, fmt.Errorf("unmarshalling clusterTime: %w", err)
			}
			return
		} else if err := s.ms.Decode(&ev.fields); err != nil {
			return false, fmt.Errorf("change stream decoding document: %w", err)
		}
	}

	var logFields log.Fields
	if log.GetLevel() == log.DebugLevel {
		logFields = log.Fields{
			"_id":                   idToString(ev.fields.DocumentKey.Id),
			"changeStreamDatabase":  s.db,
			"operationType":         ev.fields.OperationType,
			"fragments":             ev.fragments,
			"requestedAnotherBatch": requestedAnotherBatch,
		}
	}

	if !slices.Contains([]string{"insert", "update", "replace", "delete"}, ev.fields.OperationType) {
		// Event is not for a tracked operation type.
		log.WithFields(logFields).Debug("discarding event with un-tracked operation")
		return
	} else if ev.fields.OperationType != "delete" && ev.fields.FullDocument == nil {
		// FullDocument can be "null" for non-deletion events if another
		// operation has deleted the document. This happens because update
		// change events do not hold a copy of the full document, but rather
		// it is at query time that the full document is looked up, and in
		// these cases, the FullDocument can end up being null (if deleted)
		// or different from the deltas in the update event. We ignore
		// events where FullDocument is null. Another change event of type
		// delete will eventually come and delete the document.
		log.WithFields(logFields).Debug("discarding event with no FullDocument")
		return
	}

	ev.isDocument = true
	ev.binding = binding
	return
}

func readFragments(
	ctx context.Context,
	s changeStream,
	ev *changeEvent,
) (numSplitEvents int, requestedAnotherBatch bool, _ error) {
	// Fragments are repeatedly unmarshalled into a single change event since no
	// two fragments have overlapping top-level properties - MongoDB only splits
	// on top-level document fields.
	lastFragment := 0
	for {
		// Upon entering this loop, the change stream must already have been
		// advanced to a split change event. For the duration of the loop it is
		// an error if an event is not split, since we only want to read to
		// "end" of the current set of fragments.
		var se splitEvent
		if splitRaw, err := s.ms.Current.LookupErr("splitEvent"); err != nil {
			return 0, false, fmt.Errorf("finding splitEvent in document fragment: %w", err)
		} else if err := splitRaw.Unmarshal(&se); err != nil {
			return 0, false, err
		} else if lastFragment += 1; lastFragment != se.Fragment { // cheap sanity check
			return 0, false, fmt.Errorf("expected fragment %d but got %d", lastFragment, se.Fragment)
		} else if err := s.ms.Decode(&ev.fields); err != nil {
			return 0, false, fmt.Errorf("decoding fragment: %w", err)
		} else if se.Fragment == se.Of { // done reading fragments
			break
		}

		// Advance to the next fragment, blocking until it is available. This
		// may involve requesting a new batch of documents from the network.
		if s.ms.RemainingBatchLength() == 0 {
			requestedAnotherBatch = true
		}
		if !s.ms.Next(ctx) {
			return 0, false, fmt.Errorf("advancing change stream: %w", s.ms.Err())
		}
	}

	if ev.fields.Ns.Collection == "" {
		// This should never happen, since split events should always be
		// collection documents.
		return 0, false, fmt.Errorf("reading fragments produced an event with no collection")
	}

	return lastFragment, requestedAnotherBatch, nil
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
