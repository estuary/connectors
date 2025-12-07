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
		group.Go(func() error {
			defer s.ms.Close(groupCtx)

			for {
				if opTime, err := c.pullStream(groupCtx, s); err != nil {
					return fmt.Errorf("change stream for %q: %w", s.db, err)
				} else if coordinator.gotCaughtUp(s.db, opTime) && catchup {
					return nil
				}
			}
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

// pullStream pulls a new batch of documents for the change stream and processes
// them, returning the latest cluster time of any observed event. A zero-valued
// returned timestamp indicates that no documents were available for the change
// stream.
func (c *capture) pullStream(ctx context.Context, s *changeStream) (primitive.Timestamp, error) {
	var lastToken bson.Raw
	events := 0
	docs := 0
	for s.ms.TryNext(ctx) {
		events += 1

		// Transcode raw BSON to JSON - the transcoder handles split events internally.
		resp, err := c.transcoder.Transcode(s.ms.Current)
		if err != nil {
			return primitive.Timestamp{}, fmt.Errorf("transcoding event: %w", err)
		}

		// Check if this was a skipped event.
		if resp.IsSkip {
			log.WithFields(log.Fields{
				"database":   resp.Database,
				"collection": resp.Collection,
				"reason":     resp.Reason,
			}).Debug("transcoder skipped event")
			continue
		}

		// Look up binding for this collection.
		binding, tracked := c.trackedChangeStreamBindings[resourceId(resp.Database, resp.Collection)]
		if !tracked {
			continue
		}

		lastToken = getToken(s)
		docs += 1

		stateUpdate := captureState{
			DatabaseResumeTokens: map[string]bson.Raw{s.db: lastToken},
		}

		cpJson, err := json.Marshal(stateUpdate)
		if err != nil {
			return primitive.Timestamp{}, fmt.Errorf("serializing stream checkpoint: %w", err)
		}
		if err := c.output.DocumentsAndCheckpoint(cpJson, true, binding.index, resp.Document); err != nil {
			return primitive.Timestamp{}, fmt.Errorf("outputting document and checkpoint: %w", err)
		}

		if s.ms.RemainingBatchLength() == 0 || resp.CompletedSplitEvent {
			// Yield control back to the caller once this batch of documents has
			// been fully read, or when we've just completed a split event (which
			// may have required fetching additional batches for the fragments).
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
