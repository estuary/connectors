package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/segmentio/encoding/json"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
)

// backfillChangeStreamCollections backfills all capture collections until time runs out, or the backfill is
// complete. Each round of backfilling the bindings will be randomized to provide some degree of
// equality in how they are backfilled, but within a single round of backfilling a binding will be
// backfilled continuously until it finishes.
func (c *capture) backfillChangeStreamCollections(
	ctx context.Context,
	coordinator *streamBackfillCoordinator,
	bindings []bindingInfo,
	backfillFor time.Duration,
) error {
	ts := time.Now()
	log.WithField("backfillFor", backfillFor.String()).Info("backfilling collections")
	defer func() { log.WithField("took", time.Since(ts).String()).Info("finished backfill round") }()

	shuffledBindings := make([]bindingInfo, 0, len(bindings))
	for _, b := range bindings {
		if b.resource.getMode() == captureModeChangeStream {
			shuffledBindings = append(shuffledBindings, b)
		}
	}
	rand.Shuffle(len(shuffledBindings), func(i, j int) {
		shuffledBindings[i], shuffledBindings[j] = shuffledBindings[j], shuffledBindings[i]
	})

	backfillBindings := make(chan bindingInfo)
	stopBackfill := make(chan struct{})

	go func() {
		<-time.After(backfillFor)
		close(stopBackfill)
	}()

	group, groupCtx := errgroup.WithContext(ctx)

	for idx := 0; idx < concurrentBackfillLimit; idx++ {
		group.Go(func() error {
			return c.backfillWorker(groupCtx, coordinator, backfillBindings, stopBackfill)
		})
	}

	// Push bindings to backfill to the backfill workers until time runs out. Each worker backfills
	// a single binding until it is done backfilling, and is then able to start working on another.
	for _, b := range shuffledBindings {
		c.mu.Lock()
		done := c.state.Resources[b.stateKey].Backfill.done()
		c.mu.Unlock()
		if done {
			continue
		}

		select {
		case backfillBindings <- b:
		case <-ctx.Done():
			return ctx.Err()
		case <-groupCtx.Done():
			return group.Wait()
		case <-stopBackfill:
			return group.Wait()
		}
	}

	close(backfillBindings)
	return group.Wait()
}

func (c *capture) backfillWorker(
	ctx context.Context,
	coordinator *streamBackfillCoordinator,
	backfillBindings <-chan bindingInfo,
	stopBackfill <-chan struct{},
) error {
	for {
		select {
		case binding, ok := <-backfillBindings:
			if !ok {
				return nil
			}

			if err := c.doBackfill(ctx, coordinator, binding, stopBackfill); err != nil {
				return fmt.Errorf(
					"backfilling collection %q in database %q: %w",
					binding.resource.Collection,
					binding.resource.Database,
					err,
				)
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-stopBackfill:
			return nil
		}
	}
}

// doBackfill runs a backfill for a binding to completion, or until signalled to
// stop by stopBackfill.
func (c *capture) doBackfill(
	ctx context.Context,
	coordinator *streamBackfillCoordinator,
	binding bindingInfo,
	stopBackfill <-chan struct{},
) error {
	var sk = binding.stateKey

	c.mu.Lock()
	lastId := c.state.Resources[sk].Backfill.LastId
	initialDocsCaptured := c.state.Resources[sk].Backfill.BackfilledDocs
	additionalDocsCaptured := 0
	c.mu.Unlock()

	collection := c.client.Database(binding.resource.Database).Collection(binding.resource.Collection)
	// Not using the more precise `CountDocuments()` here since that requires a full collection
	// scan. Getting an estimate from the collection's metadata is very fast and should be close
	// enough for useful logging.
	estimatedTotalDocs, err := collection.EstimatedDocumentCount(ctx)
	if err != nil {
		return fmt.Errorf("getting estimated document count: %w", err)
	}

	// By not specifying a sort parameter, MongoDB uses natural sort to order documents. Natural
	// sort is approximately insertion order (but not guaranteed). We hint to MongoDB to use the _id
	// index (an index that always exists) to speed up the process. Note that if we specify the sort
	// explicitly by { $natural: 1 }, then the database will disregard any indices and do a full
	// collection scan. See https://www.mongodb.com/docs/manual/reference/method/cursor.hint
	var opts = options.Find().SetHint(bson.M{"_id": 1})

	var filter = bson.D{}
	if lastId != nil {
		var v any
		if err := lastId.Unmarshal(&v); err != nil {
			return fmt.Errorf("unmarshalling last_id: %w", err)
		}
		filter = bson.D{{Key: idProperty, Value: bson.D{{Key: "$gt", Value: v}}}}
	}

	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return fmt.Errorf("collection.Find: %w", err)
	}
	defer cursor.Close(ctx)

	logEntry := log.WithFields(log.Fields{
		"database":           binding.resource.Database,
		"collection":         binding.resource.Collection,
		"estimatedTotalDocs": estimatedTotalDocs,
	})
	logEntry.Debug("starting backfill for collection")

	for {
		select {
		case <-stopBackfill:
			if additionalDocsCaptured != 0 {
				newTotal := initialDocsCaptured + additionalDocsCaptured
				complete := fmt.Sprintf("%.0f", float64(newTotal)/float64(estimatedTotalDocs)*100)

				logEntry.WithFields(log.Fields{
					"docsCapturedThisRound": additionalDocsCaptured,
					"totalDocsCaptured":     newTotal,
					"percentComplete":       complete,
				}).Info("progressed backfill for collection")
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-coordinator.streamsCaughtUp():
			docCount, err := c.pullCursor(ctx, cursor, binding)
			if err != nil {
				return fmt.Errorf("pullCursor: %w", err)
			}

			if docCount == 0 {
				logEntry.WithField("totalDocsCaptured", initialDocsCaptured+additionalDocsCaptured).Info("completed backfill for collection")
				return nil
			}

			additionalDocsCaptured += docCount
		}
	}
}

// pullCursor requests a batch of documents for the provided cursor. It returns
// when either the current batch of documents has been processed and another
// network call would be needed for more, or the cursor is exhausted and the
// collection scan is complete. Checkpointing and updating the in-memory state
// for processed documents is handled within this function call.
func (c *capture) pullCursor(
	ctx context.Context,
	cursor *mongo.Cursor,
	binding bindingInfo,
) (int, error) {
	var sk = binding.stateKey
	// TODO(whb): Revisit this batching strategy as part of a more holistic
	// memory optimization effort.
	var docBatch []json.RawMessage
	var lastId bson.RawValue
	var batchBytes int
	var docsRead int

	emitDocs := func() error {
		c.mu.Lock()
		state := c.state.Resources[sk]
		if state.Backfill.done() {
			// First commit of a re-polled batch binding.
			state.Backfill.Done = makePtr(false)
			state.Backfill.BackfilledDocs = 0
			state.Backfill.LastPollStart = makePtr(time.Now())
		}
		state.Backfill.LastId = &lastId
		state.Backfill.BackfilledDocs += len(docBatch)
		checkpoint := captureState{
			Resources: map[boilerplate.StateKey]resourceState{
				sk: {Backfill: state.Backfill},
			},
		}
		c.mu.Unlock()

		if cpJson, err := json.Marshal(checkpoint); err != nil {
			return fmt.Errorf("serializing checkpoint: %w", err)
		} else if err := c.output.DocumentsAndCheckpoint(cpJson, true, binding.index, docBatch...); err != nil {
			return fmt.Errorf("outputting documents and checkpoint: %w", err)
		}

		docsRead += len(docBatch)
		// Reset for the next batch.
		docBatch = nil
		batchBytes = 0

		return nil
	}

	done := true
	for cursor.Next(ctx) {
		var doc bson.M
		var err error
		if lastId, err = cursor.Current.LookupErr(idProperty); err != nil {
			return 0, fmt.Errorf("looking up idProperty: %w", err)
		} else if err = cursor.Decode(&doc); err != nil {
			return 0, fmt.Errorf("backfill decoding document: %w", err)
		}

		docJson, err := makeBackfillDocument(binding, doc)
		if err != nil {
			return 0, fmt.Errorf("making backfill document: %w", err)
		}

		docBatch = append(docBatch, docJson)
		batchBytes += len(docJson)

		if len(docBatch) > backfillCheckpointSize || batchBytes > backfillBytesSize {
			if err := emitDocs(); err != nil {
				return 0, fmt.Errorf("emitting backfill batch: %w", err)
			}
		}

		if cursor.RemainingBatchLength() == 0 {
			done = false
			break
		}
	}
	if err := cursor.Err(); err != nil {
		return 0, fmt.Errorf("backfill cursor error: %w", err)
	}

	if len(docBatch) > 0 {
		if err := emitDocs(); err != nil {
			return 0, fmt.Errorf("emitting final backfill batch: %w", err)
		}
	}

	if done {
		c.mu.Lock()
		state := c.state.Resources[sk]
		state.Backfill.Done = makePtr(true)
		c.state.Resources[sk] = state
		c.mu.Unlock()

		checkpoint := captureState{
			Resources: map[boilerplate.StateKey]resourceState{
				sk: {Backfill: backfillState{Done: makePtr(true)}},
			},
		}

		if cp, err := json.Marshal(checkpoint); err != nil {
			return 0, fmt.Errorf("serializing checkpoint: %w", err)
		} else if err := c.output.Checkpoint(cp, true); err != nil {
			return 0, fmt.Errorf("outputting checkpoint: %w", err)
		}
	}

	return docsRead, nil
}

func makeBackfillDocument(binding bindingInfo, doc primitive.M) (json.RawMessage, error) {
	doc[metaProperty] = map[string]any{
		opProperty: "c",
		sourceProperty: sourceMeta{
			DB:         binding.resource.Database,
			Collection: binding.resource.Collection,
			Snapshot:   true,
		},
	}
	doc = sanitizeDocument(doc)

	docJson, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("serializing document: %w", err)
	}

	return docJson, err
}
