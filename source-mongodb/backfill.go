package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/segmentio/encoding/json"

	"github.com/estuary/connectors/go/schedule"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
)

// backfillBinding is the information needed to backfill a binding.
type backfillBinding struct {
	binding      bindingInfo
	backfillDone chan struct{}
}

// backfillChangeStreamCollections backfills all capture collections until time runs out, or the backfill is
// complete. Each round of backfilling the bindings will be randomized to provide some degree of
// equality in how they are backfilled, but within a single round of backfilling a binding will be
// backfilled continuously until it finishes.
func (c *capture) backfillChangeStreamCollections(
	ctx context.Context,
	coordinator *streamBackfillCoordinator,
	changeStreamBindings []bindingInfo,
	backfillFor time.Duration,
) error {
	ts := time.Now()
	log.WithField("backfillFor", backfillFor.String()).Info("backfilling collections")
	defer func() { log.WithField("took", time.Since(ts).String()).Info("finished backfill round") }()

	shuffledBindings := make([]bindingInfo, len(changeStreamBindings))
	copy(shuffledBindings, changeStreamBindings)
	rand.Shuffle(len(shuffledBindings), func(i, j int) {
		shuffledBindings[i], shuffledBindings[j] = shuffledBindings[j], shuffledBindings[i]
	})

	backfillBindings := make(chan backfillBinding)
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
		case backfillBindings <- backfillBinding{binding: b}:
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

// backfillBatchCollections repeatedly backfills all batch collections according
// to their schedules.
func (c *capture) backfillBatchCollections(
	ctx context.Context,
	coordinator *streamBackfillCoordinator,
	batchBindings []bindingInfo,
) error {
	backfillBindings := make(chan backfillBinding)
	group, groupCtx := errgroup.WithContext(ctx)
	for idx := 0; idx < concurrentBackfillLimit; idx++ {
		group.Go(func() error {
			return c.backfillWorker(groupCtx, coordinator, backfillBindings, make(chan struct{}))
		})
	}

	for _, binding := range batchBindings {
		binding := binding

		group.Go(func() error {
			for {
				c.mu.Lock()
				backfillState := c.state.Resources[binding.stateKey].Backfill
				c.mu.Unlock()

				if backfillState.done() {
					next := binding.schedule.Next(*backfillState.LastPollStart)
					if time.Until(next) > 0 {
						log.WithFields(log.Fields{
							"database":   binding.resource.Database,
							"collection": binding.resource.Collection,
							"next":       next.UTC().String(),
							"mode":       binding.resource.getMode(),
						}).Info("waiting to backfill batch collection")
						if err := schedule.WaitForNext(groupCtx, binding.schedule, *backfillState.LastPollStart); err != nil {
							return err
						}
					}
				}

				doneCh := make(chan struct{})
				select {
				case backfillBindings <- backfillBinding{binding: binding, backfillDone: doneCh}:
					select {
					case <-doneCh:
					case <-groupCtx.Done():
						return groupCtx.Err()
					}
				case <-groupCtx.Done():
					return groupCtx.Err()
				}
			}
		})
	}

	return group.Wait()
}

func (c *capture) backfillWorker(
	ctx context.Context,
	coordinator *streamBackfillCoordinator,
	backfillBindings <-chan backfillBinding,
	stopBackfill <-chan struct{},
) error {
	for {
		select {
		case bb, ok := <-backfillBindings:
			if !ok {
				return nil
			}

			if err := c.doBackfill(ctx, coordinator, bb.binding, bb.backfillDone, stopBackfill); err != nil {
				return fmt.Errorf(
					"backfilling collection %q in database %q: %w",
					bb.binding.resource.Collection,
					bb.binding.resource.Database,
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
// stop by stopBackfill. The backfillDone channel will be closed when the
// backfill is completely done (cursor returns no more documents) if it is not
// `nil`.
func (c *capture) doBackfill(
	ctx context.Context,
	coordinator *streamBackfillCoordinator,
	binding bindingInfo,
	backfillDone chan struct{},
	stopBackfill <-chan struct{},
) error {
	c.mu.Lock()
	backfillState := c.state.Resources[binding.stateKey].Backfill
	c.mu.Unlock()

	lastCursorValue := backfillState.LastCursorValue
	initialDocsCaptured := backfillState.BackfilledDocs
	if backfillState.done() && binding.resource.getMode() == captureModeSnapshot {
		// Starting the snapshot backfill over.
		lastCursorValue = nil
		initialDocsCaptured = 0
	}
	additionalDocsCaptured := 0

	collection := c.client.Database(binding.resource.Database).Collection(binding.resource.Collection)
	// Not using the more precise `CountDocuments()` here since that requires a full collection
	// scan. Getting an estimate from the collection's metadata is very fast and should be close
	// enough for useful logging.
	estimatedTotalDocs, err := collection.EstimatedDocumentCount(ctx)
	if err != nil {
		return fmt.Errorf("getting estimated document count: %w", err)
	}

	cursorField := binding.resource.getCursorField()
	var opts *options.FindOptions
	if cursorField == idProperty && binding.resource.getMode() == captureModeChangeStream {
		// By not specifying a sort parameter, MongoDB uses natural sort to order documents. Natural
		// sort is approximately insertion order (but not guaranteed). We hint to MongoDB to use the _id
		// index (an index that always exists) to speed up the process. Note that if we specify the sort
		// explicitly by { $natural: 1 }, then the database will disregard any indices and do a full
		// collection scan. See https://www.mongodb.com/docs/manual/reference/method/cursor.hint

		// Note: This assumption is only true for "standard" MongoDB instances that support change
		// streams. For other flavors of MongoDB that do not support change streams and/or we are using
		// a batch capture mode, a sort will be required.
		opts = options.Find().SetHint(bson.M{idProperty: 1})
	} else {
		// Other cursor fields require a sort.
		opts = options.Find().SetSort(bson.D{{Key: cursorField, Value: 1}})
	}

	var filter = bson.D{}
	if lastCursorValue != nil {
		var v any
		if err := lastCursorValue.Unmarshal(&v); err != nil {
			return fmt.Errorf("unmarshalling last_id: %w", err)
		}
		filter = bson.D{{Key: cursorField, Value: bson.D{{Key: "$gt", Value: v}}}}
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
	logEntry.Info("starting backfill for collection")

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
				if backfillDone != nil {
					close(backfillDone)
				}
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
	var cursorField = binding.resource.getCursorField()
	// TODO(whb): Revisit this batching strategy as part of a more holistic
	// memory optimization effort.
	var docBatch []json.RawMessage
	var lastCursor bson.RawValue
	var batchBytes int
	var docsRead int

	c.mu.Lock()
	state := c.state.Resources[sk]
	if state.Backfill.LastPollStart == nil || state.Backfill.done() {
		// First time ever polling this collection, or doing a scheduled re-poll
		// of a previously completed collection.
		state.Backfill.LastPollStart = makePtr(time.Now().UTC())
	}
	if state.Backfill.done() && binding.resource.getMode() == captureModeSnapshot {
		state.Backfill.BackfilledDocs = 0
	}
	state.Backfill.Done = makePtr(false)
	c.state.Resources[sk] = state
	c.mu.Unlock()

	emitDocs := func() error {
		c.mu.Lock()
		state := c.state.Resources[sk]
		state.Backfill.LastCursorValue = &lastCursor
		state.Backfill.BackfilledDocs += len(docBatch)
		c.state.Resources[sk] = state
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
		if lastCursor, err = cursor.Current.LookupErr(cursorField); err != nil {
			return 0, fmt.Errorf("looking up cursor field '%s': %w", cursorField, err)
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
			Resources: map[boilerplate.StateKey]resourceState{sk: state},
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
