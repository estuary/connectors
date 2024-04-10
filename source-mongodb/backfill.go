package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
)

const (
	// Limit the number of backfills running concurrently to avoid excessive memory use and limit the
	// number of open cursors at a single time.
	concurrentBackfillLimit = 10

	// Backfill for this long each "round" before catching up change streams.
	backfillFor = 5 * time.Minute
)

// backfillBinding is the information needed to backfill a binding.
type backfillBinding struct {
	binding      bindingInfo
	initialState resourceState
}

// backfillCollections backfills all capture collections until time runs out, or the backfill is
// complete. Each round of backfilling the bindings will be randomized to provide some degree of
// equality in how they are backfilled, but within a single round of backfilling a binding will be
// backfilled continuously until it finishes.
func (c *capture) backfillCollections(
	ctx context.Context,
	resourceState map[boilerplate.StateKey]resourceState,
	bindings []bindingInfo,
	backfillFor time.Duration,
) error {
	log.WithField("backfillFor", backfillFor.String()).Info("backfilling collections")

	shuffledBindings := make([]bindingInfo, len(bindings))
	copy(shuffledBindings, bindings)
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
			return c.backfillWorker(groupCtx, backfillBindings, stopBackfill)
		})
	}

	// Push bindings to backfill to the backfill workers until time runs out. Each worker backfills
	// a single binding until it is done backfilling, and is then able to start working on another.
	for _, b := range shuffledBindings {
		state := resourceState[b.stateKey]
		if state.Backfill.Done {
			continue
		}

		select {
		case backfillBindings <- backfillBinding{
			binding:      b,
			initialState: state,
		}:
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
	backfillBindings <-chan backfillBinding,
	stopBackfill <-chan struct{},
) error {
	for {
		select {
		case bb, ok := <-backfillBindings:
			if !ok {
				return nil
			}

			if err := c.doBackfill(ctx, bb.binding, bb.initialState, stopBackfill); err != nil {
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

func (c *capture) doBackfill(
	ctx context.Context,
	binding bindingInfo,
	state resourceState,
	stopBackfill <-chan struct{},
) error {
	var collection = c.client.Database(binding.resource.Database).Collection(binding.resource.Collection)
	var startingDocCount = state.Backfill.BackfilledDocs

	// Not using the more precise `CountDocuments()` here since that requires a full collection
	// scan. Getting an estimate from the collection's metadata is very fast and should be close
	// enough for useful logging.
	estimatedTotalDocs, err := collection.EstimatedDocumentCount(ctx)
	if err != nil {
		return fmt.Errorf("getting estimated document count: %w", err)
	}

	logEntry := log.WithFields(log.Fields{
		"database":           binding.resource.Database,
		"collection":         binding.resource.Collection,
		"estimatedTotalDocs": estimatedTotalDocs,
	})
	logEntry.Info("starting backfill for collection")

	// By not specifying a sort parameter, MongoDB uses natural sort to order documents. Natural
	// sort is approximately insertion order (but not guaranteed). We hint to MongoDB to use the _id
	// index (an index that always exists) to speed up the process. Note that if we specify the sort
	// explicitly by { $natural: 1 }, then the database will disregard any indices and do a full
	// collection scan. See https://www.mongodb.com/docs/manual/reference/method/cursor.hint
	var opts = options.Find().SetHint(bson.M{"_id": 1})

	var filter = bson.D{}
	if !state.Backfill.LastId.IsZero() {
		var v interface{}
		if err := state.Backfill.LastId.Unmarshal(&v); err != nil {
			return fmt.Errorf("unmarshalling last_id: %w", err)
		}
		filter = bson.D{{idProperty, bson.D{{"$gt", v}}}}
	}

	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return fmt.Errorf("collection.Find: %w", err)
	}
	defer cursor.Close(ctx)

	lastId := state.Backfill.LastId
	docCount := 0
	var docs []primitive.M
	for cursor.Next(ctx) {
		docCount++

		var err error
		if lastId, err = cursor.Current.LookupErr(idProperty); err != nil {
			return fmt.Errorf("looking up idProperty: %w", err)
		}

		var doc bson.M
		if err = cursor.Decode(&doc); err != nil {
			return fmt.Errorf("backfill decoding document: %w", err)
		}
		doc[metaProperty] = map[string]interface{}{
			opProperty: "c",
		}

		docs = append(docs, sanitizeDocument(doc))

		// This means the next call to cursor.Next() will trigger a new network request. Before we
		// do that, we checkpoint so that we can resume from this point after a failure.
		if cursor.RemainingBatchLength() == 0 {
			if err := c.emitEvent(ctx, backfillEvent{
				docs: docs,
				stateUpdate: resourceState{
					Backfill: backfillState{
						LastId:         lastId,
						BackfilledDocs: startingDocCount + docCount,
					},
				},
				binding: binding,
			}); err != nil {
				return fmt.Errorf("emitting backfill event: %w", err)
			}

			docs = nil // Reset for the next batch.

			select {
			case <-stopBackfill:
				// The backfill timer has run out, so don't request any more data from this cursor.
				complete := fmt.Sprintf("%.0f", float64(startingDocCount+docCount)/float64(estimatedTotalDocs)*100)

				logEntry.WithFields(log.Fields{
					"docsCapturedThisRound": docCount,
					"totalDocsCaptured":     startingDocCount + docCount,
					"percentComplete":       complete,
				}).Info("progressed backfill for collection")

				return nil
			default:
				// Continue with the backfill.
			}
		}
	}
	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor error: %w", err)
	}

	// The backfill for this binding has completed.
	if err := c.emitEvent(ctx, backfillEvent{
		docs: docs,
		stateUpdate: resourceState{
			Backfill: backfillState{
				Done:           true,
				LastId:         lastId,
				BackfilledDocs: startingDocCount + docCount,
			},
		},
		binding: binding,
	}); err != nil {
		return fmt.Errorf("emitting final backfill event: %w", err)
	}

	logEntry.WithField("totalDocsCaptured", startingDocCount+docCount).Info("completed backfill for collection")

	return nil
}
