package main

import (
	"context"
	"fmt"
	"maps"
	"math/rand"
	"time"

	"github.com/segmentio/encoding/json"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
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
	bindings []bindingInfo,
	backfillFor time.Duration,
) error {
	ts := time.Now()
	log.WithField("backfillFor", backfillFor.String()).Info("backfilling collections")
	defer func() { log.WithField("took", time.Since(ts).String()).Info("finished backfill round") }()

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

	// Create a copy of the binding state map to avoid races with the backfill worker which will
	// update the state concurrently. Each binding to be backfilled receives the initial copy of its
	// state and backfills the collection entirely from there, or time runs out and the process
	// starts over.
	resourceStates := make(map[boilerplate.StateKey]resourceState)
	maps.Copy(resourceStates, c.state.Resources)

	group, groupCtx := errgroup.WithContext(ctx)

	for idx := 0; idx < concurrentBackfillLimit; idx++ {
		group.Go(func() error {
			return c.backfillWorker(groupCtx, backfillBindings, stopBackfill)
		})
	}

	// Push bindings to backfill to the backfill workers until time runs out. Each worker backfills
	// a single binding until it is done backfilling, and is then able to start working on another.
	for _, b := range shuffledBindings {
		state := resourceStates[b.stateKey]
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
	initialState resourceState,
	stopBackfill <-chan struct{},
) error {
	var sk = binding.stateKey
	var collection = c.client.Database(binding.resource.Database).Collection(binding.resource.Collection)
	var initialDocCount = initialState.Backfill.BackfilledDocs

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
	logEntry.Debug("starting backfill for collection")

	// By not specifying a sort parameter, MongoDB uses natural sort to order documents. Natural
	// sort is approximately insertion order (but not guaranteed). We hint to MongoDB to use the _id
	// index (an index that always exists) to speed up the process. Note that if we specify the sort
	// explicitly by { $natural: 1 }, then the database will disregard any indices and do a full
	// collection scan. See https://www.mongodb.com/docs/manual/reference/method/cursor.hint
	var opts = options.Find().SetHint(bson.M{"_id": 1})

	var filter = bson.D{}
	if initialState.Backfill.LastId != nil {
		var v interface{}
		if err := initialState.Backfill.LastId.Unmarshal(&v); err != nil {
			return fmt.Errorf("unmarshalling last_id: %w", err)
		}
		filter = bson.D{{idProperty, bson.D{{"$gt", v}}}}
	}

	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return fmt.Errorf("collection.Find: %w", err)
	}
	defer cursor.Close(ctx)

	// docs represents a batch of serialized documents read from the cursor which are ready to be
	// output.
	var docs []json.RawMessage
	batchBytes := 0

	var lastId bson.RawValue

	docCount := 0
	totalDocsCaptured := initialDocCount

	// emitDocs is a helper for emitted a batch of documents.
	emitDocs := func() error {
		checkpoint := captureState{
			Resources: map[boilerplate.StateKey]resourceState{
				sk: {Backfill: backfillState{LastId: &lastId, BackfilledDocs: totalDocsCaptured}},
			},
		}

		if cpJson, err := json.Marshal(checkpoint); err != nil {
			return fmt.Errorf("serializing checkpoint: %w", err)
		} else if err := c.output.DocumentsAndCheckpoint(cpJson, true, binding.index, docs...); err != nil {
			return fmt.Errorf("ouutputting documents and checkpoint: %w", err)
		}

		// Reset for the next batch.
		docs = nil
		batchBytes = 0

		c.mu.Lock()
		state := c.state.Resources[sk]
		state.Backfill.LastId = &lastId
		state.Backfill.BackfilledDocs = totalDocsCaptured
		c.state.Resources[sk] = state
		c.mu.Unlock()

		return nil
	}

	for cursor.Next(ctx) {
		docCount++
		totalDocsCaptured++

		var doc bson.M
		if lastId, err = cursor.Current.LookupErr(idProperty); err != nil {
			return fmt.Errorf("looking up idProperty: %w", err)
		} else if err = cursor.Decode(&doc); err != nil {
			return fmt.Errorf("backfill decoding document: %w", err)
		}

		doc[metaProperty] = map[string]interface{}{
			opProperty: "c",
		}
		doc = sanitizeDocument(doc)

		docJson, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("serializing document: %w", err)
		}

		docs = append(docs, docJson)
		batchBytes += len(docJson)

		// If the batch has gotten sufficiently large either based on the number of documents it
		// contains or the cumulative size of those documents, emit a checkpoint and reset the
		// batch.
		if len(docs) > backfillCheckpointSize || batchBytes > backfillCheckpointSize {
			if err := emitDocs(); err != nil {
				return err
			}
		}

		// This means the next call to cursor.Next() will trigger a new network request. We'll check
		// and see if the backfill timer has elapsed now and emit the final partial batch, if any.
		// We wait for the current data buffered in the cursor to be consumed before checking the
		// backfill timer to avoid wasted work getting the current batch of documents from the
		// source database.
		if cursor.RemainingBatchLength() == 0 {
			select {
			case <-stopBackfill:
				if len(docs) > 0 {
					if err := emitDocs(); err != nil {
						return fmt.Errorf("emitting partial backfill batch: %w", err)
					}
				}

				complete := fmt.Sprintf("%.0f", float64(totalDocsCaptured)/float64(estimatedTotalDocs)*100)

				logEntry.WithFields(log.Fields{
					"docsCapturedThisRound": docCount,
					"totalDocsCaptured":     totalDocsCaptured,
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

	// Emit any partial batch of documents after the backfill has completed. We're going to end up
	// emitting another checkpoint with the "done" flag immediately after this, but its simpler to
	// just use the same emitDocs() helper as we've used before rather than trying to combine the
	// checkpoint updates.
	if len(docs) > 0 {
		if err := emitDocs(); err != nil {
			return fmt.Errorf("emitting final backfill batch: %w", err)
		}
	}

	logEntry.WithField("totalDocsCaptured", totalDocsCaptured).Info("completed backfill for collection")

	// Emit the final checkpoint.
	c.mu.Lock()
	state := c.state.Resources[sk]
	state.Backfill.Done = true
	c.state.Resources[sk] = state
	c.mu.Unlock()

	checkpoint := captureState{
		Resources: map[boilerplate.StateKey]resourceState{
			sk: {Backfill: backfillState{Done: true}},
		},
	}

	if cp, err := json.Marshal(checkpoint); err != nil {
		return fmt.Errorf("serializing checkpoint: %w", err)
	} else if err := c.output.Checkpoint(cp, true); err != nil {
		return fmt.Errorf("outputting checkpoint: %w", err)
	}

	return nil
}
