package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"time"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
)

// Limit the number of backfills running concurrently to avoid excessive memory use and limit the
// number of open cursors at a single time.
const concurrentBackfillLimit = 10

func (c *capture) backfillCollections(
	ctx context.Context,
	client *mongo.Client,
	bindings []bindingInfo,
	backfillFor time.Duration,
) error {
	var errBackfillTimerElapsed = errors.New("backfill timer elapsed")

	group, groupCtx := errgroup.WithContext(ctx)
	groupCtx, cancel := context.WithTimeoutCause(groupCtx, backfillFor, errBackfillTimerElapsed)
	defer cancel()

	var backfillBindings = make(chan backfillBinding)
	var docBatches = make(chan docBatch)

	group.Go(func() error {
		return c.emitWorker(groupCtx, docBatches)
	})

	for range concurrentBackfillLimit {
		group.Go(func() error {
			return backfillWorker(groupCtx, client, backfillBindings, docBatches)
		})
	}

	shuffledBindings := make([]bindingInfo, len(bindings))
	rand.Shuffle(len(shuffledBindings), func(i, j int) { shuffledBindings[i], shuffledBindings[j] = shuffledBindings[j], shuffledBindings[i] })

	for _, b := range shuffledBindings {
		// TODO: Start again here
	}

	err := group.Wait()
	if errors.Is(err, errBackfillTimerElapsed) {
		err = nil
	}

	return err
}

func (c *capture) emitWorker(ctx context.Context, docBatches <-chan docBatch) error {
	for {
		select {
		case batch := <-docBatches:
			var sk = batch.binding.stateKey
			var docs = make([]json.RawMessage, 0, len(batch.docs))
			var friendlyCollectionName = resourceId(batch.binding.resource)

			for _, doc := range batch.docs {
				if raw, err := json.Marshal(doc); err != nil {
					return fmt.Errorf("emitWorker marshalling document to json for collection %s: %w", friendlyCollectionName, err)
				} else {
					docs = append(docs, raw)
				}
			}

			stateUpdate := captureState{
				Resources: map[boilerplate.StateKey]resourceState{
					sk: batch.state,
				},
			}

			if len(docs) > 0 {
				// There may be no documents on the final "backfill complete" checkpoint.
				if err := c.output.Documents(batch.binding.index, docs...); err != nil {
					return fmt.Errorf("emitWorker output documents for collection %s: %w", friendlyCollectionName, err)
				}
			}

			if checkpointJson, err := json.Marshal(stateUpdate); err != nil {
				return fmt.Errorf("emitWorker marshalling checkpoint to json for collection %s: %w", friendlyCollectionName, err)
			} else if err := c.output.Checkpoint(checkpointJson, true); err != nil {
				return fmt.Errorf("emitWorker output checkpoint for collection %s: %w", friendlyCollectionName, err)
			}

			state := c.state.Resources[sk]
			if state.Backfill.LastId.Validate() == nil {
				// Don't blow away the final "LastId" when emitting that the backfill for this
				// binding is done.
				state.Backfill.LastId = batch.state.Backfill.LastId
			}
			state.Backfill.Done = batch.state.Backfill.Done
			c.state.Resources[sk] = state

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type backfillBinding struct {
	binding      bindingInfo
	initialState resourceState
}

type docBatch struct {
	docs    []primitive.M
	state   resourceState
	binding bindingInfo
}

func backfillWorker(
	ctx context.Context,
	client *mongo.Client,
	backfillBindings <-chan backfillBinding,
	docBatches chan<- docBatch,
) error {
	for {
		select {
		case bb := <-backfillBindings:
			if err := doBackfill(ctx, client, bb.binding, bb.initialState, docBatches); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func doBackfill(
	ctx context.Context,
	client *mongo.Client,
	binding bindingInfo,
	state resourceState,
	docBatches chan<- docBatch,
) error {
	var collection = client.Database(binding.resource.Database).Collection(binding.resource.Collection)
	var friendlyCollectionName = resourceId(binding.resource)

	// By not specifying a sort parameter, MongoDB uses natural sort to order documents. Natural
	// sort is approximately insertion order (but not guaranteed). We hint to MongoDB to use the _id
	// index (an index that always exists) to speed up the process. Note that if we specify the sort
	// explicitly by { $natural: 1 }, then the database will disregard any indices and do a full
	// collection scan. See https://www.mongodb.com/docs/manual/reference/method/cursor.hint
	var opts = options.Find().SetHint(bson.M{"_id": 1})

	var filter = bson.D{}
	if state.Backfill.LastId.Validate() == nil {
		var v interface{}
		if err := state.Backfill.LastId.Unmarshal(&v); err != nil {
			return fmt.Errorf("unmarshalling last_id for collection %s: %w", friendlyCollectionName, err)
		}
		filter = bson.D{{idProperty, bson.D{{"$gt", v}}}}
	}

	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return fmt.Errorf("backfill find for collection %s: %w", friendlyCollectionName, err)
	}
	defer cursor.Close(ctx)

	var lastId bson.RawValue
	batch := docBatch{
		binding: binding,
	}

	sendBatch := func(done bool) error {
		batch.state = resourceState{
			Backfill: backfillState{
				Done:   done,
				LastId: lastId,
			},
		}

		select {
		case docBatches <- batch:
			// Reset for the next batch.
			batch = docBatch{
				binding: binding,
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	for cursor.Next(ctx) {
		// We decode once into a map of RawValues so we can keep the _id as a
		// RawValue. This allows us to marshal it as an encoded byte array along
		// with its type code in the checkpoint, and unmarshal it when we want to
		// use it to resume the backfill, ensuring the type of the _id is consistent
		// through this process
		var rawDoc map[string]bson.RawValue
		// TODO(whb): Can we get this from cursor.Current.Lookup()? Would that make any difference?
		if err = cursor.Decode(&rawDoc); err != nil {
			return fmt.Errorf("backfill decoding document raw for collection %s: %w", friendlyCollectionName, err)
		}
		lastId = rawDoc[idProperty]

		var doc bson.M
		if err = cursor.Decode(&doc); err != nil {
			return fmt.Errorf("backfill decoding document for collection %s: %w", friendlyCollectionName, err)
		}
		doc = sanitizeDocument(doc)
		doc[metaProperty] = map[string]interface{}{
			opProperty: "c",
		}

		batch.docs = append(batch.docs, doc)

		// This means the next call to cursor.Next() will trigger a new network request. Before we
		// do that, we checkpoint so that we can resume from this point after a failure.
		if cursor.RemainingBatchLength() == 0 {
			if err := sendBatch(false); err != nil {
				return err
			}
		}
	}
	if err := cursor.Err(); err != nil {
		return fmt.Errorf("backfill find cursor error for collection %s: %w", friendlyCollectionName, err)
	}

	return sendBatch(true)
}
