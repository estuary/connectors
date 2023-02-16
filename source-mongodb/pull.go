package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"errors"
	"io"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"golang.org/x/sync/errgroup"
	log "github.com/sirupsen/logrus"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Pull is a very long lived RPC through which the Flow runtime and a
// Driver cooperatively execute an unbounded number of transactions.
func (d *driver) Pull(stream pc.Driver_PullServer) error {
	var open, err = stream.Recv()
	if err != nil {
		return fmt.Errorf("error reading PullRequest: %w", err)
	} else if open.Open == nil {
		return fmt.Errorf("expected PullRequest.Open, got %#v", open)
	}

	var cfg config
	if err := pf.UnmarshalStrict(open.Open.Capture.EndpointSpecJson, &cfg); err != nil {
		return fmt.Errorf("parsing config json: %w", err)
	}

	var ctx = context.Background()

	client, err := d.Connect(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	log.Info("connected to database")

	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	var c = capture{
		Output: &boilerplate.PullOutput{Stream: stream},
	}

	eg, ctx := errgroup.WithContext(ctx)

	if err := c.Output.Ready(); err != nil {
		return err
	}

	for idx, binding := range open.Open.Capture.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}

		if err := c.BackfillCollection(ctx, client, uint32(idx), res); err != nil {
			return fmt.Errorf("backfill: %w", err)
		}
		eg.Go(func() error { return c.ChangeStream(ctx, client, uint32(idx), res) })
	}

	if err := eg.Wait(); err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	return nil
}

type changeEvent struct{
	OperationType string `bson:"operationType"`
	FullDocument bson.M `bson:"fullDocument"`
}

func (c *capture) ChangeStream(ctx context.Context, client *mongo.Client, binding uint32, res resource) error {
	var db = client.Database(res.Database)

	var collection = db.Collection(res.Collection)

	log.Debug("listening on changes on collection ", res.Collection)
	var eventFilter = bson.D{{"$match", bson.D{{"$or",
	bson.A{
		// bson.D{{"operationType", "delete"}}, TODO: how do we handle deletion
		// events
		bson.D{{"operationType", "insert"}},
		bson.D{{"operationType", "update"}},
	}}}}}
	cursor, err := collection.Watch(ctx, mongo.Pipeline{eventFilter}, options.ChangeStream().SetFullDocument(options.UpdateLookup))
	if err != nil {
		return fmt.Errorf("change stream on collection %s: %w", res.Collection, err)
	}

	for cursor.Next(ctx) {
		var ev changeEvent
		if err = cursor.Decode(&ev); err != nil {
			return fmt.Errorf("decoding document in collection %s: %w", res.Collection, err)
		}

		var doc = sanitizeDocument(ev.FullDocument)

		js, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("encoding document in collection %s as json: %w", res.Collection, err)
		}

		if err = c.Output.Documents(binding, js); err != nil {
			return fmt.Errorf("output documents failed: %w", err)
		}

		if err = c.Output.Checkpoint([]byte("{}"), true); err != nil {
			return fmt.Errorf("output checkpoint failed: %w", err)
		}
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor error for change stream %s: %w", res.Collection, err)
	}

	defer cursor.Close(ctx)

	return nil
}

const CHECKPOINT_EVERY = 100

func (c *capture) BackfillCollection(ctx context.Context, client *mongo.Client, binding uint32, res resource) error {
	var db = client.Database(res.Database)

	var collection = db.Collection(res.Collection)

	log.Debug("finding documents in collection ", res.Collection)
	cursor, err := collection.Find(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("could not run find query on collection %s: %w", res.Collection, err)
	}

	defer cursor.Close(ctx)

	var i = 0
	for cursor.Next(ctx) {
		i += 1
		var doc bson.M
		if err = cursor.Decode(&doc); err != nil {
			return fmt.Errorf("decoding document in collection %s: %w", res.Collection, err)
		}
		doc = sanitizeDocument(doc)

		js, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("encoding document in collection %s as json: %w", res.Collection, err)
		}

		if err = c.Output.Documents(binding, js); err != nil {
			return fmt.Errorf("output documents failed: %w", err)
		}

		if i % CHECKPOINT_EVERY == 0 {
			if err = c.Output.Checkpoint([]byte("{}"), true); err != nil {
				return fmt.Errorf("output checkpoint failed: %w", err)
			}
		}
	}

	if err = c.Output.Checkpoint([]byte("{}"), true); err != nil {
		return fmt.Errorf("sending checkpoint failed: %w", err)
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor error for backfill %s: %w", res.Collection, err)
	}

	return nil
}

func sanitizeDocument(doc map[string]interface{}) map[string]interface{} {
	for key, value := range doc {
		switch v := value.(type) {
		case float64:
			if math.IsNaN(v) {
				doc[key] = "NaN"
			}
		case map[string]interface{}:
			doc[key] = sanitizeDocument(v)
		}
	}

	return doc
}

type capture struct{
	Output   *boilerplate.PullOutput
}

