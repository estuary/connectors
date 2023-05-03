package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"time"
	"reflect"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Pull is a very long lived RPC through which the Flow runtime and a
// Driver cooperatively execute an unbounded number of transactions.
func (d *driver) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	var cfg config
	if err := pf.UnmarshalStrict(open.Capture.ConfigJson, &cfg); err != nil {
		return fmt.Errorf("parsing config json: %w", err)
	}

	var prevState captureState
	if open.StateJson != nil {
		if err := pf.UnmarshalStrict(open.StateJson, &prevState); err != nil {
			return fmt.Errorf("parsing state checkpoint: %w", err)
		}
	}

	var ctx = stream.Context()

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
		Output: stream,
	}

	eg, ctx := errgroup.WithContext(ctx)

	if err := c.Output.Ready(false); err != nil {
		return err
	}

	for idx, binding := range open.Capture.Bindings {
		idx := idx

		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}
		var resState, _ = prevState.Resources[resourceId(res)]

		// Only backfill the first time, and we know that when there is no state
		var backfillFinishedAt *primitive.Timestamp = nil
		if len(prevState.Resources) == 0 {
			if backfillFinishedAt, err = c.BackfillCollection(ctx, client, idx, res); err != nil {
				return fmt.Errorf("backfill: %w", err)
			}
		}

		eg.Go(func() error {
			var e = c.ChangeStream(ctx, client, idx, res, backfillFinishedAt, resState)
			if e != nil {
				log.WithField("error", e).WithField("collection", res.Collection).Error("ChangeStream error")
			}
			return e
		})
	}

	if err := eg.Wait(); err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	return nil
}

type capture struct {
	Output *boilerplate.PullOutput
}

type captureState struct {
	Resources map[string]bson.Raw `json:"resources"`
}

func (s *captureState) Validate() error {
	return nil
}

type docKey struct {
	Id interface{} `bson:"_id"`
}
type changeEvent struct {
	DocumentKey   docKey `bson:"documentKey"`
	OperationType string `bson:"operationType"`
	FullDocument  bson.M `bson:"fullDocument"`
}

const resumePointGoneErrorMessage = "the resume point may no longer be in the oplog"

func (c *capture) ChangeStream(ctx context.Context, client *mongo.Client, binding int, res resource, backfillFinishedAt *primitive.Timestamp, resumeToken bson.Raw) error {
	var db = client.Database(res.Database)

	var collection = db.Collection(res.Collection)

	log.WithFields(log.Fields{
		"database":           res.Database,
		"collection":         res.Collection,
		"resumeToken":        resumeToken,
		"backfillFinishedAt": backfillFinishedAt,
	}).Info("listening on changes on collection")
	var eventFilter = bson.D{{"$match", bson.D{{"$or",
		bson.A{
			bson.D{{"operationType", "delete"}},
			bson.D{{"operationType", "insert"}},
			bson.D{{"operationType", "update"}},
		}}}}}
	var opts = options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if resumeToken != nil {
		opts = opts.SetResumeAfter(resumeToken)
	} else if backfillFinishedAt != nil {
		opts = opts.SetStartAtOperationTime(backfillFinishedAt)
	}
	cursor, err := collection.Watch(ctx, mongo.Pipeline{eventFilter}, opts)
	if err != nil {
		// This error means events from the starting point are no longer available,
		// which means we have hit a gap in between last run of the connector and
		// this one. We re-set the checkpoint and error out so the next run of the
		// connector will run a backfill.
		//
		// There is potential for optimisation here by only resetting the checkpoint
		// of the collection which can't resume instead of resetting the whole
		// connector's checkpoint and backfilling everything
		if e, ok := err.(mongo.ServerError); ok {
			if e.HasErrorMessage(resumePointGoneErrorMessage) {
				if err = c.Output.Checkpoint([]byte("{}"), false); err != nil {
					return fmt.Errorf("output checkpoint failed: %w", err)
				}
				return fmt.Errorf("change stream on collection %s cannot resume capture, the connector will restart and run a backfill: %w", res.Collection, err)
			}
		}

		return fmt.Errorf("change stream on collection %s: %w", res.Collection, err)
	}

	for cursor.Next(ctx) {
		var ev changeEvent
		if err = cursor.Decode(&ev); err != nil {
			return fmt.Errorf("decoding document in collection %s: %w", res.Collection, err)
		}

		if ev.OperationType == "delete" {
			var doc = map[string]interface{}{
				idProperty: ev.DocumentKey.Id,
				metaProperty: map[string]interface{}{
					deletedProperty: true,
				},
			}

			js, err := json.Marshal(doc)
			if err != nil {
				return fmt.Errorf("encoding document in collection %s as json: %w", res.Collection, err)
			}

			if err = c.Output.Documents(binding, js); err != nil {
				return fmt.Errorf("output documents failed: %w", err)
			}
		} else {
			var doc = sanitizeDocument(ev.FullDocument)

			js, err := json.Marshal(doc)
			if err != nil {
				return fmt.Errorf("encoding document in collection %s as json: %w", res.Collection, err)
			}

			if err = c.Output.Documents(binding, js); err != nil {
				return fmt.Errorf("output documents failed: %w", err)
			}
		}

		var checkpoint = captureState{
			Resources: map[string]bson.Raw{
				resourceId(res): cursor.ResumeToken(),
			},
		}

		checkpointJson, err := json.Marshal(checkpoint)
		if err != nil {
			return fmt.Errorf("encoding checkpoint to json failed: %w", err)
		}

		if err = c.Output.Checkpoint(checkpointJson, true); err != nil {
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

func (c *capture) BackfillCollection(ctx context.Context, client *mongo.Client, binding int, res resource) (*primitive.Timestamp, error) {
	var db = client.Database(res.Database)

	var collection = db.Collection(res.Collection)

	log.Debug("finding documents in collection ", res.Collection)
	cursor, err := collection.Find(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("could not run find query on collection %s: %w", res.Collection, err)
	}

	defer cursor.Close(ctx)

	var i = 0
	for cursor.Next(ctx) {
		i += 1
		var doc bson.M
		if err = cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decoding document in collection %s: %w", res.Collection, err)
		}
		doc = sanitizeDocument(doc)

		js, err := json.Marshal(doc)
		if err != nil {
			return nil, fmt.Errorf("encoding document in collection %s as json: %w", res.Collection, err)
		}

		if err = c.Output.Documents(binding, js); err != nil {
			return nil, fmt.Errorf("output documents failed: %w", err)
		}

		if i%CHECKPOINT_EVERY == 0 {
			if err = c.Output.Checkpoint([]byte("{}"), true); err != nil {
				return nil, fmt.Errorf("output checkpoint failed: %w", err)
			}
		}
	}

	if err = c.Output.Checkpoint([]byte("{}"), true); err != nil {
		return nil, fmt.Errorf("sending checkpoint failed: %w", err)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error for backfill %s: %w", res.Collection, err)
	}

	// minus five seconds for potential error
	return &primitive.Timestamp{T: uint32(time.Now().Unix()) - 5, I: 0}, nil
}

func resourceId(res resource) string {
	return fmt.Sprintf("%s.%s", res.Database, res.Collection)
}

func sanitizeDocument(doc map[string]interface{}) map[string]interface{} {
	for key, value := range doc {
		// Make sure `_id` is always captured as string
		log.WithField("key", key).WithField("value", value).WithField("type", reflect.TypeOf(value)).Debug("sanitizing document")
		if key == "_id" {
			doc[key] = idToString(value)
		} else {
			switch v := value.(type) {
			case float64:
				if math.IsNaN(v) {
					doc[key] = "NaN"
				}
			case map[string]interface{}:
				doc[key] = sanitizeDocument(v)
			}
		}
	}

	return doc
}

func idToString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case primitive.ObjectID:
		return v.Hex()
	}

	var j, err = json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("could not marshal interface{} to json: %s", err))
	}
	return string(j)
}
