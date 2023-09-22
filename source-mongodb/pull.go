package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

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

	var convertedState = false
	var prevState captureState
	if open.StateJson != nil {
		if err := pf.UnmarshalStrict(open.StateJson, &prevState); err != nil {
			// If this is coming from an old version of the connector, parse it and
			// convert it to the new format
			var depState deprecatedCaptureState
			if depErr := pf.UnmarshalStrict(open.StateJson, &depState); depErr == nil {
				var resources = make(map[string]resourceState)
				for key, resumeToken := range depState.Resources {
					var r = resourceState{}

					// if we can unmarshal to resourceState, this is a new state value
					// otherwise we assume it is a resume token from the old state format
					if err := json.Unmarshal(resumeToken, &r); err != nil {
						r.StreamResumeToken = bson.Raw(resumeToken)
						r.Status = StatusStreaming
					}

					resources[key] = r
				}

				prevState = captureState{
					Resources: resources,
				}

				convertedState = true
			} else {
				return fmt.Errorf("parsing checkpoint json %s: %w", string(open.StateJson), err)
			}
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

	eg, egCtx := errgroup.WithContext(ctx)

	if err := c.Output.Ready(false); err != nil {
		return err
	}

	// emit the converted state
	if convertedState {
		checkpointJson, err := json.Marshal(prevState)
		if err != nil {
			return fmt.Errorf("encoding checkpoint to json failed: %w", err)
		}
		log.WithField("checkpoint", string(checkpointJson)).Info("converted")
		if err = c.Output.Checkpoint(checkpointJson, false); err != nil {
			return fmt.Errorf("output checkpoint failed: %w", err)
		}
	}

	// Remove bindings from the checkpoint that are no longer part of the spec.
	activeBindings := make(map[string]struct{})
	for _, binding := range open.Capture.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}
		activeBindings[resourceId(res)] = struct{}{}
	}
	removedBindings := false
	for res := range prevState.Resources {
		if _, ok := activeBindings[res]; !ok {
			log.WithField("resource", res).Info("binding removed from catalog")
			delete(prevState.Resources, res)
			removedBindings = true
		}
	}
	if removedBindings {
		cp, err := json.Marshal(prevState)
		if err != nil {
			return fmt.Errorf("marshalling checkpoint for removed bindings: %w", err)
		} else if err = c.Output.Checkpoint(cp, false); err != nil {
			return fmt.Errorf("updating checkpoint for removed bindings: %w", err)
		}
	}

	// Capture each binding.
	for idx, binding := range open.Capture.Bindings {
		idx := idx

		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}
		var resState, _ = prevState.Resources[resourceId(res)]

		if resState.Status == StatusBackfill {
			if err = c.BackfillCollection(ctx, client, idx, res, &resState); err != nil {
				return fmt.Errorf("backfill: %w", err)
			}
		}

		eg.Go(func() error {
			return c.ChangeStream(egCtx, client, idx, res, &resState)
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

type deprecatedCaptureState struct {
	Resources map[string]json.RawMessage `json:"resources"`
}

func (s *deprecatedCaptureState) Validate() error {
	return nil
}

type captureState struct {
	Resources map[string]resourceState `json:"resources"`
}

func (s *captureState) Validate() error {
	return nil
}

const (
	StatusBackfill = 0
	StatusStreaming = 1
)

type resourceState struct {
	// default value is 0, which is StatusBackfill
	Status int `json:"status,omitempty"`

	BackfillStartedAt time.Time `json:"backfill_started_at,omitempty"`
	BackfillLastId bson.RawValue `json:"backfill_last_id,omitempty"`

	StreamResumeToken bson.Raw `json:"stream_resume_token,omitempty"`
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
const resumeTokenNotFoundErrorMessage = "the resume token was not found"

func (c *capture) ChangeStream(ctx context.Context, client *mongo.Client, binding int, res resource, state *resourceState) error {
	var db = client.Database(res.Database)

	var collection = db.Collection(res.Collection)

	log.WithFields(log.Fields{
		"database":           res.Database,
		"collection":         res.Collection,
		"state":              state,
	}).Info("listening on changes on collection")
	var eventFilter = bson.D{{"$match", bson.D{{"$or",
	bson.A{
		bson.D{{"operationType", "delete"}},
		bson.D{{"operationType", "insert"}},
		bson.D{{"operationType", "update"}},
		bson.D{{"operationType", "replace"}},
	}}}}}

	var opts = options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if len(state.StreamResumeToken) > 0 {
		opts = opts.SetResumeAfter(state.StreamResumeToken)
	} else if !state.BackfillStartedAt.IsZero() {
		var oplogSafetyBuffer, _ = time.ParseDuration("-5m")
		if err := oplogHasTimestamp(ctx, client, state.BackfillStartedAt.Add(oplogSafetyBuffer)); err != nil {
			return err
		}
		var startAt = &primitive.Timestamp{T: uint32(state.BackfillStartedAt.Unix()) - 1, I: 0}
		opts = opts.SetStartAtOperationTime(startAt)
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
			if e.HasErrorMessage(resumePointGoneErrorMessage) || e.HasErrorMessage(resumeTokenNotFoundErrorMessage) {
				return fmt.Errorf("change stream on collection %s cannot resume capture, this is usually due to a small oplog storage available. Please resize your oplog to be able to safely capture data from your database: https://go.estuary.dev/NurkrE. After resizing your uplog, you can remove the binding for this collection add it back to trigger a backfill: %w", res.Collection, e)
			}
		}

		return fmt.Errorf("change stream on collection %s: %w", res.Collection, err)
	}

	for cursor.Next(ctx) {
		var ev changeEvent
		if err = cursor.Decode(&ev); err != nil {
			return fmt.Errorf("decoding document in collection %s: %w", res.Collection, err)
		}

		state.StreamResumeToken = cursor.ResumeToken()

		var checkpoint = captureState{
			Resources: map[string]resourceState{
				resourceId(res): *state,
			},
		}

		checkpointJson, err := json.Marshal(checkpoint)
		if err != nil {
			return fmt.Errorf("encoding checkpoint to json failed: %w", err)
		}

		if ev.OperationType == "delete" {
			var doc = map[string]interface{}{
				idProperty: ev.DocumentKey.Id,
				metaProperty: map[string]interface{}{
					opProperty: "d",
				},
			}

			doc = sanitizeDocument(doc)

			js, err := json.Marshal(doc)
			if err != nil {
				return fmt.Errorf("encoding document %v in collection %s as json: %w", doc, res.Collection, err)
			}

			if err = c.Output.DocumentsAndCheckpoint(checkpointJson, true, binding, js); err != nil {
				return fmt.Errorf("output documents failed: %w", err)
			}
		} else if ev.FullDocument != nil {
			// FullDocument can be "null" if another operation has deleted the
			// document. This happens because update change events do not hold a copy of the
			// full document, but rather it is at query time that the full document is
			// looked up, and in these cases, the FullDocument can end up being null
			// (if deleted) or different from the deltas in the update event. We
			// ignore events where fullDocument is null. Another change event of type
			// delete will eventually come and delete the document (if not already).
			var doc = sanitizeDocument(ev.FullDocument)
			var op string
			if ev.OperationType == "update" || ev.OperationType == "replace" {
				op = "u"
			} else if ev.OperationType == "insert" {
				op = "c"
			}
			doc[metaProperty] = map[string]interface{}{
				opProperty: op,
			}

			js, err := json.Marshal(doc)
			if err != nil {
				return fmt.Errorf("encoding document %v in collection %s as json: %w", doc, res.Collection, err)
			}

			if err = c.Output.DocumentsAndCheckpoint(checkpointJson, true, binding, js); err != nil {
				return fmt.Errorf("output documents failed: %w", err)
			}
		}
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor error for change stream %s: %w", res.Collection, err)
	}

	defer cursor.Close(ctx)

	return nil
}

const BackfillBatchSize = 1024

const (
	NaturalSort = "$natural"
	SortAscending = 1
	SortDescending = -1
)

func (c *capture) BackfillCollection(ctx context.Context, client *mongo.Client, binding int, res resource, state *resourceState) error {
	var db = client.Database(res.Database)
	var collection = db.Collection(res.Collection)

	if state.BackfillStartedAt.IsZero() {
		// This means we are starting backfill for the first time
		state.BackfillStartedAt = time.Now()
	}

	var opts = options.Find().SetSort(bson.D{{NaturalSort, SortAscending}}).SetBatchSize(BackfillBatchSize)
	var filter = bson.D{}
	if state.BackfillLastId.Validate() == nil {
		var v interface{}
		if err := state.BackfillLastId.Unmarshal(&v); err != nil {
			return fmt.Errorf("unmarshalling backfill_last_id: %w", err)
		}
		filter = bson.D{{idProperty, bson.D{{"$gt", v}}}}
	}

	log.WithFields(log.Fields{
		"collection": res.Collection,
		"opts": opts,
		"filter": filter,
	}).Info("backfilling documents in collection")

	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return fmt.Errorf("could not run find query on collection %s: %w", res.Collection, err)
	}

	defer cursor.Close(ctx)

	var i = 0
	for cursor.Next(ctx) {
		i += 1

		// We decode once into a map of RawValues so we can keep the _id as a
		// RawValue. This allows us to marshal it as an encoded byte array along
		// with its type code in the checkpoint, and unmarshal it when we want to
		// use it to resume the backfill, ensuring the type of the _id is consistent
		// through this process
		var rawDoc map[string]bson.RawValue
		if err = cursor.Decode(&rawDoc); err != nil {
			return fmt.Errorf("decoding document raw in collection %s: %w", res.Collection, err)
		}
		state.BackfillLastId = rawDoc[idProperty]

		var doc bson.M
		if err = cursor.Decode(&doc); err != nil {
			return fmt.Errorf("decoding document in collection %s: %w", res.Collection, err)
		}
		doc = sanitizeDocument(doc)
		doc[metaProperty] = map[string]interface{}{
			opProperty: "c",
		}

		js, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("encoding document %v in collection %s as json: %w", doc, res.Collection, err)
		}

		if err = c.Output.Documents(binding, js); err != nil {
			return fmt.Errorf("output documents failed: %w", err)
		}

		if i%BackfillBatchSize == 0 {
			var checkpoint = captureState{
				Resources: map[string]resourceState{
					resourceId(res): *state,
				},
			}

			checkpointJson, err := json.Marshal(checkpoint)
			if err != nil {
				return fmt.Errorf("encoding checkpoint to json failed: %w", err)
			}

			if err = c.Output.Checkpoint(checkpointJson, true); err != nil {
				return fmt.Errorf("output checkpoint failed: %w", err)
			}

			if diff, err := oplogTimeDifference(ctx, client); err != nil {
				return fmt.Errorf("could not read oplog, access to oplog is necessary: %w", err)
			} else {
				// TODO: This is just spamming logs, but users won't see these logs. Should we
				// just error after seeing this error for a while?
				if diff < minOplogTimediff {
					log.Warn(fmt.Sprintf("the current time difference between oldest and newest records in your oplog is %d seconds. This is smaller than the minimum of 24 hours. Please resize your oplog to be able to safely capture data from your database: https://go.estuary.dev/NurkrE", diff))
				}
			}
		}
	}


	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor error for backfill %s: %w", res.Collection, err)
	}

	state.Status = StatusStreaming
	var checkpoint = captureState{
		Resources: map[string]resourceState{
			resourceId(res): *state,
		},
	}

	checkpointJson, err := json.Marshal(checkpoint)
	if err != nil {
		return fmt.Errorf("encoding checkpoint to json failed: %w", err)
	}
	if err = c.Output.Checkpoint(checkpointJson, true); err != nil {
		return fmt.Errorf("sending checkpoint failed: %w", err)
	}

	return nil
}

func resourceId(res resource) string {
	return fmt.Sprintf("%s.%s", res.Database, res.Collection)
}

func sanitizeDocument(doc map[string]interface{}) map[string]interface{} {
	for key, value := range doc {
		// Make sure `_id` is always captured as string
		if key == idProperty {
			doc[key] = idToString(value)
		} else {
			switch v := value.(type) {
			case float64:
				if math.IsNaN(v) {
					doc[key] = "NaN"
				}
			case map[string]interface{}:
			case primitive.M:
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
