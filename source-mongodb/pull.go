package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"slices"
	"time"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
)

// Pull is a very long lived RPC through which the Flow runtime and a
// Driver cooperatively execute an unbounded number of transactions.
func (d *driver) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	var cfg config
	if err := pf.UnmarshalStrict(open.Capture.ConfigJson, &cfg); err != nil {
		return fmt.Errorf("parsing config json: %w", err)
	}

	var bindings = make([]bindingInfo, len(open.Capture.Bindings))
	for idx, binding := range open.Capture.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}
		var sk = boilerplate.StateKey(binding.StateKey)
		bindings[idx] = bindingInfo{
			resource: res,
			index:    idx,
			stateKey: sk,
		}
	}

	var ctx = stream.Context()

	client, err := d.Connect(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}

	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	log.Info("connected to database")

	// TODO(whb): Replace this with a simple json.Unmarshal when all migrations are complete - see
	// comment on `decodeState`.
	prevState, err := decodeState(ctx, client, open.StateJson, bindings)
	if err != nil {
		return fmt.Errorf("decoding state: %w", err)
	}

	prevState, err = updateResourceStates(prevState, bindings)
	if err != nil {
		return fmt.Errorf("updating resource states: %w", err)
	}

	// Log some basic information about the database we are connected to if possible. buildInfo is
	// an administrator command so it may not be available on all databases / configurations.
	if info, err := getBuildInfo(ctx, client); err != nil {
		log.WithError(err).Info("could not query buildInfo")
	} else {
		log.WithFields(log.Fields{
			"version": info.Version,
		}).Info("buildInfo")
	}

	var c = capture{
		cfg:      cfg,
		output:   stream,
		bindings: bindings,
		state:    prevState,
	}

	if err := c.output.Ready(false); err != nil {
		return err
	}

	// Persist the checkpoint in full, which may have been updated in updateResourceStates to remove
	// bindings.
	if err := c.outputCheckpoint(&prevState, false); err != nil {
		return err
	}

	streams, err := initializeStreams(ctx, client, prevState, bindings)
	if err != nil {
		return fmt.Errorf("initializing changeStreams: %w", err)
	}

	for !prevState.isBackfillComplete(bindings) {
		// Catchup streams
		if err := c.streamCatchup(ctx, streams, client); err != nil {
			return fmt.Errorf("catching up streams: %w", err)
		}

		// Backfill for a while
	}

	return c.streamForever(ctx, streams)
}

func (c *capture) outputCheckpoint(state *captureState, merge bool) error {
	cp, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshalling checkpoint: %w", err)
	} else if err = c.output.Checkpoint(cp, merge); err != nil {
		return fmt.Errorf("outputting checkpoint: %w", err)
	}
	return nil
}

func (c *capture) backfillAllCollections(ctx context.Context, bindings []bindingInfo, state *captureState, client *mongo.Client) error {

	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(concurrentBackfillLimit)

	for _, binding := range bindings {
		var binding = binding
		var resState = state.Resources[binding.stateKey]

		if !resState.Backfill.Done {
			eg.Go(func() error {
				return c.BackfillCollection(egCtx, client, binding, resState)
			})
		}
	}

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("backfill: %w", err)
	}
	return nil
}

type capture struct {
	cfg      config
	output   *boilerplate.PullOutput
	bindings []bindingInfo
	state    captureState
}

type bindingInfo struct {
	resource resource
	index    int
	stateKey boilerplate.StateKey
}

type captureState struct {
	Resources map[boilerplate.StateKey]resourceState `json:"bindingStateV1,omitempty"`

	// GlobalResumeToken is used when the capture user has permissiosn to read all database of the
	// deployment, and a deployment-wide change stream can be used.
	GlobalResumeToken bson.Raw `json:"globalResumeToken,omitempty"`

	// DatabaseResumeTokens is a mapping of database names to their respective change stream resume
	// token. Database-specific change streams are used when a deployment-wide one can't be.
	DatabaseResumeTokens map[string]bson.Raw `json:"databaseResumeTokens,omitempty"`
}

// type oldCaptureState struct {
// 	Resources                map[boilerplate.StateKey]resourceState `json:"bindingStateV1,omitempty"`
// 	OplogEmptyBeforeBackfill *bool                                  `json:"oplog_empty_before_backfill,omitempty"`
// 	StreamResumeToken        bson.Raw                               `json:"stream_resume_token,omitempty"`
// }

// decodeState is mostly a pile of gnarly code to deal with migrating from `oldCaptureState` to
// `captureState`. It does a partial unmarshalling of the provided `stateJson` to determine if the
// checkpoint is from a prior structure, and arranges pre-existing data into the new checkpoint
// structure, if applicable. We should get rid of this when all existing tasks have migrated to the
// new checkpoints.
func decodeState(
	ctx context.Context,
	client *mongo.Client,
	stateJson json.RawMessage,
	bindings []bindingInfo,
) (captureState, error) {
	if stateJson == nil || reflect.DeepEqual(stateJson, json.RawMessage("{}")) {
		return captureState{
			Resources:            make(map[boilerplate.StateKey]resourceState),
			DatabaseResumeTokens: make(map[string]bson.Raw),
		}, nil
	}

	// Is this an old form the capture state? If so it needs to be migrated.
	var initialState map[string]json.RawMessage
	if err := json.Unmarshal(stateJson, &initialState); err != nil {
		return captureState{}, fmt.Errorf("initial decode of stateJson: %w", err)
	}

	var resources map[boilerplate.StateKey]resourceState
	if res, ok := initialState["bindingStateV1"]; ok {
		if err := json.Unmarshal(res, &resources); err != nil {
			return captureState{}, fmt.Errorf("unmarshal res into resources: %w", err)
		}
	} else {
		resources = make(map[boilerplate.StateKey]resourceState)
	}

	if tokJson, ok := initialState["stream_resume_token"]; ok {
		// Old state must be migrated. We have a stream_resume_token, is it for a deployment-wide
		// stream or a specific database stream?
		var tok bson.Raw
		if err := json.Unmarshal(tokJson, &tok); err != nil {
			return captureState{}, fmt.Errorf("unmarshal tokJson into tok: %w", err)
		}

		// See if we can do a global stream. If we can, that's what this token is for.
		cs, err := client.Watch(ctx, &mongo.Pipeline{})
		if err != nil {
			if e, ok := err.(mongo.ServerError); ok {
				if e.HasErrorMessage(unauthorizedMessage) {
					// This is for a database-level stream. Previous versions of the connector only
					// allowed for a database-level stream if there was a single database, so it
					// must be for that one.
					var db string
					for _, b := range bindings {
						if db != "" && b.resource.Database != db {
							// Sanity check.
							return captureState{}, fmt.Errorf("found multiple databases with a database-level change stream: %w", err)
						}
						db = b.resource.Database
					}

					log.WithFields(log.Fields{
						"db":          db,
						"resumeToken": tok.String(),
					}).Info("migrated state with database-level change stream")
					return captureState{
						Resources:            resources,
						DatabaseResumeTokens: map[string]bson.Raw{db: tok}}, nil
				}
			}
			return captureState{}, fmt.Errorf("attempting to create change stream in decodeState: %w", err)
		}
		defer cs.Close(ctx)

		// Global stream.
		log.WithFields(log.Fields{
			"resumeToken": tok.String(),
		}).Info("migrated state with global change stream")
		return captureState{
			Resources:         resources,
			GlobalResumeToken: tok,
		}, nil
	}

	var out captureState
	if err := json.Unmarshal(stateJson, &out); err != nil {
		return captureState{}, fmt.Errorf("unmarshal stateJson: %w", err)
	}

	return out, nil
}

func updateResourceStates(prevState captureState, bindings []bindingInfo) (captureState, error) {
	var newState = captureState{
		Resources:            make(map[boilerplate.StateKey]resourceState),
		GlobalResumeToken:    prevState.GlobalResumeToken,
		DatabaseResumeTokens: prevState.DatabaseResumeTokens,
	}

	// Only include bindings in the state that are currently active bindings. This is necessary
	// because the Flow runtime does not yet automatically prune stateKeys that are no longer
	// included as bindings. A binding becomes inconsistent once removed from the capture
	// because its change events will begin to be filtered out, and must start over if ever
	// re-added.
	for _, binding := range bindings {
		var sk = binding.stateKey
		if resState, ok := prevState.Resources[sk]; ok {
			newState.Resources[sk] = resState
		}
	}

	// Reset the global resume token if there are no active resources, and also the
	// database-specific resume tokens if there are no active resources for that database.
	if len(newState.Resources) == 0 && newState.GlobalResumeToken != nil {
		log.WithField("priorToken", newState.GlobalResumeToken).Info("resetting global change stream resume token")
		newState.GlobalResumeToken = nil
	}

	for db, tok := range newState.DatabaseResumeTokens {
		if !slices.ContainsFunc(bindings, func(b bindingInfo) bool { return b.resource.Database == db }) {
			log.WithFields(log.Fields{
				"database":   db,
				"priorToken": tok.String(),
			}).Info("reseting change stream resume token for database")
			delete(newState.DatabaseResumeTokens, db)
		}
	}

	return newState, nil
}

func (s *captureState) isBackfillComplete(bindings []bindingInfo) bool {
	for _, b := range bindings {
		if !s.Resources[b.stateKey].Backfill.Done {
			return false
		}
	}
	return true
}

func (s *captureState) Validate() error {
	return nil
}

type backfillState struct {
	Done bool `json:"done,omitempty"`

	StartedAt time.Time     `json:"started_at,omitempty"`
	LastId    bson.RawValue `json:"last_id,omitempty"`
}

type resourceState struct {
	Backfill backfillState `json:"backfill,omitempty"`
}

const resumePointGoneErrorMessage = "the resume point may no longer be in the oplog"
const resumeTokenNotFoundErrorMessage = "the resume token was not found"
const unauthorizedMessage = "not authorized on admin to execute command"

func (c *capture) BackfillCollection(ctx context.Context, client *mongo.Client, binding bindingInfo, state resourceState) (_err error) {
	var db = client.Database(binding.resource.Database)
	var collection = db.Collection(binding.resource.Collection)
	var res = binding.resource
	var sk = binding.stateKey

	if state.Backfill.StartedAt.IsZero() {
		// This means we are starting backfill for the first time
		state.Backfill.StartedAt = time.Now()
	}

	// By not specifying a sort parameter, MongoDB uses natural sort to order
	// documents. Natural sort is approximately insertion order (but not
	// guaranteed). We hint to MongoDB to use the _id index (an index that always
	// exists) to speed up the process. Note that if we specify the sort
	// explicitly by { $natural: 1 }, then the database will disregard any indices
	// and do a full collection scan.
	// See https://www.mongodb.com/docs/manual/reference/method/cursor.hint
	var opts = options.Find().SetBatchSize(BackfillBatchSize).SetHint(bson.M{"_id": 1})
	var filter = bson.D{}
	if state.Backfill.LastId.Validate() == nil {
		var v interface{}
		if err := state.Backfill.LastId.Unmarshal(&v); err != nil {
			return fmt.Errorf("unmarshalling backfill_last_id: %w", err)
		}
		filter = bson.D{{idProperty, bson.D{{"$gt", v}}}}
	}
	log.WithFields(log.Fields{
		"mongoCollection": res.Collection,
		"opts":            opts,
		"filter":          filter,
	}).Info("starting backfill of collection")

	var documentCount = 0
	defer func() {
		var fields = log.Fields{
			"mongoCollection":         res.Collection,
			"backfilledDocumentCount": documentCount,
		}
		if _err == nil {
			log.WithFields(fields).Info("finished backfill of collection")
		} else {
			fields["error"] = _err
			log.WithFields(fields).Errorf("failed to backfill collection")
		}
	}()

	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return fmt.Errorf("could not run find query on collection %s: %w", res.Collection, err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		documentCount += 1

		// We decode once into a map of RawValues so we can keep the _id as a
		// RawValue. This allows us to marshal it as an encoded byte array along
		// with its type code in the checkpoint, and unmarshal it when we want to
		// use it to resume the backfill, ensuring the type of the _id is consistent
		// through this process
		var rawDoc map[string]bson.RawValue
		if err = cursor.Decode(&rawDoc); err != nil {
			return fmt.Errorf("decoding document raw in collection %s: %w", res.Collection, err)
		}
		state.Backfill.LastId = rawDoc[idProperty]

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

		if err = c.output.Documents(binding.index, js); err != nil {
			return fmt.Errorf("output documents failed: %w", err)
		}

		// This means the next call to cursor.Next() will trigger a new network
		// request. Before we do that, we checkpoint so that we can resume from this
		// point after a failure.
		if cursor.RemainingBatchLength() == 0 {
			var checkpoint = captureState{
				Resources: map[boilerplate.StateKey]resourceState{
					sk: state,
				},
			}

			if err = c.outputCheckpoint(&checkpoint, true); err != nil {
				return err
			}
		}
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor error for backfill %s: %w", res.Collection, err)
	}

	state.Backfill.Done = true
	var checkpoint = captureState{
		Resources: map[boilerplate.StateKey]resourceState{
			sk: state,
		},
	}

	if err = c.outputCheckpoint(&checkpoint, true); err != nil {
		return err
	}

	return nil
}

// MongoDB considers datetimes outside of the 0-9999 year range to be _unsafe_
// so we enforce this constraint in our connector.
// see https://www.mongodb.com/docs/manual/reference/method/Date/#behavior
const maxTimeMilli = 253402300799999
const minTimeMilli = -62167219200000

func sanitizePrimitive(input interface{}) interface{} {
	switch v := input.(type) {
	case primitive.DateTime:
		if v < minTimeMilli {
			return primitive.DateTime(minTimeMilli)
		} else if v > maxTimeMilli {
			return primitive.DateTime(maxTimeMilli)
		}
	case float64:
		if math.IsNaN(v) {
			return "NaN"
		} else if math.IsInf(v, +1) {
			return "Infinity"
		} else if math.IsInf(v, -1) {
			return "-Infinity"
		}
	case map[string]interface{}:
		return sanitizeDocument(v)
	case primitive.M:
		return sanitizeDocument(v)
	case []interface{}:
		return sanitizeArray(v)
	case primitive.A:
		return sanitizeArray(v)
	}

	return input
}

func sanitizeDocument(doc map[string]interface{}) map[string]interface{} {
	for key, value := range doc {
		// Make sure `_id` is always captured as string
		if key == idProperty {
			doc[key] = idToString(value)
		} else {
			doc[key] = sanitizePrimitive(value)
		}
	}

	return doc
}

func sanitizeArray(arr []interface{}) []interface{} {
	for i, value := range arr {
		arr[i] = sanitizePrimitive(value)
	}

	return arr
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

type buildInfo struct {
	Version string `bson:"version"`
}

func getBuildInfo(ctx context.Context, client *mongo.Client) (buildInfo, error) {
	var info buildInfo
	if res := client.Database("admin").RunCommand(ctx, bson.D{{"buildInfo", 1}}); res.Err() != nil {
		return info, res.Err()
	} else if err := res.Decode(&info); err != nil {
		return info, err
	} else {
		return info, nil
	}
}
