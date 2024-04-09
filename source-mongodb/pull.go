package main

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// Pull is a very long lived RPC through which the Flow runtime and a
// Driver cooperatively execute an unbounded number of transactions.
func (d *driver) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	var cfg config
	if err := pf.UnmarshalStrict(open.Capture.ConfigJson, &cfg); err != nil {
		return fmt.Errorf("parsing config json: %w", err)
	}

	var bindings = make([]bindingInfo, len(open.Capture.Bindings))
	var collectionBindingIndex = make(map[string]bindingInfo)
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
		collectionBindingIndex[resourceId(res)] = bindings[idx]
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

	// Log some basic information about the database we are connected to if possible. buildInfo is
	// an administrator command so it may not be available on all databases / configurations.
	if info, err := getBuildInfo(ctx, client); err != nil {
		log.WithError(err).Info("could not query buildInfo")
	} else {
		log.WithFields(log.Fields{
			"version": info.Version,
		}).Info("buildInfo")
	}

	// TODO(whb): Remove this migration stuff when all migrations are complete - see comment on
	// `decodeState`.
	global, err := globalStream(ctx, client)
	if err != nil {
		return err
	}
	prevState, globalToken, err := decodeState(global, open.StateJson, bindings)
	if err != nil {
		return fmt.Errorf("decoding state: %w", err)
	}

	prevState, err = updateResourceStates(prevState, bindings)
	if err != nil {
		return fmt.Errorf("updating resource states: %w", err)
	}

	if prevState.Resources == nil {
		prevState.Resources = make(map[boilerplate.StateKey]resourceState)
	}
	if prevState.DatabaseResumeTokens == nil {
		prevState.DatabaseResumeTokens = make(map[string]bson.Raw)
	}

	var c = capture{
		client:                 client,
		output:                 stream,
		state:                  prevState,
		collectionBindingIndex: collectionBindingIndex,
	}

	// Get ready to emit documents and checkpoints.
	c.startEmitter(ctx)

	if err := c.output.Ready(false); err != nil {
		return err
	}

	// TODO(whb): More migration stuff to be removed when migrations are complete. A call to
	// populateDatabaseResumeTokens may emit documents but will not emit a checkpoint. A full
	// checkpoint will be emitted immediately after this though.
	if globalToken != nil {
		if err := c.populateDatabaseResumeTokens(ctx, *globalToken); err != nil {
			return fmt.Errorf("populating database-specific tokens from global resume token: %w", err)
		}
	}

	// Persist the checkpoint in full, which may have been updated in updateResourceStates to remove
	// bindings.
	if checkpointJson, err := json.Marshal(c.state); err != nil {
		return fmt.Errorf("marshalling prevState checkpoint: %w", err)
	} else if err := c.output.Checkpoint(checkpointJson, false); err != nil {
		return fmt.Errorf("outputting prevState checkpoint: %w", err)
	}

	for !prevState.isBackfillComplete(bindings) {
		// Create a copy of the binding state map to avoid races with the emit worker which will
		// update the global state. Each binding to be backfilled receives the initial copy of its
		// state and backfills the collection entirely from there, or time runs out and the process
		// starts over.
		resourceStates := make(map[boilerplate.StateKey]resourceState)
		maps.Copy(resourceStates, c.state.Resources)

		// Repeatedly catch-up reading change streams and backfilling tables for the specified
		// period of time. This allows resume tokens to be kept reasonably up to date while the
		// backfill is in progress. The first call to initializeStreams opens change stream cursors,
		// and streamCatchup catches up the streams to the present and closes the cursors out while
		// the backfill is in progress.
		if streams, err := c.initializeStreams(ctx, bindings); err != nil {
			return err
		} else if err := c.streamCatchup(ctx, streams); err != nil {
			return err
		} else if err := c.backfillCollections(ctx, resourceStates, bindings, backfillFor); err != nil {
			return err
		}

		// Flush the emitter before looping around to the next round where we will get a new copy of
		// the state.
		if err := c.flushEmitter(ctx); err != nil {
			return fmt.Errorf("flushing emitter after backfill round: %w", err)
		}
	}

	// Once all tables are done backfilling, we can read change streams forever.
	streams, err := c.initializeStreams(ctx, bindings)
	if err != nil {
		return err
	}

	streamFuture := pf.RunAsyncOperation(func() error {
		return c.streamForever(ctx, streams)
	})

	// If streamForever returns with an error we need to bail out, and similarly if the emitter ever
	// encounters an error we'll crash.
	select {
	case <-streamFuture.Done():
		if err := streamFuture.Err(); err != nil {
			return fmt.Errorf("streamFuture error from streamForever: %w", streamFuture.Err())
		}

		return nil
	case err := <-c.emitter.errors:
		return fmt.Errorf("emitter error while running streamForever: %w", err)
	}
}

type capture struct {
	client                 *mongo.Client
	output                 *boilerplate.PullOutput
	state                  captureState
	collectionBindingIndex map[string]bindingInfo
	emitter                emitter
}

type bindingInfo struct {
	resource resource
	index    int
	stateKey boilerplate.StateKey
}

type captureState struct {
	Resources map[boilerplate.StateKey]resourceState `json:"bindingStateV1,omitempty"`

	// GlobalResumeToken is used when the capture user has permissions to read all database of the
	// deployment, and a deployment-wide change stream can be used.
	// GlobalResumeToken bson.Raw `json:"globalResumeToken,omitempty"`

	// DatabaseResumeTokens is a mapping of database names to their respective change stream resume
	// token. Database-specific change streams are used when a deployment-wide one can't be.
	DatabaseResumeTokens map[string]bson.Raw `json:"databaseResumeTokens,omitempty"`
}

func updateResourceStates(prevState captureState, bindings []bindingInfo) (captureState, error) {
	var newState = captureState{
		Resources:            make(map[boilerplate.StateKey]resourceState),
		DatabaseResumeTokens: make(map[string]bson.Raw),
	}

	trackedDatabases := make(map[string]struct{})

	// Only include bindings in the state that are currently active bindings. This is necessary
	// because the Flow runtime does not yet automatically prune stateKeys that are no longer
	// included as bindings. A binding becomes inconsistent once removed from the capture
	// because its change events will begin to be filtered out, and must start over if ever
	// re-added.
	for _, binding := range bindings {
		var sk = binding.stateKey
		if resState, ok := prevState.Resources[sk]; ok {
			newState.Resources[sk] = resState
			trackedDatabases[binding.resource.Database] = struct{}{}
		}
	}

	// Reset the database resume tokens if there are no active bindings for a given database.
	for db, tok := range prevState.DatabaseResumeTokens {
		if _, ok := trackedDatabases[db]; !ok {
			log.WithFields(log.Fields{
				"database":   db,
				"priorToken": tok,
			}).Info("reseting change stream resume token for database")
			continue
		}

		newState.DatabaseResumeTokens[db] = tok
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
	Done   bool          `json:"done,omitempty"`
	LastId bson.RawValue `json:"last_id,omitempty"`
}

type resourceState struct {
	Backfill backfillState `json:"backfill,omitempty"`
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
