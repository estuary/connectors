package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/segmentio/encoding/json"
	log "github.com/sirupsen/logrus"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	// Limit the number of backfills running concurrently to avoid excessive memory use and limit the
	// number of open cursors at a single time.
	concurrentBackfillLimit = 10

	// Backfill for this long each "round" before catching up change streams.
	backfillFor = 5 * time.Minute

	// backfillCheckpointSize and backfillBytesSize control how often we emit checkpoints while
	// backfilling collections. We checkpoint either on reaching a large amount of documents to
	// minimize replay reads when materializing the captured collections, or on reaching a
	// moderately large amount of buffered data to regulate connector memory use.
	backfillCheckpointSize = 50000
	backfillBytesSize      = 4 * 1024 * 1024

	// When the stream logger has been started, log a progress report at this frequency. The
	// progress report is currently just the count of stream documents that have been processed,
	// which provides a nice indication in the logs of what the capture is doing when it is reading
	// change streams.
	streamLoggerInterval = 5 * time.Minute
)

// Pull is a very long lived RPC through which the Flow runtime and a
// Driver cooperatively execute an unbounded number of transactions.
func (d *driver) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	var cfg config
	if err := pf.UnmarshalStrict(open.Capture.ConfigJson, &cfg); err != nil {
		return fmt.Errorf("parsing config json: %w", err)
	}

	var bindings = make([]bindingInfo, len(open.Capture.Bindings))
	var resourceBindingInfo = make(map[string]bindingInfo)
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
		resourceBindingInfo[resourceId(res)] = bindings[idx]
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

	// Log some basic information about the database we are connected to. Even
	// though this is an "admin" command, it always works with valid
	// credentials, regardless of the user's actual permissions.
	info, err := getBuildInfo(ctx, client)
	if err != nil {
		return fmt.Errorf("could not query buildInfo: %w", err)
	}
	log.WithFields(log.Fields{"version": info.Version}).Info("buildInfo")

	var prevState captureState
	if err := pf.UnmarshalStrict(open.StateJson, &prevState); err != nil {
		return fmt.Errorf("unmarshalling previous state: %w", err)
	}

	prevState, err = updateResourceStates(prevState, bindings)
	if err != nil {
		return fmt.Errorf("updating resource states: %w", err)
	}

	var c = capture{
		client:               client,
		output:               stream,
		state:                prevState,
		resourceBindingInfo:  resourceBindingInfo,
		lastEventClusterTime: make(map[string]primitive.Timestamp),
	}

	if err := c.output.Ready(false); err != nil {
		return err
	}

	// Persist the checkpoint in full, which may have been updated in updateResourceStates to remove
	// bindings.
	if checkpointJson, err := json.Marshal(c.state); err != nil {
		return fmt.Errorf("marshalling prevState checkpoint: %w", err)
	} else if err := c.output.Checkpoint(checkpointJson, false); err != nil {
		return fmt.Errorf("outputting prevState checkpoint: %w", err)
	}

	if len(bindings) == 0 {
		// No bindings to capture.
		return nil
	}

	requestPreImages, err := supportsPreImages(ctx, info, bindings[0].resource.Database, client)
	if err != nil {
		return fmt.Errorf("checking if server supports pre-images: %w", err)
	}

	didBackfill := false
	for !prevState.isBackfillComplete(bindings) {
		didBackfill = true
		// Repeatedly catch-up reading change streams and backfilling tables for the specified
		// period of time. This allows resume tokens to be kept reasonably up to date while the
		// backfill is in progress. The first call to initializeStreams opens change stream cursors,
		// and streamCatchup catches up the streams to the present and closes the cursors out while
		// the backfill is in progress.
		if streams, err := c.initializeStreams(ctx, bindings, requestPreImages, cfg.Advanced.ExclusiveCollectionFilter); err != nil {
			return err
		} else if err := c.streamCatchup(ctx, streams); err != nil {
			return err
		} else if err := c.backfillCollections(ctx, bindings, backfillFor); err != nil {
			return err
		}
	}

	if didBackfill {
		log.Info("backfill complete")
	}

	// Once all tables are done backfilling, we can read change streams forever.
	if streams, err := c.initializeStreams(ctx, bindings, requestPreImages, cfg.Advanced.ExclusiveCollectionFilter); err != nil {
		return err
	} else if err := c.streamForever(ctx, streams); err != nil {
		return fmt.Errorf("streaming changes forever: %w", err)
	}

	return nil
}

type capture struct {
	client              *mongo.Client
	output              *boilerplate.PullOutput
	resourceBindingInfo map[string]bindingInfo

	// mu provides synchronization for values that are accessed by concurrent backfill and change
	// stream goroutines.
	mu                    sync.Mutex
	state                 captureState
	processedStreamEvents int
	emittedStreamDocs     int
	lastEventClusterTime  map[string]primitive.Timestamp

	// Controls for starting and stopping the stream progress logger.
	streamLoggerStop   chan (struct{})
	streamLoggerActive sync.WaitGroup
}

type bindingInfo struct {
	resource resource
	index    int
	stateKey boilerplate.StateKey
}

type captureState struct {
	Resources            map[boilerplate.StateKey]resourceState `json:"bindingStateV1,omitempty"`
	DatabaseResumeTokens map[string]bson.Raw                    `json:"databaseResumeTokens,omitempty"`
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
	if prevState.Resources != nil {
		for _, binding := range bindings {
			var sk = binding.stateKey
			if resState, ok := prevState.Resources[sk]; ok {
				newState.Resources[sk] = resState
				trackedDatabases[binding.resource.Database] = struct{}{}
			}
		}
	}

	// Reset the database resume tokens if there are no active bindings for a given database.
	for db, tok := range prevState.DatabaseResumeTokens {
		if _, ok := trackedDatabases[db]; !ok {
			log.WithFields(log.Fields{
				"database":   db,
				"priorToken": tok,
			}).Info("resetting change stream resume token for database")
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
	Done           bool           `json:"done,omitempty"`
	LastId         *bson.RawValue `json:"last_id,omitempty"`
	BackfilledDocs int            `json:"backfilled_docs,omitempty"`
}

type resourceState struct {
	Backfill backfillState `json:"backfill"`
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
	Version      string `bson:"version"`
	VersionArray []int  `bson:"versionArray"`
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

// supportsPreImages returns true if the server supports pre-images. To support
// pre-images, the server must be a sufficiently recent version, and it must
// support the $changeStreamSplitLargeEvent pipeline stage that is required to
// split large change event documents that may arise from including the
// pre-image for updates to large documents.
//
// Support for the $changeStreamSplitLargeEvent pipeline stage was added in
// version 6.0.9 and 7.0.0+, so that's the first thing we check. Some versions
// of MongoDB Atlas also do not support the $changeStreamSplitLargeEvent
// pipeline stage, and the only way to know if we are connecting to one of those
// short of using the MongoDB Atlas admin API (which requires a separate API
// key) is to just try opening a change stream with the option enabled and
// seeing if it works or not.
func supportsPreImages(ctx context.Context, b buildInfo, database string, client *mongo.Client) (bool, error) {
	ll := log.WithField("version", b.Version)

	if len(b.VersionArray) < 3 {
		ll.WithField("VersionArray", b.VersionArray).Info("not requesting pre-images due to insufficient version information")
		return false, nil
	}

	major := b.VersionArray[0]
	minor := b.VersionArray[1]
	patch := b.VersionArray[2]
	sufficientVersion := major >= 7 || (major == 6 && patch >= 9) || (major == 6 && minor >= 1)

	if !sufficientVersion {
		ll.Info("not requesting pre-images because this MongoDB server version is too old")
		return false, nil
	}

	// Try opening a change stream with the $changeStreamSplitLargeEvent
	// pipeline stage.
	cs, err := client.Database(database).Watch(ctx, mongo.Pipeline{bson.D{{Key: "$changeStreamSplitLargeEvent", Value: bson.D{}}}})
	if err != nil {
		var commandError mongo.CommandError
		if errors.As(err, &commandError) {
			if commandError.Name == "AtlasError" && strings.HasPrefix(commandError.Message, "$changeStreamSplitLargeEvent is not allowed") {
				ll.WithField("atlasError", err).Info("not requesting pre-images because this MongoDB Atlas instance does not support the $changeStreamSplitLargeEvent stage")
				return false, nil
			}
		}
		return false, fmt.Errorf("opening change stream to test for $changeStreamSplitLargeEvent support: %w", err)
	}

	if err := cs.Close(ctx); err != nil {
		return false, fmt.Errorf("closing change stream: %w", err)
	}

	return true, nil
}
