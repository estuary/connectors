package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/estuary/connectors/go/schedule"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/segmentio/encoding/json"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

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

	// The default polling schedule if none is supplied by the user.
	defualtBatchPollingSchedule = "24h"
)

// Pull is a very long lived RPC through which the Flow runtime and a
// Driver cooperatively execute an unbounded number of transactions.
func (d *driver) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	var cfg config
	if err := pf.UnmarshalStrict(open.Capture.ConfigJson, &cfg); err != nil {
		return fmt.Errorf("parsing config json: %w", err)
	}

	if cfg.PollSchedule == "" {
		cfg.PollSchedule = defualtBatchPollingSchedule
	}
	defaultSched, err := schedule.Parse(cfg.PollSchedule)
	if err != nil {
		return fmt.Errorf("invalid default poll schedule: %w", err)
	}

	excludeCollections, err := parseExcludeCollections(cfg.Advanced.ExcludeCollections)
	if err != nil {
		return err
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

	var databaseNames = make(map[string]bool)
	for _, binding := range open.Capture.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}
		databaseNames[res.Database] = true
	}

	var timeseriesCollections = make(map[string]bool)
	for db := range databaseNames {
		collections, err := client.Database(db).ListCollectionSpecifications(ctx, bson.D{})
		if err != nil {
			return fmt.Errorf("listing collections: %w", err)
		}

		for _, coll := range collections {
			if collectionType := mongoCollectionType(coll.Type); err != nil {
				return fmt.Errorf("unsupported collection type: %w", err)
			} else if collectionType == mongoCollectionTypeTimeseries {
				timeseriesCollections[resourceId(db, coll.Name)] = true
			}
		}
	}

	var changeStreamBindings = make([]bindingInfo, 0, len(open.Capture.Bindings))
	var batchBindings = make([]bindingInfo, 0, len(open.Capture.Bindings))
	var trackedChangeStreamBindings = make(map[string]bindingInfo)
	for idx, binding := range open.Capture.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}

		info := bindingInfo{
			resource:     res,
			index:        idx,
			stateKey:     boilerplate.StateKey(binding.StateKey),
			isTimeseries: timeseriesCollections[resourceId(res.Database, res.Collection)],
		}

		if res.getMode() == captureModeChangeStream {
			changeStreamBindings = append(changeStreamBindings, info)
			trackedChangeStreamBindings[resourceId(res.Database, res.Collection)] = info
		} else {
			info.schedule = defaultSched
			if res.PollSchedule != "" {
				info.schedule, err = schedule.Parse(res.PollSchedule)
				if err != nil {
					return fmt.Errorf("invalid poll schedule for %q: %w", binding.ResourcePath, err)
				}
			}
			batchBindings = append(batchBindings, info)
		}
	}
	allBindings := append(changeStreamBindings, batchBindings...)

	var prevState captureState
	if err := pf.UnmarshalStrict(open.StateJson, &prevState); err != nil {
		return fmt.Errorf("unmarshalling previous state: %w", err)
	}

	prevState, err = updateResourceStates(prevState, allBindings)
	if err != nil {
		return fmt.Errorf("updating resource states: %w", err)
	}

	var c = capture{
		client:                      client,
		output:                      stream,
		state:                       prevState,
		trackedChangeStreamBindings: trackedChangeStreamBindings,
		lastEventClusterTime:        make(map[string]primitive.Timestamp),
	}

	// Start transcoder for change stream bindings.
	if len(changeStreamBindings) > 0 {
		transcoder, err := NewTranscoder(ctx)
		if err != nil {
			return fmt.Errorf("starting transcoder: %w", err)
		}
		defer transcoder.Stop()
		c.transcoder = transcoder
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

	if len(allBindings) == 0 {
		// No bindings to capture.
		return nil
	}

	serverInfo, err := getServerInfo(ctx, cfg, client, allBindings[0].resource.Database)
	if err != nil {
		return fmt.Errorf("checking server info: %w", err)
	}
	log.WithFields(log.Fields{
		"version":               serverInfo.version,
		"supportsChangeStreams": serverInfo.supportsChangeStreams,
		"supportsPreImages":     serverInfo.supportsPreImages,
		"supportsStartAfter":    serverInfo.supportsStartAfter,
	}).Info("connected to database")

	coordinator := newBatchStreamCoordinator(changeStreamBindings, func(ctx context.Context) (primitive.Timestamp, error) {
		return getClusterOpTime(ctx, client)
	})

	var maxAwaitTime *time.Duration
	if cfg.Advanced.MaxAwaitTime != "" {
		dur, err := time.ParseDuration(cfg.Advanced.MaxAwaitTime)
		if err != nil {
			return fmt.Errorf("parsing maxAwaitTime: %w", err)
		}
		maxAwaitTime = &dur
	}

	requestPreImages := serverInfo.supportsPreImages && !cfg.Advanced.DisablePreImages

	// Helper for (re-)initializing change streams with their various options.
	initStreamsFn := func(ctx context.Context) ([]*changeStream, error) {
		return c.initializeStreams(
			ctx,
			changeStreamBindings,
			maxAwaitTime,
			requestPreImages,
			serverInfo.supportsStartAfter,
			cfg.Advanced.ExclusiveCollectionFilter,
			excludeCollections,
		)
	}

	didBackfill := false
	for !prevState.isChangeStreamBackfillComplete(changeStreamBindings) {
		if !didBackfill {
			log.Info("backfilling incremental change stream collections")
		}
		didBackfill = true

		// Repeatedly catch-up reading change streams and backfilling tables for the specified
		// period of time. This allows resume tokens to be kept reasonably up to date while the
		// backfill is in progress.
		if streams, err := initStreamsFn(ctx); err != nil {
			return err
		} else if err := coordinator.startCatchingUp(ctx); err != nil {
			return err
		} else if err := c.streamChanges(ctx, coordinator, streams, true); err != nil {
			return err
		} else if err := c.backfillChangeStreamCollections(ctx, coordinator, changeStreamBindings, backfillFor); err != nil {
			return err
		}
	}

	if didBackfill {
		log.Info("finished backfill of incremental change stream collections")
	}
	group, groupCtx := errgroup.WithContext(ctx)

	if len(changeStreamBindings) > 0 {
		log.Info("streaming change events indefinitely")
		streams, err := initStreamsFn(groupCtx)
		if err != nil {
			return err
		}
		group.Go(func() error { return c.streamChanges(groupCtx, coordinator, streams, false) })
	}
	if len(batchBindings) > 0 {
		log.Info("processing scheduled batch collections indefinitely")
		group.Go(func() error { return c.backfillBatchCollections(groupCtx, coordinator, batchBindings) })
	}
	if len(batchBindings) > 0 && len(changeStreamBindings) > 0 {
		group.Go(func() error { return coordinator.startRepeatingStreamCatchups(groupCtx) })
	}

	return group.Wait()
}

type capture struct {
	client                      *mongo.Client
	output                      *boilerplate.PullOutput
	trackedChangeStreamBindings map[string]bindingInfo
	transcoder                  *Transcoder

	// mu provides synchronization for values that are accessed by concurrent backfill and change
	// stream goroutines.
	mu                    sync.Mutex
	state                 captureState
	processedStreamEvents int
	emittedStreamDocs     int
	lastEventClusterTime  map[string]primitive.Timestamp
}

func getClusterOpTime(ctx context.Context, client *mongo.Client) (primitive.Timestamp, error) {
	var out primitive.Timestamp

	// Note: Although we are using the "admin" database, this command works for any database, even
	// if it doesn't exist, no matter what permissions the connecting user has.
	var raw bson.Raw
	var err error
	if raw, err = client.Database("admin").RunCommand(ctx, bson.M{"hello": "1"}).Raw(); err != nil {
		// Very old versions of MongoDB do not support the 'hello' command and
		// must use the deprecated 'isMaster' command instead. In these cases
		// it's not very efficient to always call 'hello' first and see that it
		// fails, but this function is only used every 5 minutes so it doesn't
		// really matter.
		log.Debug("running 'hello' command failed, falling back to 'isMaster' command")
		if raw, err = client.Database("admin").RunCommand(ctx, bson.M{"isMaster": "1"}).Raw(); err != nil {
			return out, fmt.Errorf("fetching 'operationTime', both 'hello' and 'isMaster' commands failed: %w", err)
		}
	}
	if opRaw, err := raw.LookupErr("operationTime"); err != nil {
		return out, fmt.Errorf("looking up 'operationTime' field: %w", err)
	} else if err := opRaw.Unmarshal(&out); err != nil {
		return out, fmt.Errorf("unmarshaling 'operationTime' field: %w", err)
	}

	return out, nil
}

type bindingInfo struct {
	resource     resource
	index        int
	stateKey     boilerplate.StateKey
	schedule     schedule.Schedule
	isTimeseries bool
}

type captureState struct {
	Resources            map[boilerplate.StateKey]resourceState `json:"bindingStateV1,omitempty"`
	DatabaseResumeTokens map[string]bson.Raw                    `json:"databaseResumeTokens,omitempty"`
}

func updateResourceStates(prevState captureState, allBindings []bindingInfo) (captureState, error) {
	var newState = captureState{
		Resources:            make(map[boilerplate.StateKey]resourceState),
		DatabaseResumeTokens: make(map[string]bson.Raw),
	}

	trackedDatabases := make(map[string]struct{})

	// Only include bindings in the state that are currently active bindings.
	// This is necessary because the Flow runtime does not yet automatically
	// prune stateKeys that are no longer included as bindings. Also, a change
	// stream binding becomes inconsistent once removed from the capture because
	// its change events will begin to be filtered out, and must start over if
	// ever re-added.
	if prevState.Resources != nil {
		for _, binding := range allBindings {
			var sk = binding.stateKey
			if resState, ok := prevState.Resources[sk]; ok {
				newState.Resources[sk] = resState
				if binding.resource.getMode() == captureModeChangeStream {
					trackedDatabases[binding.resource.Database] = struct{}{}
				}
			}
		}
	}

	// Reset the database resume tokens if there are no active change stream
	// bindings for a given database.
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

func (s *captureState) isChangeStreamBackfillComplete(changeStreamBindings []bindingInfo) bool {
	for _, b := range changeStreamBindings {
		if !s.Resources[b.stateKey].Backfill.done() {
			return false
		}
	}
	return true
}

func (s *captureState) Validate() error {
	return nil
}

type backfillState struct {
	Done *bool `json:"done,omitempty"`
	// TODO(whb): LastCursorValue is serialized as "last_id" for backward
	// compatibility. I may change this later with a state migration but am
	// refraining from doing it right now.
	LastCursorValue *bson.RawValue `json:"last_id,omitempty"`
	BackfilledDocs  int            `json:"backfilled_docs,omitempty"`
	LastPollStart   *time.Time     `json:"last_poll_start,omitempty"`
}

func (s backfillState) done() bool {
	return s.Done != nil && *s.Done
}

func makePtr[T any](in T) *T {
	return &in
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

type serverInfo struct {
	version               string
	supportsPreImages     bool
	supportsChangeStreams bool
	supportsStartAfter    bool
}

type buildInfo struct {
	Version      string `bson:"version"`
	VersionArray []int  `bson:"versionArray"`
}

func getServerInfo(ctx context.Context, cfg config, client *mongo.Client, database string) (*serverInfo, error) {
	var buildInfo buildInfo
	if res := client.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}); res.Err() != nil {
		return nil, fmt.Errorf("running 'buildInfo' command: %w", res.Err())
	} else if err := res.Decode(&buildInfo); err != nil {
		return nil, fmt.Errorf("decoding buildInfo: %w", err)
	}

	if isDocDB, err := isDocumentDB(cfg.Address); err != nil {
		return nil, err
	} else if isDocDB {
		// DocumentDB supports change streams sort-of. The main limitation is
		// that they don't currently produce a post-batch resume token, so
		// there's really no way for us to make sure that the most recently
		// obtained token doesn't expire before new events are added to the
		// change stream. This makes them basically unusable if the desire is to
		// maintain 100% data consistency with the source, short of some more
		// sophisticated strategy like watermarking that we aren't going to do
		// right now.
		return &serverInfo{
			version:               buildInfo.Version,
			supportsPreImages:     false,
			supportsChangeStreams: false,
			supportsStartAfter:    false,
		}, nil
	}

	changeStreams, err := supportsChangeStreams(ctx, client, database)
	if err != nil {
		return nil, fmt.Errorf("checking change stream support: %w", err)
	}

	var preImages bool
	if changeStreams {
		if preImages, err = supportsPreImages(ctx, client, database, buildInfo); err != nil {
			return nil, fmt.Errorf("checking change stream pre-image support: %w", err)
		}
	}

	var startAfter bool
	if len(buildInfo.VersionArray) >= 3 && (buildInfo.VersionArray[0] > 4 || buildInfo.VersionArray[0] == 4 && buildInfo.VersionArray[1] >= 2) {
		// The startAfter parameter is supported by MongoDB versions 4.2 and
		// later.
		startAfter = true
	}

	return &serverInfo{
		version:               buildInfo.Version,
		supportsPreImages:     preImages,
		supportsChangeStreams: changeStreams,
		supportsStartAfter:    startAfter,
	}, nil
}

func supportsChangeStreams(ctx context.Context, client *mongo.Client, database string) (bool, error) {
	if cs, err := client.Database(database).Watch(ctx, mongo.Pipeline{}); err != nil {
		log.WithError(err).Info("connected server does not support change streams")
		return false, nil
	} else if err := cs.Close(ctx); err != nil {
		return false, fmt.Errorf("closing change stream: %w", err)
	}

	return true, nil
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
func supportsPreImages(ctx context.Context, client *mongo.Client, database string, b buildInfo) (bool, error) {
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
