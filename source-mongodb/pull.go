package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
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

// Limit the number of backfills running concurrently to avoid
// too much memory pressure
const ConcurrentBackfillLimit = 10

// Pull is a very long lived RPC through which the Flow runtime and a
// Driver cooperatively execute an unbounded number of transactions.
func (d *driver) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	var cfg config
	if err := pf.UnmarshalStrict(open.Capture.ConfigJson, &cfg); err != nil {
		return fmt.Errorf("parsing config json: %w", err)
	}

	var prevState captureState
	if open.StateJson != nil {
		if err := json.Unmarshal(open.StateJson, &prevState); err != nil {
			return fmt.Errorf("parsing checkpoint json %s: %w", string(open.StateJson), err)
		}
	}

	var resources = make([]resource, len(open.Capture.Bindings))
	for idx, binding := range open.Capture.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}
		resources[idx] = res
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

	// Ensure that we're able to query the oplog, and get the timestamp of the oldest entry
	// so that we can check for the possibility that we've missed events during downtime.
	oldestOplogTime, err := checkOplog(ctx, client)
	if err != nil {
		return err
	}

	var c = capture{
		cfg:    cfg,
		Output: stream,
	}

	if err := c.Output.Ready(false); err != nil {
		return err
	}

	// Remove bindings from the checkpoint that are no longer part of the spec.
	activeBindings := make(map[string]struct{})
	for _, res := range resources {
		activeBindings[resourceId(res)] = struct{}{}
	}
	emitCheckpoint := false
	for res := range prevState.Resources {
		if _, ok := activeBindings[res]; !ok {
			log.WithField("resource", res).Info("binding removed from catalog")
			delete(prevState.Resources, res)
			emitCheckpoint = true
		}
	}

	// if all bindings are removed, reset the state
	if len(activeBindings) == 0 {
		prevState = captureState{}
		emitCheckpoint = true
	}
	if emitCheckpoint {
		if err = c.outputCheckpoint(&prevState, false); err != nil {
			return err
		}
	}

	if !prevState.isBackfillComplete(resources) {
		var stateUpdated, restartReason = checkBackfillState(&prevState, oldestOplogTime)
		if restartReason != "" {
			return c.startOverDueToConsistencyLoss(restartReason)
		} else if stateUpdated {
			c.outputCheckpoint(&prevState, false)
		}

		if err = c.backfillAllCollections(ctx, resources, &prevState, client); err != nil {
			return fmt.Errorf("backfilling: %w", err)
		}
		var totalBackfillTime = time.Now().UTC().Sub(prevState.getBackfillStartTime())
		log.WithField("totalBackfillTime", totalBackfillTime).Info("completed backfill")

		// Update the timestamp of the oldest oplog entry now that we've finished the backfill
		oldestOplogTime, err = oldestOplogEntry(ctx, client)
		if err == mongo.ErrNoDocuments && prevState.wasChangeStreamEmptyBeforeBackfill() {
			log.Info("ignoring empty oplog error because the oplog was empty before the backfill started")
		} else if err == mongo.ErrNoDocuments {
			// We had previously observed events in the oplog, but now there are none.
			// This means we have now way of knowing whether we've missed any of the
			// events, and we'll need to bail out.
			return fmt.Errorf("the oplog is now empty, but had previously contained entries")
		} else {
			log.WithField("oldestOplogEntry", oldestOplogTime).Info("fetched oldest oplog time after completing backfill")
		}
	}

	// Once we finish the backfill (which may have been completed by a previous instance of the
	// connector), check to make sure we can start reading the change stream before we started
	// the backfill. Skip this check if we've already started reading the change stream, or if
	// we'd only observed an empty change stream before completing the backfill.
	if len(prevState.StreamResumeToken) == 0 && !prevState.wasChangeStreamEmptyBeforeBackfill() {
		if _, restartReason := checkBackfillState(&prevState, oldestOplogTime); restartReason != "" {
			// Automatically restarting the backfill is probably not the best failure mode when
			// we weren't able to finish the first backfill in time. But until we update this to
			// work with StateKey (and thus the backfill counter), users won't have an easy way
			// to do this manually.
			c.startOverDueToConsistencyLoss(restartReason)
		}
	}

	return c.ChangeStream(ctx, client, resources, prevState)
}

func (c *capture) outputCheckpoint(state *captureState, merge bool) error {
	cp, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshalling checkpoint: %w", err)
	} else if err = c.Output.Checkpoint(cp, merge); err != nil {
		return fmt.Errorf("outputting checkpoint: %w", err)
	}
	return nil
}

func (c *capture) backfillAllCollections(ctx context.Context, resources []resource, state *captureState, client *mongo.Client) error {

	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(ConcurrentBackfillLimit)

	for idx, resource := range resources {
		idx := idx
		resource := resource

		var resState = state.Resources[resourceId(resource)]

		if !resState.Backfill.Done {
			eg.Go(func() error {
				return c.BackfillCollection(egCtx, client, idx, resource, resState)
			})
		}
	}

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("backfill: %w", err)
	}
	return nil
}

type capture struct {
	cfg    config
	Output *boilerplate.PullOutput
}

type captureState struct {
	Resources map[string]resourceState `json:"resources,omitempty"`

	// OplogEmptyBeforeBackfill indicates whether the connector has observed
	// an empty oplog before starting the backfill, and on every subsequent
	// startup before the backfill is completed. If, and only if, the oplog has
	// been observed to be empty the entire time before the backfill completes,
	// then it is allowed to still be empty after it completes. Otherwise,
	// the connector expects at least one entry in the oplog, so that it can
	// use its timestamp to ensure we haven't missed any events while backfilling.
	OplogEmptyBeforeBackfill *bool `json:"oplog_empty_before_backfill,omitempty"`

	// Resume token used to continue change stream from where we last left off,
	// see https://www.mongodb.com/docs/manual/changeStreams/#resume-tokens-from-change-events
	StreamResumeToken bson.Raw `json:"stream_resume_token,omitempty"`
}

func (s *captureState) wasChangeStreamEmptyBeforeBackfill() bool {
	if s.OplogEmptyBeforeBackfill != nil {
		return *s.OplogEmptyBeforeBackfill
	}
	return false
}

func (s *captureState) getBackfillStartTime() time.Time {
	var start time.Time
	for _, r := range s.Resources {
		if start.IsZero() || start.After(r.Backfill.StartedAt) {
			start = r.Backfill.StartedAt
		}
	}
	return start
}

func (s *captureState) isBackfillComplete(resources []resource) bool {
	for _, r := range resources {
		if !s.Resources[resourceId(r)].Backfill.Done {
			return false
		}
	}
	return len(s.Resources) > 0
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

type docKey struct {
	Id interface{} `bson:"_id"`
}
type namespace struct {
	Database   string `bson:"db"`
	Collection string `bson:"coll"`
}
type changeEvent struct {
	DocumentKey   docKey    `bson:"documentKey"`
	OperationType string    `bson:"operationType"`
	FullDocument  bson.M    `bson:"fullDocument"`
	Ns            namespace `bson:"ns"`
}

const resumePointGoneErrorMessage = "the resume point may no longer be in the oplog"
const resumeTokenNotFoundErrorMessage = "the resume token was not found"
const unauthorizedMessage = "not authorized on admin to execute command"

func (c *capture) ChangeStream(ctx context.Context, client *mongo.Client, resources []resource, state captureState) error {
	if len(resources) < 1 {
		return nil
	}

	// If we have a stream resume token, we use that to continue our change stream
	// otherwise, we look at Backfill.StartedAt of all resources and use the
	// minimum value to start our change stream from
	var streamStartAt time.Time
	if len(state.StreamResumeToken) == 0 {
		streamStartAt = state.getBackfillStartTime()
	}

	var collectionBindingIndex = make(map[string]int)
	for idx, res := range resources {
		collectionBindingIndex[resourceId(res)] = idx
	}

	log.WithFields(log.Fields{
		"state":             state,
		"streamResumeToken": state.StreamResumeToken,
		"streamStartAt":     streamStartAt,
	}).Info("listening on changes")

	var opts = options.ChangeStream().SetFullDocument(options.UpdateLookup)

	if len(state.StreamResumeToken) > 0 {
		opts = opts.SetResumeAfter(state.StreamResumeToken)
	} else if !streamStartAt.IsZero() {
		var safetyBufferConfig = c.cfg.Advanced.OplogSafetyBuffer
		if len(safetyBufferConfig) < 1 {
			safetyBufferConfig = "-5m"
		}
		var oplogSafetyBuffer, _ = time.ParseDuration(safetyBufferConfig)

		var startAtWithSafety = streamStartAt.Add(oplogSafetyBuffer)
		if e := oplogHasTimestamp(ctx, client, startAtWithSafety); e != nil {
			cp, err := json.Marshal(captureState{})
			if err != nil {
				return fmt.Errorf("marshalling reset checkpoint: %w", err)
			} else if err = c.Output.Checkpoint(cp, false); err != nil {
				return fmt.Errorf("resetting checkpoint: %w", err)
			}
			return e
		}
		var startAt = &primitive.Timestamp{T: uint32(startAtWithSafety.Unix()) - 1, I: 0}
		opts = opts.SetStartAtOperationTime(startAt)
	}

	// First attempt to do an instance-wide change stream, this is possible if we
	// have readAnyDatabase access on the instance. If this fails for permission
	// issues, we then attempt to do a database-level change stream. In the case
	// of the database-level change stream we expect only a single database to be
	// provided and do not support multiple databases in this mode.
	cursor, err := client.Watch(ctx, mongo.Pipeline{}, opts)
	if err != nil {
		// This error means events from the starting point are no longer available,
		// which means we have hit a gap in between last run of the connector and
		// this one.
		if e, ok := err.(mongo.ServerError); ok {
			if e.HasErrorMessage(resumePointGoneErrorMessage) || e.HasErrorMessage(resumeTokenNotFoundErrorMessage) {
				cp, err := json.Marshal(captureState{})
				if err != nil {
					return fmt.Errorf("marshalling reset checkpoint: %w", err)
				} else if err = c.Output.Checkpoint(cp, false); err != nil {
					return fmt.Errorf("resetting checkpoint: %w", err)
				}
				return fmt.Errorf("change stream cannot resume capture, this is usually due to insufficient oplog storage. Please consider resizing your oplog to be able to safely capture data from your database: https://go.estuary.dev/NurkrE. The connector will now attempt to backfill all bindings again: %w", e)
			} else if e.HasErrorMessage(unauthorizedMessage) {
				log.WithField("reason", e).Warn("could not start instance-level change stream, trying database-level change stream.")
				// Check that there is only one database, if so, we can start a change
				// stream on this database. If there is more than one database
				// configured, we error out as we do not support multiple change streams
				// on different databases. An instance-level change stream must be used
				// for that scenario
				var dbName = resources[0].Database
				for _, res := range resources[1:] {
					if res.Database != dbName {
						return fmt.Errorf("We are unable to establish an instance-level change stream on your database due to an error. Please consider granting permission to read all databases to the user: %w", err)
					}
				}

				var db = client.Database(dbName)
				cursor, err = db.Watch(ctx, mongo.Pipeline{}, opts)
				if e, ok := err.(mongo.ServerError); ok {
					if e.HasErrorMessage(resumePointGoneErrorMessage) || e.HasErrorMessage(resumeTokenNotFoundErrorMessage) {
						cp, err := json.Marshal(captureState{})
						if err != nil {
							return fmt.Errorf("marshalling reset checkpoint: %w", err)
						} else if err = c.Output.Checkpoint(cp, false); err != nil {
							return fmt.Errorf("resetting checkpoint: %w", err)
						}
						return fmt.Errorf("change stream cannot resume capture, this is usually due to insufficient oplog storage. Please consider resizing your oplog to be able to safely capture data from your database: https://go.estuary.dev/NurkrE. The connector will now attempt to backfill all bindings again: %w", e)
					}
				}
			}
		}

		if err != nil {
			return fmt.Errorf("change stream: %w", err)
		}
	}

	for cursor.Next(ctx) {
		var ev changeEvent
		if err = cursor.Decode(&ev); err != nil {
			return fmt.Errorf("decoding document in collection %s.%s: %w", ev.Ns.Database, ev.Ns.Collection, err)
		}
		var resId = eventResourceId(ev)
		var binding, ok = collectionBindingIndex[resId]

		var checkpoint = captureState{
			StreamResumeToken: cursor.ResumeToken(),
		}

		checkpointJson, err := json.Marshal(checkpoint)
		if err != nil {
			return fmt.Errorf("encoding checkpoint to json failed: %w", err)
		}

		// this event is from a collection that we have no binding for, skip it, but
		// still emit a checkpoint with the resule token updated so that we can stay
		// on top of oplog events across the whole instance / database
		if !ok {
			if err = c.Output.Checkpoint(checkpointJson, true); err != nil {
				return fmt.Errorf("output checkpoint failed: %w", err)
			}
			continue
		}

		// These are the events we support
		if slices.Contains([]string{"insert", "update", "replace", "delete"}, ev.OperationType) {
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
					return fmt.Errorf("encoding document %v in collection %s.%s as json: %w", doc, ev.Ns.Database, ev.Ns.Collection, err)
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
					return fmt.Errorf("encoding document %v in collection %s.%s as json: %w", doc, ev.Ns.Database, ev.Ns.Collection, err)
				}

				if err = c.Output.DocumentsAndCheckpoint(checkpointJson, true, binding, js); err != nil {
					return fmt.Errorf("output documents failed: %w", err)
				}
			}
		}
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor error for change stream: %w", err)
	}

	defer cursor.Close(ctx)

	return nil
}

func checkBackfillState(state *captureState, oldestOplogEntry time.Time) (stateUpdated bool, restartReason string) {
	// Check and update the ChangeStreamEmptyBeforeBackfill property if necessary
	var backfillStartTime = state.getBackfillStartTime()

	log.WithFields(log.Fields{
		"resumingInProgressBackfill": !backfillStartTime.IsZero(),
		"backfillStartTime":          backfillStartTime,
		"oldestOplogEntry":           oldestOplogEntry,
	}).Debug("checking backfill state")

	if backfillStartTime.IsZero() {
		// We're starting a new backfill, so emit a state update indicating
		// whether we've observed events in the oplog.
		var isEmpty = oldestOplogEntry.IsZero()
		state.OplogEmptyBeforeBackfill = &isEmpty
		return true, ""
	} else if state.wasChangeStreamEmptyBeforeBackfill() && !oldestOplogEntry.IsZero() {
		// We're resuming an in-progress backfill and there is now an event
		// that's been observed in the change stream. Update the state accordingly
		var superHighTechFalse = false
		state.OplogEmptyBeforeBackfill = &superHighTechFalse
		return true, ""
	} else if !state.wasChangeStreamEmptyBeforeBackfill() && oldestOplogEntry.IsZero() {
		// We're in a bad state. We've already started a backfill, which we're resuming.
		// We'd previously observed that there were events in the change stream, but now
		// there's no documents in the change stream, and so it's impossible for us to tell
		// if we'll be able to transition to reading the change stream without having missed
		// data. So we bail out and re-start the backfill.
		return false, "previously non-empty change stream is now empty"
	} else if backfillStartTime.Before(oldestOplogEntry) {
		return false, fmt.Sprintf("the oldest event in the mongodb oplog (%s) is more recent than the starting time of the backfill", oldestOplogEntry)
	}
	return false, ""
}

// startOverDueToConsistencyLoss outputs an empty checkpoint and completely overwrites the state
// of the capture, so that it can start over from the beginning. It always returns a non-nil
// error, which the caller should bubble up directly in order to terminate the connector.
func (c *capture) startOverDueToConsistencyLoss(detail string) error {
	log.WithField("detail", detail).Warn("re-starting due to possible loss of consistency")
	var emptyCP = captureState{}
	if cpErr := c.outputCheckpoint(&emptyCP, false); cpErr != nil {
		return fmt.Errorf("resetting state: %w", cpErr)
	}
	return fmt.Errorf("exiting in order to re-start backfill")
}

// We use a very large batch size here, however the go mongo driver will
// automatically use a smaller batch size capped at 16mb, as BSON documents have
// a 16mb size limit which restricts the size of a response from mongodb as
// well. See https://www.mongodb.com/docs/v7.0/tutorial/iterate-a-cursor/#cursor-batches
// Note that this number also represents how many documents are checkpointed at
// once
const BackfillBatchSize = 50000

const (
	NaturalSort    = "$natural"
	SortAscending  = 1
	SortDescending = -1
)

func (c *capture) BackfillCollection(ctx context.Context, client *mongo.Client, binding int, res resource, state resourceState) (_err error) {
	var db = client.Database(res.Database)
	var collection = db.Collection(res.Collection)

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

		if err = c.Output.Documents(binding, js); err != nil {
			return fmt.Errorf("output documents failed: %w", err)
		}

		// This means the next call to cursor.Next() will trigger a new network
		// request. Before we do that, we checkpoint so that we can resume from this
		// point after a failure.
		if cursor.RemainingBatchLength() == 0 {
			var checkpoint = captureState{
				Resources: map[string]resourceState{
					resourceId(res): state,
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
		Resources: map[string]resourceState{
			resourceId(res): state,
		},
	}

	if err = c.outputCheckpoint(&checkpoint, true); err != nil {
		return err
	}

	return nil
}

func resourceId(res resource) string {
	return fmt.Sprintf("%s.%s", res.Database, res.Collection)
}

func eventResourceId(ev changeEvent) string {
	return fmt.Sprintf("%s.%s", ev.Ns.Database, ev.Ns.Collection)
}

// MongoDB considers datetimes outside of the 0-9999 year range to be _unsafe_
// so we enforce this constraint in our connector.
// see https://www.mongodb.com/docs/manual/reference/method/Date/#behavior
const maxTimeMilli = 253402300799999
const minTimeMilli = -62167219200000

func sanitizeDocument(doc map[string]interface{}) map[string]interface{} {
	for key, value := range doc {
		// Make sure `_id` is always captured as string
		if key == idProperty {
			doc[key] = idToString(value)
		} else {
			switch v := value.(type) {
			case primitive.DateTime:
				if v < minTimeMilli {
					doc[key] = primitive.DateTime(minTimeMilli)
				} else if v > maxTimeMilli {
					doc[key] = primitive.DateTime(maxTimeMilli)
				}
			case float64:
				if math.IsNaN(v) {
					doc[key] = "NaN"
				} else if math.IsInf(v, +1) {
					doc[key] = "Infinity"
				} else if math.IsInf(v, -1) {
					doc[key] = "-Infinity"
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
