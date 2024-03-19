package main

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
)

// captureState can be completely empty, as in a brand new connector

// capture can be in streaming mode, which means that all bindings are done with the backfill and
// there is a single StreamResumeToken

// The StreamResumeToken is either for a global change stream or a database specific one. If its
// database specific, there is only going to be one database. It is more rare for there to be
// database specific streams, but it does happen sometimes

// We really want to be able to support multiple DB's with non-global change streams, so the
// migration will be like this:
// - Blank slate is fine, just go ahead
// - If the state is populated, it MUST have a token and all resources done backfilling; error out if not.
// -   Or does it? Have to think about a migration in the middle of a backfill. Mainly this would catch up the entire stream.
// - If a global change stream can be created, the token must be for the global stream.
// - If a global change stream can't be created, the token must be for the one and only database configured for the task.

// TODO: Check errors after TryNext etc. on change stream

// should probably re-init streams based on prior checkpoint, or make really sure that the cursors
// track state correctly...just make new ones, since they could also possibly timeout

// info logging
// Read concern for client; we should probably use "majority"

type changeStreams struct {
	streams                []changeStream
	collectionBindingIndex map[string]int
}

type changeStream struct {
	cs       *mongo.ChangeStream
	database string
	isGlobal bool
}

type docKey struct {
	Id interface{} `bson:"_id"`
}
type namespace struct {
	Database   string `bson:"db"`
	Collection string `bson:"coll"`
}
type changeEvent struct {
	DocumentKey   docKey              `bson:"documentKey"`
	OperationType string              `bson:"operationType"`
	FullDocument  bson.M              `bson:"fullDocument"`
	Ns            namespace           `bson:"ns"`
	ClusterTime   primitive.Timestamp `bson:"clusterTime"`
}

type serverHello struct {
	LastWrite     lastWrite           `bson:"lastWrite"`
	ClusterTime   clusterTime         `bson:"$clusterTime"`
	OperationTime primitive.Timestamp `bson:"operationTime"`
}

type lastWrite struct {
	OpTime         helloTs `bson:"opTime"`
	MajorityOpTime helloTs `bson:"majorityOpTime"`
}

type clusterTime struct {
	ClusterTime primitive.Timestamp `bson:"clusterTime"`
}

type helloTs struct {
	Ts primitive.Timestamp `bson:"ts"`
}

var (
	// Use a pipeline to project the fields we need from the change stream. Importantly, this
	// suppresses fields like `updateDescription`, which contain a list of fields in the document
	// that were updated. If a large field is updated, it will appear both in the `fullDocument` and
	// `updateDescription`, which can cause the change stream total BSON document size to exceed the
	// 16MB limit.
	pl = mongo.Pipeline{{{
		Key: "$project",
		Value: bson.M{
			"clusterTime":   1,
			"documentKey":   1,
			"fullDocument":  1,
			"ns":            1,
			"operationType": 1,
		},
	}}}
)

func initializeStreams(
	ctx context.Context,
	client *mongo.Client,
	state captureState,
	bindings []bindingInfo,
) (changeStreams, error) {
	out := changeStreams{
		collectionBindingIndex: make(map[string]int),
	}

	for idx, b := range bindings {
		out.collectionBindingIndex[resourceId(b.resource)] = idx
	}

	// First attempt to do an instance-wide change stream, this is possible if we have
	// readAnyDatabase access on the instance. If this fails for permission issues, we then attempt
	// to do a database-level change stream.
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup) // TODO: Required instead?
	if state.GlobalResumeToken != nil {
		opts = opts.SetResumeAfter(state.GlobalResumeToken)
	}

	cs, err := client.Watch(ctx, &pl, opts)
	if err != nil {
		if e, ok := err.(mongo.ServerError); ok {
			if e.HasErrorMessage(unauthorizedMessage) {
				// Can't do an instance-wide change stream, so do one for each database.
				// TODO: Some kind of log here
				for _, b := range bindings {
					var db = b.resource.Database
					opts := options.ChangeStream().SetFullDocument(options.UpdateLookup) // TODO: Required instead?
					if t, ok := state.DatabaseResumeTokens[db]; ok {
						opts = opts.SetResumeAfter(t)
					}

					cs, err := client.Database(db).Watch(ctx, &pl, opts)
					if err != nil {
						return changeStreams{}, fmt.Errorf("change stream on database %q: %w", db, err)
					}

					out.streams = append(out.streams, changeStream{
						cs:       cs,
						database: db,
						isGlobal: false,
					})
				}
			} else if e.HasErrorMessage(resumePointGoneErrorMessage) || e.HasErrorMessage(resumeTokenNotFoundErrorMessage) {
				// Maybe use error code 260 here? https://www.mongodb.com/docs/manual/reference/error-codes/
				return changeStreams{}, fmt.Errorf("change stream cannot resume capture, this is usually due to insufficient oplog storage. Please consider resizing your oplog to be able to safely capture data from your database: https://go.estuary.dev/NurkrE")
			}
		} else {
			return changeStreams{}, fmt.Errorf("global change stream: %w", err)
		}
	} else {
		out.streams = append(out.streams, changeStream{
			cs:       cs,
			database: "",
			isGlobal: true,
		})
	}

	return out, nil
}

// streamForever repeatedly calls `(*ChangeStream).TryNext`, emitting and checkpointing any
// documents that result. `TryNext` is used instead of the forever-blocking `Next` to allow for
// checkpoint of "post batch resume tokens", even if the change stream did emit any change events.
// `TryNext` uses a default time window of 1 second in which events can arrive before it returns.
// Under the hood, `TryNext` issues a "getMore" on the change stream cursor, and this loop provides
// similar behavior as blocking on `Next` which repeatedly issues "getMore" queries as well.
func (c *capture) streamForever(
	ctx context.Context,
	strms changeStreams,
) error {
	group, groupCtx := errgroup.WithContext(ctx)

	for _, s := range strms.streams {
		s := s
		group.Go(func() error {
			for {
				for s.cs.TryNext(groupCtx) {
					var ev changeEvent
					if err := s.cs.Decode(&ev); err != nil {
						return fmt.Errorf("streaming changes decoding document %#v: %w", s, err)
					} else if err := handleChangeEvent(c.output, s, ev, strms.collectionBindingIndex); err != nil {
						return fmt.Errorf("streaming changes handleChangeEvent: %w", err)
					}
				}
				if err := s.cs.Err(); err != nil {
					return fmt.Errorf("change stream for %q (global: %t): %w", s.database, s.isGlobal, err)
				}

				// Checkpoint the final resume token from the change stream, which may correspond to the
				// last event received, but will often also be a "post batch resume token", which does
				// not correspond to an event for this stream, but is rather a resumable token
				// representing the oplog entry the change stream has scanned up to on the server (not
				// necessarily a matching change). This will be present even if TryNext returned no
				// documents and allows us to keep the position of the change stream fresh even if we
				// are watching a database that has infrequent events.
				if checkpointJson, err := makeCheckpoint(s.cs.ResumeToken(), s); err != nil {
					return fmt.Errorf("streamForever makeCheckpoint: %w", err)
					// TODO: There is already a c.outputCheckpoint too
				} else if err := c.output.Checkpoint(checkpointJson, true); err != nil {
					return fmt.Errorf("streamForever output checkpoint: %w", err)
				}
			}
		})
	}

	return group.Wait()
}

// streamCatchup is similar to streamForever, but returns when `TryNext` yields no documents, or
// when we are sufficiently caught up on reading the change stream as determined by comparing the
// cluster time of the received event to the last majority-committed operation on the cluster at the
// start of catching up the stream.
func (c *capture) streamCatchup(
	ctx context.Context,
	strms changeStreams,
	client *mongo.Client,
) error {
	// First get the current majority-committed operation time, which will be our high
	// watermark for catching up the stream. Generally we would reach the "end" of the
	// stream and TryNext will return `false`, but in cases where the stream has a high rate
	// of changes we could end up tailing it forever and need some point to know that we are
	// sufficiently caught up to stop.

	// Note: Although we are using the "admin" database, this command works for any database, even
	// if it doesn't exist, no matter what permissions the connecting user has.
	result := client.Database("admin").RunCommand(ctx, bson.M{"hello": "1"})
	var helloRes serverHello
	if err := result.Decode(&helloRes); err != nil {
		return fmt.Errorf("decoding server 'hello' response: %w", err)
	}
	log.WithField("serverHello", helloRes).Info("catching up streams")

	group, groupCtx := errgroup.WithContext(ctx)

	for _, s := range strms.streams {
		s := s
		group.Go(func() error {
			for s.cs.TryNext(groupCtx) {
				var ev changeEvent
				if err := s.cs.Decode(&ev); err != nil {
					return fmt.Errorf("change stream catchup decoding document %#v: %w", s, err)
				} else if err := handleChangeEvent(c.output, s, ev, strms.collectionBindingIndex); err != nil {
					return fmt.Errorf("change stream catchup handleChangeEvent: %w", err)
				}

				if ev.ClusterTime.After(helloRes.LastWrite.MajorityOpTime.Ts) {
					log.WithFields(log.Fields{
						"database":         s.database,
						"isGlobal":         s.isGlobal,
						"eventClusterTime": ev.ClusterTime,
					}).Info("catch up stream reached cluster OpTime high watermark")
					// Early return without checkpointing a "post batch resume token", since be
					// definition we have received a document that corresponds to a specific resume
					// token.
					return nil
				}
			}
			if err := s.cs.Err(); err != nil {
				return fmt.Errorf("change stream catchup for %q (global: %t): %w", s.database, s.isGlobal, err)
			}

			// Checkpoint the "post batch resume token". See comment in `streamForever`.
			if checkpointJson, err := makeCheckpoint(s.cs.ResumeToken(), s); err != nil {
				return fmt.Errorf("streamCatchup makeCheckpoint: %w", err)
				// TODO: There is already a c.outputCheckpoint too
			} else if err := c.output.Checkpoint(checkpointJson, true); err != nil {
				return fmt.Errorf("streamCatchup output checkpoint: %w", err)
			}

			return nil
		})
	}

	return group.Wait()
}

func handleChangeEvent(
	output *boilerplate.PullOutput,
	s changeStream,
	ev changeEvent,
	collectionBindingIndex map[string]int,
) error {
	checkpointJson, err := makeCheckpoint(s.cs.ResumeToken(), s)
	if err != nil {
		return fmt.Errorf("handleChangeEvent makeCheckpoint: %w", err)
	}

	var doc map[string]any

	bindingIdx, ok := collectionBindingIndex[eventResourceId(ev)]
	if ok && slices.Contains([]string{"insert", "update", "replace", "delete"}, ev.OperationType) {
		// Event is for a tracked binding and an operation we care about
		if ev.OperationType == "delete" {
			doc = map[string]any{
				idProperty: ev.DocumentKey.Id,
				metaProperty: map[string]any{
					opProperty: "d",
				},
			}
		} else if ev.FullDocument == nil {
			// FullDocument can be "null" if another operation has deleted the document. This
			// happens because update change events do not hold a copy of the full document, but
			// rather it is at query time that the full document is looked up, and in these cases,
			// the FullDocument can end up being null (if deleted) or different from the deltas in
			// the update event. We ignore events where fullDocument is null. Another change event
			// of type delete will eventually come and delete the document (if not already).
		} else {
			doc = ev.FullDocument

			var op string
			if ev.OperationType == "update" || ev.OperationType == "replace" {
				op = "u"
			} else if ev.OperationType == "insert" {
				op = "c"
			}
			doc[metaProperty] = map[string]any{opProperty: op}
		}
	}

	if doc != nil {
		doc = sanitizeDocument(doc)

		if js, err := json.Marshal(doc); err != nil {
			return fmt.Errorf("encoding change stream document: %w", err)
		} else if err := output.DocumentsAndCheckpoint(checkpointJson, true, bindingIdx, js); err != nil {
			return fmt.Errorf("output change stream documents and checkpoint: %w", err)
		}
	} else {
		if err := output.Checkpoint(checkpointJson, true); err != nil {
			return fmt.Errorf("output change stream checkpoint only: %w", err)
		}
	}

	return nil
}

func makeCheckpoint(tok bson.Raw, s changeStream) (json.RawMessage, error) {
	if tok == nil {
		return nil, fmt.Errorf("application error: got a nil resume token")
	}

	checkpoint := captureState{}
	if s.isGlobal {
		checkpoint.GlobalResumeToken = tok
	} else {
		checkpoint.DatabaseResumeTokens = map[string]bson.Raw{s.database: tok}
	}

	cj, err := json.Marshal(checkpoint)
	if err != nil {
		return nil, fmt.Errorf("encoding change stream checkpoint: %w", err)
	}

	return cj, err
}

func resourceId(res resource) string {
	return fmt.Sprintf("%s.%s", res.Database, res.Collection)
}

func eventResourceId(ev changeEvent) string {
	return fmt.Sprintf("%s.%s", ev.Ns.Database, ev.Ns.Collection)
}
