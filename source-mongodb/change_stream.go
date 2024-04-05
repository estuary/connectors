package main

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
)

// changeStream associates a *mongo.ChangeStream with metadata about which database it is for and if
// it is a global change stream for checkpointing.
type changeStream struct {
	ms       *mongo.ChangeStream
	database string
	isGlobal bool
}

// makeStreamEvent produces an appropriate streamEvent for a provided changeEvent decoded from this
// changeStream.
func (cs *changeStream) makeStreamEvent(ev *changeEvent, collectionBindingIndex map[string]bindingInfo) (streamEvent, error) {
	tok := cs.ms.ResumeToken()
	if tok == nil {
		// This should never happen.
		return streamEvent{}, fmt.Errorf("application error: got a nil resume token")
	}

	stateUpdate := captureState{}
	if cs.isGlobal {
		stateUpdate.GlobalResumeToken = tok
	} else {
		stateUpdate.DatabaseResumeTokens = map[string]bson.Raw{cs.database: tok}
	}

	out := streamEvent{
		stateUpdate: stateUpdate,
	}

	if ev == nil {
		// Checkpoint only for a post-batch resume token.
		return out, nil
	}

	binding, ok := collectionBindingIndex[eventResourceId(*ev)]
	if ok {
		out.binding = binding

		switch ev.OperationType {
		case "insert", "update", "replace":
			if ev.FullDocument == nil {
				// FullDocument can be "null" for these operations if another operation has deleted
				// the document. This happens because update change events do not hold a copy of the
				// full document, but rather it is at query time that the full document is looked
				// up, and in these cases, the FullDocument can end up being null (if deleted) or
				// different from the deltas in the update event. We ignore events where
				// FullDocument is null. Another change event of type delete will eventually come
				// and delete the document.
				return out, nil
			}

			doc := sanitizeDocument(ev.FullDocument)

			var op string
			if ev.OperationType == "update" || ev.OperationType == "replace" {
				op = "u"
			} else if ev.OperationType == "insert" {
				op = "c"
			}
			doc[metaProperty] = map[string]any{
				opProperty: op,
			}

			out.doc = doc
		case "delete":
			var doc = map[string]any{
				idProperty: ev.DocumentKey.Id,
				metaProperty: map[string]any{
					opProperty: "d",
				},
			}

			out.doc = sanitizeDocument(doc)
		default:
			// Event is of an operation type that we don't care about.
		}
	}

	return out, nil
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

var (
	// Use a pipeline to project the fields we need from the change stream. Importantly, this
	// suppresses fields like `updateDescription`, which contain a list of fields in the document
	// that were updated. If a large field is updated, it will appear both in the `fullDocument` and
	// `updateDescription`, which can cause the change stream total BSON document size to exceed the
	// 16MB limit.
	pl = mongo.Pipeline{
		{{Key: "$project", Value: bson.M{
			"documentKey":   1,
			"operationType": 1,
			"fullDocument":  1,
			"ns":            1,
		}}},
	}
)

func globalStream(
	ctx context.Context,
	client *mongo.Client,
) (bool, error) {
	cs, err := client.Watch(ctx, mongo.Pipeline{})
	if err != nil {
		if e, ok := err.(mongo.ServerError); ok {
			if e.HasErrorMessage("not authorized on admin to execute command") || e.HasErrorMessage("user is not allowed to do action [changeStream] on [admin.]") {
				log.Info("will use database-level streams since user is not authorized for a global stream")
				return false, nil
			}
		}
		return false, fmt.Errorf("checking for global stream permissions: %w", err)
	}
	if err := cs.Close(ctx); err != nil {
		return false, fmt.Errorf("closing global stream test cursor: %w", err)
	}

	log.Info("using global change stream")
	return true, nil
}

// initializeStreams starts change streams for the capture; either a single global stream if
// `global` is true, or a database-level change stream for each unique database. Streams are started
// from any prior resume tokens stored in the capture's state.
func (c *capture) initializeStreams(
	ctx context.Context,
	global bool,
	bindings []bindingInfo,
) ([]changeStream, error) {
	var out []changeStream

	// Initialize a separate set of options for each stream, so that I don't have to figure out if
	// it is OK to re-use the options object for creating multiple streams when changing the resume
	// token after each stream is initialized.
	makeOpts := func() *options.ChangeStreamOptions {
		return options.ChangeStream().SetFullDocument(options.UpdateLookup)
	}

	if global {
		opts := makeOpts()
		if c.state.GlobalResumeToken != nil {
			opts = opts.SetResumeAfter(c.state.GlobalResumeToken)
		}

		cs, err := c.client.Watch(ctx, pl, opts)
		if err != nil {
			return nil, fmt.Errorf("initializing global change stream: %w", err)
		}

		out = append(out, changeStream{
			ms:       cs,
			database: "",
			isGlobal: true,
		})
	} else {
		started := make(map[string]bool)
		for _, b := range bindings {
			var db = b.resource.Database

			if started[db] {
				continue
			}
			started[db] = true

			opts := makeOpts()
			if t, ok := c.state.DatabaseResumeTokens[db]; ok {
				opts = opts.SetResumeAfter(t)
			}

			cs, err := c.client.Database(db).Watch(ctx, &pl, opts)
			if err != nil {
				return nil, fmt.Errorf("initializing change stream on database %q: %w", db, err)
			}

			out = append(out, changeStream{
				ms:       cs,
				database: db,
				isGlobal: false,
			})
		}
	}

	return out, nil
}

// streamForever repeatedly calls `(*ChangeStream).TryNext` via (*capture).tryStream, emitting and
// checkpointing any documents that result. `TryNext` is used instead of the forever-blocking `Next`
// to allow for checkpoint of "post-batch resume tokens", even if the change stream did emit any
// change events. `TryNext` uses a default time window of 1 second in which events can arrive before
// it returns. Under the hood, `TryNext` issues a "getMore" on the change stream cursor, and this
// loop provides similar behavior as blocking on `Next` which repeatedly issues "getMore" queries as
// well. See
// https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.md#why-do-we-need-to-expose-the-postbatchresumetoken
// for more information about post-batch resume tokens.
func (c *capture) streamForever(
	ctx context.Context,
	streams []changeStream,
) error {
	group, groupCtx := errgroup.WithContext(ctx)

	for _, s := range streams {
		s := s
		group.Go(func() error {
			defer s.ms.Close(groupCtx)

			for {
				if err := c.tryStream(groupCtx, s, nil); err != nil {
					return fmt.Errorf("change stream for %q (global: %t): %w", s.database, s.isGlobal, err)
				}
			}
		})
	}

	return group.Wait()
}

// serverHello is used to get information regarding the most recent operation times from the server
// to use as a high watermark for catching up change streams.
type serverHello struct {
	LastWrite lastWrite `bson:"lastWrite"`
}

type lastWrite struct {
	OpTime helloTs `bson:"opTime"`
}

type helloTs struct {
	Ts primitive.Timestamp `bson:"ts"`
}

// streamCatchup is similar to streamForever, but returns when `TryNext` yields no documents, or
// when we are sufficiently caught up on reading the change stream as determined by comparing the
// cluster time of the received event to the last majority-committed operation on the cluster at the
// start of catching up the stream.
func (c *capture) streamCatchup(
	ctx context.Context,
	streams []changeStream,
) error {
	// First get the current majority-committed operation time, which will be our high
	// watermark for catching up the stream. Generally we would reach the "end" of the
	// stream and TryNext will return `false`, but in cases where the stream has a high rate
	// of changes we could end up tailing it forever and need some point to know that we are
	// sufficiently caught up to stop.

	// Note: Although we are using the "admin" database, this command works for any database, even
	// if it doesn't exist, no matter what permissions the connecting user has.
	result := c.client.Database("admin").RunCommand(ctx, bson.M{"hello": "1"})
	var helloRes serverHello
	if err := result.Decode(&helloRes); err != nil {
		return fmt.Errorf("decoding server 'hello' response: %w", err)
	}
	log.WithField("serverHello", helloRes).Info("catching up streams")

	group, groupCtx := errgroup.WithContext(ctx)

	for _, s := range streams {
		s := s
		group.Go(func() error {
			if err := c.tryStream(groupCtx, s, &helloRes.LastWrite.OpTime.Ts); err != nil {
				return fmt.Errorf("catching up stream for %q (global: %t): %w", s.database, s.isGlobal, err)
			} else if err := s.ms.Close(groupCtx); err != nil {
				return fmt.Errorf("catching up stream closing cursor for %q (global: %t): %w", s.database, s.isGlobal, err)
			}

			return nil
		})
	}

	return group.Wait()
}

// tryStream gets events from the changeStream until there aren't anymore, or until an event is
// observed that comes after the optional tsCutoff.
func (c *capture) tryStream(
	ctx context.Context,
	s changeStream,
	tsCutoff *primitive.Timestamp,
) error {
	for s.ms.TryNext(ctx) {
		var ev changeEvent
		if err := s.ms.Decode(&ev); err != nil {
			return fmt.Errorf("change stream decoding document %#v: %w", s, err)
		} else if emit, err := s.makeStreamEvent(&ev, c.collectionBindingIndex); err != nil {
			return fmt.Errorf("change stream making stream event: %w", err)
		} else if err := c.emitEvent(ctx, emit); err != nil {
			return fmt.Errorf("change stream emitting stream event: %w", err)
		}

		if tsCutoff != nil && ev.ClusterTime.After(*tsCutoff) {
			log.WithFields(log.Fields{
				"database":         s.database,
				"isGlobal":         s.isGlobal,
				"eventClusterTime": ev.ClusterTime,
			}).Info("catch up stream reached cluster OpTime high watermark")
			// Early return without a checkpoint for the "post batch resume token", since by
			// definition we have received a document that corresponds to a specific resume
			// token.
			return nil
		}
	}
	if err := s.ms.Err(); err != nil {
		return fmt.Errorf("change stream ended with error: %w", err)
	}

	// Checkpoint the final resume token from the change stream, which may correspond to the last
	// event received, but will often also be a "post batch resume token", which does not correspond
	// to an event for this stream, but is rather a resumable token representing the oplog entry the
	// change stream has scanned up to on the server (not necessarily a matching change). This will
	// be present even if TryNext returned no documents and allows us to keep the position of the
	// change stream fresh even if we are watching a database that has infrequent events.
	if emit, err := s.makeStreamEvent(nil, c.collectionBindingIndex); err != nil {
		return fmt.Errorf("change stream making post-batch stream event: %w", err)
	} else if err := c.emitEvent(ctx, emit); err != nil {
		return fmt.Errorf("changing stream emitting post-batch stream event: %w", err)
	}

	return nil
}

func resourceId(res resource) string {
	return fmt.Sprintf("%s.%s", res.Database, res.Collection)
}

func eventResourceId(ev changeEvent) string {
	return fmt.Sprintf("%s.%s", ev.Ns.Database, ev.Ns.Collection)
}
