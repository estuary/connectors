package main

// This file contains temporary code used to migrate connectors from v1 to v2. We should remove all
// of this when all the tasks we care about have migrated.

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// decodeState is mostly a pile of gnarly code to deal with migrating from `oldCaptureState` to
// `captureState`. It does a partial unmarshalling of the provided `stateJson` to determine if the
// checkpoint is from a prior structure, and arranges pre-existing data into the new checkpoint
// structure, if applicable. A non-nil global resume token is returned if it was present on the
// input state, and should be handled separately by populateDatabaseResumeTokens. We should get rid
// of this when all existing tasks have migrated to the new checkpoints.
func decodeState(
	globalStream bool,
	stateJson json.RawMessage,
	bindings []bindingInfo,
) (captureState, *bson.Raw, error) {
	if stateJson == nil || reflect.DeepEqual(stateJson, json.RawMessage("{}")) {
		log.WithField("reason", "persisted state is empty").Debug("skipping v2 state migration")
		return captureState{}, nil, nil
	}

	// Is this an old form of the capture state?
	var initialState map[string]json.RawMessage
	if err := json.Unmarshal(stateJson, &initialState); err != nil {
		return captureState{}, nil, fmt.Errorf("initial decode of stateJson: %w", err)
	}

	tokJson, ok := initialState["stream_resume_token"]
	if !ok {
		log.WithFields(log.Fields{
			"reason":       "no persisted stream_resume_token",
			"initialState": initialState,
		}).Debug("skipping v2 state migration")

		// This is a new state checkpoint which does not have to be migrated.
		var out captureState
		if err := json.Unmarshal(stateJson, &out); err != nil {
			return captureState{}, nil, fmt.Errorf("unmarshal stateJson: %w", err)
		}

		return out, nil, nil
	}

	var resources map[boilerplate.StateKey]resourceState
	if res, ok := initialState["bindingStateV1"]; ok {
		if err := json.Unmarshal(res, &resources); err != nil {
			return captureState{}, nil, fmt.Errorf("unmarshal res into resources: %w", err)
		}
	}

	// Return a re-initialized state if none of the previously persisted bindings have active
	// stateKeys anymore. This provides an escape hatch for re-backfilling the entire capture if the
	// migration doesn't work for some reason.
	hasActiveResource := false
	for sk := range resources {
		if slices.ContainsFunc(bindings, func(b bindingInfo) bool {
			return b.stateKey == sk
		}) {
			hasActiveResource = true
		}
	}
	if !hasActiveResource {
		log.WithField("reason", "all bindings have been re-backfilled").Info("skipping v2 state migration")
		return captureState{}, nil, nil
	}

	log.WithField("globalStream", globalStream).Info("performing v2 state migration")

	// This is an old state that must be migrated.
	out := captureState{
		Resources:            resources,
		DatabaseResumeTokens: make(map[string]bson.Raw),
	}

	// We're not supporting migrating in-progress backfills. Those should be allowed to complete on
	// the old version of the connector before switching over to this one.
	for stateKey, state := range out.Resources {
		if !state.Backfill.Done {
			return captureState{}, nil, fmt.Errorf("application error: migrating state when backfill is not done for %q", stateKey)
		}
	}

	var tok bson.Raw
	if err := json.Unmarshal(tokJson, &tok); err != nil {
		return captureState{}, nil, fmt.Errorf("unmarshal tokJson into tok: %w", err)
	}

	if !globalStream {
		// This token is for a database-level stream. Previous versions of the connector only
		// allowed for a database-level stream if there was a single database, so it must be for
		// that one.
		var db string
		for _, b := range bindings {
			if db != "" && b.resource.Database != db {
				// Sanity check.
				return captureState{}, nil, fmt.Errorf("found multiple databases with a database-level change stream")
			}
			db = b.resource.Database
		}

		log.WithFields(log.Fields{
			"db":          db,
			"resumeToken": tok,
		}).Info("completed v2 state migration from a prior database-level stream")

		out.DatabaseResumeTokens[db] = tok

		return out, nil, nil
	}

	// Returning a non-nil token means there is a global resume token that needs to be resolved into
	// database-level tokens.
	return out, &tok, nil
}

// populateDatabaseResumeTokens uses a global resume token for a change stream and populates the
// capture state's database-specific change stream state from that. It may emit documents, although
// it will not emit any checkpoints. The caller should emit a full state checkpoint after this
// function returns to persist the documents and updated checkpoint.
func (c *capture) populateDatabaseResumeTokens(
	ctx context.Context,
	globalToken bson.Raw,
) error {
	// Make a set of the database names we need to populate.
	dbs := make(map[string]struct{})
	for _, b := range c.resourceBindingInfo {
		db := b.resource.Database
		dbs[db] = struct{}{}

		if _, ok := c.state.DatabaseResumeTokens[db]; ok {
			// Sanity check; this should never happen.
			return fmt.Errorf("application error: populateDatabaseResumeTokens had state already populated for database %q", db)
		}
	}

	log.WithFields(log.Fields{
		"databases":   dbs,
		"globalToken": globalToken,
	}).Info("populating database resume tokens")

	databasePollResults := make(map[string]changeEvent)

	// Poll the global stream. If there's documents available we only need one.
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup).SetResumeAfter(globalToken).SetBatchSize(1)
	cs, err := c.client.Watch(ctx, pl, opts)
	if err != nil {
		return fmt.Errorf("opening global change stream: %w", err)
	}
	defer cs.Close(ctx)

	gotEvent := false
	var ev changeEvent
	var newToken bson.Raw

	for {
		if cs.TryNext(ctx) {
			gotEvent = true
			if err := cs.Decode(&ev); err != nil {
				return fmt.Errorf("change stream decoding document: %w", err)
			}

			log.WithFields(log.Fields{
				"token":       cs.ResumeToken(),
				"id":          ev.DocumentKey.Id,
				"ns":          ev.Ns,
				"clusterTime": ev.ClusterTime,
			}).Info("v2 state migration got document from global change stream")
		}
		if err := cs.Err(); err != nil {
			return fmt.Errorf("global change stream error: %w", err)
		}

		newToken = cs.ResumeToken()

		// In rare circumstances we will get the exact same token back as a post-batch resume token,
		// if the provided global token is very recent. This is probably only really possible in
		// contrived tests, but we'll handle it anyway by re-polling until we either get an event
		// from the change stream or a different post-batch resume token.
		if newToken.String() != globalToken.String() {
			break
		}

		if gotEvent {
			return fmt.Errorf("application error: got an event but resume token didn't change")
		}

		log.WithFields(log.Fields{
			"globalToken": globalToken,
			"newToken":    newToken,
		}).Info("v2 state migration will re-poll since resume token didn't change")
	}

	if !gotEvent {
		// Easy case: Use the post-batch resume token for all database streams. We didn't get any
		// new events from the global stream, so this token will work for all databases on a
		// go-forward basis.

		log.WithFields(log.Fields{
			"token": newToken,
		}).Info("completed v2 state migration with post-batch resume token")

		for _, b := range c.resourceBindingInfo {
			c.state.DatabaseResumeTokens[b.resource.Database] = newToken
		}
		return nil
	}

	// Is the event for a database we care about? If so we can use its resume token directly for
	// that database.
	if _, ok := dbs[ev.Ns.Database]; ok {
		c.state.DatabaseResumeTokens[ev.Ns.Database] = newToken
		databasePollResults[ev.Ns.Database] = ev
	}

	// For the rest of the databases we are tracking, use the operation time (AKA cluster time) of
	// the received event to initialize the new database-specific change streams.
	startTime := &ev.ClusterTime

	for db := range dbs {
		if _, ok := databasePollResults[db]; ok {
			// Already populated from the global event. Don't poll this database-level stream again
			// to avoid duplicating its document at startTime.
			log.WithField("db", db).Info("v2 state migration database resume token already populated")
			continue
		}

		opts := options.ChangeStream().SetFullDocument(options.UpdateLookup).SetStartAtOperationTime(startTime).SetBatchSize(1)
		cs, err := c.client.Database(db).Watch(ctx, pl, opts)
		if err != nil {
			return fmt.Errorf("initializing change stream on database %q: %w", db, err)
		}
		defer cs.Close(ctx)

		if cs.TryNext(ctx) {
			var ev changeEvent
			if err := cs.Decode(&ev); err != nil {
				return fmt.Errorf("change stream decoding document: %w", err)
			}

			// For some reason, when resuming a change stream with an operation time, the "database"
			// field of the namespace is prefixed by a UUID on MongoDB Atlas. Fortunately we already
			// know what the database is so we can use that instead.
			if ev.Ns.Database != db {
				log.WithFields(log.Fields{
					"eventNamespaceDatabase": ev.Ns.Database,
					"db":                     db,
				}).Info("overriding event namespace database")
				ev.Ns.Database = db
			}

			log.WithFields(log.Fields{
				"db":          db,
				"token":       cs.ResumeToken(),
				"id":          ev.DocumentKey.Id,
				"ns":          ev.Ns,
				"clusterTime": ev.ClusterTime,
			}).Info("v2 state migration got document from database change stream")

			databasePollResults[db] = ev
		}
		if err := cs.Err(); err != nil {
			return fmt.Errorf("change stream error for database %q: %w", db, err)
		}

		c.state.DatabaseResumeTokens[db] = cs.ResumeToken()
	}

	// Emit any of the documents that resulted if they are for monitored collections.
	for _, ev := range databasePollResults {
		doc, bindingIdx := makeEventDocument(ev, c.resourceBindingInfo)
		if doc != nil {
			if docJson, err := json.Marshal(doc); err != nil {
				return fmt.Errorf("serializing stream document: %w", err)
			} else if err := c.output.Documents(bindingIdx, docJson); err != nil {
				return fmt.Errorf("outputting stream document: %w", err)
			}

			log.WithFields(log.Fields{
				"id": ev.DocumentKey.Id,
				"ns": ev.Ns,
			}).Info("v2 state migration emitted document from change stream")
		}
	}

	// Sanity check that we have a non-nil resume token for every database now.
	for _, b := range c.resourceBindingInfo {
		tok := c.state.DatabaseResumeTokens[b.resource.Database]
		if tok == nil {
			return fmt.Errorf("application error: database resume token for db %q was nil after populating tokens", b.resource.Database)
		}
	}

	log.WithFields(log.Fields{
		"databaseResumeTokens": c.state.DatabaseResumeTokens,
	}).Info("completed v2 state migration from a prior global stream")

	return nil
}

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

	return true, nil
}
