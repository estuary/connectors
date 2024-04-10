package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	firestore "cloud.google.com/go/firestore"
	firestore_v1 "cloud.google.com/go/firestore/apiv1"
	firebase "firebase.google.com/go"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"google.golang.org/api/option"
	"google.golang.org/api/transport"
	firestore_pb "google.golang.org/genproto/googleapis/firestore/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var firebaseScopes = []string{
	"https://www.googleapis.com/auth/datastore",
}

const watchTargetID = 1245678

// Non-permanent failures like Unavailable or ResourceExhausted can be retried
// after a little while.
const retryInterval = 60 * time.Second

// Log progress messages after every N documents on a particular stream
const progressLogInterval = 10000

// A new backfill of a collection may only occur after at least this much time has
// elapsed since the previous backfill was started.
//
// TODO(wgd): Consider making this user-configurable?
const backfillRestartDelay = 6 * time.Hour

const (
	backfillChunkSize   = 256
	concurrentBackfills = 2
)

func (driver) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	log.Debug("connector started")

	var cfg config
	if err := pf.UnmarshalStrict(open.Capture.ConfigJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	}

	var prevState captureState
	if open.StateJson != nil {
		if err := pf.UnmarshalStrict(open.StateJson, &prevState); err != nil {
			return fmt.Errorf("parsing state checkpoint: %w", err)
		}
	}

	if err := migrateState(&prevState, open.Capture.Bindings); err != nil {
		return fmt.Errorf("migrating previous state: %w", err)
	}

	updatedResourceStates, err := initResourceStates(prevState.Resources, open.Capture.Bindings)
	if err != nil {
		return fmt.Errorf("error initializing resource states: %w", err)
	}

	// Build a mapping of document paths to state keys, to allow efficient lookups of state keys
	// from the path of retrieved documents.
	stateKeys := make(map[string]boilerplate.StateKey)
	for sk, res := range updatedResourceStates {
		stateKeys[res.path] = sk
	}

	var capture = &capture{
		Config: cfg,
		State: &captureState{
			Resources: updatedResourceStates,
			stateKeys: stateKeys,
		},
		Output: stream,

		backfillSemaphore: semaphore.NewWeighted(concurrentBackfills),
		streamsInCatchup:  new(sync.WaitGroup),
	}
	return capture.Run(stream.Context())
}

type capture struct {
	Config config
	State  *captureState
	Output *boilerplate.PullOutput

	backfillSemaphore *semaphore.Weighted
	streamsInCatchup  *sync.WaitGroup
}

type captureState struct {
	sync.RWMutex
	Resources    map[boilerplate.StateKey]*resourceState `json:"bindingStateV1,omitempty"`
	OldResources map[string]*resourceState               `json:"Resources,omitempty"` // TODO(whb): Remove once all captures have migrated.
	stateKeys    map[string]boilerplate.StateKey         // Allow for lookups of the stateKey from a document path.
}

func migrateState(state *captureState, bindings []*pf.CaptureSpec_Binding) error {
	if state.Resources != nil && state.OldResources != nil {
		return fmt.Errorf("application error: both Resources and OldResources were non-nil")
	} else if state.Resources != nil {
		log.Info("skipping state migration since it's already done")
		return nil
	}

	state.Resources = make(map[boilerplate.StateKey]*resourceState)

	for _, b := range bindings {
		if b.StateKey == "" {
			return fmt.Errorf("state key was empty for binding %s", b.ResourcePath)
		}

		var res resource
		if err := pf.UnmarshalStrict(b.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}

		ll := log.WithFields(log.Fields{
			"stateKey": b.StateKey,
			"path":     res.Path,
		})

		stateFromOld, ok := state.OldResources[res.Path]
		if !ok {
			// This may happen if the connector has never emitted any checkpoints with data for this
			// binding.
			ll.Warn("no state found for binding while migrating state")
			continue
		}

		state.Resources[boilerplate.StateKey(b.StateKey)] = stateFromOld
		ll.Info("migrated binding state")
	}

	state.OldResources = nil

	return nil
}

type resourceState struct {
	ReadTime time.Time
	Backfill *backfillState

	// The 'Inconsistent' flag is set when catchup failure forces the connector
	// to "skip ahead" to the latest changes for some collection(s), and indicates
	// that at some point in the future a new backfill of that collection(s) needs
	// to be performed to re-establish consistency.
	Inconsistent bool `json:"Inconsistent,omitempty"`

	bindingIndex int
	path         string
}

type backfillState struct {
	// The time after which a backfill of this resource may begin processing
	// documents. Used for rate-limiting of retry attempts.
	StartAfter time.Time

	// True if the backfill is completed.
	Completed bool

	Cursor string    // The last document backfilled
	MTime  time.Time // The UpdateTime of that document when we backfilled it
}

func (s *backfillState) Equal(x *backfillState) bool {
	if s == nil {
		return x == nil
	} else if x == nil {
		return false
	} else {
		return s.Cursor == x.Cursor && s.MTime.Equal(x.MTime)
	}
}

func (s *backfillState) String() string {
	return fmt.Sprintf("%s at %s", s.Cursor, s.MTime)
}

// Given the prior resource states from the last DriverCheckpoint along with
// the current capture bindings, compute a new set of resource states.
func initResourceStates(prevStates map[boilerplate.StateKey]*resourceState, bindings []*pf.CaptureSpec_Binding) (map[boilerplate.StateKey]*resourceState, error) {
	var now = time.Now()
	var states = make(map[boilerplate.StateKey]*resourceState)
	for idx, binding := range bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}
		var stateKey = boilerplate.StateKey(binding.StateKey)

		var state = &resourceState{
			bindingIndex: idx,
			path:         res.Path,
		}
		if prevState, ok := prevStates[stateKey]; ok && !prevState.Inconsistent {
			state.ReadTime = prevState.ReadTime
			state.Backfill = prevState.Backfill
		} else if ok && prevState.Inconsistent {
			// Since we entered an inconsistent state previously, we know that some amount of
			// change data has been skipped. To get back into a consistent state, we will have
			// to restart from the current moment (to maximize our changes of staying caught
			// up going forward) and start a new backfill of the entire collection.
			var prevStartTime time.Time
			if prevState.Backfill != nil {
				prevStartTime = prevState.Backfill.StartAfter
			}
			var startTime = prevStartTime.Add(backfillRestartDelay)
			if time.Now().After(startTime) {
				startTime = time.Now()
			}
			state.ReadTime = now
			if res.BackfillMode == backfillModeNone {
				state.Backfill = nil
			} else {
				state.Backfill = &backfillState{StartAfter: startTime}
			}
		} else {
			switch res.BackfillMode {
			case backfillModeNone:
				state.ReadTime = now
				state.Backfill = nil
			case backfillModeAsync:
				state.ReadTime = now
				state.Backfill = &backfillState{StartAfter: time.Now()}
			case backfillModeSync:
				state.ReadTime = time.Time{}
				state.Backfill = nil
			default:
				return nil, fmt.Errorf("invalid backfill mode %q for %q", res.BackfillMode, res.Path)
			}
			if res.InitTimestamp != "" {
				if ts, err := time.Parse(time.RFC3339Nano, res.InitTimestamp); err != nil {
					return nil, fmt.Errorf("invalid initTimestamp value %q: %w", res.InitTimestamp, err)
				} else {
					state.ReadTime = ts
				}
			}
		}
		states[stateKey] = state
	}
	return states, nil
}

func (s *captureState) Validate() error {
	return nil
}

func (s *captureState) BindingIndex(resourcePath string) (int, bool) {
	s.RLock()
	defer s.RUnlock()
	if sk, ok := s.stateKeys[resourcePath]; ok {
		if state := s.Resources[sk]; state != nil {
			return state.bindingIndex, true
		}
	}
	// Return MaxInt just to be extra clear that we're not capturing this resource
	return math.MaxInt, false
}

func (s *captureState) ReadTime(resourcePath string) (time.Time, bool) {
	s.RLock()
	defer s.RUnlock()
	if sk, ok := s.stateKeys[resourcePath]; ok {
		if state := s.Resources[sk]; state != nil {
			return state.ReadTime, true
		}
	}
	return time.Time{}, false
}

func (s *captureState) BackfillingAsync(rpath resourcePath) bool {
	s.RLock()
	defer s.RUnlock()
	if sk, ok := s.stateKeys[rpath]; ok {
		if state := s.Resources[sk]; state != nil {
			return state.Backfill != nil
		}
	}
	return false
}

func (s *captureState) UpdateReadTimes(collectionID string, readTime time.Time) (json.RawMessage, error) {
	s.Lock()
	var updated = make(map[boilerplate.StateKey]*resourceState)
	for stateKey, resourceState := range s.Resources {
		if getLastCollectionGroupID(resourceState.path) == collectionID {
			resourceState.ReadTime = readTime
			updated[stateKey] = resourceState
		}
	}
	s.Unlock()

	var checkpointJSON, err = json.Marshal(&captureState{Resources: updated})
	if err != nil {
		return nil, fmt.Errorf("error serializing state checkpoint: %w", err)
	}
	return checkpointJSON, nil
}

func (s *captureState) UpdateBackfillState(collectionID string, state *backfillState) (json.RawMessage, error) {
	s.Lock()
	var updated = make(map[boilerplate.StateKey]*resourceState)
	for stateKey, resourceState := range s.Resources {
		if getLastCollectionGroupID(resourceState.path) == collectionID && resourceState.Backfill != nil {
			resourceState.Backfill = state
			updated[stateKey] = resourceState
		}
	}
	s.Unlock()

	var checkpointJSON, err = json.Marshal(&captureState{Resources: updated})
	if err != nil {
		return nil, fmt.Errorf("error serializing state checkpoint: %w", err)
	}
	return checkpointJSON, nil
}

func (s *captureState) MarkInconsistent(collectionID string) (json.RawMessage, error) {
	s.Lock()
	var updated = make(map[boilerplate.StateKey]*resourceState)
	for stateKey, resourceState := range s.Resources {
		if getLastCollectionGroupID(resourceState.path) == collectionID {
			resourceState.Inconsistent = true
			updated[stateKey] = resourceState
		}
	}
	s.Unlock()

	var checkpointJSON, err = json.Marshal(&captureState{Resources: updated})
	if err != nil {
		return nil, fmt.Errorf("error serializing state checkpoint: %w", err)
	}
	return checkpointJSON, nil
}

func (c *capture) Run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)

	// Enumerate the sets of watch streams and async backfills we'll need to perform.
	// In both cases we map resource paths to collection IDs, because that's how the
	// underlying API works (so for instance 'users/*/messages' and 'groups/*/messages'
	// are both 'messages').
	var watchCollections = make(map[collectionGroupID]time.Time)
	var backfillCollections = make(map[collectionGroupID]*backfillState)
	for _, resourceState := range c.State.Resources {
		var collectionID = getLastCollectionGroupID(resourceState.path)
		if startTime, ok := watchCollections[collectionID]; !ok || resourceState.ReadTime.Before(startTime) {
			watchCollections[collectionID] = resourceState.ReadTime
		}
		if resourceState.Backfill == nil || resourceState.Backfill.Completed {
			// Do nothing when no backfill is required
			continue
		}
		log.WithFields(log.Fields{
			"resource":   resourceState.path,
			"collection": collectionID,
			"startAfter": resourceState.Backfill.StartAfter,
			"cursor":     resourceState.Backfill.Cursor,
		}).Debug("backfill required for binding")
		if resumeState, ok := backfillCollections[collectionID]; !ok {
			backfillCollections[collectionID] = resourceState.Backfill
		} else if !resumeState.Equal(resourceState.Backfill) {
			log.WithFields(log.Fields{
				"resource":   resourceState.path,
				"collection": collectionID,
			}).Warn("backfill state mismatch, restarting all impacted collections")

			resumeState.Cursor = ""
			resumeState.MTime = time.Time{}
			if resumeState.StartAfter.After(resourceState.Backfill.StartAfter) {
				// Take the minimum StartAfter time across all collections so that we begin
				// backfilling the new one as soon as possible.
				resumeState.StartAfter = resourceState.Backfill.StartAfter
			}
		}
	}

	// Connect to Firestore gRPC API
	var credsOpt = option.WithCredentialsJSON([]byte(c.Config.CredentialsJSON))
	var scopesOpt = option.WithScopes(firebaseScopes...)
	rpcClient, err := firestore_v1.NewClient(ctx, credsOpt, scopesOpt)
	if err != nil {
		return err
	}
	defer rpcClient.Close()

	// If we're going to perform any async backfills, connect to Firestore via the client library too
	var libraryClient *firestore.Client
	if len(backfillCollections) > 0 {
		log.WithField("backfills", len(backfillCollections)).Debug("opening second firestore client for async backfills")
		app, err := firebase.NewApp(ctx, nil, credsOpt)
		if err != nil {
			return err
		}
		libraryClient, err = app.Firestore(ctx)
		if err != nil {
			return err
		}
		defer libraryClient.Close()
	}

	// If the 'database' config property is unspecified, try to autodetect it from
	// the provided credentials.
	if c.Config.DatabasePath == "" {
		var creds, _ = transport.Creds(ctx, credsOpt)
		if creds == nil || creds.ProjectID == "" {
			return fmt.Errorf("unable to determine project ID (set 'database' config property)")
		}
		c.Config.DatabasePath = fmt.Sprintf("projects/%s/databases/(default)", creds.ProjectID)
		log.WithField("path", c.Config.DatabasePath).Warn("using autodetected database path (set 'database' config property to override)")
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "google-cloud-resource-prefix", c.Config.DatabasePath)

	// Notify Flow that we're starting.
	if err := c.Output.Ready(false); err != nil {
		return err
	}

	// Emit the initial state checkpoint, as this may differ from the previous
	// state when bindings are removed.
	if checkpointJSON, err := json.Marshal(c.State); err != nil {
		return fmt.Errorf("error serializing state checkpoint: %w", err)
	} else if err := c.Output.Checkpoint(checkpointJSON, false); err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"bindings":       len(c.State.Resources),
		"watches":        len(watchCollections),
		"asyncBackfills": len(backfillCollections),
	}).Info("capture starting")
	for collectionID, startTime := range watchCollections {
		var collectionID, startTime = collectionID, startTime // Copy the loop variables for each closure
		log.WithField("collection", collectionID).Debug("starting worker")
		eg.Go(func() error {
			if err := c.StreamChanges(ctx, rpcClient, collectionID, startTime); err != nil {
				return fmt.Errorf("error streaming changes for collection %q: %w", collectionID, err)
			}
			return nil
		})
	}
	for collectionID, resumeState := range backfillCollections {
		var collectionID, resumeState = collectionID, resumeState // Copy loop variables for each closure
		log.WithField("collection", collectionID).Debug("starting backfill worker")
		eg.Go(func() error {
			if err := c.BackfillAsync(ctx, libraryClient, collectionID, resumeState); err != nil {
				return fmt.Errorf("error backfilling collection %q: %w", collectionID, err)
			}
			return nil
		})
	}
	defer log.Info("capture terminating")
	if err := eg.Wait(); err != nil && !errors.Is(err, io.EOF) {
		log.WithField("err", err).Error("capture worker failed")
		return err
	}
	return nil
}

func (c *capture) BackfillAsync(ctx context.Context, client *firestore.Client, collectionID string, resumeState *backfillState) error {
	var logEntry = log.WithFields(log.Fields{"collection": collectionID})

	// This should never happen since we only run BackfillAsync when there's a
	// backfill to perform, but seemed safe enough to check anyway.
	if resumeState == nil || resumeState.Completed {
		logEntry.Warn("internal error: no backfill necessary")
		return nil
	}

	// If the StartAfter time is in the future then we need to wait until it's
	// the appropriate time.
	if dt := time.Until(resumeState.StartAfter); dt > 0 {
		logEntry.WithField("wait", dt.String()).Info("waiting to start backfill")
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(dt):
			// proceed to the rest of the function
		}
	}

	var cursor *firestore.DocumentSnapshot
	if resumeState.Cursor == "" {
		// If the cursor path is empty then we just leave the cursor document pointer nil
		logEntry.Info("starting async backfill")
	} else if resumeDocument, err := client.Doc(resumeState.Cursor).Get(ctx); err != nil {
		// If we fail to fetch the resume document, we clear the relevant backfill cursor and error out.
		// This will cause the backfill to restart from the beginning after the capture gets restarted,
		// and in the meantime it will show up as an error in the UI in case there's a persistent issue.
		resumeState.Cursor = ""
		if checkpointJSON, err := c.State.UpdateBackfillState(collectionID, resumeState); err != nil {
			return err
		} else if err := c.Output.Checkpoint(checkpointJSON, true); err != nil {
			return err
		}
		return fmt.Errorf("restarting backfill %q: error fetching resume document %q", collectionID, resumeState.Cursor)
	} else if !resumeDocument.UpdateTime.Equal(resumeState.MTime) {
		// Just like if the resume document fetch fails, mtime mismatches cause us to error out, so
		// we'll restart from the beginning when the connector gets restarted and in the meantime
		// it'll show up red in the UI.
		resumeState.Cursor = ""
		if checkpointJSON, err := c.State.UpdateBackfillState(collectionID, resumeState); err != nil {
			return err
		} else if err := c.Output.Checkpoint(checkpointJSON, true); err != nil {
			return err
		}
		return fmt.Errorf("restarting backfill %q: resume document %q modified during backfill", collectionID, resumeState.Cursor)
	} else {
		cursor = resumeDocument
	}

	// In order to limit the number of concurrent backfills we're buffering
	// in memory at any moment we use a semaphore. Instead of waiting to
	// acquire the semaphore before each query, we instead acquire it up-
	// front so that we can ensure that any return path from this function
	// will correctly release it. Then before each query we *release and
	// reacquire* the semaphore to give other backfills a chance to make
	// progress.
	if err := c.backfillSemaphore.Acquire(ctx, 1); err != nil {
		return err
	}
	defer c.backfillSemaphore.Release(1)

	var numDocuments int
	for {
		// Give other backfills a chance to acquire the semaphore, then take
		// it back for ourselves.
		c.backfillSemaphore.Release(1)
		if err := c.backfillSemaphore.Acquire(ctx, 1); err != nil {
			return err
		}

		// Block any further backfill work so long as any StreamChanges workers are
		// not fully caught up. Async backfills are not time-critical -- while it's
		// nice for them to finish as quickly as they can, nothing major will break
		// if a backfill takes a bit longer. Change streaming however *must* always
		// remain fully caught up or Very Bad Things happen.
		c.streamsInCatchup.Wait()

		var query firestore.Query = client.CollectionGroup(collectionID).Query
		if cursor != nil {
			query = query.StartAfter(cursor)
		}
		query = query.Limit(backfillChunkSize)

		var docs, err = query.Documents(ctx).GetAll()
		if err != nil {
			if status.Code(err) == codes.Canceled {
				err = context.Canceled // Undo an awful bit of wrapping which breaks errors.Is()
			}
			return fmt.Errorf("error backfilling %q: chunk query failed after %d documents: %w", collectionID, numDocuments, err)
		}
		logEntry.WithFields(log.Fields{
			"total": numDocuments,
			"chunk": len(docs),
		}).Debug("processing backfill documents")
		if len(docs) == 0 {
			break
		}

		for _, doc := range docs {
			logEntry.WithField("doc", doc.Ref.Path).Trace("got document")

			// We update the cursor before checking whether this document is being
			// backfilled. This does, unfortunately, mean that it's possible for changes
			// to a document which *isn't being captured* could break the ongoing backfill
			// resume behavior, but that's just how Firestore collection group queries
			// work.
			cursor = doc

			// The 'CollectionGroup' query is potentially over-broad, so skip documents
			// which aren't actually part of a resource being backfilled.
			var resourcePath = documentToResourcePath(doc.Ref.Path)
			if !c.State.BackfillingAsync(resourcePath) {
				continue
			}

			// Convert the document into JSON-serializable form
			var fields = doc.Data()
			for key, value := range fields {
				fields[key] = sanitizeValue(value)
			}
			fields[metaProperty] = &documentMetadata{
				Path:       doc.Ref.Path,
				CreateTime: &doc.CreateTime,
				UpdateTime: &doc.UpdateTime,
			}

			if bindingIndex, ok := c.State.BindingIndex(resourcePath); !ok {
				return fmt.Errorf("internal error: no binding index for async backfill of resource %q", resourcePath)
			} else if docJSON, err := json.Marshal(fields); err != nil {
				return fmt.Errorf("error serializing document %q: %w", doc.Ref.Path, err)
			} else if err := c.Output.Documents(bindingIndex, docJSON); err != nil {
				return err
			}
			numDocuments++
		}

		resumeState.Cursor = trimDatabasePath(cursor.Ref.Path)
		resumeState.MTime = cursor.UpdateTime
		logEntry.WithFields(log.Fields{
			"total":  numDocuments,
			"cursor": resumeState.Cursor,
			"mtime":  resumeState.MTime,
		}).Debug("updating backfill cursor")
		if checkpointJSON, err := c.State.UpdateBackfillState(collectionID, resumeState); err != nil {
			return err
		} else if err := c.Output.Checkpoint(checkpointJSON, true); err != nil {
			return err
		}
	}

	logEntry.WithField("docs", numDocuments).Info("backfill complete")
	resumeState.Completed = true
	resumeState.Cursor = ""
	resumeState.MTime = time.Time{}
	if checkpointJSON, err := c.State.UpdateBackfillState(collectionID, resumeState); err != nil {
		return err
	} else if err := c.Output.Checkpoint(checkpointJSON, true); err != nil {
		return err
	}
	return nil
}

func (c *capture) StreamChanges(ctx context.Context, client *firestore_v1.Client, collectionID string, readTime time.Time) error {
	var logEntry = log.WithFields(log.Fields{
		"collection": collectionID,
	})
	logEntry.WithField("readTime", readTime.Format(time.RFC3339)).Info("streaming changes from collection")

	var target = &firestore_pb.Target{
		TargetType: &firestore_pb.Target_Query{
			Query: &firestore_pb.Target_QueryTarget{
				Parent: c.Config.DatabasePath + `/documents`,
				QueryType: &firestore_pb.Target_QueryTarget_StructuredQuery{
					StructuredQuery: &firestore_pb.StructuredQuery{
						From: []*firestore_pb.StructuredQuery_CollectionSelector{{
							CollectionId:   collectionID,
							AllDescendants: true,
						}},
					},
				},
			},
		},
		TargetId: watchTargetID,
	}
	if !readTime.IsZero() {
		target.ResumeType = &firestore_pb.Target_ReadTime{
			ReadTime: timestamppb.New(readTime),
		}
	}
	var req = &firestore_pb.ListenRequest{
		Database: c.Config.DatabasePath,
		TargetChange: &firestore_pb.ListenRequest_AddTarget{
			AddTarget: target,
		},
	}

	var listenClient firestore_pb.Firestore_ListenClient
	var numRestarts, numDocuments int
	var isCurrent, catchupStreaming bool
	for {
		if listenClient == nil {
			var err error
			listenClient, err = client.Listen(ctx)
			if err != nil {
				return fmt.Errorf("error opening Listen RPC client: %w", err)
			} else if err := listenClient.Send(req); err != nil {
				return fmt.Errorf("error sending Listen RPC: %w", err)
			}

			logEntry.WithFields(log.Fields{
				"restarts":             numRestarts,
				"docsSinceLastRestart": numDocuments,
			}).Debug("opened listen stream")

			numRestarts++
			numDocuments = 0
			isCurrent = false
			if !catchupStreaming {
				catchupStreaming = true
				c.streamsInCatchup.Add(1)

				// Ensure that we call Done if there's an early return
				defer func() {
					if catchupStreaming {
						c.streamsInCatchup.Done()
					}
				}()
			}
		}

		resp, err := listenClient.Recv()
		if err == io.EOF {
			logEntry.Debug("listen stream closed, shutting down")
			return fmt.Errorf("listen stream was closed unexpectedly: %w", err)
		} else if status.Code(err) == codes.Canceled {
			logEntry.Debug("context canceled, shutting down")
			return context.Canceled // Undo an awful bit of wrapping which breaks errors.Is()
		} else if retryableStatus(err) {
			logEntry.WithFields(log.Fields{
				"err":  err,
				"docs": numDocuments,
			}).Errorf("retryable failure, will retry in %s", retryInterval)
			if err := listenClient.CloseSend(); err != nil {
				logEntry.WithField("err", err).Warn("error closing listen client")
			}
			time.Sleep(retryInterval)
			listenClient = nil
			continue
		} else if err != nil {
			return fmt.Errorf("error streaming %q changes: %w", collectionID, err)
		}
		logEntry.WithField("resp", resp).Trace("got response")

		switch resp := resp.ResponseType.(type) {
		case *firestore_pb.ListenResponse_TargetChange:
			logEntry.WithField("tc", resp.TargetChange).Trace("TargetChange Event")
			switch tc := resp.TargetChange; tc.TargetChangeType {
			case firestore_pb.TargetChange_NO_CHANGE:
				var ts = tc.ReadTime.AsTime().Format(time.RFC3339Nano)
				if log.IsLevelEnabled(log.TraceLevel) {
					logEntry.WithField("readTime", ts).Trace("TargetChange.NO_CHANGE")
				}
				if len(tc.TargetIds) == 0 && tc.ReadTime != nil && isCurrent {
					logEntry.WithFields(log.Fields{
						"readTime": ts,
						"docs":     numDocuments,
					}).Debug("consistent point reached")
					if catchupStreaming {
						logEntry.WithFields(log.Fields{
							"readTime": ts,
							"docs":     numDocuments,
						}).Info("stream caught up")
						catchupStreaming = false
						c.streamsInCatchup.Done()
					}
					target.ResumeType = &firestore_pb.Target_ReadTime{ReadTime: tc.ReadTime}
					if checkpointJSON, err := c.State.UpdateReadTimes(collectionID, tc.ReadTime.AsTime()); err != nil {
						return err
					} else if err := c.Output.Checkpoint(checkpointJSON, true); err != nil {
						return err
					}
				}
			case firestore_pb.TargetChange_ADD:
				logEntry.WithField("targets", tc.TargetIds).Trace("TargetChange.ADD")
				if len(tc.TargetIds) != 1 || tc.TargetIds[0] != watchTargetID {
					return fmt.Errorf("unexpected target ID %d", tc.TargetIds[0])
				}
			case firestore_pb.TargetChange_REMOVE:
				listenClient = nil
				if catchupStreaming {
					logEntry.WithField("docs", numDocuments).Warn("replication failed to catch up, skipping to latest changes (go.estuary.dev/YRDsKd)")
					if checkpointJSON, err := c.State.MarkInconsistent(collectionID); err != nil {
						return err
					} else if err := c.Output.Checkpoint(checkpointJSON, true); err != nil {
						return err
					}
					time.AfterFunc(backfillRestartDelay+time.Hour, func() {
						logEntry.Fatal("forcing connector restart to establish consistency")
					})
					target.ResumeType = &firestore_pb.Target_ReadTime{ReadTime: timestamppb.New(time.Now())}
				} else if tc.Cause != nil {
					logEntry.WithField("cause", tc.Cause.Message).Warn("unexpected TargetChange.REMOVE")
					time.Sleep(retryInterval)
				} else {
					logEntry.Warn("unexpected TargetChange.REMOVE")
					time.Sleep(retryInterval)
				}
			case firestore_pb.TargetChange_CURRENT:
				if log.IsLevelEnabled(log.TraceLevel) {
					var ts = resp.TargetChange.ReadTime.AsTime().Format(time.RFC3339Nano)
					logEntry.WithField("readTime", ts).Trace("TargetChange.CURRENT")
				}
				isCurrent = true
			default:
				return fmt.Errorf("unhandled TargetChange (%s)", tc)
			}
		case *firestore_pb.ListenResponse_DocumentChange:
			if len(resp.DocumentChange.RemovedTargetIds) != 0 {
				return fmt.Errorf("internal error: removed target IDs %v", resp.DocumentChange.RemovedTargetIds)
			}
			var doc = resp.DocumentChange.Document
			var resourcePath = documentToResourcePath(doc.Name)
			if getLastCollectionGroupID(resourcePath) != collectionID {
				// This should never happen, but is an opportunistic sanity check to ensure
				// that we're receiving documents on the goroutines which requested them. If
				// this fails it likely means that Firestore has changed some details of how
				// the gRPC 'Listen' API works.
				return fmt.Errorf("internal error: recieved document %q on listener for %q", doc.Name, collectionID)
			}
			numDocuments++
			if numDocuments%progressLogInterval == 0 {
				logEntry.WithField("docs", numDocuments).Debug("replication progress")
			}
			if err := c.HandleDocument(ctx, resourcePath, doc); err != nil {
				return err
			}
		case *firestore_pb.ListenResponse_DocumentDelete:
			var doc = resp.DocumentDelete.Document
			var readTime = resp.DocumentDelete.ReadTime.AsTime()
			var resourcePath = documentToResourcePath(doc)
			numDocuments++
			if numDocuments%progressLogInterval == 0 {
				logEntry.WithField("docs", numDocuments).Debug("replication progress")
			}
			if err := c.HandleDelete(ctx, resourcePath, doc, readTime); err != nil {
				return err
			}
		case *firestore_pb.ListenResponse_DocumentRemove:
			var doc = resp.DocumentRemove.Document
			var readTime = resp.DocumentRemove.ReadTime.AsTime()
			var resourcePath = documentToResourcePath(doc)
			numDocuments++
			if numDocuments%progressLogInterval == 0 {
				logEntry.WithField("docs", numDocuments).Debug("replication progress")
			}
			if err := c.HandleDelete(ctx, resourcePath, doc, readTime); err != nil {
				return err
			}
		case *firestore_pb.ListenResponse_Filter:
			logEntry.WithField("filter", resp.Filter).Debug("ListenResponse.Filter")
		default:
			return fmt.Errorf("unhandled ListenResponse: %v", resp)
		}
	}
}

// When any watch stream reaches a consistent point a checkpoint is emitted to
// update the 'Read Time' associated with the impacted bindings. However, the
// first consistent point only occurs after the initial state of the dataset is
// fully synced, and this could potentially be many gigabytes of data.
//
// By emitting empty checkpoints periodically during the capture we unblock Flow
// to persist our capture output instead of buffering everything, at the cost of
// potentially duplicating documents in the event of a connector restart. I think
// this is the best we can do, given the Firestore APIs and the constraint of not
// buffering the entire dataset locally.
const emptyCheckpoint string = `{}`

func (c *capture) HandleDocument(ctx context.Context, resourcePath string, doc *firestore_pb.Document) error {
	// Ignore document changes which occurred prior to the last read time of the collection.
	var ctime = doc.CreateTime.AsTime()            // The time at which this document was first created
	var mtime = doc.UpdateTime.AsTime()            // The time at which this document was last modified
	var rtime, ok = c.State.ReadTime(resourcePath) // The latest read time for this resource path
	if lvl := log.TraceLevel; log.IsLevelEnabled(lvl) {
		log.WithFields(log.Fields{
			"doc":   doc.Name,
			"ctime": ctime.Format(time.RFC3339Nano),
			"mtime": mtime.Format(time.RFC3339Nano),
			"rtime": rtime.Format(time.RFC3339Nano),
			"res":   resourcePath,
		}).Log(lvl, "document change")
	}
	if !ok {
		log.WithField("doc", doc.Name).Trace("ignoring document (resource not captured)")
		return nil
	}
	if delta := mtime.Sub(rtime); delta < 0 {
		log.WithField("doc", doc.Name).Trace("ignoring document (mtime < rtime)")
		return nil
	}

	// Convert the document into a JSON-serializable map of fields
	var fields = make(map[string]interface{})
	for id, val := range doc.Fields {
		var tval, err = translateValue(val)
		if err != nil {
			return fmt.Errorf("error translating value: %w", err)
		}
		fields[id] = tval
	}
	fields[metaProperty] = &documentMetadata{
		Path:       doc.Name,
		CreateTime: &ctime,
		UpdateTime: &mtime,
	}

	if bindingIndex, ok := c.State.BindingIndex(resourcePath); !ok {
		// Listen streams can be a bit over-broad. For instance if there are
		// collections 'users/*/docs' and 'groups/*/docs' in the database, but
		// only 'users/*/docs' is captured, we need to ignore any documents
		// from paths like 'groups/*/docs' which don't map to any binding.
		return nil
	} else if docJSON, err := json.Marshal(fields); err != nil {
		return fmt.Errorf("error serializing document %q: %w", doc.Name, err)
	} else if err := c.Output.Documents(bindingIndex, docJSON); err != nil {
		return err
	} else if err := c.Output.Checkpoint(json.RawMessage(emptyCheckpoint), true); err != nil {
		return err
	}
	return nil
}

func (c *capture) HandleDelete(ctx context.Context, resourcePath string, docName string, readTime time.Time) error {
	if lvl := log.TraceLevel; log.IsLevelEnabled(lvl) {
		log.WithFields(log.Fields{
			"doc":   docName,
			"mtime": readTime.Format(time.RFC3339Nano),
			"res":   resourcePath,
		}).Log(lvl, "document delete")
	}

	var bindingIndex, ok = c.State.BindingIndex(resourcePath)
	if !ok {
		return nil
	}
	var fields = map[string]interface{}{
		metaProperty: &documentMetadata{
			Path:       docName,
			UpdateTime: &readTime,
			Deleted:    true,
		},
	}
	if docJSON, err := json.Marshal(fields); err != nil {
		return fmt.Errorf("error serializing deletion record %q: %w", docName, err)
	} else if err := c.Output.Documents(bindingIndex, docJSON); err != nil {
		return err
	} else if err := c.Output.Checkpoint(json.RawMessage(emptyCheckpoint), true); err != nil {
		return err
	}
	return nil
}

func translateValue(val *firestore_pb.Value) (interface{}, error) {
	switch val := val.ValueType.(type) {
	case *firestore_pb.Value_NullValue:
		return nil, nil
	case *firestore_pb.Value_BooleanValue:
		return val.BooleanValue, nil
	case *firestore_pb.Value_IntegerValue:
		return val.IntegerValue, nil
	case *firestore_pb.Value_DoubleValue:
		if math.IsNaN(val.DoubleValue) {
			return "NaN", nil
		}
		return val.DoubleValue, nil
	case *firestore_pb.Value_TimestampValue:
		return val.TimestampValue.AsTime(), nil
	case *firestore_pb.Value_StringValue:
		return val.StringValue, nil
	case *firestore_pb.Value_BytesValue:
		return val.BytesValue, nil
	case *firestore_pb.Value_ReferenceValue:
		// TODO(wgd): Is it okay/good to flatten the string-vs-reference distinction here?
		// My gut says yes, in general we probably want to just coerce document references
		// into the name/path of that document as a string, but I can see an argument for
		// turning references into some sort of object instead.
		return val.ReferenceValue, nil
	case *firestore_pb.Value_GeoPointValue:
		return val.GeoPointValue, nil
	case *firestore_pb.Value_ArrayValue:
		var xs = make([]interface{}, len(val.ArrayValue.Values))
		for i, v := range val.ArrayValue.Values {
			var x, err = translateValue(v)
			if err != nil {
				return nil, err
			}
			xs[i] = x
		}
		return xs, nil
	case *firestore_pb.Value_MapValue:
		var xs = make(map[string]interface{}, len(val.MapValue.Fields))
		for k, v := range val.MapValue.Fields {
			var x, err = translateValue(v)
			if err != nil {
				return nil, err
			}
			xs[k] = x
		}
		return xs, nil
	}
	return nil, fmt.Errorf("unknown value type %T", val)
}

func sanitizeValue(x interface{}) interface{} {
	switch x := x.(type) {
	case float64:
		if math.IsNaN(x) {
			return "NaN"
		}
	case []interface{}:
		for idx, value := range x {
			x[idx] = sanitizeValue(value)
		}
		return x
	case map[string]interface{}:
		for key, value := range x {
			x[key] = sanitizeValue(value)
		}
		return x
	}
	return x
}
