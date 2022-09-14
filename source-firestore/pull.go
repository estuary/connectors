package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	firestore_v1 "cloud.google.com/go/firestore/apiv1"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"
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

func (driver) Pull(stream pc.Driver_PullServer) error {
	log.Debug("connector started")

	var open, err = stream.Recv()
	if err != nil {
		return fmt.Errorf("error reading PullRequest: %w", err)
	} else if open.Open == nil {
		return fmt.Errorf("expected PullRequest.Open, got %#v", open)
	}

	// TODO(wgd): Move this into source-boilerplate?
	//
	// Translate any other PullRequest RPCs (Acknowledges) into a channel
	// so the capture logic can handle them without blocking reads.
	var pullRequests = make(chan *pc.PullRequest)
	go func(ctx context.Context, ch chan *pc.PullRequest) {
		defer close(ch)
		for {
			var msg, err = stream.Recv()
			if err != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case pullRequests <- msg:
				continue
			}
		}
	}(stream.Context(), pullRequests)

	var cfg config
	if err := pf.UnmarshalStrict(open.Open.Capture.EndpointSpecJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	}

	var prevState captureState
	if open.Open.DriverCheckpointJson != nil {
		if err := pf.UnmarshalStrict(open.Open.DriverCheckpointJson, &prevState); err != nil {
			return fmt.Errorf("parsing state checkpoint: %w", err)
		}
	}

	var resourceBindings []resource
	for _, binding := range open.Open.Capture.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}
		resourceBindings = append(resourceBindings, res)
	}

	var capture = &capture{
		Requests: pullRequests,
		Config:   cfg,
		State: &captureState{
			Resources: initResourceStates(prevState.Resources, resourceBindings),
		},
		Output: &captureOutput{
			Stream: stream,
		},
		Tail: open.Open.Tail,
	}
	return capture.Run(stream.Context())
}

type capture struct {
	Requests chan *pc.PullRequest
	Config   config
	State    *captureState
	Output   *captureOutput
	Tail     bool
}

type captureState struct {
	sync.RWMutex
	Resources map[string]*resourceState
}

type resourceState struct {
	ReadTime     time.Time
	bindingIndex uint32
}

// Given the prior resource states from the last DriverCheckpoint along with
// the current capture bindings, compute a new set of resource states.
func initResourceStates(prevStates map[string]*resourceState, resourceBindings []resource) map[string]*resourceState {
	var states = make(map[string]*resourceState)
	for idx, resource := range resourceBindings {
		var state = &resourceState{bindingIndex: uint32(idx)}
		if prevState, ok := prevStates[resource.Path]; ok {
			state.ReadTime = prevState.ReadTime
		}
		states[resource.Path] = state
	}
	return states
}

func (s *captureState) Validate() error {
	return nil
}

func (s *captureState) UpdateReadTimes(collectionID string, readTime time.Time) (json.RawMessage, error) {
	s.Lock()
	var updated = make(map[string]*resourceState)
	for resourcePath, resourceState := range s.Resources {
		if getLastCollectionGroupID(resourcePath) == collectionID {
			resourceState.ReadTime = readTime
			updated[resourcePath] = resourceState
		}
	}
	s.Unlock()

	var checkpointJSON, err = json.Marshal(&captureState{Resources: updated})
	if err != nil {
		return nil, fmt.Errorf("error serializing state checkpoint: %w", err)
	}
	return checkpointJSON, nil
}

func (s *captureState) BindingIndex(resourcePath string) (uint32, bool) {
	s.RLock()
	defer s.RUnlock()
	if state := s.Resources[resourcePath]; state != nil {
		return state.bindingIndex, true
	}
	return ^uint32(0), false
}

func (s *captureState) ReadTime(resourcePath string) (time.Time, bool) {
	s.RLock()
	defer s.RUnlock()
	if state := s.Resources[resourcePath]; state != nil {
		return state.ReadTime, true
	}
	return time.Time{}, false
}

func (s *captureState) SerializeCheckpoint() (json.RawMessage, error) {
	s.RLock()
	defer s.RUnlock()
	var bs, err = json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("error serializing state checkpoint: %w", err)
	}
	return json.RawMessage(bs), nil
}

// TODO(wgd): Move the whole captureOutput thing into source-boilerplate?
type captureOutput struct {
	sync.Mutex

	Stream pc.Driver_PullServer
}

func (out *captureOutput) Ready() error {
	log.Debug("sending PullResponse.Opened")
	out.Lock()
	defer out.Unlock()
	if err := out.Stream.Send(&pc.PullResponse{Opened: &pc.PullResponse_Opened{}}); err != nil {
		return fmt.Errorf("error sending PullResponse.Opened: %w", err)
	}
	return nil
}

func (out *captureOutput) Documents(binding uint32, docs ...json.RawMessage) error {
	// Concatenate multiple documents into the arena, with appropriate indices
	//
	// TODO(wgd): The Firestore capture actually emits documents one at a time, but
	//   I'm trying to be a bit forward-looking towards making this generic capture
	//   boilerplate code. So there should be some sort of automatic splitting into
	//   multiple Documents messages if we try to emit a ton of messages in one call,
	//   right?
	var arena []byte
	var slices []pf.Slice
	for _, doc := range docs {
		var begin = uint32(len(arena))
		arena = append(arena, []byte(doc)...)
		slices = append(slices, pf.Slice{Begin: begin, End: uint32(len(arena))})
	}

	var msg = &pc.PullResponse{
		Documents: &pc.Documents{
			Binding:  binding,
			Arena:    arena,
			DocsJson: slices,
		},
	}

	out.Lock()
	defer out.Unlock()
	if err := out.Stream.Send(msg); err != nil {
		log.WithField("err", err).Error("stream send error")
		return fmt.Errorf("error emitting documents: %w", err)
	}
	return nil
}

func (out *captureOutput) Checkpoint(checkpoint json.RawMessage, merge bool) error {
	log.WithFields(log.Fields{
		"checkpoint": checkpoint,
		"merge":      merge,
	}).Trace("emitting checkpoint")

	var msg = &pc.PullResponse{
		Checkpoint: &pf.DriverCheckpoint{
			DriverCheckpointJson: []byte(checkpoint),
			Rfc7396MergePatch:    merge,
		},
	}

	out.Lock()
	defer out.Unlock()
	if err := out.Stream.Send(msg); err != nil {
		log.WithField("err", err).Error("stream send error")
		return fmt.Errorf("error emitting checkpoint: %w", err)
	}
	return nil
}

// DO NOT COMMIT : Hacks for testing
var databaseName = "projects/helpful-kingdom-273219/databases/(default)"

func (c *capture) Run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)

	// Discard PullRequest.Acknowledge RPCs. This goroutine can only "leak" until
	// the requests channel is closed, which will happen when the underlying gRPC
	// stream is closed or its context is cancelled.
	go func() {
		for range c.Requests {
		}
	}()

	// Connect to Firestore
	opts := []option.ClientOption{option.WithScopes(firebaseScopes...), option.WithCredentialsJSON([]byte(c.Config.CredentialsJSON))}
	client, err := firestore_v1.NewClient(ctx, opts...)
	if err != nil {
		return err
	}
	defer client.Close()
	ctx = metadata.AppendToOutgoingContext(ctx, "google-cloud-resource-prefix", databaseName)

	// Notify Flow that we're starting.
	if err := c.Output.Ready(); err != nil {
		return err
	}

	// Enumerate unique collection IDs in a map. Multiple distinct resource paths
	// can still have the same collection ID, for example 'foo/*/asdf' and 'bar/*/asdf'
	// both have collection ID 'asdf'.
	c.State.RLock()
	var watchCollections = make(map[string]time.Time)
	for resourcePath, resourceState := range c.State.Resources {
		var collectionID = getLastCollectionGroupID(resourcePath)
		if startTime, ok := watchCollections[collectionID]; !ok || resourceState.ReadTime.Before(startTime) {
			watchCollections[collectionID] = resourceState.ReadTime
		}
	}
	c.State.RUnlock()

	for collectionID, startTime := range watchCollections {
		var collectionID, startTime = collectionID, startTime // Copy the loop variables for each closure
		log.WithField("collection", collectionID).Debug("starting worker")
		eg.Go(func() error {
			return c.Capture(ctx, client, collectionID, startTime)
		})
	}
	defer log.Info("capture terminating")
	if err := eg.Wait(); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func (c *capture) Capture(ctx context.Context, client *firestore_v1.Client, collectionID string, startTime time.Time) error {
	var logEntry = log.WithFields(log.Fields{
		"collection": collectionID,
	})
	logEntry.WithField("startTime", startTime.Format(time.RFC3339)).Debug("capture collection")

	listenClient, err := client.Listen(ctx)
	if err != nil {
		return fmt.Errorf("listen error: %w", err)
	}

	var target = &firestore_pb.Target{
		TargetType: &firestore_pb.Target_Query{
			Query: &firestore_pb.Target_QueryTarget{
				Parent: databaseName + `/documents`,
				QueryType: &firestore_pb.Target_QueryTarget_StructuredQuery{
					StructuredQuery: &firestore_pb.StructuredQuery{
						From: []*firestore_pb.StructuredQuery_CollectionSelector{{
							CollectionId:   collectionID,
							AllDescendants: true,
						}},
						OrderBy: []*firestore_pb.StructuredQuery_Order{{
							Field: &firestore_pb.StructuredQuery_FieldReference{
								FieldPath: "__name__",
							},
						}},
					},
				},
			},
		},
		TargetId: watchTargetID,
		// ResumeType: &pb.Target_ResumeToken{},
	}
	if !startTime.IsZero() {
		target.ResumeType = &firestore_pb.Target_ReadTime{
			ReadTime: timestamppb.New(startTime),
		}
	}
	var req = &firestore_pb.ListenRequest{
		Database: databaseName,
		TargetChange: &firestore_pb.ListenRequest_AddTarget{
			AddTarget: target,
		},
	}
	if err := listenClient.Send(req); err != nil {
		return fmt.Errorf("send error: %w", err)
	}

	var isCurrent = false
	for {
		resp, err := listenClient.Recv()
		if err == io.EOF {
			logEntry.Debug("listen stream closed, shutting down")
			return nil
		} else if status.Code(err) == codes.Canceled {
			logEntry.Debug("context canceled, shutting down")
			return nil
		} else if err != nil {
			// TODO(wgd): Some statuses are retryable: codes.Unknown, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Internal, codes.Unavailable, codes.Unauthenticated
			return fmt.Errorf("recv error: %w", err)
		}
		logEntry.WithField("resp", resp).Trace("got response")

		switch resp := resp.ResponseType.(type) {
		case *firestore_pb.ListenResponse_TargetChange:
			logEntry.WithField("tc", resp.TargetChange).Debug("TargetChange Event")
			switch tc := resp.TargetChange; tc.TargetChangeType {
			case firestore_pb.TargetChange_NO_CHANGE:
				var ts = tc.ReadTime.AsTime().Format(time.RFC3339Nano)
				if log.IsLevelEnabled(log.TraceLevel) {
					logEntry.WithField("readTime", ts).Trace("TargetChange.NO_CHANGE")
				}
				if len(tc.TargetIds) == 0 && tc.ReadTime != nil && isCurrent {
					logEntry.WithField("readTime", ts).Debug("consistent point reached")
					if checkpointJSON, err := c.State.UpdateReadTimes(collectionID, tc.ReadTime.AsTime()); err != nil {
						return err
					} else if err := c.Output.Checkpoint(checkpointJSON, false); err != nil {
						return err
					}
					// In polling mode (tests), shut down the capture once caught up
					if !c.Tail {
						return nil
					}
				}
			case firestore_pb.TargetChange_ADD:
				logEntry.WithField("targets", tc.TargetIds).Trace("TargetChange.ADD")
				if len(tc.TargetIds) != 1 || tc.TargetIds[0] != watchTargetID {
					return fmt.Errorf("unexpected target ID %d", tc.TargetIds[0])
				}
			case firestore_pb.TargetChange_REMOVE:
				if tc.Cause != nil {
					return fmt.Errorf("unexpected TargetChange.REMOVE: %v", tc.Cause.Message)
				}
				return fmt.Errorf("unexpected TargetChange.REMOVE")
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
			if err := c.HandleDocument(ctx, resourcePath, doc); err != nil {
				return err
			}
		case *firestore_pb.ListenResponse_DocumentDelete:
			var doc = resp.DocumentDelete.Document
			var readTime = resp.DocumentDelete.ReadTime.AsTime()
			var resourcePath = documentToResourcePath(doc)
			if err := c.HandleDelete(ctx, resourcePath, doc, readTime); err != nil {
				return err
			}
		case *firestore_pb.ListenResponse_DocumentRemove:
			var doc = resp.DocumentRemove.Document
			var readTime = resp.DocumentRemove.ReadTime.AsTime()
			var resourcePath = documentToResourcePath(doc)
			if err := c.HandleDelete(ctx, resourcePath, doc, readTime); err != nil {
				return err
			}
		case *firestore_pb.ListenResponse_Filter:
			// TODO(wgd): Do we need to do anything with Filter responses?
			// From the Firestore client library code it looks like this is
			// essentially a sanity check, so if we maintained a running count
			// of how many documents we expect to exist, it might be possible
			// to use this to detect data corruption. But it doesn't look like
			// we need to do anything else with it.
			logEntry.WithField("filter", resp.Filter).Debug("ListenResponse.Filter")
		default:
			return fmt.Errorf("unhandled ListenResponse: %v", resp)
		}
	}
}

// TODO(wgd): Can this fudge factor safely be set to zero? In theory if
// we're performing safe updates to rtime we will never see documents with
// mtime < rtime except during connector startups with prior state.
const updateTimeFudgeFactor = 1 * time.Second

// The only "true checkpoints" (ones which meaningfully update capture state)
// are emitted when a consistent point is reached on some watch stream. However
// the first consistent point is only reached after the initial state of the
// dataset is fully synced, and this could potentially be many gigabytes of
// data.
//
// By emptting "empty checkpoints" periodically during the capture we allow
// Flow to persist documents and not have to buffer all that at once, at the
// cost of possibly producing duplicate documents. I believe that this is
// the best we can do.
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
	if delta := mtime.Sub(rtime); delta < -updateTimeFudgeFactor {
		log.WithField("doc", doc.Name).Trace("ignoring document (mtime < rtime)")
		return nil
	} else if delta < 0 {
		log.WithFields(log.Fields{
			"doc":   doc.Name,
			"mtime": mtime.Format(time.RFC3339Nano),
			"rtime": rtime.Format(time.RFC3339Nano),
		}).Warn("document mtime is shortly before the current rtime")
		return fmt.Errorf("causality error: mtime %s < rtime %s", mtime.Format(time.RFC3339Nano), rtime.Format(time.RFC3339Nano))
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
	// TODO(wgd): Should these properties be moved into /_meta or something?
	fields["__path__"] = doc.Name
	fields["__ctime__"] = doc.CreateTime.AsTime()
	fields["__mtime__"] = doc.UpdateTime.AsTime()

	if bindingIndex, ok := c.State.BindingIndex(resourcePath); !ok {
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
	//var fields = map[string]interface{}{
	//	"__path__":    resp.DocumentDelete.Document,
	//	"__dtime__":   resp.DocumentDelete.ReadTime.AsTime(),
	//	"__deleted__": true,
	//}
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
		return val.DoubleValue, nil
	case *firestore_pb.Value_TimestampValue:
		return val.TimestampValue.AsTime(), nil
	case *firestore_pb.Value_StringValue:
		return val.StringValue, nil
	case *firestore_pb.Value_BytesValue:
		return val.BytesValue, nil
	case *firestore_pb.Value_ReferenceValue:
		// TODO(wgd): Emit as a string or as an object?
		return nil, fmt.Errorf("reference values not yet handled")
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
