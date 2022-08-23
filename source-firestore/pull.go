package main

import (
	firestore "cloud.google.com/go/firestore"
	"context"
	"encoding/json"
	"errors"
	firebase "firebase.google.com/go"
	"fmt"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"io"
	"time"
)

type capture struct {
	Stream   pc.Driver_PullServer
	Bindings []resource
	Config   config
	Acks     <-chan *pc.Acknowledge

	// The channel where documents will be sent to from different streams
	// this is here to ensure we write documents sequentially and avoid scrambled outputs
	Output chan emitRequest

	Tail bool
}

type EmitterRequestType int8

const (
	EncodeDocument EmitterRequestType = 0
)

// struct sent over channel to emitter
type emitRequest struct {
	requestType EmitterRequestType
	arena       []byte
	binding     uint32
	state       []byte
}

func (driver) Pull(stream pc.Driver_PullServer) error {
	log.Debug("connector started")

	var open, err = stream.Recv()
	if err != nil {
		return fmt.Errorf("error reading PullRequest: %w", err)
	} else if open.Open == nil {
		return fmt.Errorf("expected PullRequest.Open, got %#v", open)
	}

	var cfg config
	if err := pf.UnmarshalStrict(open.Open.Capture.EndpointSpecJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	}

	var resources []resource
	for _, binding := range open.Open.Capture.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}
		resources = append(resources, res)
	}

	var capture = &capture{
		Stream:   stream,
		Bindings: resources,
		Config:   cfg,
		Output:   make(chan emitRequest),
		Tail:     open.Open.Tail,
	}
	return capture.Run()
}

func (c *capture) Run() error {
	// Spawn a goroutine which will translate gRPC PullRequest.Acknowledge messages
	// into equivalent messages on a channel. Since the ServerStream interface doesn't
	// have any sort of polling receive, this is necessary to allow captures to handle
	// acknowledgements without blocking their own capture progress.
	var acks = make(chan *pc.Acknowledge)
	var eg, ctx = errgroup.WithContext(c.Stream.Context())
	eg.Go(func() error {
		defer close(acks)
		for {
			var msg, err = c.Stream.Recv()
			if errors.Is(err, io.EOF) {
				return nil
			} else if err != nil {
				return fmt.Errorf("error reading PullRequest: %w", err)
			} else if msg.Acknowledge == nil {
				return fmt.Errorf("expected PullRequest.Acknowledge, got %#v", msg)
			}
			select {
			case <-ctx.Done():
				return nil
			case acks <- msg.Acknowledge:
				// Loop and receive another acknowledgement
			}
		}
	})
	c.Acks = acks

	// Notify Flow that we're starting.
	log.Debug("sending PullResponse.Opened")
	if err := c.Stream.Send(&pc.PullResponse{Opened: &pc.PullResponse_Opened{}}); err != nil {
		return fmt.Errorf("error sending PullResponse.Opened: %w", err)
	}

	eg.Go(func() error {
		return c.Capture(ctx)
	})
	return eg.Wait()
}

func (c *capture) Capture(ctx context.Context) error {
	var scanInterval, _ = time.ParseDuration(defaultScanInterval)
	if c.Config.ScanInterval != "" && c.Config.ScanInterval != scanIntervalNever {
		var err error
		scanInterval, err = time.ParseDuration(c.Config.ScanInterval)

		if err != nil {
			return fmt.Errorf("parsing scan interval failed: %w", err)
		}
	}

	// Listening on snapshots gives you a copy of all documents initially
	// so in essence it is a full scan. Hence we consider the current moment to be the last scan, and then start from there
	var lastScan = time.Now()

	sa := option.WithCredentialsJSON([]byte(c.Config.CredentialsJSON))
	app, err := firebase.NewApp(ctx, nil, sa)
	if err != nil {
		return err
	}

	client, err := app.Firestore(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	var eg = new(errgroup.Group)

	for _, binding := range c.Bindings {
		var groupID = getLastCollectionGroupID(binding.Path)
		var query firestore.Query

		if isRootCollection(binding.Path) {
			query = client.Collection(binding.Path).Query
		} else {
			query = client.CollectionGroup(getLastCollectionGroupID(binding.Path)).Query
		}

		if c.Config.ScanInterval != scanIntervalNever {
			log.WithField("group", groupID).Debug("full scan")
			eg.Go(func() error {
				return c.FullScan(ctx, lastScan, scanInterval, query)
			})
		}

		if c.Tail {
			log.WithField("group", groupID).Debug("listening for changes")
			eg.Go(func() error {
				return c.ListenForChanges(ctx, query)
			})
		}
	}

	eg.Go(func() error {
		return c.EmitDocuments()
	})

	if err := eg.Wait(); err != nil {
		return err
	}

	close(c.Output)

	return nil
}

// TODO: listen on all changes on all documents
func (c *capture) ListenForChanges(ctx context.Context, query firestore.Query) error {
	var snapshotsIterator = query.Snapshots(ctx)

	for {
		var snapshot, err = snapshotsIterator.Next()

		if err == iterator.Done {
			break
		} else if err != nil {
			return err
		}

		for _, change := range snapshot.Changes {
			if change.Kind == firestore.DocumentAdded || change.Kind == firestore.DocumentModified {

				// Make sure this document matches the binding path
				var index = findBindingForCollectionPath(change.Doc.Ref.Parent.Path, c.Bindings)
				log.WithFields(log.Fields{
					"kind":    change.Kind,
					"path":    change.Doc.Ref.Path,
					"p":       collectionToResourcePath(change.Doc.Ref.Parent.Path),
					"binding": index,
				}).Debug("received change")
				if index == -1 {
					continue
				}

				var doc = change.Doc
				var data = doc.Data()
				data[firestore.DocumentID] = doc.Ref.ID
				data[pathField] = doc.Ref.Path
				docJson, err := json.Marshal(data)
				if err != nil {
					return err
				}
				log.WithFields(log.Fields{
					"doc": string(docJson),
					"id":  change.Doc.Ref.ID,
				}).Debug("received change doc")
				c.Output <- emitRequest{
					requestType: EncodeDocument,
					binding:     uint32(index),
					arena:       docJson,
				}
			}
		}
	}

	return nil
}

func (c *capture) FullScan(ctx context.Context, lastScan time.Time, scanInterval time.Duration, query firestore.Query) error {
	var nextScan = time.Until(lastScan.Add(scanInterval))
	// Wait until next scan
	time.Sleep(nextScan)

	var docsIterator = query.Documents(ctx)

	var scanTime = time.Now()
	for {
		var doc, err = docsIterator.Next()

		if err == iterator.Done {
			break
		} else if err != nil {
			return err
		}

		var index = findBindingForCollectionPath(doc.Ref.Parent.Path, c.Bindings)
		if index == -1 {
			continue
		}

		var data = doc.Data()
		data[firestore.DocumentID] = doc.Ref.ID
		data[pathField] = doc.Ref.Path
		docJson, err := json.Marshal(data)
		if err != nil {
			return err
		}
		c.Output <- emitRequest{
			requestType: EncodeDocument,
			binding:     uint32(index),
			arena:       docJson,
		}
	}

	// If we are not tailing, we close the channel after the full scan
	// to allow the encoding routine to finish so the connector can exit
	if !c.Tail {
		return nil
	}

	// We start the next full scan using tail recursion
	return c.FullScan(ctx, scanTime, scanInterval, query)
}

func (c *capture) EmitDocuments() error {
	for {
		emitDoc := <-c.Output

		if emitDoc.requestType == EncodeDocument {
			var msg = &pc.PullResponse{
				Documents: &pc.Documents{
					Binding:  emitDoc.binding,
					Arena:    emitDoc.arena,
					DocsJson: []pf.Slice{{Begin: 0, End: uint32(len(emitDoc.arena))}},
				},
			}
			if err := c.Stream.Send(msg); err != nil {
				return err
			}
			if err := c.Stream.Send(&pc.PullResponse{
				Checkpoint: &pf.DriverCheckpoint{
					DriverCheckpointJson: []byte("{}"),
					Rfc7396MergePatch:    true,
				},
			}); err != nil {
				return err
			}
		} else {
			panic(fmt.Sprintf("unknown EmitterRequestType %v", emitDoc.requestType))
		}
	}
}
