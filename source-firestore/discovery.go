package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	firestore "cloud.google.com/go/firestore"
	firebase "firebase.google.com/go"
	"github.com/estuary/connectors/schema_inference"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	// discoverMaxCollectionsPerResource bounds the number of distinct collections
	// that an invocation of discoverResource will process. If more collections are
	// discovered which map to a particular resource, the excess will first pile up
	// in the buffered channel and then after that they will be silently discarded.
	discoverMaxCollectionsPerResource = 10

	// discoverMaxDocumentsPerCollection bounds the number of documents which will
	// be fetched by discoverResource from any one collection.
	discoverMaxDocumentsPerCollection = 50

	// discoverMaxConcurrentWorkers bounds the number of concurrent `discoverResource()`
	// workers which can proceed at any given time.
	discoverMaxConcurrentWorkers = 32
)

// placeholderSchema is the maximally-permissive schema that should be satisfied
// by any Firestore document. It is returned if schema inference fails.
const placeholderSchema = `{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["__name__", "__path__"],
    "properties": {
        "__name__": { "type": "string" },
        "__path__": { "type": "string" }
    }
}`

// Discover RPC
func (driver) Discover(ctx context.Context, req *pc.DiscoverRequest) (*pc.DiscoverResponse, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.EndpointSpecJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	sa := option.WithCredentialsJSON([]byte(cfg.CredentialsJSON))
	app, err := firebase.NewApp(ctx, nil, sa)
	if err != nil {
		return nil, err
	}

	client, err := app.Firestore(ctx)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	bindings, err := discoverCollections(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("discovery error: %w", err)
	}
	return &pc.DiscoverResponse{
		Bindings: bindings,
	}, nil
}

type discoveryState struct {
	// group contains all worker goroutines launched during discovery.
	group *errgroup.Group

	// workerLimit controls the number of concurrent discoverResource
	// workers which may be active at any given time.
	workerLimit chan struct{}

	// resources contains mutex-guarded state which may be modified from
	// within the numerous discovery worker goroutines.
	resources struct {
		sync.Mutex
		collections map[string]chan *firestore.CollectionRef
		bindings    []*pc.DiscoverResponse_Binding
	}
}

func discoverCollections(ctx context.Context, client *firestore.Client) ([]*pc.DiscoverResponse_Binding, error) {
	eg, ctx := errgroup.WithContext(ctx)
	var state = &discoveryState{
		group:       eg,
		workerLimit: make(chan struct{}, discoverMaxConcurrentWorkers),
	}
	state.resources.collections = make(map[string]chan *firestore.CollectionRef)

	// Request processing of all top-level collections.
	var collections = client.Collections(ctx)
	for {
		var collection, err = collections.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return nil, err
		}
		var resourcePath = state.addCollection(ctx, collection)
		state.closeResource(resourcePath)
	}

	// Wait for all workers to terminate, and if they all did so without
	// error then return the bindings list they put together.
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	// Just to be extra nice, sort the bindings list by recommended name.
	// Since prefixes sort before their longer versions this approximates
	// a nice hierarchical interpretation.
	var bindings = state.resources.bindings
	sort.Slice(bindings, func(i, j int) bool {
		return bindings[i].RecommendedName < bindings[j].RecommendedName
	})
	return bindings, nil
}

// addCollection is called for every new collection during the course of discovery.
// It maps the new collection to the appropriate resource path (which is the collection
// path, except with '*' in the place of each document ID portion) and then sends the
// CollectionRef over a buffered channel to the goroutine tasked with handling that
// resource.
//
// If there is not currently a channel (and goroutine) dedicated to this resource path,
// a new one will be allocated and added to the map.
func (ds *discoveryState) addCollection(ctx context.Context, collection *firestore.CollectionRef) string {
	var resourcePath = collectionToResourcePath(collection.Path)
	log.WithFields(log.Fields{
		"fullpath": collection.Path,
		"resource": resourcePath,
	}).Trace("discovered collection of resource")

	ds.resources.Lock()
	defer ds.resources.Unlock()

	// If this is the first collection we've observed which maps to this particular
	// resource path, initialize process state and spawn a new discovery worker.
	// Otherwise just reuse the already-started process.
	collectionsCh, ok := ds.resources.collections[resourcePath]
	if !ok {
		log.WithField("resource", resourcePath).Info("discovered resource")
		collectionsCh = make(chan *firestore.CollectionRef, discoverMaxCollectionsPerResource)
		ds.resources.collections[resourcePath] = collectionsCh
		ds.group.Go(func() error { return ds.discoverResource(ctx, resourcePath, collectionsCh) })
	}

	// Send the new collection to the process that's already handling this
	// resource if possible. If not possible (the channel buffer is full)
	// then we ignore this particular collection on the theory that we've
	// already have plenty of other examples to work with.
	select {
	case collectionsCh <- collection:
		log.WithField("resource", resourcePath).Trace("queued subcollection")
	default:
		log.WithField("resource", resourcePath).Trace("buffer full, ignoring subcollection")
	}
	return resourcePath
}

// closeResource is used to indicate that no further collections for a particular
// resource path will be forthcoming.
func (ds *discoveryState) closeResource(resourcePath string) {
	log.WithFields(log.Fields{"resource": resourcePath}).Info("close resource")

	ds.resources.Lock()
	defer ds.resources.Unlock()
	if collectionsCh, ok := ds.resources.collections[resourcePath]; ok {
		close(collectionsCh)
	}
}

// discoverResource iterates over each* document of each* collection which maps
// to a particular resource path. In the process each document is fed into schema
// inference and also checked for sub-collections, which may in turn be queued up for
// another discoverResource worker thread (possibly freshly spawned) to process.
//
// A single invocation of discoverResource will handle the scanning of every collection
// with that particular resource path, and so when it terminates we can be certain that
// no further sub-resources of that path will be found. This allows everything to shut
// down cleanly when done.
//
// *subject to limits
func (ds *discoveryState) discoverResource(ctx context.Context, resourcePath string, collectionsCh <-chan *firestore.CollectionRef) error {
	var collectionsCount, documentsCount int

	// Only discoverMaxConcurrentWorkers instances of discoverResource may be
	// active at any given time.
	ds.workerLimit <- struct{}{}
	defer func() { <-ds.workerLimit }()

	// Launch a separate goroutine which will run schema inference while the
	// main thread of execution in this function feeds it documents. When the
	// discoverResource() invocation returns the documents channel will be
	// closed, whereupon schema inference will finish, and the resulting
	// schema will be added to a new binding for the discovery response.
	var docsCh = make(chan json.RawMessage)
	defer close(docsCh)
	ds.group.Go(func() error {
		var logEntry = log.WithField("resource", resourcePath)
		var schema, err = schema_inference.Run(ctx, logEntry, docsCh)
		if err != nil {
			// Ideally schema inference shouldn't ever fail, but in the event that
			// it does we should just use a maximally permissive Firestore document
			// placeholder and keep going.
			logEntry.WithField("err", err).Warn("schema inference failed")
			schema = json.RawMessage(placeholderSchema)
		}
		resourceJSON, err := json.Marshal(resource{Path: resourcePath})
		if err != nil {
			return fmt.Errorf("error serializing resource json: %w", err)
		}
		var binding = &pc.DiscoverResponse_Binding{
			RecommendedName:    pf.Collection(collectionRecommendedName(resourcePath)),
			ResourceSpecJson:   resourceJSON,
			DocumentSchemaJson: schema,
			KeyPtrs:            []string{"/" + firestore.DocumentID},
		}
		ds.resources.Lock()
		ds.resources.bindings = append(ds.resources.bindings, binding)
		ds.resources.Unlock()
		return nil
	})

	// Keep track of any resources nested under this one. After we're all done
	// processing this resource the input channels to the sub-resource workers
	// will be closed so that they, in turn, can shut down.
	var subResources = make(map[string]struct{})
	defer func() {
		for subResource := range subResources {
			ds.closeResource(subResource)
		}
	}()

	for collection := range collectionsCh {
		collectionsCount++
		if collectionsCount > discoverMaxCollectionsPerResource {
			break
		} else if err := ctx.Err(); err != nil {
			return err
		}

		log.WithFields(log.Fields{
			"path":     collection.Path,
			"resource": resourcePath,
		}).Debug("scanning collection")

		var docs = collection.Query.Limit(discoverMaxDocumentsPerCollection).Documents(ctx)
		for {
			if err := ctx.Err(); err != nil {
				return err
			}

			var doc, err = docs.Next()
			if err == iterator.Done {
				break
			} else if err != nil {
				return err
			}

			var data = doc.Data()
			data[firestore.DocumentID] = doc.Ref.ID
			data[pathField] = doc.Ref.Path
			docJSON, err := json.Marshal(data)
			if err != nil {
				return fmt.Errorf("error marshalling document: %w", err)
			}
			docsCh <- docJSON
			documentsCount++

			var subcollections = doc.Ref.Collections(ctx)
			for {
				var subcollection, err = subcollections.Next()
				if err == iterator.Done {
					break
				} else if err != nil {
					return err
				}
				var resourcePath = ds.addCollection(ctx, subcollection)
				subResources[resourcePath] = struct{}{}
			}
		}
	}

	log.WithFields(log.Fields{
		"resource":    resourcePath,
		"documents":   documentsCount,
		"collections": collectionsCount,
	}).Info("finished scanning resource")
	return nil
}

// The collection name must not have slashes in it, otherwise we end up with parent
// collections being a prefix of the child collections, which is prohibited by Flow.
func collectionRecommendedName(name string) string {
	return strings.ReplaceAll(strings.ReplaceAll(name, "/*/", "_"), "/", "_")
}
