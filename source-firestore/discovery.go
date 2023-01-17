package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	firestore "cloud.google.com/go/firestore"
	firebase "firebase.google.com/go"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"
)

const (
	// discoverSubresourceMinimum and discoverSubresourceProbability work together
	// to limit the number of documents which we query for subcollections. The logic
	// here is that if some collection (let's call it "users/*/messages" for example)
	// has a subcollection ("users/*/messages/*/attachments") we don't need to check
	// every single message for every user to learn this fact. Furthermore since every
	// document we check requires a distinct RPC, this could take quite a while to do.
	//
	// So instead we can apply some common-sense policy which mimics what a user might
	// do: check the first N exhaustively and then start skimming through and randomly
	// picking a document to check every so often.
	discoverSubresourceMinimum     = 100
	discoverSubresourceProbability = 0.01

	// discoverMaxDocumentsPerCollection limits the maximum number of documents which
	// will be fetched from any specific collection.
	discoverMaxDocumentsPerCollection = 1000

	// discoverMaxDocumentsPerResource *approximately* limits the maximum number of
	// documents which will be fetched from any specific resource path.
	discoverMaxDocumentsPerResource = 10000

	// discoverConcurrentScanners limits the number of scan worker goroutines which
	// may execute concurrently.
	discoverConcurrentScanners = 16
)

const (
	metaProperty  = "_meta"
	documentPath  = "/_meta/path"
	documentMTime = "/_meta/mtime"
)

type documentMetadata struct {
	Path       string     `json:"path" jsonschema:"title=Document Path,description=Fully qualified document path including Project ID and database name."`
	CreateTime *time.Time `json:"ctime,omitempty" jsonschema:"title=Create Time,description=The time at which the document was created. Unset if the document is deleted."`
	UpdateTime *time.Time `json:"mtime" jsonschema:"title=Update Time,description=The time at which the document was most recently updated (or deleted)."`
	Deleted    bool       `json:"delete,omitempty" jsonschema:"title=Delete Flag,description=True if the document has been deleted, unset otherwise."`
}

// minimalSchema is the maximally-permissive schema which just specifies the
// metadata our connector adds. The schema of collections is minimalSchema as we
// rely on Flow's schema inference to infer the collection schema
var minimalSchema = generateMinimalSchema()

func generateMinimalSchema() json.RawMessage {
	// Generate schema for the metadata via reflection
	var reflector = jsonschema.Reflector{
		ExpandedStruct: true,
		DoNotReference: true,
	}
	var metadataSchema = reflector.ReflectFromType(reflect.TypeOf(documentMetadata{}))
	metadataSchema.Definitions = nil
	metadataSchema.AdditionalProperties = nil

	// Wrap metadata into an enclosing object schema with a /_meta property
	// and a 'maximize by timestamp' reduction strategy.
	var schema = &jsonschema.Schema{
		Type:                 "object",
		Required:             []string{metaProperty},
		AdditionalProperties: nil,
		Extras: map[string]interface{}{
			"reduce": map[string]interface{}{
				"strategy": "maximize",
				"key":      []string{documentMTime},
			},
			"properties": map[string]*jsonschema.Schema{
				metaProperty: metadataSchema,
			},
		},
	}

	// Marshal schema to JSON
	bs, err := json.Marshal(schema)
	if err != nil {
		panic(fmt.Errorf("error generating schema: %v", err))
	}
	return json.RawMessage(bs)
}

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
	log.WithField("collections", len(bindings)).Info("discovery complete")
	return &pc.DiscoverResponse{
		Bindings: bindings,
	}, nil
}

type discoveryState struct {
	client        *firestore.Client
	scanners      *errgroup.Group // Contains all scanner goroutines launched during discovery.
	scanSemaphore chan struct{}   // Semaphore channel used to limit number of concurrent scanners

	// Mutex-guarded state which may be modified from within worker goroutines.
	shared struct {
		sync.Mutex
		counts   map[resourcePath]int                  // Map from resource paths to the number of documents processed.
		bindings []*pc.DiscoverResponse_Binding        // List of output bindings from inference workers which have terminated.
	}
}

func discoverCollections(ctx context.Context, client *firestore.Client) ([]*pc.DiscoverResponse_Binding, error) {
	scanners, ctx := errgroup.WithContext(ctx)
	var state = &discoveryState{
		client:        client,
		scanners:      scanners,
		scanSemaphore: make(chan struct{}, discoverConcurrentScanners),
	}
	state.shared.counts = make(map[resourcePath]int)

	// Request processing of all top-level collections.
	var collections, err = client.Collections(ctx).GetAll()
	if err != nil {
		return nil, err
	}
	for _, coll := range collections {
		state.handleCollection(ctx, coll)
	}

	// Wait for all scanners to terminate
	if err := state.scanners.Wait(); err != nil {
		return nil, err
	}

	// Just to be extra nice, sort the bindings list by recommended name.
	// Since prefixes sort before their longer versions this approximates
	// a nice hierarchical interpretation.
	var bindings = state.shared.bindings
	sort.Slice(bindings, func(i, j int) bool {
		return bindings[i].RecommendedName < bindings[j].RecommendedName
	})
	return bindings, nil
}

// discoverCollection iterates over every document of a collection, and recursively checks some of the documents
// for subcollections.
func (ds *discoveryState) discoverCollection(ctx context.Context, coll *firestore.CollectionRef) error {
	var resourcePath = collectionToResourcePath(coll.Path)
	var collectionPath = trimDatabasePath(coll.Path)
	var logEntry = log.WithFields(log.Fields{
		"collection": collectionPath,
		"resource":   resourcePath,
	})

	// Acquire one of the scanner semaphores before doing any further work
	ds.scanSemaphore <- struct{}{}
	defer func() { <-ds.scanSemaphore }()

	// Terminate immediately if the maximum number of documents has already been reached
	ds.shared.Lock()
	if total := ds.shared.counts[resourcePath]; total >= discoverMaxDocumentsPerResource {
		ds.shared.Unlock()
		return nil
	}
	ds.shared.Unlock()

	var numDocuments int
	logEntry.Debug("scanning collection")
	defer func() {
		logEntry.WithField("count", numDocuments).Debug("done scanning collection")
	}()

	var docs, err = coll.Limit(discoverMaxDocumentsPerCollection).Documents(ctx).GetAll()
	if err != nil {
		// Retryable error statuses generally mean that something went wrong over the network
		// or internally to Firestore. We shouldn't take down the entire discovery run as this
		// might only be an issue with the one collection query and it's best to provide the
		// user with whatever degraded results we can.
		if retryableStatus(err) {
			logEntry.WithField("err", err).Warn("error scanning collection")
			return nil
		}
		return fmt.Errorf("error fetching documents from collection %q: %w", collectionPath, err)
	}
	for _, doc := range docs {
		if err := ds.handleDocument(ctx, doc); err != nil {
			return err
		}
		numDocuments++
	}

  resourceJSON, err := json.Marshal(resource{
          Path:         resourcePath,
          BackfillMode: backfillModeAsync,
  })
  if err != nil {
          return fmt.Errorf("error serializing resource json: %w", err)
  }
  var binding = &pc.DiscoverResponse_Binding{
          RecommendedName:    pf.Collection(collectionRecommendedName(resourcePath)),
          ResourceSpecJson:   resourceJSON,
          DocumentSchemaJson: minimalSchema,
          KeyPtrs:            []string{documentPath},
  }
  ds.shared.Lock()
  ds.shared.bindings = append(ds.shared.bindings, binding)
  ds.shared.Unlock()

	return nil
}

func (ds *discoveryState) handleDocument(ctx context.Context, doc *firestore.DocumentSnapshot) error {
	// Map the document path to a resource path like 'users/*/messages' and then send
	// it to the appropriate schema inference worker.
	var resourcePath = collectionToResourcePath(doc.Ref.Parent.Path)
	log.WithFields(log.Fields{"path": doc.Ref.Path, "resource": resourcePath}).Trace("process document")

	// Increment the document count for this resource path
	ds.shared.Lock()
	var count = ds.shared.counts[resourcePath] + 1
	ds.shared.counts[resourcePath] = count
	ds.shared.Unlock()

	// Possibly launch an asynchronous worker to check the document for subcollections,
	// according to an "Always check the first K and then randomly check P% of the rest"
	// heuristic.
	if count < discoverSubresourceMinimum || rand.Float64() < discoverSubresourceProbability {
		ds.scanners.Go(func() error {
			subcolls, err := doc.Ref.Collections(ctx).GetAll()
			if err != nil {
				log.WithFields(log.Fields{
					"doc": doc.Ref.Path,
					"err": err,
				}).Error("error listing subcollections")
				return fmt.Errorf("error listing subcollections: %w", err)
			}
			for _, subcoll := range subcolls {
				ds.handleCollection(ctx, subcoll)
			}
			return nil
		})
	}
	return nil
}

func (ds *discoveryState) handleCollection(ctx context.Context, coll *firestore.CollectionRef) {
	ds.scanners.Go(func() error {
		var err = ds.discoverCollection(ctx, coll)
		if err != nil {
			log.WithFields(log.Fields{
				"collection": trimDatabasePath(coll.Path),
				"error":      err,
			}).Error("error scanning collection")
		}

		return err
	})
}

// The collection name must not have slashes in it, otherwise we end up with parent
// collections being a prefix of the child collections, which is prohibited by Flow.
func collectionRecommendedName(name resourcePath) string {
	return strings.ReplaceAll(strings.ReplaceAll(name, "/*/", "_"), "/", "_")
}
