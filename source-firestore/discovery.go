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
	"github.com/estuary/connectors/schema_inference"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
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
// metadata our connector adds. If schema inference succeeds the discovered
// schema is allOf(minimalSchema, inferredSchema) and if inference fails then
// the discovered schema defaults to minimalSchema.
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
	client  *firestore.Client
	workers *errgroup.Group // Contains all worker goroutines launched during discovery.

	// Mutex-guarded state which may be modified from within worker goroutines.
	shared struct {
		sync.Mutex
		groups   map[collectionGroupID]struct{}        // Set tracking which 'Collection Groups' have already been seen.
		channels map[resourcePath]chan json.RawMessage // Map from resource paths to inference worker input channels.
		counts   map[resourcePath]int                  // Map from resource paths to the number of documents processed.
		bindings []*pc.DiscoverResponse_Binding        // List of output bindings from inference workers which have terminated.
	}
}

func discoverCollections(ctx context.Context, client *firestore.Client) ([]*pc.DiscoverResponse_Binding, error) {
	eg, ctx := errgroup.WithContext(ctx)
	var state = &discoveryState{
		client:  client,
		workers: eg,
	}
	state.shared.groups = make(map[collectionGroupID]struct{})
	state.shared.channels = make(map[resourcePath]chan json.RawMessage)
	state.shared.counts = make(map[resourcePath]int)

	// Request processing of all top-level collections.
	var collections, err = client.Collections(ctx).GetAll()
	if err != nil {
		return nil, err
	}
	for _, coll := range collections {
		state.processCollection(ctx, coll)
	}

	// Wait for all workers to terminate, and if they all did so without
	// error then return the bindings list they put together.
	if err := eg.Wait(); err != nil {
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

func (ds *discoveryState) processCollection(ctx context.Context, coll *firestore.CollectionRef) {
	ds.shared.Lock()
	defer ds.shared.Unlock()
	if _, ok := ds.shared.groups[coll.ID]; !ok {
		log.WithField("group", coll.ID).Debug("discovered new collection group")
		ds.shared.groups[coll.ID] = struct{}{}
		ds.workers.Go(func() error {
			var err = ds.discoverCollectionGroup(ctx, coll.ID)
			if err != nil {
				log.WithFields(log.Fields{
					"group": coll.ID,
					"error": err,
				}).Error("error scanning documents")
			}
			return err
		})
	}
}

// discoverCollectionGroup iterates over every document of a "collection group" [1],
// feeds each document into the appropriate inference process, and recursively checks
// each one for subcollections.
//
// [1] A collection group is basically the set of all documents which share the same
// penultimate path element. So for instance 'users/<userid>/events/<eventid>' and
// 'groups/<groupid>/events/<eventid>' are in the 'events' collection group. Querying
// by groups rather than individual collections massively improves discovery speed.
func (ds *discoveryState) discoverCollectionGroup(ctx context.Context, group string) error {
	var logEntry = log.WithField("group", group)
	logEntry.Debug("scanning collection group")

	defer func() {
		logEntry.Debug("finished scanning group")
		ds.closeInferenceGroup(group)
	}()

	var retries int
	var docs = ds.client.CollectionGroup(group).Documents(ctx)
	defer docs.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var doc, err = docs.Next()
		if err == iterator.Done {
			break
		} else if err != nil && retryableStatus(err) {
			// Exponential backoff: 100ms, 200ms, 400ms, 800ms, 1.6s, ..., 25.6s plus [0,100ms) jitter
			retries++
			if retryLimit := 9; retries >= retryLimit {
				return fmt.Errorf("error fetching documents from group %q: retry limit (%d) reached: %w", group, retryLimit, err)
			}
			time.Sleep(time.Duration((1<<retries)*50+rand.Intn(100)) * time.Millisecond)
			continue
		} else if err != nil {
			return fmt.Errorf("error fetching documents from group %q: %w", group, err)
		}
		retries = 0
		if err := ds.processDocument(ctx, doc); err != nil {
			return err
		}
	}
	return nil
}

func (ds *discoveryState) processDocument(ctx context.Context, doc *firestore.DocumentSnapshot) error {
	// Marshal the document to JSON that we'll send to schema inference
	var docJSON, err = json.Marshal(doc.Data())
	if err != nil {
		return fmt.Errorf("error marshalling document: %w", err)
	}

	// Map the document path to a resource path like 'users/*/messages' and then send
	// it to the appropriate schema inference worker.
	var resourcePath = collectionToResourcePath(doc.Ref.Parent.Path)
	log.WithFields(log.Fields{"path": doc.Ref.Path, "resource": resourcePath}).Trace("process document")
	var docsCh = ds.inferenceChannel(ctx, resourcePath)
	docsCh <- docJSON

	// Increment the document count for this resource path
	ds.shared.Lock()
	var count = ds.shared.counts[resourcePath] + 1
	ds.shared.counts[resourcePath] = count
	ds.shared.Unlock()

	// Possibly launch an asynchronous worker to check the document for subcollections,
	// according to an "Always check the first K and then randomly check P% of the rest"
	// heuristic.
	if count < discoverSubresourceMinimum || rand.Float64() < discoverSubresourceProbability {
		ds.workers.Go(func() error {
			subcolls, err := doc.Ref.Collections(ctx).GetAll()
			if err != nil {
				log.WithFields(log.Fields{
					"doc": doc.Ref.Path,
					"err": err,
				}).Error("error listing subcollections")
				return fmt.Errorf("error listing subcollections: %w", err)
			}
			for _, subcoll := range subcolls {
				ds.processCollection(ctx, subcoll)
			}
			return nil
		})
	}
	return nil
}

func (ds *discoveryState) inferenceChannel(ctx context.Context, resourcePath resourcePath) chan json.RawMessage {
	ds.shared.Lock()
	defer ds.shared.Unlock()
	if ch, ok := ds.shared.channels[resourcePath]; ok {
		return ch
	}

	var ch = make(chan json.RawMessage)
	ds.shared.channels[resourcePath] = ch
	ds.workers.Go(func() error {
		var err = ds.inferenceWorker(ctx, resourcePath, ch)
		if err != nil {
			log.WithFields(log.Fields{
				"resource": resourcePath,
				"error":    err,
			}).Error("inference error")
		}
		return err
	})
	return ch
}

func (ds *discoveryState) inferenceWorker(ctx context.Context, resourcePath resourcePath, docsCh chan json.RawMessage) error {
	var logEntry = log.WithField("resource", resourcePath)
	var inferredSchema, err = schema_inference.Run(ctx, logEntry, docsCh)
	var documentSchema json.RawMessage
	if err != nil {
		// Ideally schema inference shouldn't ever fail, but in the event that
		// it does we should just use a maximally permissive Firestore document
		// placeholder and keep going.
		logEntry.WithField("err", err).Warn("schema inference failed")
		documentSchema = minimalSchema
	} else {
		// In the happy path when schema inference succeeds, we want to combine
		// the inferred document schema with the minimal "just /_meta" schema to
		// produce a schema which accurately describes the capture output.
		combinedSchema, err := json.Marshal(map[string]interface{}{
			"allOf": []interface{}{inferredSchema, minimalSchema},
		})
		if err != nil {
			return fmt.Errorf("error serializing combined schema: %w", err)
		}
		documentSchema = combinedSchema
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
		DocumentSchemaJson: documentSchema,
		KeyPtrs:            []string{documentPath},
	}

	ds.shared.Lock()
	ds.shared.bindings = append(ds.shared.bindings, binding)
	ds.shared.Unlock()
	return nil
}

func (ds *discoveryState) closeInferenceGroup(group string) {
	ds.shared.Lock()
	defer ds.shared.Unlock()
	for resourcePath, ch := range ds.shared.channels {
		if getLastCollectionGroupID(resourcePath) == group {
			close(ch)
		}
	}
}

// The collection name must not have slashes in it, otherwise we end up with parent
// collections being a prefix of the child collections, which is prohibited by Flow.
func collectionRecommendedName(name resourcePath) string {
	return strings.ReplaceAll(strings.ReplaceAll(name, "/*/", "_"), "/", "_")
}
