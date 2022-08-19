package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

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

const DISCOVER_DOC_LIMIT = 50

type inferenceRequest struct {
	document   schema_inference.Document
	collection string
}

type inferenceProcess struct {
	docsCh chan schema_inference.Document
	count  int
	open   bool
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

	var response = &pc.DiscoverResponse{
		Bindings: []*pc.DiscoverResponse_Binding{},
	}
	err = discoverCollections(ctx, client, response)
	if err != nil {
		return nil, err
	}

	return response, nil
}

// The collection name must not have slashes in it, otherwise we end up with parent
// collections being a prefix of the child collections, which is prohibited by flow
func collectionRecommendedName(name string) string {
	return strings.ReplaceAll(strings.ReplaceAll(name, "/*/", "_"), "/", "_")
}

// Start a channel where we receive documents for each collection
// if they match an existing collection (e.g. group/*/subgroup), we send the docs to the
// existing inference channel, otherwise we create a new inference channel for it.
// Once we receive DISCOVER_DOC_LIMIT documents for each collection, we close the channel for inference
// and stop writing to it.
func discoveryRoutine(
	ctx context.Context,
	response *pc.DiscoverResponse,
	eg *errgroup.Group,
	ch chan inferenceRequest,
	inferenceChannels map[string]*inferenceProcess,
) error {
	for request := range ch {
		if process, exists := inferenceChannels[request.collection]; !exists {
			log.WithField("collection", request.collection).Debug("created a new inference channel")
			var docsCh = make(chan schema_inference.Document)

			var logEntry = log.WithField("collection", request.collection)
			inferenceChannels[request.collection] = &inferenceProcess{
				docsCh: docsCh,
				count:  0,
				open:   true,
			}

			eg.Go(func() error {
				var collection = request.collection
				var err error
				var schema schema_inference.Schema
				log.WithField("collection", collection).Debug("schema_inference.Run")
				schema, err = schema_inference.Run(ctx, logEntry, docsCh)
				if err != nil {
					return fmt.Errorf("schema inference: %w", err)
				}

				resourceJSON, err := json.Marshal(resource{Path: collection})
				if err != nil {
					return fmt.Errorf("serializing resource json: %w", err)
				}

				log.WithField("collection", collection).Debug("finished schema inference")
				response.Bindings = append(response.Bindings, &pc.DiscoverResponse_Binding{
					RecommendedName:    pf.Collection(collectionRecommendedName(collection)),
					ResourceSpecJson:   resourceJSON,
					DocumentSchemaJson: schema,
					KeyPtrs:            []string{"/" + firestore.DocumentID},
				})
				return nil
			})

			docsCh <- request.document
		} else {
			if process.open {
				log.WithField("collection", request.collection).WithField("count", process.count).Debug("channel exists, sending document")
				process.docsCh <- request.document
				process.count = process.count + 1
				if process.count > DISCOVER_DOC_LIMIT {
					process.open = false
					close(process.docsCh)
				}
			} else {
				log.WithField("collection", request.collection).Debug("channel is closed")
			}
		}
	}

	return nil
}

// Go through all collections and discover them recursively. Spawn a discoverRoutine for handling management
// of inference channels
func discoverCollections(ctx context.Context, client *firestore.Client, response *pc.DiscoverResponse) error {
	var collections = client.Collections(ctx)
	var ch = make(chan inferenceRequest)
	var firestoreGroup = new(errgroup.Group)
	var routineGroup = new(errgroup.Group)

	var inferenceChannels = map[string]*inferenceProcess{}

	for {
		var collection, err = collections.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return err
		}

		firestoreGroup.Go(func() error {
			return discoverCollection(ctx, ch, inferenceChannels, response, collection)
		})
	}

	routineGroup.Go(func() error { return discoveryRoutine(ctx, response, routineGroup, ch, inferenceChannels) })

	if err := firestoreGroup.Wait(); err != nil {
		return err
	}

	close(ch)
	for _, process := range inferenceChannels {
		if process.open {
			close(process.docsCh)
		}
	}

	if err := routineGroup.Wait(); err != nil {
		return err
	}

	return nil
}

// Recursively discover collections
func discoverCollection(
	ctx context.Context,
	ch chan inferenceRequest,
	inferenceChannels map[string]*inferenceProcess,
	response *pc.DiscoverResponse,
	collection *firestore.CollectionRef,
) error {
	var process, exists = inferenceChannels[collectionToResourcePath(collection.Path)]
	// if the channel is closed, don't bother reading more documents
	if exists && !process.open {
		return nil
	}
	log.WithField("collection", collectionToResourcePath(collection.Path)).Info("starting discovery of collection")
	var query = collection.Query.Limit(DISCOVER_DOC_LIMIT)
	var docs = query.Documents(ctx)

	for {
		// If at any point the channel is closed, just exit
		if exists && !process.open {
			return nil
		}

		var doc, err = docs.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return err
		}
		var data = doc.Data()
		data[firestore.DocumentID] = doc.Ref.ID
		data[PATH_FIELD] = doc.Ref.Path
		docJson, err := json.Marshal(data)
		log.WithField("collection", collectionToResourcePath(collection.Path)).Debug("sending inference request")
		ch <- inferenceRequest{
			document:   docJson,
			collection: collectionToResourcePath(collection.Path),
		}

		log.WithField("collection", collectionToResourcePath(collection.Path)).Debug("iterating over sub collections")
		var docCollectionsIterator = doc.Ref.Collections(ctx)
		for {
			var docCol, err = docCollectionsIterator.Next()
			if err == iterator.Done {
				break
			} else if err != nil {
				return err
			}

			err = discoverCollection(ctx, ch, inferenceChannels, response, docCol)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
