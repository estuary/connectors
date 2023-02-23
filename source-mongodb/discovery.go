package main

import (
	"context"
	"encoding/json"
	"fmt"

	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"

	"github.com/invopop/jsonschema"

	"go.mongodb.org/mongo-driver/bson"
)

// minimalSchema is the maximally-permissive schema which just specifies the
// _id key. The schema of collections is minimalSchema as we
// rely on Flow's schema inference to infer the collection schema
var minimalSchema = generateMinimalSchema()
const idProperty = "_id"

func generateMinimalSchema() json.RawMessage {
	// Wrap metadata into an enclosing object schema with a /_meta property
	var schema = &jsonschema.Schema{
		Type:                 "object",
		Required:             []string{idProperty},
		AdditionalProperties: nil,
		Extras: map[string]interface{}{
			"properties": map[string]*jsonschema.Schema{
				idProperty: &jsonschema.Schema{
					Type: "string",
				},
			},
			"x-infer-schema": true,
		},
	}

	// Marshal schema to JSON
	bs, err := json.Marshal(schema)
	if err != nil {
		panic(fmt.Errorf("error generating schema: %v", err))
	}
	return json.RawMessage(bs)
}

// Discover returns the set of resources available from this Driver.
func (d *driver) Discover(ctx context.Context, req *pc.DiscoverRequest) (*pc.DiscoverResponse, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.EndpointSpecJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config json: %w", err)
	}

	var client, err = d.Connect(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to database: %w", err)
	}

	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	var db = client.Database(cfg.Database)

	collections, err := db.ListCollectionSpecifications(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("listing collections: %w", err)
	}

	var bindings = []*pc.DiscoverResponse_Binding{}
	for _, collection := range collections {
		resourceJSON, err := json.Marshal(resource{Database: db.Name(), Collection: collection.Name})
		if err != nil {
			return nil, fmt.Errorf("serializing resource json: %w", err)
		}

		bindings = append(bindings, &pc.DiscoverResponse_Binding{
			RecommendedName: pf.Collection(fmt.Sprintf("%s/%s", db.Name(), collection.Name)),
			ResourceSpecJson: resourceJSON,
			DocumentSchemaJson: minimalSchema,
			KeyPtrs: []string{"/" + idProperty},
		})
	}

	return &pc.DiscoverResponse{Bindings: bindings}, nil
}
