package main

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

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

const (
	metaProperty    = "_meta"
	opProperty      = "op"
)

type documentMetadata struct {
	Op string `json:"op,omitempty" jsonschema:"title=Change Operation,description=Change operation type: 'c' Create/Insert 'u' Update 'd' Delete."`
}

func generateMinimalSchema() json.RawMessage {
	var reflector = jsonschema.Reflector{
		ExpandedStruct: true,
		DoNotReference: true,
	}
	var metadataSchema = reflector.ReflectFromType(reflect.TypeOf(documentMetadata{}))
	metadataSchema.Definitions = nil
	metadataSchema.AdditionalProperties = nil
	metadataSchema.Extras = map[string]interface{}{
		"reduce": map[string]interface{}{
			"strategy": "merge",
		},
	}

	// Wrap metadata into an enclosing object schema with a /_meta property
	var schema = &jsonschema.Schema{
		Type:                 "object",
		Required:             []string{idProperty},
		AdditionalProperties: nil,
		Extras: map[string]interface{}{
			"properties": map[string]*jsonschema.Schema{
				idProperty: {
					Type: "string",
				},
				metaProperty: metadataSchema,
			},
			"x-infer-schema": true,
			"reduce": map[string]interface{}{
				"strategy": "merge",
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

// Discover returns the set of resources available from this Driver.
func (d *driver) Discover(ctx context.Context, req *pc.Request_Discover) (*pc.Response_Discovered, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
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

	var bindings = []*pc.Response_Discovered_Binding{}
	for _, collection := range collections {
		// Views cannot be used with change streams, so we don't support them for
		// capturing at the moment
		if collection.Type == "view" {
			continue
		}
		resourceJSON, err := json.Marshal(resource{Database: db.Name(), Collection: collection.Name})
		if err != nil {
			return nil, fmt.Errorf("serializing resource json: %w", err)
		}

		bindings = append(bindings, &pc.Response_Discovered_Binding{
			RecommendedName:    fmt.Sprintf("%s/%s", db.Name(), collection.Name),
			ResourceConfigJson: resourceJSON,
			DocumentSchemaJson: minimalSchema,
			Key:                []string{"/" + idProperty},
		})
	}

	return &pc.Response_Discovered{Bindings: bindings}, nil
}
