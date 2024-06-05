package main

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"

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
	metaProperty = "_meta"
	opProperty   = "op"
)

type documentMetadata struct {
	Op string `json:"op,omitempty" jsonschema:"title=Change Operation,description=Change operation type: 'c' Create/Insert 'u' Update 'd' Delete.,enum=c,enum=u,enum=d"`
}

func generateMinimalSchema() json.RawMessage {
	var reflector = jsonschema.Reflector{
		ExpandedStruct: true,
		DoNotReference: true,
	}
	var metadataSchema = reflector.ReflectFromType(reflect.TypeOf(documentMetadata{}))
	metadataSchema.Definitions = nil
	metadataSchema.AdditionalProperties = nil

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
		},
		If: &jsonschema.Schema{
			Extras: map[string]interface{}{
				"properties": map[string]*jsonschema.Schema{
					"_meta": {
						Extras: map[string]interface{}{
							"properties": map[string]*jsonschema.Schema{
								"op": {
									Extras: map[string]interface{}{
										"const": "d",
									},
								},
							},
						},
					},
				},
			},
		},
		Then: &jsonschema.Schema{
			Extras: map[string]interface{}{
				"reduce": map[string]interface{}{
					"strategy": "lastWriteWins",
					"delete":   true,
				},
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

	var systemDatabases = []string{"config", "local"}

	var databaseNames []string
	// If no databases are provided in config, we discover across all databases
	if cfg.Database == "" {
		rawNames, err := client.ListDatabaseNames(ctx, bson.D{})
		if err != nil {
			return nil, fmt.Errorf("getting list of databases: %w", err)
		}
		for _, d := range rawNames {
			if slices.Contains(systemDatabases, d) {
				continue
			}

			databaseNames = append(databaseNames, d)
		}
	} else {
		databaseNames = strings.Split(cfg.Database, ",")
		for i, d := range databaseNames {
			databaseNames[i] = strings.TrimSpace(d)
		}
	}

	var bindings = []*pc.Response_Discovered_Binding{}

	for _, dbName := range databaseNames {
		var db = client.Database(dbName)

		collections, err := db.ListCollectionSpecifications(ctx, bson.D{})
		if err != nil {
			return nil, fmt.Errorf("listing collections: %w", err)
		}

		for _, collection := range collections {
			// Views cannot be used with change streams, so we don't support them for
			// capturing at the moment
			if collection.Type == "view" {
				continue
			}
			if strings.HasPrefix(collection.Name, "system.") {
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
	}

	return &pc.Response_Discovered{Bindings: bindings}, nil
}
