package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	schemagen "github.com/estuary/connectors/go-schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"go.gazette.dev/core/consumer/protocol"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// driver implements the DriverServer interface.
type driver struct{}

func (d *driver) connect(ctx context.Context, cfg config) (*mongo.Client, error) {
	// Create a new client and connect to the server
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.ToURI()))
	if err != nil {
		return nil, err
	}

	// Ping the primary
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, err
	}

	return client, nil
}

func (d driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	es := schemagen.GenerateSchema("Materialize MongoDB Spec", &config{})
	endpointSchema, err := es.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("MongoDB Collection", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/materialize-mongodb",
		Oauth2:                   nil,
	}, nil
}

func (d driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	cfg, err := resolveEndpointConfig(req.ConfigJson)
	if err != nil {
		return nil, err
	}

	_, err = d.connect(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to database: %w", err)
	}

	// MongoDB supports arbitrary JSON documents so there are no constraints on
	// any fields and all fields are marked as "recommended"
	var out []*pm.Response_Validated_Binding
	for _, b := range req.Bindings {
		res, err := resolveResourceConfig(b.ResourceConfigJson)
		if err != nil {
			return nil, err
		}

		constraints := make(map[string]*pm.Response_Validated_Constraint)
		for _, projection := range b.Collection.Projections {
			var constraint = new(pm.Response_Validated_Constraint)
			switch {
			case projection.IsRootDocumentProjection():
				constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
				constraint.Reason = "The root document must be materialized"
			case projection.IsPrimaryKey:
				constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
				constraint.Reason = "Document keys must be included"
			default:
				constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
				constraint.Reason = "MongoDB only materializes the full document"
			}
			constraints[projection.Field] = constraint
		}

		resourcePath := []string{cfg.Database, res.Collection}

		out = append(out, &pm.Response_Validated_Binding{
			Constraints:  constraints,
			DeltaUpdates: res.DeltaUpdates,
			ResourcePath: resourcePath,
		})
	}

	return &pm.Response_Validated{Bindings: out}, nil
}

// Apply checks for bindings that have been removed by comparing them with
// previously persisted spec, and deletes the corresponding collection
func (d driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	cfg, err := resolveEndpointConfig(req.Materialization.ConfigJson)
	if err != nil {
		return nil, err
	}

	client, err := d.connect(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to database: %w", err)
	}

	var db = client.Database(cfg.Database)

	var newCollections []string
	for _, binding := range req.Materialization.Bindings {
		newCollections = append(newCollections, binding.ResourcePath[1])
	}

	existing, err := d.LoadSpec(ctx, cfg, string(req.Materialization.Name))
	if err != nil {
		return nil, fmt.Errorf("loading spec: %w", err)
	}

	var actions []string
	if existing != nil {
		for _, binding := range existing.Bindings {
			var collection = binding.ResourcePath[1]
			// A binding that has been removed
			if !SliceContains(collection, newCollections) {
				if !req.DryRun {
					var col = db.Collection(collection)
					if err := col.Drop(ctx); err != nil {
						return nil, fmt.Errorf("dropping collection %s: %w", collection, err)
					}
				}

				actions = append(actions, fmt.Sprintf("drop collection %s", collection))
			}
		}
	}

	err = d.WriteSpec(ctx, cfg, req.Materialization, req.Version)
	if err != nil {
		return nil, fmt.Errorf("writing spec: %w", err)
	}

	return &pm.Response_Applied{
		ActionDescription: strings.Join(actions, "\n"),
	}, nil
}

func (d driver) NewTransactor(ctx context.Context, open pm.Request_Open) (pm.Transactor, *pm.Response_Opened, error) {
	var cfg, err = resolveEndpointConfig(open.Materialization.ConfigJson)
	if err != nil {
		return nil, nil, err
	}

	client, err := d.connect(ctx, cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("connecting to database: %w", err)
	}

	var bindings []*binding
	for _, b := range open.Materialization.Bindings {
		res, err := resolveResourceConfig(b.ResourceConfigJson)
		if err != nil {
			return nil, nil, err
		}
		var collection = client.Database(cfg.Database).Collection(res.Collection)

		bindings = append(bindings, &binding{
			collection: collection,
			res:        res,
		})
	}

	var fenceCollection = client.Database(cfg.Database).Collection(fenceCollectionName)

	var materialization = string(open.Materialization.Name)
	var filter = bson.D{{
		"materialization", bson.D{{"$eq", materialization}},
	}}

	var fence fenceRecord
	if err := fenceCollection.FindOne(ctx, filter, options.FindOne()).Decode(&fence); err != nil && err != mongo.ErrNoDocuments {
		return nil, nil, fmt.Errorf("finding existing fence: %w", err)
	} else if err == mongo.ErrNoDocuments {
		fence.Materialization = materialization
		if _, err = fenceCollection.InsertOne(ctx, fence, options.InsertOne()); err != nil {
			return nil, nil, fmt.Errorf("inserting new fence: %w", err)
		}
	}

	var bump = bson.D{{"$inc", bson.D{{"fence", 1}}}}
	var updateOpts = options.FindOneAndUpdate().SetReturnDocument(options.After)
	if err = fenceCollection.FindOneAndUpdate(ctx, filter, bump, updateOpts).Decode(&fence); err != nil {
		return nil, nil, fmt.Errorf("bumping fence: %w", err)
	}

	var cp *protocol.Checkpoint
	if err := cp.Unmarshal(fence.Checkpoint); err != nil {
		return nil, nil, fmt.Errorf("unmarshalling checkpoint, %w", err)
	}

	return &transactor{
			materialization: materialization,
			client:          client,
			bindings:        bindings,
			fenceCollection: fenceCollection,
			fence:           &fence,
		}, &pm.Response_Opened{
			RuntimeCheckpoint: cp,
		}, nil
}

func resolveEndpointConfig(specJson json.RawMessage) (config, error) {
	var cfg = config{}
	if err := pf.UnmarshalStrict(specJson, &cfg); err != nil {
		return cfg, fmt.Errorf("parsing MongoDB config: %w", err)
	}

	return cfg, nil
}

func resolveResourceConfig(specJson json.RawMessage) (resource, error) {
	var res = resource{}
	if err := pf.UnmarshalStrict(specJson, &res); err != nil {
		return res, fmt.Errorf("parsing resource config: %w", err)
	}

	return res, nil
}

func SliceContains(expected string, actual []string) bool {
	for _, ty := range actual {
		if ty == expected {
			return true
		}
	}
	return false
}

func main() {
	boilerplate.RunMain(driver{})
}
