package main

import (
	"context"
	"fmt"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	proto "github.com/gogo/protobuf/proto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const specCollection = "flow_materializations"

type mongoApplier struct {
	client *mongo.Client
	cfg    config
}

func (e *mongoApplier) CreateMetaTables(ctx context.Context, spec *pf.MaterializationSpec) (string, boilerplate.ActionApplyFn, error) {
	// No-op since new collections are automatically created when data is added to them.
	return "", nil, nil
}

func (e *mongoApplier) CreateResource(ctx context.Context, spec *pf.MaterializationSpec, bindingIndex int) (string, boilerplate.ActionApplyFn, error) {
	// No-op since new collections are automatically created when data is added to them.
	return "", nil, nil
}

func (a *mongoApplier) LoadSpec(ctx context.Context, materialization pf.Materialization) (*pf.MaterializationSpec, error) {
	metaCollection := a.client.Database(a.cfg.Database).Collection(specCollection)

	type savedSpec struct {
		Id   string `bson:"uuid"`
		Spec []byte `bson:"spec"`
	}

	var s savedSpec
	if err := metaCollection.FindOne(ctx, bson.D{{Key: idField, Value: bson.D{{Key: "$eq", Value: materialization}}}}).Decode(&s); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("could not find spec: %w", err)
	}

	var existing pf.MaterializationSpec
	if err := proto.Unmarshal(s.Spec, &existing); err != nil {
		return nil, fmt.Errorf("parsing existing materialization spec: %w", err)
	}

	return &existing, nil
}

func (a *mongoApplier) PutSpec(ctx context.Context, spec *pf.MaterializationSpec, version string, _ bool) (string, boilerplate.ActionApplyFn, error) {
	bs, err := proto.Marshal(spec)
	if err != nil {
		return "", nil, fmt.Errorf("encoding existing materialization spec: %w", err)
	}

	return "update persisted spec", func(ctx context.Context) error {
		metaCollection := a.client.Database(a.cfg.Database).Collection(specCollection)
		opts := options.Replace().SetUpsert(true)
		if _, err := metaCollection.ReplaceOne(ctx, bson.D{{Key: idField, Value: spec.Name.String()}}, bson.D{{Key: "spec", Value: bs}}, opts); err != nil {
			return fmt.Errorf("upserting spec: %w", err)
		}
		return nil
	}, nil
}

func (a *mongoApplier) ReplaceResource(ctx context.Context, spec *pf.MaterializationSpec, bindingIndex int) (string, boilerplate.ActionApplyFn, error) {
	binding := spec.Bindings[bindingIndex]

	res, err := resolveResourceConfig(binding.ResourceConfigJson)
	if err != nil {
		return "", nil, err
	}

	return fmt.Sprintf("drop collection %q", res.Collection), func(ctx context.Context) error {
		// All we do here is drop the collection, since it will be re-created automatically.
		return a.client.Database(a.cfg.Database).Collection(res.Collection).Drop(ctx)
	}, nil
}

func (e *mongoApplier) UpdateResource(ctx context.Context, spec *pf.MaterializationSpec, bindingIndex int, applyParams boilerplate.BindingUpdate) (string, boilerplate.ActionApplyFn, error) {
	// No-op since nothing in particular is currently configured for created collection.
	return "", nil, nil
}
