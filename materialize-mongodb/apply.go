package main

import (
	"context"
	"fmt"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	"go.mongodb.org/mongo-driver/mongo"
)

const specCollection = "flow_materializations"

type mongoApplier struct {
	client *mongo.Client
	cfg    config
}

func (e *mongoApplier) CreateResource(ctx context.Context, spec *pf.MaterializationSpec, bindingIndex int) (string, boilerplate.ActionApplyFn, error) {
	// No-op since new collections are automatically created when data is added to them.
	return "", nil, nil
}

func (a *mongoApplier) DeleteResource(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	return fmt.Sprintf("drop collection %q", path[1]), func(ctx context.Context) error {
		return a.client.Database(path[0]).Collection(path[1]).Drop(ctx)
	}, nil
}

func (e *mongoApplier) UpdateResource(ctx context.Context, spec *pf.MaterializationSpec, bindingIndex int, bindingUpdate boilerplate.BindingUpdate) (string, boilerplate.ActionApplyFn, error) {
	// No-op since nothing in particular is currently configured for created collection.
	return "", nil, nil
}
