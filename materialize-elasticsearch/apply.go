package main

import (
	"context"
	"fmt"
	"strings"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
)

type elasticApplier struct {
	client *client
	cfg    config
}

func (e *elasticApplier) CreateMetaTables(ctx context.Context, spec *pf.MaterializationSpec) (string, boilerplate.ActionApplyFn, error) {
	return fmt.Sprintf("create index %q", defaultFlowMaterializations), func(ctx context.Context) error {
		return e.client.createMetaIndex(ctx, e.cfg.Advanced.Replicas)
	}, nil
}

func (e *elasticApplier) CreateResource(ctx context.Context, spec *pf.MaterializationSpec, bindingIndex int) (string, boilerplate.ActionApplyFn, error) {
	binding := spec.Bindings[bindingIndex]

	var res resource
	if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
		return "", nil, fmt.Errorf("parsing resource config: %w", err)
	}

	props, err := buildIndexProperties(binding)
	if err != nil {
		return "", nil, err
	}

	return fmt.Sprintf("create index %q", binding.ResourcePath[0]), func(ctx context.Context) error {
		return e.client.createIndex(ctx, binding.ResourcePath[0], res.Shards, e.cfg.Advanced.Replicas, props)
	}, nil
}

func (e *elasticApplier) LoadSpec(ctx context.Context, materialization pf.Materialization) (*pf.MaterializationSpec, error) {
	return e.client.getSpec(ctx, materialization)
}

func (e *elasticApplier) PutSpec(ctx context.Context, spec *pf.MaterializationSpec, version string, _ bool) (string, boilerplate.ActionApplyFn, error) {
	return fmt.Sprintf("update stored materialization spec and set version = %s", version), func(ctx context.Context) error {
		return e.client.putSpec(ctx, spec, version)
	}, nil

}

func (e *elasticApplier) ReplaceResource(ctx context.Context, spec *pf.MaterializationSpec, bindingIndex int) (string, boilerplate.ActionApplyFn, error) {
	binding := spec.Bindings[bindingIndex]

	var res resource
	if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
		return "", nil, fmt.Errorf("parsing resource config: %w", err)
	}

	props, err := buildIndexProperties(binding)
	if err != nil {
		return "", nil, err
	}

	return fmt.Sprintf("replace index %q", binding.ResourcePath[0]), func(ctx context.Context) error {
		return e.client.replaceIndex(ctx, binding.ResourcePath[0], res.Shards, e.cfg.Advanced.Replicas, props)
	}, nil
}

func (e *elasticApplier) UpdateResource(ctx context.Context, spec *pf.MaterializationSpec, bindingIndex int, bindingUpdate boilerplate.BindingUpdate) (string, boilerplate.ActionApplyFn, error) {
	binding := spec.Bindings[bindingIndex]

	// ElasticSearch only considers new projections, since index mappings are always nullable.
	if len(bindingUpdate.NewProjections) == 0 {
		return "", nil, nil
	}

	var actions []string
	for _, newProjection := range bindingUpdate.NewProjections {
		prop, err := propForField(newProjection.Field, binding)
		if err != nil {
			return "", nil, err
		}

		actions = append(actions, fmt.Sprintf(
			"add mapping %q to index %q with type %q",
			translateField(newProjection.Field),
			binding.ResourcePath[0],
			prop.Type,
		))
	}

	return strings.Join(actions, "\n"), func(ctx context.Context) error {
		for _, newProjection := range bindingUpdate.NewProjections {
			if prop, err := propForField(newProjection.Field, binding); err != nil {
				return err
			} else if err := e.client.addMappingToIndex(ctx, binding.ResourcePath[0], translateField(newProjection.Field), prop); err != nil {
				return err
			}
		}
		return nil
	}, nil
}
