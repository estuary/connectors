package boilerplate

import (
	"context"
	"fmt"
	"slices"
	"strings"

	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"golang.org/x/sync/errgroup"
)

const (
	maxConcurrentUpdateActions = 5
)

// ActionApplyFn is a callback that will be executed to carry out some action to achieve a change to
// a materialized resource.
type ActionApplyFn func(context.Context) error

// BindingUpdate is a distilled representation of the typical kinds of changes a destination system
// will care about in response to a new binding or change to an existing binding.
type BindingUpdate struct {
	// NewProjections is the projections of a proposed materialization spec that are included in the
	// field selection but there are no fields in the materialized resource for.
	NewProjections []pf.Projection

	// NewlyNullableFields is the fields in the endpoint that should now be made nullable, that
	// weren't before. They may need to be nullable because a materialized field is now nullable and
	// wasn't before, or because they exist as non-nullable in the destination and aren't included
	// in the materialization's field selection.
	NewlyNullableFields []EndpointField

	// NewlyDeltaUpdates is if the materialized binding was standard updates per the previously
	// applied materialization spec, and is now delta updates. Some systems may need to do things
	// like drop primary key restraints in response to this change.
	NewlyDeltaUpdates bool
}

// Applier represents the capabilities needed for an endpoint to apply changes to materialized
// resources based on binding changes. Many of these functions should return an ActionApplyFn, which
// will be executed only if the `Apply` is not a dry-run.
type Applier interface {
	// CreateMetaTables is called to create the tables (or the equivalent endpoint concept) that
	// store a persisted spec and any other metadata the materialization needs to persist.
	CreateMetaTables(ctx context.Context, spec *pf.MaterializationSpec) (string, ActionApplyFn, error)

	// LoadSpec loads the persisted spec from the metadata table.
	LoadSpec(ctx context.Context, materialization pf.Materialization) (*pf.MaterializationSpec, error)

	// PutSpec upserts a spec into the metadata table.
	PutSpec(ctx context.Context, spec *pf.MaterializationSpec, version string, exists bool) (string, ActionApplyFn, error)

	// CreateResource creates a new resource in the endpoint. It is called only if the resource does
	// not already exist.
	CreateResource(ctx context.Context, spec *pf.MaterializationSpec, bindingIndex int) (string, ActionApplyFn, error)

	// ReplaceResource replaces a resource in the endpoint so that it can start materializing from
	// the beginning. It may be called even if the resource doesn't already exist. This is usually
	// carried out with a "CREATE OR REPLACE ..." type of action.
	ReplaceResource(ctx context.Context, spec *pf.MaterializationSpec, bindingIndex int) (string, ActionApplyFn, error)

	// UpdateResource updates an existing resource. The `BindingUpdate` contains specific
	// information about what is changing for the resource. `NewProjections` are assured to not
	// already exist in the destination, and `NewlyNullableFields` are assured to be non-nullable in
	// the destination. It's called for every binding, although it may not have any `BindingUpdate`
	// parameters. This is to allow materializations to perform additional specific actions on
	// binding changes that are not covered by the general cases of the `BindingUpdate` parameters.
	UpdateResource(ctx context.Context, spec *pf.MaterializationSpec, bindingIndex int, applyParams BindingUpdate) (string, ActionApplyFn, error)
}

// ApplyChanges applies changes to an endpoint. It computes these changes from the apply request and
// the state of the endpoint per the `InfoSchema`. The `Applier` executes the resulting actions,
// optionally with a concurrent scatter/gather for expedience on endpoints that would benefit from
// that sort of thing.
func ApplyChanges(ctx context.Context, req *pm.Request_Apply, applier Applier, is *InfoSchema, concurrent bool) (*pm.Response_Applied, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	storedSpec, err := applier.LoadSpec(ctx, req.Materialization.Name)
	if err != nil {
		return nil, fmt.Errorf("getting stored spec: %w", err)
	}

	actionDescriptions := []string{}
	actions := []ActionApplyFn{}

	addAction := func(desc string, a ActionApplyFn) {
		if a != nil { // Convenience for handling endpoints that return `nil` for a no-op action.
			actionDescriptions = append(actionDescriptions, desc)
			actions = append(actions, a)
		}
	}

	// TODO(whb): We will eventually stop persisting specs for materializations, and instead include
	// the previous spec as part of the protocol. When that happens this can go away. Then, if
	// individual materializations still need individual metadata tables (ex: SQL materializations),
	// they should create them as needed separately from the Applier. For now, we always call
	// CreateMetaTables, and materializations are free to either create them or not.
	desc, action, err := applier.CreateMetaTables(ctx, req.Materialization)
	if err != nil {
		return nil, fmt.Errorf("getting CreateMetaTables action: %w", err)
	}
	addAction(desc, action)

	for bindingIdx, binding := range req.Materialization.Bindings {
		// The existing binding spec is used to extract various properties that can't be learned
		// from introspecting the destination system, such as the backfill counter and if the
		// materialization was previously delta updates.
		existingBinding, err := findExistingBinding(binding.ResourcePath, binding.Collection.Name, storedSpec)
		if err != nil {
			return nil, fmt.Errorf("finding existing binding: %w", err)
		}

		if !is.HasResource(binding.ResourcePath) {
			// Resource does not yet exist, and must be created.
			desc, action, err := applier.CreateResource(ctx, req.Materialization, bindingIdx)
			if err != nil {
				return nil, fmt.Errorf("getting CreateResource action: %w", err)
			}
			addAction(desc, action)
		} else if existingBinding != nil && existingBinding.Backfill != binding.Backfill {
			// Resource does exist but the backfill counter is being increased, so it must be
			// replaced.
			desc, action, err := applier.ReplaceResource(ctx, req.Materialization, bindingIdx)
			if err != nil {
				return nil, fmt.Errorf("getting ReplaceResource action: %w", err)
			}
			addAction(desc, action)
		} else {
			// Resource does exist and may need updated for changes in the binding specification.
			params := BindingUpdate{
				NewlyDeltaUpdates: existingBinding != nil && !existingBinding.DeltaUpdates && binding.DeltaUpdates,
			}

			for _, proposedField := range binding.FieldSelection.AllFields() {
				proposedProjection := *binding.Collection.GetProjection(proposedField)

				if is.HasField(binding.ResourcePath, proposedField) {
					existingField, err := is.GetField(binding.ResourcePath, proposedField)
					if err != nil {
						return nil, fmt.Errorf("getting existing field information for field %q of resource %q: %w", proposedField, binding.ResourcePath, err)
					}

					newRequired := proposedProjection.Inference.Exists == pf.Inference_MUST && !slices.Contains(proposedProjection.Inference.Types, pf.JsonTypeNull)
					if !existingField.Nullable && !newRequired {
						// The existing field is not nullable, but the proposed projection for the
						// field is. The existing field will need to be modified to be made
						// nullable.
						params.NewlyNullableFields = append(params.NewlyNullableFields, existingField)
					}
				} else {
					// Field does not exist in the materialized resource, so this is a new
					// projection to add to it.
					params.NewProjections = append(params.NewProjections, proposedProjection)
				}
			}

			// Fields that exist in the endpoint as non-nullable but aren't in the field selection
			// need to be made nullable too.
			existingFields, err := is.FieldsForResource(binding.ResourcePath)
			if err != nil {
				return nil, fmt.Errorf("getting list of existing fields for resource %s: %w", binding.ResourcePath, err)
			}

			for _, existingField := range existingFields {
				inFieldSelection, err := is.InSelectedFields(existingField.Name, binding.FieldSelection)
				if err != nil {
					return nil, fmt.Errorf("determining if existing field %q is in field selection for resource %q: %w", existingField.Name, binding.ResourcePath, err)
				}

				if !inFieldSelection && !existingField.Nullable {
					params.NewlyNullableFields = append(params.NewlyNullableFields, existingField)
				}
			}

			desc, action, err := applier.UpdateResource(ctx, req.Materialization, bindingIdx, params)
			if err != nil {
				return nil, fmt.Errorf("getting UpdateResource action: %w", err)
			}
			addAction(desc, action)
		}
	}

	// TODO(whb): DryRun as a concept will no longer exist after async applies are active in the
	// runtime.
	if !req.DryRun {
		if concurrent {
			group, groupCtx := errgroup.WithContext(ctx)
			group.SetLimit(maxConcurrentUpdateActions)

			for _, a := range actions {
				a := a
				group.Go(func() error {
					return a(groupCtx)
				})
			}

			if err := group.Wait(); err != nil {
				return nil, err
			}
		} else {
			for _, a := range actions {
				if err := a(ctx); err != nil {
					return nil, err
				}
			}
		}
	}

	// Only update the spec after all other actions have completed successfully.
	desc, action, err = applier.PutSpec(ctx, req.Materialization, req.Version, storedSpec != nil)
	if err != nil {
		return nil, fmt.Errorf("getting PutSpec action: %w", err)
	}
	// Although all current materializations always do persist a spec, its possible that some may
	// not in the future as we transition to runtime provided specs for apply.
	if action != nil {
		actionDescriptions = append(actionDescriptions, desc)
		if !req.DryRun { // Don't actually update the spec if its a dry run.
			if err := action(ctx); err != nil {
				return nil, fmt.Errorf("updating persisted specification: %w", err)
			}
		}
	}

	return &pm.Response_Applied{ActionDescription: strings.Join(actionDescriptions, "\n")}, nil
}
