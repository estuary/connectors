package boilerplate

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/sirupsen/logrus"
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
	// NewProjections are selected Projections which must be added to the materialized resource
	// because they do not already exist. The definition for the materialized field must be computed
	// from the Projection.
	NewProjections []pf.Projection

	// NewlyNullableFields is the fields in the endpoint that should now be made nullable, that
	// weren't before. They may need to be nullable because a materialized field is now nullable and
	// wasn't before, or because they exist as non-nullable in the destination and aren't included
	// in the materialization's field selection.
	NewlyNullableFields []ExistingField

	// NewlyDeltaUpdates is if the materialized binding was standard updates per the previously
	// applied materialization spec, and is now delta updates. Some systems may need to do things
	// like drop primary key restraints in response to this change.
	NewlyDeltaUpdates bool
}

// Applier represents the capabilities needed for an endpoint to apply changes to materialized
// resources based on binding changes. Many of these functions should return an ActionApplyFn, which
// may be executed concurrently.
type Applier interface {
	// CreateResource creates a new resource in the endpoint. It is called only if the resource does
	// not already exist, either because it is brand new or because it was previously deleted as
	// part of a resource replacement.
	CreateResource(ctx context.Context, spec *pf.MaterializationSpec, bindingIndex int) (string, ActionApplyFn, error)

	// DeleteResource deletes a resource from the endpoint. It is used for replacing a materialized
	// resource when the `backfill` counter is incremented. It will only be called if the
	// materialized resource exists in the destination system, the resource exists in the prior
	// spec, and the backfill counter of the new spec is greater than the prior spec.
	DeleteResource(ctx context.Context, path []string) (string, ActionApplyFn, error)

	// UpdateResource updates an existing resource. The `BindingUpdate` contains specific
	// information about what is changing for the resource. `NewProjections` are assured to not
	// already exist in the destination, and `NewlyNullableFields` are assured to be non-nullable in
	// the destination. It's called for every binding, although it may not have any `BindingUpdate`
	// parameters. This is to allow materializations to perform additional specific actions on
	// binding changes that are not covered by the general cases of the `BindingUpdate` parameters.
	UpdateResource(ctx context.Context, spec *pf.MaterializationSpec, bindingIndex int, bindingUpdate BindingUpdate) (string, ActionApplyFn, error)
}

// ApplyChanges applies changes to an endpoint. It computes these changes from the apply request and
// the state of the endpoint per the `InfoSchema`. The `Applier` executes the resulting actions,
// optionally with a concurrent scatter/gather for expedience on endpoints that would benefit from
// that sort of thing.
func ApplyChanges(ctx context.Context, req *pm.Request_Apply, applier Applier, is *InfoSchema, concurrent bool) (*pm.Response_Applied, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	actionDescriptions := []string{}
	actions := []ActionApplyFn{}

	addAction := func(desc string, a ActionApplyFn) {
		if a != nil { // Convenience for handling endpoints that return `nil` for a no-op action.
			actionDescriptions = append(actionDescriptions, desc)
			actions = append(actions, a)
		}
	}

	computed, err := computeCommonUpdates(req.LastMaterialization, req.Materialization, is, false)
	if err != nil {
		return nil, fmt.Errorf("computing common updates: %w", err)
	}

	for _, bindingIdx := range computed.newBindings {
		desc, action, err := applier.CreateResource(ctx, req.Materialization, bindingIdx)
		if err != nil {
			return nil, fmt.Errorf("getting CreateResource action: %w", err)
		}
		addAction(desc, action)
	}

	for _, bindingIdx := range computed.backfillBindings {
		deleteDesc, deleteAction, err := applier.DeleteResource(ctx, req.Materialization.Bindings[bindingIdx].ResourcePath)
		if err != nil {
			return nil, fmt.Errorf("getting DeleteResource action to replace resource: %w", err)
		}

		createDesc, createAction, err := applier.CreateResource(ctx, req.Materialization, bindingIdx)
		if err != nil {
			return nil, fmt.Errorf("getting CreateResource action to replace resource: %w", err)
		}

		if deleteAction == nil && createAction == nil {
			// Currently only applicable to materialize-sqlite.
			continue
		}

		desc := []string{deleteDesc}
		if createDesc != "" {
			desc = append(desc, createDesc)
		}

		action := func(ctx context.Context) error {
			if err := deleteAction(ctx); err != nil {
				return err
			}

			if createAction != nil {
				if err := createAction(ctx); err != nil {
					return err
				}
			}

			return nil
		}

		addAction(strings.Join(desc, "\n"), action)
	}

	for bindingIdx, commonUpdates := range computed.updatedBindings {
		desc, action, err := applier.UpdateResource(ctx, req.Materialization, bindingIdx, commonUpdates)
		if err != nil {
			return nil, fmt.Errorf("getting UpdateResource action: %w", err)
		}
		addAction(desc, action)
	}

	if err := runActions(ctx, actions, actionDescriptions, concurrent); err != nil {
		return nil, err
	}

	return &pm.Response_Applied{ActionDescription: strings.Join(actionDescriptions, "\n")}, nil
}

type computedUpdates struct {
	newBindings      []int
	backfillBindings []int
	updatedBindings  map[int]BindingUpdate
}

func computeCommonUpdates(last, next *pf.MaterializationSpec, is *InfoSchema, caseInsensitiveFields bool) (*computedUpdates, error) {
	out := computedUpdates{
		updatedBindings: make(map[int]BindingUpdate),
	}

	for bindingIdx, nextBinding := range next.Bindings {
		// The existing binding spec is used to extract various properties that can't be learned
		// from introspecting the destination system, such as the backfill counter and if the
		// materialization was previously delta updates.
		lastBinding := findLastBinding(nextBinding.ResourcePath, last)
		if existingResource := is.GetResource(nextBinding.ResourcePath); existingResource == nil {
			// Resource does not yet exist, and must be created.
			out.newBindings = append(out.newBindings, bindingIdx)
		} else if lastBinding != nil && lastBinding.Backfill != nextBinding.Backfill {
			// Resource does exist but the backfill counter is being increased, so it must deleted
			// and re-created anew.
			out.backfillBindings = append(out.backfillBindings, bindingIdx)
		} else {
			// Resource does exist and may need updated for changes in the binding specification.
			update := BindingUpdate{
				NewlyDeltaUpdates: lastBinding != nil && !lastBinding.DeltaUpdates && nextBinding.DeltaUpdates,
			}

			for _, field := range nextBinding.FieldSelection.AllFields() {
				projection := *nextBinding.Collection.GetProjection(field)

				if existingField := existingResource.GetField(field); existingField != nil {
					newRequired := projection.Inference.Exists == pf.Inference_MUST && !slices.Contains(projection.Inference.Types, pf.JsonTypeNull)
					newlyNullable := !existingField.Nullable && !newRequired
					projectionHasDefault := projection.Inference.DefaultJson != nil
					if newlyNullable && !existingField.HasDefault && !projectionHasDefault {
						// The field has newly been made nullable and neither the existing field nor
						// the projection has a default value. The existing field will need to be
						// modified to be made nullable since it may need to hold null values now.
						update.NewlyNullableFields = append(update.NewlyNullableFields, *existingField)
					}
				} else {
					// Field does not exist in the materialized resource, so this is a new
					// projection to add to it.
					update.NewProjections = append(update.NewProjections, projection)
				}
			}

			// Fields that exist in the endpoint as non-nullable but aren't in the field selection
			// need to be made nullable too.
			for _, existingField := range existingResource.AllFields() {
				inFieldSelection, err := is.inSelectedFields(existingField.Name, nextBinding.FieldSelection)
				if err != nil {
					return nil, fmt.Errorf("determining if existing field %q is in field selection for resource %q: %w", existingField.Name, nextBinding.ResourcePath, err)
				}

				if !inFieldSelection && !existingField.Nullable {
					update.NewlyNullableFields = append(update.NewlyNullableFields, existingField)
				}
			}

			out.updatedBindings[bindingIdx] = update
		}
	}

	return &out, nil
}

func runActions(ctx context.Context, actions []ActionApplyFn, descriptions []string, concurrent bool) error {
	if concurrent {
		group, groupCtx := errgroup.WithContext(ctx)
		group.SetLimit(maxConcurrentUpdateActions)

		for i, a := range actions {
			a := a
			i := i
			group.Go(func() error {
				if err := a(groupCtx); err != nil {
					if !errors.Is(err, context.Canceled) {
						logrus.WithFields(logrus.Fields{
							"actionIndex":       i,
							"totalActions":      len(actions),
							"actionDescription": descriptions[i],
							"error":             fmt.Sprintf("%+v", err),
						}).Error("error executing apply action")
					}
					return err
				}
				return nil
			})
		}

		if err := group.Wait(); err != nil {
			return fmt.Errorf("executing concurrent apply actions: %w", err)
		}
	} else {
		for i, a := range actions {
			if err := a(ctx); err != nil {
				logrus.WithFields(logrus.Fields{
					"actionIndex":       i,
					"totalActions":      len(actions),
					"actionDescription": descriptions[i],
					"error":             fmt.Sprintf("%+v", err),
				}).Error("error executing apply action")
				return fmt.Errorf("executing apply actions: %w", err)
			}
		}
	}

	return nil
}
