package boilerplate

import (
	"context"
	"errors"
	"fmt"
	"slices"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const (
	maxConcurrentUpdateActions = 5
)

// ActionApplyFn is a callback that will be executed to carry out some action to achieve a change to
// a materialized resource.
type ActionApplyFn func(context.Context) error

type updateBinding struct {
	newProjections      []pf.Projection
	newlyNullableFields []ExistingField
	newlyDeltaUpdates   bool
}

type computedUpdates struct {
	newBindings      []int
	backfillBindings []int
	updatedBindings  map[int]updateBinding
}

func computeCommonUpdates(last, next *pf.MaterializationSpec, is *InfoSchema) (*computedUpdates, error) {
	out := computedUpdates{
		updatedBindings: make(map[int]updateBinding),
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
			u, err := computeBindingUpdate(is, existingResource, lastBinding, nextBinding)
			if err != nil {
				return nil, err
			}
			out.updatedBindings[bindingIdx] = *u
		}
	}

	return &out, nil
}

func computeBindingUpdate(is *InfoSchema, existing *ExistingResource, last, next *pf.MaterializationSpec_Binding) (*updateBinding, error) {
	update := updateBinding{
		newlyDeltaUpdates: last != nil && !last.DeltaUpdates && next.DeltaUpdates,
	}

	for _, field := range next.FieldSelection.AllFields() {
		projection := *next.Collection.GetProjection(field)

		if existingField := existing.GetField(field); existingField != nil {
			newRequired := projection.Inference.Exists == pf.Inference_MUST && !slices.Contains(projection.Inference.Types, pf.JsonTypeNull)
			newlyNullable := !existingField.Nullable && !newRequired
			projectionHasDefault := projection.Inference.DefaultJson != nil
			if newlyNullable && !existingField.HasDefault && !projectionHasDefault {
				// The field has newly been made nullable and neither the existing field nor
				// the projection has a default value. The existing field will need to be
				// modified to be made nullable since it may need to hold null values now.
				update.newlyNullableFields = append(update.newlyNullableFields, *existingField)
			}
		} else {
			// Field does not exist in the materialized resource, so this is a new
			// projection to add to it.
			update.newProjections = append(update.newProjections, projection)
		}
	}

	// Fields that exist in the endpoint as non-nullable but aren't in the field selection
	// need to be made nullable too.
	for _, existingField := range existing.AllFields() {
		inFieldSelection, err := is.inSelectedFields(existingField.Name, next.FieldSelection)
		if err != nil {
			return nil, fmt.Errorf("determining if existing field %q is in field selection for resource %q: %w", existingField.Name, next.ResourcePath, err)
		}

		if !inFieldSelection && !existingField.Nullable {
			update.newlyNullableFields = append(update.newlyNullableFields, existingField)
		}
	}

	return &update, nil
}

func runActions(ctx context.Context, actions []ActionApplyFn, descriptions []string, concurrent bool) error {
	if concurrent {
		group, groupCtx := errgroup.WithContext(ctx)
		group.SetLimit(maxConcurrentUpdateActions)

		for i, a := range actions {
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
