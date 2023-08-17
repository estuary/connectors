package main

import (
	"fmt"

	"github.com/estuary/connectors/go/pkg/slices"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

// validateSelectedFields validates that the field selection with a binding are compatible with its
// own constraints. If any constraints would be violated, an error is returned with the reason why.
func validateSelectedFields(
	res resource,
	binding *pf.MaterializationSpec_Binding,
	storedSpec *pf.MaterializationSpec,
) error {
	// Calculated constraints for the fields of this binding.
	constraints, err := validateBinding(res, binding.ResourcePath, binding.Collection, storedSpec)
	if err != nil {
		return err
	}

	// Track all the location pointers for each included field so that we can verify all the
	// LOCATION_REQUIRED constraints are met.
	includedPointers := make(map[string]bool)

	// Does each field in the materialization have an allowable constraint?
	allFields := binding.FieldSelection.AllFields()
	for _, field := range allFields {
		var projection = binding.Collection.GetProjection(field)
		if projection == nil {
			return fmt.Errorf("no such projection for field '%s'", field)
		}
		includedPointers[projection.Ptr] = true
		constraint := constraints[field]
		if constraints[field].Type.IsForbidden() {
			return fmt.Errorf("the field '%s' may not be materialized because it has constraint: %s with reason: %s", field, constraint.Type, constraint.Reason)
		}
	}

	// Are all of the required fields and locations included?
	for field, constraint := range constraints {
		switch constraint.Type {
		case pm.Response_Validated_Constraint_FIELD_REQUIRED:
			if !slices.Contains(allFields, field) {
				return fmt.Errorf("required field '%s' is missing. It is required because: %s", field, constraint.Reason)
			}
		case pm.Response_Validated_Constraint_LOCATION_REQUIRED:
			var projection = binding.Collection.GetProjection(field)
			locDescription := fmt.Sprintf("location '%s'", projection.Ptr)
			if projection.IsRootDocumentProjection() {
				// The pointer is "" for the root document, which doesn't make much sense if
				// included directly in the error message.
				locDescription = "the root document"
			}

			if !includedPointers[projection.Ptr] {
				return fmt.Errorf(
					"the materialization must include a projection of %s, but no such projection is included. It is required because: %s",
					locDescription,
					constraint.Reason,
				)
			}
		}
	}

	return err
}

// validateBinding calculates the constraints for a new binding or a change to an existing binding.
func validateBinding(
	res resource,
	path []string,
	boundCollection pf.CollectionSpec,
	storedSpec *pf.MaterializationSpec,
) (map[string]*pm.Response_Validated_Constraint, error) {
	existingBinding, err := findExistingBinding(path, boundCollection.Name, storedSpec)
	if err != nil {
		return nil, err
	}

	var constraints map[string]*pm.Response_Validated_Constraint
	if existingBinding == nil {
		constraints = validateNewBinding(boundCollection, res.DeltaUpdates)
	} else {
		if existingBinding.DeltaUpdates && !res.DeltaUpdates {
			// We allow a binding to switch from standard => delta updates but not the other
			// way. This is because a standard materialization is trivially a valid
			// delta-updates materialization.
			return nil, fmt.Errorf("changing binding of collection %s from delta updates to standard updates is not allowed", existingBinding.Collection.Name.String())
		}
		constraints = validateMatchesExistingBinding(existingBinding, boundCollection, res.DeltaUpdates)
	}

	return constraints, nil
}

func validateNewBinding(boundCollection pf.CollectionSpec, deltaUpdates bool) map[string]*pm.Response_Validated_Constraint {
	constraints := make(map[string]*pm.Response_Validated_Constraint)
	for _, projection := range boundCollection.Projections {
		constraints[projection.Field] = validateNewProjection(projection, deltaUpdates)
	}
	return constraints
}

func validateMatchesExistingBinding(
	existing *pf.MaterializationSpec_Binding,
	boundCollection pf.CollectionSpec,
	deltaUpdates bool,
) map[string]*pm.Response_Validated_Constraint {
	constraints := make(map[string]*pm.Response_Validated_Constraint)

	for _, field := range existing.FieldSelection.Keys {
		constraints[field] = &pm.Response_Validated_Constraint{
			Type:   pm.Response_Validated_Constraint_FIELD_REQUIRED,
			Reason: "This field is a key in the current materialization",
		}
	}

	for _, field := range existing.FieldSelection.Values {
		constraint := new(pm.Response_Validated_Constraint)

		existingProjection := existing.Collection.GetProjection(field)
		proposedProjection := boundCollection.GetProjection(field)

		if proposedProjection == nil {
			// A value field has been removed, so no constraint is needed.
			continue
		}

		// Is the proposed type completely disallowed by the materialization? This differs from
		// being UNSATISFIABLE, which implies that re-creating the materialization could resolve the
		// difference.
		if c := validateNewProjection(*proposedProjection, deltaUpdates); c.Type == pm.Response_Validated_Constraint_FIELD_FORBIDDEN {
			constraints[field] = c
			continue
		}

		existingType := propForProjection(existingProjection).Type
		proposedType := propForProjection(proposedProjection).Type

		// We match on the endpoint-specific type here and not just the JSON type to account for
		// incompatible changes to string format fields, such as removing a numeric format. This
		// would imply that the field is no longer a numeric value, which would be incompatible with
		// the materialized mapping and should trigger an evolution of the materialization.
		if existingType == proposedType {
			constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
			constraint.Reason = "This location is part of the current materialization"
		} else {
			constraint.Type = pm.Response_Validated_Constraint_UNSATISFIABLE
			constraint.Reason = fmt.Sprintf(
				"Field '%s' is already being materialized as type '%s' and cannot be changed to type '%s'",
				field,
				existingType,
				proposedType,
			)
		}

		constraints[field] = constraint
	}

	for _, proj := range boundCollection.Projections {
		if !deltaUpdates && proj.IsRootDocumentProjection() {
			// Standard updates materializations cannot change the projection field of the root
			// document after the initial publication.
			if proj.Field != existing.FieldSelection.Document {
				constraints[proj.Field] = &pm.Response_Validated_Constraint{
					Type: pm.Response_Validated_Constraint_UNSATISFIABLE,
					Reason: fmt.Sprintf(
						"The root document was previously projected as field '%s' and cannot be changed to projection with field '%s'",
						existing.FieldSelection.Document,
						proj.Field,
					),
				}
			}
		}

		// Build constraints for new projections of the binding.
		if _, ok := constraints[proj.Field]; !ok {
			constraints[proj.Field] = validateNewProjection(proj, deltaUpdates)
		}
	}

	return constraints
}

func validateNewProjection(projection pf.Projection, deltaUpdates bool) *pm.Response_Validated_Constraint {
	// Types like ["string", "integer"] with "format: integer" have multiple types which we
	// cannot support, but the string value can be coerced to a isNumeric value, which we can
	// support.
	_, isNumeric := asFormattedNumeric(&projection)

	var constraint = pm.Response_Validated_Constraint{}
	switch {
	case projection.IsPrimaryKey:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "Primary key locations are required"
	case projection.IsRootDocumentProjection() && !deltaUpdates:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "The root document is required for a standard updates materialization"
	case projection.IsRootDocumentProjection():
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The root document should usually be materialized"
	case projection.Inference.IsSingleScalarType() || isNumeric:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The projection has a single scalar type"
	case projection.Inference.IsSingleType() && !slices.Contains(projection.Inference.Types, "array"):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "This field is able to be materialized"

	default:
		// Anything else is either multiple different types, a single 'null' type, or an
		// array type which we currently don't support. We could potentially support array
		// types if they made the "elements" configuration avaiable and that was a single
		// type.
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize this field"
	}

	return &constraint
}

func findExistingBinding(
	resourcePath []string,
	proposedCollection pf.Collection,
	storedSpec *pf.MaterializationSpec,
) (*pf.MaterializationSpec_Binding, error) {
	if storedSpec == nil {
		return nil, nil // Binding is trivially not found
	}
	for _, existingBinding := range storedSpec.Bindings {
		if existingBinding.Collection.Name == proposedCollection && pathEquals(resourcePath, existingBinding.ResourcePath) {
			// The binding already exists for this collection and is being materialized to the
			// index.
			return existingBinding, nil
		} else if pathEquals(resourcePath, existingBinding.ResourcePath) {
			// There is a binding already materializing to the index, but for a different
			// collection.
			return nil, fmt.Errorf(
				"cannot add a new binding to materialize collection '%s' to index '%s' because an existing binding for collection '%s' is already materializing to index '%s'",
				proposedCollection.String(),
				resourcePath,
				existingBinding.Collection.Name,
				resourcePath,
			)
		}
	}
	return nil, nil
}

func pathEquals(p1 []string, p2 []string) bool {
	if len(p1) != len(p2) {
		return false
	}

	for idx := range p1 {
		if p1[idx] != p2[idx] {
			return false
		}
	}

	return true
}
