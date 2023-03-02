package sql

import (
	"fmt"

	"github.com/estuary/connectors/go/protocol"
)

// ValidateSelectedFields validates a proposed MaterializationSpec against a set of constraints. If
// any constraints would be violated, then an error is returned.
func ValidateSelectedFields(constraints map[string]protocol.Constraint, proposed protocol.ApplyBinding) error {
	// Track all the location pointers for each included field so that we can verify all the
	// LOCATION_REQUIRED constraints are met.
	var includedPointers = make(map[string]bool)

	// Does each field in the materialization have an allowable constraint?
	var allFields = proposed.FieldSelection.AllFields()
	for _, field := range allFields {
		var projection = proposed.Collection.GetProjection(field)
		if projection == nil {
			return fmt.Errorf("no such projection for field '%s'", field)
		}
		includedPointers[projection.Ptr] = true
		var constraint = constraints[field]
		if constraint.Type.IsForbidden() {
			return fmt.Errorf("the field '%s' may not be materialize because it has constraint: %s with reason: %s", field, constraint.Type, constraint.Reason)
		}
	}

	// Are all of the required fields and locations included?
	for field, constraint := range constraints {
		switch constraint.Type {
		case protocol.FieldRequired:
			if !SliceContains(field, allFields) {
				return fmt.Errorf("required field '%s' is missing. It is required because: %s", field, constraint.Reason)
			}
		case protocol.LocationRequired:
			var projection = proposed.Collection.GetProjection(field)
			if !includedPointers[projection.Ptr] {
				return fmt.Errorf("the materialization must include a projections of location '%s', but no such projection is included", projection.Ptr)
			}
		}
	}

	return nil
}

// ValidateNewSQLProjections returns a set of constraints for a proposed flow collection for a
// **new** materialization (one that is not running and has never been Applied). Note that this will
// "recommend" all projections of single scalar types, which is what drives the default field
// selection in flowctl.
func ValidateNewSQLProjections(resource Resource, proposed protocol.CollectionSpec) map[string]protocol.Constraint {
	var constraints = make(map[string]protocol.Constraint)
	for _, projection := range proposed.Projections {
		var constraint = protocol.Constraint{}
		switch {
		case len(projection.Field) > 63:
			constraint.Type = protocol.FieldForbidden
			constraint.Reason = "Field names must be less than 63 bytes in length."
		case projection.IsPrimaryKey:
			constraint.Type = protocol.LocationRequired
			constraint.Reason = "All Locations that are part of the collections key are required"
		case projection.IsRootDocumentProjection() && resource.DeltaUpdates():
			constraint.Type = protocol.LocationRecommended
			constraint.Reason = "The root document should usually be materialized"
		case projection.IsRootDocumentProjection():
			constraint.Type = protocol.LocationRequired
			constraint.Reason = "The root document must be materialized"
		case projection.Inference.IsSingleScalarType():
			constraint.Type = protocol.LocationRecommended
			constraint.Reason = "The projection has a single scalar type"

		case projection.Inference.IsSingleType() || len(effectiveJsonTypes(&projection)) == 1:
			constraint.Type = protocol.FieldOptional
			constraint.Reason = "This field is able to be materialized"
		default:
			// If we got here, then either the field may have multiple incompatible types, or the
			// only possible type is "null". In either case, we're not going to allow it.
			// Technically, we could allow the null type to be materialized, but I can't think of a
			// use case where that would be desirable.
			constraint.Type = protocol.FieldForbidden
			constraint.Reason = "Cannot materialize this field"
		}
		constraints[projection.Field] = constraint
	}
	return constraints
}

// ValidateMatchesExisting returns a set of constraints to use when there is a new proposed
// CollectionSpec for a materialization that is already running, or has been Applied. The returned
// constraints will explicitly require all fields that are currently materialized, as long as they
// are not unsatisfiable, and forbid any fields that are not currently materialized.
func ValidateMatchesExisting(existing protocol.ApplyBinding, proposed protocol.CollectionSpec) map[string]protocol.Constraint {
	var constraints = make(map[string]protocol.Constraint)
	for _, field := range existing.FieldSelection.AllFields() {
		var constraint = protocol.Constraint{}
		var typeError = checkTypeError(field, existing.Collection, proposed)
		if len(typeError) > 0 {
			constraint.Type = protocol.Unsatisfiable
			constraint.Reason = typeError
		} else {
			constraint.Type = protocol.FieldRequired
			constraint.Reason = "This field is part of the current materialization"
		}

		constraints[field] = constraint
	}
	// We'll loop through the proposed projections and forbid any that aren't already in our map.
	// This is done solely so that we can supply a descriptive reason, since any fields we fail to
	// mention are implicitly forbidden.
	for _, proj := range proposed.Projections {
		if _, ok := constraints[proj.Field]; !ok {
			var constraint = protocol.Constraint{}
			constraint.Type = protocol.FieldForbidden
			constraint.Reason = "This field is not included in the existing materialization."
			constraints[proj.Field] = constraint
		}
	}

	return constraints
}

func checkTypeError(field string, existing protocol.CollectionSpec, proposed protocol.CollectionSpec) string {
	// existingProjection is guaranteed to exist since the MaterializationSpec has already been
	// validated.
	var existingProjection = existing.GetProjection(field)
	var proposedProjection = proposed.GetProjection(field)
	if proposedProjection == nil {
		return "The proposed materialization is missing the projection, which is required because it's included in the existing materialization"
	}

	// Ensure that the possible types of the proposed are compatible with the possible types of the
	// existing. The new projection is always allowed to contain fewer types than the original since
	// that will always work with the original database schema. Additional proposed types may be
	// compatible if they do not alter the effective FlatType, such as adding a string formatted as
	// an integer to an existing integer type.
	for _, pt := range effectiveJsonTypes(proposedProjection) {
		if !SliceContains(pt, effectiveJsonTypes(existingProjection)) && pt != "null" {
			return fmt.Sprintf("The proposed projection may contain the type '%s', which is not part of the original projection", pt)
		}
	}

	return ""
}

func SliceContains(expected string, actual []string) bool {
	for _, ty := range actual {
		if ty == expected {
			return true
		}
	}
	return false
}
