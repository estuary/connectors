package sql

import (
	"fmt"
	"strings"

	"github.com/estuary/connectors/go/pkg/slices"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

// ValidateSelectedFields validates a proposed MaterializationSpec against a set of constraints. If
// any constraints would be violated, then an error is returned.
func ValidateSelectedFields(constraints map[string]*pm.Response_Validated_Constraint, proposed *pf.MaterializationSpec_Binding) error {
	// Track all the location pointers for each included field so that we can verify all the
	// LOCATION_REQUIRED constraints are met.
	var includedPointers = make(map[string]bool)

	// Does each field in the materialization have an allowable constraint?
	var allFields = proposed.FieldSelection.AllFields()
	for _, field := range allFields {
		var projection = proposed.Collection.GetProjection(field)
		if projection == nil {
			return fmt.Errorf("No such projection for field '%s'", field)
		}
		includedPointers[projection.Ptr] = true
		var constraint = constraints[field]
		if constraint.Type.IsForbidden() {
			return fmt.Errorf("The field '%s' may not be materialize because it has constraint: %s with reason: %s", field, constraint.Type, constraint.Reason)
		}
	}

	// Are all of the required fields and locations included?
	for field, constraint := range constraints {
		switch constraint.Type {
		case pm.Response_Validated_Constraint_FIELD_REQUIRED:
			var projection = proposed.Collection.GetProjection(field)
			if !slices.Contains(allFields, field) {
				return fmt.Errorf("Required field '%s' is missing. It is required because: %s", field, constraint.Reason)
			}
			if projection == nil || projection.Inference.Exists != pf.Inference_MUST {
				return fmt.Errorf("Required field '%s' is marked as optional in collection schema. It is required because: %s", field, constraint.Reason)
			}
		case pm.Response_Validated_Constraint_LOCATION_REQUIRED:
			var projection = proposed.Collection.GetProjection(field)
			if !includedPointers[projection.Ptr] {
				return fmt.Errorf("The materialization must include a projections of location '%s', but no such projection is included. It is required because: %s", projection.Ptr, constraint.Reason)
			}
			if projection == nil || projection.Inference.Exists != pf.Inference_MUST {
				return fmt.Errorf("The materialization must include a projection of location '%s', but no such projection is included. It is required because: %s", field, constraint.Reason)
			}
		}
	}

	return nil
}

// ValidateNewSQLProjections returns a set of constraints for a proposed flow collection for a
// **new** materialization (one that is not running and has never been Applied). Note that this will
// "recommend" all projections of single scalar types, which is what drives the default field
// selection in flowctl.
func ValidateNewSQLProjections(resource Resource, proposed *pf.CollectionSpec) (map[string]*pm.Response_Validated_Constraint, error) {
	var constraints = make(map[string]*pm.Response_Validated_Constraint)
	for _, projection := range proposed.Projections {
		var constraint = validateNewProjection(resource, projection)
		constraints[projection.Field] = constraint

		switch constraint.Type {
		case pm.Response_Validated_Constraint_FIELD_REQUIRED:
			if projection.Inference.Exists != pf.Inference_MUST {
				return nil, fmt.Errorf("Required field '%s' is marked as optional in collection schema. It is required because: %s", projection.Field, constraint.Reason)
			}
		case pm.Response_Validated_Constraint_LOCATION_REQUIRED:
			if projection.Inference.Exists != pf.Inference_MUST {
				return nil, fmt.Errorf("Required location '%s' is marked as optional in collection schema. It is required because: %s", projection.Field, constraint.Reason)
			}
		}
	}
	return constraints, nil
}

func validateNewProjection(resource Resource, projection pf.Projection) *pm.Response_Validated_Constraint {
	// Allow for providing constraints based on a projection's SQL FlatType, which may recommended
	// JSON strings that are formatted as numeric values.
	sqlProjection := Projection{
		Projection: projection,
	}
	flatType, _ := sqlProjection.AsFlatType()

	var constraint = new(pm.Response_Validated_Constraint)
	switch {
	case len(projection.Field) > 63:
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Field names must be less than 63 bytes in length."
	case projection.IsPrimaryKey:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "All Locations that are part of the collections key are required"
	case projection.IsRootDocumentProjection() && resource.DeltaUpdates():
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The root document should usually be materialized"
	case projection.IsRootDocumentProjection():
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "The root document must be materialized"
	case strings.HasPrefix(projection.Field, "_meta/") && projection.Inference.IsSingleType():
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "Metadata fields fields are able to be materialized"
	case projection.Inference.IsSingleScalarType() || flatType == INTEGER || flatType == NUMBER:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The projection has a single scalar type"

	case projection.Inference.IsSingleType():
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "This field is able to be materialized"
	default:
		// If we got here, then either the field may have multiple incompatible types, or the
		// only possible type is "null". In either case, we're not going to allow it.
		// Technically, we could allow the null type to be materialized, but I can't think of a
		// use case where that would be desirable.
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize this field"
	}
	return constraint
}

// ValidateMatchesExisting returns a set of constraints to use when there is a new proposed
// CollectionSpec for a materialization that is already running, or has been Applied. The returned
// constraints will check type compatibility of projections that exist both in
// the new and the existing binding, and give new constraints for additionally
// supplied fields. Additional requires that keys from the existing binding must
// be present in the proposed spec as well.
func ValidateMatchesExisting(resource Resource, existing *pf.MaterializationSpec_Binding, proposed *pf.CollectionSpec) map[string]*pm.Response_Validated_Constraint {
	var constraints = make(map[string]*pm.Response_Validated_Constraint)

	for _, field := range existing.FieldSelection.Keys {
		var constraint = new(pm.Response_Validated_Constraint)
		constraint.Type = pm.Response_Validated_Constraint_FIELD_REQUIRED
		constraint.Reason = "This field is a key in the current materialization"
		constraints[field] = constraint
	}

	for _, field := range existing.FieldSelection.AllFields() {
		// we already have a constraint for this field
		if _, ok := constraints[field]; ok {
			continue
		}

		var constraint = new(pm.Response_Validated_Constraint)
		// If a field has been removed from the proposed projection, we will migrate
		// it at ApplyUpsert
		if proposed.GetProjection(field) == nil {
			continue
		}
		var typeError = checkTypeError(field, &existing.Collection, proposed)
		if len(typeError) > 0 {
			constraint.Type = pm.Response_Validated_Constraint_UNSATISFIABLE
			constraint.Reason = typeError
		} else {
			constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
			constraint.Reason = "This location is part of the current materialization"
		}

		constraints[field] = constraint
	}
	// We'll loop through the proposed projections and create a new constraint for
	// fields that are not among our existing binding projections
	for _, proj := range proposed.Projections {
		if _, ok := constraints[proj.Field]; !ok {
			var constraint = validateNewProjection(resource, proj)
			constraints[proj.Field] = constraint
		}
	}

	return constraints
}

func checkTypeError(field string, existing *pf.CollectionSpec, proposed *pf.CollectionSpec) string {
	var existingProjection = existing.GetProjection(field)
	var proposedProjection = proposed.GetProjection(field)
	var existingEffectiveType = effectiveJsonTypes(existingProjection)

	// Ensure that the possible types of the proposed are compatible with the possible types of the
	// existing. The new projection is always allowed to contain fewer types than the original since
	// that will always work with the original database schema. Additional proposed types may be
	// compatible if they do not alter the effective FlatType, such as adding a string formatted as
	// an integer to an existing integer type.
	for _, pt := range effectiveJsonTypes(proposedProjection) {
		if !slices.Contains(existingEffectiveType, pt) && pt != "null" {
			return fmt.Sprintf("The existing materialization for field %q of collection %q has type %q, but the proposed update requires the field type to be %q which is not compatible. Please consider materializing to a new table.", field, existing.Name, strings.Join(existingEffectiveType, ","), pt)
		}
	}

	return ""
}
