package boilerplate

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

// Constrainter represents the endpoint requirements for validating new projections or changes to
// existing projections.
type Constrainter interface {
	// NewConstraints produces constraints for a projection that is not part of the current
	// materialization.
	NewConstraints(p *pf.Projection, deltaUpdates bool) *pm.Response_Validated_Constraint

	// Compatible reports whether an existing materialized field in a destination system (such as a
	// column in a table) is compatible with a proposed projection.
	Compatible(existing EndpointField, proposed *pf.Projection, rawFieldConfig json.RawMessage) (bool, error)

	// DescriptionForType produces a human-readable description for a type, which is used in
	// constraint descriptions when there is an incompatible type change.
	DescriptionForType(p *pf.Projection) string
}

// Validator performs validation of a materialization binding.
type Validator struct {
	c  Constrainter
	is *InfoSchema
}

func NewValidator(c Constrainter, is *InfoSchema) Validator {
	return Validator{
		c:  c,
		is: is,
	}
}

// ValidateBinding calculates the constraints for a new binding or a change to an existing binding.
func (v Validator) ValidateBinding(
	path []string,
	deltaUpdates bool,
	backfill uint32,
	boundCollection pf.CollectionSpec,
	fieldConfigJsonMap map[string]json.RawMessage,
	storedSpec *pf.MaterializationSpec,
) (map[string]*pm.Response_Validated_Constraint, error) {
	existingBinding, err := findExistingBinding(path, boundCollection.Name, storedSpec)
	if err != nil {
		return nil, err
	}

	if existingBinding != nil && existingBinding.Backfill > backfill {
		// Sanity check: Don't allow backfill counters to decrease.
		// TODO(whb): This check will soon be moved to the control plane, and then can be removed
		// from here.
		return nil, fmt.Errorf(
			"backfill count %d is less than previously applied count of %d",
			backfill,
			existingBinding.Backfill,
		)
	}

	var constraints map[string]*pm.Response_Validated_Constraint
	if !v.is.HasResource(path) || (existingBinding != nil && backfill != existingBinding.Backfill) {
		// Always validate as a new table if the existing table doesn't exist, since there is no
		// existing table to be incompatible with. Also validate as a new table if we are going to
		// be replacing the table.
		constraints = v.validateNewBinding(boundCollection, deltaUpdates)
	} else {
		if existingBinding != nil && existingBinding.DeltaUpdates && !deltaUpdates {
			// We allow a binding to switch from standard => delta updates but not the other
			// way. This is because a standard materialization is trivially a valid
			// delta-updates materialization.
			return nil, fmt.Errorf("changing from delta updates to standard updates is not allowed")
		}
		constraints, err = v.validateMatchesExistingBinding(existingBinding, path, boundCollection, deltaUpdates, fieldConfigJsonMap)
		if err != nil {
			return nil, err
		}
	}

	return v.forbidAmbiguousFields(constraints), nil
}

func (v Validator) validateNewBinding(boundCollection pf.CollectionSpec, deltaUpdates bool) map[string]*pm.Response_Validated_Constraint {
	constraints := make(map[string]*pm.Response_Validated_Constraint)

	sawRoot := false

	for _, projection := range boundCollection.Projections {
		if projection.IsRootDocumentProjection() {
			if sawRoot && !deltaUpdates {
				constraints[projection.Field] = &pm.Response_Validated_Constraint{
					Type:   pm.Response_Validated_Constraint_FIELD_FORBIDDEN,
					Reason: "Only a single root document projection can be materialized for standard updates",
				}
				continue
			}
			sawRoot = true
		}

		constraints[projection.Field] = v.c.NewConstraints(&projection, deltaUpdates)
	}
	return constraints
}

func (v Validator) validateMatchesExistingBinding(
	existing *pf.MaterializationSpec_Binding,
	path []string,
	boundCollection pf.CollectionSpec,
	deltaUpdates bool,
	fieldConfigJsonMap map[string]json.RawMessage,
) (map[string]*pm.Response_Validated_Constraint, error) {
	constraints := make(map[string]*pm.Response_Validated_Constraint)

	fields, err := v.is.FieldsForResource(path)
	if err != nil {
		// This should never error, since it has previously been established that the resource does
		// exist in the destination system.
		return nil, fmt.Errorf("getting fields for existing resource: %w", err)
	}

	for _, f := range fields {
		constraint := new(pm.Response_Validated_Constraint)

		proposedProjection, inCollection, err := v.is.ExtractProjection(f.Name, boundCollection)
		if err != nil {
			return nil, fmt.Errorf("extracting projection for existing endpoint field %q: %w", f.Name, err)
		}

		if !inCollection {
			// A field that exists in the destination but not in the collection is not a problem,
			// but Apply may need to make the destination field not required later.
			continue
		} else if !deltaUpdates && proposedProjection.IsRootDocumentProjection() {
			// Root document constraints are handled as a separate loop, further down.
			continue
		}

		// Is the proposed type completely disallowed by the materialization? This differs from
		// being UNSATISFIABLE, which implies that re-creating the materialization could resolve the
		// difference.
		if c := v.c.NewConstraints(&proposedProjection, deltaUpdates); c.Type == pm.Response_Validated_Constraint_FIELD_FORBIDDEN {
			constraints[proposedProjection.Field] = c
			continue
		}

		if compatible, err := v.c.Compatible(f, &proposedProjection, fieldConfigJsonMap[proposedProjection.Field]); err != nil {
			return nil, fmt.Errorf("determining compatibility for endpoint field %q vs. selected field %q: %w", f.Name, proposedProjection.Field, err)
		} else if compatible {
			if proposedProjection.IsPrimaryKey {
				constraint.Type = pm.Response_Validated_Constraint_FIELD_REQUIRED
				constraint.Reason = "This field is a key in the current materialization"
			} else {
				// TODO(whb): Really this should be "FIELD_RECOMMENDED", but that is not a
				// constraint that has been implemented currently. This would be an issue if there
				// are multiple projections of the same location.
				constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
				constraint.Reason = "This location is part of the current materialization"
			}
		} else {
			constraint.Type = pm.Response_Validated_Constraint_UNSATISFIABLE
			constraint.Reason = fmt.Sprintf(
				"Field '%s' is already being materialized as endpoint type '%s' and cannot be changed to type '%s'",
				proposedProjection.Field,
				f.Type,
				v.c.DescriptionForType(&proposedProjection),
			)
		}

		constraints[proposedProjection.Field] = constraint
	}

	docFields := []string{}
	for _, p := range boundCollection.Projections {
		if !deltaUpdates && p.IsRootDocumentProjection() {
			docFields = append(docFields, p.Field)
			// Only the originally selected root document projection is allowed to be selected for
			// changes to a standard updates materialization. If there is no previously persisted
			// spec, the first root document projection is selected as the root document.
			if (existing != nil && p.Field == existing.FieldSelection.Document) || (existing == nil && len(docFields) == 1) {
				constraints[p.Field] = &pm.Response_Validated_Constraint{
					Type:   pm.Response_Validated_Constraint_FIELD_REQUIRED,
					Reason: "This field is the document in the current materialization",
				}
			} else {
				constraints[p.Field] = &pm.Response_Validated_Constraint{
					Type: pm.Response_Validated_Constraint_FIELD_FORBIDDEN,
					Reason: fmt.Sprintf(
						"Cannot materialize root document projection '%s' because field '%s' is already being materialized as the document",
						p.Field,
						func() string {
							if existing != nil {
								return existing.FieldSelection.Document
							} else {
								return docFields[0]
							}
						}(),
					),
				}
			}
		}

		// Build constraints for new projections of the binding.
		if _, ok := constraints[p.Field]; !ok {
			constraints[p.Field] = v.c.NewConstraints(&p, deltaUpdates)
		}
	}

	if existing != nil && !deltaUpdates && !slices.Contains(docFields, existing.FieldSelection.Document) {
		// For standard updates, the proposed binding must still have the original document field
		// from a prior specification, if that's known. If it doesn't, make sure to fail the build
		// with a constraint on a root document projection that it does have.
		constraints[docFields[0]] = &pm.Response_Validated_Constraint{
			Type: pm.Response_Validated_Constraint_UNSATISFIABLE,
			Reason: fmt.Sprintf(
				"The root document must be materialized as field '%s'",
				existing.FieldSelection.Document,
			),
		}
	}

	return constraints, nil
}

func (v Validator) forbidAmbiguousFields(constraints map[string]*pm.Response_Validated_Constraint) map[string]*pm.Response_Validated_Constraint {
	// Fields are "ambiguous" if their destination system treats more than one Flow collection field
	// name as the same materialized field name. For example, if a destination system is strictly
	// case-insensitive and lowercases all field names, `ThisField` and `thisField` are ambiguous
	// and cannot be materialized. If there is a set of ambiguous fields, we forbid all of them from
	// being materialized, and suggest an alternate projection be used instead.
	ambiguousFields := make(map[string][]string) // Translated endpoint field name -> List of Flow collection field names
	for field := range constraints {
		efn := v.is.translateField(field)
		ambiguousFields[efn] = append(ambiguousFields[efn], field)
	}
	for efn, flowFields := range ambiguousFields {
		if len(flowFields) == 1 {
			// No ambiguity, since only a single Flow field translates to this endpoint field name.
			continue
		}

		for _, f := range flowFields {
			constraints[f] = &pm.Response_Validated_Constraint{
				Type: pm.Response_Validated_Constraint_FIELD_FORBIDDEN,
				Reason: fmt.Sprintf(
					"Flow collection field '%s' would be materialized `%s`, which is ambiguous with the materializations for other Flow collection fields [%s]. Consider using an alternate, unambiguous projection of this field to allow it to be materialized.",
					f,
					efn,
					strings.Join(flowFields, ","),
				)}
		}
	}

	return constraints
}

// findExistingBinding locates a binding within an existing stored specification, and verifies that
// there are no target path conflicts with a proposed binding that does not already exist.
func findExistingBinding(
	resourcePath []string,
	proposedCollection pf.Collection,
	storedSpec *pf.MaterializationSpec,
) (*pf.MaterializationSpec_Binding, error) {
	if storedSpec == nil {
		return nil, nil // Binding is trivially not found
	}
	for _, existingBinding := range storedSpec.Bindings {
		if existingBinding.Collection.Name == proposedCollection && slices.Equal(resourcePath, existingBinding.ResourcePath) {
			// The binding already exists for this collection and is being materialized to the
			// target.
			return existingBinding, nil
		} else if slices.Equal(resourcePath, existingBinding.ResourcePath) {
			// There is a binding already materializing to the target, but for a different
			// collection.
			return nil, fmt.Errorf(
				"cannot add a new binding to materialize collection '%s' to '%s' because an existing binding for collection '%s' is already materializing to '%s'",
				proposedCollection.String(),
				resourcePath,
				existingBinding.Collection.Name,
				resourcePath,
			)
		}
	}
	return nil, nil
}

type StringWithNumericFormat string

const (
	StringFormatInteger StringWithNumericFormat = "stringFormatInteger"
	StringFormatNumber  StringWithNumericFormat = "stringFormatNumber"
)

func AsFormattedNumeric(projection *pf.Projection) (StringWithNumericFormat, bool) {
	typesMatch := func(actual, allowed []string) bool {
		for _, t := range actual {
			if !slices.Contains(allowed, t) {
				return false
			}
		}
		return true
	}

	if !projection.IsPrimaryKey && projection.Inference.String_ != nil {
		switch {
		case projection.Inference.String_.Format == "integer" && typesMatch(projection.Inference.Types, []string{"integer", "null", "string"}):
			return StringFormatInteger, true
		case projection.Inference.String_.Format == "number" && typesMatch(projection.Inference.Types, []string{"null", "number", "string"}):
			return StringFormatNumber, true
		default:
			// Fallthrough.
		}
	}

	// Not a formatted numeric field.
	return "", false
}
