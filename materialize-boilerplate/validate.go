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
	DescriptionForType(p *pf.Projection, rawFieldConfig json.RawMessage) (string, error)
}

// Validator performs validation of a materialization binding.
type Validator struct {
	c  Constrainter
	is *InfoSchema
	// maxFieldLength is used to produce "forbidden" constraints on fields that are too long. If
	// maxFieldLength is 0, no constraints are enforced. The length of a field name is in terms of
	// characters, not bytes.
	maxFieldLength int

	// CaseInsensitiveFields is used to indicate if fields that differ only in capitalization will
	// conflict in the materialized resource. For example, "thisfield" and "thisField" may have
	// their capitalization preserved from the information_schema view (or equivalent), but the
	// materialized resource creation will still result in an error due to conflicts.
	caseInsensitiveFields bool
}

func NewValidator(
	c Constrainter,
	is *InfoSchema,
	maxFieldLength int,
	caseInsensitiveFields bool,
) Validator {
	return Validator{
		c:                     c,
		is:                    is,
		maxFieldLength:        maxFieldLength,
		caseInsensitiveFields: caseInsensitiveFields,
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
	existingBinding, err := findExistingBinding(path, storedSpec)
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

	for _, p := range boundCollection.Projections {
		// Don't allow collection keys to be nullable unless they have a default value set. If a
		// default value is set, there will always be a value provided for the field from the
		// runtime.
		if !p.IsPrimaryKey {
			continue
		}

		mustExist := p.Inference.Exists == pf.Inference_MUST && !slices.Contains(p.Inference.Types, "null")
		hasDefault := p.Inference.DefaultJson != nil

		if !mustExist && !hasDefault {
			return nil, fmt.Errorf("cannot materialize collection with nullable key field '%s' unless it has a default value annotation", p.Field)
		}
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

	return forbidLongFields(v.maxFieldLength, boundCollection, constraints)
}

func (v Validator) validateNewBinding(boundCollection pf.CollectionSpec, deltaUpdates bool) map[string]*pm.Response_Validated_Constraint {
	constraints := make(map[string]*pm.Response_Validated_Constraint)

	sawRoot := false

	for _, p := range boundCollection.Projections {
		c := v.c.NewConstraints(&p, deltaUpdates)

		if p.IsRootDocumentProjection() {
			if sawRoot && !deltaUpdates {
				c = &pm.Response_Validated_Constraint{
					Type:   pm.Response_Validated_Constraint_FIELD_FORBIDDEN,
					Reason: "Only a single root document projection can be materialized for standard updates",
				}
			}
			sawRoot = true
		} else if ambiguousFields := v.ambiguousFields(p, boundCollection.Projections); len(ambiguousFields) > 0 && c.Type == pm.Response_Validated_Constraint_LOCATION_RECOMMENDED {
			// Any fields that would be ambiguous to materialize should be marked as optional if
			// they would otherwise be recommended. Only one of these fields should be selected to
			// materialize.
			c = &pm.Response_Validated_Constraint{
				Type: pm.Response_Validated_Constraint_FIELD_OPTIONAL,
				Reason: fmt.Sprintf(
					// See identical "reason" text in validateMatchesExistingBinding for optional
					// ambiguous field constraints. These two messages should be kept in sync.
					"Flow collection field '%s' would be materialized as '%s', which is ambiguous with fields [%s]. Only a single field from this set should be selected. Consider using alternate projections if you want to materialize more than one of these fields",
					p.Field,
					v.is.translateField(p.Field),
					strings.Join(ambiguousFields, ","),
				),
			}
		}

		constraints[p.Field] = c

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

	docFields := []string{}
	for _, p := range boundCollection.Projections {
		// Base constraint used for all new projections of the binding. This may be re-evaluated
		// below, depending on the details of the projection and any pre-existing materialization of
		// it.
		c := v.c.NewConstraints(&p, deltaUpdates)

		if c.Type == pm.Response_Validated_Constraint_FIELD_FORBIDDEN {
			// Is the proposed type completely disallowed by the materialization? This differs from
			// being UNSATISFIABLE, which implies that re-creating the materialization could resolve
			// the difference.
		} else if !deltaUpdates && p.IsRootDocumentProjection() {
			docFields = append(docFields, p.Field)
			// Only the originally selected root document projection is allowed to be selected for
			// changes to a standard updates materialization. If there is no previously persisted
			// spec, the first root document projection is selected as the root document.
			if (existing != nil && p.Field == existing.FieldSelection.Document) || (existing == nil && len(docFields) == 1) {
				c = &pm.Response_Validated_Constraint{
					Type:   pm.Response_Validated_Constraint_FIELD_REQUIRED,
					Reason: "This field is the document in the current materialization",
				}
			} else {
				c = &pm.Response_Validated_Constraint{
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
		} else if ambiguousFields := v.ambiguousFields(p, boundCollection.Projections); len(ambiguousFields) > 0 {
			// Projections that would result in ambiguous materialized fields are forbidden if a
			// different ambiguous projection has already been selected, or optional if none have
			// yet been selected.
			if existing != nil && slices.Contains(existing.FieldSelection.AllFields(), p.Field) {
				// This field has already been selected as the ambiguous field to materialize.
				c = &pm.Response_Validated_Constraint{
					Type:   pm.Response_Validated_Constraint_LOCATION_RECOMMENDED,
					Reason: "This location is part of the current materialization",
				}
			} else if existing != nil && v.ambiguousFieldIsSelected(p, existing.FieldSelection.AllFields()) {
				// A different field has been selected, so this one can't be.
				c = &pm.Response_Validated_Constraint{
					Type: pm.Response_Validated_Constraint_FIELD_FORBIDDEN,
					Reason: fmt.Sprintf(
						"Flow collection field '%s' is ambiguous with fields already being materialized as '%s' in the destination. Consider using an alternate, unambiguous projection of this field to allow it to be materialized",
						p.Field,
						v.is.translateField(p.Field),
					),
				}
			} else if c.Type == pm.Response_Validated_Constraint_LOCATION_RECOMMENDED || c.Type == pm.Response_Validated_Constraint_FIELD_OPTIONAL {
				// None of these ambiguous fields have been selected yet, so it's still possible to
				// pick one.
				c = &pm.Response_Validated_Constraint{
					Type: pm.Response_Validated_Constraint_FIELD_OPTIONAL,
					Reason: fmt.Sprintf(
						// See identical "reason" text in validateNewBinding for optional ambiguous
						// field constraints. These two messages should be kept in sync.
						"Flow collection field '%s' would be materialized as '%s', which is ambiguous with fields [%s]. Only a single field from this set should be selected. Consider using alternate projections if you want to materialize more than one of these fields",
						p.Field,
						v.is.translateField(p.Field),
						strings.Join(ambiguousFields, ","),
					),
				}
			}
		} else if existingField, err := v.is.GetField(path, p.Field); err == nil {
			// All other fields that are already being materialized. Any error from GetField is
			// because the field does not already exist.
			rawConfig := fieldConfigJsonMap[p.Field]
			if compatible, err := v.c.Compatible(existingField, &p, rawConfig); err != nil {
				return nil, fmt.Errorf("determining compatibility for endpoint field %q vs. selected field %q: %w", existingField.Name, p.Field, err)
			} else if compatible {
				if p.IsPrimaryKey {
					c = &pm.Response_Validated_Constraint{
						Type:   pm.Response_Validated_Constraint_FIELD_REQUIRED,
						Reason: "This field is a key in the current materialization",
					}
				} else {
					// TODO(whb): Really this should be "FIELD_RECOMMENDED", but that is not a
					// constraint that has been implemented currently. This would be an issue if there
					// are multiple projections of the same location.
					c = &pm.Response_Validated_Constraint{
						Type:   pm.Response_Validated_Constraint_LOCATION_RECOMMENDED,
						Reason: "This location is part of the current materialization",
					}
				}
			} else {
				newDesc, err := v.c.DescriptionForType(&p, rawConfig)
				if err != nil {
					return nil, fmt.Errorf("getting description for field %q of bound collection %q: %w", p.Field, boundCollection.Name.String(), err)
				}

				c = &pm.Response_Validated_Constraint{
					Type: pm.Response_Validated_Constraint_UNSATISFIABLE,
					Reason: fmt.Sprintf(
						"Field '%s' is already being materialized as endpoint type '%s' but endpoint type '%s' is required by its schema '%s'",
						p.Field,
						strings.ToUpper(existingField.Type),
						strings.ToUpper(newDesc),
						fieldSchema(p),
					),
				}
			}
		}

		// Continue to recommended any optional fields that were included in a prior spec's
		// field selection, even if the materialized field is not reported to exist in the
		// destination. This is primarily to produce more useful constraints for systems that do
		// not have "columns" that can be inspected, but still support field selection (ex:
		// materialize-dynamodb), so that selected fields can continue to be recommended.
		if existing != nil &&
			c.Type == pm.Response_Validated_Constraint_FIELD_OPTIONAL &&
			slices.Contains(existing.FieldSelection.AllFields(), p.Field) {
			c = &pm.Response_Validated_Constraint{
				Type:   pm.Response_Validated_Constraint_LOCATION_RECOMMENDED,
				Reason: "This location is part of the current materialization",
			}
		}

		constraints[p.Field] = c
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

// ambiguousFields determines if the given projection is part of a set of projections that would
// result in ambiguous field names in the destination system. Fields are "ambiguous" if their
// destination treats more than one Flow collection field name as the same materialized field name.
// For example, if a destination is strictly case-insensitive and lowercases all field names,
// `ThisField` and `thisField` are ambiguous and cannot be materialized together. If there is a set
// of ambiguous fields, only a single one of them can be materialized.
func (v Validator) ambiguousFields(p pf.Projection, ps []pf.Projection) []string {
	ambiguous := []string{}

	compareFields := func(f1, f2 string) bool {
		f1 = v.is.translateField(f1)
		f2 = v.is.translateField(f2)

		if v.caseInsensitiveFields {
			return strings.EqualFold(f1, f2)
		}

		return f1 == f2
	}

	for _, pp := range ps {
		if p.Field != pp.Field && compareFields(p.Field, pp.Field) {
			ambiguous = append(ambiguous, pp.Field)
		}
	}

	slices.Sort(ambiguous)

	return ambiguous
}

// ambiguousFieldIsSelected returns true if any of the selected fields are ambiguous with the
// provided projection, excluding the projection itself being selected.
func (v Validator) ambiguousFieldIsSelected(p pf.Projection, fieldSelection []string) bool {
	endpointFieldName := v.is.translateField(p.Field)

	for _, f := range fieldSelection {
		if f != p.Field && endpointFieldName == v.is.translateField(f) {
			return true
		}
	}

	return false
}

// findExistingBinding locates a binding within an existing stored specification.
func findExistingBinding(resourcePath []string, storedSpec *pf.MaterializationSpec) (*pf.MaterializationSpec_Binding, error) {
	if storedSpec == nil {
		return nil, nil // Binding is trivially not found
	}
	for _, existingBinding := range storedSpec.Bindings {
		if slices.Equal(resourcePath, existingBinding.ResourcePath) {
			return existingBinding, nil
		}
	}
	return nil, nil
}

// forbidLongFields returns a "forbidden" constraint for fields with names longer than maxLength. It
// will return an error if a field that would otherwise be required has a name that is too long, or
// if all of the projections of a location that is required have names that are too long - at least
// one of the projections of a required location must be able to be materialized. If maxLength is 0
// no restrictions are enforced.
func forbidLongFields(maxLength int, collection pf.CollectionSpec, constraints map[string]*pm.Response_Validated_Constraint) (map[string]*pm.Response_Validated_Constraint, error) {
	if maxLength == 0 {
		return constraints, nil
	}

	requiredLocations := make(map[string]bool)
	for field, constraint := range constraints {
		tooLong := len([]rune(field)) > maxLength

		p := collection.GetProjection(field)

		if constraint.Type == pm.Response_Validated_Constraint_LOCATION_REQUIRED {
			if _, ok := requiredLocations[p.Ptr]; !ok {
				requiredLocations[p.Ptr] = false
			}
			if !tooLong {
				// At least one projection of this required location is not too long to be materialized.
				requiredLocations[p.Ptr] = true
			}
		}

		if !tooLong {
			continue
		}

		if constraint.Type == pm.Response_Validated_Constraint_FIELD_REQUIRED {
			return nil, fmt.Errorf(
				"field '%s' is required to be materialized but has a length of %d which exceeds the maximum length allowable by the destination of %d",
				field,
				len(field),
				maxLength,
			)
		}

		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = fmt.Sprintf(
			"Field '%s' has a length of %d which exceeds the maximum length allowable by the destination of %d. Use an alternate projection with a shorter name to materialize this location",
			field,
			len(field),
			maxLength,
		)
	}

	for location, hasAllowable := range requiredLocations {
		if !hasAllowable {
			return nil, fmt.Errorf(
				"at least one field from location '%s' is required to be materialized, but all projections exceed the maximum length allowable by the destination of %d: must provide an alternate projection with a shorter name",
				location,
				maxLength,
			)
		}
	}

	return constraints, nil
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

func fieldSchema(p pf.Projection) string {
	var out strings.Builder

	out.WriteString("{ type: [" + strings.Join(p.Inference.Types, ", ") + "]")

	if p.Inference.String_ != nil {
		if p.Inference.String_.Format != "" {
			out.WriteString(", format: " + p.Inference.String_.Format)
		}
		if p.Inference.String_.ContentType != "" {
			out.WriteString(", content-type: " + p.Inference.String_.ContentType)
		}
		if p.Inference.String_.ContentEncoding != "" {
			out.WriteString(", content-encoding: " + p.Inference.String_.ContentEncoding)
		}
	}

	out.WriteString(" }")

	return out.String()
}
