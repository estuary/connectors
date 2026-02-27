package boilerplate

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

// Constrainter represents the endpoint requirements for validating new projections or changes to
// existing projections.
type Constrainter interface {
	// NewConstraints produces constraints for a projection that is not part of the current
	// materialization.
	NewConstraints(p *pf.Projection, deltaUpdates bool, rawFieldConfig json.RawMessage) (*pm.Response_Validated_Constraint, error)

	// Compatible reports whether an existing materialized field in a destination system (such as a
	// column in a table) is compatible with a proposed projection.
	Compatible(existing ExistingField, proposed *pf.Projection, rawFieldConfig json.RawMessage) (bool, error)

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

	// caseInsensitiveFields indicates if fields that differ only in capitalization will
	// conflict in the materialized resource. This is passed to the protocol-level
	// case_insensitive_fields setting in validated binding responses.
	caseInsensitiveFields bool

	// featureFlags contains feature flags that control validation behavior.
	featureFlags map[string]bool
}

func NewValidator(
	c Constrainter,
	is *InfoSchema,
	maxFieldLength int,
	caseInsensitiveFields bool,
	featureFlags map[string]bool,
) Validator {
	return Validator{
		c:                     c,
		is:                    is,
		maxFieldLength:        maxFieldLength,
		caseInsensitiveFields: caseInsensitiveFields,
		featureFlags:          featureFlags,
	}
}

// ValidateBinding calculates the constraints for a new binding or a change to an existing binding.
func (v Validator) ValidateBinding(
	path []string,
	deltaUpdates bool,
	backfill uint32,
	boundCollection pf.CollectionSpec,
	fieldConfigJsonMap map[string]json.RawMessage,
	lastSpec *pf.MaterializationSpec,
) (map[string]*pm.Response_Validated_Constraint, error) {
	lastBinding := findLastBinding(path, lastSpec)
	existingResource := v.is.GetResource(path)

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
			return nil, fmt.Errorf("cannot materialize collection '%s' with nullable key field '%s' unless it has a default value annotation", boundCollection.Name.String(), p.Field)
		}
	}

	// Check if this is a new binding (no lastBinding) but the table already exists (existingResource != nil).
	// This is a case we want to prevent unless its feature flag allows it.
	if lastBinding == nil && existingResource != nil {
		if !v.featureFlags["allow_existing_tables_for_new_bindings"] {
			return nil, fmt.Errorf(
				"table %v already exists for new binding %q. "+
					"You must drop this table to continue. "+
					"A 'allow_existing_tables_for_new_bindings' feature flag is available but has several caveats: "+
					"contact Estuary support to determine if it's appropriate for your use case",
				path,
				boundCollection.Name.String(),
			)
		}
	}

	var err error
	var constraints map[string]*pm.Response_Validated_Constraint
	if existingResource == nil || (lastBinding != nil && backfill != lastBinding.Backfill) {
		// Always validate as a new table if the existing table doesn't exist, since there is no
		// existing table to be incompatible with. Also validate as a new table if we are going to
		// be replacing the table.
		if constraints, err = v.validateNewBinding(boundCollection, deltaUpdates, fieldConfigJsonMap); err != nil {
			return nil, err
		}
	} else {
		if lastBinding != nil && lastBinding.DeltaUpdates && !deltaUpdates {
			// We allow a binding to switch from standard => delta updates but not the other
			// way. This is because a standard materialization is trivially a valid
			// delta-updates materialization.
			return nil, fmt.Errorf("Changing from delta updates to standard updates is not allowed. Please click the backfill button first before disabling delta updates.")
		} else if constraints, err = v.validateMatchesExistingResource(*existingResource, lastBinding, boundCollection, deltaUpdates, fieldConfigJsonMap); err != nil {
			return nil, err
		}
	}

	// Set folded field for each constraint where field translation changes the field name.
	// This applies to both new and existing fields, ensuring complete coverage.
	for field, constraint := range constraints {
		if t := v.is.translateField(field); t != field {
			constraint.FoldedField = t
		}
	}

	return forbidLongFields(v.maxFieldLength, boundCollection, constraints)
}

func (v Validator) validateNewBinding(
	boundCollection pf.CollectionSpec,
	deltaUpdates bool,
	fieldConfigJsonMap map[string]json.RawMessage,
) (map[string]*pm.Response_Validated_Constraint, error) {
	constraints := make(map[string]*pm.Response_Validated_Constraint)

	sawRoot := false

	for _, p := range boundCollection.Projections {
		c, err := v.c.NewConstraints(&p, deltaUpdates, fieldConfigJsonMap[p.Field])
		if err != nil {
			return nil, err
		}

		if p.IsRootDocumentProjection() {
			if sawRoot && !deltaUpdates {
				c = &pm.Response_Validated_Constraint{
					Type:   pm.Response_Validated_Constraint_FIELD_FORBIDDEN,
					Reason: "Only a single root document projection can be materialized for standard updates",
				}
			}
			sawRoot = true
		} else if !deltaUpdates && p.Ptr == boundCollection.Key[0] && isOptional(c.Type) {
			// The _first_ key location is always required, but others may be optional if the connector allows it
			c = &pm.Response_Validated_Constraint{
				Type:   pm.Response_Validated_Constraint_LOCATION_REQUIRED,
				Reason: "The first collection key component is required to be included for standard updates",
			}
		}

		constraints[p.Field] = c

	}
	return constraints, nil
}

func isOptional(c pm.Response_Validated_Constraint_Type) bool {
	return c == pm.Response_Validated_Constraint_LOCATION_RECOMMENDED ||
		c == pm.Response_Validated_Constraint_FIELD_OPTIONAL
}

func (v Validator) validateMatchesExistingResource(
	existingResource ExistingResource,
	lastBinding *pf.MaterializationSpec_Binding,
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
		c, err := v.c.NewConstraints(&p, deltaUpdates, fieldConfigJsonMap[p.Field])
		if err != nil {
			return nil, err
		}

		if c.Type == pm.Response_Validated_Constraint_FIELD_FORBIDDEN {
			// Is the proposed type completely disallowed by the materialization? This differs from
			// being INCOMPATIBLE, which implies that re-creating the materialization could resolve
			// the difference.
		} else if !deltaUpdates && p.IsRootDocumentProjection() {
			docFields = append(docFields, p.Field)
			// Only the originally selected root document projection is allowed to be selected for
			// changes to a standard updates materialization. If there is no previously persisted
			// spec, the first root document projection is selected as the root document.
			if (lastBinding != nil && p.Field == lastBinding.FieldSelection.Document) || (lastBinding == nil && len(docFields) == 1) {
				c = &pm.Response_Validated_Constraint{
					Type:   pm.Response_Validated_Constraint_FIELD_REQUIRED,
					Reason: "This field is the document in the current materialization",
				}

				// Do not allow incompatible type changes to the root document
				// field if it has already been materialized.
				if existingField := existingResource.GetField(p.Field); existingField != nil {
					if constraintFromExisting, err := v.constraintForExistingField(boundCollection, p, *existingField, fieldConfigJsonMap); err != nil {
						return nil, err
					} else if constraintFromExisting.Type.IsForbidden() {
						c = constraintFromExisting
					}
				}
			} else if lastBinding != nil && lastBinding.FieldSelection.Document == "" {
				c = &pm.Response_Validated_Constraint{
					Type:   pm.Response_Validated_Constraint_INCOMPATIBLE,
					Reason: "Cannot add a new root document projection to materialization without backfilling",
				}
			} else {
				c = &pm.Response_Validated_Constraint{
					Type: pm.Response_Validated_Constraint_FIELD_FORBIDDEN,
					Reason: fmt.Sprintf(
						"Cannot materialize root document projection '%s' because field '%s' is already being materialized as the document",
						p.Field,
						func() string {
							if lastBinding != nil {
								return lastBinding.FieldSelection.Document
							} else {
								return docFields[0]
							}
						}(),
					),
				}
			}
		} else if !deltaUpdates && p.IsPrimaryKey && lastBinding != nil && !slices.Contains(lastBinding.FieldSelection.Keys, p.Field) {
			// Locations that are part of the primary key may not be added without backfilling the binding
			c = &pm.Response_Validated_Constraint{
				Type:   pm.Response_Validated_Constraint_FIELD_FORBIDDEN,
				Reason: "Cannot add a new key location to the field selection of an existing non-delta-updates materialization without backfilling",
			}
			if lastBinding != nil && slices.Contains(lastBinding.FieldSelection.AllFields(), p.Field) {
				if existingField := existingResource.GetField(p.Field); existingField != nil {
					if c, err = v.constraintForExistingField(boundCollection, p, *existingField, fieldConfigJsonMap); err != nil {
						return nil, fmt.Errorf("evaluating constraint for existing field: %w", err)
					}
				} else {
					c = &pm.Response_Validated_Constraint{
						Type:   pm.Response_Validated_Constraint_LOCATION_RECOMMENDED,
						Reason: "This location is part of the current materialization",
					}
				}
			}
		} else if existingField := existingResource.GetField(p.Field); existingField != nil {
			// All other fields that are already being materialized.
			if c, err = v.constraintForExistingField(boundCollection, p, *existingField, fieldConfigJsonMap); err != nil {
				return nil, err
			}
		}

		// Continue to recommended any optional fields that were included in a prior spec's
		// field selection, even if the materialized field is not reported to exist in the
		// destination. This is primarily to produce more useful constraints for systems that do
		// not have "columns" that can be inspected, but still support field selection (ex:
		// materialize-dynamodb), so that selected fields can continue to be recommended.
		if lastBinding != nil &&
			c.Type == pm.Response_Validated_Constraint_FIELD_OPTIONAL &&
			slices.Contains(lastBinding.FieldSelection.AllFields(), p.Field) {
			c = &pm.Response_Validated_Constraint{
				Type:   pm.Response_Validated_Constraint_LOCATION_RECOMMENDED,
				Reason: "This location is part of the current materialization",
			}
		}

		constraints[p.Field] = c
	}

	if lastBinding != nil && !deltaUpdates && lastBinding.FieldSelection.Document != "" && !slices.Contains(docFields, lastBinding.FieldSelection.Document) {
		// For standard updates, the proposed binding must still have the original document field
		// from a prior specification, if that's known. If it doesn't, make sure to fail the build
		// with a constraint on a root document projection that it does have.
		constraints[docFields[0]] = &pm.Response_Validated_Constraint{
			Type: pm.Response_Validated_Constraint_INCOMPATIBLE,
			Reason: fmt.Sprintf(
				"The root document must be materialized as field '%s'",
				lastBinding.FieldSelection.Document,
			),
		}
	}

	return constraints, nil
}

func (v Validator) constraintForExistingField(
	boundCollection pf.CollectionSpec,
	p pf.Projection,
	existingField ExistingField,
	fieldConfigJsonMap map[string]json.RawMessage,
) (*pm.Response_Validated_Constraint, error) {
	var out *pm.Response_Validated_Constraint

	rawConfig := fieldConfigJsonMap[p.Field]
	if compatible, err := v.c.Compatible(existingField, &p, rawConfig); err != nil {
		return nil, fmt.Errorf("determining compatibility for endpoint field %q vs. selected field %q: %w", existingField.Name, p.Field, err)
	} else if compatible {
		if p.IsPrimaryKey {
			out = &pm.Response_Validated_Constraint{
				Type:   pm.Response_Validated_Constraint_FIELD_REQUIRED,
				Reason: "This field is a key in the current materialization",
			}
		} else {
			// TODO(whb): Really this should be "FIELD_RECOMMENDED", but that is not a
			// constraint that has been implemented currently. This would be an issue if there
			// are multiple projections of the same location.
			out = &pm.Response_Validated_Constraint{
				Type:   pm.Response_Validated_Constraint_LOCATION_RECOMMENDED,
				Reason: "This location is part of the current materialization",
			}
		}
	} else {
		newDesc, err := v.c.DescriptionForType(&p, rawConfig)
		if err != nil {
			return nil, fmt.Errorf("getting description for field %q of bound collection %q: %w", p.Field, boundCollection.Name.String(), err)
		}

		existingType := strings.ToUpper(existingField.Type)
		if existingField.CharacterMaxLength > 0 {
			existingType += fmt.Sprintf("(%d)", existingField.CharacterMaxLength)
		}

		out = &pm.Response_Validated_Constraint{
			Type: pm.Response_Validated_Constraint_INCOMPATIBLE,
			Reason: fmt.Sprintf(
				"Field '%s' is already being materialized as endpoint type '%s' but endpoint type '%s' is required by its schema '%s'",
				p.Field,
				existingType,
				strings.ToUpper(newDesc),
				fieldSchema(p),
			),
		}
	}

	return out, nil
}

// findLastBinding locates a binding within a previously applied or validated specification.
func findLastBinding(resourcePath []string, lastSpec *pf.MaterializationSpec) *pf.MaterializationSpec_Binding {
	if lastSpec == nil {
		return nil // Binding is trivially not found
	}
	for _, existingBinding := range lastSpec.Bindings {
		if slices.Equal(resourcePath, existingBinding.ResourcePath) {
			return existingBinding
		}
	}

	// For the purposes of Validate and Apply actions, a binding that was
	// previously disabled should be considered as the "last binding" for
	// evaluating the current (enabled) binding. This ensures that bindings are
	// correctly backfilled and invalid configuration transitions are not
	// allowed, even if the binding was disabled and then is re-enabled.
	for _, inactiveBinding := range lastSpec.InactiveBindings {
		if slices.Equal(resourcePath, inactiveBinding.ResourcePath) {
			return inactiveBinding
		}
	}

	return nil
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
		if p.Inference.String_.MaxLength > 0 {
			out.WriteString(", maxLength: " + strconv.FormatUint(uint64(p.Inference.String_.MaxLength), 10))
		}
	}

	out.WriteString(" }")

	return out.String()
}
