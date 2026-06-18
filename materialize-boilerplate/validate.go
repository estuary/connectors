package boilerplate

import (
	"encoding/json"
	"fmt"
	"maps"
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
// Each field may have more than one constraint: notably, a required field whose existing
// materialized column is incompatible with the proposed projection carries both the requiredness
// constraint and the INCOMPATIBLE constraint, so that the control plane fails the build with a
// clear error.
func (v Validator) ValidateBinding(
	path []string,
	deltaUpdates bool,
	backfill uint32,
	boundCollection pf.CollectionSpec,
	fieldConfigJsonMap map[string]json.RawMessage,
	lastSpec *pf.MaterializationSpec,
) (map[string][]*pm.Response_Validated_Constraint, error) {
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
					"An 'allow_existing_tables_for_new_bindings' feature flag is available but has several caveats: "+
					"contact Estuary support to determine if it's appropriate for your use case. https://go.estuary.dev/matff",
				path,
				boundCollection.Name.String(),
			)
		}
	}

	var err error
	var constraints map[string][]*pm.Response_Validated_Constraint
	if existingResource == nil || (lastBinding != nil && backfill != lastBinding.Backfill) {
		// Always validate as a new table if the existing table doesn't exist, since there is no
		// existing table to be incompatible with. Also validate as a new table if we are going to
		// be replacing the table.
		newConstraints, err := v.validateNewBinding(boundCollection, deltaUpdates, fieldConfigJsonMap)
		if err != nil {
			return nil, err
		}
		constraints = make(map[string][]*pm.Response_Validated_Constraint, len(newConstraints))
		for field, c := range newConstraints {
			constraints[field] = []*pm.Response_Validated_Constraint{c}
		}
	} else {
		if lastBinding != nil && lastBinding.DeltaUpdates && !deltaUpdates {
			// We allow a binding to switch from standard => delta updates but not the other
			// way. This is because a standard materialization is trivially a valid
			// delta-updates materialization.
			return nil, fmt.Errorf("changing from delta updates to standard updates is not allowed")
		} else if constraints, err = v.validateMatchesExistingResource(*existingResource, lastBinding, boundCollection, deltaUpdates, fieldConfigJsonMap); err != nil {
			return nil, err
		}
	}

	// Set folded field for each constraint where field translation changes the field name.
	// This applies to both new and existing fields, ensuring complete coverage. All
	// constraints of a field must agree on the folded field, per the protocol.
	for field, cs := range constraints {
		if t := v.is.translateField(field); t != field {
			for _, constraint := range cs {
				constraint.FoldedField = t
			}
		}
	}

	return forbidLongFields(v.maxFieldLength, boundCollection, constraints)
}

// ValidatedConstraints flattens per-field constraint lists into the Validated
// response's projection_constraints representation, which can express multiple
// constraints per field.
func ValidatedConstraints(
	constraints map[string][]*pm.Response_Validated_Constraint,
) []*pm.Response_Validated_ProjectionConstraint {
	list := make([]*pm.Response_Validated_ProjectionConstraint, 0, len(constraints))

	for _, field := range slices.Sorted(maps.Keys(constraints)) {
		for _, c := range constraints[field] {
			list = append(list, &pm.Response_Validated_ProjectionConstraint{Field: field, Constraint: c})
		}
	}

	return list
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
) (map[string][]*pm.Response_Validated_Constraint, error) {
	// Root document projections, in projection order. The first one is the
	// document when there is no prior selection to defer to.
	var docFields []string
	for _, p := range boundCollection.Projections {
		if p.IsRootDocumentProjection() {
			docFields = append(docFields, p.Field)
		}
	}

	// Standard updates require a root document projection: docFields[0] is
	// dereferenced below as the fallback document field, and the protocol does
	// not guarantee its presence.
	if !deltaUpdates && len(docFields) == 0 {
		return nil, fmt.Errorf("collection %q has no root document projection, which is required for a standard-updates materialization", boundCollection.Name.String())
	}

	constraints := make(map[string][]*pm.Response_Validated_Constraint)
	for _, p := range boundCollection.Projections {
		cs, err := v.projectionConstraints(p, existingResource, lastBinding, boundCollection, deltaUpdates, docFields, fieldConfigJsonMap)
		if err != nil {
			return nil, err
		}

		// Continue to recommend an optional field that was part of a prior
		// selection, even when the destination doesn't report it as an existing
		// column (e.g. materialize-dynamodb), so that it stays selected.
		if lastBinding != nil &&
			len(cs) == 1 &&
			cs[0].Type == pm.Response_Validated_Constraint_FIELD_OPTIONAL &&
			slices.Contains(lastBinding.FieldSelection.AllFields(), p.Field) {
			cs = []*pm.Response_Validated_Constraint{{
				Type:   pm.Response_Validated_Constraint_LOCATION_RECOMMENDED,
				Reason: "This location is part of the current materialization",
			}}
		}

		constraints[p.Field] = cs
	}

	if lastBinding != nil && !deltaUpdates && lastBinding.FieldSelection.Document != "" && !slices.Contains(docFields, lastBinding.FieldSelection.Document) {
		// For standard updates, the proposed binding must still have the original document field
		// from a prior specification, if that's known. If it doesn't, make sure to fail the build
		// with a constraint on a root document projection that it does have. The paired
		// LOCATION_REQUIRED constraint guarantees the conflict is surfaced as a clean error.
		constraints[docFields[0]] = []*pm.Response_Validated_Constraint{
			{
				Type:   pm.Response_Validated_Constraint_LOCATION_REQUIRED,
				Reason: "The root document must be materialized for standard updates",
			},
			{
				Type: pm.Response_Validated_Constraint_INCOMPATIBLE,
				Reason: fmt.Sprintf(
					"The root document must be materialized as field '%s'",
					lastBinding.FieldSelection.Document,
				),
			},
		}
	}

	return constraints, nil
}

// projectionConstraints computes the validation constraint(s) for a single
// projection of a binding that updates an existing resource. A field carries a
// single constraint in the common case; a required projection whose existing
// column is incompatible carries both the requiredness and an INCOMPATIBLE
// constraint, so the control plane surfaces a clear conflict.
func (v Validator) projectionConstraints(
	p pf.Projection,
	existingResource ExistingResource,
	lastBinding *pf.MaterializationSpec_Binding,
	boundCollection pf.CollectionSpec,
	deltaUpdates bool,
	docFields []string,
	fieldConfigJsonMap map[string]json.RawMessage,
) ([]*pm.Response_Validated_Constraint, error) {
	// Base constraint, the connector's opinion of the projection on its own.
	base, err := v.c.NewConstraints(&p, deltaUpdates, fieldConfigJsonMap[p.Field])
	if err != nil {
		return nil, err
	}

	switch {
	case base.Type == pm.Response_Validated_Constraint_FIELD_FORBIDDEN:
		// The proposed type is completely disallowed by the materialization. This
		// differs from INCOMPATIBLE, which re-creating the materialization could
		// resolve.
		return []*pm.Response_Validated_Constraint{base}, nil

	case !deltaUpdates && p.IsRootDocumentProjection():
		return v.rootDocumentConstraints(p, base, existingResource, lastBinding, boundCollection, docFields, fieldConfigJsonMap)

	case !deltaUpdates && p.IsPrimaryKey && lastBinding != nil && !slices.Contains(lastBinding.FieldSelection.Keys, p.Field):
		return v.addedKeyConstraints(p, existingResource, lastBinding, boundCollection, fieldConfigJsonMap)

	case existingResource.GetField(p.Field) != nil:
		// An already-materialized field: validate against its existing column.
		c, err := v.constraintForExistingField(boundCollection, p, *existingResource.GetField(p.Field), fieldConfigJsonMap)
		if err != nil {
			return nil, err
		}
		return []*pm.Response_Validated_Constraint{c}, nil

	default:
		return []*pm.Response_Validated_Constraint{base}, nil
	}
}

// withIncompatibility pairs an INCOMPATIBLE constraint with the base
// requiredness when the base requires the projection, so the control plane
// surfaces a clear conflict; for a non-required projection it returns only the
// incompatibility, which the control plane drops.
func withIncompatibility(base, incompatible *pm.Response_Validated_Constraint) []*pm.Response_Validated_Constraint {
	if base.Type == pm.Response_Validated_Constraint_LOCATION_REQUIRED ||
		base.Type == pm.Response_Validated_Constraint_FIELD_REQUIRED {
		return []*pm.Response_Validated_Constraint{base, incompatible}
	}
	return []*pm.Response_Validated_Constraint{incompatible}
}

// rootDocumentConstraints computes the constraint(s) for a root document
// projection of a standard-updates binding.
func (v Validator) rootDocumentConstraints(
	p pf.Projection,
	base *pm.Response_Validated_Constraint,
	existingResource ExistingResource,
	lastBinding *pf.MaterializationSpec_Binding,
	boundCollection pf.CollectionSpec,
	docFields []string,
	fieldConfigJsonMap map[string]json.RawMessage,
) ([]*pm.Response_Validated_Constraint, error) {
	// Only the originally selected root document projection is allowed to be
	// selected. If there is no prior spec, the first root document projection is
	// the document.
	selectedDoc := (lastBinding != nil && p.Field == lastBinding.FieldSelection.Document) ||
		(lastBinding == nil && p.Field == docFields[0])

	switch {
	case selectedDoc:
		docConstraint := &pm.Response_Validated_Constraint{
			Type:   pm.Response_Validated_Constraint_FIELD_REQUIRED,
			Reason: "This field is the document in the current materialization",
		}

		// Mark this field as the document, unless it is already materialized with
		// an incompatible type, in which case surface the incompatibility.
		existingField := existingResource.GetField(p.Field)
		if existingField == nil {
			return []*pm.Response_Validated_Constraint{docConstraint}, nil
		}
		c, err := v.constraintForExistingField(boundCollection, p, *existingField, fieldConfigJsonMap)
		if err != nil {
			return nil, err
		}
		if !c.Type.IsForbidden() {
			return []*pm.Response_Validated_Constraint{docConstraint}, nil
		}
		return withIncompatibility(base, c), nil

	case lastBinding != nil && lastBinding.FieldSelection.Document == "":
		return withIncompatibility(base, &pm.Response_Validated_Constraint{
			Type:   pm.Response_Validated_Constraint_INCOMPATIBLE,
			Reason: "Cannot add a new root document projection to materialization without backfilling",
		}), nil

	default:
		// Another root document projection when one is already materialized.
		materializedAs := docFields[0]
		if lastBinding != nil {
			materializedAs = lastBinding.FieldSelection.Document
		}
		return []*pm.Response_Validated_Constraint{{
			Type: pm.Response_Validated_Constraint_FIELD_FORBIDDEN,
			Reason: fmt.Sprintf(
				"Cannot materialize root document projection '%s' because field '%s' is already being materialized as the document",
				p.Field,
				materializedAs,
			),
		}}, nil
	}
}

// addedKeyConstraints computes the constraint(s) for a primary key projection
// that is not part of the existing binding's key, which cannot be added to a
// standard-updates materialization without backfilling.
func (v Validator) addedKeyConstraints(
	p pf.Projection,
	existingResource ExistingResource,
	lastBinding *pf.MaterializationSpec_Binding,
	boundCollection pf.CollectionSpec,
	fieldConfigJsonMap map[string]json.RawMessage,
) ([]*pm.Response_Validated_Constraint, error) {
	if !slices.Contains(lastBinding.FieldSelection.AllFields(), p.Field) {
		return []*pm.Response_Validated_Constraint{{
			Type:   pm.Response_Validated_Constraint_FIELD_FORBIDDEN,
			Reason: "Cannot add a new key location to the field selection of an existing non-delta-updates materialization without backfilling",
		}}, nil
	}

	// The key location was already part of the selection: validate it against
	// its existing column, or recommend it if the destination doesn't report
	// columns.
	existingField := existingResource.GetField(p.Field)
	if existingField == nil {
		return []*pm.Response_Validated_Constraint{{
			Type:   pm.Response_Validated_Constraint_LOCATION_RECOMMENDED,
			Reason: "This location is part of the current materialization",
		}}, nil
	}
	c, err := v.constraintForExistingField(boundCollection, p, *existingField, fieldConfigJsonMap)
	if err != nil {
		return nil, fmt.Errorf("evaluating constraint for existing field: %w", err)
	}
	return []*pm.Response_Validated_Constraint{c}, nil
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
func forbidLongFields(maxLength int, collection pf.CollectionSpec, constraints map[string][]*pm.Response_Validated_Constraint) (map[string][]*pm.Response_Validated_Constraint, error) {
	if maxLength == 0 {
		return constraints, nil
	}

	hasType := func(cs []*pm.Response_Validated_Constraint, t pm.Response_Validated_Constraint_Type) bool {
		return slices.ContainsFunc(cs, func(c *pm.Response_Validated_Constraint) bool {
			return c.Type == t
		})
	}

	requiredLocations := make(map[string]bool)
	for field, cs := range constraints {
		tooLong := len([]rune(field)) > maxLength

		p := collection.GetProjection(field)

		if hasType(cs, pm.Response_Validated_Constraint_LOCATION_REQUIRED) {
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

		if hasType(cs, pm.Response_Validated_Constraint_FIELD_REQUIRED) {
			return nil, fmt.Errorf(
				"field '%s' is required to be materialized but has a length of %d which exceeds the maximum length allowable by the destination of %d",
				field,
				len(field),
				maxLength,
			)
		}

		constraints[field] = []*pm.Response_Validated_Constraint{{
			Type: pm.Response_Validated_Constraint_FIELD_FORBIDDEN,
			Reason: fmt.Sprintf(
				"Field '%s' has a length of %d which exceeds the maximum length allowable by the destination of %d. Use an alternate projection with a shorter name to materialize this location",
				field,
				len(field),
				maxLength,
			),
			FoldedField: cs[0].FoldedField,
		}}
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
