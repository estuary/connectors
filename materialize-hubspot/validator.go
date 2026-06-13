package hubspot

import (
	"fmt"
	"slices"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

var (
	coreMetadata = []string{
		"_meta/op",
		"flow_published_at",
	}
)

type Validator struct {
	schema *Schema
}

type selection struct {
	keys      []string
	documents []string
}

func NewValidator(schema *Schema) *Validator {
	return &Validator{
		schema: schema,
	}
}

// ValidateBinding returns the constraints for the binding.
func (v *Validator) ValidateBinding(
	resource Resource,
	binding *pm.Request_Validate_Binding,
	lastSpec *pf.MaterializationSpec,
) (map[string]*pm.Response_Validated_Constraint, error) {
	path, err := resource.Path()
	if err != nil {
		return nil, err
	}

	lastBinding := boilerplate.FindLastBinding(path, lastSpec)

	for _, p := range binding.Collection.Projections {
		// Don't allow collection keys to be nullable unless they have a default value set. If a
		// default value is set, there will always be a value provided for the field from the
		// runtime.
		if !p.IsPrimaryKey {
			continue
		}

		mustExist := p.Inference.Exists == pf.Inference_MUST && !slices.Contains(p.Inference.Types, "null")
		hasDefault := p.Inference.DefaultJson != nil

		if !mustExist && !hasDefault {
			return nil, fmt.Errorf(
				"cannot materialize collection '%s' with nullable key field '%s' unless it has a default value annotation",
				binding.Collection.Name.String(),
				p.Field)
		}
	}

	object, err := resource.CRMObject()
	if err != nil {
		return nil, err
	}

	constraints := make(map[string]*pm.Response_Validated_Constraint)
	keys := []string{}
	for _, projection := range binding.Collection.Projections {
		constraint, err := v.NewConstraint(projection, object, binding, lastBinding, keys)
		if err != nil {
			return nil, err
		}
		constraints[projection.Field] = constraint

		if projection.IsPrimaryKey && constraint.Type != pm.Response_Validated_Constraint_FIELD_FORBIDDEN {
			keys = append(keys, projection.Field)
		}
	}

	return constraints, nil
}

func (v *Validator) NewConstraint(
	projection pf.Projection,
	object CRMObject,
	binding *pm.Request_Validate_Binding,
	lastBinding *pf.MaterializationSpec_Binding,
	keys []string,
) (*pm.Response_Validated_Constraint, error) {
	objectSchema, ok := v.schema.CRM.Objects[object]
	if !ok {
		return nil, fmt.Errorf("no properties loaded for object: %q", object)
	}

	name, err := PropertyName(projection.Field)
	if err != nil {
		return nil, err
	}

	property, ok := objectSchema.Properties[name]
	if !ok {
		// New properties cannot be created since we do not request the
		// required scopes.
		return &pm.Response_Validated_Constraint{
			Type:        pm.Response_Validated_Constraint_FIELD_FORBIDDEN,
			Reason:      fmt.Sprintf("A property for this field does not exist: %q", name),
			FoldedField: name,
		}, nil
	}

	if property.ModificationMetadata.ReadOnlyValue {
		return &pm.Response_Validated_Constraint{
			Type:        pm.Response_Validated_Constraint_FIELD_FORBIDDEN,
			Reason:      "The existing property for this field is read-only",
			FoldedField: name,
		}, nil
	}

	var fieldConfig FieldConfig
	if raw := binding.FieldConfigJsonMap[projection.Field]; raw != nil {
		if err := boilerplate.UnmarshalStrict(raw, &fieldConfig); err != nil {
			return nil, fmt.Errorf("parsing field config json: %w", err)
		}
	}

	mappedField, err := NewMappedField(projection, fieldConfig)
	if err != nil {
		return nil, err
	}
	if !mappedField.Compatible(property) {
		return &pm.Response_Validated_Constraint{
			Type:        pm.Response_Validated_Constraint_FIELD_FORBIDDEN,
			Reason:      fmt.Sprintf("The existing property for this field has an incompatible type: %q", property.Type),
			FoldedField: name,
		}, nil
	}

	switch {
	case projection.IsPrimaryKey:
		if len(keys) != 0 {
			return &pm.Response_Validated_Constraint{
				Type:        pm.Response_Validated_Constraint_FIELD_FORBIDDEN,
				Reason:      "Only one key field may be selected for record matching",
				FoldedField: name,
			}, nil
		}

		return &pm.Response_Validated_Constraint{
			Type:        pm.Response_Validated_Constraint_FIELD_REQUIRED,
			Reason:      "One collections key field is required",
			FoldedField: name,
		}, nil
	case projection.IsRootDocumentProjection():
		// You can optionally store the root document but note it isn't used
		// for loads.
		return &pm.Response_Validated_Constraint{
			Type:        pm.Response_Validated_Constraint_FIELD_OPTIONAL,
			Reason:      "The root document can be materialized",
			FoldedField: name,
		}, nil
	case slices.Contains(coreMetadata, projection.Field):
		return &pm.Response_Validated_Constraint{
			Type:        pm.Response_Validated_Constraint_FIELD_OPTIONAL,
			Reason:      "Core metadata should usually be materialized",
			FoldedField: name,
		}, nil
	default:
		return &pm.Response_Validated_Constraint{
			Type:        pm.Response_Validated_Constraint_FIELD_OPTIONAL,
			Reason:      "This field is able to be materialized",
			FoldedField: name,
		}, nil
	}
}
