package main

import (
	"fmt"
	"slices"

	"github.com/estuary/connectors/materialize-boilerplate/validate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

type elasticPropertyType string

const (
	elasticTypeKeyword   elasticPropertyType = "keyword"
	elasticTypeBoolean   elasticPropertyType = "boolean"
	elasticTypeLong      elasticPropertyType = "long"
	elasticTypeDouble    elasticPropertyType = "double"
	elasticTypeText      elasticPropertyType = "text"
	elasticTypeBinary    elasticPropertyType = "binary"
	elasticTypeDate      elasticPropertyType = "date"
	elasticTypeIp        elasticPropertyType = "ip"
	elasticTypeFlattened elasticPropertyType = "flattened"
)

type property struct {
	Type   elasticPropertyType `json:"type"`
	Coerce bool                `json:"coerce,omitempty"`
	Index  *bool               `json:"index,omitempty"`
}

func propForField(field string, binding *pf.MaterializationSpec_Binding) property {
	return propForProjection(binding.Collection.GetProjection(field))
}

var numericStringTypes = map[validate.StringWithNumericFormat]elasticPropertyType{
	validate.StringFormatInteger: elasticTypeLong,
	validate.StringFormatNumber:  elasticTypeDouble,
}

func propForProjection(p *pf.Projection) property {
	if numericString, ok := validate.AsFormattedNumeric(p); ok {
		return property{Type: numericStringTypes[numericString], Coerce: true}
	}

	typesWithoutNull := func(ts []string) []string {
		out := []string{}
		for _, t := range ts {
			if t != "null" {
				out = append(out, t)
			}
		}
		return out
	}

	switch t := typesWithoutNull(p.Inference.Types)[0]; t {
	case pf.JsonTypeBoolean:
		return property{Type: elasticTypeBoolean}
	case pf.JsonTypeInteger:
		return property{Type: elasticTypeLong}
	case pf.JsonTypeString:
		if p.Inference.String_.ContentEncoding == "base64" {
			return property{Type: elasticTypeBinary}
		}

		switch f := p.Inference.String_.Format; f {
		// Formats for "integer" and "number" are handled above.
		case "date":
			return property{Type: elasticTypeDate}
		case "date-time":
			return property{Type: elasticTypeDate}
		case "ipv4":
			return property{Type: elasticTypeIp}
		case "ipv6":
			return property{Type: elasticTypeIp}
		default:
			if p.IsPrimaryKey {
				return property{Type: elasticTypeKeyword}
			} else {
				return property{Type: elasticTypeText}
			}
		}
	case pf.JsonTypeObject:
		return property{Type: elasticTypeFlattened}
	case pf.JsonTypeNumber:
		return property{Type: elasticTypeDouble}
	default:
		panic(fmt.Sprintf("unsupported type %T (%#v)", t, t))
	}
}

func buildIndexProperties(b *pf.MaterializationSpec_Binding) map[string]property {
	props := make(map[string]property)

	for _, v := range append(b.FieldSelection.Keys, b.FieldSelection.Values...) {
		props[v] = propForField(v, b)
	}

	if d := b.FieldSelection.Document; d != "" {
		// Do not index the root document projection, since this would be less useful than other
		// selected fields and potentially costly.
		props[d] = property{Type: elasticTypeFlattened, Index: boolPtr(false)}
	}

	return props
}

func boolPtr(b bool) *bool {
	return &b
}

var elasticValidator = validate.NewValidator(constrainter{})

type constrainter struct{}

func (constrainter) NewConstraints(p *pf.Projection, deltaUpdates bool) *pm.Response_Validated_Constraint {
	_, isNumeric := validate.AsFormattedNumeric(p)

	var constraint = pm.Response_Validated_Constraint{}
	switch {
	case p.IsPrimaryKey:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "Primary key locations are required"
	case p.IsRootDocumentProjection() && !deltaUpdates:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "The root document is required for a standard updates materialization"
	case p.IsRootDocumentProjection():
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The root document should usually be materialized"
	case p.Inference.IsSingleScalarType() || isNumeric:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The projection has a single scalar type"
	case p.Inference.IsSingleType() && !slices.Contains(p.Inference.Types, "array"):
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

func (constrainter) Compatible(existing *pf.Projection, proposed *pf.Projection) bool {
	return propForProjection(existing).Type == propForProjection(proposed).Type
}

func (constrainter) DescriptionForType(p *pf.Projection) string {
	return string(propForProjection(p).Type)
}
