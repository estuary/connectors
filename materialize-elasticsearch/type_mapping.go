package main

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
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
	Type        elasticPropertyType `json:"type"`
	Coerce      bool                `json:"coerce,omitempty"`
	Index       *bool               `json:"index,omitempty"`
	IgnoreAbove int                 `json:"ignore_above,omitempty"`
}

func propForField(field string, binding *pf.MaterializationSpec_Binding) (property, error) {
	p := binding.Collection.GetProjection(field)
	return propForProjection(p, p.Inference.Types, binding.FieldSelection.FieldConfigJsonMap[field])
}

var numericStringTypes = map[boilerplate.StringWithNumericFormat]elasticPropertyType{
	boilerplate.StringFormatInteger: elasticTypeLong,
	boilerplate.StringFormatNumber:  elasticTypeDouble,
}

func propForProjection(p *pf.Projection, types []string, fc json.RawMessage) (property, error) {
	if mustWrapAndFlatten(p) {
		return objProp(), nil
	}

	type fieldConfig struct {
		Keyword bool `json:"keyword"`
	}

	var conf fieldConfig
	if fc != nil {
		if err := json.Unmarshal(fc, &conf); err != nil {
			return property{}, fmt.Errorf("unmarshalling raw field config: %w", err)
		}
	}

	if numericString, ok := boilerplate.AsFormattedNumeric(p); ok {
		return property{Type: numericStringTypes[numericString], Coerce: true}, nil
	}

	switch t := typesWithoutNull(types)[0]; t {
	case pf.JsonTypeArray:
		// Arrays of the same item type can be added to a field  that has that
		// type, so the created mapping will be for that singly-typed array
		// item.
		return propForProjection(p, p.Inference.Array.ItemTypes, fc)
	case pf.JsonTypeBoolean:
		return property{Type: elasticTypeBoolean}, nil
	case pf.JsonTypeInteger:
		return property{Type: elasticTypeLong}, nil
	case pf.JsonTypeString:
		inf := p.Inference.String_
		if inf == nil {
			// This simplifies handling for arrays with string item types, since
			// these will not have a string inference set.
			inf = &pf.Inference_String{}
		}

		if inf.ContentEncoding == "base64" {
			return property{Type: elasticTypeBinary}, nil
		}

		switch f := inf.Format; f {
		// Formats for "integer" and "number" are handled above.
		case "date":
			return property{Type: elasticTypeDate}, nil
		case "date-time":
			return property{Type: elasticTypeDate}, nil
		case "ipv4":
			return property{Type: elasticTypeIp}, nil
		case "ipv6":
			return property{Type: elasticTypeIp}, nil
		default:
			if p.IsPrimaryKey || conf.Keyword {
				return property{Type: elasticTypeKeyword}, nil
			} else {
				return property{Type: elasticTypeText}, nil
			}
		}
	case pf.JsonTypeObject:
		return objProp(), nil
	case pf.JsonTypeNumber:
		return property{Type: elasticTypeDouble}, nil
	default:
		return property{}, fmt.Errorf("propForProjection unsupported type %T (%#v)", t, t)
	}
}

func buildIndexProperties(b *pf.MaterializationSpec_Binding) (map[string]property, error) {
	props := make(map[string]property)

	for _, v := range append(b.FieldSelection.Keys, b.FieldSelection.Values...) {
		if p, err := propForField(v, b); err != nil {
			return nil, fmt.Errorf("buildIndexProperties: %w", err)
		} else {
			props[translateField(v)] = p
		}
	}

	if d := b.FieldSelection.Document; d != "" {
		// Do not index the root document projection, since this would be less useful than other
		// selected fields and potentially costly.
		props[translateField(d)] = property{
			Type: elasticTypeFlattened,
			// See above for comment on pf.JsonTypeObject.
			IgnoreAbove: 32766 / 4,
			Index:       boolPtr(false)}
	}

	return props, nil
}

func boolPtr(b bool) *bool {
	return &b
}

type constrainter struct{}

func (constrainter) NewConstraints(p *pf.Projection, deltaUpdates bool) *pm.Response_Validated_Constraint {
	_, isNumeric := boilerplate.AsFormattedNumeric(p)

	var constraint = pm.Response_Validated_Constraint{}
	switch {
	case p.IsPrimaryKey:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "All Locations that are part of the collections key are required"
	case p.IsRootDocumentProjection() && deltaUpdates:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The root document should usually be materialized"
	case p.IsRootDocumentProjection():
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "The root document must be materialized"
	case len(p.Inference.Types) == 0:
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize a field with no types"
	case p.Field == "_meta/op":
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The operation type should usually be materialized"
	case strings.HasPrefix(p.Field, "_meta/"):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "Metadata fields are able to be materialized"
	case p.Inference.IsSingleScalarType() || isNumeric:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The projection has a single scalar type"
	case slices.Equal(p.Inference.Types, []string{"null"}):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize a field where the only possible type is 'null'"
	case p.Inference.IsSingleType() && slices.Contains(p.Inference.Types, "object"):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "Object fields may be materialized"
	default:
		// Any other case is one where the field is an array or has multiple types.
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "This field is able to be materialized"
	}

	return &constraint
}

func (constrainter) Compatible(existing boilerplate.EndpointField, proposed *pf.Projection, fc json.RawMessage) (bool, error) {
	prop, err := propForProjection(proposed, proposed.Inference.Types, fc)
	if err != nil {
		return false, err
	}

	return strings.EqualFold(existing.Type, string(prop.Type)), nil
}

func (constrainter) DescriptionForType(p *pf.Projection, fc json.RawMessage) (string, error) {
	prop, err := propForProjection(p, p.Inference.Types, fc)
	if err != nil {
		return "", err
	}

	return string(prop.Type), nil
}

func typesWithoutNull(ts []string) []string {
	out := []string{}
	for _, t := range ts {
		if t != "null" {
			out = append(out, t)
		}
	}

	return out
}

func mustWrapAndFlatten(p *pf.Projection) bool {
	nonNullTypes := typesWithoutNull(p.Inference.Types)

	if len(nonNullTypes) != 1 {
		return true
	}

	if nonNullTypes[0] == "array" {
		if p.Inference.Array == nil {
			return true
		}

		items := typesWithoutNull(p.Inference.Array.ItemTypes)
		if len(items) != 1 || items[0] == "array" {
			// Nested arrays don't work because we need some non-array type
			// somewhere in order to actually create the mapping.
			return true
		}
	}

	return false
}

func objProp() property {
	return property{
		Type: elasticTypeFlattened,
		// See https://www.elastic.co/guide/en/elasticsearch/reference/current/ignore-above.html
		// This setting is to avoid extremely long strings causing errors due to Elastic's
		// requirement that strings do not have a byte length longer than 32766. Long strings
		// will not be indexed or stored in the Lucene index, but will still be present in the
		// _source field.
		IgnoreAbove: 32766 / 4,
	}
}
