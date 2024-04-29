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
	return propForProjection(binding.Collection.GetProjection(field), binding.FieldSelection.FieldConfigJsonMap[field])
}

var numericStringTypes = map[boilerplate.StringWithNumericFormat]elasticPropertyType{
	boilerplate.StringFormatInteger: elasticTypeLong,
	boilerplate.StringFormatNumber:  elasticTypeDouble,
}

func propForProjection(p *pf.Projection, fc json.RawMessage) (property, error) {
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
		return property{Type: elasticTypeBoolean}, nil
	case pf.JsonTypeInteger:
		return property{Type: elasticTypeLong}, nil
	case pf.JsonTypeString:
		if p.Inference.String_.ContentEncoding == "base64" {
			return property{Type: elasticTypeBinary}, nil
		}

		switch f := p.Inference.String_.Format; f {
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
		return property{
			Type: elasticTypeFlattened,
			// See https://www.elastic.co/guide/en/elasticsearch/reference/current/ignore-above.html
			// This setting is to avoid extremely long strings causing errors due to Elastic's
			// requirement that strings do not have a byte length longer than 32766. Long strings
			// will not be indexed or stored in the Lucene index, but will still be present in the
			// _source field.
			IgnoreAbove: 32766 / 4,
		}, nil
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
		// Anything else is either multiple different types, a single 'null' type, or an array type
		// which we currently don't support. We could potentially support array types if they made
		// the "elements" configuration available and that was a single type.
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize this field"
	}

	return &constraint
}

func (constrainter) Compatible(existing boilerplate.EndpointField, proposed *pf.Projection, fc json.RawMessage) (bool, error) {
	prop, err := propForProjection(proposed, fc)
	if err != nil {
		return false, err
	}

	return strings.EqualFold(existing.Type, string(prop.Type)), nil
}

func (constrainter) DescriptionForType(p *pf.Projection, fc json.RawMessage) (string, error) {
	prop, err := propForProjection(p, fc)
	if err != nil {
		return "", err
	}

	return string(prop.Type), nil
}
