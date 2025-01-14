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

	nonNullTypes := typesWithoutNull(types)
	if len(nonNullTypes) == 0 {
		panic("internal application error: expected a single non-null type")
	}

	switch t := nonNullTypes[0]; t {
	case pf.JsonTypeArray:
		// You can add an array of the same item to any field in Elasticsearch,
		// and we only allow schematized arrays having a single type to be
		// materialized.
		return propForProjection(p, p.Inference.Array.ItemTypes, nil)
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
	case p.Inference.IsSingleType() && slices.Contains(p.Inference.Types, "array"):
		var itemTypes []string
		if p.Inference.Array != nil {
			itemTypes = typesWithoutNull(p.Inference.Array.ItemTypes)
		}

		if len(itemTypes) == 1 && itemTypes[0] == "array" {
			constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
			constraint.Reason = "Nested arrays cannot be materialized"
		} else if len(itemTypes) == 1 {
			constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
			constraint.Reason = "Arrays with a single item type can be materialized"
		} else {
			constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
			constraint.Reason = "Arrays with multiple or unknown item types cannot be materialized"
		}
	case p.Field == "_meta/op":
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The operation type should usually be materialized"
	case strings.HasPrefix(p.Field, "_meta/"):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "Metadata fields are able to be materialized"
	case p.Inference.IsSingleScalarType() || isNumeric:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The projection has a single scalar type"
	case p.Inference.IsSingleType():
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "This field is able to be materialized"

	default:
		// Anything else is either multiple different types or a single 'null'
		// type.
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize this field"
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
