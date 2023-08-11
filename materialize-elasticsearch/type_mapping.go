package main

import (
	"fmt"
	"reflect"

	pf "github.com/estuary/flow/go/protocols/flow"
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
}

func propForField(field string, binding *pf.MaterializationSpec_Binding) property {
	return propForProjection(binding.Collection.GetProjection(field))
}

func propForProjection(p *pf.Projection) property {
	if t, ok := isFormattedNumeric(p); ok {
		return property{Type: t, Coerce: true}
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
		// Formats for "integer" and "number" are handle above.
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
		props[d] = property{Type: elasticTypeFlattened}
	}

	return props
}

func isFormattedNumeric(projection *pf.Projection) (elasticPropertyType, bool) {
	if !projection.IsPrimaryKey && projection.Inference.String_ != nil {
		switch {
		case projection.Inference.String_.Format == "integer" && reflect.DeepEqual(projection.Inference.Types, []string{"integer", "null", "string"}):
			return elasticTypeLong, true
		case projection.Inference.String_.Format == "number" && reflect.DeepEqual(projection.Inference.Types, []string{"null", "number", "string"}):
			return elasticTypeDouble, true
		case projection.Inference.String_.Format == "integer" && reflect.DeepEqual(projection.Inference.Types, []string{"integer", "string"}):
			return elasticTypeLong, true
		case projection.Inference.String_.Format == "number" && reflect.DeepEqual(projection.Inference.Types, []string{"number", "string"}):
			return elasticTypeDouble, true
		case projection.Inference.String_.Format == "integer" && reflect.DeepEqual(projection.Inference.Types, []string{"null", "string"}):
			return elasticTypeLong, true
		case projection.Inference.String_.Format == "number" && reflect.DeepEqual(projection.Inference.Types, []string{"null", "string"}):
			return elasticTypeDouble, true
		case projection.Inference.String_.Format == "integer" && reflect.DeepEqual(projection.Inference.Types, []string{"string"}):
			return elasticTypeLong, true
		case projection.Inference.String_.Format == "number" && reflect.DeepEqual(projection.Inference.Types, []string{"string"}):
			return elasticTypeDouble, true
		default:
			// Fallthrough, not a formatted numeric field.
		}
	}

	return "", false
}
