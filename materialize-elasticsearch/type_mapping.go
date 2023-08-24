package main

import (
	"fmt"

	"github.com/estuary/connectors/go/pkg/slices"
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
	Index  *bool               `json:"index,omitempty"`
}

func propForField(field string, binding *pf.MaterializationSpec_Binding) property {
	return propForProjection(binding.Collection.GetProjection(field))
}

func propForProjection(p *pf.Projection) property {
	if t, ok := asFormattedNumeric(p); ok {
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

func asFormattedNumeric(projection *pf.Projection) (elasticPropertyType, bool) {
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
			return elasticTypeLong, true
		case projection.Inference.String_.Format == "number" && typesMatch(projection.Inference.Types, []string{"null", "number", "string"}):
			return elasticTypeDouble, true
		default:
			// Fallthrough.
		}
	}

	// Not a formatted numeric field.
	return "", false
}

func boolPtr(b bool) *bool {
	return &b
}
