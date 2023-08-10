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

func buildIndexProperties(b *pf.MaterializationSpec_Binding) map[string]property {
	props := make(map[string]property)

	typesWithoutNull := func(ts []string) []string {
		out := []string{}
		for _, t := range ts {
			if t != "null" {
				out = append(out, t)
			}
		}
		return out
	}

	for _, v := range append(b.FieldSelection.Keys, b.FieldSelection.Values...) {
		p := *b.Collection.GetProjection(v)

		if t, ok := isFormattedNumeric(p); ok {
			props[v] = property{Type: t, Coerce: true}
			continue
		}

		switch t := typesWithoutNull(p.Inference.Types)[0]; t {
		case pf.JsonTypeBoolean:
			props[v] = property{Type: elasticTypeBoolean}
		case pf.JsonTypeInteger:
			props[v] = property{Type: elasticTypeLong}
		case pf.JsonTypeString:
			if p.Inference.String_.ContentEncoding == "base64" {
				props[v] = property{Type: elasticTypeBinary}
				continue
			}

			switch f := p.Inference.String_.Format; f {
			// Formats for "integer" and "number" are handle above.
			case "date":
				props[v] = property{Type: elasticTypeDate}
			case "date-time":
				props[v] = property{Type: elasticTypeDate}
			case "ipv4":
				props[v] = property{Type: elasticTypeIp}
			case "ipv6":
				props[v] = property{Type: elasticTypeIp}
			default:
				if p.IsPrimaryKey {
					props[v] = property{Type: elasticTypeKeyword}
				} else {
					props[v] = property{Type: elasticTypeText}
				}
			}
		case pf.JsonTypeObject:
			props[v] = property{Type: elasticTypeFlattened}
		case pf.JsonTypeNumber:
			props[v] = property{Type: elasticTypeDouble}
		default:
			panic(fmt.Sprintf("unsupported type %T (%#v)", t, t))
		}
	}

	if d := b.FieldSelection.Document; d != "" {
		props[d] = property{Type: elasticTypeFlattened}
	}

	return props
}

func isFormattedNumeric(projection pf.Projection) (elasticPropertyType, bool) {
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
