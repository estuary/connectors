package main

import (
	"fmt"
	"strings"

	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
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
	Type        elasticPropertyType `json:"type"`
	Coerce      bool                `json:"coerce,omitempty"`
	Index       *bool               `json:"index,omitempty"`
	IgnoreAbove int                 `json:"ignore_above,omitempty"`
}

func (p property) String() string {
	return string(p.Type)
}

func (p property) Compatible(existing boilerplate.ExistingField) bool {
	return strings.EqualFold(existing.Type, string(p.Type))
}

func (p property) CanMigrate(existing boilerplate.ExistingField) bool {
	// Migrations are not implemented for ElasticSearch, yet.
	return false
}

var numericStringTypes = map[m.StringWithNumericFormat]elasticPropertyType{
	m.StringFormatInteger: elasticTypeLong,
	m.StringFormatNumber:  elasticTypeDouble,
}

type fieldConfig struct {
	Keyword bool `json:"keyword"`
}

func (fc fieldConfig) Validate() error {
	return nil
}

func (fc fieldConfig) CastToString() bool {
	// TODO(whb): Add support for casting to string.
	return false
}

func propForProjection(p *pf.Projection, types []string, fc fieldConfig) property {
	if mustWrapAndFlatten(p) {
		return objProp()
	}

	if numericString, ok := m.AsFormattedNumeric(p); ok {
		return property{Type: numericStringTypes[numericString], Coerce: true}
	}

	switch t := typesWithoutNull(types)[0]; t {
	case pf.JsonTypeArray:
		// Arrays of the same item type can be added to a field  that has that
		// type, so the created mapping will be for that singly-typed array
		// item.
		return propForProjection(p, p.Inference.Array.ItemTypes, fc)
	case pf.JsonTypeBoolean:
		return property{Type: elasticTypeBoolean}
	case pf.JsonTypeInteger:
		return property{Type: elasticTypeLong}
	case pf.JsonTypeString:
		inf := p.Inference.String_
		if inf == nil {
			// This simplifies handling for arrays with string item types, since
			// these will not have a string inference set.
			inf = &pf.Inference_String{}
		}

		if inf.ContentEncoding == "base64" {
			return property{Type: elasticTypeBinary}
		}

		switch f := inf.Format; f {
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
			if p.IsPrimaryKey || fc.Keyword {
				return property{Type: elasticTypeKeyword}
			} else {
				return property{Type: elasticTypeText}
			}
		}
	case pf.JsonTypeObject:
		return objProp()
	case pf.JsonTypeNumber:
		return property{Type: elasticTypeDouble}
	default:
		panic(fmt.Sprintf("propForProjection unsupported type %T (%#v)", t, t))
	}
}

func buildIndexProperties(keys, values []boilerplate.MappedProjection[property], document *boilerplate.MappedProjection[property]) map[string]property {
	props := make(map[string]property)

	for _, v := range append(keys, values...) {
		props[translateField(v.Field)] = v.Mapped
	}

	if document != nil {
		// Do not index the root document projection, since this would be less useful than other
		// selected fields and potentially costly.
		props[translateField(document.Field)] = property{
			Type: elasticTypeFlattened,
			// See above for comment on pf.JsonTypeObject.
			IgnoreAbove: 32766 / 4,
			Index:       boolPtr(false)}

	}

	return props
}

func boolPtr(b bool) *bool {
	return &b
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
	if _, isNumeric := m.AsFormattedNumeric(p); isNumeric {
		return false
	}

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
