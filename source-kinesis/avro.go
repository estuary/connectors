package main

import "encoding/json"

type avroSchema struct {
	Type        interface{} `json:"type"`
	Name        string      `json:"name,omitempty"`
	LogicalType string      `json:"logicalType,omitempty"`
	Fields      []struct {
		Name    string      `json:"name"`
		Type    interface{} `json:"type"`
		Default interface{} `json:"default,omitempty"`
	} `json:"fields,omitempty"`
	Items   interface{} `json:"items,omitempty"`   // For array types
	Values  interface{} `json:"values,omitempty"`  // For map types
	Symbols []string    `json:"symbols,omitempty"` // For enum types
}

func convertAvroType(avroType interface{}) (map[string]interface{}, bool) {
	switch t := avroType.(type) {

	case string:
		return avroTypeToJSONSchema(t, ""), false

	case []interface{}:
		// Union type - check for nullable pattern ["null", "actualType"]
		nullable := false
		var inner map[string]interface{}

		for _, v := range t {
			if v == "null" {
				nullable = true
			} else {
				inner, _ = convertAvroType(v)
			}
		}

		if inner == nil {
			inner = map[string]interface{}{"type": "string"}
		}
		return inner, nullable

	case map[string]interface{}:
		typeStr, _ := t["type"].(string)
		logicalType, _ := t["logicalType"].(string)

		switch typeStr {
		case "array":
			items, _ := convertAvroType(t["items"])
			return map[string]interface{}{
				"type":  "array",
				"items": items,
			}, false

		case "map":
			values, _ := convertAvroType(t["values"])
			return map[string]interface{}{
				"type":                 "object",
				"additionalProperties": values,
			}, false

		case "enum":
			symbols, _ := t["symbols"].([]interface{})
			enumValues := make([]string, 0, len(symbols))
			for _, s := range symbols {
				if str, ok := s.(string); ok {
					enumValues = append(enumValues, str)
				}
			}
			return map[string]interface{}{
				"type": "string",
				"enum": enumValues,
			}, false

		case "fixed":
			// Fixed bytes - encode as base64 string
			return map[string]interface{}{
				"type":            "string",
				"contentEncoding": "base64",
			}, false

		case "record":
			// Nested record - recursively convert
			fields, _ := t["fields"].([]interface{})
			properties := map[string]interface{}{}
			required := []string{}

			for _, f := range fields {
				field, ok := f.(map[string]interface{})
				if !ok {
					continue
				}
				fieldName, _ := field["name"].(string)
				fieldType := field["type"]
				propSchema, nullable := convertAvroType(fieldType)
				properties[fieldName] = propSchema
				if !nullable {
					_, hasDefault := field["default"]
					if !hasDefault {
						required = append(required, fieldName)
					}
				}
			}

			return map[string]interface{}{
				"type":       "object",
				"properties": properties,
				"required":   required,
			}, false

		default:
			// Check for logical types on primitive types
			if logicalType != "" {
				return avroTypeToJSONSchema(typeStr, logicalType), false
			}
			return avroTypeToJSONSchema(typeStr, ""), false
		}
	}

	return map[string]interface{}{"type": "string"}, true
}

// avroTypeToJSONSchema converts an Avro primitive type (with optional logical type) to JSON Schema
func avroTypeToJSONSchema(avroType string, logicalType string) map[string]interface{} {
	// Handle logical types first (matching source-kafka behavior)
	switch logicalType {
	case "date":
		return map[string]interface{}{"type": "string", "format": "date"}
	case "time-millis", "time-micros":
		return map[string]interface{}{"type": "string", "format": "time"}
	case "timestamp-millis", "timestamp-micros", "local-timestamp-millis", "local-timestamp-micros":
		return map[string]interface{}{"type": "string", "format": "date-time"}
	case "duration":
		return map[string]interface{}{"type": "string", "format": "duration"}
	case "uuid":
		return map[string]interface{}{"type": "string", "format": "uuid"}
	case "decimal":
		return map[string]interface{}{"type": "string", "format": "number"}
	}

	// Handle primitive types
	switch avroType {
	case "int", "long":
		return map[string]interface{}{"type": "integer"}
	case "float", "double":
		return map[string]interface{}{"type": "number"}
	case "boolean":
		return map[string]interface{}{"type": "boolean"}
	case "bytes":
		return map[string]interface{}{"type": "string", "contentEncoding": "base64"}
	case "null":
		return map[string]interface{}{"type": "null"}
	default:
		return map[string]interface{}{"type": "string"}
	}
}

func avroToJSONSchema(schema string) (map[string]interface{}, error) {
	var avro avroSchema
	if err := json.Unmarshal([]byte(schema), &avro); err != nil {
		return nil, err
	}

	properties := map[string]interface{}{}
	required := []string{}

	for _, field := range avro.Fields {
		propSchema, nullable := convertAvroType(field.Type)
		properties[field.Name] = propSchema
		// A field is required if it's not nullable AND has no default value
		if !nullable && field.Default == nil {
			required = append(required, field.Name)
		}
	}

	return map[string]interface{}{
		"type":       "object",
		"properties": properties,
		"required":   required,
	}, nil
}
