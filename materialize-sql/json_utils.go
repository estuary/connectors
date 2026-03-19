package sql

import (
	"encoding/json"
	"fmt"
)

// StripNullFields removes top-level keys with null values from a JSON object,
// but only for the specified field names. This is used during no_flow_document
// Load reconstruction to avoid returning null for optional fields whose schemas
// don't allow null, while preserving null for explicitly nullable fields.
func StripNullFields(doc json.RawMessage, fields []string) (json.RawMessage, error) {
	if len(fields) == 0 {
		return doc, nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(doc, &m); err != nil {
		return nil, fmt.Errorf("unmarshalling document for null field stripping: %w", err)
	}
	stripped := false
	for _, f := range fields {
		if v, ok := m[f]; ok && string(v) == "null" {
			delete(m, f)
			stripped = true
		}
	}
	if !stripped {
		return doc, nil
	}
	result, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshalling document after null field stripping: %w", err)
	}
	return result, nil
}
