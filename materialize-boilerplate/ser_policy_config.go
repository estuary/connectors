package boilerplate

import pf "github.com/estuary/flow/go/protocols/flow"

type SerPolicyConfig struct {
	StringTruncateAfter           uint32 `json:"stringTruncateAfter,omitempty" jsonschema:"title=Truncate Strings After Length" jsonschema_extras:"order=0"`
	DisableStringTruncation       bool   `json:"disableStringTruncation,omitempty" jsonschema:"title=Disable String Truncation" jsonschema_extras:"order=1"`
	NestedObjectTruncateAfter     uint32 `json:"nestedObjectTruncateAfter,omitempty" jsonschema:"title=Truncate Nested Objects After Length" jsonschema_extras:"order=2"`
	DisableNestedObjectTruncation bool   `json:"disableNestedObjectTruncation,omitempty" jsonschema:"title=Disable Nested Object Truncation" jsonschema_extras:"order=3"`
	ArrayTruncateAfter            uint32 `json:"arrayTruncateAfter,omitempty" jsonschema:"title=Truncate Arrays After Length" jsonschema_extras:"order=4"`
	DisableArrayTruncation        bool   `json:"disableArrayTruncation,omitempty" jsonschema:"title=Disable Array Truncation" jsonschema_extras:"order=5"`
}

func (SerPolicyConfig) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "StringTruncateAfter":
		return "Maximum length of strings before they are truncated. Set to 0 or leave blank to use the connector default."
	case "DisableStringTruncation":
		return "Completely disable truncation of strings. The string truncation length is ignored if this is set."
	case "NestedObjectTruncateAfter":
		return "Maximum number of properties in an object before they are truncated. Set to 0 or leave blank to use the connector default."
	case "DisableNestedObjectTruncation":
		return "Completely disable truncation of nested object fields. The nested object field truncation length is ignored if this is set."
	case "ArrayTruncateAfter":
		return "Maximum number of items in an array before they are truncated. Set to 0 or leave blank to use the connector default."
	case "DisableArrayTruncation":
		return "Completely disable truncation of arrays. The array truncation length is ignored if this is set."
	default:
		return ""
	}
}

// ResolveSerPolicy overrides a default serialization policy with values from
// the configuration.
func ResolveSerPolicy(def *pf.SerPolicy, cfg *SerPolicyConfig) *pf.SerPolicy {
	// If a configured truncation length is 0, do not override the value from
	// the default.
	if cfg.StringTruncateAfter != 0 {
		def.StrTruncateAfter = cfg.StringTruncateAfter
	}
	if cfg.NestedObjectTruncateAfter != 0 {
		def.NestedObjTruncateAfter = cfg.NestedObjectTruncateAfter
	}
	if cfg.ArrayTruncateAfter != 0 {
		def.ArrayTruncateAfter = cfg.ArrayTruncateAfter
	}

	// A setting of 0 disables truncation for these characteristics.
	if cfg.DisableStringTruncation {
		def.StrTruncateAfter = 0
	}
	if cfg.DisableNestedObjectTruncation {
		def.NestedObjTruncateAfter = 0
	}
	if cfg.DisableArrayTruncation {
		def.ArrayTruncateAfter = 0
	}

	return def
}
