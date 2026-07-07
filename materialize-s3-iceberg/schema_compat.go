package main

import "github.com/invopop/jsonschema"

// collapseNullableScalars rewrites the `nullable` field wrappers emitted by
// invopop — `oneOf: [{type: X, ...}, {type: null}]` — into the primitive
// type-array form `type: [X, "null"]`, hoisting the scalar branch's other
// keywords (title/description/format/order/secret/…) up to the field itself.
//
// This reproduces the schema the removed pyiceberg tool generated via
// pydantic's `primitive_type_array` union format, which the dashboard's form
// generator expects for nullable scalars. It runs only on this connector's
// spec (materialize-s3-iceberg is the sole user of the `nullable` tag), so the
// shared schema-gen behavior is left untouched.
//
// Two-branch oneOf shapes that are NOT a nullable scalar are deliberately left
// alone: discriminated unions (credentials, catalog) have no bare {type: null}
// branch, and a nullable object (advanced) keeps its oneOf wrapper because a
// `["object", "null"]` array form is not what the UI expects. Recursion always
// descends into those branches so nullable scalars nested inside them (e.g.
// advanced.feature_flags) are still collapsed.
func collapseNullableScalars(s *jsonschema.Schema) {
	if s == nil {
		return
	}

	if s.Properties != nil {
		for pair := s.Properties.Oldest(); pair != nil; pair = pair.Next() {
			collapseNullableScalars(pair.Value)
		}
	}
	for _, branch := range s.OneOf {
		collapseNullableScalars(branch)
	}
	for _, branch := range s.AnyOf {
		collapseNullableScalars(branch)
	}
	collapseNullableScalars(s.Items)
	collapseNullableScalars(s.AdditionalProperties)

	if len(s.OneOf) != 2 {
		return
	}
	scalar, null := s.OneOf[0], s.OneOf[1]
	if null.Type != "null" {
		scalar, null = null, scalar
	}
	if null.Type != "null" || !isScalarType(scalar.Type) {
		return
	}

	// Widen the scalar branch's type in place, then replace the wrapper with
	// it. `type` must go through Extras because jsonschema.Schema.Type is a
	// plain string that cannot hold the [type, "null"] array.
	scalarType := scalar.Type
	scalar.Type = ""
	if scalar.Extras == nil {
		scalar.Extras = map[string]any{}
	}
	scalar.Extras["type"] = []any{scalarType, "null"}
	*s = *scalar
}

func isScalarType(t string) bool {
	switch t {
	case "string", "integer", "number", "boolean":
		return true
	default:
		return false
	}
}
