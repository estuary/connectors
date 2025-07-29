package materialize

import (
	"slices"

	pf "github.com/estuary/flow/go/protocols/flow"
)

type StringWithNumericFormat string

const (
	StringFormatInteger StringWithNumericFormat = "stringFormatInteger"
	StringFormatNumber  StringWithNumericFormat = "stringFormatNumber"
)

func AsFormattedNumeric(projection *pf.Projection) (StringWithNumericFormat, bool) {
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
			return StringFormatInteger, true
		case projection.Inference.String_.Format == "number" && typesMatch(projection.Inference.Types, []string{"null", "number", "string"}):
			return StringFormatNumber, true
		default:
			// Fallthrough.
		}
	}

	// Not a formatted numeric field.
	return "", false
}
