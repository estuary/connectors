package datatypes

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

// Google Cloud DATETIME columns support microsecond precision at most.
const datetimeFormatMicros = "2006-01-02T15:04:05.000000"

// TranslateValue converts a single BigQuery row value into a JSON-friendly Go
// value, recursing into RECORD and ARRAY types as needed. The fieldSchema must
// describe the value's column.
func TranslateValue(val any, fieldSchema *bigquery.FieldSchema) (any, error) {
	// Arrays are represented as their element schema with `Repeated = true` set, so
	// to translate them we need to check `Repeated` before anything else.
	if fieldSchema.Repeated {
		if vals, ok := val.([]bigquery.Value); ok {
			var elementSchema = *fieldSchema
			elementSchema.Repeated = false

			var translated = make([]any, len(vals))
			for idx, val := range vals {
				var tval, err = TranslateValue(val, &elementSchema)
				if err != nil {
					return nil, fmt.Errorf("error translating array index %d: %w", idx, err)
				}
				translated[idx] = tval
			}
			return translated, nil
		} else {
			return val, nil
		}
	}

	switch fieldSchema.Type {
	case "RECORD":
		if vals, ok := val.([]bigquery.Value); ok {
			if len(vals) > len(fieldSchema.Schema) {
				return nil, fmt.Errorf("more values than record fields (%d > %d)", len(vals), len(fieldSchema.Schema))
			}
			var translated = make(map[string]any)
			for idx, val := range vals {
				var fieldName = fieldSchema.Schema[idx].Name
				var tval, err = TranslateValue(val, fieldSchema.Schema[idx])
				if err != nil {
					return nil, fmt.Errorf("error translating record field %q: %w", fieldName, err)
				}
				translated[fieldName] = tval
			}
			return translated, nil
		} else {
			return val, nil
		}
	}

	switch val := val.(type) {
	case *big.Rat:
		n, exact := val.FloatPrec()
		if !exact {
			// Add three additional digits of precision to inexact representations.
			// The amount is arbitrary but was chosen so 1/3 becomes "0.333" instead of "0"
			// In theory it should never apply anyway since BigQuery seems to always return
			// rationals with a power-of-ten denominator.
			n += 3
		}
		return val.FloatString(n), nil
	case civil.DateTime:
		return val.In(time.UTC).Format(datetimeFormatMicros), nil
	case string:
		if fieldSchema.Type == "JSON" && json.Valid([]byte(val)) {
			return json.RawMessage([]byte(val)), nil
		}
	case float64:
		if math.IsNaN(val) {
			return "NaN", nil
		}
	}
	return val, nil
}
