package sql

import (
	"fmt"
	"slices"
	"strings"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// ColumnValidator validates existing columns against proposed Flow collection projections.
type ColumnValidator struct {
	validationStrategies map[string]ColumnValidationFn
}

// NewColumnValidator creates a ColumnValidator from one or more ColValidations, which are
// validation strategies for types of columns. The validations do not need to be in any particular
// order and are not case sensitive, but there must be only one occurrence of each endpoint type.
func NewColumnValidator(validations ...ColValidation) ColumnValidator {
	cv := ColumnValidator{
		validationStrategies: make(map[string]ColumnValidationFn),
	}

	for _, v := range validations {
		for _, t := range v.Types {
			t := strings.ToLower(t)
			if _, ok := cv.validationStrategies[t]; ok {
				// This is an application logic error, and would happen immediately when the
				// connector starts.
				panic(fmt.Sprintf("NewColumnValidator duplicated endpoint type %q", t))
			}
			cv.validationStrategies[t] = v.Validate
		}
	}

	return cv
}

// ValidateColumn validates that an existing column is compatible with a Flow collection projection.
func (cv ColumnValidator) ValidateColumn(existing boilerplate.EndpointField, p pf.Projection) (bool, error) {
	v, ok := cv.validationStrategies[strings.ToLower(existing.Type)]
	if !ok {
		return false, fmt.Errorf("no column validator found for endpoint column %q [type %q]", existing.Name, existing.Type)
	}

	return v(p), nil
}

// ColValidation represents a column validation strategy for one or more endpoint column types. Each
// of the column types in `Types` is validated with the validation function.
type ColValidation struct {
	Types    []string
	Validate ColumnValidationFn
}

// ColumnValidationFn is a function that reports if the Flow projection is compatible. Individual
// materializations are free to define their own column validation functions to serve their unique
// needs, or use one of the standard functions included below.
type ColumnValidationFn func(pf.Projection) bool

// JsonCompatible is for columns that are typically materialized as a "json" type of field: arrays,
// objects, and fields with multiple types.
func JsonCompatible(p pf.Projection) bool {
	if TypesOrNull(p.Inference.Types, []string{"array"}) ||
		TypesOrNull(p.Inference.Types, []string{"object"}) {
		return true
	}

	return MultipleCompatible(p)
}

func MultipleCompatible(p pf.Projection) bool {
	if _, ok := boilerplate.AsFormattedNumeric(&p); ok {
		// Fields with multiple types like "string, integer" with appropriate format strings are
		// treated as the single scalar numeric value rather than a "multiple" type.
		return false
	}

	nonNullTypes := 0
	for _, t := range p.Inference.Types {
		if t == "null" {
			continue
		}
		nonNullTypes += 1
	}

	return nonNullTypes > 1
}

// BinaryCompatible is compatible with base64-encoded strings.
func BinaryCompatible(p pf.Projection) bool {
	return TypesOrNull(p.Inference.Types, []string{"string"}) &&
		p.Inference.String_.ContentEncoding == "base64"
}

// BooleanCompatible is compatible with booleans.
func BooleanCompatible(p pf.Projection) bool {
	return TypesOrNull(p.Inference.Types, []string{"boolean"})
}

// StringCompatible is compatible with strings of any format.
func StringCompatible(p pf.Projection) bool {
	return TypesOrNull(p.Inference.Types, []string{"string"})
}

// NumberCompatible is compatible with integers or numbers, and strings formatted as these.
func NumberCompatible(p pf.Projection) bool {
	if _, ok := boilerplate.AsFormattedNumeric(&p); ok {
		return true
	}

	return TypesOrNull(p.Inference.Types, []string{"number", "integer"})
}

// IntegerCompatible is compatible with integers, and strings formatted as integers. Note that
// numbers are not compatible with integers.
func IntegerCompatible(p pf.Projection) bool {
	if f, ok := boilerplate.AsFormattedNumeric(&p); ok {
		return f == boilerplate.StringFormatInteger
	}

	return TypesOrNull(p.Inference.Types, []string{"integer"})
}

// DateCompatible is compatible with strings with format: date.
func DateCompatible(p pf.Projection) bool {
	return stringWithFormat(p, "date")
}

// DateTimeCompatible is compatible with strings with format: date-time.
func DateTimeCompatible(p pf.Projection) bool {
	return stringWithFormat(p, "date-time")
}

// DurationCompatible is compatible with strings with format: duration.
func DurationCompatible(p pf.Projection) bool {
	return stringWithFormat(p, "duration")
}

// IPv4or6Compatible is compatible with strings with format: ipv4 or format: ipv6.
func IPv4or6Compatible(p pf.Projection) bool {
	return stringWithFormat(p, "ipv4") || stringWithFormat(p, "ipv6")
}

// MacAddrCompatible is compatible with strings with format: macaddr.
func MacAddrCompatible(p pf.Projection) bool {
	return stringWithFormat(p, "macaddr")
}

// MacAddr8Compatible is compatible with strings with format: macaddr8.
func MacAddr8Compatible(p pf.Projection) bool {
	return stringWithFormat(p, "macaddr8")
}

// TimeCompatible is compatible with strings with format: time.
func TimeCompatible(p pf.Projection) bool {
	return stringWithFormat(p, "time")
}

// DateCompatible is compatible with strings with format: uuid.
func UuidCompatible(p pf.Projection) bool {
	return stringWithFormat(p, "uuid")
}

// TypesOrNull reports whether the actual list of types provided is entirely contained within the
// allowed list of types, ignoring "null" as a type.
func TypesOrNull(actual, allowed []string) bool {
	for _, t := range actual {
		if !slices.Contains(append(allowed, "null"), t) {
			return false
		}
	}
	return true
}

func stringWithFormat(p pf.Projection, format string) bool {
	return TypesOrNull(p.Inference.Types, []string{"string"}) &&
		p.Inference.String_ != nil &&
		p.Inference.String_.Format == format
}
