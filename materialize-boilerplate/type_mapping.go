package boilerplate

import (
	"fmt"
	"slices"

	pf "github.com/estuary/flow/go/protocols/flow"
)

// FlatType is a flattened, materialization-friendly representation of a
// document location's type.
// It differs from JSON types by:
//   - Having a single type, with cases like "JSON string OR integer" delegated to
//     a MULTIPLE case.
//   - Hoisting JSON `null` out of the type representation and into a separate
//     orthogonal concern.
type FlatType interface {
	isFlatType()
}

type FlatTypeArray struct {
	InferenceArray       pf.Inference_Array
	NullableItems        bool
	ItemTypesWithoutNull []string
}

type FlatTypeBinary struct {
	InferenceString pf.Inference_String
}

type FlatTypeBoolean struct{}

type FlatTypeInteger struct {
	InferenceNumeric pf.Inference_Numeric
}

type FlatTypeMultiple struct{}

type FlatTypeNumber struct {
	InferenceNumeric pf.Inference_Numeric
}

type FlatTypeNever struct{}

type FlatTypeObject struct{}

type FlatTypeString struct {
	InferenceString pf.Inference_String
}

type FlatTypeStringFormatInteger struct {
	InferenceString  pf.Inference_String
	InferenceNumeric pf.Inference_Numeric
}

type FlatTypeStringFormatNumber struct {
	InferenceString  pf.Inference_String
	InferenceNumeric pf.Inference_Numeric
}

func (FlatTypeArray) isFlatType()               {}
func (FlatTypeBinary) isFlatType()              {}
func (FlatTypeBoolean) isFlatType()             {}
func (FlatTypeInteger) isFlatType()             {}
func (FlatTypeMultiple) isFlatType()            {}
func (FlatTypeNever) isFlatType()               {}
func (FlatTypeNumber) isFlatType()              {}
func (FlatTypeObject) isFlatType()              {}
func (FlatTypeString) isFlatType()              {}
func (FlatTypeStringFormatInteger) isFlatType() {}
func (FlatTypeStringFormatNumber) isFlatType()  {}

// Projection lifts a pf.Projection into a form that's more easily worked with
// for materialization-specific type mapping.
type Projection struct {
	pf.Projection
	FlatType  FlatType
	MustExist bool
}

func mapProjection(p pf.Projection, fc FieldConfiger) Projection {
	mustExist := p.Inference.Exists == pf.Inference_MUST && !slices.Contains(p.Inference.Types, "null")
	typesWithoutNull := getTypesWithoutNull(p.Inference.Types)

	out := Projection{
		Projection: p,
		MustExist:  mustExist,
	}

	if fc.CastToString() {
		out.FlatType = FlatTypeString{}
		return out
	}

	if len(typesWithoutNull) == 0 {
		out.FlatType = FlatTypeNever{}
		return out
	}

	if format, ok := AsFormattedNumeric(&p); ok {
		switch format {
		case StringFormatInteger:
			flat := FlatTypeStringFormatInteger{}
			if inf := p.Inference.String_; inf != nil {
				flat.InferenceString = *inf
			}
			if inf := p.Inference.Numeric; inf != nil {
				flat.InferenceNumeric = *inf
			}
			out.FlatType = flat
		case StringFormatNumber:
			flat := FlatTypeStringFormatNumber{}
			if inf := p.Inference.String_; inf != nil {
				flat.InferenceString = *inf
			}
			if inf := p.Inference.Numeric; inf != nil {
				flat.InferenceNumeric = *inf
			}
			out.FlatType = flat
		default:
			panic(fmt.Sprintf("unhandled format %q from AsFormattedNumeric", format))
		}
		return out
	}

	if len(typesWithoutNull) > 1 {
		out.FlatType = FlatTypeMultiple{}
		return out
	}

	switch typesWithoutNull[0] {
	case "integer":
		flat := FlatTypeInteger{}
		if inf := p.Inference.Numeric; inf != nil {
			flat.InferenceNumeric = *inf
		}
		out.FlatType = flat
	case "number":
		flat := FlatTypeNumber{}
		if inf := p.Inference.Numeric; inf != nil {
			flat.InferenceNumeric = *inf
		}
		out.FlatType = flat
	case "boolean":
		out.FlatType = FlatTypeBoolean{}
	case "string":
		flat := FlatTypeString{}
		if inf := p.Inference.String_; inf != nil {
			flat.InferenceString = *inf
		}

		if flat.InferenceString.ContentEncoding == "base64" {
			out.FlatType = FlatTypeBinary(flat)
		} else {
			out.FlatType = flat
		}
	case "object":
		out.FlatType = FlatTypeObject{}
	case "array":
		flat := FlatTypeArray{}
		if inf := p.Inference.Array; inf != nil {
			flat.InferenceArray = *inf
			flat.NullableItems = !slices.Contains(p.Inference.Array.ItemTypes, "null")
			flat.ItemTypesWithoutNull = getTypesWithoutNull(p.Inference.Array.ItemTypes)
		}
		out.FlatType = flat
	default:
		panic(fmt.Sprintf("unhandled type %q", typesWithoutNull[0]))
	}

	return out
}

func getTypesWithoutNull(types []string) []string {
	var out []string

	for _, t := range types {
		if t != "null" {
			out = append(out, t)
		}
	}

	return out
}
