package main

import (
	"fmt"
	"math"
	"slices"

	"github.com/apache/iceberg-go"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
)

type fieldConfig struct {
	CastToString_ bool `json:"castToString"`
}

func (fc fieldConfig) Validate() error { return nil }

func (fc fieldConfig) CastToString() bool { return fc.CastToString_ }

type mapped struct {
	type_    iceberg.Type
	required bool
}

func mapProjection(p boilerplate.Projection) (mapped, boilerplate.ElementConverter) {
	m := mapped{required: p.MustExist}
	var converter boilerplate.ElementConverter

	switch ft := p.FlatType.(type) {
	case boilerplate.FlatTypeArray:
		if len(ft.ItemTypesWithoutNull) == 1 {
			// NB: ElementID must be populated when creating/updating a table
			// with a column that has a ListType.
			switch ft.ItemTypesWithoutNull[0] {
			case "integer":
				m.type_ = &iceberg.ListType{Element: iceberg.Int64Type{}, ElementRequired: !ft.NullableItems}
			case "number":
				m.type_ = &iceberg.ListType{Element: iceberg.Float64Type{}, ElementRequired: !ft.NullableItems}
			case "boolean":
				m.type_ = &iceberg.ListType{Element: iceberg.BooleanType{}, ElementRequired: !ft.NullableItems}
			case "string":
				m.type_ = &iceberg.ListType{Element: iceberg.StringType{}, ElementRequired: !ft.NullableItems}
			default:
				m.type_ = iceberg.StringType{}
			}
		} else {
			m.type_ = iceberg.StringType{}
		}
	case boilerplate.FlatTypeBinary:
		m.type_ = iceberg.BinaryType{}
	case boilerplate.FlatTypeBoolean:
		m.type_ = iceberg.BooleanType{}
	case boilerplate.FlatTypeInteger:
		if ft.InferenceNumeric.Minimum < math.MinInt64 || ft.InferenceNumeric.Maximum > math.MaxInt64 {
			m.type_ = iceberg.DecimalTypeOf(38, 0)
		} else {
			m.type_ = iceberg.Int64Type{}
		}
	case boilerplate.FlatTypeMultiple:
		m.type_ = iceberg.StringType{}
	case boilerplate.FlatTypeNumber:
		m.type_ = iceberg.Float64Type{}
	case boilerplate.FlatTypeObject:
		m.type_ = iceberg.StringType{}
	case boilerplate.FlatTypeString:
		switch ft.InferenceString.Format {
		case "date":
			m.type_ = iceberg.DateType{}
		case "date-time":
			m.type_ = iceberg.TimestampTzType{}
		default:
			m.type_ = iceberg.StringType{}
		}
	case boilerplate.FlatTypeStringFormatInteger:
		if ft.InferenceString.MaxLength > 38 {
			m.type_ = iceberg.StringType{}
		} else {
			m.type_ = iceberg.DecimalTypeOf(38, 0)
		}
	case boilerplate.FlatTypeStringFormatNumber:
		m.type_ = iceberg.Float64Type{}
		converter = func(te tuple.TupleElement) (any, error) {
			if v, ok := te.(string); ok {
				if v == "Infinity" || v == "-Infinity" {
					return nil, nil
				}
			}
			return te, nil
		}
	default:
		panic(fmt.Sprintf("unhandled flat type: %T", p.FlatType))
	}

	return m, converter
}

func computeSchemaForNewTable(res boilerplate.MappedBinding[mapped, resource]) *iceberg.Schema {
	var fields []iceberg.NestedField

	// In Iceberg terms, identifier fields are the "keys" of a table.
	identifierFields := appendProjectionsAsFields(&fields, res.Keys, 0)
	appendProjectionsAsFields(&fields, res.Values, len(fields))
	if p := res.Document; p != nil {
		appendProjectionsAsFields(&fields, []boilerplate.MappedProjection[mapped]{*p}, len(fields))
	}

	if res.DeltaUpdates {
		identifierFields = nil
	}

	return iceberg.NewSchemaWithIdentifiers(1, identifierFields, fields...)
}

func computeSchemaForUpdatedTable(current *iceberg.Schema, update boilerplate.MaterializerBindingUpdate[mapped]) *iceberg.Schema {
	var nextFields []iceberg.NestedField
	for _, f := range current.Fields() {
		if slices.ContainsFunc(update.NewlyNullableFields, func(field boilerplate.ExistingField) bool {
			return field.Name == f.Name
		}) {
			f.Required = false
		}
		nextFields = append(nextFields, f)
	}

	appendProjectionsAsFields(&nextFields, update.NewProjections, current.HighestFieldID())

	return iceberg.NewSchemaWithIdentifiers(current.ID+1, current.IdentifierFieldIDs, nextFields...)
}

func appendProjectionsAsFields(dst *[]iceberg.NestedField, ps []boilerplate.MappedProjection[mapped], startID int) []int {
	id := startID
	var ids []int

	for _, p := range ps {
		id += 1
		// List elements get their own ID. Map elements do too and probably
		// others, but we are only handling a limited set of array -> list
		// mappings right now.
		if m, ok := p.Mapped.type_.(*iceberg.ListType); ok {
			m.ElementID = id
			id += 1
		}

		*dst = append(*dst, iceberg.NestedField{
			ID:       id,
			Name:     p.Field,
			Type:     p.Mapped.type_,
			Required: p.Mapped.required,
			Doc:      p.Comment,
		})
		ids = append(ids, id)
	}

	return ids
}
