package connector

import (
	"fmt"
	"math"
	"slices"
	"strings"

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
	type_ iceberg.Type
}

func (m mapped) String() string {
	return m.type_.String()
}

func (m mapped) Compatible(existing boilerplate.ExistingField) bool {
	return strings.EqualFold(existing.Type, m.type_.String())
}

func (m mapped) CanMigrate(existing boilerplate.ExistingField) bool {
	return allowedMigrations.CanMigrate(existing.Type, m.type_)
}

var allowedMigrations = boilerplate.TypeMigrations[iceberg.Type]{
	"long":                      {iceberg.DecimalTypeOf(38, 0), iceberg.Float64Type{}},
	"decimal(38, 0)":            {iceberg.Float64Type{}},
	boilerplate.AnyExistingType: {iceberg.StringType{}},
}

var migrateFieldSuffix = "_flow_tmp"

func mapProjection(p boilerplate.Projection) (mapped, boilerplate.ElementConverter) {
	var m mapped
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

		// TODO(whb): If we want to support arrays with a single element type as
		// Iceberg lists, remove this line which unconditionally makes them
		// strings. I'm not doing that right now since it is a big pain reading
		// from CSV as strings and then parsing to the specific list type in the
		// queries. V3 of the Iceberg spec includes a VARIANT type which is
		// probably what we'll use for all arrays when that is widely supported.
		m.type_ = iceberg.StringType{}
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

func computeSchemaForNewTable(res boilerplate.MappedBinding[config, resource, mapped]) *iceberg.Schema {
	var fields []iceberg.NestedField

	// In Iceberg terms, identifier fields are the "keys" of a table.
	identifierFields, lastId := appendProjectionsAsFields(&fields, res.Keys, 0)
	_, lastId = appendProjectionsAsFields(&fields, res.Values, lastId)
	if p := res.Document; p != nil {
		appendProjectionsAsFields(&fields, []boilerplate.MappedProjection[mapped]{*p}, lastId)
	}

	if res.DeltaUpdates {
		identifierFields = nil
	}

	return iceberg.NewSchemaWithIdentifiers(1, identifierFields, fields...)
}

func computeSchemaForUpdatedTable(
	currentHighestID int,
	current *iceberg.Schema,
	update boilerplate.MaterializerBindingUpdate[mapped],
) *iceberg.Schema {
	var nextFields []iceberg.NestedField
	for _, f := range current.Fields() {
		if slices.ContainsFunc(update.FieldsToMigrate, func(upd boilerplate.MigrateField[mapped]) bool {
			return f.Name == upd.From.Name+migrateFieldSuffix
		}) {
			// Prune columns from a prior failed migrations of this spec update.
			// This prevents rare cases where a prior migration column type is
			// incompatible with a source field that has undergone further
			// schema evolution before the migration could be applied.
			continue
		}

		if slices.ContainsFunc(update.NewlyNullableFields, func(field boilerplate.ExistingField) bool {
			return field.Name == f.Name
		}) {
			f.Required = false
		}

		nextFields = append(nextFields, f)
	}

	var tempMigrateProjections []boilerplate.MappedProjection[mapped]
	for _, f := range update.FieldsToMigrate {
		temp := f.To
		temp.Field += migrateFieldSuffix
		tempMigrateProjections = append(tempMigrateProjections, temp)
	}

	_, lastId := appendProjectionsAsFields(&nextFields, update.NewProjections, currentHighestID)
	appendProjectionsAsFields(&nextFields, tempMigrateProjections, lastId)

	return iceberg.NewSchemaWithIdentifiers(current.ID+1, current.IdentifierFieldIDs, nextFields...)
}

func computeSchemaForCompletedMigrations(current *iceberg.Schema, fieldsToMigrate []boilerplate.MigrateField[mapped]) *iceberg.Schema {
	fieldWasMigrated := func(f iceberg.NestedField) bool {
		return slices.ContainsFunc(fieldsToMigrate, func(field boilerplate.MigrateField[mapped]) bool {
			return f.Name == field.From.Name
		})
	}

	fieldMigratedTo := func(f iceberg.NestedField) bool {
		return slices.ContainsFunc(fieldsToMigrate, func(field boilerplate.MigrateField[mapped]) bool {
			return f.Name == field.From.Name+migrateFieldSuffix
		})
	}

	// Sanity checks that we don't remove a column without also renaming one to
	// its original name.
	var fieldsRemoved []string
	var fieldsRenamedTo []string

	var nextFields []iceberg.NestedField
	for _, f := range current.Fields() {
		if fieldWasMigrated(f) {
			fieldsRemoved = append(fieldsRemoved, f.Name)
			continue
		} else if fieldMigratedTo(f) {
			f.Name = strings.TrimSuffix(f.Name, migrateFieldSuffix)
			fieldsRenamedTo = append(fieldsRenamedTo, f.Name)
		}

		nextFields = append(nextFields, f)
	}

	slices.Sort(fieldsRemoved)
	slices.Sort(fieldsRenamedTo)
	if !slices.Equal(fieldsRemoved, fieldsRenamedTo) {
		panic(fmt.Sprintf("application error: fields removed and renamed to are not equal: %s vs %s", fieldsRemoved, fieldsRenamedTo))
	}

	return iceberg.NewSchemaWithIdentifiers(current.ID+1, current.IdentifierFieldIDs, nextFields...)
}

func appendProjectionsAsFields(dst *[]iceberg.NestedField, ps []boilerplate.MappedProjection[mapped], startID int) ([]int, int) {
	id := startID
	var ids []int

	for _, p := range ps {
		id += 1
		if m, ok := p.Mapped.type_.(*iceberg.ListType); ok {
			m.ElementID = id
			id += 1
		}

		*dst = append(*dst, iceberg.NestedField{
			ID:       id,
			Name:     p.Field,
			Type:     p.Mapped.type_,
			Required: p.MustExist || p.IsPrimaryKey,
			Doc:      strings.ReplaceAll(p.Comment, "\n", " - "), // Glue catalogs don't support newlines in field comments
		})
		ids = append(ids, id)
	}

	return ids, id
}
