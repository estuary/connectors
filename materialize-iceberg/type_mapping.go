package connector

import (
	"encoding/json"
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
	Nullable      bool `json:"nullable"`
}

func (fc fieldConfig) Validate() error { return nil }

func (fc fieldConfig) CastToString() bool { return fc.CastToString_ }

type mapped struct {
	type_ iceberg.Type
	// Name of the table column, which may be different than the Flow field
	// name, depending on the materialization configuration. This is exported so
	// it can be used in templates.
	Name string
	// Nullable overrides the field's inferred nullability, forcing it to be
	// nullable even if the collection schema says it's always present and
	// non-null. Primary key fields are always required regardless of this flag.
	Nullable bool
}

func (m mapped) isVariant() bool { return m.type_.Equals(iceberg.VariantType{}) }

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
	"string":                    {iceberg.BinaryType{}},
	boilerplate.AnyExistingType: {iceberg.StringType{}, iceberg.VariantType{}},
}

func jsonColumnType(variant bool) iceberg.Type {
	if variant {
		return iceberg.VariantType{}
	}
	return iceberg.StringType{}
}

var migrateFieldSuffix = "_flow_tmp"

// mapProjection maps a projection to its Iceberg column type. With
// variantColumns set, JSON-shaped projections (objects, arrays, multi-type
// fields, and the root document) map to variant instead of a JSON string.
func mapProjection(p boilerplate.Projection, translateField boilerplate.TranslateFieldFn, variantColumns bool) (mapped, boilerplate.ElementConverter) {
	var m mapped
	var converter boilerplate.ElementConverter

	m.Name = translateField(p.Field)
	useVariant := variantColumns && !p.IsPrimaryKey

	switch ft := p.FlatType.(type) {
	case boilerplate.FlatTypeArray:
		// Arrays are always JSON text (or variant): reading typed lists out
		// of the staged CSV files would need per-type parsing in the queries.
		m.type_ = jsonColumnType(useVariant)
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
		m.type_ = jsonColumnType(useVariant)
	case boilerplate.FlatTypeNumber:
		m.type_ = iceberg.Float64Type{}
	case boilerplate.FlatTypeObject:
		m.type_ = jsonColumnType(useVariant)
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

	if m.isVariant() {
		converter = variantConverter
	}

	return m, converter
}

// variantConverter makes every staged value of a variant column valid JSON
// text for parse_json. Objects, arrays, and the root document already arrive
// as JSON, and numbers and booleans are written as their JSON literals, but a
// string value of a multi-type field would be written bare.
func variantConverter(te tuple.TupleElement) (any, error) {
	if s, ok := te.(string); ok {
		return json.Marshal(s)
	}
	return te, nil
}

func computeSchemaForNewTable(res boilerplate.MappedBinding[config, resource, mapped]) *iceberg.Schema {
	var fields []iceberg.NestedField

	// In Iceberg terms, identifier fields are the "keys" of a table.
	identifierFields, lastId := appendProjectionsAsFields(&fields, res.Keys, 0, false)
	_, lastId = appendProjectionsAsFields(&fields, res.Values, lastId, false)
	if p := res.Document; p != nil {
		appendProjectionsAsFields(&fields, []boilerplate.MappedProjection[mapped]{*p}, lastId, false)
	}

	if res.DeltaUpdates {
		identifierFields = nil
	}

	return iceberg.NewSchemaWithIdentifiers(1, identifierFields, fields...)
}

func computeSchemaForUpdatedTable(
	currentHighestID int,
	current *iceberg.Schema,
	update boilerplate.BindingUpdate[config, resource, mapped],
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
		temp.Mapped.Name += migrateFieldSuffix
		tempMigrateProjections = append(tempMigrateProjections, temp)
	}

	// S3 Tables' REST catalog rejects an add-schema update that introduces a
	// brand-new required column, even with initial-default/write-default set.
	// Columns newly appended to an existing table - ordinary new projections
	// and migration temp columns alike - are therefore always created
	// optional, matching how materialize-sql treats newly-added columns as
	// nullable regardless of MustExist and only enforces required-ness at
	// fresh table creation.
	_, lastId := appendProjectionsAsFields(&nextFields, update.NewProjections, currentHighestID, true)
	appendProjectionsAsFields(&nextFields, tempMigrateProjections, lastId, true)

	return iceberg.NewSchemaWithIdentifiers(current.ID+1, current.IdentifierFieldIDs, nextFields...)
}

func computeSchemaForCompletedMigrations(current *iceberg.Schema, fieldsToMigrate []boilerplate.MigrateField[mapped]) *iceberg.Schema {
	fieldWasMigrated := func(f iceberg.NestedField) bool {
		return slices.ContainsFunc(fieldsToMigrate, func(field boilerplate.MigrateField[mapped]) bool {
			return f.Name == field.From.Name
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
		} else if migrationIdx := slices.IndexFunc(fieldsToMigrate, func(field boilerplate.MigrateField[mapped]) bool {
			return f.Name == field.To.Mapped.Name+migrateFieldSuffix
		}); migrationIdx != -1 {
			f.Name = fieldsToMigrate[migrationIdx].From.Name
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

func appendProjectionsAsFields(dst *[]iceberg.NestedField, ps []boilerplate.MappedProjection[mapped], startID int, forceOptional bool) ([]int, int) {
	id := startID
	var ids []int

	for _, p := range ps {
		id += 1
		if m, ok := p.Mapped.type_.(*iceberg.ListType); ok {
			m.ElementID = id
			id += 1
		}

		required := !forceOptional && ((p.MustExist && !p.Mapped.Nullable) || p.IsPrimaryKey)

		*dst = append(*dst, iceberg.NestedField{
			ID:       id,
			Name:     p.Mapped.Name,
			Type:     p.Mapped.type_,
			Required: required,
			Doc:      strings.ReplaceAll(p.Comment, "\n", " - "), // Glue catalogs don't support newlines in field comments
		})
		ids = append(ids, id)
	}

	return ids, id
}
