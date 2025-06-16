package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
)

// TableCreate is a new table that needs to be created.
type TableCreate struct {
	Table
	TableCreateSql string

	ResourceConfigJson json.RawMessage
}

// TableAlter is the alterations for a table that are needed, including new columns that should be
// added and existing columns that should have their nullability constraints dropped.
type TableAlter struct {
	Table
	AddColumns []Column

	// DropNotNulls is a list of existing columns that need their nullability dropped, either
	// because the projection is not required anymore, or because the column exists in the
	// materialized table and is required but is not included in the field selection for the
	// materialization.
	DropNotNulls []boilerplate.ExistingField

	// ColumnTypeChanges is a list of columns that need their type changed. For connectors that use column renaming to migrate columns from one type to another
	// it is possible that the migration is interrupted by a crash / restart. In these instances
	// we detect the temporary columns using their suffixes and pass these in-progress migrations
	// to the connector client to finish
	ColumnTypeChanges []ColumnTypeMigration
}

var _ boilerplate.Applier = (*sqlApplier)(nil)

type sqlApplier struct {
	client       Client
	is           *boilerplate.InfoSchema
	endpoint     *Endpoint
	constrainter constrainter
}

func newSqlApplier(client Client, is *boilerplate.InfoSchema, endpoint *Endpoint, constrainter constrainter) *sqlApplier {
	return &sqlApplier{
		client:       client,
		is:           is,
		endpoint:     endpoint,
		constrainter: constrainter,
	}
}

func (a *sqlApplier) CreateResource(ctx context.Context, spec *pf.MaterializationSpec, bindingIndex int) (string, boilerplate.ActionApplyFn, error) {
	table, err := getTable(a.endpoint, spec, bindingIndex)
	if err != nil {
		return "", nil, err
	}

	createStatement, err := RenderTableTemplate(table, a.endpoint.CreateTableTemplate)
	if err != nil {
		return "", nil, err
	}

	return createStatement, func(ctx context.Context) error {
		if err := a.client.CreateTable(ctx, TableCreate{
			Table:              table,
			TableCreateSql:     createStatement,
			ResourceConfigJson: spec.Bindings[bindingIndex].ResourceConfigJson,
		}); err != nil {
			log.WithFields(log.Fields{
				"table":          table.Identifier,
				"tableCreateSql": createStatement,
			}).Error("table creation failed")
			return fmt.Errorf("failed to create table %q: %w", table.Identifier, err)
		}

		return nil
	}, nil
}

func (a *sqlApplier) DeleteResource(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	return a.client.DeleteTable(ctx, path)
}

type ColumnTypeMigration struct {
	Column
	MigrationSpec
	ProgressColumnExists bool
	OriginalColumnExists bool
}

// Column name suffix used during column migration using the rename method
const ColumnMigrationTemporarySuffix = "_flowtmp1"

func (a *sqlApplier) UpdateResource(ctx context.Context, spec *pf.MaterializationSpec, bindingIndex int, bindingUpdate boilerplate.BindingUpdate) (string, boilerplate.ActionApplyFn, error) {
	table, err := getTable(a.endpoint, spec, bindingIndex)
	if err != nil {
		return "", nil, err
	}

	getColumn := func(field string) (Column, error) {
		for _, c := range table.Columns() {
			if field == c.Field {
				return *c, nil
			}
		}
		return Column{}, fmt.Errorf("could not find column for field %q in table %s", field, table.Identifier)
	}

	alter := TableAlter{
		Table:        table,
		DropNotNulls: bindingUpdate.NewlyNullableFields,
	}

	existingResource := a.is.GetResource(table.Path)
	for _, newProjection := range bindingUpdate.NewProjections {
		col, err := getColumn(newProjection.Field)
		if err != nil {
			return "", nil, err
		}

		if existingResource.GetField(col.Field+ColumnMigrationTemporarySuffix) != nil {
			// At this stage we don't have the target MappedType anymore, but it's okay because if we don't have the original column anymore
			// (hence the new projection), it means we have already created the new column and set its value.
			alter.ColumnTypeChanges = append(alter.ColumnTypeChanges, ColumnTypeMigration{
				Column:               col,
				ProgressColumnExists: true,
				OriginalColumnExists: false,
			})
			continue
		}
		alter.AddColumns = append(alter.AddColumns, col)
	}

	var binding = spec.Bindings[bindingIndex]
	var collection = binding.Collection
	for _, field := range binding.FieldSelection.AllFields() {
		proposed := *collection.GetProjection(field)
		if slices.ContainsFunc(bindingUpdate.NewProjections, func(p pf.Projection) bool {
			return p.Field == proposed.Field
		}) {
			// Migration does not apply to newly included projections.
			continue
		}

		existing := existingResource.GetField(proposed.Field)
		var rawFieldConfig = binding.FieldSelection.FieldConfigJsonMap[proposed.Field]
		compatible, err := a.constrainter.compatibleType(*existing, &proposed, rawFieldConfig)
		if err != nil {
			return "", nil, fmt.Errorf("checking compatibility of %q: %w", proposed.Field, err)
		}

		migratable, migrationSpec, err := a.constrainter.migratable(*existing, &proposed, rawFieldConfig)
		if err != nil {
			return "", nil, fmt.Errorf("checking migratability of %q: %w", proposed.Field, err)
		}

		// If the types are not compatible, but are migratable, attempt to migrate
		if !compatible && migratable {
			col, err := getColumn(proposed.Field)
			if err != nil {
				return "", nil, err
			}
			var m = ColumnTypeMigration{
				Column:               col,
				MigrationSpec:        *migrationSpec,
				OriginalColumnExists: true,
				ProgressColumnExists: existingResource.GetField(col.Field+ColumnMigrationTemporarySuffix) != nil,
			}
			alter.ColumnTypeChanges = append(alter.ColumnTypeChanges, m)
		}
	}

	// If there is nothing to do, skip
	if len(alter.AddColumns) == 0 && len(alter.DropNotNulls) == 0 && len(alter.ColumnTypeChanges) == 0 {
		return "", nil, nil
	}

	return a.client.AlterTable(ctx, alter)
}

func getTable(endpoint *Endpoint, spec *pf.MaterializationSpec, bindingIndex int) (Table, error) {
	binding := spec.Bindings[bindingIndex]
	resource := endpoint.NewResource(endpoint)
	if err := pf.UnmarshalStrict(binding.ResourceConfigJson, resource); err != nil {
		return Table{}, fmt.Errorf("unmarshalling resource binding for collection %q: %w", binding.Collection.Name.String(), err)
	}

	tableShape := BuildTableShape(spec, bindingIndex, resource)
	return ResolveTable(tableShape, endpoint.Dialect)
}
