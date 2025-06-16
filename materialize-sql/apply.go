package sql

import (
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
)

// TableCreate is a new table that needs to be created.
type TableCreate struct {
	Table
	TableCreateSql string
	Resource       any
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

type ColumnTypeMigration struct {
	Column
	MigrationSpec
	ProgressColumnExists bool
	OriginalColumnExists bool
}

// Column name suffix used during column migration using the rename method
const ColumnMigrationTemporarySuffix = "_flowtmp1"
