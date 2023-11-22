package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	log "github.com/sirupsen/logrus"
)

// ApplyActions is a list of actions that must be taken to bring an endpoint into consistency with a
// proposed materialization change, by creating new tables and/or altering existing columns.
type ApplyActions struct {
	CreateTables []TableCreate
	AlterTables  []TableAlter
}

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
	AddColumns   []Column
	DropNotNulls []Column
}

// ExistingColumns is a mapping of existing table columns to their tables and schemas. The top-level
// key in the `tables` map is the database schema, and the key in the nested map is the table name
// which accesses its list of columns. These string keys are as reported by the database's
// INFORMATION_SCHEMA view, and may not match directly with the values from the specification
// depending on how the materialized system transforms values, particularly with respect to
// capitalization.
type ExistingColumns struct {
	tables map[string]map[string][]ExistingColumn
}

type ExistingColumn struct {
	Name               string
	Nullable           bool
	Type               string // Per INFORMATION_SCHEMA; database-specific, not currently used.
	CharacterMaxLength int    // May be 0 if the column is not a text/varchar type.
}

// PushColumn adds a column to the list of columns in the table of the schema.
func (e *ExistingColumns) PushColumn(
	schema string,
	table string,
	column string,
	nullable bool,
	endpointType string,
	maxLength int,
) {
	if e.tables == nil {
		e.tables = make(map[string]map[string][]ExistingColumn)
	}
	if _, ok := e.tables[schema]; !ok {
		e.tables[schema] = make(map[string][]ExistingColumn)
	}

	if slices.ContainsFunc(e.tables[schema][table], func(ec ExistingColumn) bool {
		return column == ec.Name
	}) {
		// This should never happen and would represent an application logic error, but sanity
		// checking it here just in case to mitigate what might otherwise be very difficult to debug
		// situations.
		panic(fmt.Sprintf(
			"logic error: PushColumn %q when table %q in schema %q already contains column %q",
			column, table, schema, column,
		))
	}

	e.tables[schema][table] = append(e.tables[schema][table], ExistingColumn{
		Name:               column,
		Nullable:           nullable,
		Type:               endpointType,
		CharacterMaxLength: maxLength,
	})
}

func (e *ExistingColumns) GetColumn(schema string, table string, column string) (ExistingColumn, error) {
	if exists, err := e.hasColumn(schema, table, column); err != nil {
		return ExistingColumn{}, err
	} else if !exists {
		return ExistingColumn{}, fmt.Errorf("column %q does not exist table %q of schema %q", column, table, schema)
	}

	cols := e.tables[schema][table]
	for _, c := range cols {
		if column == c.Name {
			return c, nil
		}
	}
	panic("not reached")
}

func (e *ExistingColumns) hasTable(schema, table string) bool {
	if _, ok := e.tables[schema]; !ok {
		return false
	} else if _, ok := e.tables[schema][table]; !ok {
		return false
	}

	return true
}

func (e *ExistingColumns) hasColumn(schema, table, column string) (bool, error) {
	if !e.hasTable(schema, table) {
		// This _probably_ shouldn't ever happen, but might if the user dropped a table without
		// removing the binding for it from their spec *and* is doing something that would require
		// altering the table.
		return false, fmt.Errorf("table '%s.%s' not found in destination, but binding still exists", schema, table)
	}

	exists := slices.ContainsFunc(e.tables[schema][table], func(ec ExistingColumn) bool {
		return column == ec.Name
	})

	return exists, nil
}

func (e *ExistingColumns) nullable(schema, table, column string) (bool, error) {
	exists, err := e.hasColumn(schema, table, column)
	if err != nil {
		return false, fmt.Errorf("unable to determine nullability of column %q: %w", column, err)
	}
	if !exists {
		// Similar to the comment in `hasColumn`, the only way this could happen is if a column was
		// dropped in a bound table without removing that field from the materialization, which is
		// pretty unlikely.
		return false, fmt.Errorf("could not find column %q in table '%s.%s' in existing tables, but field is still part of the materialization", column, schema, table)
	}

	nullable := slices.ContainsFunc(e.tables[schema][table], func(ec ExistingColumn) bool {
		return column == ec.Name && ec.Nullable
	})

	return nullable, nil
}

// FilterActions takes a set of ApplyActions and filters it down to only those that (still) need to
// be executed based on the reported state of the destination per `existing`. Generally speaking,
// this allows a re-application attempt of a previously failed application to pick up where the
// previous one left off, and still make incremental progress. This could be important for
// destination systems that do not support efficient bulk actions of certain kinds.
func FilterActions(in ApplyActions, dialect Dialect, existing *ExistingColumns) (ApplyActions, error) {
	out := ApplyActions{}

	// Only create tables that don't already exist. TODO(whb): Consider checking the existence of
	// columns, nullability, and data types of columns for tables that do already exist where the
	// spec proposes creating the table. It would be nice to at least give a descriptive error
	// message if the existing table isn't compatible with the materialization, and perhaps even
	// transform the table creation action into table alteration actions to make the table
	// compatible.
	for _, tc := range in.CreateTables {
		if !existing.hasTable(tc.InfoLocation.TableSchema, tc.InfoLocation.TableName) {
			out.CreateTables = append(out.CreateTables, tc)
		}
	}

	countAddColumns := 0
	countAlterColumns := 0

	for _, ta := range in.AlterTables {
		alter := TableAlter{
			Table: ta.Table,
		}

		// Only add columns that don't already exist.
		for _, c := range ta.AddColumns {
			if exists, err := existing.hasColumn(ta.InfoLocation.TableSchema, ta.InfoLocation.TableName, dialect.ColumnLocator(c.Field)); err != nil {
				return ApplyActions{}, err
			} else if !exists {
				alter.AddColumns = append(alter.AddColumns, c)
				countAddColumns++
			}
		}

		// Only drop nullability constraints for columns that are not already nullable.
		for _, c := range ta.DropNotNulls {
			if nullable, err := existing.nullable(ta.InfoLocation.TableSchema, ta.InfoLocation.TableName, dialect.ColumnLocator(c.Field)); err != nil {
				return ApplyActions{}, err
			} else if !nullable {
				alter.DropNotNulls = append(alter.DropNotNulls, c)
				countAlterColumns++
			}
		}

		if len(alter.AddColumns) > 0 || len(alter.DropNotNulls) > 0 {
			out.AlterTables = append(out.AlterTables, alter)
		}
	}

	log.WithFields(log.Fields{
		"createTables": len(out.CreateTables),
		"addColumns":   countAddColumns,
		"alterColumns": countAlterColumns,
	}).Info("required apply actions")

	return out, nil
}

// FetchExistingColumns returns the existing columns for implementations that use a standard *sql.DB
// and make a compliant INFORMATION_SCHEMA view available.
func FetchExistingColumns(
	ctx context.Context,
	db *sql.DB,
	dialect Dialect,
	catalog string, // typically the "database"
	schemas []string,
) (*ExistingColumns, error) {
	existingColumns := &ExistingColumns{}

	if len(schemas) == 0 {
		return existingColumns, nil
	}

	// Map the schemas to an appropriate identifier for inclusion in the coming query.
	for i := range schemas {
		schemas[i] = dialect.Literal(schemas[i])
	}

	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
		select table_schema, table_name, column_name, is_nullable, data_type, character_maximum_length
		from information_schema.columns
		where table_catalog = %s
		and table_schema in (%s);
		`,
		dialect.Literal(catalog),
		strings.Join(schemas, ","),
	))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type columnRow struct {
		TableSchema            string
		TableName              string
		ColumnName             string
		IsNullable             string
		DataType               string
		CharacterMaximumLength sql.NullInt64
	}

	for rows.Next() {
		var c columnRow
		if err := rows.Scan(&c.TableSchema, &c.TableName, &c.ColumnName, &c.IsNullable, &c.DataType, &c.CharacterMaximumLength); err != nil {
			return nil, err
		}

		existingColumns.PushColumn(
			c.TableSchema,
			c.TableName,
			c.ColumnName,
			strings.EqualFold(c.IsNullable, "yes"),
			c.DataType,
			int(c.CharacterMaximumLength.Int64),
		)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return existingColumns, nil
}

// ResolveActions determines the minimal set of actions needed to be taken to satisfy a total list
// of actions as calculated by comparing a persisted spec and proposed spec. It does this by
// querying the INFORMATION_SCHEMA view of the destination system to filter out any actions that may
// have already been taken. It is compatible with implementations that use a standard *sql.DB and
// make an INFORMATION_SCHEMA view available.
func ResolveActions(
	ctx context.Context,
	db *sql.DB,
	in ApplyActions,
	dialect Dialect,
	catalog string, // typically the "database"
) (ApplyActions, error) {
	// Determine which schemas are in scope for querying the existing columns. This will be at a
	// minimum the endpoint schema, which is where the metadata tables go and typically where tables
	// are created unless they have a separate explicit schema.

	if len(in.CreateTables) == 0 {
		// Sanity check: At least one of the "specs" or "checkpoints" tables must always be included
		// in the ApplyActions as a CreateTable for now. This is how the endpoint-level schema is
		// determined.
		return ApplyActions{}, fmt.Errorf("logic error: at least one CreateTable must be present in ApplyActions")
	}

	var schemas []string
	for _, t := range in.CreateTables {
		if !slices.Contains(schemas, t.InfoLocation.TableSchema) {
			schemas = append(schemas, t.InfoLocation.TableSchema)
		}
	}
	for _, t := range in.AlterTables {
		if !slices.Contains(schemas, t.InfoLocation.TableSchema) {
			schemas = append(schemas, t.InfoLocation.TableSchema)
		}
	}

	existing, err := FetchExistingColumns(ctx, db, dialect, catalog, schemas)
	if err != nil {
		return ApplyActions{}, fmt.Errorf("fetching existing columns: %w", err)
	}

	filtered, err := FilterActions(in, dialect, existing)
	if err != nil {
		return ApplyActions{}, fmt.Errorf("filtering actions: %w", err)
	}

	return filtered, nil
}
