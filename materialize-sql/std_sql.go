package sql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
)

// StdFetchSpecAndVersion is a convenience for Client implementations which
// use Go's standard `sql.DB` type under the hood.
func StdFetchSpecAndVersion(ctx context.Context, db *sql.DB, specs Table, materialization pf.Materialization) (spec, version string, err error) {
	// Fail-fast: surface a connection issue.
	if err = db.PingContext(ctx); err != nil {
		err = fmt.Errorf("connecting to DB: %w", err)
		return
	}
	err = db.QueryRowContext(
		ctx,
		fmt.Sprintf(
			"SELECT version, spec FROM %s WHERE materialization = %s;",
			specs.Identifier,
			specs.Keys[0].Placeholder,
		),
		materialization.String(),
	).Scan(&version, &spec)

	return
}

// StdSQLExecStatements is a convenience for Client implementations which
// use Go's standard `sql.DB` type under the hood.
func StdSQLExecStatements(ctx context.Context, db *sql.DB, statements []string) error {
	// Obtain a verified connection to the database.
	// We don't explicitly wrap `statements` in a transaction, as not all
	// databases support transactional DDL statements, but we do run them
	// through a single connection. This allows a driver to explicitly run
	// `BEGIN;` and `COMMIT;` statements around a transactional operation.
	var conn, err = db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("connecting to DB: %w", err)
	}
	defer func() {
		err = conn.Close()
	}()

	if err = conn.PingContext(ctx); err != nil {
		return fmt.Errorf("ping DB: %w", err)
	}

	for _, statement := range statements {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("executing statement (%s): %w", statement, err)
		}
		log.WithField("sql", statement).Debug("executed statement")
	}

	return err
}

// StdInstallFence is a convenience for Client implementations which
// use Go's standard `sql.DB` type under the hood.
func StdInstallFence(ctx context.Context, db *sql.DB, checkpoints Table, fence Fence, decodeFence func(string) ([]byte, error)) (Fence, error) {
	var txn, err = db.BeginTx(ctx, nil)
	if err != nil {
		return Fence{}, fmt.Errorf("db.BeginTx: %w", err)
	}
	defer func() {
		if txn != nil {
			_ = txn.Rollback()
		}
	}()

	// Increment the fence value of _any_ checkpoint which overlaps our key range.
	if _, err = txn.Exec(
		fmt.Sprintf(`
			UPDATE %s
				SET fence=fence+1
				WHERE materialization=%s
				AND key_end>=%s
				AND key_begin<=%s
			;
			`,
			checkpoints.Identifier,
			checkpoints.Keys[0].Placeholder,
			checkpoints.Keys[1].Placeholder,
			checkpoints.Keys[2].Placeholder,
		),
		fence.Materialization,
		fence.KeyBegin,
		fence.KeyEnd,
	); err != nil {
		return Fence{}, fmt.Errorf("incrementing fence: %w", err)
	}

	// Read the checkpoint with the narrowest [key_begin, key_end] which fully overlaps our range.
	var readBegin, readEnd uint32
	var checkpoint string

	if err = txn.QueryRow(
		fmt.Sprintf(`
			SELECT fence, key_begin, key_end, checkpoint
				FROM %s
				WHERE materialization=%s
				AND key_begin<=%s
				AND key_end>=%s
				ORDER BY key_end - key_begin ASC
				LIMIT 1
			;
			`,
			checkpoints.Identifier,
			checkpoints.Keys[0].Placeholder,
			checkpoints.Keys[1].Placeholder,
			checkpoints.Keys[2].Placeholder,
		),
		fence.Materialization,
		fence.KeyBegin,
		fence.KeyEnd,
	).Scan(&fence.Fence, &readBegin, &readEnd, &checkpoint); err == sql.ErrNoRows {
		// Set an invalid range, which compares as unequal to trigger an insertion below.
		readBegin, readEnd = 1, 0
	} else if err != nil {
		return Fence{}, fmt.Errorf("scanning fence and checkpoint: %w", err)
	} else if fence.Checkpoint, err = decodeFence(checkpoint); err != nil {
		return Fence{}, fmt.Errorf("decodeFence(checkpoint): %w", err)
	}

	// If a checkpoint for this exact range doesn't exist then insert it now.
	if readBegin == fence.KeyBegin && readEnd == fence.KeyEnd {
		// Exists; no-op.
	} else if _, err = txn.Exec(
		fmt.Sprintf(
			"INSERT INTO %s (materialization, key_begin, key_end, fence, checkpoint) VALUES (%s, %s, %s, %s, %s);",
			checkpoints.Identifier,
			checkpoints.Keys[0].Placeholder,
			checkpoints.Keys[1].Placeholder,
			checkpoints.Keys[2].Placeholder,
			checkpoints.Values[0].Placeholder,
			checkpoints.Values[1].Placeholder,
		),
		fence.Materialization,
		fence.KeyBegin,
		fence.KeyEnd,
		fence.Fence,
		base64.StdEncoding.EncodeToString(fence.Checkpoint),
	); err != nil {
		return Fence{}, fmt.Errorf("inserting fence: %w", err)
	}

	err = txn.Commit()
	txn = nil // Disable deferred rollback.

	if err != nil {
		return Fence{}, fmt.Errorf("txn.Commit: %w", err)
	}
	return fence, nil
}

// StdUpdateFence updates a Fence within the checkpoints Table.
// It's a convenience for Client implementations which use Go's standard `sql.DB` type under the hood.
func StdUpdateFence(ctx context.Context, txn *sql.Tx, checkpoints Table, fence Fence) error {
	var result, err = txn.ExecContext(ctx,
		fmt.Sprintf(
			"UPDATE %s SET checkpoint=%s WHERE materialization=%s AND key_begin=%s AND key_end=%s AND fence=%s;",
			checkpoints.Identifier,
			checkpoints.Values[1].Placeholder,
			checkpoints.Keys[0].Placeholder,
			checkpoints.Keys[1].Placeholder,
			checkpoints.Keys[2].Placeholder,
			checkpoints.Values[0].Placeholder,
		),
		fence.Materialization,
		fence.KeyBegin,
		fence.KeyEnd,
		fence.Fence,
		base64.StdEncoding.EncodeToString(fence.Checkpoint),
	)

	if err != nil {
		return fmt.Errorf("updating fence: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("fetching fence update rows: %w", err)
	} else if rows != 1 {
		return fmt.Errorf("this transactions session was fenced off by another")
	}
	return nil
}

// StdDumpTable returns a debug representation of the contents of a table.
// It's a convenience for Client implementations which use Go's standard `sql.DB` type under the hood.
func StdDumpTable(ctx context.Context, db *sql.DB, table Table) (string, error) {
	var b strings.Builder

	var keys []string
	var all []string

	for _, key := range table.Keys {
		keys = append(keys, key.Identifier)
		all = append(all, key.Identifier)
	}
	for _, val := range table.Values {
		all = append(all, val.Identifier)
	}

	var sql = fmt.Sprintf("select %s from %s order by %s asc;",
		strings.Join(all, ","),
		table.Identifier,
		strings.Join(keys, ","))

	rows, err := db.Query(sql)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	b.WriteString(strings.Join(all, ", "))

	for rows.Next() {
		var data = make([]anyColumn, len(table.Columns()))
		var ptrs = make([]interface{}, len(table.Columns()))
		for i := range data {
			ptrs[i] = &data[i]
		}
		if err = rows.Scan(ptrs...); err != nil {
			return "", err
		}
		b.WriteString("\n")
		for i, v := range ptrs {
			if i > 0 {
				b.WriteString(", ")
			}
			var val = v.(*anyColumn)
			b.WriteString(val.String())
		}
	}
	return b.String(), nil
}

type anyColumn string

func (col *anyColumn) Scan(i interface{}) error {
	var sval string

	switch ii := i.(type) {
	case []byte:
		sval = string(ii)
	case string:
		if _, err := strconv.Atoi(ii); err == nil {
			// Snowflake integer value columns scan into an interface{} with a concrete type of
			// string.
			sval = fmt.Sprint(i)
		} else if hexBytes, err := hex.DecodeString(ii); err == nil {
			// Redshift checkpoint columns have an additional layer of hex encoding.
			sval = string(hexBytes)
		} else {
			sval = fmt.Sprint(i)
		}

	default:
		sval = fmt.Sprint(i)
	}
	*col = anyColumn(sval)
	return nil
}
func (col anyColumn) String() string {
	return string(col)
}

// StdGetSchema is a convenience function for getting a formatted schema for a table for Client
// implementations which use Go's standard `sql.DB` type and systems with an information_schema
// schema.
func StdGetSchema(ctx context.Context, db *sql.DB, catalog string, schema string, name string) (string, error) {
	q := fmt.Sprintf(`
	select column_name, is_nullable, data_type
	from information_schema.columns
	where 
		table_catalog = '%s' 
		and table_schema = '%s'
		and table_name = '%s';
`,
		catalog,
		schema,
		name,
	)

	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	type foundColumn struct {
		Name     string
		Nullable string // string "YES" or "NO"
		Type     string
	}

	cols := []foundColumn{}
	for rows.Next() {
		var c foundColumn
		if err := rows.Scan(&c.Name, &c.Nullable, &c.Type); err != nil {
			return "", err
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	slices.SortFunc(cols, func(a, b foundColumn) int {
		return strings.Compare(a.Name, b.Name)
	})

	var out strings.Builder
	enc := json.NewEncoder(&out)
	for _, c := range cols {
		if err := enc.Encode(c); err != nil {
			return "", err
		}
	}

	return out.String(), nil
}

// StdFetchInfoSchema returns the existing columns for implementations that use a standard *sql.DB
// and make a compliant INFORMATION_SCHEMA view available.
func StdFetchInfoSchema(
	ctx context.Context,
	db *sql.DB,
	dialect Dialect,
	catalog string, // typically the "database"
	resourcePaths [][]string,
) (*boilerplate.InfoSchema, error) {
	is := boilerplate.NewInfoSchema(
		ToLocatePathFn(dialect.TableLocator),
		dialect.ColumnLocator,
	)

	if len(resourcePaths) == 0 {
		// Trivial case: No resources, so there are no applicable tables or columns. This is only
		// possible if the materialization has no bindings and the endpoint doesn't have any
		// metadata tables or their table paths are not included in the list of resource paths.
		return is, nil
	}

	// Map the resource paths to an appropriate identifier for inclusion in the coming query.
	schemas := make([]string, 0, len(resourcePaths))
	for _, p := range resourcePaths {
		loc := dialect.TableLocator(p)
		schemas = append(schemas, dialect.Literal(loc.TableSchema))
	}

	slices.Sort(schemas)
	schemas = slices.Compact(schemas)

	// Populate the list of applicable tables first, since it is possible for a table to exist with
	// no columns that we'd otherwise not know about when only looking for columns.
	tables, err := db.QueryContext(ctx, fmt.Sprintf(`
		select table_schema, table_name
		from information_schema.tables
		where table_catalog = %s
		and table_schema in (%s);
		`,
		dialect.Literal(catalog),
		strings.Join(schemas, ","),
	))
	if err != nil {
		return nil, err
	}
	defer tables.Close()

	type tableRow struct {
		TableSchema string
		TableName   string
	}

	for tables.Next() {
		var t tableRow
		if err := tables.Scan(&t.TableSchema, &t.TableName); err != nil {
			return nil, err
		}

		is.PushResource(t.TableSchema, t.TableName)
	}

	// Populate the list of columns.
	columns, err := db.QueryContext(ctx, fmt.Sprintf(`
		select table_schema, table_name, column_name, is_nullable, data_type, character_maximum_length, column_default
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
	defer columns.Close()

	type columnRow struct {
		tableRow
		ColumnName             string
		IsNullable             string
		DataType               string
		CharacterMaximumLength sql.NullInt64
		ColumnDefault          sql.NullString
	}

	for columns.Next() {
		var c columnRow
		if err := columns.Scan(&c.TableSchema, &c.TableName, &c.ColumnName, &c.IsNullable, &c.DataType, &c.CharacterMaximumLength, &c.ColumnDefault); err != nil {
			return nil, err
		}

		is.PushField(boilerplate.EndpointField{
			Name:               c.ColumnName,
			Nullable:           strings.EqualFold(c.IsNullable, "yes"),
			Type:               c.DataType,
			CharacterMaxLength: int(c.CharacterMaximumLength.Int64),
			HasDefault:         c.ColumnDefault.Valid,
		}, c.TableSchema, c.TableName)
	}
	if err := columns.Err(); err != nil {
		return nil, err
	}

	return is, nil
}

type ListSchemasFn func(context.Context) ([]string, error)

type CreateSchemaFn func(context.Context, string) error

func StdListSchemas(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, "select schema_name from information_schema.schemata")
	if err != nil {
		return nil, fmt.Errorf("querying information_schema.schemata: %w", err)
	}
	defer rows.Close()

	out := []string{}

	for rows.Next() {
		var schema string
		if err := rows.Scan(&schema); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		out = append(out, schema)
	}

	return out, nil
}

func StdCreateSchema(ctx context.Context, db *sql.DB, dialect Dialect, schemaName string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf("create schema %s", dialect.Identifier(schemaName)))
	return err
}

type ColumnMigrationStep func(dialect Dialect, table Table, migration ColumnTypeMigration, firstTempColumn, secondTempColumn string) (string, error)

var StdMigrationSteps = []ColumnMigrationStep{
	func(dialect Dialect, table Table, migration ColumnTypeMigration, firstTempColumnIdentifier, _ string) (string, error) {
		return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s;",
			table.Identifier,
			firstTempColumnIdentifier,
			migration.DDL,
		), nil
	},
	func(dialect Dialect, table Table, migration ColumnTypeMigration, firstTempColumnIdentifier, _ string) (string, error) {
		return fmt.Sprintf(
			// The WHERE filter is required by some warehouses (bigquery)
			"UPDATE %s SET %s = CAST(%s AS %s) WHERE true;",
			table.Identifier,
			firstTempColumnIdentifier,
			migration.Identifier,
			migration.DDL,
		), nil
	},
	func(dialect Dialect, table Table, migration ColumnTypeMigration, _, secondTempColumnIdentifier string) (string, error) {
		return fmt.Sprintf(
			"ALTER TABLE %s RENAME COLUMN %s TO %s;",
			table.Identifier,
			migration.Identifier,
			secondTempColumnIdentifier,
		), nil
	},
	func(dialect Dialect, table Table, migration ColumnTypeMigration, firstTempColumnIdentifier, _ string) (string, error) {
		return fmt.Sprintf(
			"ALTER TABLE %s RENAME COLUMN %s TO %s;",
			table.Identifier,
			firstTempColumnIdentifier,
			migration.Identifier,
		), nil
	},
	func(dialect Dialect, table Table, migration ColumnTypeMigration, _, secondTempColumnIdentifier string) (string, error) {
		return fmt.Sprintf(
			"ALTER TABLE %s DROP COLUMN %s;",
			table.Identifier,
			secondTempColumnIdentifier,
		), nil
	},
}

func StdColumnTypeMigration(ctx context.Context, dialect Dialect, table Table, migration ColumnTypeMigration, steps ...ColumnMigrationStep) ([]string, error) {
	var firstTempColumn = migration.Field + ColumnMigrationFirstStepSuffix
	var firstTempColumnIdentifier = dialect.Identifier(firstTempColumn)
	var secondTempColumn = migration.Field + ColumnMigrationSecondStepSuffix
	var secondTempColumnIdentifier = dialect.Identifier(secondTempColumn)

	var step = 0
	if len(migration.ProgressFields) == 1 && migration.ProgressFields[0] == firstTempColumn {
		step = 1
	} else if len(migration.ProgressFields) == 2 {
		step = 3
	} else if len(migration.ProgressFields) == 1 && migration.ProgressFields[0] == secondTempColumn {
		step = 4
	}

	log.WithFields(log.Fields{
		"table":          table.Identifier,
		"column":         migration.Field,
		"progressFields": migration.ProgressFields,
		"ddl":            migration.DDL,
		"step":           step,
	}).Info("migrating columns using column renaming")

	if len(steps) == 0 {
		steps = StdMigrationSteps
	}

	if len(steps) < len(StdMigrationSteps) {
		return nil, fmt.Errorf("must have at least %d steps", len(StdMigrationSteps))
	}

	var renderedSteps = make([]string, len(steps)-step)
	for i, s := range steps[step:] {
		var err error
		renderedSteps[i], err = s(dialect, table, migration, firstTempColumnIdentifier, secondTempColumnIdentifier)
		if err != nil {
			return nil, fmt.Errorf("rendering step %d: %w", i, err)
		}
	}

	return renderedSteps, nil
}

func ToLocatePathFn(fn TableLocatorFn) boilerplate.LocatePathFn {
	return func(in []string) []string {
		loc := fn(in)
		return []string{
			loc.TableSchema,
			loc.TableName,
		}
	}
}
