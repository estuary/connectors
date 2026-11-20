package connector

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"regexp"
	"slices"
	"strings"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

var _ sql.Client = (*client)(nil)

type client struct {
	db *stdsql.DB
	ep *sql.Endpoint[config]
}

func newClient(ctx context.Context, materializationName string, ep *sql.Endpoint[config]) (sql.Client, error) {
	db, err := openDB(ep.Config)
	if err != nil {
		return nil, err
	}
	return &client{db: db, ep: ep}, nil
}

var timestampPrecision = regexp.MustCompile(`^TIMESTAMP\([0-9]+\)`)

func (c *client) PopulateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema, resourcePaths [][]string) error {
	if len(resourcePaths) == 0 {
		return nil
	}
	var schemas []string
	for _, path := range resourcePaths {
		schemas = append(schemas, c.ep.Dialect.Literal(c.ep.Dialect.TableLocator(path).TableSchema))
	}
	slices.Sort(schemas)
	schemas = slices.Compact(schemas)

	rows, err := c.db.QueryContext(ctx, "SELECT USERNAME FROM ALL_USERS WHERE USERNAME IN ("+strings.Join(schemas, ",")+")")
	if err != nil {
		return fmt.Errorf("querying schemas: %w", err)
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return err
		}
		is.PushNamespace(name)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	// DATA_LENGTH is in bytes, like the VARCHAR2 sizes the dialect maps, and
	// is only a size for character types.
	rows, err = c.db.QueryContext(ctx, `
SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, NULLABLE, DATA_LENGTH, DEFAULT_LENGTH, DATA_PRECISION, DATA_SCALE
FROM ALL_TAB_COLUMNS WHERE OWNER IN (`+strings.Join(schemas, ",")+`) ORDER BY OWNER, TABLE_NAME, COLUMN_ID`)
	if err != nil {
		return fmt.Errorf("querying columns: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var schema, table, column, dataType, nullable string
		var dataLength, defaultLength, precision, scale stdsql.NullInt64
		if err := rows.Scan(&schema, &table, &column, &dataType, &nullable, &dataLength, &defaultLength, &precision, &scale); err != nil {
			return err
		}
		is.PushResource(schema, table).PushField(boilerplate.ExistingField{
			Name:               column,
			Nullable:           nullable == "Y",
			Type:               existingColumnType(dataType, precision, scale),
			CharacterMaxLength: existingCharacterLength(dataType, dataLength.Int64),
			HasDefault:         defaultLength.Valid && defaultLength.Int64 > 0,
		})
	}
	return rows.Err()
}

// existingColumnType is the type reported for an existing column. NUMBER
// columns include their precision and scale, so that a NUMBER(1) boolean
// column is not taken for an integer column; a NUMBER without precision
// holds any integer.
func existingColumnType(dataType string, precision, scale stdsql.NullInt64) string {
	if !strings.EqualFold(dataType, "NUMBER") {
		return timestampPrecision.ReplaceAllString(dataType, "TIMESTAMP")
	} else if !precision.Valid && (!scale.Valid || scale.Int64 == 0) {
		return dataType
	} else if !precision.Valid {
		return fmt.Sprintf("%s(*,%d)", dataType, scale.Int64)
	} else if !scale.Valid || scale.Int64 == 0 {
		return fmt.Sprintf("%s(%d)", dataType, precision.Int64)
	}
	return fmt.Sprintf("%s(%d,%d)", dataType, precision.Int64, scale.Int64)
}

func (c *client) CreateTable(ctx context.Context, tc sql.TableCreate) error {
	_, err := c.db.ExecContext(ctx, tc.TableCreateSql)
	return err
}

func (c *client) DeleteTable(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	stmt := "DROP TABLE " + c.ep.Dialect.Identifier(path...)
	return stmt, func(ctx context.Context) error {
		_, err := c.db.ExecContext(ctx, stmt)
		return err
	}, nil
}

func (c *client) TruncateTable(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	stmt := "TRUNCATE TABLE " + c.ep.Dialect.Identifier(path...)
	return stmt, func(ctx context.Context) error {
		_, err := c.db.ExecContext(ctx, stmt)
		return err
	}, nil
}

// columnMigrationSteps migrate a column by copying it into a temporary column
// of the new type, which then replaces it.
var columnMigrationSteps = []sql.ColumnMigrationStep{
	func(_ sql.Dialect, table sql.Table, instructions []sql.MigrationInstruction) ([]string, error) {
		var queries []string
		for _, ins := range instructions {
			queries = append(queries, fmt.Sprintf("ALTER TABLE %s ADD (%s %s)", table.Identifier, ins.TempColumnIdentifier, ins.TypeMigration.NullableDDL))
		}
		return queries, nil
	},
	func(_ sql.Dialect, table sql.Table, instructions []sql.MigrationInstruction) ([]string, error) {
		var sets []string
		for _, ins := range instructions {
			sets = append(sets, fmt.Sprintf("%s = %s", ins.TempColumnIdentifier, ins.TypeMigration.CastSQL(ins.TypeMigration)))
		}
		return []string{fmt.Sprintf("UPDATE %s SET %s", table.Identifier, strings.Join(sets, ", "))}, nil
	},
	sql.StdMigrationSteps[2],
	sql.StdMigrationSteps[3],
	func(_ sql.Dialect, table sql.Table, instructions []sql.MigrationInstruction) ([]string, error) {
		var queries []string
		for _, ins := range instructions {
			if ins.TypeMigration.NullableDDL != ins.TypeMigration.DDL {
				queries = append(queries, fmt.Sprintf("ALTER TABLE %s MODIFY (%s NOT NULL)", table.Identifier, ins.TypeMigration.Identifier))
			}
		}
		return queries, nil
	},
}

func (c *client) AlterTable(ctx context.Context, ta sql.TableAlter) (string, boilerplate.ActionApplyFn, error) {
	var statements []string
	for _, col := range ta.AddColumns {
		statements = append(statements, fmt.Sprintf("ALTER TABLE %s ADD (%s %s)", ta.Identifier, col.Identifier, col.NullableDDL))
	}
	for _, col := range ta.DropNotNulls {
		statements = append(statements, fmt.Sprintf("ALTER TABLE %s MODIFY (%s NULL)", ta.Identifier, c.ep.Dialect.Identifier(col.Name)))
	}
	if len(ta.ColumnTypeChanges) != 0 {
		steps, err := sql.StdColumnTypeMigrations(ctx, c.ep.Dialect, ta.Table, ta.ColumnTypeChanges, columnMigrationSteps...)
		if err != nil {
			return "", nil, fmt.Errorf("rendering column migration steps: %w", err)
		}
		statements = append(statements, steps...)
	}
	return strings.Join(statements, "\n"), func(ctx context.Context) error {
		return c.ExecStatements(ctx, statements)
	}, nil
}

func (c *client) InstallFence(ctx context.Context, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	return installFence(ctx, c.db, checkpoints, fence)
}

func (c *client) ExecStatements(ctx context.Context, statements []string) error {
	return sql.StdSQLExecStatements(ctx, c.db, statements)
}

func (c *client) MustRecreateResource(req *pm.Request_Apply, lastBinding, newBinding *pf.MaterializationSpec_Binding) (bool, error) {
	return lastBinding.DeltaUpdates != newBinding.DeltaUpdates || !slices.Equal(lastBinding.FieldSelection.Keys, newBinding.FieldSelection.Keys), nil
}

func (c *client) checkpointsIdentifier() string {
	return c.ep.Dialect.Identifier(c.ep.MetaCheckpoints.Path...)
}

func (c *client) ListCheckpointsEntries(ctx context.Context) ([]string, error) {
	rows, err := c.db.QueryContext(ctx, "SELECT DISTINCT MATERIALIZATION FROM "+c.checkpointsIdentifier())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

func (c *client) DeleteCheckpointsEntry(ctx context.Context, taskName string) error {
	_, err := c.db.ExecContext(ctx, "DELETE FROM "+c.checkpointsIdentifier()+" WHERE MATERIALIZATION = :1", taskName)
	return err
}

func (c *client) SnapshotTestTable(ctx context.Context, path []string) ([]string, [][]any, error) {
	identifier := c.ep.Dialect.Identifier(path...)
	return sql.SnapshotTestTableQuery(ctx, c.db, identifier, "SELECT * FROM "+identifier)
}

func (c *client) Close() {
	c.db.Close()
}
