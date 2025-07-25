package main

import (
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	mssqldb "github.com/microsoft/go-mssqldb"
)

var _ sql.SchemaManager = (*client)(nil)

type client struct {
	db *stdsql.DB
	ep *sql.Endpoint[config]
}

func newClient(ctx context.Context, ep *sql.Endpoint[config]) (sql.Client, error) {
	db, err := stdsql.Open("sqlserver", ep.Config.ToURI())
	if err != nil {
		return nil, err
	}

	return &client{
		db: db,
		ep: ep,
	}, nil
}

func preReqs(ctx context.Context, cfg config, tenant string) *cerrors.PrereqErr {
	errs := &cerrors.PrereqErr{}

	db, err := stdsql.Open("sqlserver", cfg.ToURI())
	if err != nil {
		errs.Err(err)
		return errs
	}

	// Use a reasonable timeout for this connection test. It is not uncommon for a misconfigured
	// connection (wrong host, wrong port, etc.) to hang for several minutes on Ping and we want to
	// bail out well before then.
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		// Provide a more user-friendly representation of some common error causes.
		var sqlServerErr *mssqldb.Error
		var netConnErr *net.DNSError
		var netOpErr *net.OpError

		if errors.As(err, &sqlServerErr) {
			// See SQLServer error reference: https://learn.microsoft.com/en-us/sql/relational-databases/errors-events/database-engine-events-and-errors?view=sql-server-2017
			switch sqlServerErr.Number {
			}
		} else if errors.As(err, &netConnErr) {
			if netConnErr.IsNotFound {
				err = fmt.Errorf("host at address %q cannot be found", cfg.Address)
			}
		} else if errors.As(err, &netOpErr) {
			if netOpErr.Timeout() {
				err = fmt.Errorf("connection to host at address %q timed out (incorrect host or port?)", cfg.Address)
			}
		}

		errs.Err(err)
	}

	return errs
}

func (c *client) PopulateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema, resourcePaths [][]string) error {
	return sql.StdPopulateInfoSchema(ctx, is, c.db, c.ep.Dialect, c.ep.Config.Database, resourcePaths)
}

var columnMigrationSteps = []sql.ColumnMigrationStep{
	func(dialect sql.Dialect, table sql.Table, instructions []sql.MigrationInstruction) ([]string, error) {
		var queries []string

		for _, ins := range instructions {
			queries = append(
				queries,
				fmt.Sprintf("ALTER TABLE %s ADD %s %s;",
					table.Identifier,
					ins.TempColumnIdentifier,
					ins.TypeMigration.NullableDDL,
				),
			)
		}

		return queries, nil
	},
	func(dialect sql.Dialect, table sql.Table, instructions []sql.MigrationInstruction) ([]string, error) {
		var query strings.Builder
		query.WriteString(fmt.Sprintf("UPDATE %s SET ", table.Identifier))

		for i, ins := range instructions {
			if i > 0 {
				query.WriteString(", ")
			}
			query.WriteString(fmt.Sprintf("%s = %s", ins.TempColumnIdentifier, ins.TypeMigration.CastSQL(ins.TypeMigration)))
		}

		return []string{query.String()}, nil
	},
	sql.StdMigrationSteps[2],
	func(dialect sql.Dialect, table sql.Table, instructions []sql.MigrationInstruction) ([]string, error) {
		var queries []string

		for _, ins := range instructions {
			var tempColumn = ins.TypeMigration.Field + sql.ColumnMigrationTemporarySuffix
			queries = append(
				queries,
				fmt.Sprintf(
					"EXEC sp_rename '%s.%s', '%s', 'COLUMN';",
					table.Path[0],
					tempColumn,
					ins.TypeMigration.Field,
				),
			)
		}

		return queries, nil
	},
	func(dialect sql.Dialect, table sql.Table, instructions []sql.MigrationInstruction) ([]string, error) {
		var queries []string

		for _, ins := range instructions {
			if ins.TypeMigration.NullableDDL != ins.TypeMigration.DDL {
				queries = append(
					queries,
					fmt.Sprintf(
						"ALTER TABLE %s ALTER COLUMN %s %s;",
						table.Identifier,
						ins.TypeMigration.Identifier,
						ins.TypeMigration.DDL,
					),
				)
			}
		}

		return queries, nil
	},
}

func (c *client) AlterTable(ctx context.Context, ta sql.TableAlter) (string, boilerplate.ActionApplyFn, error) {
	var stmts []string

	// SQL Server supports adding multiple columns in a single statement, but only a single
	// modification per statement.
	if len(ta.AddColumns) > 0 {
		var addColumnsStmt strings.Builder
		if err := renderTemplates(c.ep.Dialect).alterTableColumns.Execute(&addColumnsStmt, ta); err != nil {
			return "", nil, fmt.Errorf("rendering alter table columns statement: %w", err)
		}
		stmts = append(stmts, addColumnsStmt.String())
	}

	if len(ta.DropNotNulls) > 0 {
		// Dropping a NOT NULL constraint requires re-stating the full field definition without the
		// NOT NULL part. The `data_type` from the information_schema.columns can mostly be used as
		// the column's definition. Some columns have a length component which must be included, and
		// string-type columns also have an collation that needs to be considered. Flow will create
		// new string-type columns with a selected collation, but won't change the collation for
		// pre-existing columns.
		tableDetails, err := tableDetails(ctx, c.db, c.ep.Dialect, c.ep.Config.Database, ta.InfoLocation.TableSchema, ta.InfoLocation.TableName)
		if err != nil {
			return "", nil, fmt.Errorf("getting table details: %w", err)
		}

		for _, dn := range ta.DropNotNulls {
			col, ok := tableDetails[dn.Name]
			if !ok {
				return "", nil, fmt.Errorf("could not find column info for %q", dn.Name)
			}

			ddl, err := col.nullableDDL()
			if err != nil {
				return "", nil, err
			}

			stmts = append(stmts, fmt.Sprintf(
				"ALTER TABLE %s ALTER COLUMN %s %s;",
				ta.Identifier,
				c.ep.Dialect.Identifier(dn.Name),
				ddl,
			))
		}
	}

	if len(ta.ColumnTypeChanges) > 0 {
		if steps, err := sql.StdColumnTypeMigrations(ctx, c.ep.Dialect, ta.Table, ta.ColumnTypeChanges, columnMigrationSteps...); err != nil {
			return "", nil, fmt.Errorf("rendering column migration steps: %w", err)
		} else {
			stmts = append(stmts, steps...)
		}
	}

	return strings.Join(stmts, "\n"), func(ctx context.Context) error {
		for _, stmt := range stmts {
			if _, err := c.db.ExecContext(ctx, stmt); err != nil {
				return err
			}
		}
		return nil
	}, nil
}

func (c *client) ListSchemas(ctx context.Context) ([]string, error) {
	return sql.StdListSchemas(ctx, c.db)
}

func (c *client) CreateSchema(ctx context.Context, schemaName string) (string, error) {
	return sql.StdCreateSchema(ctx, c.db, c.ep.Dialect, schemaName)
}

func (c *client) CreateTable(ctx context.Context, tc sql.TableCreate) error {
	_, err := c.db.ExecContext(ctx, tc.TableCreateSql)
	return err
}

func (c *client) DeleteTable(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	stmt := fmt.Sprintf("DROP TABLE %s;", c.ep.Dialect.Identifier(path...))

	return stmt, func(ctx context.Context) error {
		_, err := c.db.ExecContext(ctx, stmt)
		return err
	}, nil
}

func (c *client) InstallFence(ctx context.Context, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	return installFence(ctx, c.ep.Dialect, c.db, checkpoints, fence, base64.StdEncoding.DecodeString)
}

func (c *client) ExecStatements(ctx context.Context, statements []string) error {
	return sql.StdSQLExecStatements(ctx, c.db, statements)
}

func (c *client) Close() {
	c.db.Close()
}

func tableDetails(ctx context.Context, db *stdsql.DB, dialect sql.Dialect, catalog string, tableSchema string, tableName string) (map[string]foundColumn, error) {
	columns := make(map[string]foundColumn)

	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
		select column_name, is_nullable, data_type, character_maximum_length, collation_name, numeric_precision, numeric_scale, datetime_precision
		from information_schema.columns where table_catalog=%s and table_schema=%s and table_name=%s
		`,
		dialect.Literal(catalog),
		dialect.Literal(tableSchema),
		dialect.Literal(tableName),
	))
	if err != nil {
		return nil, fmt.Errorf("querying table %q in schema %q for table details: %w", tableSchema, tableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var col foundColumn
		if err := rows.Scan(&col.Name, &col.Nullable, &col.Type, &col.CharacterMaximumLength, &col.CollationName, &col.NumericPrecision, &col.NumericScale, &col.DatetimePrecision); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		columns[col.Name] = col
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("closing rows: %w", err)
	}

	return columns, nil
}

type foundColumn struct {
	Name                   string
	Nullable               string
	Type                   string
	CollationName          stdsql.NullString
	CharacterMaximumLength stdsql.NullInt64
	NumericPrecision       stdsql.NullInt64
	NumericScale           stdsql.NullInt64
	DatetimePrecision      stdsql.NullInt64
}

func (c foundColumn) nullableDDL() (string, error) {
	ddl := strings.ToUpper(c.Type)

	intOrMax := func(i int64) string {
		if i == -1 {
			return "MAX"
		}
		return strconv.Itoa(int(i))
	}

	if slices.Contains([]string{"DECIMAL", "NUMERIC"}, ddl) { // Precision and scale.
		ddl += fmt.Sprintf("(%d,%d)", c.NumericPrecision.Int64, c.NumericScale.Int64)
	} else if slices.Contains([]string{"TIME", "DATETIME2", "DATETIMEOFFSET"}, ddl) { // Datetime precision.
		ddl += fmt.Sprintf("(%d)", c.DatetimePrecision.Int64)
	} else if c.CharacterMaximumLength.Valid && ddl != "TEXT" { // TEXT always uses the maximum allowable length.
		ddl += fmt.Sprintf("(%s)", intOrMax(c.CharacterMaximumLength.Int64))
	}

	if c.CollationName.Valid {
		ddl += fmt.Sprintf(" COLLATE %s", c.CollationName.String)
	}

	return ddl, nil
}
