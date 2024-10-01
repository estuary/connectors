package main

import (
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/estuary/flow/go/protocols/flow"
	"github.com/go-sql-driver/mysql"
)

type client struct {
	db         *stdsql.DB
	cfg        *config
	ep         *sql.Endpoint
	tzLocation *time.Location
}

func prepareNewClient(tzLocation *time.Location) func(ctx context.Context, ep *sql.Endpoint) (sql.Client, error) {
	return func(ctx context.Context, ep *sql.Endpoint) (sql.Client, error) {
		cfg := ep.Config.(*config)

		db, err := stdsql.Open("mysql", cfg.ToURI())
		if err != nil {
			return nil, err
		}

		return &client{
			db:         db,
			cfg:        cfg,
			ep:         ep,
			tzLocation: tzLocation,
		}, nil
	}
}

func preReqs(ctx context.Context, conf any, tenant string) *sql.PrereqErr {
	errs := &sql.PrereqErr{}

	cfg := conf.(*config)

	db, err := stdsql.Open("mysql", cfg.ToURI())
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
		var mysqlErr *mysql.MySQLError
		var netConnErr *net.DNSError
		var netOpErr *net.OpError

		if errors.As(err, &mysqlErr) {
			// See MySQL error reference: https://dev.mysql.com/doc/mysql-errors/5.7/en/error-reference-introduction.html
			switch mysqlErr.Number {
			case 1045:
				err = fmt.Errorf("incorrect username or password (%d): %s", mysqlErr.Number, mysqlErr.Message)
			case 1049:
				err = fmt.Errorf("database %q cannot be accessed, it might not exist or you do not have permission to access it (%d): %s", cfg.Database, mysqlErr.Number, mysqlErr.Message)
			case 1044:
				err = fmt.Errorf("database %q cannot be accessed, it might not exist or you do not have permission to access it (%d): %s", cfg.Database, mysqlErr.Number, mysqlErr.Message)
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
	} else {
		var row = db.QueryRowContext(ctx, "SELECT @@GLOBAL.local_infile;")
		var localInFileEnabled bool

		if err := row.Scan(&localInFileEnabled); err != nil {
			errs.Err(fmt.Errorf("could not read `local_infile` global variable: %w", err))
		} else if !localInFileEnabled {
			errs.Err(fmt.Errorf("`local_infile` global variable must be enabled on your mysql server. You can enable this using `SET GLOBAL local_infile = true`"))
		}
	}

	return errs
}

func (c *client) InfoSchema(ctx context.Context, resourcePaths [][]string) (is *boilerplate.InfoSchema, err error) {
	return sql.StdFetchInfoSchema(ctx, c.db, c.ep.Dialect, "def", resourcePaths)
}

func formatOffset(loc *time.Location) string {
	now := time.Now().In(loc)
	_, offset := now.Zone()

	sign := "+"
	if offset < 0 {
		sign = "-"
		offset = -offset
	}
	hours := offset / 3600
	minutes := (offset % 3600) / 60
	return fmt.Sprintf("%s%02d:%02d", sign, hours, minutes)
}

func castRfc3339ToDateTime(loc *time.Location, identifier string) string {
	return fmt.Sprintf("CONVERT_TZ(STR_TO_DATE(SUBSTRING(%s, 1, 19), '%%Y-%%m-%%dT%%H:%%i:%%s'), '+00:00', '%s')", identifier, formatOffset(loc))
}

func (c *client) columnMigrationSteps(ctx context.Context) []sql.ColumnMigrationStep {
	return []sql.ColumnMigrationStep{
		func(dialect sql.Dialect, table sql.Table, migration sql.ColumnTypeMigration, tempColumnIdentifier string) (string, error) {
			return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s;",
				table.Identifier,
				tempColumnIdentifier,
				migration.DDL,
			), nil
		},
		func(dialect sql.Dialect, table sql.Table, migration sql.ColumnTypeMigration, tempColumnIdentifier string) (string, error) {
			// Migrating from string to datetime
			var cast = migration.Identifier
			if migration.DDL == "DATETIME(6)" {
				cast = castRfc3339ToDateTime(c.tzLocation, migration.Identifier)
			}
			return fmt.Sprintf(
				"UPDATE %s SET %s = %s;",
				table.Identifier,
				tempColumnIdentifier,
				cast,
			), nil
		},
		func(dialect sql.Dialect, table sql.Table, migration sql.ColumnTypeMigration, _ string) (string, error) {
			return fmt.Sprintf(
				"ALTER TABLE %s DROP COLUMN %s;",
				table.Identifier,
				migration.Identifier,
			), nil
		},
		func(dialect sql.Dialect, table sql.Table, migration sql.ColumnTypeMigration, tempColumnIdentifier string) (string, error) {
			return fmt.Sprintf(
				"ALTER TABLE %s CHANGE COLUMN %s %s %s;",
				table.Identifier,
				tempColumnIdentifier,
				migration.Identifier,
				migration.DDL,
			), nil
		},
	}
}

func (c *client) AlterTable(ctx context.Context, ta sql.TableAlter) (string, boilerplate.ActionApplyFn, error) {
	var stmts []string

	if len(ta.DropNotNulls) > 0 {
		// In order to drop a NOT NULL constraint, the full field definition must be re-stated
		// without the NOT NULL part. There may be columns that aren't and/or never were part of our
		// collection specification, so we must determine what appropriate DDL for columns that will
		// be made nullable dynamically.
		colDDL := make(map[string]string)

		rows, err := c.db.QueryContext(ctx, fmt.Sprintf(
			"select column_name, column_type from information_schema.columns where table_schema=%s and table_name=%s;",
			c.ep.Dialect.Literal(ta.InfoLocation.TableSchema),
			c.ep.Dialect.Literal(ta.InfoLocation.TableName),
		))
		if err != nil {
			return "", nil, fmt.Errorf("querying table %q in schema %q for column_type: %w", ta.InfoLocation.TableName, ta.InfoLocation.TableSchema, err)
		}
		defer rows.Close()

		for rows.Next() {
			var columnName, columnType string
			if err := rows.Scan(&columnName, &columnType); err != nil {
				return "", nil, fmt.Errorf("scanning row: %w", err)
			}
			colDDL[columnName] = columnType
		}
		if err := rows.Err(); err != nil {
			return "", nil, fmt.Errorf("closing rows: %w", err)
		}

		for idx := range ta.DropNotNulls {
			col := ta.DropNotNulls[idx].Name
			ddl, ok := colDDL[col]
			if !ok {
				return "", nil, fmt.Errorf("could not determine DDL for column %q", col)
			}
			// Swap out the "type" of the column with the DDL reported from the database.
			// column_type is (at least somewhat) unique to MySQL and is different from the standard
			// data_type. It is the specific DDL needed to create the column, minus the NOT NULL
			// part.
			ta.DropNotNulls[idx].Type = ddl
		}
	}

	if len(ta.DropNotNulls) > 0 || len(ta.AddColumns) > 0 {
		var alterColumnStmtBuilder strings.Builder
		if err := renderTemplates(c.ep.Dialect).alterTableColumns.Execute(&alterColumnStmtBuilder, ta); err != nil {
			return "", nil, fmt.Errorf("rendering alter table columns statement: %w", err)
		}
		alterColumnStmt := alterColumnStmtBuilder.String()

		stmts = append(stmts, alterColumnStmt)
	}

	if len(ta.ColumnTypeChanges) > 0 {
		for _, m := range ta.ColumnTypeChanges {
			if steps, err := sql.StdColumnTypeMigration(ctx, c.ep.Dialect, ta.Table, m, c.columnMigrationSteps(ctx)...); err != nil {
				return "", nil, fmt.Errorf("rendering column migration steps: %w", err)
			} else {
				stmts = append(stmts, steps...)
			}
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

func (c *client) PutSpec(ctx context.Context, updateSpec sql.MetaSpecsUpdate) error {
	_, err := c.db.ExecContext(ctx, updateSpec.ParameterizedQuery, updateSpec.Parameters...)
	return err
}

func (c *client) InstallFence(ctx context.Context, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	return sql.StdInstallFence(ctx, c.db, checkpoints, fence, base64.StdEncoding.DecodeString)
}

func (c *client) ExecStatements(ctx context.Context, statements []string) error {
	return sql.StdSQLExecStatements(ctx, c.db, statements)
}

func (c *client) FetchSpecAndVersion(ctx context.Context, specs sql.Table, materialization flow.Materialization) (string, string, error) {
	return sql.StdFetchSpecAndVersion(ctx, c.db, specs, materialization)
}

func (c *client) Close() {
	c.db.Close()
}
