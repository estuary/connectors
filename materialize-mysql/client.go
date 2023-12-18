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
	db  *stdsql.DB
	cfg *config
	ep  *sql.Endpoint
}

func newClient(ctx context.Context, ep *sql.Endpoint) (sql.Client, error) {
	cfg := ep.Config.(*config)

	db, err := stdsql.Open("mysql", cfg.ToURI())
	if err != nil {
		return nil, err
	}

	return &client{
		db:  db,
		cfg: cfg,
		ep:  ep,
	}, nil
}

func (c *client) PreReqs(ctx context.Context) *sql.PrereqErr {
	errs := &sql.PrereqErr{}

	// Use a reasonable timeout for this connection test. It is not uncommon for a misconfigured
	// connection (wrong host, wrong port, etc.) to hang for several minutes on Ping and we want to
	// bail out well before then.
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	if err := c.db.PingContext(ctx); err != nil {
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
				err = fmt.Errorf("database %q cannot be accessed, it might not exist or you do not have permission to access it (%d): %s", c.cfg.Database, mysqlErr.Number, mysqlErr.Message)
			case 1044:
				err = fmt.Errorf("database %q cannot be accessed, it might not exist or you do not have permission to access it (%d): %s", c.cfg.Database, mysqlErr.Number, mysqlErr.Message)
			}
		} else if errors.As(err, &netConnErr) {
			if netConnErr.IsNotFound {
				err = fmt.Errorf("host at address %q cannot be found", c.cfg.Address)
			}
		} else if errors.As(err, &netOpErr) {
			if netOpErr.Timeout() {
				err = fmt.Errorf("connection to host at address %q timed out (incorrect host or port?)", c.cfg.Address)
			}
		}

		errs.Err(err)
	} else {
		var row = c.db.QueryRowContext(ctx, "SELECT @@GLOBAL.local_infile;")
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
	return sql.StdFetchInfoSchema(ctx, c.db, c.ep.Dialect, "def", c.cfg.Database, resourcePaths)
}

func (c *client) AlterTable(ctx context.Context, ta sql.TableAlter) (string, boilerplate.ActionApplyFn, error) {
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

	var alterColumnStmtBuilder strings.Builder
	if err := renderTemplates(c.ep.Dialect)["alterTableColumns"].Execute(&alterColumnStmtBuilder, ta); err != nil {
		return "", nil, fmt.Errorf("rendering alter table columns statement: %w", err)
	}
	alterColumnStmt := alterColumnStmtBuilder.String()

	return alterColumnStmt, func(ctx context.Context) error {
		_, err := c.db.ExecContext(ctx, alterColumnStmt)
		return checkIdentifierLengthError(err)
	}, nil
}

func (c *client) CreateTable(ctx context.Context, tc sql.TableCreate) error {
	_, err := c.db.ExecContext(ctx, tc.TableCreateSql)
	return checkIdentifierLengthError(err)
}

func (c *client) ReplaceTable(ctx context.Context, tr sql.TableReplace) (string, boilerplate.ActionApplyFn, error) {
	stmts := []string{
		// `TableReplaceSql` is only the "create table" part, and we need to also include the
		// statement to drop the table first. Also see additional comments in sqlgen.go.
		fmt.Sprintf("DROP TABLE IF EXISTS %s;", tr.Identifier),
		tr.TableReplaceSql,
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

func checkIdentifierLengthError(err error) error {
	if err == nil {
		return nil
	}

	// Handling for errors resulting from identifiers being too long.
	// TODO(whb): At some point we could consider moving this into `Validate` to check
	// identifier lengths, rather than passing validate but failing in apply. I'm holding
	// off on that for now since I don't know if the 64 character limit is universal, or
	// specific to certain versions of MySQL/MariaDB.
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		// See MySQL error reference: https://dev.mysql.com/doc/mysql-errors/5.7/en/error-reference-introduction.html
		switch mysqlErr.Number {
		case 1059:
			err = fmt.Errorf("%w.\nPossible resolutions include:\n%s\n%s\n%s",
				err,
				"1. Adding a projection to rename this field, see https://go.estuary.dev/docs-projections",
				"2. Exclude the field, see https://go.estuary.dev/docs-field-selection",
				"3. Disable the corresponding binding")
		}
	}
	return err
}
