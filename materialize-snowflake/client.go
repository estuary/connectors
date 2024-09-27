package main

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"strings"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	sf "github.com/snowflakedb/gosnowflake"
)

var _ sql.SchemaManager = (*client)(nil)

type client struct {
	db  *stdsql.DB
	cfg *config
	ep  *sql.Endpoint
}

func newClient(ctx context.Context, ep *sql.Endpoint) (sql.Client, error) {
	cfg := ep.Config.(*config)

	dsn, err := cfg.toURI(ep.Tenant)
	if err != nil {
		return nil, err
	}

	db, err := stdsql.Open("snowflake", dsn)
	if err != nil {
		return nil, err
	}

	return &client{
		db:  db,
		cfg: cfg,
		ep:  ep,
	}, nil
}

func (c *client) InfoSchema(ctx context.Context, resourcePaths [][]string) (is *boilerplate.InfoSchema, err error) {
	// Currently the "catalog" is always the database value from the endpoint configuration in all
	// capital letters. It is possible to connect to Snowflake databases that aren't in all caps by
	// quoting the database name. We don't do that currently and it's hard to say if we ever will
	// need to, although that means we can't connect to databases that aren't in the Snowflake
	// default ALL CAPS format. The practical implications are that if somebody puts in a database
	// like "database", we'll actually connect to the database "DATABASE", and so we can't rely on
	// the endpoint configuration value entirely and will query it here to be future-proof.
	var catalog string
	if err := c.db.QueryRowContext(ctx, "SELECT CURRENT_DATABASE()").Scan(&catalog); err != nil {
		return nil, fmt.Errorf("querying for connected database: %w", err)
	}

	return sql.StdFetchInfoSchema(ctx, c.db, c.ep.Dialect, catalog, resourcePaths)
}

func (c *client) PutSpec(ctx context.Context, updateSpec sql.MetaSpecsUpdate) error {
	_, err := c.db.ExecContext(ctx, updateSpec.ParameterizedQuery, updateSpec.Parameters...)
	return err
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

func (c *client) AlterTable(ctx context.Context, ta sql.TableAlter) (string, boilerplate.ActionApplyFn, error) {
	var stmts []string
	var alterColumnStmtBuilder strings.Builder
	if err := renderTemplates(c.ep.Dialect).alterTableColumns.Execute(&alterColumnStmtBuilder, ta); err != nil {
		return "", nil, fmt.Errorf("rendering alter table columns statement: %w", err)
	}
	alterColumnStmt := alterColumnStmtBuilder.String()
	if len(strings.Trim(alterColumnStmt, "\n")) > 0 {
		stmts = append(stmts, alterColumnStmt)
	}

	if len(ta.ColumnTypeChanges) > 0 {
		for _, m := range ta.ColumnTypeChanges {
			steps := sql.StdColumnTypeMigration(ctx, c.ep.Dialect, ta.Table, m)
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

func (c *client) CreateSchema(ctx context.Context, schemaName string) error {
	return sql.StdCreateSchema(ctx, c.db, c.ep.Dialect, schemaName)
}

func preReqs(ctx context.Context, conf any, tenant string) *sql.PrereqErr {
	errs := &sql.PrereqErr{}

	cfg := conf.(*config)

	dsn, err := cfg.toURI(tenant)
	if err != nil {
		errs.Err(err)
		return errs
	}

	db, err := stdsql.Open("snowflake", dsn)
	if err != nil {
		errs.Err(err)
		return errs
	}

	if err := db.PingContext(ctx); err != nil {
		var sfError *sf.SnowflakeError
		if errors.As(err, &sfError) {
			switch sfError.Number {
			case 260008:
				// This is the error if the host URL has an incorrect account identifier. The error
				// message from the Snowflake driver will accurately report that the account name is
				// incorrect, but would be confusing for a user because we have a separate "Account"
				// input field. We want to be specific here and report that it is the account
				// identifier in the host URL.
				err = fmt.Errorf("incorrect account identifier %q in host URL", strings.TrimSuffix(cfg.Host, ".snowflakecomputing.com"))
			case 390100:
				err = fmt.Errorf("incorrect username or password")
			case 390201:
				// This means "doesn't exist or not authorized", and we don't have a great way to
				// distinguish between that for the database, schema, or warehouse. The snowflake
				// error message in these cases is fairly decent fortunately.
			case 390189:
				err = fmt.Errorf("role %q does not exist", cfg.Role)
			}
		}

		errs.Err(err)
	} else {
		// Check for an active warehouse for the connection. If there is no default warehouse for
		// the user and the configuration did not set a warehouse, this may be `null`, and the user
		// needs to configure a specific warehouse to use.
		var currentWarehouse *string
		if err := db.QueryRowContext(ctx, "SELECT CURRENT_WAREHOUSE();").Scan(&currentWarehouse); err != nil {
			errs.Err(fmt.Errorf("checking for active warehouse: %w", err))
		} else {
			if currentWarehouse == nil {
				errs.Err(fmt.Errorf("no warehouse configured and default warehouse not set for user '%s': must set a value for 'Warehouse' in the endpoint configuration", cfg.Credentials.User))
			}
		}
	}

	return errs
}

func (c *client) FetchSpecAndVersion(ctx context.Context, specs sql.Table, materialization pf.Materialization) (string, string, error) {
	return sql.StdFetchSpecAndVersion(ctx, c.db, specs, materialization)
}

func (c *client) ExecStatements(ctx context.Context, statements []string) error {
	return sql.StdSQLExecStatements(ctx, c.db, statements)
}

func (c *client) InstallFence(ctx context.Context, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	return sql.Fence{}, nil
}

func (c *client) Close() {
	c.db.Close()
}
