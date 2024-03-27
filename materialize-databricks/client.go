package main

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/databricks/databricks-sdk-go"
	dbConfig "github.com/databricks/databricks-sdk-go/config"
	databricksSql "github.com/databricks/databricks-sdk-go/service/sql"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"

	_ "github.com/databricks/databricks-sql-go"
)

type client struct {
	db  *stdsql.DB
	cfg *config
	ep  *sql.Endpoint
}

func newClient(ctx context.Context, ep *sql.Endpoint) (sql.Client, error) {
	cfg := ep.Config.(*config)

	db, err := stdsql.Open("databricks", cfg.ToURI())
	if err != nil {
		return nil, err
	}

	return &client{
		db:  db,
		cfg: cfg,
		ep:  ep,
	}, nil
}

// InfoSchema for materialize-databricks is almost exactly the same as StdFetchInfoSchema, but
// Databricks requires the information_schema view to be qualified with "system", at least on
// some deployments.
// TODO(whb): This should likely be refactored to use the Databricks WorkspaceClient, since queries
// to the information_schema view will be non-responsive if the warehouse has gone to sleep -
// assuming, of course, the WorkspaceClient is not also non-responsive if the warehouse is asleep.
func (c *client) InfoSchema(ctx context.Context, resourcePaths [][]string) (*boilerplate.InfoSchema, error) {
	is := boilerplate.NewInfoSchema(
		sql.ToLocatePathFn(c.ep.Dialect.TableLocator),
		c.ep.Dialect.ColumnLocator,
	)

	schemas := []string{c.ep.Dialect.Literal(c.cfg.SchemaName)}
	for _, p := range resourcePaths {
		loc := c.ep.Dialect.TableLocator(p)
		schemas = append(schemas, c.ep.Dialect.Literal(loc.TableSchema))
	}

	slices.Sort(schemas)
	schemas = slices.Compact(schemas)

	rows, err := c.db.QueryContext(ctx, fmt.Sprintf(`
		select table_schema, table_name, column_name, is_nullable, data_type, character_maximum_length, column_default
		from system.information_schema.columns
		where table_catalog = %s
		and table_schema in (%s);
	`,
		c.ep.Dialect.Literal(c.cfg.CatalogName),
		strings.Join(schemas, ","),
	))
	if err != nil {
		return nil, fmt.Errorf("querying system.information_schema.columns: %w", err)
	}
	defer rows.Close()

	type columnRow struct {
		TableSchema            string
		TableName              string
		ColumnName             string
		IsNullable             string
		DataType               string
		CharacterMaximumLength stdsql.NullInt64
		ColumnDefault          stdsql.NullString
	}

	for rows.Next() {
		var c columnRow
		if err := rows.Scan(&c.TableSchema, &c.TableName, &c.ColumnName, &c.IsNullable, &c.DataType, &c.CharacterMaximumLength, &c.ColumnDefault); err != nil {
			return nil, fmt.Errorf("scanning column row: %w", err)
		}

		is.PushField(boilerplate.EndpointField{
			Name:               c.ColumnName,
			Nullable:           strings.EqualFold(c.IsNullable, "yes"),
			Type:               c.DataType,
			CharacterMaxLength: int(c.CharacterMaximumLength.Int64),
			HasDefault:         c.ColumnDefault.Valid,
		}, c.TableSchema, c.TableName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows.Err(): %w", err)
	}

	return is, nil
}

func (c *client) PutSpec(ctx context.Context, updateSpec sql.MetaSpecsUpdate) error {
	_, err := c.db.ExecContext(ctx, updateSpec.QueryString)
	return err
}

func (c *client) CreateTable(ctx context.Context, tc sql.TableCreate) error {
	_, err := c.db.ExecContext(ctx, tc.TableCreateSql)
	return err
}

func (c *client) DeleteTable(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	stmt := fmt.Sprintf("DROP TABLE %s;", databricksDialect.Identifier(path...))

	return stmt, func(ctx context.Context) error {
		_, err := c.db.ExecContext(ctx, stmt)
		return err
	}, nil
}

func (c *client) AlterTable(ctx context.Context, ta sql.TableAlter) (string, boilerplate.ActionApplyFn, error) {
	var stmts []string

	// Databricks doesn't support multi-statement queries with the driver we are using, and also
	// doesn't support dropping nullability for multiple columns in a single statement. Multiple
	// columns can be added in a single statement though.
	if len(ta.AddColumns) > 0 {
		var addColumnsStmt strings.Builder
		if err := tplAlterTableColumns.Execute(&addColumnsStmt, ta); err != nil {
			return "", nil, fmt.Errorf("rendering alter table columns statement: %w", err)
		}
		stmts = append(stmts, addColumnsStmt.String())
	}
	for _, f := range ta.DropNotNulls {
		stmts = append(stmts, fmt.Sprintf(
			"ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;",
			ta.Identifier,
			c.ep.Dialect.Identifier(f.Name),
		))
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

// TODO(whb): Consider using a WorkspaceClient here exclusively to check what we can and forego the
// ping so that a sleeping warehouse doesn't hang on Validate.
func (c *client) PreReqs(ctx context.Context) *sql.PrereqErr {
	errs := &sql.PrereqErr{}

	wsClient, err := databricks.NewWorkspaceClient(&databricks.Config{
		Host:        fmt.Sprintf("%s/%s", c.cfg.Address, c.cfg.HTTPPath),
		Token:       c.cfg.Credentials.PersonalAccessToken,
		Credentials: dbConfig.PatCredentials{}, // enforce PAT auth
	})
	if err != nil {
		errs.Err(err)
	}

	var httpPathSplit = strings.Split(c.cfg.HTTPPath, "/")
	var warehouseId = httpPathSplit[len(httpPathSplit)-1]
	var warehouseStopped = true
	var warehouseErr error
	if res, err := wsClient.Warehouses.GetById(ctx, warehouseId); err != nil {
		errs.Err(err)
	} else {
		switch res.State {
		case databricksSql.StateDeleted:
			errs.Err(fmt.Errorf("The selected SQL Warehouse is deleted, please use an active SQL warehouse."))
		case databricksSql.StateDeleting:
			errs.Err(fmt.Errorf("The selected SQL Warehouse is being deleted, please use an active SQL warehouse."))
		case databricksSql.StateStarting:
			warehouseErr = fmt.Errorf("The selected SQL Warehouse is starting, please wait a couple of minutes before trying again.")
		case databricksSql.StateStopped:
			warehouseErr = fmt.Errorf("The selected SQL Warehouse is stopped, please start the SQL warehouse and try again.")
		case databricksSql.StateStopping:
			warehouseErr = fmt.Errorf("The selected SQL Warehouse is stopping, please start the SQL warehouse and try again.")
		case databricksSql.StateRunning:
			warehouseStopped = false
		}
	}

	if errs.Len() > 0 {
		return errs
	}

	if warehouseStopped {
		// Use a reasonable timeout for this connection test. It is not uncommon for a misconfigured
		// connection (wrong host, wrong port, etc.) to hang for several minutes on Ping and we want to
		// bail out well before then. Note that it is normal for Databricks warehouses to go offline
		// after inactivity, and this attempt to connect to the warehouse will initiate their boot-up
		// process however we don't want to wait 5 minutes as that does not create a good UX for the
		// user in the UI
		if r, err := wsClient.Warehouses.Start(ctx, databricksSql.StartRequest{Id: warehouseId}); err != nil {
			errs.Err(fmt.Errorf("Could not start the warehouse: %w", err))
		} else if _, err := r.GetWithTimeout(60 * time.Second); err != nil {
			errs.Err(warehouseErr)
		}

		if errs.Len() > 0 {
			return errs
		}
	}

	// We avoid running this ping if the warehouse is not awake, see
	// the issue below for more information on why:
	// https://github.com/databricks/databricks-sql-go/issues/198
	if err := c.db.PingContext(ctx); err != nil {
		// Provide a more user-friendly representation of some common error causes.
		var execErr dbsqlerr.DBExecutionError
		var netConnErr *net.DNSError
		var netOpErr *net.OpError

		if errors.As(err, &execErr) {
			// See https://pkg.go.dev/github.com/databricks/databricks-sql-go/errors#pkg-constants
			// and https://docs.databricks.com/en/error-messages/index.html
			switch execErr.SqlState() {
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
	}

	return errs
}

func (c *client) FetchSpecAndVersion(ctx context.Context, specs sql.Table, materialization pf.Materialization) (string, string, error) {
	var version, spec string

	if err := c.db.QueryRowContext(
		ctx,
		fmt.Sprintf(
			"SELECT version, spec FROM %s WHERE materialization = %s;",
			specs.Identifier,
			databricksDialect.Literal(materialization.String()),
		),
	).Scan(&version, &spec); err != nil {
		return "", "", err
	}

	return spec, version, nil
}

func (c *client) ExecStatements(ctx context.Context, statements []string) error {
	return sql.StdSQLExecStatements(ctx, c.db, statements)
}

// InstallFence is a no-op since materialize-databricks doesn't use fencing.
func (c *client) InstallFence(ctx context.Context, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	return sql.Fence{}, nil
}

func (c *client) Close() {
	c.db.Close()
}
