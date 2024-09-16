package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/databricks/databricks-sdk-go"
	dbConfig "github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sdk-go/service/catalog"
	databricksSql "github.com/databricks/databricks-sdk-go/service/sql"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"

	_ "github.com/databricks/databricks-sql-go"
)

var _ sql.SchemaManager = (*client)(nil)

type client struct {
	db       *stdsql.DB
	cfg      *config
	ep       *sql.Endpoint
	wsClient *databricks.WorkspaceClient
}

func newClient(ctx context.Context, ep *sql.Endpoint) (sql.Client, error) {
	cfg := ep.Config.(*config)

	db, err := stdsql.Open("databricks", cfg.ToURI())
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	wsClient, err := databricks.NewWorkspaceClient(&databricks.Config{
		Host:        fmt.Sprintf("%s/%s", cfg.Address, cfg.HTTPPath),
		Token:       cfg.Credentials.PersonalAccessToken,
		Credentials: dbConfig.PatCredentials{}, // enforce PAT auth
	})
	if err != nil {
		return nil, fmt.Errorf("creating workspace client: %w", err)
	}

	return &client{
		db:       db,
		cfg:      cfg,
		ep:       ep,
		wsClient: wsClient,
	}, nil
}

func (c *client) InfoSchema(ctx context.Context, resourcePaths [][]string) (*boilerplate.InfoSchema, error) {
	// First check if there are any interrupted column migrations which must be resumed, before we
	// construct the InfoSchema
	migrationsTable := c.ep.Dialect.Identifier(c.cfg.CatalogName, c.cfg.SchemaName, "flow_migrations")
	flowMigrationsExists, err := c.wsClient.Tables.ExistsByFullName(ctx, migrationsTable)
	if err == nil && flowMigrationsExists.TableExists {
		rows, err := c.db.QueryContext(ctx, fmt.Sprintf("SELECT table, step, col_identifier, col_field, col_ddl FROM %s", migrationsTable))
		if err != nil {
			return nil, fmt.Errorf("finding flow_migrations: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var stmts [][]string
			var tableIdentifier, colIdentifier, colField, DDL string
			var step int
			if err := rows.Scan(&tableIdentifier, &step, &colIdentifier, &colField, &DDL); err != nil {
				return nil, fmt.Errorf("reading flow_migrations row: %w", err)
			}
			var steps, acceptableErrors = c.columnChangeSteps(tableIdentifier, colField, colIdentifier, DDL)

			for s := step; s < len(steps); s++ {
				stmts = append(stmts, []string{steps[s], acceptableErrors[s]})
				stmts = append(stmts, []string{fmt.Sprintf("UPDATE %s SET step = %d WHERE table='%s';", migrationsTable, s+1, tableIdentifier)})
			}

			stmts = append(stmts, []string{fmt.Sprintf("DELETE FROM %s WHERE table='%s';", migrationsTable, tableIdentifier)})

			for _, stmt := range stmts {
				query := stmt[0]
				var acceptableError string
				if len(stmt) > 1 {
					acceptableError = stmt[1]
				}

				if _, err := c.db.ExecContext(ctx, query); err != nil {
					if acceptableError != "" && strings.Contains(err.Error(), acceptableError) {
						// This is an acceptable error for this step, so we do not throw an error
					} else {
						return nil, err
					}
				}
			}
		}

		if _, err := c.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", migrationsTable)); err != nil {
			return nil, fmt.Errorf("dropping flow_migrations: %w", err)
		}
	}

	// Construct the InfoSchema after migrations
	is := boilerplate.NewInfoSchema(
		sql.ToLocatePathFn(c.ep.Dialect.TableLocator),
		c.ep.Dialect.ColumnLocator,
	)

	rpSchemas := make(map[string]struct{})
	for _, p := range resourcePaths {
		rpSchemas[databricksDialect.TableLocator(p).TableSchema] = struct{}{}
	}

	// Databricks' Tables API provides a free-form "metadata" field that is a JSON object containing
	// some additional useful information.
	type columnMeta struct {
		Metadata struct {
			Default json.RawMessage `json:"CURRENT_DEFAULT,omitempty"`
		} `json:"metadata"`
	}

	// The table listing will fail if the schema doesn't already exist, so only attempt to list
	// tables in schemas that do exist.
	existingSchemas, err := c.ListSchemas(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing schemas: %w", err)
	}

	for sc := range rpSchemas {
		if !slices.Contains(existingSchemas, sc) {
			log.WithField("schema", sc).Debug("not listing tables for schema since it doesn't exist")
			continue
		}

		tableIter := c.wsClient.Tables.List(ctx, catalog.ListTablesRequest{
			CatalogName: c.cfg.CatalogName,
			SchemaName:  sc,
		})

		for tableIter.HasNext(ctx) {
			t, err := tableIter.Next(ctx)
			if err != nil {
				return nil, fmt.Errorf("iterating tables: %w", err)
			}

			is.PushResource(t.SchemaName, t.Name)

			for _, c := range t.Columns {
				var colMeta columnMeta
				if err := json.Unmarshal([]byte(c.TypeJson), &colMeta); err != nil {
					return nil, fmt.Errorf("unmarshalling column metadata: %w", err)
				}

				is.PushField(boilerplate.EndpointField{
					Name:               c.Name,
					Nullable:           c.Nullable,
					Type:               string(c.TypeName),
					CharacterMaxLength: 0, // TODO(whb): Currently not supported by us, although we could parse the metadata for VARCHAR columns.
					HasDefault:         colMeta.Metadata.Default != nil,
				}, t.SchemaName, t.Name)
			}
		}
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

const ColumnChangeLength = ""

func (c *client) columnChangeSteps(tableIdentifier, colField, colIdentifier, DDL string) ([]string, []string) {
	var tempColumnName = fmt.Sprintf("%s_flowtmp1", colField)
	var tempColumnIdentifier = c.ep.Dialect.Identifier(tempColumnName)
	var tempOriginalRename = fmt.Sprintf("%s_flowtmp2", colField)
	var tempOriginalRenameIdentifier = c.ep.Dialect.Identifier(tempOriginalRename)
	return []string{
			fmt.Sprintf(
				"ALTER TABLE %s ADD COLUMN %s %s;",
				tableIdentifier,
				tempColumnIdentifier,
				DDL,
			),
			fmt.Sprintf(
				"UPDATE %s SET %s = %s;",
				tableIdentifier,
				tempColumnIdentifier,
				colIdentifier,
			),
			fmt.Sprintf(
				"ALTER TABLE %s RENAME COLUMN %s TO %s;",
				tableIdentifier,
				colIdentifier,
				tempOriginalRenameIdentifier,
			),
			fmt.Sprintf(
				"ALTER TABLE %s RENAME COLUMN %s TO %s;",
				tableIdentifier,
				tempColumnIdentifier,
				colIdentifier,
			),
			fmt.Sprintf(
				"ALTER TABLE %s DROP COLUMN %s;",
				tableIdentifier,
				tempOriginalRenameIdentifier,
			),
		}, []string{
			"FIELDS_ALREADY_EXISTS",
			"",
			fmt.Sprintf("Missing field %s", colField),
			fmt.Sprintf("Missing field %s", tempColumnName),
			fmt.Sprintf("Missing field %s", tempOriginalRename),
		}
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

	if len(ta.ColumnTypeChanges) > 0 {
		stmts = append(stmts, "CREATE OR REPLACE TABLE flow_migrations(table STRING, step INTEGER, col_identifier STRING, col_field STRING, col_ddl STRING);")

		for _, ch := range ta.ColumnTypeChanges {
			stmts = append(stmts, fmt.Sprintf(
				"INSERT INTO flow_migrations(table, step, col_identifier, col_field, col_ddl) VALUES ('%s', 0, '%s', '%s', '%s');",
				ta.Identifier,
				ch.Identifier,
				ch.Field,
				ch.DDL,
			))
		}

		for _, ch := range ta.ColumnTypeChanges {
			var steps, _ = c.columnChangeSteps(ta.Identifier, ch.Field, ch.Identifier, ch.DDL)

			for s := 0; s < len(steps); s++ {
				stmts = append(stmts, steps[s])
				stmts = append(stmts, fmt.Sprintf("UPDATE flow_migrations SET STEP=%d WHERE table='%s';", s+1, ta.Identifier))
			}
		}

		stmts = append(stmts, "DROP TABLE IF EXISTS flow_migrations;")
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
	listed, err := c.wsClient.Schemas.ListAll(ctx, catalog.ListSchemasRequest{
		CatalogName: c.cfg.CatalogName,
	})
	if err != nil {
		return nil, err
	}

	schemaNames := make([]string, 0, len(listed))
	for _, ls := range listed {
		schemaNames = append(schemaNames, ls.Name)
	}

	return schemaNames, nil
}

func (c *client) CreateSchema(ctx context.Context, schemaName string) error {
	_, err := c.wsClient.Schemas.Create(ctx, catalog.CreateSchema{
		CatalogName: c.cfg.CatalogName,
		Name:        schemaName,
	})

	return err
}

func preReqs(ctx context.Context, conf any, tenant string) *sql.PrereqErr {
	errs := &sql.PrereqErr{}

	cfg := conf.(*config)
	wsClient, err := databricks.NewWorkspaceClient(&databricks.Config{
		Host:        fmt.Sprintf("%s/%s", cfg.Address, cfg.HTTPPath),
		Token:       cfg.Credentials.PersonalAccessToken,
		Credentials: dbConfig.PatCredentials{}, // enforce PAT auth
	})
	if err != nil {
		errs.Err(fmt.Errorf("creating workspace client: %w", err))
		return errs
	}

	db, err := stdsql.Open("databricks", cfg.ToURI())
	if err != nil {
		errs.Err(fmt.Errorf("opening database: %w", err))
		return errs
	}

	var httpPathSplit = strings.Split(cfg.HTTPPath, "/")
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
			errs.Err(fmt.Errorf("could not start the warehouse: %w", err))
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
	if err := db.PingContext(ctx); err != nil {
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
