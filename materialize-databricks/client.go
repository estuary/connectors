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
	cerrors "github.com/estuary/connectors/go/connector-errors"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	log "github.com/sirupsen/logrus"

	_ "github.com/databricks/databricks-sql-go"
)

var _ sql.SchemaManager = (*client)(nil)

type client struct {
	db        *stdsql.DB
	cfg       config
	ep        *sql.Endpoint[config]
	wsClient  *databricks.WorkspaceClient
	templates templates
}

func newClient(ctx context.Context, ep *sql.Endpoint[config]) (sql.Client, error) {
	cfg := ep.Config

	db, err := stdsql.Open("databricks", cfg.ToURI())
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	var wsConfig *databricks.Config
	switch cfg.Credentials.AuthType {
	case OAUTH_M2M_AUTH_TYPE:
		wsConfig = &databricks.Config{
			Host:         fmt.Sprintf("%s/%s", cfg.Address, cfg.HTTPPath),
			ClientID:     cfg.Credentials.ClientID,
			ClientSecret: cfg.Credentials.ClientSecret,
		}
	default: // PAT_AUTH_TYPE
		wsConfig = &databricks.Config{
			Host:        fmt.Sprintf("%s/%s", cfg.Address, cfg.HTTPPath),
			Token:       cfg.Credentials.PersonalAccessToken,
			Credentials: dbConfig.PatCredentials{}, // enforce PAT auth
		}
	}

	wsClient, err := databricks.NewWorkspaceClient(wsConfig)
	if err != nil {
		return nil, fmt.Errorf("creating workspace client: %w", err)
	}

	return &client{
		db:        db,
		cfg:       cfg,
		ep:        ep,
		wsClient:  wsClient,
		templates: renderTemplates(ep.Dialect),
	}, nil
}

func (c *client) PopulateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema, resourcePaths [][]string) error {
	rpSchemas := make(map[string]struct{})
	for _, p := range resourcePaths {
		rpSchemas[c.ep.Dialect.TableLocator(p).TableSchema] = struct{}{}
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
		return fmt.Errorf("listing schemas: %w", err)
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
				return fmt.Errorf("iterating tables: %w", err)
			}

			res := is.PushResource(t.SchemaName, t.Name)
			for _, c := range t.Columns {
				var colMeta columnMeta
				if err := json.Unmarshal([]byte(c.TypeJson), &colMeta); err != nil {
					return fmt.Errorf("unmarshalling column metadata: %w", err)
				}

				res.PushField(boilerplate.ExistingField{
					Name:               c.Name,
					Nullable:           c.Nullable,
					Type:               string(c.TypeName),
					CharacterMaxLength: 0, // TODO(whb): Currently not supported by us, although we could parse the metadata for VARCHAR columns.
					HasDefault:         colMeta.Metadata.Default != nil,
				})
			}
		}
	}

	return nil
}

func (c *client) CreateTable(ctx context.Context, tc sql.TableCreate) error {
	_, err := c.db.ExecContext(ctx, tc.TableCreateSql)
	if err != nil {
		return err
	}

	var res = tc.Resource.(tableConfig).WithDefaults(c.cfg)
	if res.AdditionalSql != "" {
		if _, err := c.db.ExecContext(ctx, res.AdditionalSql); err != nil {
			return fmt.Errorf("executing additional SQL statement '%s': %w", res.AdditionalSql, err)
		}

		log.WithFields(log.Fields{
			"table": tc.Identifier,
			"query": res.AdditionalSql,
		}).Info("executed AdditionalSql")
	}

	return nil
}

func (c *client) DeleteTable(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	stmt := fmt.Sprintf("DROP TABLE %s;", c.ep.Dialect.Identifier(path...))

	return stmt, func(ctx context.Context) error {
		_, err := c.db.ExecContext(ctx, stmt)
		return err
	}, nil
}

func (c *client) TruncateTable(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	stmt := fmt.Sprintf("TRUNCATE TABLE %s;", c.ep.Dialect.Identifier(path...))

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
		if err := c.templates.alterTableColumns.Execute(&addColumnsStmt, ta); err != nil {
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
		if steps, err := sql.StdColumnTypeMigrations(ctx, c.ep.Dialect, ta.Table, ta.ColumnTypeChanges); err != nil {
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

func (c *client) CreateSchema(ctx context.Context, schemaName string) (string, error) {
	_, err := c.wsClient.Schemas.Create(ctx, catalog.CreateSchema{
		CatalogName: c.cfg.CatalogName,
		Name:        schemaName,
	})

	return fmt.Sprintf("CREATE SCHEMA %s;", schemaName), err
}

func preReqs(ctx context.Context, cfg config) *cerrors.PrereqErr {
	errs := &cerrors.PrereqErr{}

	var wsConfig *databricks.Config
	switch cfg.Credentials.AuthType {
	case OAUTH_M2M_AUTH_TYPE:
		wsConfig = &databricks.Config{
			Host:         fmt.Sprintf("%s/%s", cfg.Address, cfg.HTTPPath),
			ClientID:     cfg.Credentials.ClientID,
			ClientSecret: cfg.Credentials.ClientSecret,
		}
	default: // PAT_AUTH_TYPE
		wsConfig = &databricks.Config{
			Host:        fmt.Sprintf("%s/%s", cfg.Address, cfg.HTTPPath),
			Token:       cfg.Credentials.PersonalAccessToken,
			Credentials: dbConfig.PatCredentials{}, // enforce PAT auth
		}
	}

	wsClient, err := databricks.NewWorkspaceClient(wsConfig)
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
