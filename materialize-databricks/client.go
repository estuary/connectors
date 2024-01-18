package main

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/databricks/databricks-sdk-go"
	dbConfig "github.com/databricks/databricks-sdk-go/config"
	databricksCatalog "github.com/databricks/databricks-sdk-go/service/catalog"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"

	_ "github.com/databricks/databricks-sql-go"
)

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
		return nil, err
	}

	wsClient, err := databricks.NewWorkspaceClient(&databricks.Config{
		Host:               fmt.Sprintf("%s/%s", cfg.Address, cfg.HTTPPath),
		Token:              cfg.Credentials.PersonalAccessToken,
		Credentials:        dbConfig.PatCredentials{}, // enforce PAT auth
		HTTPTimeoutSeconds: 5 * 60,                    // This is necessary for file uploads as they can sometimes take longer than the default 60s
	})
	if err != nil {
		return nil, fmt.Errorf("initialising workspace client: %w", err)
	}

	return &client{
		db:       db,
		cfg:      cfg,
		ep:       ep,
		wsClient: wsClient,
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

	schemas := []string{c.cfg.SchemaName}
	for _, p := range resourcePaths {
		loc := c.ep.Dialect.TableLocator(p)
		schemas = append(schemas, loc.TableSchema)
	}

	slices.Sort(schemas)
	schemas = slices.Compact(schemas)

	for _, schema := range schemas {
		tableInfos, err := c.wsClient.Tables.ListAll(ctx, databricksCatalog.ListTablesRequest{
			CatalogName: c.cfg.CatalogName,
			SchemaName:  schema,
		})

		if err != nil {
			return nil, fmt.Errorf("fetching table metadata: %w", err)
		}

		for _, tableInfo := range tableInfos {
			for _, col := range tableInfo.Columns {
				is.PushField(boilerplate.EndpointField{
					Name:               col.Name,
					Nullable:           col.Nullable,
					Type:               string(col.TypeName),
					CharacterMaxLength: 0, // FIXME
				}, tableInfo.SchemaName, tableInfo.Name)
			}
		}
	}

	return is, nil
}

func (c *client) CreateTable(ctx context.Context, tc sql.TableCreate) error {
	_, err := c.db.ExecContext(ctx, tc.TableCreateSql)
	return err
}

func (c *client) ReplaceTable(ctx context.Context, tr sql.TableReplace) (string, boilerplate.ActionApplyFn, error) {
	// TODO(whb): There's a chance that if a previous driver checkpoint was persisted before the
	// actual data load completed, these table replacements will not be compatible with the data
	// referenced by the persisted driver checkpoint. This is pretty unlikely to happen, and will be
	// resolved when we incorporate the use of state keys, which incorporate the backfill counter.
	return tr.TableReplaceSql, func(ctx context.Context) error {
		_, err := c.db.ExecContext(ctx, tr.TableReplaceSql)
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

// Given that Databricks warehouses can take a long time to start up, we avoid
// a ping to the database since that requires the workspace to be up and running
// instead we let the Apply RPC take care of starting up the workspace since Apply
// runs asynchronously in the background, but Validate is presented to the user
// and the user must wait for it to finish
func (c *client) PreReqs(ctx context.Context) *sql.PrereqErr {
	errs := &sql.PrereqErr{}

	var httpPathSplit = strings.Split(c.cfg.HTTPPath, "/")
	var warehouseId = httpPathSplit[len(httpPathSplit)-1]
	if _, err := c.wsClient.Warehouses.GetById(ctx, warehouseId); err != nil {
		errs.Err(err)
	}

	return errs
}

func (c *client) ExecStatements(ctx context.Context, statements []string) error {
	return sql.StdSQLExecStatements(ctx, c.db, statements)
}

// FetchSpecAndVersion is a no-op since materialize-databricks doesn't use fencing
func (c *client) FetchSpecAndVersion(ctx context.Context, specs sql.Table, materialization pf.Materialization) (string, string, error) {
	return "", "", nil
}

// PutSpec is a no-op since materialize-databricks doesn't use fencing
func (c *client) PutSpec(ctx context.Context, updateSpec sql.MetaSpecsUpdate) error {
	return nil
}

// InstallFence is a no-op since materialize-databricks doesn't use fencing.
func (c *client) InstallFence(ctx context.Context, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	return sql.Fence{}, nil
}

func (c *client) Close() {
	c.db.Close()
}
