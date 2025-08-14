package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/jmoiron/sqlx"
	sf "github.com/snowflakedb/gosnowflake"
	"golang.org/x/sync/errgroup"
)

const (
	// showLimit is the maximum allowable for SHOW SCHEMAS and SHOW TABLES IN
	// <SCHEMA> metadata queries. You can't use a higher limit than this, and if
	// no limit is set then result sets larger will throw an error. So this must
	// be set and results potentially paginated to handle databases with huge
	// numbers of schemas or tables.
	showQueryLimit = 10_000
)

var _ sql.SchemaManager = (*client)(nil)

type client struct {
	db         *stdsql.DB
	dbNoSchema *stdsql.DB // for metadata operations performed before the endpoint-level schema is created
	xdb        *sqlx.DB   // used to easily read the results of SHOW queries
	cfg        config
	ep         *sql.Endpoint[config]
}

func newClient(ctx context.Context, ep *sql.Endpoint[config]) (sql.Client, error) {
	dsnWithSchema, err := ep.Config.toURI(ep.Tenant, true)
	if err != nil {
		return nil, err
	}

	db, err := stdsql.Open("snowflake", dsnWithSchema)
	if err != nil {
		return nil, err
	}

	dsnNoSchema, err := ep.Config.toURI(ep.Tenant, false)
	if err != nil {
		return nil, err
	}

	dbNoSchema, err := stdsql.Open("snowflake", dsnNoSchema)
	if err != nil {
		return nil, err
	}

	return &client{
		db:         db,
		dbNoSchema: dbNoSchema,
		xdb:        sqlx.NewDb(dbNoSchema, "snowflake").Unsafe(),
		cfg:        ep.Config,
		ep:         ep,
	}, nil
}

// InfoSchema uses various "SHOW" queries to obtain information about existing
// resources. These kinds of queries do not require a warehouse to run, and
// instead use the Snowflake metadata APIs under the hood. Each individual table
// is used for a SHOW COLUMNS IN TABLE <table> - it may be more efficient to do
// SHOW COLUMNS IN SCHEMA, but there is a documented limit of 10,000 results
// from SHOW COLUMNS and it can't be paginated.
func (c *client) PopulateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema, resourcePaths [][]string) error {
	existingSchemas, err := c.ListSchemas(ctx)
	if err != nil {
		return fmt.Errorf("listing schemas: %w", err)
	}

	// Filter down to just schemas that are relevant for the included resource
	// paths, and for each of those fetch & filter down to just the tables that
	// are relevant.
	relevantExistingSchemasAndTables := make(map[string][]string)
	for _, rp := range resourcePaths {
		loc := c.ep.Dialect.TableLocator(rp)
		if _, ok := relevantExistingSchemasAndTables[loc.TableSchema]; ok {
			continue // already populated the list of existing, relevant tables
		} else if !slices.Contains(existingSchemas, loc.TableSchema) {
			continue // schema doesn't exist, so we don't need to check for tables
		}

		// SHOW TABLES is only run a single time per schema, as it occurs in the
		// list of resource paths.
		relevantExistingSchemasAndTables[loc.TableSchema] = []string{}
		tables, err := runShowPaginated(ctx, c.xdb, func(cursor *string) string {
			if cursor != nil {
				return fmt.Sprintf("SHOW TERSE TABLES IN %q LIMIT %d FROM '%s';", loc.TableSchema, showQueryLimit, *cursor)
			} else {
				return fmt.Sprintf("SHOW TERSE TABLES IN %q LIMIT %d;", loc.TableSchema, showQueryLimit)
			}
		})
		if err != nil {
			return fmt.Errorf("listing tables in schema %q: %w", loc.TableSchema, err)
		}

		for _, table := range tables {
			if slices.ContainsFunc(resourcePaths, func(rrp []string) bool {
				thisLoc := c.ep.Dialect.TableLocator(rrp)
				return thisLoc.TableSchema == loc.TableSchema && thisLoc.TableName == table
			}) {
				// This is a relevant table for the materialization, and its
				// columns will be fetched a little further down.
				relevantExistingSchemasAndTables[loc.TableSchema] = append(relevantExistingSchemasAndTables[loc.TableSchema], table)
			}
		}
	}

	var mu sync.Mutex
	group, groupCtx := errgroup.WithContext(ctx)
	// Some amount of concurrency bounding is helpful when there are a large
	// number of tables. Sending excessive numbers of concurrent requests seems
	// to trigger something in Snowflake that starts to throttle them. Running
	// 10 at a time is fine though.
	group.SetLimit(10)

	type showColumnsResult struct {
		ColumnName string `db:"column_name"`
		DataType   string `db:"data_type"`
		Default    string `db:"default"`
	}

	type dataTypeDef struct {
		Type     string `json:"type"`
		Nullable bool   `json:"nullable"`
		Length   int    `json:"length"`
	}

	for schema, tables := range relevantExistingSchemasAndTables {
		for _, table := range tables {
			group.Go(func() error {
				var columns []showColumnsResult
				// In this SHOW COLUMNS query, both the schema and table name
				// are quoted since they are already in exactly the form that
				// they exist in Snowflake. Quoting allows for special
				// characters and mixed capitalization to work.
				if err := c.xdb.SelectContext(groupCtx, &columns, fmt.Sprintf("SHOW COLUMNS IN TABLE %q.%q;", schema, table)); err != nil {
					return err
				}

				mu.Lock()
				defer mu.Unlock()
				res := is.PushResource(schema, table)
				for _, col := range columns {
					var columnDef dataTypeDef
					if err := json.Unmarshal([]byte(col.DataType), &columnDef); err != nil {
						return fmt.Errorf("unmarshalling column data type: %w", err)
					}

					res.PushField(boilerplate.ExistingField{
						Name:               col.ColumnName,
						Nullable:           columnDef.Nullable,
						Type:               columnDef.Type,
						CharacterMaxLength: columnDef.Length,
						HasDefault:         col.Default != "",
					})
				}

				return nil
			})
		}
	}

	return group.Wait()
}

// The error message returned from Snowflake for the multi-statement table
// creation queries is mostly a bunch of garbled nonsense about Javascript
// execution errors if the user doesn't have permission to create tables in the
// schema, but it does embed the useful part in the midst of all that. This is a
// common enough error mode that we do some extra processing for this case to
// make it more obvious what is happening.
var errInsufficientPrivileges = regexp.MustCompile(`Insufficient privileges to operate on schema '([^']+)'`)

func (c *client) CreateTable(ctx context.Context, tc sql.TableCreate) error {
	if _, err := c.db.ExecContext(ctx, tc.TableCreateSql); err != nil {
		if matches := errInsufficientPrivileges.FindStringSubmatch(err.Error()); len(matches) > 0 {
			err = errors.New(matches[0])
		}
		return err
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
	if len(ta.DropNotNulls) > 0 || len(ta.AddColumns) > 0 {
		var alterColumnStmtBuilder strings.Builder
		if err := renderTemplates(c.ep.Dialect).alterTableColumns.Execute(&alterColumnStmtBuilder, ta); err != nil {
			return "", nil, fmt.Errorf("rendering alter table columns statement: %w", err)
		}
		alterColumnStmt := alterColumnStmtBuilder.String()
		stmts = append(stmts, alterColumnStmt)
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
	return runShowPaginated(ctx, c.xdb, func(cursor *string) string {
		if cursor != nil {
			return fmt.Sprintf("SHOW TERSE SCHEMAS LIMIT %d FROM '%s';", showQueryLimit, *cursor)
		} else {
			return fmt.Sprintf("SHOW TERSE SCHEMAS LIMIT %d;", showQueryLimit)
		}
	})
}

func (c *client) CreateSchema(ctx context.Context, schemaName string) (string, error) {
	return sql.StdCreateSchema(ctx, c.dbNoSchema, c.ep.Dialect, schemaName)
}

func preReqs(ctx context.Context, cfg config, tenant string) *cerrors.PrereqErr {
	errs := &cerrors.PrereqErr{}

	dsn, err := cfg.toURI(tenant, false)
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
			case 390100:
				err = fmt.Errorf("incorrect username or password")
			case 390201:
				// This means "doesn't exist or not authorized", and we don't have a great way to
				// distinguish between that for the database, schema, or warehouse. The snowflake
				// error message in these cases is fairly decent fortunately.
			case 390189:
				err = fmt.Errorf("role %q does not exist", cfg.Role)
			}

			if strings.Contains(sfError.Message, "MFA with TOTP is required.") {
				err = fmt.Errorf("username/password authentication is not supported for your account - try a different authentication method")
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

func (c *client) ExecStatements(ctx context.Context, statements []string) error {
	return sql.StdSQLExecStatements(ctx, c.db, statements)
}

func (c *client) InstallFence(ctx context.Context, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	return sql.Fence{}, nil
}

func (c *client) Close() {
	c.db.Close()
	c.dbNoSchema.Close()
}

// runShowPaginated runs a SHOW query, handling pagination for extremely large
// result sets that might have more than 10,000 items.
func runShowPaginated(ctx context.Context, xdb *sqlx.DB, queryGen func(*string) string) ([]string, error) {
	type res struct {
		Name string `db:"name"`
	}

	var out []string
	var cursor *string
	for {
		var r []res
		if err := xdb.SelectContext(ctx, &r, queryGen(cursor)); err != nil {
			return nil, err
		}

		for _, rr := range r {
			out = append(out, rr.Name)
		}
		if len(r) < showQueryLimit {
			break
		}

		cursor = &r[len(r)-1].Name
	}

	return out, nil
}
