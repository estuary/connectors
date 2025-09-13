package main

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/estuary/connectors/go/blob"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"

	_ "github.com/marcboeker/go-duckdb/v2"
)

var _ sql.SchemaManager = (*client)(nil)

type client struct {
	db *stdsql.DB
	ep *sql.Endpoint[config]
}

func newClient(ctx context.Context, ep *sql.Endpoint[config]) (sql.Client, error) {
	db, err := ep.Config.db(ctx)
	if err != nil {
		return nil, err
	}

	return &client{
		db: db,
		ep: ep,
	}, nil
}

func (c *client) PopulateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema, resourcePaths [][]string) error {
	return sql.StdPopulateInfoSchema(ctx, is, c.db, c.ep.Dialect, c.ep.Config.Database, resourcePaths)
}

func (c *client) CreateTable(ctx context.Context, tc sql.TableCreate) error {
	_, err := c.db.ExecContext(ctx, tc.TableCreateSql)
	return err
}

func (c *client) DeleteTable(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	stmt := fmt.Sprintf("DROP TABLE %s;", duckDialect.Identifier(path...))

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

	// Duckdb only supports a single ALTER TABLE operation per statement.
	for _, col := range ta.AddColumns {
		stmts = append(stmts, fmt.Sprintf(
			"ALTER TABLE %s ADD COLUMN %s %s;",
			ta.Identifier,
			col.Identifier,
			col.NullableDDL,
		))
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
	// MotherDuck does not limit the schema listing to the currently connected database, so the
	// StdListSchemasFn won't work if there are schemas with the same name in other databases.
	rows, err := c.db.QueryContext(ctx, fmt.Sprintf(
		"select schema_name from information_schema.schemata where catalog_name = %s",
		duckDialect.Literal(c.ep.Config.Database),
	))
	if err != nil {
		return nil, fmt.Errorf("querying schemata: %w", err)
	}
	defer rows.Close()

	out := []string{}

	for rows.Next() {
		var schema string
		if err := rows.Scan(&schema); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		out = append(out, schema)
	}

	return out, nil
}

func (c *client) CreateSchema(ctx context.Context, schemaName string) (string, error) {
	return sql.StdCreateSchema(ctx, c.db, duckDialect, schemaName)
}

func preReqs(ctx context.Context, cfg config) *cerrors.PrereqErr {
	errs := &cerrors.PrereqErr{}

	db, err := cfg.db(ctx)
	if err != nil {
		errs.Err(err)
		return errs
	}

	pingCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	if err := db.PingContext(pingCtx); err != nil {
		errs.Err(err)
	}

	bucket, path, err := cfg.toBucketAndPath(ctx)
	if err != nil {
		errs.Err(err)
		return errs
	}

	if err := bucket.CheckPermissions(ctx, blob.CheckPermissionsConfig{Prefix: path}); err != nil {
		errs.Err(err)
	}

	return errs
}

func (c *client) ExecStatements(ctx context.Context, statements []string) error {
	return sql.StdSQLExecStatements(ctx, c.db, statements)
}

func (c *client) InstallFence(ctx context.Context, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	return sql.StdInstallFence(ctx, c.db, checkpoints, fence)
}

func (c *client) Close() {
	c.db.Close()
}
