package main

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"strings"
	"time"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

type client struct {
	db *stdsql.DB
	ep *sql.Endpoint[config]
}

func newClient(ctx context.Context, ep *sql.Endpoint[config]) (sql.Client, error) {
	db := ep.Config.openDB()
	return &client{db: db, ep: ep}, nil
}

func preReqs(ctx context.Context, cfg config) *cerrors.PrereqErr {
	errs := &cerrors.PrereqErr{}

	db := cfg.openDB()
	defer db.Close()

	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		errs.Err(fmt.Errorf("unable to connect to ClickHouse at %q: %w", cfg.Address, err))
	}

	return errs
}

func (c *client) PopulateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema, resourcePaths [][]string) error {
	if len(resourcePaths) == 0 {
		return nil
	}

	database := c.ep.Config.Database

	// Query tables from system.tables.
	tableRows, err := c.db.QueryContext(ctx, fmt.Sprintf(
		"SELECT database, name FROM system.tables WHERE database = %s",
		c.ep.Dialect.Literal(database),
	))
	if err != nil {
		return fmt.Errorf("querying system.tables: %w", err)
	}
	defer tableRows.Close()

	for tableRows.Next() {
		var dbName, tableName string
		if err := tableRows.Scan(&dbName, &tableName); err != nil {
			return fmt.Errorf("scanning table row: %w", err)
		}
		is.PushResource(dbName, tableName)
	}
	if err := tableRows.Err(); err != nil {
		return fmt.Errorf("iterating table rows: %w", err)
	}

	// Query columns from system.columns.
	colRows, err := c.db.QueryContext(ctx, fmt.Sprintf(
		"SELECT database, table, name, type, default_expression FROM system.columns WHERE database = %s",
		c.ep.Dialect.Literal(database),
	))
	if err != nil {
		return fmt.Errorf("querying system.columns: %w", err)
	}
	defer colRows.Close()

	for colRows.Next() {
		var dbName, tableName, colName, colType, defaultExpr string
		if err := colRows.Scan(&dbName, &tableName, &colName, &colType, &defaultExpr); err != nil {
			return fmt.Errorf("scanning column row: %w", err)
		}

		// Skip internal columns managed by the connector.
		if colName == "_version" || colName == "_is_deleted" {
			continue
		}

		// Strip Nullable wrapper to get the base type.
		isNullable := strings.HasPrefix(colType, "Nullable(")
		baseType := colType
		if isNullable {
			baseType = colType[len("Nullable(") : len(colType)-1]
		}

		is.PushResource(dbName, tableName).PushField(boilerplate.ExistingField{
			Name:       colName,
			Nullable:   isNullable,
			Type:       strings.ToLower(baseType),
			HasDefault: defaultExpr != "",
		})
	}
	if err := colRows.Err(); err != nil {
		return fmt.Errorf("iterating column rows: %w", err)
	}

	return nil
}

func (c *client) CreateTable(ctx context.Context, tc sql.TableCreate) error {
	_, err := c.db.ExecContext(ctx, tc.TableCreateSql)
	return err
}

func (c *client) DeleteTable(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	stmt := fmt.Sprintf("DROP TABLE IF EXISTS %s;", c.ep.Dialect.Identifier(path...))
	return stmt, func(ctx context.Context) error {
		_, err := c.db.ExecContext(ctx, stmt)
		return err
	}, nil
}

func (c *client) AlterTable(ctx context.Context, ta sql.TableAlter) (string, boilerplate.ActionApplyFn, error) {
	var stmts []string

	if len(ta.AddColumns) > 0 || len(ta.DropNotNulls) > 0 {
		var alterStmtBuilder strings.Builder
		if err := renderTemplates(c.ep.Dialect).alterTableColumns.Execute(&alterStmtBuilder, ta); err != nil {
			return "", nil, fmt.Errorf("rendering alter table statement: %w", err)
		}
		alterStmt := alterStmtBuilder.String()
		// The template generates separate ALTER statements per column.
		for _, s := range strings.Split(alterStmt, ";") {
			s = strings.TrimSpace(s)
			if s != "" {
				stmts = append(stmts, s+";")
			}
		}
	}

	if len(ta.ColumnTypeChanges) > 0 {
		steps, err := sql.StdColumnTypeMigrations(ctx, c.ep.Dialect, ta.Table, ta.ColumnTypeChanges)
		if err != nil {
			return "", nil, fmt.Errorf("rendering column migration steps: %w", err)
		}
		stmts = append(stmts, steps...)
	}

	return strings.Join(stmts, "\n"), func(ctx context.Context) error {
		for _, stmt := range stmts {
			if _, err := c.db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("executing %q: %w", stmt, err)
			}
		}
		return nil
	}, nil
}

func (c *client) TruncateTable(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	stmt := fmt.Sprintf("TRUNCATE TABLE %s;", c.ep.Dialect.Identifier(path...))
	return stmt, func(ctx context.Context) error {
		_, err := c.db.ExecContext(ctx, stmt)
		return err
	}, nil
}

func (c *client) InstallFence(ctx context.Context, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	// ClickHouse does not support fencing. This should never be called since MetaCheckpoints is nil.
	return sql.Fence{}, fmt.Errorf("fencing is not supported by ClickHouse")
}

func (c *client) ExecStatements(ctx context.Context, statements []string) error {
	return sql.StdSQLExecStatements(ctx, c.db, statements)
}

func (c *client) MustRecreateResource(req *pm.Request_Apply, lastBinding, newBinding *pf.MaterializationSpec_Binding) (bool, error) {
	return false, nil
}

func (c *client) Close() {
	c.db.Close()
}
