package main

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
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

func newClient(_ context.Context, materializationName string, ep *sql.Endpoint[config]) (sql.Client, error) {
	var db = clickhouse.OpenDB(ep.Config.newClickhouseOptions())
	return &client{db: db, ep: ep}, nil
}

func preReqs(ctx context.Context, cfg config) *cerrors.PrereqErr {
	var errs = &cerrors.PrereqErr{}

	var db = clickhouse.OpenDB(cfg.newClickhouseOptions())
	defer db.Close()

	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, 20*time.Second)
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

	var database = c.ep.Config.Database

	// Query tables from system.tables.
	var tableRows, err = c.db.QueryContext(ctx, fmt.Sprintf(
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
	var colRows *stdsql.Rows
	colRows, err = c.db.QueryContext(ctx, fmt.Sprintf(
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

		// Skip the internal column managed by the connector.
		if colName == "_is_deleted" {
			continue
		}

		// Strip Nullable wrapper to get the base type.
		var isNullable = strings.HasPrefix(colType, "Nullable(")
		var baseType = colType
		if isNullable {
			baseType = colType[len("Nullable(") : len(colType)-1]
		}

		is.PushResource(dbName, tableName).PushField(boilerplate.ExistingField{
			Name:       colName,
			Nullable:   isNullable,
			Type:       baseType,
			HasDefault: defaultExpr != "",
		})
	}
	if err := colRows.Err(); err != nil {
		return fmt.Errorf("iterating column rows: %w", err)
	}

	return nil
}

func (c *client) CreateTable(ctx context.Context, tc sql.TableCreate) error {
	if _, err := c.db.ExecContext(ctx, tc.TableCreateSql); err != nil {
		return fmt.Errorf("creating table: %w", err)
	}
	return nil
}

func (c *client) DeleteTable(_ context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	var stmt = fmt.Sprintf("DROP TABLE IF EXISTS %s;", c.ep.Dialect.Identifier(path...))
	return stmt, func(ctx context.Context) error {
		if _, err := c.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("executing delete table: %w", err)
		}
		return nil
	}, nil
}

func (c *client) AlterTable(ctx context.Context, ta sql.TableAlter) (string, boilerplate.ActionApplyFn, error) {
	var stmts []string

	if len(ta.AddColumns) > 0 || len(ta.DropNotNulls) > 0 {
		var alterStmtBuilder strings.Builder
		if err := renderTemplates(c.ep.Dialect).targetAlterColumns.Execute(&alterStmtBuilder, ta); err != nil {
			return "", nil, fmt.Errorf("rendering alter table statement: %w", err)
		}
		var alterStmt = alterStmtBuilder.String()
		// The template generates separate ALTER statements per column.
		for _, s := range strings.Split(alterStmt, ";") {
			s = strings.TrimSpace(s)
			if s != "" {
				stmts = append(stmts, s+";")
			}
		}
	}

	if len(ta.ColumnTypeChanges) > 0 {
		var steps, err = sql.StdColumnTypeMigrations(ctx, c.ep.Dialect, ta.Table, ta.ColumnTypeChanges)
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

func (c *client) TruncateTable(_ context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	var stmt = fmt.Sprintf("TRUNCATE TABLE %s;", c.ep.Dialect.Identifier(path...))
	return stmt, func(ctx context.Context) error {
		if _, err := c.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("executing truncate table: %w", err)
		}
		return nil
	}, nil
}

func (c *client) InstallFence(_ context.Context, _ sql.Table, _ sql.Fence) (sql.Fence, error) {
	// ClickHouse does not support fencing. This should never be called since MetaCheckpoints is nil.
	return sql.Fence{}, fmt.Errorf("fencing is not supported by ClickHouse")
}

func (c *client) ExecStatements(ctx context.Context, statements []string) error {
	return sql.StdSQLExecStatements(ctx, c.db, statements)
}

func (c *client) MustRecreateResource(_ *pm.Request_Apply, _, _ *pf.MaterializationSpec_Binding) (bool, error) {
	return false, nil
}

func (c *client) Close() {
	_ = c.db.Close()
}
