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

func (c *client) DeleteTable(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	storeNames, err := c.findStoreTableNames(ctx, path)
	if err != nil {
		return "", nil, err
	}

	var stmts []string
	for _, name := range storeNames {
		stmts = append(stmts, fmt.Sprintf("DROP TABLE IF EXISTS %s;", c.ep.Dialect.Identifier(name)))
	}
	stmts = append(stmts, fmt.Sprintf("DROP TABLE IF EXISTS %s;", c.ep.Dialect.Identifier(path...)))

	return strings.Join(stmts, "\n"), func(ctx context.Context) error {
		for _, stmt := range stmts {
			if _, err := c.db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("executing %q: %w", stmt, err)
			}
		}
		return nil
	}, nil
}

func (c *client) AlterTable(ctx context.Context, ta sql.TableAlter) (string, boilerplate.ActionApplyFn, error) {
	var tpls = renderTemplates(c.ep.Dialect, c.ep.Config.HardDelete)

	storeNames, err := c.findStoreTableNames(ctx, ta.Table.Path)
	if err != nil {
		return "", nil, err
	}

	var tables = []sql.Table{ta.Table}
	for _, storeName := range storeNames {
		var storeTable = ta.Table
		storeTable.Identifier = c.ep.Dialect.Identifier(storeName)
		storeTable.Path = sql.TablePath{storeName}
		storeTable.InfoLocation = c.ep.Dialect.TableLocator(storeTable.Path)
		tables = append(tables, storeTable)
	}

	var stmts []string

	if len(ta.AddColumns) > 0 || len(ta.DropNotNulls) > 0 {
		for _, table := range tables {
			var b strings.Builder
			if err = tpls.alterTargetColumns.Execute(&b, sql.TableAlter{
				Table:        table,
				AddColumns:   ta.AddColumns,
				DropNotNulls: ta.DropNotNulls,
			}); err != nil {
				return "", nil, fmt.Errorf("rendering alter table statement for %s: %w", table.Identifier, err)
			}
			// The template generates separate ALTER statements per column.
			for _, s := range strings.Split(b.String(), ";") {
				s = strings.TrimSpace(s)
				if s != "" {
					stmts = append(stmts, s+";")
				}
			}
		}
	}

	if len(ta.ColumnTypeChanges) > 0 {
		for _, table := range tables {
			var steps, err = sql.StdColumnTypeMigrations(ctx, c.ep.Dialect, table, ta.ColumnTypeChanges)
			if err != nil {
				return "", nil, fmt.Errorf("rendering column migration steps for %s: %w", table.Identifier, err)
			}
			stmts = append(stmts, steps...)
		}
	}

	return strings.Join(stmts, "\n"), func(ctx context.Context) error {
		for _, stmt := range stmts {
			if _, err = c.db.ExecContext(ctx, stmt); err != nil {
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

// findStoreTableNames returns the names of any store tables that exist for the
// given target table path. Store tables follow the naming convention
// flow_temp_store_{hex_rangekey}_{tablename}.
func (c *client) findStoreTableNames(ctx context.Context, path []string) ([]string, error) {
	var tpls = renderTemplates(c.ep.Dialect, c.ep.Config.HardDelete)
	var table = sql.Table{TableShape: sql.TableShape{Path: path}}

	findSQL, err := sql.RenderTableTemplate(table, tpls.queryStoreTablesAllRangeKeys)
	if err != nil {
		return nil, fmt.Errorf("rendering queryStoreTablesAllRangeKeys: %w", err)
	}

	rows, err := c.db.QueryContext(ctx, findSQL)
	if err != nil {
		return nil, fmt.Errorf("querying for store tables: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scanning store table name: %w", err)
		}
		names = append(names, name)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating store table rows: %w", err)
	}

	return names, nil
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
