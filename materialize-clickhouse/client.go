package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

type client struct {
	db *stdsql.DB
	ep *sql.Endpoint[config]
}

// existingFieldMeta is ClickHouse-specific column metadata attached to
// boilerplate.ExistingField.Meta by PopulateInfoSchema.
type existingFieldMeta struct {
	// isKeyColumn is set for columns in the table's sorting key or partition
	// key. ClickHouse forbids ALTERing the type of such columns (code 524),
	// including widening them to Nullable.
	isKeyColumn bool
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
		"SELECT database, table, name, type, default_expression, is_in_sorting_key, is_in_partition_key FROM system.columns WHERE database = %s",
		c.ep.Dialect.Literal(database),
	))
	if err != nil {
		return fmt.Errorf("querying system.columns: %w", err)
	}
	defer colRows.Close()

	for colRows.Next() {
		var dbName, tableName, colName, colType, defaultExpr string
		var isInSortingKey, isInPartitionKey uint8
		if err := colRows.Scan(&dbName, &tableName, &colName, &colType, &defaultExpr, &isInSortingKey, &isInPartitionKey); err != nil {
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
			Meta:       existingFieldMeta{isKeyColumn: isInSortingKey == 1 || isInPartitionKey == 1},
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

var clickHouseMigrationSteps = []sql.ColumnMigrationStep{

	// Step 0: Add new columns using temporary names.
	func(dialect sql.Dialect, table sql.Table, instructions []sql.MigrationInstruction) ([]string, error) {
		d := struct {
			Table        sql.Table
			Instructions []sql.MigrationInstruction
		}{
			Table:        table,
			Instructions: instructions,
		}

		var alterStmt strings.Builder
		if err := renderTemplates(dialect, false).alterTargetMigrateAddColumn.Execute(&alterStmt, d); err != nil {
			return nil, fmt.Errorf("executing alter target migrate add column: %w", err)
		}
		stmts := make([]string, 0, len(instructions))
		for _, s := range strings.Split(alterStmt.String(), ";") {
			if s = strings.TrimSpace(s); s != "" {
				stmts = append(stmts, s+";")
			}
		}
		return stmts, nil
	},

	// Step 1: Populate the temporary columns
	func(dialect sql.Dialect, table sql.Table, instructions []sql.MigrationInstruction) ([]string, error) {
		d := struct {
			Table        sql.Table
			Instructions []sql.MigrationInstruction
		}{
			Table:        table,
			Instructions: instructions,
		}

		var alterStmt strings.Builder
		if err := renderTemplates(dialect, false).alterTargetMigratePopulateColumn.Execute(&alterStmt, d); err != nil {
			return nil, fmt.Errorf("executing alter target migrate populate column: %w", err)
		}
		stmts := make([]string, 0, len(instructions))
		for _, s := range strings.Split(alterStmt.String(), ";") {
			if s = strings.TrimSpace(s); s != "" {
				stmts = append(stmts, s+";")
			}
		}
		return stmts, nil
	},

	// Step 2: Drop the old columns. SETTINGS mutations_sync = 1 makes the
	// DROP block until the mutation completes, so an interruption between
	// steps cannot leave a *_flowtmp1 orphan paired with its original.
	func(dialect sql.Dialect, table sql.Table, instructions []sql.MigrationInstruction) ([]string, error) {
		d := struct {
			Table        sql.Table
			Instructions []sql.MigrationInstruction
		}{
			Table:        table,
			Instructions: instructions,
		}

		var alterStmt strings.Builder
		if err := renderTemplates(dialect, false).alterTargetMigrateDropColumn.Execute(&alterStmt, d); err != nil {
			return nil, fmt.Errorf("executing alter target migrate drop column: %w", err)
		}
		stmts := make([]string, 0, len(instructions))
		for _, s := range strings.Split(alterStmt.String(), ";") {
			if s = strings.TrimSpace(s); s != "" {
				stmts = append(stmts, s+";")
			}
		}
		return stmts, nil
	},

	// Step 3: Rename the temporary columns, also blocking on completion.
	func(dialect sql.Dialect, table sql.Table, instructions []sql.MigrationInstruction) ([]string, error) {
		d := struct {
			Table        sql.Table
			Instructions []sql.MigrationInstruction
		}{
			Table:        table,
			Instructions: instructions,
		}

		var alterStmt strings.Builder
		if err := renderTemplates(dialect, false).alterTargetMigrateRenameColumn.Execute(&alterStmt, d); err != nil {
			return nil, fmt.Errorf("executing alter target migrate rename column: %w", err)
		}
		stmts := make([]string, 0, len(instructions))
		for _, s := range strings.Split(alterStmt.String(), ";") {
			if s = strings.TrimSpace(s); s != "" {
				stmts = append(stmts, s+";")
			}
		}
		return stmts, nil
	},

	// Step 4: No-op. ClickHouse non-nullable columns are not required, so the
	// new columns' ultimate nullability was set in step 0.
	func(dialect sql.Dialect, table sql.Table, instructions []sql.MigrationInstruction) ([]string, error) {
		return nil, nil
	},
}

func (c *client) AlterTable(ctx context.Context, ta sql.TableAlter) (string, boilerplate.ActionApplyFn, error) {
	var stmts []string

	// ClickHouse forbids changing the type of a sorting-key or partition-key
	// column (code 524), so such a column cannot be widened to Nullable. It
	// doesn't need to be: inserts name their columns explicitly, and a
	// non-nullable column omitted from an insert receives the type's zero value.
	var dropNotNulls []boilerplate.ExistingField
	for _, col := range ta.DropNotNulls {
		if meta, ok := col.Meta.(existingFieldMeta); ok && meta.isKeyColumn {
			log.WithFields(log.Fields{
				"table":  ta.Identifier,
				"column": col.Name,
			}).Warn("not making key column nullable because ClickHouse forbids ALTER of sorting-key and partition-key columns")
			continue
		}
		dropNotNulls = append(dropNotNulls, col)
	}
	ta.DropNotNulls = dropNotNulls

	if len(ta.AddColumns) > 0 || len(ta.DropNotNulls) > 0 {
		var alterStmtBuilder strings.Builder
		if err := renderTemplates(c.ep.Dialect, c.ep.Config.HardDelete).alterTargetColumns.Execute(&alterStmtBuilder, ta); err != nil {
			return "", nil, fmt.Errorf("rendering alter table statement: %w", err)
		}
		var alterStmt = alterStmtBuilder.String()
		// The template generates separate ALTER statements per column.
		for _, s := range strings.Split(alterStmt, ";\n") {
			s = strings.TrimSpace(s)
			if s != "" {
				stmts = append(stmts, s+";")
			}
		}
	}

	if len(ta.ColumnTypeChanges) > 0 {
		// A column referenced by the table's PARTITION BY key cannot change
		// type at all: ClickHouse forbids both MODIFY COLUMN (code 524) and
		// DROP COLUMN (code 47) on partition-key columns, so the multi-step
		// rename migration can never succeed. Fail before executing any step,
		// so no temporary column is left behind and the error says what to do.
		partitionKeyCols, err := c.partitionKeyColumns(ctx, ta.Path[0])
		if err != nil {
			return "", nil, err
		}
		for _, m := range ta.ColumnTypeChanges {
			if partitionKeyCols[m.Field] {
				return "", nil, fmt.Errorf(
					"cannot change the type of column %q: it is referenced by the PARTITION BY key of table %s, which ClickHouse forbids altering; backfill the binding to re-create the table with the new column type",
					m.Field, ta.Identifier)
			}
		}

		steps, err := sql.StdColumnTypeMigrations(ctx, c.ep.Dialect, ta.Table, ta.ColumnTypeChanges, clickHouseMigrationSteps...)
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

// partitionKeyColumns returns the set of column names referenced by the
// table's PARTITION BY key.
func (c *client) partitionKeyColumns(ctx context.Context, table string) (map[string]bool, error) {
	rows, err := c.db.QueryContext(ctx, fmt.Sprintf(
		"SELECT name FROM system.columns WHERE database = %s AND table = %s AND is_in_partition_key = 1",
		c.ep.Dialect.Literal(c.ep.Config.Database), c.ep.Dialect.Literal(table),
	))
	if err != nil {
		return nil, fmt.Errorf("querying partition key columns of %q: %w", table, err)
	}
	defer rows.Close()

	var out = make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scanning partition key column of %q: %w", table, err)
		}
		out[name] = true
	}
	return out, rows.Err()
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

// MustRecreateResource reports a changed partition_by, which can only be
// applied by dropping and re-creating the table: ClickHouse never accepts
// PARTITION BY via ALTER, and TRUNCATE preserves the partition key.
func (c *client) MustRecreateResource(_ *pm.Request_Apply, lastBinding, newBinding *pf.MaterializationSpec_Binding) (bool, error) {
	if lastBinding == nil || newBinding == nil {
		return false, nil
	}
	var lastRC, newRC tableConfig
	if err := json.Unmarshal(lastBinding.ResourceConfigJson, &lastRC); err != nil {
		return false, fmt.Errorf("parsing last binding resource config: %w", err)
	}
	if err := json.Unmarshal(newBinding.ResourceConfigJson, &newRC); err != nil {
		return false, fmt.Errorf("parsing new binding resource config: %w", err)
	}
	return strings.TrimSpace(lastRC.PartitionBy) != strings.TrimSpace(newRC.PartitionBy), nil
}

func (c *client) ListCheckpointsEntries(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (c *client) DeleteCheckpointsEntry(ctx context.Context, taskName string) error {
	return nil
}

func (c *client) SnapshotTestTable(ctx context.Context, path []string) (columnNames []string, rows [][]any, _ error) {
	return sql.SnapshotTestTable(ctx, c.db, c.ep.Dialect.Identifier(path...))
}

func (c *client) Close() {
	_ = c.db.Close()
}
