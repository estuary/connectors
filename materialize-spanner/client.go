package main

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	log "github.com/sirupsen/logrus"
)

type client struct {
	dataClient  *spanner.Client
	adminClient *database.DatabaseAdminClient
	cfg         config
	ep          *sql.Endpoint[config]
	templates   templates
	dbPath      string // Full database path: projects/{project}/instances/{instance}/databases/{database}

	// DDL batching
	pendingDDL []string
	ddlMutex   sync.Mutex
}

func newClient(ctx context.Context, ep *sql.Endpoint[config]) (sql.Client, error) {
	cfg := ep.Config

	// Build the database path
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s",
		cfg.ProjectID, cfg.InstanceID, cfg.Database)

	var opts []option.ClientOption

	// Add credentials if provided
	if cfg.Credentials != nil {
		if cfg.Credentials.ServiceAccountJSON != "" {
			opts = append(opts, option.WithCredentialsJSON([]byte(cfg.Credentials.ServiceAccountJSON)))
		}
		// If no service account JSON provided, use Application Default Credentials (ADC)
	}

	// Create admin client for DDL operations
	adminClient, err := database.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating Spanner admin client: %w", err)
	}

	// Create data client for query and DML operations
	dataClient, err := spanner.NewClient(ctx, dbPath, opts...)
	if err != nil {
		adminClient.Close()
		return nil, fmt.Errorf("creating Spanner data client: %w", err)
	}

	templates := renderTemplates(ep.Dialect)

	return &client{
		dataClient:  dataClient,
		adminClient: adminClient,
		cfg:         cfg,
		ep:          ep,
		templates:   templates,
		dbPath:      dbPath,
	}, nil
}

func preReqs(ctx context.Context, cfg config) *cerrors.PrereqErr {
	errs := &cerrors.PrereqErr{}

	// Build the database path
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s",
		cfg.ProjectID, cfg.InstanceID, cfg.Database)

	var opts []option.ClientOption

	if cfg.Credentials != nil && cfg.Credentials.ServiceAccountJSON != "" {
		opts = append(opts, option.WithCredentialsJSON([]byte(cfg.Credentials.ServiceAccountJSON)))
	}

	// Try to create a client to verify connectivity
	client, err := spanner.NewClient(ctx, dbPath, opts...)
	if err != nil {
		if strings.Contains(err.Error(), "PermissionDenied") {
			errs.Err(fmt.Errorf("permission denied: check your credentials and IAM roles"))
		} else if strings.Contains(err.Error(), "NotFound") {
			errs.Err(fmt.Errorf("database not found: %s (check project, instance, and database IDs)", dbPath))
		} else {
			errs.Err(fmt.Errorf("connecting to Spanner: %w", err))
		}
		return errs
	}
	defer client.Close()

	// Verify we can query the database
	stmt := spanner.Statement{SQL: "SELECT 1"}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	if _, err := iter.Next(); err != nil && err != iterator.Done {
		errs.Err(fmt.Errorf("querying Spanner database: %w", err))
	}

	return errs
}

// addPendingDDL accumulates DDL statements to be executed in a batch later
func (c *client) addPendingDDL(statements ...string) {
	c.ddlMutex.Lock()
	defer c.ddlMutex.Unlock()
	for _, stmt := range statements {
		if stmt != "" {
			c.pendingDDL = append(c.pendingDDL, stmt)
		}
	}
}

// FlushDDL executes all accumulated DDL statements in a single batch operation
func (c *client) FlushDDL(ctx context.Context) error {
	c.ddlMutex.Lock()
	defer c.ddlMutex.Unlock()

	if len(c.pendingDDL) == 0 {
		return nil
	}

	log.WithField("count", len(c.pendingDDL)).Info("flushing batched DDL statements")

	op, err := c.adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   c.dbPath,
		Statements: c.pendingDDL,
	})
	if err != nil {
		return fmt.Errorf("submitting batched DDL: %w", err)
	}

	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("executing batched DDL: %w", err)
	}

	log.Info("successfully executed batched DDL")
	c.pendingDDL = nil
	return nil
}

func (c *client) PopulateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema, resourcePaths [][]string) error {
	// Query INFORMATION_SCHEMA.TABLES and INFORMATION_SCHEMA.COLUMNS
	// Build a map of tables we care about (both schema.table and just table for root namespace)
	tableMap := make(map[string]bool)
	for _, path := range resourcePaths {
		locator := c.ep.Dialect.TableLocator(path)
		// Store both "schema.table" and "table" formats
		if locator.TableSchema != "" {
			tableMap[locator.TableSchema+"."+locator.TableName] = true
		} else {
			tableMap[locator.TableName] = true
		}
	}

	// Query for table information including schema
	stmt := spanner.Statement{
		SQL: `
			SELECT
				t.table_schema,
				t.table_name,
				c.column_name,
				c.spanner_type,
				c.is_nullable,
				c.ordinal_position
			FROM information_schema.tables t
			JOIN information_schema.columns c
				ON t.table_schema = c.table_schema
				AND t.table_name = c.table_name
			ORDER BY t.table_schema, t.table_name, c.ordinal_position
		`,
	}

	iter := c.dataClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("querying INFORMATION_SCHEMA: %w", err)
		}

		var schemaName, tableName, columnName, spannerType, isNullable string
		var ordinalPosition int64

		if err := row.Columns(&schemaName, &tableName, &columnName, &spannerType, &isNullable, &ordinalPosition); err != nil {
			return fmt.Errorf("scanning row: %w", err)
		}

		// Build the resource key
		var resourceKey string
		if schemaName != "" {
			resourceKey = schemaName + "." + tableName
		} else {
			resourceKey = tableName
		}

		// Only process tables we care about
		if !tableMap[resourceKey] {
			continue
		}

		is.PushResource(resourceKey).PushField(boilerplate.ExistingField{
			Name:               columnName,
			Nullable:           isNullable == "YES",
			Type:               spannerType,
			CharacterMaxLength: 0,
		})
	}

	return nil
}

func (c *client) CreateTable(ctx context.Context, tc sql.TableCreate) error {
	log.WithField("name", tc.Identifier).Info("client: queueing table creation for batched DDL")
	var res tableConfig
	if tc.Resource != nil {
		res = tc.Resource.(tableConfig)
	}

	// Accumulate DDL statements for batch execution
	c.addPendingDDL(tc.TableCreateSql)

	if res.AdditionalSql != "" {
		c.addPendingDDL(res.AdditionalSql)
		log.WithFields(log.Fields{
			"table": tc.Identifier,
			"query": res.AdditionalSql,
		}).Info("queuing AdditionalSql for batched DDL")
	}

	log.WithField("name", tc.Identifier).Info("client: queued table creation")
	return nil
}

func (c *client) DeleteTable(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	stmt := fmt.Sprintf("DROP TABLE %s", c.ep.Dialect.Identifier(path...))

	return stmt, func(ctx context.Context) error {
		// Accumulate for batched DDL execution
		c.addPendingDDL(stmt)
		return nil
	}, nil
}

func (c *client) TruncateTable(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	// Spanner doesn't have TRUNCATE TABLE, so we use DELETE FROM without WHERE clause
	// Note: For large tables, this can be slow. Consider using Partitioned DML in the future.
	stmt := fmt.Sprintf("DELETE FROM %s WHERE TRUE", c.ep.Dialect.Identifier(path...))

	return stmt, func(ctx context.Context) error {
		// Use DML to delete all rows
		_, err := c.dataClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			_, err := txn.Update(ctx, spanner.Statement{SQL: stmt})
			return err
		})
		return err
	}, nil
}

func (c *client) AlterTable(ctx context.Context, ta sql.TableAlter) (string, boilerplate.ActionApplyFn, error) {
	var ddlStatements []string

	// Spanner only supports adding columns, not dropping NOT NULL constraints after creation
	if len(ta.AddColumns) > 0 {
		var alterColumnStmtBuilder strings.Builder
		if err := c.templates.alterTableColumns.Execute(&alterColumnStmtBuilder, ta); err != nil {
			return "", nil, fmt.Errorf("rendering alter table columns statement: %w", err)
		}
		alterColumnStmt := alterColumnStmtBuilder.String()

		if alterColumnStmt != "" {
			ddlStatements = append(ddlStatements, alterColumnStmt)
		}
	}

	if len(ta.DropNotNulls) > 0 {
		// Spanner does not support dropping NOT NULL constraints
		return "", nil, fmt.Errorf("Spanner does not support dropping NOT NULL constraints on existing columns")
	}

	if len(ta.ColumnTypeChanges) > 0 {
		// Spanner has limited support for column type changes
		// For now, we'll try to generate migration steps, but many won't work
		if steps, err := sql.StdColumnTypeMigrations(ctx, c.ep.Dialect, ta.Table, ta.ColumnTypeChanges); err != nil {
			return "", nil, fmt.Errorf("rendering column migration steps: %w", err)
		} else {
			ddlStatements = append(ddlStatements, steps...)
		}
	}

	return strings.Join(ddlStatements, "\n"), func(ctx context.Context) error {
		if len(ddlStatements) == 0 {
			return nil
		}

		// Accumulate for batched DDL execution
		c.addPendingDDL(ddlStatements...)
		return nil
	}, nil
}

func (c *client) ListSchemas(ctx context.Context) ([]string, error) {
	// Query INFORMATION_SCHEMA.SCHEMATA for Cloud Spanner
	stmt := spanner.Statement{
		SQL: `SELECT schema_name FROM information_schema.schemata WHERE schema_name != ''`,
	}

	iter := c.dataClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	var schemas []string
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("querying information_schema.schemata: %w", err)
		}

		var schemaName string
		if err := row.Columns(&schemaName); err != nil {
			return nil, fmt.Errorf("scanning schema name: %w", err)
		}
		schemas = append(schemas, schemaName)
	}

	return schemas, nil
}

func (c *client) CreateSchema(ctx context.Context, schemaName string) (string, error) {
	stmt := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", c.ep.Dialect.Identifier(schemaName))

	// Accumulate for batched DDL execution
	c.addPendingDDL(stmt)

	return stmt, nil
}

func (c *client) ExecStatements(ctx context.Context, statements []string) error {
	if len(statements) == 0 {
		return nil
	}

	// Execute DDL statements
	op, err := c.adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   c.dbPath,
		Statements: statements,
	})
	if err != nil {
		return fmt.Errorf("submitting DDL statements: %w", err)
	}

	return op.Wait(ctx)
}

func (c *client) InstallFence(ctx context.Context, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	log.Info("client: installing fence")
	// First, ensure the checkpoints table exists
	createTableStmt := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			materialization STRING(MAX) NOT NULL,
			key_begin INT64 NOT NULL,
			key_end INT64 NOT NULL,
			fence INT64 NOT NULL,
			checkpoint STRING(MAX)
		) PRIMARY KEY (materialization, key_begin, key_end)
	`, c.ep.Dialect.Identifier(checkpoints.Path...))

	op, err := c.adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   c.dbPath,
		Statements: []string{createTableStmt},
	})
	if err != nil {
		return sql.Fence{}, fmt.Errorf("creating checkpoints table: %w", err)
	}

	if err := op.Wait(ctx); err != nil {
		// Ignore "already exists" errors
		if !strings.Contains(err.Error(), "already exists") {
			return sql.Fence{}, fmt.Errorf("waiting for checkpoints table creation: %w", err)
		}
	}

	// Now install/update the fence using the template
	var fenceStmtBuilder strings.Builder
	if err := c.templates.installFence.Execute(&fenceStmtBuilder, fence); err != nil {
		return sql.Fence{}, fmt.Errorf("rendering install fence statement: %w", err)
	}
	fenceStmt := fenceStmtBuilder.String()

	// Execute the fence installation/update
	// This is a multi-statement operation, so we need to split it
	statements := strings.Split(fenceStmt, ";")
	var mergeStmt, selectStmt string
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if strings.HasPrefix(strings.ToUpper(stmt), "MERGE") {
			mergeStmt = stmt
		} else if strings.HasPrefix(strings.ToUpper(stmt), "SELECT") {
			selectStmt = stmt
		}
	}

	// Execute merge in a transaction
	_, err = c.dataClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		if mergeStmt != "" {
			_, err := txn.Update(ctx, spanner.Statement{SQL: mergeStmt})
			if err != nil {
				return fmt.Errorf("executing MERGE: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return sql.Fence{}, fmt.Errorf("installing fence: %w", err)
	}

	// Read back the fence value
	if selectStmt != "" {
		iter := c.dataClient.Single().Query(ctx, spanner.Statement{SQL: selectStmt})
		defer iter.Stop()

		row, err := iter.Next()
		if err != nil {
			return sql.Fence{}, fmt.Errorf("reading fence value: %w", err)
		}

		var fenceValue int64
		var checkpoint []byte
		if err := row.Columns(&fenceValue, &checkpoint); err != nil {
			return sql.Fence{}, fmt.Errorf("scanning fence row: %w", err)
		}

		fence.Fence = fenceValue
		fence.Checkpoint = checkpoint
	}

	log.Info("client: installed fence")
	return fence, nil
}

func (c *client) Close() {
	if c.dataClient != nil {
		c.dataClient.Close()
	}
	if c.adminClient != nil {
		c.adminClient.Close()
	}
}
