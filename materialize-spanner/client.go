package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
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

	opts = append(opts, option.WithCredentialsJSON([]byte(cfg.Credentials.ServiceAccountJSON)))

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

	templates := renderTemplates(ep.Dialect, !ep.Config.Advanced.DisableKeyDistributionOptimization)

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

	opts = append(opts, option.WithCredentialsJSON([]byte(cfg.Credentials.ServiceAccountJSON)))

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
		c.pendingDDL = append(c.pendingDDL, stmt)
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
	if len(resourcePaths) == 0 {
		return nil
	}

	// Build list of schemas to query
	schemas := make([]string, 0, len(resourcePaths))
	for _, p := range resourcePaths {
		loc := c.ep.Dialect.TableLocator(p)
		schemas = append(schemas, c.ep.Dialect.Literal(loc.TableSchema))
	}

	// Deduplicate schemas
	schemaSet := make(map[string]bool)
	uniqueSchemas := make([]string, 0, len(schemas))
	for _, s := range schemas {
		if !schemaSet[s] {
			schemaSet[s] = true
			uniqueSchemas = append(uniqueSchemas, s)
		}
	}

	// Query for tables first
	tablesStmt := spanner.Statement{
		SQL: fmt.Sprintf(`
			SELECT table_schema, table_name
			FROM information_schema.tables
			WHERE table_schema IN (%s)
		`, strings.Join(uniqueSchemas, ",")),
	}

	tablesIter := c.dataClient.Single().Query(ctx, tablesStmt)
	defer tablesIter.Stop()

	for {
		row, err := tablesIter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("querying INFORMATION_SCHEMA.TABLES: %w", err)
		}

		var tableSchema, tableName string
		if err := row.Columns(&tableSchema, &tableName); err != nil {
			return fmt.Errorf("scanning table row: %w", err)
		}

		is.PushResource(tableSchema, tableName)
	}

	// Query for columns
	columnsStmt := spanner.Statement{
		SQL: fmt.Sprintf(`
			SELECT table_schema, table_name, column_name, is_nullable, spanner_type
			FROM information_schema.columns
			WHERE table_schema IN (%s)
		`, strings.Join(uniqueSchemas, ",")),
	}

	columnsIter := c.dataClient.Single().Query(ctx, columnsStmt)
	defer columnsIter.Stop()

	for {
		row, err := columnsIter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("querying INFORMATION_SCHEMA.COLUMNS: %w", err)
		}

		var tableSchema, tableName, columnName, isNullable, spannerType string
		if err := row.Columns(&tableSchema, &tableName, &columnName, &isNullable, &spannerType); err != nil {
			return fmt.Errorf("scanning column row: %w", err)
		}

		is.PushResource(tableSchema, tableName).PushField(boilerplate.ExistingField{
			Name:               columnName,
			Nullable:           strings.EqualFold(isNullable, "YES"),
			Type:               spannerType,
			CharacterMaxLength: 0,
		})
	}

	return nil
}

func (c *client) MustRecreateResource(req *pm.Request_Apply, lastBinding, newBinding *pf.MaterializationSpec_Binding) (bool, error) {
	return keyDistributionOptimizationChanged(req.LastMaterialization.ConfigJson, req.Materialization.ConfigJson)
}

func (c *client) CreateTable(ctx context.Context, tc sql.TableCreate) error {
	log.WithField("name", tc.Identifier).Info("client: queueing table creation for batched DDL")
	var res tableConfig
	if tc.Resource != nil {
		res = tc.Resource.(tableConfig)
	}

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
		c.addPendingDDL(stmt)
		return nil
	}, nil
}

func (c *client) TruncateTable(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	// Spanner doesn't have TRUNCATE TABLE, so we use DELETE FROM without WHERE clause.
	// Use Partitioned DML to avoid mutation limits on large tables.
	// See: https://cloud.google.com/spanner/docs/dml-partitioned
	stmt := fmt.Sprintf("DELETE FROM %s WHERE TRUE", c.ep.Dialect.Identifier(path...))

	return stmt, func(ctx context.Context) error {
		_, err := c.dataClient.PartitionedUpdate(ctx, spanner.Statement{SQL: stmt})
		return err
	}, nil
}

func (c *client) AlterTable(ctx context.Context, ta sql.TableAlter) (string, boilerplate.ActionApplyFn, error) {
	var ddlStatements []string

	if len(ta.DropNotNulls) > 0 {
		// Query information_schema to get full column definitions
		colDDL := make(map[string]string)

		stmt := spanner.Statement{
			SQL: `SELECT column_name, spanner_type, column_default
			      FROM information_schema.columns
			      WHERE table_schema = @schema AND table_name = @table`,
			Params: map[string]interface{}{
				"schema": ta.InfoLocation.TableSchema,
				"table":  ta.InfoLocation.TableName,
			},
		}

		iter := c.dataClient.Single().Query(ctx, stmt)
		defer iter.Stop()

		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return "", nil, fmt.Errorf("querying columns for table %q in schema %q: %w",
					ta.InfoLocation.TableName, ta.InfoLocation.TableSchema, err)
			}

			var columnName, spannerType string
			var columnDefault spanner.NullString

			if err := row.Columns(&columnName, &spannerType, &columnDefault); err != nil {
				return "", nil, fmt.Errorf("scanning column row: %w", err)
			}

			// Build the DDL: spanner_type [DEFAULT (expression)]
			ddl := spannerType
			if columnDefault.Valid {
				ddl += " DEFAULT " + columnDefault.StringVal
			}

			colDDL[columnName] = ddl
		}

		// Update the Type field with the complete DDL (without NOT NULL)
		for idx := range ta.DropNotNulls {
			col := ta.DropNotNulls[idx].Name
			ddl, ok := colDDL[col]
			if !ok {
				return "", nil, fmt.Errorf("could not determine DDL for column %q", col)
			}
			ta.DropNotNulls[idx].Type = ddl
		}
	}

	if len(ta.AddColumns) > 0 || len(ta.DropNotNulls) > 0 {
		var alterColumnStmtBuilder strings.Builder
		if err := c.templates.alterTableColumns.Execute(&alterColumnStmtBuilder, ta); err != nil {
			return "", nil, fmt.Errorf("rendering alter table columns statement: %w", err)
		}
		alterColumnStmt := alterColumnStmtBuilder.String()

		if alterColumnStmt != "" {
			ddlStatements = append(ddlStatements, alterColumnStmt)
		}
	}

	if len(ta.ColumnTypeChanges) > 0 {
		// TODO: test migrations
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

	c.addPendingDDL(stmt)

	return stmt, nil
}

func (c *client) ExecStatements(ctx context.Context, statements []string) error {
	if len(statements) == 0 {
		return nil
	}

	// Strip trailing semicolons from statements (Spanner DDL API doesn't accept them)
	cleanedStatements := make([]string, len(statements))
	for i, stmt := range statements {
		cleanedStatements[i] = strings.TrimSuffix(strings.TrimSpace(stmt), ";")
	}

	// Execute DDL statements
	op, err := c.adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   c.dbPath,
		Statements: cleanedStatements,
	})
	if err != nil {
		return fmt.Errorf("submitting DDL statements: %w", err)
	}

	return op.Wait(ctx)
}

func (c *client) InstallFence(ctx context.Context, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	// The checkpoints table is created by CreateTable during Apply, not here.
	tableName := checkpoints.Identifier

	// Use a read-write transaction to implement fencing logic
	var resultFence sql.Fence
	_, err := c.dataClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// Step 1: Increment fence of ANY checkpoint that overlaps our key range
		// This uses DML UPDATE since mutations don't support conditional updates
		updateStmt := spanner.Statement{
			SQL: fmt.Sprintf(`UPDATE %s SET fence = fence + 1
				WHERE materialization = @materialization
				AND key_end >= @key_begin
				AND key_begin <= @key_end`, tableName),
			Params: map[string]interface{}{
				"materialization": fence.Materialization.String(),
				"key_begin":       int64(fence.KeyBegin),
				"key_end":         int64(fence.KeyEnd),
			},
		}
		if _, err := txn.Update(ctx, updateStmt); err != nil {
			return fmt.Errorf("incrementing overlapping fences: %w", err)
		}

		// Step 2: Read the checkpoint with the narrowest range that fully contains our range
		selectStmt := spanner.Statement{
			SQL: fmt.Sprintf(`SELECT fence, key_begin, key_end, checkpoint FROM %s
				WHERE materialization = @materialization
				AND key_begin <= @key_begin
				AND key_end >= @key_end
				ORDER BY (key_end - key_begin) ASC
				LIMIT 1`, tableName),
			Params: map[string]interface{}{
				"materialization": fence.Materialization.String(),
				"key_begin":       int64(fence.KeyBegin),
				"key_end":         int64(fence.KeyEnd),
			},
		}

		iter := txn.Query(ctx, selectStmt)
		defer iter.Stop()

		row, err := iter.Next()
		var readFence int64
		var readBegin, readEnd int64
		var checkpoint spanner.NullString

		if err == iterator.Done {
			// No overlapping row found - set invalid range to trigger insertion
			readBegin, readEnd = 1, 0
		} else if err != nil {
			return fmt.Errorf("reading narrowest overlapping fence: %w", err)
		} else {
			if err := row.Columns(&readFence, &readBegin, &readEnd, &checkpoint); err != nil {
				return fmt.Errorf("scanning fence row: %w", err)
			}
			fence.Fence = readFence
			if checkpoint.Valid {
				fence.Checkpoint, _ = base64.StdEncoding.DecodeString(checkpoint.StringVal)
			}
		}

		// Step 3: If no exact match exists, insert a new row for this key range
		if readBegin == int64(fence.KeyBegin) && readEnd == int64(fence.KeyEnd) {
			// Exact match exists, no need to insert
		} else {
			// Insert new row using mutation
			mutation := spanner.Insert(
				tableName,
				[]string{"materialization", "key_begin", "key_end", "fence", "checkpoint"},
				[]interface{}{
					fence.Materialization.String(),
					int64(fence.KeyBegin),
					int64(fence.KeyEnd),
					fence.Fence,
					base64.StdEncoding.EncodeToString(fence.Checkpoint),
				},
			)
			if err := txn.BufferWrite([]*spanner.Mutation{mutation}); err != nil {
				return fmt.Errorf("inserting new fence: %w", err)
			}
		}

		resultFence = fence
		return nil
	})
	if err != nil {
		return sql.Fence{}, fmt.Errorf("installing fence: %w", err)
	}

	log.Info("client: installed fence")
	return resultFence, nil
}

func (c *client) Close() {
	if c.dataClient != nil {
		c.dataClient.Close()
	}
	if c.adminClient != nil {
		c.adminClient.Close()
	}
}
