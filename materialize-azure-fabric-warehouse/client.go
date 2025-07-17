package main

import (
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/estuary/connectors/go/blob"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/google/uuid"
	"github.com/segmentio/encoding/json"
	log "github.com/sirupsen/logrus"
)

var _ sql.SchemaManager = (*client)(nil)

type client struct {
	db *stdsql.DB
	ep *sql.Endpoint[config]
}

func newClient(ctx context.Context, ep *sql.Endpoint[config]) (sql.Client, error) {
	db, err := ep.Config.db()
	if err != nil {
		return nil, err
	}

	return &client{
		db: db,
		ep: ep,
	}, nil
}

func (c *client) PopulateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema, resourcePaths [][]string) error {
	// The body of this function is a copy of sql.StdPopulateInfoSchema, except the
	// identifiers for the information schema views need to be in capital
	// letters for Fabric Warehouse. I'd hope to replace this at some point with
	// REST API calls if the necessary REST endpoints added to Fabric Warehouse,
	// since right now there's only endpoints to list warehouses and they don't
	// even work with service principal authentication.
	if len(resourcePaths) == 0 {
		return nil
	}

	schemas := make([]string, 0, len(resourcePaths))
	for _, p := range resourcePaths {
		loc := dialect.TableLocator(p)
		schemas = append(schemas, dialect.Literal(loc.TableSchema))
	}

	slices.Sort(schemas)
	schemas = slices.Compact(schemas)

	tables, err := c.db.QueryContext(ctx, fmt.Sprintf(`
		select table_schema, table_name
		from INFORMATION_SCHEMA.TABLES
		where table_catalog = %s
		and table_schema in (%s);
		`,
		dialect.Literal(c.ep.Config.Warehouse),
		strings.Join(schemas, ","),
	))
	if err != nil {
		return err
	}
	defer tables.Close()

	type tableRow struct {
		TableSchema string
		TableName   string
	}

	for tables.Next() {
		var t tableRow
		if err := tables.Scan(&t.TableSchema, &t.TableName); err != nil {
			return err
		}

		is.PushResource(t.TableSchema, t.TableName)
	}

	columns, err := c.db.QueryContext(ctx, fmt.Sprintf(`
		select table_schema, table_name, column_name, is_nullable, data_type, character_maximum_length, column_default
		from INFORMATION_SCHEMA.COLUMNS
		where table_catalog = %s
		and table_schema in (%s);
		`,
		dialect.Literal(c.ep.Config.Warehouse),
		strings.Join(schemas, ","),
	))
	if err != nil {
		return err
	}
	defer columns.Close()

	type columnRow struct {
		tableRow
		ColumnName             string
		IsNullable             string
		DataType               string
		CharacterMaximumLength stdsql.NullInt64
		ColumnDefault          stdsql.NullString
	}

	for columns.Next() {
		var c columnRow
		if err := columns.Scan(&c.TableSchema, &c.TableName, &c.ColumnName, &c.IsNullable, &c.DataType, &c.CharacterMaximumLength, &c.ColumnDefault); err != nil {
			return err
		}

		is.PushResource(c.TableSchema, c.TableName).PushField(boilerplate.ExistingField{
			Name:               c.ColumnName,
			Nullable:           strings.EqualFold(c.IsNullable, "yes"),
			Type:               c.DataType,
			CharacterMaxLength: int(c.CharacterMaximumLength.Int64),
			HasDefault:         c.ColumnDefault.Valid,
		})
	}
	if err := columns.Err(); err != nil {
		return err
	}

	return nil
}

func (c *client) CreateTable(ctx context.Context, tc sql.TableCreate) error {
	_, err := c.db.ExecContext(ctx, tc.TableCreateSql)
	return err
}

func (c *client) DeleteTable(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	stmt := fmt.Sprintf("DROP TABLE %s;", dialect.Identifier(path...))

	return stmt, func(ctx context.Context) error {
		_, err := c.db.ExecContext(ctx, stmt)
		return err
	}, nil
}

func (c *client) AlterTable(ctx context.Context, ta sql.TableAlter) (string, boilerplate.ActionApplyFn, error) {
	if len(ta.DropNotNulls) != 0 {
		return "", nil, fmt.Errorf("cannot drop nullability constraints but got %d DropNotNulls for table %s", len(ta.DropNotNulls), ta.Identifier)
	}

	var addColumnsQuery []string
	if len(ta.AddColumns) > 0 {
		var addColumnsStmt strings.Builder
		if err := tplAlterTableColumns.Execute(&addColumnsStmt, ta); err != nil {
			return "", nil, fmt.Errorf("rendering alter table columns statement: %w", err)
		}

		addColumnsQuery = append(addColumnsQuery, addColumnsStmt.String())
	}

	var migrateQueries []string
	if len(ta.ColumnTypeChanges) > 0 {
		sourceTable := ta.Table.Identifier
		tmpTable := dialect.Identifier(ta.InfoLocation.TableSchema, uuid.NewString())

		params := migrateParams{
			SourceTable: sourceTable,
			TmpName:     tmpTable,
		}

		for _, col := range ta.Columns() {
			mCol := migrateColumn{Identifier: col.Identifier}

			if n := slices.IndexFunc(ta.ColumnTypeChanges, func(m sql.ColumnTypeMigration) bool {
				return m.Identifier == col.Identifier
			}); n != -1 {
				m := ta.ColumnTypeChanges[n]
				mCol.CastSQL = m.CastSQL(m)
			}

			params.Columns = append(params.Columns, mCol)
		}

		var migrateTableQuery strings.Builder
		if err := tplCreateMigrationTable.Execute(&migrateTableQuery, params); err != nil {
			return "", nil, fmt.Errorf("rendering create migration table statement: %w", err)
		}

		migrateQueries = []string{
			migrateTableQuery.String(),
			fmt.Sprintf("DROP TABLE %s;", sourceTable),
			fmt.Sprintf(
				"EXEC sp_rename %s, %s;",
				dialect.Literal(tmpTable),
				dialect.Literal(dialect.Identifier(ta.InfoLocation.TableName))),
		}
	}

	allQueries := append(addColumnsQuery, migrateQueries...)

	return strings.Join(allQueries, "\n"), func(ctx context.Context) error {
		if len(addColumnsQuery) == 1 { // slice is either empty or has a single query
			if _, err := c.db.ExecContext(ctx, addColumnsQuery[0]); err != nil {
				log.WithField("query", addColumnsQuery[0]).Error("alter table query failed")
				return err
			}
		}

		// The queries for a table migration are run in a transaction since this
		// involves copying the existing table into a "temporary" table with a
		// different name, dropping the original table, and renaming the
		// temporary table to replace the original table. This will all be done
		// as an atomic action via the transaction.
		txn, err := c.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("db.BeginTx: %w", err)
		}
		defer txn.Rollback()

		for _, query := range migrateQueries {
			if _, err := txn.ExecContext(ctx, query); err != nil {
				log.WithField("query", query).Error("migrate table query failed")
				return err
			}
		}

		if err := txn.Commit(); err != nil {
			return fmt.Errorf("txn.Commit: %w", err)
		}

		return nil
	}, nil
}

func (c *client) ListSchemas(ctx context.Context) ([]string, error) {
	rows, err := c.db.QueryContext(ctx, "select schema_name from INFORMATION_SCHEMA.SCHEMATA")
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
	return sql.StdCreateSchema(ctx, c.db, dialect, schemaName)
}

type badRequestResponseBody struct {
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
	ErrorCodes       []int  `json:"error_codes"`
}

func preReqs(ctx context.Context, cfg config, tenant string) *cerrors.PrereqErr {
	errs := &cerrors.PrereqErr{}

	db, err := cfg.db()
	if err != nil {
		errs.Err(err)
		return errs
	}

	pingCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	var wh int
	if err := db.QueryRowContext(pingCtx, fmt.Sprintf("SELECT 1 from sys.databases WHERE name = %s;", dialect.Literal(cfg.Warehouse))).Scan(&wh); err != nil {
		var authErr *azidentity.AuthenticationFailedError
		var netOpErr *net.OpError

		if errors.As(err, &netOpErr) {
			err = fmt.Errorf("could not connect to endpoint: ensure the connection string '%s' is correct", cfg.ConnectionString)
		} else if errors.Is(err, stdsql.ErrNoRows) {
			err = fmt.Errorf("warehouse '%s' does not exist", cfg.Warehouse)
		} else if errors.As(err, &authErr) {
			var res badRequestResponseBody
			if err := json.NewDecoder(authErr.RawResponse.Body).Decode(&res); err != nil {
				panic(err)
			}

			if slices.Contains(res.ErrorCodes, 700016) {
				err = fmt.Errorf("invalid client ID '%s': ensure that the client ID for the correct application is configured", cfg.ClientID)
			} else if slices.Contains(res.ErrorCodes, 7000215) {
				err = fmt.Errorf("invalid client secret provided: ensure the secret being sent in the request is the client secret value, not the client secret ID, for a secret added to app '%s'", cfg.ClientID)
			}

			log.WithField("response", res).Error("connection error")
		}

		errs.Err(err)
	}

	bucket, err := blob.NewAzureBlobBucket(
		ctx,
		cfg.ContainerName,
		cfg.StorageAccountName,
		blob.WithAzureStorageAccountKey(cfg.StorageAccountKey),
	)
	if err != nil {
		errs.Err(err)
		return errs
	}
	if err := bucket.CheckPermissions(ctx, blob.CheckPermissionsConfig{}); err != nil {
		errs.Err(err)
	}

	return errs
}

func (c *client) ExecStatements(ctx context.Context, statements []string) error {
	return sql.StdSQLExecStatements(ctx, c.db, statements)
}

func (c *client) InstallFence(ctx context.Context, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	var txn, err = c.db.BeginTx(ctx, nil)
	if err != nil {
		return sql.Fence{}, fmt.Errorf("db.BeginTx: %w", err)
	}
	defer func() {
		if txn != nil {
			_ = txn.Rollback()
		}
	}()

	// Increment the fence value of _any_ checkpoint which overlaps our key range.
	if _, err = txn.Exec(
		fmt.Sprintf(`
			UPDATE %s
				SET fence=fence+1
				WHERE materialization=%s
				AND key_end>=%s
				AND key_begin<=%s
			;
			`,
			checkpoints.Identifier,
			checkpoints.Keys[0].Placeholder,
			checkpoints.Keys[1].Placeholder,
			checkpoints.Keys[2].Placeholder,
		),
		fence.Materialization,
		fence.KeyBegin,
		fence.KeyEnd,
	); err != nil {
		return sql.Fence{}, fmt.Errorf("incrementing fence: %w", err)
	}

	// Read the checkpoint with the narrowest [key_begin, key_end] which fully overlaps our range.
	var readBegin, readEnd uint32
	var checkpoint string

	if err = txn.QueryRow(
		fmt.Sprintf(`
			SELECT TOP 1 fence, key_begin, key_end, "checkpoint"
				FROM %s
				WHERE materialization=%s
				AND key_begin<=%s
				AND key_end>=%s
				ORDER BY key_end - key_begin ASC
			;
			`,
			checkpoints.Identifier,
			checkpoints.Keys[0].Placeholder,
			checkpoints.Keys[1].Placeholder,
			checkpoints.Keys[2].Placeholder,
		),
		fence.Materialization,
		fence.KeyBegin,
		fence.KeyEnd,
	).Scan(&fence.Fence, &readBegin, &readEnd, &checkpoint); err == stdsql.ErrNoRows {
		// Set an invalid range, which compares as unequal to trigger an insertion below.
		readBegin, readEnd = 1, 0
	} else if err != nil {
		return sql.Fence{}, fmt.Errorf("scanning fence and checkpoint: %w", err)
	} else if fence.Checkpoint, err = base64.StdEncoding.DecodeString(checkpoint); err != nil {
		return sql.Fence{}, fmt.Errorf("base64.Decode(checkpoint): %w", err)
	}

	// If a checkpoint for this exact range doesn't exist then insert it now.
	if readBegin == fence.KeyBegin && readEnd == fence.KeyEnd {
		// Exists; no-op.
	} else if _, err = txn.Exec(
		fmt.Sprintf(
			`INSERT INTO %s (materialization, key_begin, key_end, fence, "checkpoint") VALUES (%s, %s, %s, %s, %s);`,
			checkpoints.Identifier,
			checkpoints.Keys[0].Placeholder,
			checkpoints.Keys[1].Placeholder,
			checkpoints.Keys[2].Placeholder,
			checkpoints.Values[0].Placeholder,
			checkpoints.Values[1].Placeholder,
		),
		fence.Materialization,
		fence.KeyBegin,
		fence.KeyEnd,
		fence.Fence,
		base64.StdEncoding.EncodeToString(fence.Checkpoint),
	); err != nil {
		return sql.Fence{}, fmt.Errorf("inserting fence: %w", err)
	}

	err = txn.Commit()
	txn = nil // Disable deferred rollback.

	if err != nil {
		return sql.Fence{}, fmt.Errorf("txn.Commit: %w", err)
	}
	return fence, nil
}

func (c *client) Close() {
	c.db.Close()
}
